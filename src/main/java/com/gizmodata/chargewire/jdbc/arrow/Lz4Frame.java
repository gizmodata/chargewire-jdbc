package com.gizmodata.chargewire.jdbc.arrow;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Decodes the LZ4 frame format (lz4_Frame_format.md, version 1.6), the format of Arrow's {@code
 * LZ4_FRAME} buffer compression.
 *
 * <p>Arrow C++ (and so pyarrow, DuckDB's Arrow output and Hardcharger) writes frames of linked 64
 * KiB blocks without checksums, which lz4-java's {@code LZ4FrameInputStream} refuses; Arrow Go
 * writes independent 4 MiB blocks with a content checksum. This decoder reads both: it decodes a
 * frame's blocks into one contiguous array, so a linked block's matches reach back into the blocks
 * before it. It verifies the header checksum, and the block and content checksums when a frame has
 * them. Concatenated and skippable frames are read too; dictionary frames are refused.
 *
 * <p>All access goes through bounds-checked Java arrays: malformed input fails with an {@link
 * IllegalArgumentException} and cannot read or write outside the arrays.
 */
final class Lz4Frame {
  /**
   * How far the decoder may write past the end of the output, and read past the end of the input:
   * both arrays must have this many spare bytes at the end.
   */
  static final int SLACK = 32;

  private static final int MAGIC = 0x184D2204;
  private static final int SKIPPABLE_MAGIC = 0x184D2A50;
  private static final int SKIPPABLE_MASK = 0xFFFFFFF0;
  private static final int MIN_MATCH = 4;

  private static final VarHandle LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private Lz4Frame() {}

  /**
   * Decodes LZ4 frames that hold exactly {@code length} bytes.
   *
   * @param src the frames, at {@code src[0..srcLength)}, followed by at least {@link #SLACK} bytes
   * @param srcLength the frames' length
   * @param dst where the content goes, at {@code dst[0..length)}; at least {@code length + SLACK}
   *     long
   * @param length how many bytes the frames must hold
   * @throws IllegalArgumentException if the input is not LZ4 frames of {@code length} bytes
   */
  static void decode(byte[] src, int srcLength, byte[] dst, int length) {
    if (src.length - srcLength < SLACK || dst.length - length < SLACK) {
      throw new IllegalArgumentException("LZ4 decode buffers need " + SLACK + " spare bytes");
    }
    int s = 0;
    int d = 0;
    boolean anyFrame = false;
    while (s < srcLength || !anyFrame) {
      if (srcLength - s < 4) {
        throw malformed("truncated frame magic");
      }
      int magic = (int) INT.get(src, s);
      s += 4;
      if ((magic & SKIPPABLE_MASK) == SKIPPABLE_MAGIC) {
        if (srcLength - s < 4) {
          throw malformed("truncated skippable frame");
        }
        long size = Integer.toUnsignedLong((int) INT.get(src, s));
        s += 4;
        if (size > srcLength - s) {
          throw malformed("truncated skippable frame");
        }
        s += (int) size;
        continue;
      }
      if (magic != MAGIC) {
        throw malformed("not an LZ4 frame (magic " + Integer.toHexString(magic) + ")");
      }
      anyFrame = true;
      long packed = frame(src, s, srcLength, dst, d, length);
      s = (int) (packed >>> 32);
      d = (int) packed;
    }
    if (d != length) {
      throw malformed("the frames hold " + d + " bytes, not " + length);
    }
  }

  /** Decodes one frame after its magic; returns the input and output positions after it. */
  private static long frame(byte[] src, int s, int srcEnd, byte[] dst, int d, int dstEnd) {
    int descriptor = s;
    if (srcEnd - s < 3) {
      throw malformed("truncated frame descriptor");
    }
    int flg = src[s++] & 0xFF;
    int bd = src[s++] & 0xFF;
    if ((flg >>> 6) != 1) {
      throw malformed("unsupported frame version " + (flg >>> 6));
    }
    if ((flg & 0x02) != 0 || (bd & 0x8F) != 0) {
      throw malformed("reserved descriptor bits are set");
    }
    boolean independent = (flg & 0x20) != 0;
    boolean blockChecksum = (flg & 0x10) != 0;
    boolean hasContentSize = (flg & 0x08) != 0;
    boolean contentChecksum = (flg & 0x04) != 0;
    if ((flg & 0x01) != 0) {
      throw malformed("frames with a dictionary are not supported");
    }
    int blockSizeId = (bd >>> 4) & 0x07;
    if (blockSizeId < 4) {
      throw malformed("invalid block size " + blockSizeId);
    }
    int maxBlock = 1 << (8 + 2 * blockSizeId);
    long contentSize = -1;
    if (hasContentSize) {
      if (srcEnd - s < 9) {
        throw malformed("truncated frame descriptor");
      }
      contentSize = (long) LONG.get(src, s);
      s += 8;
    }
    int headerChecksum = (XxHash32.hash(src, descriptor, s - descriptor, 0) >>> 8) & 0xFF;
    if ((src[s++] & 0xFF) != headerChecksum) {
      throw malformed("frame descriptor checksum mismatch");
    }
    int frameStart = d;
    while (true) {
      if (srcEnd - s < 4) {
        throw malformed("truncated block size");
      }
      int word = (int) INT.get(src, s);
      s += 4;
      if (word == 0) {
        break;
      }
      int size = word & 0x7FFFFFFF;
      if (size > maxBlock || size > srcEnd - s) {
        throw malformed("block of " + size + " bytes is too large or truncated");
      }
      if (blockChecksum) {
        if (srcEnd - s - size < 4) {
          throw malformed("truncated block checksum");
        }
        if ((int) INT.get(src, s + size) != XxHash32.hash(src, s, size, 0)) {
          throw malformed("block checksum mismatch");
        }
      }
      if (word < 0) {
        if (size > dstEnd - d) {
          throw malformed("more content than expected");
        }
        System.arraycopy(src, s, dst, d, size);
        d += size;
      } else {
        int produced = block(src, s, s + size, dst, d, dstEnd, independent ? d : frameStart);
        if (produced - d > maxBlock) {
          throw malformed("a block decodes to more than the frame's block size");
        }
        d = produced;
      }
      s += size;
      if (blockChecksum) {
        s += 4;
      }
    }
    if (contentChecksum) {
      if (srcEnd - s < 4) {
        throw malformed("truncated content checksum");
      }
      if ((int) INT.get(src, s) != XxHash32.hash(dst, frameStart, d - frameStart, 0)) {
        throw malformed("content checksum mismatch");
      }
      s += 4;
    }
    if (contentSize >= 0 && contentSize != d - frameStart) {
      throw malformed("the frame holds " + (d - frameStart) + " bytes, not " + contentSize);
    }
    return ((long) s << 32) | (d & 0xFFFFFFFFL);
  }

  /**
   * Decodes one LZ4 block from {@code src[s..srcEnd)} into {@code dst} from {@code d}; matches may
   * reach back to {@code windowStart}. Returns the output position after the block.
   */
  private static int block(
      byte[] src, int s, int srcEnd, byte[] dst, int d, int dstEnd, int windowStart) {
    final int dstLimit = dst.length - 16;
    final int srcLimit = src.length - 16;
    if (s == srcEnd) {
      throw malformed("empty compressed block");
    }
    while (true) {
      int token = src[s++] & 0xFF;

      int literals = token >>> 4;
      if (literals == 15) {
        int b;
        do {
          if (s >= srcEnd) {
            throw malformed("truncated literal length");
          }
          b = src[s++] & 0xFF;
          literals += b;
        } while (b == 255);
        if (literals < 0) {
          throw malformed("literal length overflows");
        }
      }
      if (literals > srcEnd - s || literals > dstEnd - d) {
        throw malformed("literals run past the block or the content");
      }
      if (literals <= 16 && s <= srcLimit && d <= dstLimit) {
        LONG.set(dst, d, (long) LONG.get(src, s));
        LONG.set(dst, d + 8, (long) LONG.get(src, s + 8));
      } else {
        System.arraycopy(src, s, dst, d, literals);
      }
      s += literals;
      d += literals;
      if (s == srcEnd) {
        return d;
      }

      if (srcEnd - s < 2) {
        throw malformed("truncated match offset");
      }
      int offset = (src[s] & 0xFF) | ((src[s + 1] & 0xFF) << 8);
      s += 2;
      if (offset == 0 || offset > d - windowStart) {
        throw malformed("match offset " + offset + " is outside the window");
      }
      int match = token & 0x0F;
      if (match == 15) {
        int b;
        do {
          if (s >= srcEnd) {
            throw malformed("truncated match length");
          }
          b = src[s++] & 0xFF;
          match += b;
        } while (b == 255);
        if (match < 0 || match > Integer.MAX_VALUE - MIN_MATCH) {
          throw malformed("match length overflows");
        }
      }
      match += MIN_MATCH;
      if (match > dstEnd - d) {
        throw malformed("match runs past the content");
      }
      copyMatch(dst, d - offset, d, match, offset, dstLimit);
      d += match;
    }
  }

  /**
   * Copies a match of {@code length} bytes from {@code from} to {@code to}, {@code offset} bytes
   * back; it may overlap the bytes it writes, which then repeat with a period of {@code offset}.
   */
  private static void copyMatch(byte[] dst, int from, int to, int length, int offset, int limit) {
    int end = to + length;
    if (end > limit) {
      // Too near the end of the array for copies that run past the match.
      if (offset >= length) {
        System.arraycopy(dst, from, dst, to, length);
      } else {
        repeat(dst, from, to, length);
      }
      return;
    }
    if (offset < 8) {
      // Write the pattern byte by byte until it has repeated over at least eight bytes; from
      // there on, eight-byte copies a whole number of periods back read bytes already written.
      int period = offset;
      while (period < 8) {
        period += offset;
      }
      int head = Math.min(period, length);
      for (int i = 0; i < head; i++) {
        dst[to + i] = dst[from + i];
      }
      if (head == length) {
        return;
      }
      to += period;
      from = to - period;
    } else if (offset >= 16) {
      // Sixteen bytes at a time, past the match's end into the spare bytes.
      do {
        long a = (long) LONG.get(dst, from);
        long b = (long) LONG.get(dst, from + 8);
        LONG.set(dst, to, a);
        LONG.set(dst, to + 8, b);
        from += 16;
        to += 16;
      } while (to < end);
      return;
    }
    // Eight bytes at a time, past the match's end into the spare bytes: each read is at least
    // eight bytes behind the write, so it reads bytes already written.
    while (to < end) {
      LONG.set(dst, to, (long) LONG.get(dst, from));
      from += 8;
      to += 8;
    }
  }

  /** Copies an overlapping match by doubling the repeated pattern with each copy. */
  private static void repeat(byte[] dst, int from, int to, int length) {
    int done = 0;
    while (done < length) {
      int chunk = Math.min(to + done - from, length - done);
      System.arraycopy(dst, from, dst, to + done, chunk);
      done += chunk;
    }
  }

  private static IllegalArgumentException malformed(String why) {
    return new IllegalArgumentException("Malformed LZ4 frame: " + why);
  }

  /** XXH32, the checksum of the LZ4 frame format. */
  static final class XxHash32 {
    private static final int P1 = 0x9E3779B1;
    private static final int P2 = 0x85EBCA77;
    private static final int P3 = 0xC2B2AE3D;
    private static final int P4 = 0x27D4EB2F;
    private static final int P5 = 0x165667B1;

    private XxHash32() {}

    static int hash(byte[] data, int offset, int length, int seed) {
      int end = offset + length;
      int p = offset;
      int h;
      if (length >= 16) {
        int v1 = seed + P1 + P2;
        int v2 = seed + P2;
        int v3 = seed;
        int v4 = seed - P1;
        int limit = end - 16;
        while (p <= limit) {
          v1 = Integer.rotateLeft(v1 + (int) INT.get(data, p) * P2, 13) * P1;
          v2 = Integer.rotateLeft(v2 + (int) INT.get(data, p + 4) * P2, 13) * P1;
          v3 = Integer.rotateLeft(v3 + (int) INT.get(data, p + 8) * P2, 13) * P1;
          v4 = Integer.rotateLeft(v4 + (int) INT.get(data, p + 12) * P2, 13) * P1;
          p += 16;
        }
        h =
            Integer.rotateLeft(v1, 1)
                + Integer.rotateLeft(v2, 7)
                + Integer.rotateLeft(v3, 12)
                + Integer.rotateLeft(v4, 18);
      } else {
        h = seed + P5;
      }
      h += length;
      while (p + 4 <= end) {
        h = Integer.rotateLeft(h + (int) INT.get(data, p) * P3, 17) * P4;
        p += 4;
      }
      while (p < end) {
        h = Integer.rotateLeft(h + (data[p] & 0xFF) * P5, 11) * P1;
        p++;
      }
      h ^= h >>> 15;
      h *= P2;
      h ^= h >>> 13;
      h *= P3;
      h ^= h >>> 16;
      return h;
    }
  }
}
