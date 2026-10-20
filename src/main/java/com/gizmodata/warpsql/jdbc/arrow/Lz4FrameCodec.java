package com.gizmodata.warpsql.jdbc.arrow;

import java.lang.ref.SoftReference;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * Arrow's {@code LZ4_FRAME} buffer compression, decoded by {@link Lz4Frame}.
 *
 * <p>Arrow Java's own LZ4 codec goes through commons-compress streams and decodes at a fraction of
 * the speed, and lz4-java's frame reader refuses the linked blocks Arrow C++ writes. This one
 * copies each compressed buffer to a heap array, decodes it there and copies the result into an
 * Arrow buffer. The heap arrays are kept per thread (softly, so the garbage collector may take
 * them) up to {@link #MAX_CACHED} bytes each.
 *
 * <p>The driver does not compress uploads, so {@link #compress} is not supported.
 */
final class Lz4FrameCodec extends AbstractCompressionCodec {
  /** The largest scratch array kept for the next buffer; larger ones are allocated each time. */
  private static final int MAX_CACHED = 16 << 20;

  private static final ThreadLocal<SoftReference<byte[]>> SOURCE = new ThreadLocal<>();
  private static final ThreadLocal<SoftReference<byte[]>> TARGET = new ThreadLocal<>();

  static final Lz4FrameCodec INSTANCE = new Lz4FrameCodec();

  private Lz4FrameCodec() {}

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    throw new UnsupportedOperationException("The driver does not compress Arrow buffers");
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    long length = readUncompressedLength(compressedBuffer);
    long compressed = compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
    if (length < 0 || length > Integer.MAX_VALUE - Lz4Frame.SLACK) {
      throw new IllegalArgumentException(
          "Malformed LZ4 buffer: uncompressed length " + length + " is out of range");
    }
    if (compressed > Integer.MAX_VALUE - Lz4Frame.SLACK) {
      throw new IllegalArgumentException("LZ4 buffer of " + compressed + " bytes is too large");
    }
    int size = (int) length;
    int sourceSize = (int) compressed;
    byte[] source = scratch(SOURCE, sourceSize + Lz4Frame.SLACK);
    byte[] target = scratch(TARGET, size + Lz4Frame.SLACK);
    compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, source, 0, sourceSize);
    Lz4Frame.decode(source, sourceSize, target, size);
    ArrowBuf out = allocator.buffer(size);
    try {
      out.setBytes(0, target, 0, size);
    } catch (RuntimeException e) {
      out.close();
      throw e;
    }
    out.writerIndex(size);
    return out;
  }

  /** Returns a thread's scratch array of at least {@code size} bytes. */
  private static byte[] scratch(ThreadLocal<SoftReference<byte[]>> cache, int size) {
    SoftReference<byte[]> reference = cache.get();
    byte[] array = reference == null ? null : reference.get();
    if (array != null && array.length >= size) {
      return array;
    }
    // Grow by half again, so a stream of slowly growing buffers does not reallocate each time.
    int capacity = size > MAX_CACHED ? size : Math.min(MAX_CACHED, size + (size >> 1));
    array = new byte[capacity];
    if (capacity <= MAX_CACHED) {
      cache.set(new SoftReference<>(array));
    }
    return array;
  }
}
