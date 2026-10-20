package com.gizmodata.warpsql.jdbc.arrow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The LZ4 frame decoder behind {@code LZ4_FRAME} results, on frames from the reference {@code lz4}
 * tool (linked and independent blocks, every block size, block and content checksums, content size,
 * stored blocks), on Arrow IPC streams from Arrow C++ (pyarrow) and Arrow Java, and on malformed
 * input. The fixtures in {@code src/test/resources/lz4} come from {@code dev/lz4-fixtures}; the
 * contents they hold are rebuilt here the way those scripts build them.
 */
class Lz4FrameTest {
  private final BufferAllocator allocator = new RootAllocator();

  @AfterEach
  void everyBufferIsFreed() {
    assertEquals(0, allocator.getAllocatedMemory(), "Arrow memory left allocated");
    allocator.close();
  }

  private static long lcg(long x) {
    return x * 6364136223846793005L + 1442695040888963407L;
  }

  /** {@code text(3000)} of {@code gen_content.py}: about 100 KB of compressible lines. */
  private static byte[] text() {
    StringBuilder out = new StringBuilder();
    long x = 1;
    for (int i = 0; i < 3000; i++) {
      x = lcg(x);
      out.append(
          String.format(
              Locale.ROOT,
              "row %06d status=%s value=%d\n",
              i,
              (x >>> 33) % 4 != 0 ? "OK" : "FAIL",
              (x >>> 40) % 100000));
    }
    return out.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** {@code noise(70000)} of {@code gen_content.py}: bytes that do not compress. */
  private static byte[] noise() {
    byte[] out = new byte[70000];
    long x = 2;
    for (int i = 0; i < out.length; i++) {
      x = lcg(x);
      out[i] = (byte) (x >>> 56);
    }
    return out;
  }

  /** {@code gen_runs.py}: runs with periods 1 to 20, so matches overlap the bytes they write. */
  private static byte[] runs() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int period = 1; period <= 20; period++) {
      for (int k = 0; k < 3000; k++) {
        out.write((period * 7 + k % period) % 256);
      }
    }
    return out.toByteArray();
  }

  private static byte[] resource(String name) throws IOException {
    try (InputStream in = Lz4FrameTest.class.getResourceAsStream("/lz4/" + name)) {
      if (in == null) {
        throw new IOException("missing test resource " + name);
      }
      return in.readAllBytes();
    }
  }

  /** Decodes frames that must hold {@code length} bytes. */
  private static byte[] decode(byte[] frames, int length) {
    byte[] src = Arrays.copyOf(frames, frames.length + Lz4Frame.SLACK);
    byte[] dst = new byte[length + Lz4Frame.SLACK];
    Lz4Frame.decode(src, frames.length, dst, length);
    return Arrays.copyOf(dst, length);
  }

  @Test
  void xxHash32MatchesTheReference() {
    assertEquals(0x02CC5D05, Lz4Frame.XxHash32.hash(new byte[0], 0, 0, 0));
    byte[] abc = "abc".getBytes(StandardCharsets.US_ASCII);
    assertEquals(0x32D153FF, Lz4Frame.XxHash32.hash(abc, 0, 3, 0));
    byte[] fox = "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.US_ASCII);
    assertEquals(0xE85EA4DE, Lz4Frame.XxHash32.hash(fox, 0, fox.length, 0));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "text-default.lz4",
        "text-linked-64k.lz4",
        "text-block-checksums.lz4",
        "text-content-size.lz4"
      })
  void decodesTheReferenceToolsFrames(String name) throws IOException {
    byte[] expected = text();
    assertArrayEquals(expected, decode(resource(name), expected.length));
  }

  @Test
  void decodesStoredBlocks() throws IOException {
    byte[] expected = noise();
    assertArrayEquals(expected, decode(resource("noise-64k.lz4"), expected.length));
  }

  @Test
  void decodesMatchesThatOverlapWhatTheyWrite() throws IOException {
    byte[] expected = runs();
    assertArrayEquals(expected, decode(resource("runs-linked.lz4"), expected.length));
  }

  @Test
  void decodesConcatenatedAndSkippableFrames() throws IOException {
    byte[] text = text();
    byte[] noise = noise();
    ByteArrayOutputStream frames = new ByteArrayOutputStream();
    frames.write(resource("text-linked-64k.lz4"));
    frames.write(new byte[] {0x53, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 1, 2, 3});
    frames.write(resource("noise-64k.lz4"));
    byte[] expected = Arrays.copyOf(text, text.length + noise.length);
    System.arraycopy(noise, 0, expected, text.length, noise.length);
    assertArrayEquals(expected, decode(frames.toByteArray(), expected.length));
  }

  private static void assertMalformed(byte[] frames, int length, String why) {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> decode(frames, length));
    assertTrue(error.getMessage().contains(why), error.getMessage());
  }

  @Test
  void refusesContentOfAnotherLength() throws IOException {
    byte[] frames = resource("text-linked-64k.lz4");
    int length = text().length;
    assertMalformed(frames, length + 1, "hold " + length + " bytes");
    assertMalformed(frames, length - 1, "content");
    assertMalformed(resource("text-content-size.lz4"), length - 1, "content");
  }

  @Test
  void refusesTruncatedFrames() throws IOException {
    byte[] frames = resource("text-linked-64k.lz4");
    int length = text().length;
    for (int cut = 0; cut < frames.length; cut += 997) {
      byte[] truncated = Arrays.copyOf(frames, cut);
      assertThrows(IllegalArgumentException.class, () -> decode(truncated, length));
    }
    assertMalformed(Arrays.copyOf(frames, frames.length - 1), length, "truncated");
  }

  @Test
  void refusesBadChecksums() throws IOException {
    int length = text().length;
    byte[] blocks = resource("text-block-checksums.lz4");
    blocks[100] ^= 1;
    assertMalformed(blocks, length, "block checksum mismatch");
    byte[] content = resource("text-default.lz4");
    content[content.length - 1] ^= 1;
    assertMalformed(content, length, "content checksum mismatch");
    byte[] header = resource("text-default.lz4");
    header[6] ^= 1;
    assertMalformed(header, length, "descriptor checksum mismatch");
  }

  @Test
  void refusesWhatItDoesNotSupport() throws IOException {
    int length = text().length;
    byte[] magic = resource("text-default.lz4");
    magic[0] = 0;
    assertMalformed(magic, length, "not an LZ4 frame");
    byte[] dictionary = resource("text-linked-64k.lz4");
    dictionary[4] |= 0x01;
    assertMalformed(dictionary, length, "dictionary");
    byte[] version = resource("text-linked-64k.lz4");
    version[4] = (byte) (version[4] & 0x3F);
    assertMalformed(version, length, "version");
    assertMalformed(new byte[0], 0, "truncated frame magic");
  }

  /** Damaged input fails with the decoder's own error or decodes to something, never worse. */
  @ParameterizedTest
  @ValueSource(strings = {"text-linked-64k.lz4", "runs-linked.lz4", "text-default.lz4"})
  void damagedInputFailsCleanly(String name) throws IOException {
    byte[] original = resource(name);
    int length = name.startsWith("runs") ? runs().length : text().length;
    Random random = new Random(7);
    int decoded = 0;
    for (int round = 0; round < 3000; round++) {
      byte[] damaged = original.clone();
      int changes = 1 + random.nextInt(4);
      for (int c = 0; c < changes; c++) {
        // Mostly the block payload, sometimes the frame's header.
        int at = random.nextInt(4) == 0 ? random.nextInt(16) : random.nextInt(damaged.length);
        damaged[at] = (byte) random.nextInt(256);
      }
      try {
        assertEquals(length, decode(damaged, length).length);
        decoded++;
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage().startsWith("Malformed LZ4"), expected.getMessage());
      }
    }
    assertTrue(decoded < 3000, "no damage was ever noticed");
  }

  // ---- Arrow IPC streams ----

  private static final Schema SCHEMA =
      new Schema(
          java.util.List.of(
              Field.nullable("i", new ArrowType.Int(64, true)),
              Field.nullable(
                  "d",
                  new ArrowType.FloatingPoint(
                      org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
              Field.nullable("s", ArrowType.Utf8.INSTANCE)));

  /** The value {@code gen_ipc.py} puts in row {@code k}'s {@code d}: random, incompressible. */
  private static double d(long k) {
    long z = k + 0x9E3779B97F4A7C15L;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    z ^= z >>> 31;
    return (z >>> 11) / 9007199254740992.0;
  }

  private static String s(long k) {
    return k % 7 == 0 ? null : "s" + (k * 31 % 1000);
  }

  /** Reads a stream, checking every row against {@code gen_ipc.py}'s values; returns the rows. */
  private long readAndCheck(byte[] stream, CompressionCodec.Factory factory) throws IOException {
    long expected = 0;
    try (ArrowStreamReader reader =
        new ArrowStreamReader(
            new MessageChannelReader(
                new ArrowIpc.HeapReadChannel(new ByteArrayInputStream(stream)), allocator),
            allocator,
            factory)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        BigIntVector i = (BigIntVector) root.getVector("i");
        Float8Vector d = (Float8Vector) root.getVector("d");
        VarCharVector s = (VarCharVector) root.getVector("s");
        for (int row = 0; row < root.getRowCount(); row++, expected++) {
          assertEquals(expected, i.get(row));
          assertEquals(d(expected), d.get(row));
          String text = s.isNull(row) ? null : new String(s.get(row), StandardCharsets.UTF_8);
          if (s(expected) == null) {
            assertNull(text, "row " + expected);
          } else {
            assertEquals(s(expected), text, "row " + expected);
          }
        }
      }
    }
    return expected;
  }

  @ParameterizedTest
  @ValueSource(strings = {"pyarrow-lz4.arrows", "pyarrow-zstd.arrows"})
  void readsArrowCppStreams(String name) throws IOException {
    assertEquals(6000, readAndCheck(resource(name), WarpCompressionFactory.INSTANCE));
  }

  /** Counts the buffers a stream left uncompressed (uncompressed length -1). */
  private static final class Counting implements CompressionCodec.Factory {
    final AtomicInteger stored = new AtomicInteger();
    final AtomicInteger compressed = new AtomicInteger();

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
      CompressionCodec codec = WarpCompressionFactory.INSTANCE.createCodec(codecType);
      return new CompressionCodec() {
        @Override
        public ArrowBuf compress(BufferAllocator allocator, ArrowBuf buffer) {
          throw new UnsupportedOperationException();
        }

        @Override
        public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf buffer) {
          if (buffer.writerIndex() >= 8) {
            (buffer.getLong(0) == CompressionUtil.NO_COMPRESSION_LENGTH ? stored : compressed)
                .incrementAndGet();
          }
          return codec.decompress(allocator, buffer);
        }

        @Override
        public CompressionUtil.CodecType getCodecType() {
          return codec.getCodecType();
        }
      };
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int level) {
      return createCodec(codecType);
    }
  }

  /**
   * Arrow Java writes LZ4 with commons-compress (independent blocks, content checksum) and leaves a
   * buffer uncompressed when compressing would grow it; both kinds are read.
   */
  @Test
  void readsArrowJavaStreamsWithUncompressedBuffers() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    CompressionCodec lz4 =
        CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
    try (ArrowIpc.HeapWriteChannel out = new ArrowIpc.HeapWriteChannel(Channels.newChannel(bytes));
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      MessageSerializer.serialize(out, SCHEMA, IpcOption.DEFAULT);
      for (int batch = 0; batch < 3; batch++) {
        int rows = 2000;
        BigIntVector i = (BigIntVector) root.getVector("i");
        Float8Vector d = (Float8Vector) root.getVector("d");
        VarCharVector s = (VarCharVector) root.getVector("s");
        root.allocateNew();
        for (int row = 0; row < rows; row++) {
          long k = batch * rows + row;
          i.setSafe(row, k);
          d.setSafe(row, d(k));
          if (s(k) == null) {
            s.setNull(row);
          } else {
            s.setSafe(row, s(k).getBytes(StandardCharsets.UTF_8));
          }
        }
        root.setRowCount(rows);
        try (ArrowRecordBatch records =
            new VectorUnloader(root, true, lz4, true).getRecordBatch()) {
          MessageSerializer.serialize(out, records, IpcOption.DEFAULT);
        }
      }
      out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
      out.writeIntLittleEndian(0);
    }
    Counting counting = new Counting();
    assertEquals(6000, readAndCheck(bytes.toByteArray(), counting));
    assertTrue(counting.stored.get() > 0, "no buffer was left uncompressed");
    assertTrue(counting.compressed.get() > 0, "no buffer was compressed");
  }
}
