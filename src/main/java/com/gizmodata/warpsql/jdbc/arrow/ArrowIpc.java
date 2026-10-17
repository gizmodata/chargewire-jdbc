package com.gizmodata.warpsql.jdbc.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Reads and writes Arrow IPC streams.
 *
 * <p>Arrow Java's own channels move bytes through {@link ArrowBuf#nioBuffer()}, which needs {@code
 * --add-opens=java.base/java.nio=ALL-UNNAMED} on JDK 17 and later. The channels here copy through
 * a heap array instead, so the driver works in any JVM without extra options. Arrow buffers are
 * still allocated off-heap with {@code sun.misc.Unsafe}.
 */
public final class ArrowIpc {
  private static final int COPY_CHUNK = 64 * 1024;

  static {
    // Arrow registers its canonical UUID type only when the class loads; register it before
    // any schema is read, so arrow.uuid columns always decode as UUIDs.
    ExtensionTypeRegistry.register(UuidType.INSTANCE);
  }

  private ArrowIpc() {}

  /**
   * Opens a stream reader that decodes ZSTD and LZ4 compressed buffers.
   *
   * @param channel where the stream comes from
   * @param allocator where batches are allocated
   * @return the reader
   */
  public static ArrowStreamReader reader(HeapReadChannel channel, BufferAllocator allocator) {
    return new ArrowStreamReader(
        new MessageChannelReader(channel, allocator), allocator, CommonsCompressionFactory.INSTANCE);
  }

  /**
   * Reads a schema serialized as an IPC message or as a schema-only IPC stream.
   *
   * @param bytes the serialized schema
   * @return the schema
   * @throws IOException if the bytes are not a schema message
   */
  public static Schema readSchema(byte[] bytes) throws IOException {
    try (HeapReadChannel channel =
        new HeapReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes)))) {
      return MessageSerializer.deserializeSchema(channel);
    }
  }

  /**
   * Serializes the rows of a root as one complete IPC stream: schema, one batch, end of stream.
   *
   * @param root the rows
   * @return the stream's bytes
   * @throws IOException if serialization fails
   */
  public static byte[] writeStream(VectorSchemaRoot root) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (HeapWriteChannel out = new HeapWriteChannel(Channels.newChannel(bytes))) {
      IpcOption option = IpcOption.DEFAULT;
      MessageSerializer.serialize(out, root.getSchema(), option);
      if (root.getRowCount() > 0) {
        VectorUnloader unloader = new VectorUnloader(root);
        try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
          MessageSerializer.serialize(out, batch, option);
        }
      }
      out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
      out.writeIntLittleEndian(0);
    }
    return bytes.toByteArray();
  }

  /**
   * Reads into Arrow buffers through a heap array, and remembers whether the input ended.
   *
   * <p>Arrow's reader treats a clean end of input exactly like the end-of-stream marker; {@link
   * #reachedEnd()} tells them apart, so a stream cut at a message boundary is still detected as
   * incomplete (spec §6.6).
   */
  public static final class HeapReadChannel extends ReadChannel {
    private boolean reachedEnd;

    /**
     * Wraps a channel.
     *
     * @param in the input
     */
    public HeapReadChannel(ReadableByteChannel in) {
      super(in);
    }

    /**
     * Wraps a stream.
     *
     * @param in the input
     */
    public HeapReadChannel(InputStream in) {
      this(Channels.newChannel(in));
    }

    /**
     * Returns whether a read ran out of input.
     *
     * @return {@code true} once the input ended before a read was satisfied
     */
    public boolean reachedEnd() {
      return reachedEnd;
    }

    @Override
    public int readFully(ByteBuffer buffer) throws IOException {
      int read = super.readFully(buffer);
      if (buffer.hasRemaining()) {
        reachedEnd = true;
      }
      return read;
    }

    @Override
    public long readFully(ArrowBuf buffer, long length) throws IOException {
      byte[] chunk = new byte[(int) Math.min(COPY_CHUNK, Math.max(length, 1))];
      long total = 0;
      while (total < length) {
        int wanted = (int) Math.min(chunk.length, length - total);
        int read = readFully(ByteBuffer.wrap(chunk, 0, wanted));
        buffer.setBytes(buffer.writerIndex(), chunk, 0, read);
        buffer.writerIndex(buffer.writerIndex() + read);
        total += read;
        if (read < wanted) {
          break;
        }
      }
      return total;
    }
  }

  /** Writes Arrow buffers through a heap array. */
  static final class HeapWriteChannel extends WriteChannel {
    HeapWriteChannel(WritableByteChannel out) {
      super(out);
    }

    @Override
    public void write(ArrowBuf buffer) throws IOException {
      long length = buffer.readableBytes();
      byte[] chunk = new byte[(int) Math.min(COPY_CHUNK, Math.max(length, 1))];
      long written = 0;
      while (written < length) {
        int count = (int) Math.min(chunk.length, length - written);
        buffer.getBytes(buffer.readerIndex() + written, chunk, 0, count);
        write(ByteBuffer.wrap(chunk, 0, count));
        written += count;
      }
    }
  }
}
