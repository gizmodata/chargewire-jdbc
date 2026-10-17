package com.gizmodata.warpsql.jdbc.arrow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/** Arrow IPC streams for tests: several batches, with or without the end-of-stream marker. */
public final class TestStreams {
  private TestStreams() {}

  /** One stream whose batches hold the given rows, ending with the end-of-stream marker. */
  public static byte[] stream(BufferAllocator allocator, Schema schema, List<List<Object[]>> batches)
      throws IOException, SQLException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ArrowIpc.HeapWriteChannel out = new ArrowIpc.HeapWriteChannel(Channels.newChannel(bytes))) {
      MessageSerializer.serialize(out, schema, IpcOption.DEFAULT);
      for (List<Object[]> rows : batches) {
        try (VectorSchemaRoot root = Rows.toRoot(allocator, schema, rows);
            ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
          MessageSerializer.serialize(out, batch, IpcOption.DEFAULT);
        }
      }
      out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
      out.writeIntLittleEndian(0);
    }
    return bytes.toByteArray();
  }

  /** The stream without its end-of-stream marker: complete batches, but an incomplete stream. */
  public static byte[] withoutEndMarker(byte[] stream) {
    return Arrays.copyOf(stream, stream.length - 8);
  }

  /** A schema serialized as a schema-only stream, as statement documents carry it. */
  public static byte[] schemaStream(BufferAllocator allocator, Schema schema) throws IOException {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      return ArrowIpc.writeStream(root);
    }
  }
}
