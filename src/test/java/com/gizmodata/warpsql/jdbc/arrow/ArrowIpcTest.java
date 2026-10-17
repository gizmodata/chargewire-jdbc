package com.gizmodata.warpsql.jdbc.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The heap-copy IPC channels, and how an incomplete stream is told from a complete one. */
class ArrowIpcTest {
  private final BufferAllocator allocator = new RootAllocator();
  private static final Schema SCHEMA = new Schema(List.of(
      Field.nullable("i", new ArrowType.Int(64, true)), Field.nullable("s", ArrowType.Utf8.INSTANCE)));

  @AfterEach
  void close() {
    allocator.close();
  }

  private byte[] stream(int rows) throws Exception {
    List<Object[]> values = new ArrayList<>();
    for (int i = 0; i < rows; i++) {
      values.add(new Object[] {(long) i, i % 3 == 0 ? null : "s" + i});
    }
    try (VectorSchemaRoot root = Rows.toRoot(allocator, SCHEMA, values)) {
      return ArrowIpc.writeStream(root);
    }
  }

  /** Reads every batch; returns the rows read, or throws if the input breaks off. */
  private long read(byte[] bytes, boolean[] reachedEnd) throws IOException {
    long rows = 0;
    try (ArrowIpc.HeapReadChannel channel = new ArrowIpc.HeapReadChannel(new ByteArrayInputStream(bytes));
        ArrowStreamReader reader = ArrowIpc.reader(channel, allocator)) {
      while (reader.loadNextBatch()) {
        BigIntVector ints = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        for (int r = 0; r < reader.getVectorSchemaRoot().getRowCount(); r++) {
          assertEquals(rows + r, ints.get(r));
        }
        rows += reader.getVectorSchemaRoot().getRowCount();
      }
      reachedEnd[0] = channel.reachedEnd();
    }
    return rows;
  }

  @Test
  void roundTripsThroughHeapChannels() throws Exception {
    boolean[] reachedEnd = new boolean[1];
    assertEquals(100_000, read(stream(100_000), reachedEnd));
    assertFalse(reachedEnd[0], "a complete stream ends with its end-of-stream marker");
  }

  @Test
  void anEmptyResultIsStillAStream() throws Exception {
    boolean[] reachedEnd = new boolean[1];
    assertEquals(0, read(stream(0), reachedEnd));
    assertFalse(reachedEnd[0]);
  }

  @Test
  void aStreamCutAtAMessageBoundaryIsIncomplete() throws Exception {
    byte[] complete = stream(10);
    byte[] withoutMarker = Arrays.copyOf(complete, complete.length - 8);
    boolean[] reachedEnd = new boolean[1];
    assertEquals(10, read(withoutMarker, reachedEnd), "every batch arrived");
    assertTrue(reachedEnd[0], "but the end-of-stream marker did not, which Arrow alone would miss");
  }

  @Test
  void aStreamCutMidBatchFails() throws Exception {
    byte[] complete = stream(10_000);
    byte[] cut = Arrays.copyOf(complete, complete.length / 2);
    assertThrows(IOException.class, () -> read(cut, new boolean[1]));
  }

  @Test
  void readsASchemaFromAStreamOrAMessage() throws Exception {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      byte[] schemaOnly = ArrowIpc.writeStream(root);
      assertEquals(SCHEMA, ArrowIpc.readSchema(schemaOnly));
      byte[] message = Arrays.copyOf(schemaOnly, schemaOnly.length - 8);
      assertEquals(SCHEMA, ArrowIpc.readSchema(message));
    }
    assertThrows(IOException.class, () -> ArrowIpc.readSchema(new byte[] {1, 2, 3}));
  }
}
