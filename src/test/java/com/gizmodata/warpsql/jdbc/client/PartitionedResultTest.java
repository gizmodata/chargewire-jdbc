package com.gizmodata.warpsql.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Merging a result's partitions (spec §6.10) with fake partition streams: the order rebuilt from
 * interleaved partitions, any-order reading, empty partitions, and failure, abort and close
 * stopping every partition.
 */
class PartitionedResultTest {
  private static final Schema SCHEMA =
      new Schema(List.of(Field.nullable("v", new ArrowType.Int(64, true))));
  private static final Duration LIMIT = Duration.ofSeconds(20);

  private BufferAllocator allocator;

  @BeforeEach
  void allocate() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void everyBufferIsFreed() {
    assertEquals(0, allocator.getAllocatedMemory(), "Arrow memory left allocated");
    allocator.close();
  }

  /** A partition's stream: batches of BIGINT values, then an end, a failure or a hang. */
  private final class FakePartition implements ResultStream {
    final List<long[]> batches;
    SQLException failure;
    CountDownLatch gate;
    int gateBefore = -1;
    long delayMillis;
    volatile boolean aborted;
    volatile boolean closed;
    private final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
    private final Random random = new Random(42);
    private int position;

    FakePartition(List<long[]> batches) {
      this.batches = batches;
    }

    @Override
    public String statementId() {
      return "q_test";
    }

    @Override
    public Schema schema() {
      return SCHEMA;
    }

    @Override
    public VectorSchemaRoot root() {
      return root;
    }

    @Override
    public DictionaryProvider dictionaries() {
      return new DictionaryProvider.MapDictionaryProvider();
    }

    @Override
    public int generation() {
      return 0;
    }

    @Override
    public boolean isFinished() {
      return position >= batches.size();
    }

    @Override
    public boolean next() throws SQLException {
      if (position == gateBefore) {
        try {
          while (!gate.await(10, TimeUnit.MILLISECONDS)) {
            checkAborted();
          }
        } catch (InterruptedException e) {
          throw new SQLException("interrupted", "HY008", e);
        }
      }
      if (delayMillis > 0) {
        try {
          Thread.sleep(random.nextInt((int) delayMillis + 1));
        } catch (InterruptedException e) {
          throw new SQLException("interrupted", "HY008", e);
        }
      }
      checkAborted();
      if (position < batches.size()) {
        long[] values = batches.get(position++);
        // Like an Arrow stream reader, reuse the root: earlier batches must not change.
        BigIntVector vector = (BigIntVector) root.getVector(0);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
          vector.set(i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return true;
      }
      if (failure != null) {
        throw failure;
      }
      return false;
    }

    private void checkAborted() throws SQLException {
      if (aborted) {
        throw new SQLException("The result was closed", "24000");
      }
    }

    @Override
    public void buffer(long limitBytes) {}

    @Override
    public void abort() {
      aborted = true;
    }

    @Override
    public void close() {
      closed = true;
      root.close();
    }
  }

  /** Deals {@code batches} batches of {@code rows} rows (values 0, 1, …) over {@code k}. */
  private List<FakePartition> interleaved(int k, int batches, int rows) {
    List<List<long[]>> parts = new ArrayList<>();
    for (int p = 0; p < k; p++) {
      parts.add(new ArrayList<>());
    }
    long value = 0;
    for (int b = 0; b < batches; b++) {
      long[] batch = new long[rows];
      for (int r = 0; r < rows; r++) {
        batch[r] = value++;
      }
      parts.get(b % k).add(batch);
    }
    List<FakePartition> fakes = new ArrayList<>();
    for (List<long[]> part : parts) {
      fakes.add(new FakePartition(part));
    }
    return fakes;
  }

  private PartitionedResult start(List<FakePartition> parts, PartitionedResult.Order order) {
    List<PartitionedResult.Opener> others = new ArrayList<>();
    for (FakePartition part : parts.subList(1, parts.size())) {
      others.add(() -> part);
    }
    return new PartitionedResult("q_test", allocator, parts.get(0), others, order, () -> false);
  }

  private static List<Long> readAll(PartitionedResult result) throws SQLException {
    List<Long> values = new ArrayList<>();
    while (result.next()) {
      BigIntVector vector = (BigIntVector) result.root().getVector(0);
      for (int i = 0; i < result.root().getRowCount(); i++) {
        values.add(vector.get(i));
      }
    }
    return values;
  }

  private static List<Long> range(long count) {
    List<Long> values = new ArrayList<>();
    for (long i = 0; i < count; i++) {
      values.add(i);
    }
    return values;
  }

  @Test
  void interleavedPartitionsComeBackInResultOrder() throws Exception {
    List<FakePartition> parts = interleaved(4, 23, 5);
    for (FakePartition part : parts) {
      part.delayMillis = 3;
    }
    try (PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED)) {
      assertEquals(range(23 * 5), assertTimeoutPreemptively(LIMIT, () -> readAll(result)));
      assertTrue(result.isFinished());
      assertFalse(result.next());
    }
    for (FakePartition part : parts) {
      assertTrue(part.closed, "every partition's stream is closed");
    }
  }

  @Test
  void emptyPartitionsEndAnInterleavedResult() throws Exception {
    // Two batches over five partitions: partitions 2 to 4 have none.
    List<FakePartition> parts = interleaved(5, 2, 3);
    try (PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED)) {
      assertEquals(range(6), assertTimeoutPreemptively(LIMIT, () -> readAll(result)));
      assertTrue(result.isFinished());
    }
  }

  @Test
  void anyOrderReturnsEveryRowOnce() throws Exception {
    List<FakePartition> parts = interleaved(3, 30, 4);
    for (FakePartition part : parts) {
      part.delayMillis = 2;
    }
    try (PartitionedResult result = start(parts, PartitionedResult.Order.ANY)) {
      List<Long> values = assertTimeoutPreemptively(LIMIT, () -> readAll(result));
      Collections.sort(values);
      assertEquals(range(120), values);
    }
  }

  @Test
  void anyOrderReadsWhicheverPartitionIsReady() throws Exception {
    // Partition 0 delivers nothing until partition 1 has been read to its end, far more batches
    // than one partition may queue: an any-order reader must not wait for partition 0.
    List<long[]> slow = List.of(new long[] {-1});
    List<long[]> fast = new ArrayList<>();
    for (int i = 0; i < PartitionedResult.LANE_DEPTH * 5; i++) {
      fast.add(new long[] {i});
    }
    FakePartition first = new FakePartition(slow);
    first.gate = new CountDownLatch(1);
    first.gateBefore = 0;
    FakePartition second = new FakePartition(fast);
    try (PartitionedResult result = start(List.of(first, second), PartitionedResult.Order.ANY)) {
      List<Long> values =
          assertTimeoutPreemptively(
              LIMIT,
              () -> {
                List<Long> read = new ArrayList<>();
                for (int i = 0; i < fast.size(); i++) {
                  assertTrue(result.next());
                  read.add(((BigIntVector) result.root().getVector(0)).get(0));
                }
                first.gate.countDown();
                read.addAll(readAll(result));
                return read;
              });
      assertEquals(fast.size() + 1, values.size());
      assertEquals(-1L, values.get(values.size() - 1));
    }
  }

  /**
   * A failure reaches the reader before the result stops the other downloads (the download that
   * failed aborts them after waking the reader), so the stops are waited for, up to {@link #LIMIT}.
   */
  private static void awaitAborted(String message, FakePartition... partitions)
      throws InterruptedException {
    long deadline = System.nanoTime() + LIMIT.toNanos();
    while (true) {
      boolean all = true;
      for (FakePartition partition : partitions) {
        all &= partition.aborted;
      }
      if (all) {
        return;
      }
      assertTrue(System.nanoTime() < deadline, message);
      Thread.sleep(5);
    }
  }

  @Test
  void aFailingPartitionFailsTheWholeResult() throws Exception {
    List<FakePartition> parts = interleaved(3, 30, 2);
    SQLException boom = new SQLException("boom", "HY000");
    parts.get(1).failure = boom;
    // Partition 2 never ends by itself: the failure must stop it.
    parts.get(2).gate = new CountDownLatch(1);
    parts.get(2).gateBefore = parts.get(2).batches.size();
    try (PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED)) {
      SQLException error =
          assertTimeoutPreemptively(
              LIMIT, () -> assertThrows(SQLException.class, () -> readAll(result)));
      assertSame(boom, error, "the failing partition's own error");
      assertFalse(result.isFinished(), "a failed result never ends cleanly");
      assertSame(boom, assertThrows(SQLException.class, result::next), "and stays failed");
      awaitAborted("the other downloads stop", parts.get(0), parts.get(2));
    }
  }

  @Test
  void aFailureWakesAReaderWaitingForAnotherPartition() throws Exception {
    // The reader waits for partition 0, which hangs; partition 1 fails.
    FakePartition hanging = new FakePartition(List.of(new long[] {0}));
    hanging.gate = new CountDownLatch(1);
    hanging.gateBefore = 0;
    FakePartition failing = new FakePartition(List.of());
    failing.failure = new SQLException("cancelled", "57014");
    try (PartitionedResult result =
        start(List.of(hanging, failing), PartitionedResult.Order.INTERLEAVED)) {
      SQLException error =
          assertTimeoutPreemptively(LIMIT, () -> assertThrows(SQLException.class, result::next));
      assertEquals("57014", error.getSQLState());
      awaitAborted("the hanging download stops", hanging);
    }
  }

  /**
   * Partition 1 ends with a cancellation nobody asked for (the server stopped the statement because
   * partition 0's connection broke); partition 0 then reports the broken connection.
   */
  private SQLException consequentialCancel(boolean applicationCancelled) throws Exception {
    FakePartition first = new FakePartition(List.of(new long[] {1}));
    first.failure = new SQLException("the connection broke; set retainResults", "08006");
    first.gate = new CountDownLatch(1);
    first.gateBefore = 1;
    FakePartition second = new FakePartition(List.of());
    second.failure = new SQLException("the statement was cancelled (client)", "57014");
    Thread opener =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                return;
              }
              first.gate.countDown();
            });
    opener.start();
    try (PartitionedResult result =
        new PartitionedResult(
            "q_test",
            allocator,
            first,
            List.of(() -> second),
            PartitionedResult.Order.INTERLEAVED,
            () -> applicationCancelled)) {
      return assertTimeoutPreemptively(
          LIMIT, () -> assertThrows(SQLException.class, () -> readAll(result)));
    } finally {
      opener.join();
    }
  }

  @Test
  void aCancellationCausedByAnotherPartitionReportsThatPartitionsError() throws Exception {
    assertEquals("08006", consequentialCancel(false).getSQLState());
  }

  @Test
  void aCancellationTheApplicationAskedForIsReportedAsSuch() throws Exception {
    assertEquals("57014", consequentialCancel(true).getSQLState());
  }

  @Test
  void anOpenThatFailsFailsTheResult() throws Exception {
    FakePartition first = new FakePartition(List.of(new long[] {1}));
    List<PartitionedResult.Opener> others =
        List.of(
            () -> {
              throw new SQLException("statement failed", "42000");
            });
    try (PartitionedResult result =
        new PartitionedResult(
            "q_test", allocator, first, others, PartitionedResult.Order.ANY, () -> false)) {
      SQLException error =
          assertTimeoutPreemptively(
              LIMIT, () -> assertThrows(SQLException.class, () -> readAll(result)));
      assertEquals("42000", error.getSQLState());
    }
  }

  @Test
  void tooManyBatchesInALaterPartitionIsAProtocolError() throws Exception {
    // Partition 0 has one batch, partition 1 two: not round robin.
    List<FakePartition> parts =
        List.of(
            new FakePartition(List.of(new long[] {0})),
            new FakePartition(List.of(new long[] {1}, new long[] {2})));
    try (PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED)) {
      SQLException error =
          assertTimeoutPreemptively(
              LIMIT, () -> assertThrows(SQLException.class, () -> readAll(result)));
      assertEquals("08P01", error.getSQLState(), error.getMessage());
    }
  }

  @Test
  void closingEarlyStopsEveryPartition() throws Exception {
    List<FakePartition> parts = interleaved(4, 400, 10);
    for (FakePartition part : parts) {
      part.gate = new CountDownLatch(1);
      part.gateBefore = 50;
    }
    PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED);
    assertTrue(result.next());
    assertTimeoutPreemptively(LIMIT, result::close);
    for (FakePartition part : parts) {
      assertTrue(part.aborted, "every download stops");
      assertTrue(part.closed, "every stream is closed");
    }
    assertEquals("24000", assertThrows(SQLException.class, result::next).getSQLState());
  }

  @Test
  void closingWhileAPartitionOpensAbortsItsStream() throws Exception {
    FakePartition first = new FakePartition(List.of(new long[] {1}));
    FakePartition late = new FakePartition(List.of(new long[] {2}));
    CountDownLatch opening = new CountDownLatch(1);
    PartitionedResult.Opener opener =
        () -> {
          opening.countDown();
          try {
            new CountDownLatch(1).await();
            throw new SQLException("The opener must be interrupted by close");
          } catch (InterruptedException expected) {
            // An HTTP open can finish just as close interrupts its partition thread.
            return late;
          }
        };
    try (PartitionedResult result =
        new PartitionedResult(
            "q_test",
            allocator,
            first,
            List.of(opener),
            PartitionedResult.Order.INTERLEAVED,
            () -> false)) {
      assertTrue(opening.await(20, TimeUnit.SECONDS));
      assertTimeoutPreemptively(LIMIT, result::close);
      assertTrue(late.aborted, "a stream opened after cancellation is aborted too");
      assertTrue(late.closed, "the late stream is closed before close returns");
    }
  }

  @Test
  void abortWakesAWaitingReader() throws Exception {
    FakePartition hanging = new FakePartition(List.of(new long[] {0}));
    hanging.gate = new CountDownLatch(1);
    hanging.gateBefore = 0;
    FakePartition other = new FakePartition(List.of());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (PartitionedResult result =
        start(List.of(hanging, other), PartitionedResult.Order.INTERLEAVED)) {
      Future<SQLException> reading =
          executor.submit(() -> assertThrows(SQLException.class, result::next));
      Thread.sleep(100);
      result.abort();
      assertEquals("24000", reading.get(20, TimeUnit.SECONDS).getSQLState());
      awaitAborted("the hanging download stops", hanging);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void bufferingReadsTheRestIntoMemory() throws Exception {
    List<FakePartition> parts = interleaved(3, 20, 4);
    try (PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED)) {
      assertTrue(result.next());
      long first = ((BigIntVector) result.root().getVector(0)).get(0);
      assertTimeoutPreemptively(LIMIT, () -> result.buffer(Long.MAX_VALUE));
      for (FakePartition part : parts) {
        assertTrue(part.closed, "every partition was read to its end");
      }
      assertEquals(0, first);
      assertEquals(
          0, ((BigIntVector) result.root().getVector(0)).get(0), "the current batch stays");
      List<Long> rest = readAll(result);
      assertEquals(range(80).subList(4, 80), rest);
    }
  }

  @Test
  void bufferingPastTheLimitDropsTheRest() throws Exception {
    List<FakePartition> parts = interleaved(2, 200, 100);
    try (PartitionedResult result = start(parts, PartitionedResult.Order.INTERLEAVED)) {
      assertTrue(result.next());
      assertTimeoutPreemptively(LIMIT, () -> result.buffer(4000));
      SQLException error =
          assertThrows(
              SQLException.class,
              () -> {
                while (result.next()) {
                  // The buffered part reads; then the drop is reported.
                }
              });
      assertTrue(error.getMessage().contains("resultBufferSize"), error.getMessage());
    }
  }
}
