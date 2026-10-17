package com.gizmodata.warpsql.jdbc.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A result split into partitions (spec §6.10), read over one connection per partition, all at once,
 * and presented as one stream of batches.
 *
 * <p>Each partition has a thread that reads it with its own {@link ResultReader} (which detects an
 * incomplete stream, reports the statement's error and resumes a retained partition from its next
 * batch) and queues up to {@link #LANE_DEPTH} batches. The reading thread takes them back:
 *
 * <ul>
 *   <li><b>interleaved</b> results in partition order, round robin (batch {@code j} of the result
 *       is batch {@code j div k} of partition {@code j mod k}), so an {@code ORDER BY} survives;
 *   <li><b>any</b>-order results from whichever partition has a batch ready.
 * </ul>
 *
 * <p>The result is complete only once every partition reached its end-of-stream marker. A failure
 * in any partition fails the whole result and stops every download; so do {@link #abort()} and
 * {@link #close()}.
 *
 * <p>Internal API.
 */
public final class PartitionedResult implements ResultStream {
  /** Batches each partition may hold before its thread waits for the reader. */
  static final int LANE_DEPTH = 3;

  /** How long {@link #close()} waits for the partitions' threads to let go of their streams. */
  private static final long CLOSE_WAIT_NANOS = TimeUnit.SECONDS.toNanos(30);

  /** How the result's rows are spread over its partitions. */
  enum Order {
    /** Batches dealt round robin in result order. */
    INTERLEAVED,
    /** Rows in no particular order. */
    ANY
  }

  /** Opens one partition's stream, on that partition's thread. */
  @FunctionalInterface
  interface Opener {
    ResultStream open() throws SQLException;
  }

  private final String statementId;
  private final BufferAllocator allocator;
  private final Schema schema;
  private final boolean hasDictionaries;
  private final Dictionaries initialDictionaries;
  private final Order order;
  private final Lane[] lanes;
  private final Thread[] threads;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private SQLException failure;
  private volatile boolean closed;

  // The reading thread's state, guarded by this.
  private Batch current;
  private int cursor;
  private volatile boolean finished;
  private boolean buffered;
  private final Deque<Batch> held = new ArrayDeque<>();
  private SQLException heldError;
  private boolean freed;

  /** One partition: its stream and the batches read from it that wait for the reader. */
  private static final class Lane {
    final int index;
    final ArrayDeque<Batch> batches = new ArrayDeque<>();
    volatile ResultStream stream;
    boolean ended;

    Lane(int index) {
      this.index = index;
    }
  }

  /** A batch taken out of its partition's stream, with the dictionaries it was read with. */
  private static final class Batch implements AutoCloseable {
    final VectorSchemaRoot root;
    final Dictionaries dictionaries;

    Batch(VectorSchemaRoot root, Dictionaries dictionaries) {
      this.root = root;
      this.dictionaries = dictionaries;
    }

    @Override
    public void close() {
      root.close();
      if (dictionaries != null) {
        dictionaries.close();
      }
    }
  }

  /**
   * Starts reading every partition.
   *
   * @param statementId the statement
   * @param allocator where queued batches are allocated
   * @param first partition 0, already open: its schema is the result's
   * @param others opens partitions 1 to {@code k - 1}, each on its own thread
   * @param order how the rows are spread over the partitions
   */
  PartitionedResult(
      String statementId,
      BufferAllocator allocator,
      ResultStream first,
      List<Opener> others,
      Order order) {
    this.statementId = statementId;
    this.allocator = allocator;
    this.schema = first.schema();
    this.order = order;
    boolean dictionaryEncoded = false;
    for (Field field : schema.getFields()) {
      dictionaryEncoded |= field.getDictionary() != null;
    }
    this.hasDictionaries = dictionaryEncoded;
    this.initialDictionaries = Dictionaries.share(first.dictionaries(), allocator);
    int count = others.size() + 1;
    this.lanes = new Lane[count];
    this.threads = new Thread[count];
    for (int i = 0; i < count; i++) {
      lanes[i] = new Lane(i);
    }
    lanes[0].stream = first;
    for (int i = 0; i < count; i++) {
      Lane lane = lanes[i];
      Opener opener = i == 0 ? () -> first : others.get(i - 1);
      Thread thread =
          new Thread(
              () -> produce(lane, opener),
              "warp-sql-jdbc " + statementId + " partition " + i + "/" + count);
      thread.setDaemon(true);
      threads[i] = thread;
    }
    for (Thread thread : threads) {
      thread.start();
    }
  }

  // ---- Opening ----

  /**
   * Reads a result streaming in a submission's response: as is when it is one partition, and with
   * its other partitions fetched alongside it when the response says the result is split (spec
   * §6.10).
   *
   * @param client the client
   * @param allocator where batches are allocated
   * @param sessionId the session
   * @param response the streaming response, partition 0 of a split result
   * @param anyOrderRequested whether the submission asked for {@code Warp-Result-Order: any}
   * @param cancelRequested whether the application cancelled the statement
   * @return the result, positioned before its first batch
   * @throws SQLException if the result fails before its schema, with the statement's error, or the
   *     server's split headers are not valid
   */
  public static ResultStream streamed(
      WarpClient client,
      BufferAllocator allocator,
      String sessionId,
      WarpClient.ArrowResponse response,
      boolean anyOrderRequested,
      BooleanSupplier cancelRequested)
      throws SQLException {
    int count;
    Order order;
    try {
      count = partitionCount(response.partitions());
      order = count > 1 ? order(response.order(), anyOrderRequested) : null;
    } catch (SQLException e) {
      closeQuietly(response.body());
      throw e;
    }
    if (count == 1) {
      return ResultReader.streamed(client, allocator, sessionId, response, cancelRequested);
    }
    String statementId = response.statementId();
    if (statementId == null) {
      closeQuietly(response.body());
      throw new SQLNonTransientConnectionException(
          "The server split the result into partitions without naming its statement"
              + " (Warp-Statement-Id)",
          "08P01");
    }
    ResultReader first =
        ResultReader.streamed(client, allocator, sessionId, response, cancelRequested, true);
    List<Opener> others = new ArrayList<>();
    for (int i = 1; i < count; i++) {
      int position = i;
      others.add(
          () ->
              ResultReader.partition(
                  client, allocator, sessionId, statementId, null, position, cancelRequested));
    }
    return new PartitionedResult(statementId, allocator, first, others, order);
  }

  /**
   * Fetches the result of a succeeded statement, every partition at once when it is split (spec
   * §6.10).
   *
   * @param client the client
   * @param allocator where batches are allocated
   * @param sessionId the session
   * @param document the succeeded statement's document
   * @param anyOrderRequested whether the submission asked for {@code Warp-Result-Order: any}
   * @param cancelRequested whether the application cancelled the statement
   * @return the result, positioned before its first batch
   * @throws SQLException if the result cannot be fetched
   */
  public static ResultStream fetched(
      WarpClient client,
      BufferAllocator allocator,
      String sessionId,
      StatementDocument document,
      boolean anyOrderRequested,
      BooleanSupplier cancelRequested)
      throws SQLException {
    List<StatementDocument.Partition> partitions = document.partitions();
    if (partitions.size() <= 1 || document.partitionOrder() == null) {
      // One partition, or partitions on other servers (capability partitioned_results), read one
      // after another.
      return ResultReader.fetched(client, allocator, sessionId, document, cancelRequested);
    }
    Order order = order(document.partitionOrder(), anyOrderRequested);
    String statementId = document.statementId();
    ResultReader first =
        ResultReader.partition(
            client, allocator, sessionId, statementId, partitions, 0, cancelRequested);
    List<Opener> others = new ArrayList<>();
    for (int i = 1; i < partitions.size(); i++) {
      int position = i;
      others.add(
          () ->
              ResultReader.partition(
                  client,
                  allocator,
                  sessionId,
                  statementId,
                  partitions,
                  position,
                  cancelRequested));
    }
    return new PartitionedResult(statementId, allocator, first, others, order);
  }

  private static int partitionCount(String header) throws SQLException {
    if (header == null) {
      return 1;
    }
    try {
      int count = Integer.parseInt(header.trim());
      if (count >= 1 && count <= WarpClient.MAX_RESULT_PARTITIONS) {
        return count;
      }
    } catch (NumberFormatException e) {
      // Reported below.
    }
    throw new SQLNonTransientConnectionException(
        "The server's Warp-Result-Partitions header is not a partition count: " + header, "08P01");
  }

  private static Order order(String value, boolean anyOrderRequested) throws SQLException {
    String text = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    if ("interleaved".equals(text)) {
      // A client that does not need the order may still take batches as they come.
      return anyOrderRequested ? Order.ANY : Order.INTERLEAVED;
    }
    if ("any".equals(text)) {
      if (!anyOrderRequested) {
        throw new SQLNonTransientConnectionException(
            "The server split the result without keeping its order (partition order 'any'),"
                + " though the driver asked for it to be preserved",
            "08P01");
      }
      return Order.ANY;
    }
    throw new SQLNonTransientConnectionException(
        "The server split the result without saying how (partition order '" + value + "')",
        "08P01");
  }

  // ---- The partitions' threads ----

  private void produce(Lane lane, Opener opener) {
    ResultStream stream = null;
    try {
      stream = opener.open();
      lane.stream = stream;
      if (closed) {
        return;
      }
      if (!schema.equals(stream.schema())) {
        throw new SQLNonTransientConnectionException(
            "Partition " + lane.index + "'s schema differs from the result's", "08P01");
      }
      while (stream.next()) {
        Batch batch = copyBatch(stream);
        if (!offer(lane, batch)) {
          batch.close();
          return;
        }
      }
      lock.lock();
      try {
        lane.ended = true;
        changed.signalAll();
      } finally {
        lock.unlock();
      }
    } catch (SQLException e) {
      fail(e);
    } catch (RuntimeException e) {
      fail(new SQLException("Cannot read partition " + lane.index + ": " + e, "HY000", e));
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (RuntimeException e) {
          // Freeing buffers cannot fail in a way the reader can act on.
        }
      }
    }
  }

  /** Takes the stream's current batch, and its dictionaries, out of the stream. */
  private Batch copyBatch(ResultStream stream) throws SQLException {
    VectorSchemaRoot copy = share(stream.root(), allocator);
    if (!hasDictionaries) {
      return new Batch(copy, null);
    }
    try {
      return new Batch(copy, Dictionaries.share(stream.dictionaries(), allocator));
    } catch (RuntimeException e) {
      copy.close();
      throw e;
    }
  }

  /** Queues a batch, waiting while the partition's queue is full; {@code false} once stopped. */
  private boolean offer(Lane lane, Batch batch) {
    lock.lock();
    try {
      while (!closed && failure == null && lane.batches.size() >= LANE_DEPTH) {
        changed.await();
      }
      if (closed || failure != null) {
        return false;
      }
      lane.batches.add(batch);
      changed.signalAll();
      return true;
    } catch (InterruptedException e) {
      return false;
    } finally {
      lock.unlock();
    }
  }

  /** Records the result's failure, the first one only, and stops every partition. */
  private void fail(SQLException error) {
    boolean first = false;
    lock.lock();
    try {
      if (failure == null && !closed) {
        failure = error;
        first = true;
        changed.signalAll();
      }
    } finally {
      lock.unlock();
    }
    if (first) {
      stopStreams();
    }
  }

  private void stopStreams() {
    for (Lane lane : lanes) {
      ResultStream stream = lane.stream;
      if (stream != null) {
        stream.abort();
      }
    }
  }

  // ---- The reading thread ----

  /** Waits for the result's next batch in its order; {@code null} once every partition ended. */
  private Batch nextBatch() throws SQLException {
    lock.lock();
    try {
      for (; ; ) {
        if (closed) {
          throw new SQLException("The result was closed", "24000");
        }
        if (failure != null) {
          throw failure;
        }
        Batch batch = order == Order.INTERLEAVED ? nextInterleaved() : nextAny();
        if (batch != null) {
          changed.signalAll();
          return batch;
        }
        if (allEnded()) {
          return null;
        }
        changed.await();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while waiting for the result", "HY008", e);
    } finally {
      lock.unlock();
    }
  }

  /** The cursor's partition's next batch, or {@code null} if it has none yet or has ended. */
  private Batch nextInterleaved() throws SQLException {
    Lane lane = lanes[cursor];
    Batch batch = lane.batches.poll();
    if (batch != null) {
      cursor = (cursor + 1) % lanes.length;
      return batch;
    }
    if (lane.ended) {
      // That partition has no next batch, so the result is over: every partition must end here.
      for (Lane other : lanes) {
        if (!other.batches.isEmpty()) {
          SQLException error =
              new SQLNonTransientConnectionException(
                  "Partition "
                      + other.index
                      + " has more batches than an interleaved result allows (partition "
                      + lane.index
                      + " ended first)",
                  "08P01");
          failLocked(error);
          throw error;
        }
      }
    }
    return null;
  }

  private Batch nextAny() {
    for (int n = 0; n < lanes.length; n++) {
      Lane lane = lanes[(cursor + n) % lanes.length];
      Batch batch = lane.batches.poll();
      if (batch != null) {
        cursor = (lane.index + 1) % lanes.length;
        return batch;
      }
    }
    return null;
  }

  private boolean allEnded() {
    for (Lane lane : lanes) {
      if (!lane.ended || !lane.batches.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@link #fail} with the lock held. Stopping a stream only closes its response body, which never
   * waits for this lock.
   */
  private void failLocked(SQLException error) {
    if (failure == null) {
      failure = error;
      changed.signalAll();
      stopStreams();
    }
  }

  @Override
  public String statementId() {
    return statementId;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public synchronized VectorSchemaRoot root() throws SQLException {
    if (current == null) {
      throw new SQLException("No batch has been read", "24000");
    }
    return current.root;
  }

  @Override
  public synchronized DictionaryProvider dictionaries() {
    return current != null && current.dictionaries != null
        ? current.dictionaries
        : initialDictionaries;
  }

  @Override
  public int generation() {
    // The current batch is its own root and never moves.
    return 0;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public synchronized boolean next() throws SQLException {
    if (closed) {
      throw new SQLException("The result was closed", "24000");
    }
    closeCurrent();
    if (finished) {
      return false;
    }
    if (buffered) {
      current = held.poll();
      if (current != null) {
        return true;
      }
      finished = true;
      if (heldError != null) {
        SQLException error = heldError;
        heldError = null;
        throw error;
      }
      return false;
    }
    current = nextBatch();
    if (current == null) {
      finished = true;
      return false;
    }
    return true;
  }

  @Override
  public synchronized void buffer(long limitBytes) {
    if (buffered || finished || closed) {
      return;
    }
    buffered = true;
    long bytes = current == null ? 0 : size(current.root);
    try {
      for (Batch batch = nextBatch(); batch != null; batch = nextBatch()) {
        held.add(batch);
        bytes += size(batch.root);
        if (bytes > limitBytes) {
          heldError =
              new SQLException(
                  "The rest of this result was dropped: the connection ran another statement while"
                      + " it was open, and it is larger than resultBufferSize ("
                      + limitBytes
                      + " bytes). Read results before running other statements, or set"
                      + " retainResults=true to keep several results open.",
                  "HY000");
          lock.lock();
          try {
            failLocked(heldError);
          } finally {
            lock.unlock();
          }
          return;
        }
      }
    } catch (SQLException e) {
      heldError = e;
    }
  }

  @Override
  public void abort() {
    lock.lock();
    try {
      closed = true;
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    stopStreams();
    for (Thread thread : threads) {
      if (thread != Thread.currentThread()) {
        thread.interrupt();
      }
    }
  }

  @Override
  public void close() {
    abort();
    long deadline = System.nanoTime() + CLOSE_WAIT_NANOS;
    for (Thread thread : threads) {
      long left = deadline - System.nanoTime();
      if (left <= 0) {
        break;
      }
      try {
        thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    lock.lock();
    try {
      for (Lane lane : lanes) {
        for (Batch batch = lane.batches.poll(); batch != null; batch = lane.batches.poll()) {
          batch.close();
        }
      }
    } finally {
      lock.unlock();
    }
    synchronized (this) {
      if (freed) {
        return;
      }
      freed = true;
      closeCurrent();
      for (Batch batch = held.poll(); batch != null; batch = held.poll()) {
        batch.close();
      }
      initialDictionaries.close();
    }
  }

  private void closeCurrent() {
    if (current != null) {
      current.close();
      current = null;
    }
  }

  // ---- Arrow plumbing ----

  private static long size(VectorSchemaRoot root) {
    long bytes = 0;
    for (FieldVector vector : root.getFieldVectors()) {
      bytes += vector.getBufferSize();
    }
    return bytes;
  }

  /** A zero-copy copy: the new root shares the batch's buffers, which outlive the stream's. */
  private static VectorSchemaRoot share(VectorSchemaRoot root, BufferAllocator allocator) {
    VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
    try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
      new VectorLoader(copy).load(batch);
    } catch (RuntimeException e) {
      copy.close();
      throw e;
    }
    return copy;
  }

  /** A stream's dictionaries as they were when a batch was read, sharing their buffers. */
  private static final class Dictionaries implements DictionaryProvider, AutoCloseable {
    private final Map<Long, Dictionary> byId = new HashMap<>();
    private final List<VectorSchemaRoot> roots = new ArrayList<>();

    static Dictionaries share(DictionaryProvider source, BufferAllocator allocator) {
      Dictionaries copy = new Dictionaries();
      if (source == null) {
        return copy;
      }
      try {
        for (long id : source.getDictionaryIds()) {
          Dictionary dictionary = source.lookup(id);
          FieldVector vector = dictionary.getVector();
          // A root over the stream's own vector, only to unload it: closing it would free the
          // stream's dictionary.
          VectorSchemaRoot view =
              new VectorSchemaRoot(
                  List.of(vector.getField()), List.of(vector), vector.getValueCount());
          VectorSchemaRoot shared = PartitionedResult.share(view, allocator);
          copy.roots.add(shared);
          copy.byId.put(id, new Dictionary(shared.getVector(0), dictionary.getEncoding()));
        }
      } catch (RuntimeException e) {
        copy.close();
        throw e;
      }
      return copy;
    }

    @Override
    public Dictionary lookup(long id) {
      return byId.get(id);
    }

    @Override
    public Set<Long> getDictionaryIds() {
      return byId.keySet();
    }

    @Override
    public void close() {
      for (VectorSchemaRoot root : roots) {
        root.close();
      }
      roots.clear();
      byId.clear();
    }
  }

  private static void closeQuietly(InputStream body) {
    try {
      body.close();
    } catch (IOException e) {
      // Closing only releases the connection.
    }
  }
}
