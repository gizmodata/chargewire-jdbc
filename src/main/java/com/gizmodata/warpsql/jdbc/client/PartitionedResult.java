package com.gizmodata.warpsql.jdbc.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
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

  /** How long a failure that is only a consequence waits for the partition that caused it. */
  private static final long CAUSE_WAIT_NANOS = 2_000_000_000L;

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
  private final BooleanSupplier cancelRequested;
  private int running; // partition threads still running, guarded by lock

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
      Order order,
      BooleanSupplier cancelRequested) {
    this.statementId = statementId;
    this.cancelRequested = cancelRequested;
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
    running = count;
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
      throw SqlErrors.protocolError(
          "The server split the result into partitions without naming its statement"
              + " (Warp-Statement-Id)");
    }
    List<StatementDocument.Partition> partitions;
    try {
      partitions = locations(client, sessionId, statementId, count);
    } catch (SQLException | RuntimeException e) {
      closeQuietly(response.body());
      throw e;
    }
    List<Opener> openers = new ArrayList<>();
    for (int i = 1; i < count; i++) {
      int position = i;
      openers.add(
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
    // The other partitions start before partition 0's schema is read: all at once (spec §6.10).
    List<Opener> others = openEarly(statementId, openers);
    ResultReader first;
    try {
      first = ResultReader.streamed(client, allocator, sessionId, response, cancelRequested);
    } catch (SQLException | RuntimeException e) {
      closeLater(others);
      throw e;
    }
    return new PartitionedResult(statementId, allocator, first, others, order, cancelRequested);
  }

  /**
   * Fetches the result of a succeeded statement, every partition at once when it is split (spec
   * §6.10), wherever each partition is held (§6.2).
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
    if (partitions.size() <= 1) {
      return ResultReader.fetched(client, allocator, sessionId, document, cancelRequested);
    }
    if (document.partitionOrder() == null) {
      throw SqlErrors.protocolError(
          "The server split the result into "
              + partitions.size()
              + " partitions without saying how (no partition_order)");
    }
    Order order = order(document.partitionOrder(), anyOrderRequested);
    String statementId = document.statementId();
    List<Opener> openers = new ArrayList<>();
    for (int i = 1; i < partitions.size(); i++) {
      int position = i;
      openers.add(
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
    // Every partition is requested at once, partition 0 included (spec §6.10): a server may answer
    // none of them until it can serve them all.
    List<Opener> others = openEarly(statementId, openers);
    ResultReader first;
    try {
      first =
          ResultReader.partition(
              client, allocator, sessionId, statementId, partitions, 0, cancelRequested);
    } catch (SQLException | RuntimeException e) {
      closeLater(others);
      throw e;
    }
    return new PartitionedResult(statementId, allocator, first, others, order, cancelRequested);
  }

  /**
   * Returns where a streamed split result's partitions are. Without remote locations every
   * partition is on this server; with them, the statement document says (spec §6.2).
   *
   * @return the partitions, or {@code null} when all are on this server
   */
  private static List<StatementDocument.Partition> locations(
      WarpClient client, String sessionId, String statementId, int count) throws SQLException {
    if (!client.followsLocations()) {
      return null;
    }
    StatementDocument document = client.status(sessionId, statementId, 0);
    if (document.state() == StatementDocument.State.FAILED
        || document.state() == StatementDocument.State.CANCELLED) {
      throw document.error() != null
          ? SqlErrors.toSqlException(document.error())
          : new SQLException("The statement " + document.state() + " mid-result", "HY000");
    }
    List<StatementDocument.Partition> partitions = document.partitions();
    if (partitions.size() != count) {
      throw SqlErrors.protocolError(
          "The response says the result has "
              + count
              + " partitions, its statement document lists "
              + partitions.size());
    }
    for (int i = 0; i < count; i++) {
      if (partitions.get(i).index() != i) {
        throw SqlErrors.protocolError("The statement document lists its partitions out of order");
      }
    }
    return partitions;
  }

  /** Starts opening each partition on a thread of its own; each opener then waits for its open. */
  private static List<Opener> openEarly(String statementId, List<Opener> openers) {
    List<Opener> started = new ArrayList<>();
    for (int i = 0; i < openers.size(); i++) {
      FutureTask<ResultStream> task = new FutureTask<>(openers.get(i)::open);
      Thread thread =
          new Thread(task, "warp-sql-jdbc " + statementId + " opening partition " + (i + 1));
      thread.setDaemon(true);
      thread.start();
      started.add(
          () -> {
            try {
              return task.get();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              closeWhenOpen(task);
              throw new SQLException("Interrupted while opening a partition", "HY008", e);
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if (cause instanceof SQLException) {
                throw (SQLException) cause;
              }
              if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
              }
              throw new SQLException("Cannot open a partition: " + cause, "HY000", cause);
            }
          });
    }
    return started;
  }

  /** Closes, once they are open, partitions whose result failed before it could read them. */
  private static void closeLater(List<Opener> openers) {
    Thread closer =
        new Thread(
            () -> {
              for (Opener opener : openers) {
                try {
                  opener.open().close();
                } catch (SQLException | RuntimeException e) {
                  // It failed to open; nothing to close.
                }
              }
            },
            "warp-sql-jdbc closing partitions");
    closer.setDaemon(true);
    closer.start();
  }

  private static void closeWhenOpen(FutureTask<ResultStream> task) {
    closeLater(
        List.of(
            () -> {
              try {
                return task.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new SQLException("not opened", "HY000", e);
              }
            }));
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
    throw SqlErrors.protocolError(
        "The server's Warp-Result-Partitions header is not a partition count: " + header);
  }

  private static Order order(String value, boolean anyOrderRequested) throws SQLException {
    if (value == null) {
      // The order is never guessed (spec §6.10): a split result must say how it is split.
      throw SqlErrors.protocolError(
          "The server split the result into partitions without a Warp-Result-Order header");
    }
    String text = value.trim().toLowerCase(Locale.ROOT);
    if ("interleaved".equals(text)) {
      // A client that does not need the order may still take batches as they come.
      return anyOrderRequested ? Order.ANY : Order.INTERLEAVED;
    }
    if ("any".equals(text)) {
      if (!anyOrderRequested) {
        throw SqlErrors.protocolError(
            "The server split the result without keeping its order (partition order 'any'),"
                + " though the driver asked for it to be preserved");
      }
      return Order.ANY;
    }
    // A new partition order reaches only a client that asked for it (spec §12).
    throw SqlErrors.protocolError(
        "The server split the result in a way the driver did not ask for (partition order '"
            + value
            + "')");
  }

  // ---- The partitions' threads ----

  private void produce(Lane lane, Opener opener) {
    ResultStream stream = null;
    try {
      stream = opener.open();
      lane.stream = stream;
      if (closed) {
        // close may have stopped the other lanes before this opener published its stream.
        stream.abort();
        return;
      }
      if (!schema.equals(stream.schema())) {
        throw SqlErrors.protocolError(
            "Partition " + lane.index + "'s schema differs from the result's");
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
      lock.lock();
      try {
        running--;
        changed.signalAll();
      } finally {
        lock.unlock();
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
      if (failure != null && !closed && consequentialCancel(failure)) {
        // Keep reading, without keeping the rows: this partition may be the one whose
        // failure caused the cancellation, and its stream says how when it ends.
        batch.close();
        return true;
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

  /**
   * Records the result's failure, the first one only, and stops every partition. One exception: a
   * cancellation the application did not ask for is usually the server reacting to another
   * partition's failure (its connection broke, so the server stopped the statement), so that
   * partition's own error replaces it when it comes.
   */
  private void fail(SQLException error) {
    boolean first = false;
    lock.lock();
    try {
      if (failure == null && !closed) {
        failure = error;
        // A consequential cancellation stops nothing yet: the server has stopped the statement,
        // so the other partitions end on their own, and the one that caused it can say why.
        first = !consequentialCancel(error);
        changed.signalAll();
      } else if (failure != null && consequentialCancel(failure) && !isCancel(error)) {
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

  private static boolean isCancel(SQLException error) {
    return "57014".equals(error.getSQLState());
  }

  /** A cancellation the application did not ask for. */
  private boolean consequentialCancel(SQLException error) {
    return isCancel(error) && !cancelRequested.getAsBoolean();
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
      long causeDeadline = 0;
      for (; ; ) {
        if (closed) {
          throw new SQLException("The result was closed", "24000");
        }
        if (failure != null) {
          // A cancellation nobody asked for: give the partition that caused it a moment to
          // report its own error, which explains the failure better.
          if (consequentialCancel(failure) && running > 0) {
            long now = System.nanoTime();
            if (causeDeadline == 0) {
              causeDeadline = now + CAUSE_WAIT_NANOS;
            }
            if (now < causeDeadline) {
              changed.awaitNanos(causeDeadline - now);
              continue;
            }
            stopStreams();
          }
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
              SqlErrors.protocolError(
                  "Partition "
                      + other.index
                      + " has more batches than an interleaved result allows (partition "
                      + lane.index
                      + " ended first)");
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
      return;
    }
    // Every partition has ended: wait for the readers to let go of their connections, so
    // the statement that made the session buffer this result finds them released.
    awaitReaders();
  }

  private void awaitReaders() {
    long deadline = System.nanoTime() + CLOSE_WAIT_NANOS;
    for (Thread thread : threads) {
      long left = deadline - System.nanoTime();
      if (left <= 0 || thread == Thread.currentThread()) {
        continue;
      }
      try {
        thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
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
