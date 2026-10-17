package com.gizmodata.warpsql.jdbc.client;

import com.gizmodata.warpsql.jdbc.arrow.ArrowIpc;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Streams a statement's result batch by batch, holding one batch at a time.
 *
 * <p>A stream that breaks off, whether the connection dropped or the statement failed mid-stream
 * (spec §6.6), is explained by the statement document: a failed or cancelled statement raises its
 * own error, and a succeeded statement whose result the server retained is fetched again from the
 * next unread batch (spec §6.5). A result with several partitions is read partition by partition.
 *
 * <p>{@link #buffer(long)} reads the rest of the stream into memory at once. The connection does
 * that before it sends anything else that needs the session, because a server may hold a session's
 * other work until an unretained result has been read. Methods are synchronized, so the buffering
 * can happen on another thread than the reading.
 *
 * <p>Internal API.
 */
public final class ResultReader implements AutoCloseable {
  private static final int MAX_RESUMES = 3;

  private final WarpClient client;
  private final BufferAllocator allocator;
  private final String sessionId;
  private final String statementId;
  private final BooleanSupplier cancelRequested;
  private List<StatementDocument.Partition> partitions;
  private int partition;
  private long batchesRead;
  private int resumesWithoutProgress;

  private InputStream body;
  private ArrowIpc.HeapReadChannel channel;
  private ArrowStreamReader reader;
  private Schema schema;
  private boolean finished;
  private volatile boolean closed;

  private boolean buffered;
  private final Deque<VectorSchemaRoot> pending = new ArrayDeque<>();
  private VectorSchemaRoot current;
  private Dictionaries bufferedDictionaries;
  private SQLException pendingError;
  private int generation;

  private ResultReader(
      WarpClient client,
      BufferAllocator allocator,
      String sessionId,
      String statementId,
      BooleanSupplier cancelRequested) {
    this.client = client;
    this.allocator = allocator;
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.cancelRequested = cancelRequested;
  }

  /**
   * Reads a result streaming in a submission's response (the one-round-trip path).
   *
   * @param client the client
   * @param allocator where batches are allocated
   * @param sessionId the session
   * @param response the streaming response
   * @param cancelRequested whether the application cancelled the statement, so that a stream the
   *     server ends because of it reports the cancellation rather than a lost connection
   * @return a reader positioned before the first batch
   * @throws SQLException if the stream fails before its schema, with the statement's error
   */
  public static ResultReader streamed(
      WarpClient client,
      BufferAllocator allocator,
      String sessionId,
      WarpClient.ArrowResponse response,
      BooleanSupplier cancelRequested)
      throws SQLException {
    ResultReader result =
        new ResultReader(client, allocator, sessionId, response.statementId(), cancelRequested);
    result.start(response.body());
    return result;
  }

  /**
   * Fetches the result of a succeeded statement (the status-document path).
   *
   * @param client the client
   * @param allocator where batches are allocated
   * @param sessionId the session
   * @param document the succeeded statement's document
   * @param cancelRequested whether the application cancelled the statement
   * @return a reader positioned before the first batch
   * @throws SQLException if the result cannot be fetched
   */
  public static ResultReader fetched(
      WarpClient client,
      BufferAllocator allocator,
      String sessionId,
      StatementDocument document,
      BooleanSupplier cancelRequested)
      throws SQLException {
    ResultReader result =
        new ResultReader(client, allocator, sessionId, document.statementId(), cancelRequested);
    result.partitions = document.partitions().isEmpty() ? null : document.partitions();
    result.start(result.open(0));
    return result;
  }

  /**
   * Returns the statement the result belongs to.
   *
   * @return the statement id
   */
  public String statementId() {
    return statementId;
  }

  /**
   * Returns the result's schema, with dictionary-encoded columns as their index type.
   *
   * @return the schema of {@link #root()}
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Returns the batch most recently loaded. The root object can change after {@link #next()} or
   * {@link #buffer(long)}; {@link #generation()} tells when.
   *
   * @return the current batch
   * @throws SQLException if the stream cannot be read
   */
  public synchronized VectorSchemaRoot root() throws SQLException {
    if (buffered) {
      return current;
    }
    try {
      return reader.getVectorSchemaRoot();
    } catch (IOException e) {
      throw new SQLNonTransientConnectionException(
          "Cannot read the result: " + e.getMessage(), "08P01", e);
    }
  }

  /**
   * Returns the dictionaries for dictionary-encoded columns.
   *
   * @return the dictionaries
   */
  public synchronized DictionaryProvider dictionaries() {
    return buffered ? bufferedDictionaries : reader;
  }

  /**
   * Returns a number that changes whenever the object {@link #root()} returns for the current batch
   * changes without {@link #next()}, so readers know to look it up again.
   *
   * @return the generation
   */
  public synchronized int generation() {
    return generation;
  }

  /**
   * Returns whether every batch was read.
   *
   * @return {@code true} once {@link #next()} returned {@code false}
   */
  public synchronized boolean isFinished() {
    return finished;
  }

  /**
   * Loads the next batch into {@link #root()}.
   *
   * @return {@code false} at the end of the result
   * @throws SQLException if the result cannot be read, with the statement's error when it failed
   */
  public synchronized boolean next() throws SQLException {
    if (closed) {
      throw new SQLException("The result was closed", "24000");
    }
    if (!buffered) {
      return nextFromStream();
    }
    if (current != null) {
      current.close();
      current = null;
    }
    current = pending.poll();
    if (current != null) {
      return true;
    }
    finished = true;
    if (pendingError != null) {
      SQLException error = pendingError;
      pendingError = null;
      throw error;
    }
    return false;
  }

  /**
   * Reads the rest of the stream into memory, keeping the current batch, so the server can go on
   * with the session's other work. A failure is raised later, by the {@link #next()} that reaches
   * it. A result larger than {@code limitBytes} is not kept: the stream is abandoned (the server
   * then cancels the statement) and reading past what was buffered raises an error.
   *
   * @param limitBytes the most Arrow buffer memory to hold
   */
  public synchronized void buffer(long limitBytes) {
    if (buffered || finished || closed) {
      return;
    }
    try {
      long held = 0;
      if (batchesRead > 0) {
        current = copy(reader.getVectorSchemaRoot());
        held += size(current);
      }
      while (nextFromStream()) {
        VectorSchemaRoot copy = copy(reader.getVectorSchemaRoot());
        pending.add(copy);
        held += size(copy);
        if (held > limitBytes && !finished) {
          pendingError =
              new SQLException(
                  "The rest of this result was dropped: the connection ran another statement while"
                      + " it was open, and it is larger than resultBufferSize ("
                      + limitBytes
                      + " bytes). Read results before running other statements, or set"
                      + " retainResults=true to keep several results open.",
                  "HY000");
          break;
        }
      }
    } catch (SQLException e) {
      pendingError = e;
    } catch (IOException e) {
      pendingError =
          new SQLNonTransientConnectionException(
              "Cannot read the result: " + e.getMessage(), "08P01", e);
    }
    bufferedDictionaries = new Dictionaries(reader, allocator);
    finished = false;
    buffered = true;
    generation++;
    closeBody();
    closeReader();
  }

  private static long size(VectorSchemaRoot root) {
    long bytes = 0;
    for (FieldVector vector : root.getFieldVectors()) {
      bytes += vector.getBufferSize();
    }
    return bytes;
  }

  /** A zero-copy copy: the new root shares the batch's buffers. */
  private VectorSchemaRoot copy(VectorSchemaRoot root) {
    VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
    try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
      new VectorLoader(copy).load(batch);
    }
    return copy;
  }

  /** Stops reading at once, from any thread: the reading thread's next read fails. */
  public void abort() {
    closed = true;
    closeBody();
  }

  @Override
  public synchronized void close() {
    closed = true;
    closeBody();
    closeReader();
    if (current != null) {
      current.close();
      current = null;
    }
    for (VectorSchemaRoot root : pending) {
      root.close();
    }
    pending.clear();
    if (bufferedDictionaries != null) {
      bufferedDictionaries.close();
      bufferedDictionaries = null;
    }
  }

  private boolean nextFromStream() throws SQLException {
    while (!finished) {
      if (closed) {
        throw new SQLException("The result was closed", "24000");
      }
      boolean loaded;
      try {
        loaded = reader.loadNextBatch();
      } catch (IOException | RuntimeException e) {
        start(reopenAfter(e));
        continue;
      }
      if (loaded) {
        batchesRead++;
        resumesWithoutProgress = 0;
        return true;
      }
      if (channel.reachedEnd()) {
        start(
            reopenAfter(
                new IOException("the result stream ended without its end-of-stream marker")));
        continue;
      }
      drainBody();
      if (partitions != null && partition + 1 < partitions.size()) {
        partition++;
        batchesRead = 0;
        start(open(0));
        continue;
      }
      finished = true;
    }
    return false;
  }

  private void start(InputStream newBody) throws SQLException {
    for (; ; ) {
      try {
        attach(newBody);
        Schema read = reader.getVectorSchemaRoot().getSchema();
        if (schema != null && !schema.equals(read)) {
          throw new SQLNonTransientConnectionException(
              "A resumed or partitioned result's schema differs from the result's", "08P01");
        }
        schema = read;
        return;
      } catch (IOException | RuntimeException e) {
        newBody = reopenAfter(e);
      }
    }
  }

  private void attach(InputStream newBody) {
    closeReader();
    body = newBody;
    channel = new ArrowIpc.HeapReadChannel(newBody);
    reader = ArrowIpc.reader(channel, allocator);
  }

  /**
   * Decides what a broken stream means. The statement's own error wins; a retained result is
   * fetched again from the next unread batch; anything else cannot be recovered.
   */
  private InputStream reopenAfter(Exception cause) throws SQLException {
    if (closed) {
      throw new SQLException("The result was closed", "24000", cause);
    }
    closeBody();
    StatementDocument document = client.awaitTerminal(sessionId, statementId);
    if (document.state() != StatementDocument.State.SUCCEEDED) {
      boolean droppedByUs =
          document.state() == StatementDocument.State.CANCELLED
              && "client".equals(document.cancelReason())
              && !cancelRequested.getAsBoolean();
      if (droppedByUs) {
        // The server cancels an unretained statement when its client disconnects (spec §6.5).
        throw new SQLNonTransientConnectionException(
            "The connection broke off while reading the result, and the server cancelled the"
                + " statement because the result was not retained (see the retainResults"
                + " property): "
                + cause.getMessage(),
            "08006",
            cause);
      }
      SQLException error =
          document.error() != null
              ? SqlErrors.toSqlException(document.error())
              : new SQLException("The statement " + document.state() + " mid-result", "HY000");
      error.addSuppressed(cause);
      throw error;
    }
    if (!document.retained() || ++resumesWithoutProgress > MAX_RESUMES) {
      throw new SQLNonTransientConnectionException(
          (document.retained()
                  ? "The result stream kept breaking off"
                  : "The connection broke off while reading a result the server did not retain,"
                      + " so it cannot be resumed (see the retainResults property)")
              + ": "
              + cause.getMessage(),
          "08006",
          cause);
    }
    if (partitions == null && !document.partitions().isEmpty()) {
      partitions = document.partitions();
    }
    return open(batchesRead);
  }

  private InputStream open(long fromBatch) throws SQLException {
    StatementDocument.Partition part = partitions == null ? null : partitions.get(partition);
    int index = part == null ? partition : part.index();
    return client
        .result(sessionId, statementId, index, part == null ? null : part.location(), fromBatch)
        .body();
  }

  private void drainBody() {
    try {
      body.transferTo(OutputStream.nullOutputStream());
    } catch (IOException e) {
      // The stream is complete; the connection just cannot be reused.
    }
    closeBody();
  }

  private void closeBody() {
    InputStream currentBody = body;
    if (currentBody != null) {
      try {
        currentBody.close();
      } catch (IOException e) {
        // Closing only releases the connection.
      }
    }
  }

  private void closeReader() {
    if (reader != null) {
      try {
        reader.close(false);
      } catch (IOException | RuntimeException e) {
        // Freeing buffers cannot fail in a way the caller can act on.
      }
      reader = null;
    }
  }

  /** The stream's dictionaries, moved out of the reader so they outlive it. */
  private static final class Dictionaries implements DictionaryProvider, AutoCloseable {
    private final Map<Long, Dictionary> byId = new HashMap<>();

    Dictionaries(DictionaryProvider source, BufferAllocator allocator) {
      for (long id : source.getDictionaryIds()) {
        Dictionary dictionary = source.lookup(id);
        FieldVector vector = dictionary.getVector();
        TransferPair transfer = vector.getTransferPair(allocator);
        transfer.transfer();
        byId.put(id, new Dictionary((FieldVector) transfer.getTo(), dictionary.getEncoding()));
      }
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
      for (Dictionary dictionary : byId.values()) {
        dictionary.getVector().close();
      }
    }
  }
}
