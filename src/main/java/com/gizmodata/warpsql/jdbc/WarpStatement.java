package com.gizmodata.warpsql.jdbc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gizmodata.warpsql.jdbc.client.PartitionedResult;
import com.gizmodata.warpsql.jdbc.client.ResultStream;
import com.gizmodata.warpsql.jdbc.client.SqlErrors;
import com.gizmodata.warpsql.jdbc.client.StatementDocument;
import com.gizmodata.warpsql.jdbc.client.WarpClient;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs SQL text (spec §6).
 *
 * <p>By default a statement is submitted on the one-round-trip streamed path: the response is the
 * result's Arrow stream, or a statement document for statements without rows. With the {@code
 * async} property it is submitted with {@code respond-async}, long-polled until done, and its
 * result fetched; the server then retains the result, so a dropped download resumes. {@link
 * #setQueryTimeout(int)} becomes {@code Warp-Statement-Timeout}, and {@link #cancel()} posts the
 * statement's cancel request.
 *
 * <p>With {@link #setResultPartitions(int)} above 1 (the {@code resultPartitions} property, 8 by
 * default), a server with the {@code result_partitions} capability may split a result into
 * partitions, which the driver reads over separate connections at once and returns as one result
 * set (spec §6.10).
 */
public class WarpStatement implements Statement {
  /** The connection the statement belongs to. */
  final WarpConnection connection;

  private volatile boolean closed;
  private WarpResultSet resultSet;
  private long updateCount = -1;
  private int queryTimeoutSeconds;
  private long maxRows;
  private int maxFieldSize;
  private int fetchSize;
  private boolean closeOnCompletion;
  private boolean poolable;
  private SQLWarning warnings;
  private final List<String> batch = new ArrayList<>();
  private volatile String runningSessionId;
  private volatile String runningStatementId;
  private volatile boolean cancelRequested;
  private volatile boolean submitting;
  private int resultPartitions;
  private boolean anyOrder;

  WarpStatement(WarpConnection connection) {
    this.connection = connection;
    this.resultPartitions = connection.client().config().resultPartitions();
    this.anyOrder = connection.client().config().anyOrder();
  }

  /** Submits one statement in a session. */
  @FunctionalInterface
  interface Submitter {
    WarpClient.Submission submit(String sessionId, WarpClient.SubmitOptions options)
        throws SQLException;
  }

  WarpClient.SubmitOptions submitOptions() {
    return new WarpClient.SubmitOptions(
        queryTimeoutSeconds * 1000L,
        connection.client().config().async(),
        resultPartitions,
        anyOrder);
  }

  /**
   * Runs a statement and keeps its outcome: a result set, or an update count.
   *
   * @param submitter how to submit it
   * @return {@code true} if it produced a result set
   * @throws SQLException with the statement's error if it failed
   */
  final boolean run(Submitter submitter) throws SQLException {
    checkOpen();
    closeResultSet();
    updateCount = -1;
    warnings = null;
    cancelRequested = false;
    WarpClient client = connection.client();
    WarpClient.SubmitOptions options = submitOptions();
    WarpClient.Submission submission =
        connection.withSession(
            id -> {
              runningSessionId = id;
              submitting = true;
              try {
                return submitter.submit(id, options);
              } finally {
                submitting = false;
              }
            });
    connection.statementRan();
    String sessionId = runningSessionId;
    String statementId = submission.statementId();
    runningStatementId = statementId;
    boolean producedRows = false;
    try {
      if (cancelRequested && statementId != null) {
        cancelOnServer(sessionId, statementId);
      }
      if (submission.arrow() != null) {
        ResultStream reader =
            PartitionedResult.streamed(
                client,
                connection.allocator(),
                sessionId,
                submission.arrow(),
                options.anyOrder(),
                () -> cancelRequested);
        if (!Boolean.TRUE.equals(client.config().retainResults())) {
          connection.streaming(reader);
        }
        open(reader);
        producedRows = true;
        return true;
      }
      StatementDocument document = submission.document();
      if (!document.state().isTerminal()) {
        document = client.awaitTerminal(sessionId, document.statementId());
      }
      if (document.state() != StatementDocument.State.SUCCEEDED) {
        throw failure(document);
      }
      if (document.hasRows()) {
        open(
            PartitionedResult.fetched(
                client,
                connection.allocator(),
                sessionId,
                document,
                options.anyOrder(),
                () -> cancelRequested));
        producedRows = true;
        return true;
      }
      updateCount = Math.max(document.rowsAffected(), 0);
      return false;
    } finally {
      if (!producedRows) {
        runningStatementId = null;
      }
    }
  }

  private static SQLException failure(StatementDocument document) {
    if (document.error() != null) {
      return SqlErrors.toSqlException(document.error());
    }
    return new SQLException(
        "The statement " + document.state().name().toLowerCase(java.util.Locale.ROOT),
        document.state() == StatementDocument.State.CANCELLED ? "57014" : "HY000");
  }

  /**
   * Opens the result and reads up to its first rows, so that a statement that fails as it starts
   * fails here, in {@code execute}, even when the server sent the result's schema first.
   */
  private void open(ResultStream reader) throws SQLException {
    String sessionId = runningSessionId;
    WarpResultSet[] holder = new WarpResultSet[1];
    holder[0] =
        new WarpResultSet(
            this,
            new ResultBatches.Streamed(reader),
            maxRows,
            () -> resultSetClosed(holder[0], reader, sessionId, reader.statementId()));
    resultSet = holder[0];
    try {
      holder[0].readAhead();
    } catch (SQLException e) {
      holder[0].close();
      throw e;
    }
  }

  /** Frees what the server still holds for a result that was not read to the end, or retained. */
  private void resultSetClosed(
      WarpResultSet closedSet, ResultStream reader, String sessionId, String statementId) {
    connection.streamClosed(reader);
    runningStatementId = null;
    if (resultSet == closedSet) {
      resultSet = null;
    }
    boolean retained =
        connection.client().config().async()
            || Boolean.TRUE.equals(connection.client().config().retainResults());
    if (statementId != null && !connection.isClosing() && (retained || !closedSet.isFinished())) {
      try {
        connection.client().deleteStatement(sessionId, statementId);
      } catch (SQLException e) {
        // The server frees it with the session at the latest.
      }
    }
    if (closeOnCompletion && !closed) {
      closeQuietly();
    }
  }

  private void cancelOnServer(String sessionId, String statementId) {
    try {
      connection.client().cancel(sessionId, statementId);
    } catch (SQLException e) {
      // The statement may already be finished; cancelling is best effort.
    }
  }

  boolean closesOnCompletion() {
    return closeOnCompletion;
  }

  void checkOpen() throws SQLException {
    if (closed) {
      throw new SQLException("The statement is closed", "HY010");
    }
    connection.checkOpen();
  }

  private void closeResultSet() {
    WarpResultSet current = resultSet;
    resultSet = null;
    if (current != null) {
      current.close();
    }
  }

  private static ObjectNode body(WarpClient client, String sql) throws SQLException {
    if (sql == null) {
      throw new SQLException("The SQL must not be null", "HY009");
    }
    return client.newObject().put("sql", sql);
  }

  // ---- Execution ----

  @Override
  public boolean execute(String sql) throws SQLException {
    checkOpen();
    ObjectNode body = body(connection.client(), sql);
    return run((id, options) -> connection.client().submit(id, body, options));
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (!execute(sql)) {
      throw new SQLException("The statement did not return a result set", "02000");
    }
    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return saturate(executeLargeUpdate(sql));
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    if (execute(sql)) {
      throw returnedRows();
    }
    return updateCount;
  }

  /** Closes the unexpected result and reports it; a write it belongs to still runs to its end. */
  final SQLException returnedRows() {
    resultSet.close();
    return new SQLException(
        "The statement returned a result set; use executeQuery or execute", "07000");
  }

  static int saturate(long count) {
    return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    requireNoGeneratedKeys(autoGeneratedKeys);
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw noGeneratedKeys();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw noGeneratedKeys();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    requireNoGeneratedKeys(autoGeneratedKeys);
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw noGeneratedKeys();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw noGeneratedKeys();
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    requireNoGeneratedKeys(autoGeneratedKeys);
    return executeLargeUpdate(sql);
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw noGeneratedKeys();
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    throw noGeneratedKeys();
  }

  private static void requireNoGeneratedKeys(int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
      throw noGeneratedKeys();
    }
    if (autoGeneratedKeys != NO_GENERATED_KEYS) {
      throw new SQLException("Unknown autoGeneratedKeys value " + autoGeneratedKeys, "HY024");
    }
  }

  private static SQLException noGeneratedKeys() {
    return new SQLFeatureNotSupportedException(
        "Generated keys are not supported; use INSERT ... RETURNING", "0A000");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw noGeneratedKeys();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkOpen();
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return saturate(getLargeUpdateCount());
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    checkOpen();
    return updateCount;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return getMoreResults(CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    checkOpen();
    if (current != KEEP_CURRENT_RESULT) {
      closeResultSet();
    } else {
      resultSet = null;
    }
    updateCount = -1;
    return false;
  }

  // ---- Batches of SQL text ----

  @Override
  public void addBatch(String sql) throws SQLException {
    checkOpen();
    if (sql == null) {
      throw new SQLException("The SQL must not be null", "HY009");
    }
    batch.add(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    checkOpen();
    batch.clear();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    long[] counts = executeLargeBatch();
    int[] result = new int[counts.length];
    for (int i = 0; i < counts.length; i++) {
      result[i] = saturate(counts[i]);
    }
    return result;
  }

  /** {@inheritDoc} Runs the statements one after another and stops at the first failure. */
  @Override
  public long[] executeLargeBatch() throws SQLException {
    checkOpen();
    List<String> statements = new ArrayList<>(batch);
    batch.clear();
    long[] counts = new long[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      try {
        if (execute(statements.get(i))) {
          closeResultSet();
          throw new SQLException("Batch statement " + (i + 1) + " returned a result set", "07000");
        }
      } catch (SQLException e) {
        throw new BatchUpdateException(
            e.getMessage(),
            e.getSQLState(),
            e.getErrorCode(),
            java.util.Arrays.copyOf(counts, i),
            e);
      }
      counts[i] = updateCount;
    }
    updateCount = -1;
    return counts;
  }

  // ---- Cancel and close ----

  /**
   * {@inheritDoc}
   *
   * <p>Asks the server to cancel the running statement (spec §6.8). While the submission's answer
   * has not arrived, so that the statement has no id yet, it cancels what the session runs; on a
   * connection shared between threads, that includes other statements running at that moment.
   */
  @Override
  public void cancel() throws SQLException {
    checkOpen();
    cancelRequested = true;
    String statementId = runningStatementId;
    if (statementId != null) {
      cancelOnServer(runningSessionId, statementId);
    } else if (submitting) {
      try {
        connection.client().cancelSession(runningSessionId);
      } catch (SQLException e) {
        // A server without session cancel: the statement is cancelled once its id arrives.
      }
    }
  }

  void abortQuietly() {
    cancelRequested = true;
    String statementId = runningStatementId;
    if (statementId != null) {
      cancelOnServer(runningSessionId, statementId);
    }
    WarpResultSet current = resultSet;
    if (current != null) {
      current.abort();
    }
  }

  @Override
  public void close() throws SQLException {
    closeQuietly();
  }

  void closeQuietly() {
    if (closed) {
      return;
    }
    closed = true;
    closeResultSet();
    connection.forget(this);
    closeServerSide();
  }

  /** Frees server-side state the statement owns; prepared statements override it. */
  void closeServerSide() {}

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    checkOpen();
    closeOnCompletion = true;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    checkOpen();
    return closeOnCompletion;
  }

  // ---- Settings ----

  @Override
  public int getQueryTimeout() throws SQLException {
    checkOpen();
    return queryTimeoutSeconds;
  }

  /** {@inheritDoc} Sent as {@code Warp-Statement-Timeout}; the server cancels the statement. */
  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    checkOpen();
    if (seconds < 0) {
      throw new SQLException("The timeout must not be negative", "HY024");
    }
    queryTimeoutSeconds = seconds;
  }

  /**
   * Sets the most partitions of each result to read at once, each over its own connection (spec
   * §6.10), for statements run afterwards. The default is the {@code resultPartitions} property's.
   * The server decides how many partitions a result gets, if any; it keeps small results in one.
   *
   * @param partitions 1 (one connection per result) to 256
   * @throws SQLException if the statement is closed or the number is out of range
   */
  public void setResultPartitions(int partitions) throws SQLException {
    checkOpen();
    if (partitions < 1 || partitions > WarpClient.MAX_RESULT_PARTITIONS) {
      throw new SQLException(
          "The partition count must be from 1 to " + WarpClient.MAX_RESULT_PARTITIONS, "HY024");
    }
    resultPartitions = partitions;
  }

  /**
   * Returns the most partitions of each result to read at once.
   *
   * @return 1 to 256
   * @throws SQLException if the statement is closed
   */
  public int getResultPartitions() throws SQLException {
    checkOpen();
    return resultPartitions;
  }

  /**
   * Sets whether a result read in partitions must keep its row order, for statements run
   * afterwards. With {@code any}, batches come back as the partitions deliver them, which can be
   * faster; the order of an {@code ORDER BY} is then lost. The default is the {@code resultOrder}
   * property's.
   *
   * @param order {@code preserve} or {@code any}
   * @throws SQLException if the statement is closed or the value is neither
   */
  public void setResultOrder(String order) throws SQLException {
    checkOpen();
    try {
      anyOrder = WarpDriver.resultOrderIsAny(order);
    } catch (SQLException e) {
      throw new SQLException(e.getMessage(), "HY024", e);
    }
  }

  /**
   * Returns whether a result read in partitions must keep its row order.
   *
   * @return {@code preserve} or {@code any}
   * @throws SQLException if the statement is closed
   */
  public String getResultOrder() throws SQLException {
    checkOpen();
    return anyOrder ? "any" : "preserve";
  }

  @Override
  public int getMaxRows() throws SQLException {
    return saturate(getLargeMaxRows());
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    setLargeMaxRows(max);
  }

  @Override
  public long getLargeMaxRows() throws SQLException {
    checkOpen();
    return maxRows;
  }

  /** {@inheritDoc} Applies to results of statements run afterwards. */
  @Override
  public void setLargeMaxRows(long max) throws SQLException {
    checkOpen();
    if (max < 0) {
      throw new SQLException("The maximum must not be negative", "HY024");
    }
    maxRows = max;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    checkOpen();
    return maxFieldSize;
  }

  /** {@inheritDoc} Recorded only: values are never truncated. */
  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    checkOpen();
    if (max < 0) {
      throw new SQLException("The maximum must not be negative", "HY024");
    }
    maxFieldSize = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    checkOpen();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Named cursors are not supported", "0A000");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkOpen();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Only FETCH_FORWARD is supported", "HY024");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkOpen();
    return ResultSet.FETCH_FORWARD;
  }

  /** {@inheritDoc} A hint only: the server chooses its batch size. */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkOpen();
    if (rows < 0) {
      throw new SQLException("The fetch size must not be negative", "HY024");
    }
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkOpen();
    return fetchSize;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    checkOpen();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkOpen();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    checkOpen();
    return connection.getHoldability();
  }

  @Override
  public Connection getConnection() throws SQLException {
    checkOpen();
    return connection;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkOpen();
    return warnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkOpen();
    warnings = null;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    checkOpen();
    this.poolable = poolable;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    checkOpen();
    return poolable;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Not a wrapper for " + iface.getName(), "HY000");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
