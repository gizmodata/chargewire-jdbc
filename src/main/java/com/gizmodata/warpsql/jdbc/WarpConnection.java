package com.gizmodata.warpsql.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gizmodata.warpsql.jdbc.client.ConnectionConfig;
import com.gizmodata.warpsql.jdbc.client.ResultStream;
import com.gizmodata.warpsql.jdbc.client.WarpClient;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * A Warp SQL session (spec §5) as a JDBC connection.
 *
 * <p>If the server no longer has the session (evicted for idleness, or the server restarted), the
 * connection opens a new one, replays its catalog, schema and autocommit setting, and reports the
 * loss of temporary state as a {@link SQLWarning} on {@link #getWarnings()}. A statement that found
 * the session gone is run again in the new session, unless a transaction was open: its work is
 * gone, so that statement fails with SQLSTATE {@code 08007} instead.
 */
public final class WarpConnection implements Connection {
  private static final BufferAllocator ROOT = new RootAllocator();

  private final WarpClient client;
  private final String url;
  private final Map<String, String> properties;
  private final BufferAllocator allocator;
  private final Set<WarpStatement> statements = ConcurrentHashMap.newKeySet();
  private final Set<WarpResultSet> metadataResults = ConcurrentHashMap.newKeySet();
  private final Set<ResultStream> unreadStreams = ConcurrentHashMap.newKeySet();
  private final long resultBufferSize;
  private final Object sessionLock = new Object();

  private volatile String sessionId;
  private volatile int sessionGeneration;
  private String catalog;
  private String schema;
  private volatile boolean optionsStale;
  private volatile boolean autoCommit = true;
  private volatile boolean transactionDirty;
  private volatile boolean closed;
  private volatile boolean closing;
  private volatile boolean released;
  private volatile boolean ended;
  private boolean readOnly;
  private int transactionIsolation = TRANSACTION_READ_COMMITTED;
  private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
  private Map<String, Class<?>> typeMap = new HashMap<>();
  private final Properties clientInfo = new Properties();
  private SQLWarning warnings;
  private JsonNode serverInfo;

  private WarpConnection(
      WarpClient client, String url, Map<String, String> properties, long resultBufferSize) {
    this.client = client;
    this.url = url;
    this.properties = properties;
    this.resultBufferSize = resultBufferSize;
    this.allocator = ROOT.newChildAllocator("connection", 0, Long.MAX_VALUE);
  }

  static WarpConnection open(ConnectionConfig config, String url, Map<String, String> properties)
      throws SQLException {
    long networkTimeout = WarpDriver.number(properties, WarpProperty.NETWORK_TIMEOUT);
    long resultBufferSize = WarpDriver.number(properties, WarpProperty.RESULT_BUFFER_SIZE);
    WarpClient client =
        WarpClient.connect(
            config,
            "warp-sql-jdbc/" + WarpDriver.version() + " Java/" + System.getProperty("java.version"),
            networkTimeout > 0 ? Duration.ofMillis(networkTimeout) : null);
    WarpConnection connection = new WarpConnection(client, url, properties, resultBufferSize);
    try {
      connection.catalog = WarpProperty.CATALOG.get(properties);
      connection.schema = WarpProperty.SCHEMA.get(properties);
      connection.openSession();
      client.setRequestTimeout(networkTimeout > 0 ? Duration.ofMillis(networkTimeout) : null);
      return connection;
    } catch (SQLException | RuntimeException e) {
      connection.allocator.close();
      client.close();
      throw e;
    }
  }

  /** Creates a session with the known options, and takes the server's view of them. */
  private void openSession() throws SQLException {
    ObjectNode options = client.newObject();
    if (catalog != null) {
      options.put("catalog", catalog);
    }
    if (schema != null) {
      options.put("schema", schema);
    }
    options.put("autocommit", autoCommit);
    JsonNode session = client.createSession(options);
    String id = session.path("session_id").asText(null);
    if (id == null || id.isEmpty()) {
      throw new SQLNonTransientConnectionException(
          "The server's session response has no session_id", "08P01");
    }
    sessionId = id;
    sessionGeneration++;
    try {
      applyOptions(session.path("options"), catalog, schema);
    } catch (SQLException e) {
      String failed = id;
      sessionId = null;
      client.deleteSession(failed);
      throw e;
    }
  }

  /** Reads the session's options, failing if a requested catalog or schema did not take. */
  private void applyOptions(JsonNode options, String wantedCatalog, String wantedSchema)
      throws SQLException {
    String actualCatalog = options.path("catalog").asText(null);
    String actualSchema = options.path("schema").asText(null);
    if (wantedCatalog != null
        && actualCatalog != null
        && !wantedCatalog.equalsIgnoreCase(actualCatalog)) {
      throw new SQLNonTransientConnectionException(
          "The server did not set catalog '" + wantedCatalog + "' (it is '" + actualCatalog + "')",
          "3D000");
    }
    if (wantedSchema != null
        && actualSchema != null
        && !wantedSchema.equalsIgnoreCase(actualSchema)) {
      throw new SQLNonTransientConnectionException(
          "The server did not set schema '" + wantedSchema + "' (it is '" + actualSchema + "')",
          "3F000");
    }
    catalog = actualCatalog;
    schema = actualSchema;
    optionsStale = false;
  }

  // ---- Session-aware calls ----

  /** A call that needs the current session. */
  @FunctionalInterface
  interface SessionCall<T> {
    T apply(String sessionId) throws SQLException;
  }

  /**
   * Runs a call in the session, recovering once if the server no longer has the session.
   *
   * @param call the call
   * @param <T> what it returns
   * @return the call's result
   * @throws SQLException on any error, or {@code 08007} if a transaction was lost
   */
  <T> T withSession(SessionCall<T> call) throws SQLException {
    return withSession(call, true);
  }

  /**
   * Runs a call in the session, recovering once if the server no longer has the session.
   *
   * @param call the call
   * @param needsEngine whether the call uses the session's engine (statements, metadata, options,
   *     transactions): unread results are buffered first, because the server may hold such calls
   *     until a streaming result is read
   * @param <T> what it returns
   * @return the call's result
   * @throws SQLException on any error, or {@code 08007} if a transaction was lost
   */
  <T> T withSession(SessionCall<T> call, boolean needsEngine) throws SQLException {
    checkOpen();
    if (needsEngine) {
      for (ResultStream stream : unreadStreams) {
        stream.buffer(resultBufferSize);
      }
    }
    int generation = sessionGeneration;
    String id = sessionId;
    try {
      return call.apply(id);
    } catch (SQLException e) {
      String code = WarpProblem.codeOf(e);
      if (!"session_not_found".equals(code) && !"session_ended".equals(code)) {
        throw e;
      }
      recover(generation, e);
      return call.apply(sessionId);
    }
  }

  private void recover(int generation, SQLException cause) throws SQLException {
    synchronized (sessionLock) {
      if (sessionGeneration != generation) {
        return;
      }
      WarpProblem problem = WarpProblem.of(cause);
      String lost = sessionId;
      if ("session_ended".equals(problem.code())) {
        if (!"token_expired".equals(problem.reason())) {
          // Unusable from now on; close() still releases what the connection holds.
          ended = true;
          closed = true;
          throw new SQLNonTransientConnectionException(
              "The server ended the session (" + problem.reason() + "); reconnect", "08003", cause);
        }
        client.renewToken();
      }
      boolean lostTransaction = !autoCommit && transactionDirty;
      transactionDirty = false;
      openSession();
      addWarning(
          new SQLWarning(
              "The server no longer had session "
                  + lost
                  + " ("
                  + problem.code()
                  + "); session "
                  + sessionId
                  + " was opened with the same catalog, schema and autocommit. Temporary tables,"
                  + " prepared statements and settings made in SQL were lost.",
              "01000"));
      if (lostTransaction) {
        throw new SQLTransientConnectionException(
            "The session was lost with a transaction open, so its uncommitted work is gone; a new"
                + " session is open: run the transaction again",
            "08007",
            cause);
      }
    }
  }

  /**
   * Marks that a statement ran, so the session's options may have changed and a transaction may be
   * open.
   */
  void statementRan() {
    optionsStale = true;
    if (!autoCommit) {
      transactionDirty = true;
    }
  }

  WarpClient client() {
    return client;
  }

  /** Whether the connection is closing: ending the session frees everything it holds. */
  boolean isClosing() {
    return closing;
  }

  BufferAllocator allocator() {
    return allocator;
  }

  int sessionGeneration() {
    return sessionGeneration;
  }

  /**
   * Returns the id of the connection's current session.
   *
   * @return the session id; it changes if the connection had to open a new session
   */
  public String getSessionId() {
    return sessionId;
  }

  String url() {
    return url;
  }

  String user() {
    return WarpProperty.USER.get(properties);
  }

  synchronized JsonNode serverInfo() throws SQLException {
    if (serverInfo == null) {
      serverInfo = withSession(client::serverInfo, false);
    }
    return serverInfo;
  }

  void forget(WarpStatement statement) {
    statements.remove(statement);
  }

  /** Notes a result that streams without retention, so it is buffered before other work. */
  void streaming(ResultStream stream) {
    unreadStreams.add(stream);
  }

  void streamClosed(ResultStream stream) {
    unreadStreams.remove(stream);
  }

  /** Keeps a metadata result, which has no statement, so closing the connection closes it. */
  WarpResultSet track(WarpResultSet metadataResult) {
    metadataResults.add(metadataResult);
    return metadataResult;
  }

  void forget(WarpResultSet metadataResult) {
    metadataResults.remove(metadataResult);
  }

  private <T extends WarpStatement> T register(T statement) {
    statements.add(statement);
    return statement;
  }

  void checkOpen() throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("The connection is closed", "08003");
    }
  }

  synchronized void addWarning(SQLWarning warning) {
    if (warnings == null) {
      warnings = warning;
    } else {
      warnings.setNextWarning(warning);
    }
  }

  // ---- Statements ----

  @Override
  public Statement createStatement() throws SQLException {
    checkOpen();
    return register(new WarpStatement(this));
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkResultSetOptions(resultSetType, resultSetConcurrency, holdability);
    return createStatement();
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkResultSetOptions(resultSetType, resultSetConcurrency, resultSetHoldability);
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    checkOpen();
    return register(new WarpPreparedStatement(this, sql));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkResultSetOptions(resultSetType, resultSetConcurrency, holdability);
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    checkResultSetOptions(resultSetType, resultSetConcurrency, resultSetHoldability);
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
      throw new SQLFeatureNotSupportedException(
          "Generated keys are not supported; use INSERT ... RETURNING", "0A000");
    }
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Generated keys are not supported; use INSERT ... RETURNING", "0A000");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Generated keys are not supported; use INSERT ... RETURNING", "0A000");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Stored procedure calls are not supported", "0A000");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return prepareCall(sql);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return prepareCall(sql);
  }

  private void checkResultSetOptions(int type, int concurrency, int holdability)
      throws SQLException {
    checkOpen();
    if (type != ResultSet.TYPE_FORWARD_ONLY) {
      throw new SQLFeatureNotSupportedException(
          "Only forward-only result sets are supported", "0A000");
    }
    if (concurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
          "Only read-only result sets are supported", "0A000");
    }
    if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT
        && holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLException("Unknown holdability " + holdability, "HY024");
    }
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    checkOpen();
    return sql;
  }

  // ---- Transactions (spec §5.5) ----

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkOpen();
    if (autoCommit == this.autoCommit) {
      return;
    }
    if (autoCommit) {
      commit();
    }
    setOption("autocommit", autoCommit);
    this.autoCommit = autoCommit;
    transactionDirty = false;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkOpen();
    return autoCommit;
  }

  @Override
  public void commit() throws SQLException {
    endTransaction(true);
  }

  @Override
  public void rollback() throws SQLException {
    endTransaction(false);
  }

  private void endTransaction(boolean commit) throws SQLException {
    checkOpen();
    if (autoCommit) {
      throw new SQLException(
          "Cannot " + (commit ? "commit" : "roll back") + " in autocommit mode", "25000");
    }
    withSession(
        id -> {
          client.endTransaction(id, commit);
          return null;
        });
    transactionDirty = false;
  }

  private static SQLException noSavepoints() {
    return new SQLFeatureNotSupportedException("Savepoints are not supported", "0A000");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw noSavepoints();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw noSavepoints();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw noSavepoints();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw noSavepoints();
  }

  /**
   * {@inheritDoc} The server sets the isolation level; other levels are recorded with a warning.
   */
  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    checkOpen();
    if (level != TRANSACTION_NONE
        && level != TRANSACTION_READ_UNCOMMITTED
        && level != TRANSACTION_READ_COMMITTED
        && level != TRANSACTION_REPEATABLE_READ
        && level != TRANSACTION_SERIALIZABLE) {
      throw new SQLException("Unknown transaction isolation level " + level, "HY024");
    }
    if (level != transactionIsolation) {
      addWarning(
          new SQLWarning(
              "Warp SQL has no isolation setting; the server's own isolation level applies",
              "01000"));
    }
    transactionIsolation = level;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkOpen();
    return transactionIsolation;
  }

  // ---- Session options (spec §5.3) ----

  private void setOption(String name, Object value) throws SQLException {
    ObjectNode options = client.newObject();
    if (value instanceof Boolean) {
      options.put(name, (Boolean) value);
    } else {
      options.put(name, (String) value);
    }
    JsonNode response = withSession(id -> client.patchSession(id, options));
    JsonNode error = response.path("errors").path(name);
    if (!error.isMissingNode() && !error.isNull()) {
      String code = error.path("code").asText("invalid_option_value");
      String detail = error.path("detail").asText("the server refused " + name);
      throw new SQLException(
          detail + " [" + code + "]", "unknown_option".equals(code) ? "HY092" : "HY024");
    }
    JsonNode actual = response.path("options");
    synchronized (sessionLock) {
      catalog = actual.path("catalog").asText(catalog);
      schema = actual.path("schema").asText(schema);
      optionsStale = false;
    }
  }

  private void refreshOptions() throws SQLException {
    if (optionsStale) {
      JsonNode session = withSession(client::getSession, false);
      synchronized (sessionLock) {
        catalog = session.path("options").path("catalog").asText(null);
        schema = session.path("options").path("schema").asText(null);
        optionsStale = false;
      }
    }
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    checkOpen();
    if (catalog != null) {
      setOption("catalog", catalog);
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    checkOpen();
    refreshOptions();
    return catalog;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    checkOpen();
    if (schema != null) {
      setOption("schema", schema);
    }
  }

  @Override
  public String getSchema() throws SQLException {
    checkOpen();
    refreshOptions();
    return schema;
  }

  /** {@inheritDoc} A hint only: read-only is the server's and the principal's to decide. */
  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkOpen();
    this.readOnly = readOnly;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkOpen();
    return readOnly;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkOpen();
    return new WarpDatabaseMetaData(this);
  }

  // ---- Lifecycle ----

  /**
   * {@inheritDoc}
   *
   * <p>Probes the session with {@code GET /warp/v1/sessions/{id}}, opening a new session if the
   * server no longer has it.
   */
  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("The timeout must not be negative", "HY024");
    }
    if (closed) {
      return false;
    }
    Duration wait = timeout > 0 ? Duration.ofSeconds(timeout) : null;
    try {
      withSession(id -> client.getSession(id, wait), false);
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  /** {@inheritDoc} Closes the statements and ends the server session. */
  @Override
  public void close() throws SQLException {
    if (released) {
      return;
    }
    released = true;
    closing = true;
    closeStatements();
    for (WarpResultSet result : metadataResults) {
      result.close();
    }
    closed = true;
    try {
      if (sessionId != null && !ended) {
        client.deleteSession(sessionId);
      }
    } catch (SQLException e) {
      // The server ends idle sessions on its own.
    } finally {
      client.close();
      try {
        allocator.close();
      } catch (IllegalStateException e) {
        throw new SQLException(
            "The driver did not release all Arrow memory: " + e.getMessage(), "HY000", e);
      }
    }
  }

  private void closeStatements() {
    for (Iterator<WarpStatement> it = statements.iterator(); it.hasNext(); ) {
      WarpStatement statement = it.next();
      it.remove();
      statement.closeQuietly();
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /** {@inheritDoc} Stops running statements and ends the session without waiting. */
  @Override
  public void abort(Executor executor) throws SQLException {
    if (executor == null) {
      throw new SQLException("The executor must not be null", "HY009");
    }
    if (released) {
      return;
    }
    for (WarpStatement statement : statements) {
      statement.abortQuietly();
    }
    executor.execute(
        () -> {
          try {
            close();
          } catch (SQLException e) {
            // Aborting is best effort.
          }
        });
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    checkOpen();
    if (milliseconds < 0) {
      throw new SQLException("The timeout must not be negative", "HY024");
    }
    client.setRequestTimeout(milliseconds == 0 ? null : Duration.ofMillis(milliseconds));
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    checkOpen();
    Duration timeout = client.requestTimeout();
    return timeout == null ? 0 : (int) Math.min(timeout.toMillis(), Integer.MAX_VALUE);
  }

  @Override
  public synchronized SQLWarning getWarnings() throws SQLException {
    checkOpen();
    return warnings;
  }

  @Override
  public synchronized void clearWarnings() throws SQLException {
    checkOpen();
    warnings = null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkOpen();
    return typeMap;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    checkOpen();
    typeMap = map == null ? new HashMap<>() : map;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    checkResultSetOptions(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, holdability);
    this.holdability = holdability;
  }

  @Override
  public int getHoldability() throws SQLException {
    checkOpen();
    return holdability;
  }

  // ---- Client info: kept locally ----

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (closed) {
      throw new SQLClientInfoException(
          "The connection is closed", "08003", 0, Map.of(name, ClientInfoStatus.REASON_UNKNOWN));
    }
    if (value == null) {
      clientInfo.remove(name);
    } else {
      clientInfo.setProperty(name, value);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    if (closed) {
      throw new SQLClientInfoException("The connection is closed", "08003", 0, Map.of());
    }
    clientInfo.clear();
    if (properties != null) {
      clientInfo.putAll(properties);
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkOpen();
    return clientInfo.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkOpen();
    Properties copy = new Properties();
    copy.putAll(clientInfo);
    return copy;
  }

  // ---- Unsupported value factories ----

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Clob values are not supported; use String", "0A000");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Blob values are not supported; use byte[]", "0A000");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "NClob values are not supported; use String", "0A000");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("SQLXML values are not supported", "0A000");
  }

  /** {@inheritDoc} Returns an array that list parameters accept. */
  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkOpen();
    return new ParameterArray(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Struct parameters are not supported", "0A000");
  }

  // ---- Wrapper ----

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
