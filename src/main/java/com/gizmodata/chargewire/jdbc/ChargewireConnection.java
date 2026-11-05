package com.gizmodata.chargewire.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gizmodata.chargewire.jdbc.client.ChargewireClient;
import com.gizmodata.chargewire.jdbc.client.ConnectionConfig;
import com.gizmodata.chargewire.jdbc.client.ResultStream;
import com.gizmodata.chargewire.jdbc.client.SqlErrors;
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
import java.sql.SQLTransactionRollbackException;
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * A Chargewire session (spec §5) as a JDBC connection.
 *
 * <p>If the server no longer has the session (evicted for idleness, or the server restarted), the
 * connection opens a new one, replays its catalog, schema and autocommit setting, and reports the
 * loss of temporary state as a {@link SQLWarning} on {@link #getWarnings()}. A statement that found
 * the session gone is run again in the new session only when the driver knows no transaction was
 * open in the old one, since a transaction's work is rolled back with its session and the statement
 * would otherwise commit on its own: with autocommit off, when no statement ran since the last
 * commit or rollback; with autocommit on, when the session document read after the last statement
 * said {@code none} ({@link #getTransactionState()}, {@link #isValid(int)}, or {@link
 * #getCatalog()} and {@link #getSchema()} after a statement; a SQL {@code BEGIN} shows only there,
 * spec §5.2), and no statement whose text may open a transaction ran since the last one whose text
 * may end it (servers up to Chargewire 2.2.0 can report {@code none} for a moment after a {@code
 * BEGIN}; see {@link #statementRan(StatementText.Reading)}). Otherwise the statement fails with
 * SQLSTATE {@code 08007} and is not run; the new session is open for the next one.
 *
 * <p>Nor is it run again after the old session ran SQL that changed where or how statements run,
 * unless the driver restores that itself from what it knows to be fresh: after {@code SET}, {@code
 * RESET} or {@code PRAGMA} (settings, which the new session does not have), and after {@code USE}
 * until the session has been read after a later statement's answer (the new session opens with the
 * catalog and schema read then). It fails with SQLSTATE {@code 08007} too, and is not run.
 *
 * <p>A session the server ended ({@code session_ended}, spec §5.6) for a transaction left idle
 * ({@code idle_in_transaction}, §5.7) is replaced the same way, and the statement fails with
 * SQLSTATE {@code 40000} ({@link java.sql.SQLTransactionRollbackException}): the server rolled the
 * transaction back. A session ended because its token expired is reopened after signing in again,
 * under the rule above. After any other reason ({@code killed}, {@code token_revoked}, {@code
 * credentials_changed}, {@code principal_dropped}, or one this driver does not know) the connection
 * is closed and the error names the reason.
 */
public final class ChargewireConnection implements Connection {
  /**
   * Where the session's transaction stands (spec §5.1, the session document's {@code transaction}).
   */
  public enum TransactionState {
    /** No transaction is open: each statement commits as it runs. */
    NONE,
    /** A transaction is open. */
    OPEN,
    /**
     * A statement failed inside the transaction, and the server refuses every statement until a
     * rollback.
     */
    ABORTED,
    /**
     * The server did not say, or said something this driver does not know (spec §12); inside the
     * driver also: a statement ran since the session was last read, or the session said {@code
     * none} while a statement may have opened a transaction (the read may be stale).
     */
    UNKNOWN;

    static TransactionState of(JsonNode session) {
      switch (session.path("transaction").asText("")) {
        case "none":
          return NONE;
        case "open":
          return OPEN;
        case "aborted":
          return ABORTED;
        default:
          return UNKNOWN;
      }
    }
  }

  private static final BufferAllocator ROOT = new RootAllocator();

  /** The {@code session_ended} reason for a transaction left idle too long (spec §5.6, §5.7). */
  static final String IDLE_IN_TRANSACTION = "idle_in_transaction";

  private final ChargewireClient client;
  private final String url;
  private final Map<String, String> properties;
  private final BufferAllocator allocator;
  private final Set<ChargewireStatement> statements = ConcurrentHashMap.newKeySet();
  private final Set<ChargewireResultSet> metadataResults = ConcurrentHashMap.newKeySet();
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
  private volatile TransactionState transaction = TransactionState.UNKNOWN;

  /**
   * With autocommit on: a statement whose text may open a transaction ran in this session since the
   * last one whose text may end one, so a session document saying {@code none} may be stale and
   * does not count (see {@link #statementRan(StatementText.Reading)}).
   */
  private volatile boolean transactionMayBeOpen;

  /**
   * The number ({@link #statementsRun}) of the last statement in this session whose text starts
   * with {@code USE}, until a session read taken after a later statement's answer brought its
   * catalog and schema back; 0 when there is none. A read right after the {@code USE} itself may be
   * stale on servers up to Chargewire 2.2.0, but one taken after the next statement's answer is
   * not: the server runs a session's statements one after another, and records what one did before
   * the next starts.
   */
  private long placeChangedBy;

  /**
   * A statement whose text starts with {@code SET}, {@code RESET} or {@code PRAGMA} ran in this
   * session: settings the driver does not restore in a new session may differ.
   */
  private boolean settingsChanged;

  /**
   * Counts statements run, so a session read that a statement on another thread overtook does not
   * report the state from before it as current.
   */
  private final AtomicLong statementsRun = new AtomicLong();

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

  private ChargewireConnection(
      ChargewireClient client, String url, Map<String, String> properties, long resultBufferSize) {
    this.client = client;
    this.url = url;
    this.properties = properties;
    this.resultBufferSize = resultBufferSize;
    this.allocator = ROOT.newChildAllocator("connection", 0, Long.MAX_VALUE);
  }

  static ChargewireConnection open(
      ConnectionConfig config, String url, Map<String, String> properties) throws SQLException {
    long networkTimeout = ChargewireDriver.number(properties, ChargewireProperty.NETWORK_TIMEOUT);
    long resultBufferSize =
        ChargewireDriver.number(properties, ChargewireProperty.RESULT_BUFFER_SIZE);
    ChargewireClient client =
        ChargewireClient.connect(
            config,
            "chargewire-jdbc/"
                + ChargewireDriver.version()
                + " Java/"
                + System.getProperty("java.version"),
            networkTimeout > 0 ? Duration.ofMillis(networkTimeout) : null);
    ChargewireConnection connection =
        new ChargewireConnection(client, url, properties, resultBufferSize);
    try {
      connection.catalog = ChargewireProperty.CATALOG.get(properties);
      connection.schema = ChargewireProperty.SCHEMA.get(properties);
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
      throw SqlErrors.protocolError("The server's session response has no session_id");
    }
    sessionId = id;
    sessionGeneration++;
    transactionMayBeOpen = false;
    placeChangedBy = 0;
    settingsChanged = false;
    transaction = TransactionState.of(session);
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
      String code = ChargewireProblem.codeOf(e);
      if (!"session_not_found".equals(code) && !"session_ended".equals(code)) {
        throw e;
      }
      recover(generation, e);
      return call.apply(sessionId);
    }
  }

  /**
   * Replaces a session the server no longer has, and returns normally only when the call that found
   * it gone may run again in the new session: when the driver knows no transaction was open in the
   * old one. Otherwise the new session is open and the error says the call did not run.
   */
  private void recover(int generation, SQLException cause) throws SQLException {
    synchronized (sessionLock) {
      if (sessionGeneration != generation) {
        return;
      }
      ChargewireProblem problem = ChargewireProblem.of(cause);
      String lost = sessionId;
      String reason = problem.reason();
      boolean idleInTransaction = false;
      if ("session_ended".equals(problem.code())) {
        if (IDLE_IN_TRANSACTION.equals(reason)) {
          // The server rolled the transaction back (spec §5.7); a new session may use the same
          // token (§5.6).
          idleInTransaction = true;
        } else if (!"token_expired".equals(reason)) {
          // Unusable from now on; close() still releases what the connection holds.
          ended = true;
          closed = true;
          throw new SQLNonTransientConnectionException(endedMessage(reason), "08003", cause);
        } else {
          // The session's token expired: a new session needs a new token (spec §5.6).
          client.renewToken();
        }
      }
      // Whether a transaction was open is known only from the session document read after the
      // last statement, or, with autocommit off, from no statement having run since the last
      // commit or rollback (statementRan() marks both).
      TransactionState before = autoCommit ? transaction : TransactionState.NONE;
      boolean openTransaction =
          idleInTransaction
              || (autoCommit
                  ? before == TransactionState.OPEN || before == TransactionState.ABORTED
                  : transactionDirty);
      boolean unknownTransaction = !openTransaction && before != TransactionState.NONE;
      // What SQL changed in the old session that the new one won't have: settings (never
      // restored), or a USE whose catalog and schema the driver has not read back fresh.
      boolean lostSettings = settingsChanged;
      boolean lostPlace = placeChangedBy != 0;
      transactionDirty = false;
      openSession();
      addWarning(
          new SQLWarning(
              "The server no longer had session "
                  + lost
                  + " ("
                  + problem.code()
                  + (reason == null ? "" : ": " + reason)
                  + "); session "
                  + sessionId
                  + " was opened with the same catalog, schema and autocommit. Temporary tables,"
                  + " prepared statements and settings made in SQL were lost.",
              "01000"));
      if (idleInTransaction) {
        throw new SQLTransactionRollbackException(
            "The server ended session "
                + lost
                + " because its transaction was left idle longer than the server allows ("
                + IDLE_IN_TRANSACTION
                + "), and rolled the transaction back; session "
                + sessionId
                + " is open, and this statement was not run: run the transaction again",
            "40000",
            cause);
      }
      if (openTransaction) {
        throw new SQLTransientConnectionException(
            "The session was lost with a transaction open, so its uncommitted work is gone;"
                + " session "
                + sessionId
                + " is open, and this statement was not run: run the transaction again",
            "08007",
            cause);
      }
      if (unknownTransaction) {
        throw new SQLTransientConnectionException(
            "The session was lost, and the driver cannot tell whether a transaction was open in it"
                + " (a statement ran since the session was last read, and a SQL BEGIN shows only"
                + " there, or a statement that may begin one ran since the last that may end one);"
                + " if one was, its uncommitted work is gone. Session "
                + sessionId
                + " is open, and this statement was not run, so that it cannot commit on its own:"
                + " run it again, or the transaction if there was one",
            "08007",
            cause);
      }
      if (lostSettings || lostPlace) {
        throw new SQLTransientConnectionException(
            "The session was lost after it ran "
                + (lostSettings && lostPlace
                    ? "SET, RESET or PRAGMA, and USE"
                    : lostSettings ? "SET, RESET or PRAGMA" : "USE")
                + ", and what that changed is gone with it: "
                + (lostSettings
                    ? "settings made in SQL are not restored in a new session"
                    : "the driver had not read the session back since, so it can't tell which"
                        + " catalog and schema it used")
                + ". Session "
                + sessionId
                + " is open (catalog "
                + catalog
                + ", schema "
                + schema
                + "), and this statement was not run there, where it could have run differently: "
                + (lostSettings ? "run the settings again" : "run the USE again")
                + ", then the statement",
            "08007",
            cause);
      }
    }
  }

  private static String endedMessage(String reason) {
    if (reason == null) {
      return "The server ended the session; reconnect";
    }
    if (!ChargewireClient.mayRenewAfter(reason)) {
      // token_revoked, credentials_changed, principal_dropped: signing in again would not help.
      return "The server ended the session ("
          + reason
          + "): the access it was opened with was withdrawn; reconnect with valid credentials";
    }
    return "The server ended the session (" + reason + "); reconnect";
  }

  /**
   * Marks that a statement ran (or may have, having failed) in the current session, so its options
   * may have changed and a transaction may be open: with autocommit off, the transaction has work;
   * with autocommit on, whether one is open is unknown until the session is read again (a SQL
   * {@code BEGIN}, spec §5.2).
   *
   * <p>With autocommit on, what the statement's text may do to the transaction also decides which
   * later reads of the session count. Chargewire servers up to 2.2.0 (every Hardcharger through
   * 2.7.0) answer a statement without rows on the streamed path as soon as its outcome is known,
   * and copy the transaction state into the session document slightly after that, so a read right
   * after a statement may show the state from before it ({@code none} right after {@code BEGIN},
   * {@code open} right after {@code COMMIT}); so may a read while a statement's rows are still
   * unread, on any server. The rule:
   *
   * <ul>
   *   <li>Text that may open a transaction or change how one opens ({@code BEGIN}, {@code START},
   *       anything naming autocommit, {@code EXECUTE} of a prepared statement): the transaction
   *       counts as possibly open, and a read saying {@code none} does not clear that, because it
   *       may be stale. It holds until a statement whose text may end a transaction runs, or the
   *       session is replaced, or autocommit is switched.
   *   <li>Text that may end one ({@code COMMIT}, {@code ROLLBACK}, {@code END}, {@code ABORT}): a
   *       later read counts for {@code none}, because a stale read can only lag toward {@code
   *       open}, the safe side.
   *   <li>Text that may do both: the safe side, as for opening.
   *   <li>Text that may do neither leaves the rule as it stands: a later read counts, unless an
   *       earlier statement left the transaction possibly open.
   * </ul>
   *
   * <p>A lost session's statement is run again only when the state the driver holds is {@code
   * none}, so a stale {@code none} after a SQL {@code BEGIN} can no longer let a statement run on
   * its own in autocommit on a new session.
   *
   * <p>Text that changes where or how statements run marks what a new session would lack: {@code
   * SET}, {@code RESET} and {@code PRAGMA} for the rest of the session (the driver does not restore
   * settings), {@code USE} until a session read after a later statement's answer.
   *
   * @param text what the statement's text may do to its session ({@link StatementText})
   */
  void statementRan(StatementText.Reading text) {
    StatementText.Effect effect = text.transaction();
    synchronized (sessionLock) {
      if (autoCommit) {
        if (effect.mayOpen()) {
          transactionMayBeOpen = true;
        } else if (effect.mayEnd()) {
          transactionMayBeOpen = false;
        }
      }
      long number = statementsRun.incrementAndGet();
      if (text.changesPlace()) {
        placeChangedBy = number;
      }
      if (text.changesSettings()) {
        settingsChanged = true;
      }
      optionsStale = true;
      if (autoCommit) {
        transaction = TransactionState.UNKNOWN;
      } else {
        transactionDirty = true;
      }
    }
  }

  ChargewireClient client() {
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
    return ChargewireProperty.USER.get(properties);
  }

  synchronized JsonNode serverInfo() throws SQLException {
    if (serverInfo == null) {
      serverInfo = withSession(client::serverInfo, false);
    }
    return serverInfo;
  }

  void forget(ChargewireStatement statement) {
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
  ChargewireResultSet track(ChargewireResultSet metadataResult) {
    metadataResults.add(metadataResult);
    return metadataResult;
  }

  void forget(ChargewireResultSet metadataResult) {
    metadataResults.remove(metadataResult);
  }

  private <T extends ChargewireStatement> T register(T statement) {
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
    return register(new ChargewireStatement(this));
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
    return register(new ChargewirePreparedStatement(this, sql));
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
    transactionMayBeOpen = false;
    // Off opens a transaction; on comes after the commit above (spec §5.1).
    transaction = autoCommit ? TransactionState.NONE : TransactionState.OPEN;
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
              "Chargewire has no isolation setting; the server's own isolation level applies",
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
      readSession();
    }
  }

  /** Reads the session document: its options and its transaction. */
  private void readSession() throws SQLException {
    long statements = statementsRun.get();
    JsonNode session = withSession(client::getSession, false);
    takeSession(session, statements);
  }

  /**
   * Takes the options and the transaction from a session document whose read began when {@code
   * statementsBefore} statements had run. The catalog and schema of a {@code USE} count as read
   * back only from a read that began after a later statement's answer ({@link #placeChangedBy}).
   */
  private void takeSession(JsonNode session, long statementsBefore) {
    synchronized (sessionLock) {
      catalog = session.path("options").path("catalog").asText(null);
      schema = session.path("options").path("schema").asText(null);
      optionsStale = false;
      if (placeChangedBy != 0 && statementsBefore > placeChangedBy) {
        placeChangedBy = 0;
      }
      takeTransaction(session, statementsBefore);
    }
  }

  /**
   * Takes the transaction state from a session document, unless a statement ran after the read
   * began (its effect may not show in it), and except a {@code none} while a statement may have
   * opened a transaction ({@link #statementRan(StatementText.Reading)}: the read may be stale).
   */
  private void takeTransaction(JsonNode session, long statementsBefore) {
    synchronized (sessionLock) {
      if (statementsRun.get() == statementsBefore) {
        TransactionState read = TransactionState.of(session);
        transaction =
            read == TransactionState.NONE && autoCommit && transactionMayBeOpen
                ? TransactionState.UNKNOWN
                : read;
      } else if (autoCommit) {
        transaction = TransactionState.UNKNOWN;
      }
    }
  }

  /**
   * Returns where the session's transaction stands, as the server reports it (spec §5.2): a SQL
   * {@code BEGIN} shows here once its statement has ended, and so does a transaction the server
   * aborted after a failed statement. With autocommit off a transaction is always open. After a
   * statement whose text may open a transaction ({@code BEGIN}, {@code START}, {@code EXECUTE},
   * anything naming autocommit), a {@code none} reads as {@link TransactionState#UNKNOWN} until one
   * whose text may end it ({@code COMMIT}, {@code ROLLBACK}, {@code END}, {@code ABORT}): servers
   * up to Chargewire 2.2.0 can report {@code none} for a moment after a {@code BEGIN}.
   *
   * @return the session's transaction
   * @throws SQLException if the session cannot be read
   */
  public TransactionState getTransactionState() throws SQLException {
    checkOpen();
    readSession();
    return transaction;
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
    return new ChargewireDatabaseMetaData(this);
  }

  // ---- Lifecycle ----

  /**
   * {@inheritDoc}
   *
   * <p>Probes the session with {@code GET /chargewire/v1/sessions/{id}}, opening a new session if
   * the server no longer has it.
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
      long statements = statementsRun.get();
      JsonNode session = withSession(id -> client.getSession(id, wait), false);
      takeSession(session, statements);
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
    for (ChargewireResultSet result : metadataResults) {
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
    for (Iterator<ChargewireStatement> it = statements.iterator(); it.hasNext(); ) {
      ChargewireStatement statement = it.next();
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
    for (ChargewireStatement statement : statements) {
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
