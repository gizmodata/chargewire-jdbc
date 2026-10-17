package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpConnection;
import com.gizmodata.warpsql.jdbc.WarpDataSource;
import com.gizmodata.warpsql.jdbc.WarpProblem;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Connecting and the connection's own state (ported from {@code ConnectionTest}, {@code
 * TokenAuthenticationTest} and {@code ArrowFlightJdbcDriverTest}'s connecting cases).
 */
class ConnectionIT {
  @Test
  void connectsWithUserAndPasswordInTheUrl() throws SQLException {
    WarpServer server = SharedServer.get();
    String url =
        server.url(
            "useEncryption=false&user=warp&password="
                + URLEncoder.encode(server.password, StandardCharsets.UTF_8));
    try (Connection connection = DriverManager.getConnection(url)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void connectsWithSemicolonOptionsAndAnyKeyCase() throws SQLException {
    WarpServer server = SharedServer.get();
    Properties properties = new Properties();
    properties.setProperty("USER", "warp");
    properties.setProperty("PassWord", server.password);
    try (Connection connection =
        DriverManager.getConnection(server.url("") + ";USEENCRYPTION=false", properties)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void propertiesPutAsBooleansAndNumbers() throws SQLException {
    WarpServer server = SharedServer.get();
    Properties properties = new Properties();
    properties.put("user", "warp");
    properties.put("password", server.password);
    properties.put("useEncryption", false);
    properties.put("networkTimeout", 15000);
    try (Connection connection = DriverManager.getConnection(server.url(""), properties)) {
      assertTrue(connection.isValid(2));
      assertEquals(15000, connection.getNetworkTimeout());
    }
    try (Connection connection =
        DriverManager.getConnection(server.url("useEncryption=0"), server.properties())) {
      assertTrue(connection.isValid(2), "0 and 1 are booleans too");
    }
  }

  @Test
  void urlOptionsWinOverProperties() throws SQLException {
    WarpServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("useEncryption", "true");
    try (Connection connection =
        DriverManager.getConnection(server.url("useEncryption=false"), properties)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void wrongPasswordIsRefused() {
    WarpServer server = SharedServer.get();
    for (String password :
        new String[] {server.password + "x", server.password.split(":")[0], ""}) {
      Properties properties = server.properties();
      properties.setProperty("password", password);
      SQLInvalidAuthorizationSpecException error =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class,
              () -> DriverManager.getConnection(server.url(""), properties));
      assertEquals("28000", error.getSQLState());
      assertEquals("invalid_credentials", WarpProblem.codeOf(error));
      assertEquals(401, error.getErrorCode());
    }
  }

  @Test
  void unknownUserIsRefused() {
    WarpServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("user", "nobody");
    SQLException error =
        assertThrows(
            SQLException.class, () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("invalid_credentials", WarpProblem.codeOf(error));
  }

  @Test
  void missingCredentialsAreReportedBeforeAnyRequest() {
    WarpServer server = SharedServer.get();
    SQLException none =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url("useEncryption=false")));
    assertEquals("28000", none.getSQLState());
    assertNull(WarpProblem.of(none), "the driver refuses before asking the server");
    SQLException noPassword =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url("useEncryption=false&user=warp")));
    assertEquals("28000", noPassword.getSQLState());
  }

  @Test
  void connectsWithAnAccessToken() throws Exception {
    WarpServer server = SharedServer.get();
    String token = new RawWarp(server).token();
    Properties properties = new Properties();
    properties.setProperty("token", token);
    properties.setProperty("useEncryption", "false");
    try (Connection connection = DriverManager.getConnection(server.url(""), properties);
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1")) {
      assertTrue(rows.next());
    }
  }

  @Test
  void aTokenWinsOverUserAndPassword() throws Exception {
    WarpServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("password", "wrong");
    properties.setProperty("token", new RawWarp(server).token());
    try (Connection connection = DriverManager.getConnection(server.url(""), properties)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void aForgedTokenIsRefused() {
    WarpServer server = SharedServer.get();
    Properties properties = new Properties();
    properties.setProperty("token", "forged");
    properties.setProperty("useEncryption", "false");
    SQLException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("invalid_token", WarpProblem.codeOf(error));
  }

  @Test
  void nothingListeningIsAConnectionError() {
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () ->
                DriverManager.getConnection(
                    "jdbc:warp://127.0.0.1:1?useEncryption=false&user=u&password=p"));
    assertEquals("08001", error.getSQLState());
  }

  @Test
  void aServerThatIsNotWarpSqlIsRefused() throws SQLException {
    WarpServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("basePath", "/warp/v1/health");
    SQLException error =
        assertThrows(
            SQLException.class, () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("08001", error.getSQLState(), error.getMessage());
  }

  @Test
  void plainHttpToATlsDefaultFails() {
    WarpServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.remove("useEncryption");
    SQLException error =
        assertThrows(
            SQLException.class, () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("08001", error.getSQLState(), error.getMessage());
  }

  // ---- Catalog, schema and options ----

  @Test
  void catalogAndSchemaComeFromTheSession() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertEquals("memory", connection.getCatalog());
      assertEquals("main", connection.getSchema());
    }
  }

  @Test
  void urlPathAndPropertiesSetTheCatalogAndSchema() throws SQLException {
    try (Connection setup = SharedServer.connect();
        Statement statement = setup.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS from_url");
    }
    WarpServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("schema", "from_url");
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:warp://127.0.0.1:" + server.port + "/memory", properties)) {
      assertEquals("memory", connection.getCatalog());
      assertEquals("from_url", connection.getSchema());
    }
  }

  @Test
  void aMissingCatalogFailsToConnect() {
    WarpServer server = SharedServer.get();
    SQLException error =
        assertThrows(
            SQLException.class,
            () ->
                DriverManager.getConnection(
                    "jdbc:warp://127.0.0.1:" + server.port + "/no_such_catalog",
                    server.properties()));
    assertEquals("invalid_option_value", WarpProblem.codeOf(error));
  }

  @Test
  void setSchemaAndSetCatalogChangeTheSession() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS other_schema");
      connection.setSchema("other_schema");
      assertEquals("other_schema", connection.getSchema());
      try (ResultSet rows = statement.executeQuery("SELECT current_schema()")) {
        assertTrue(rows.next());
        assertEquals("other_schema", rows.getString(1));
      }
      SQLException error =
          assertThrows(SQLException.class, () -> connection.setSchema("no_such_schema"));
      assertEquals("HY024", error.getSQLState());
      assertTrue(error.getMessage().contains("invalid_option_value"), error.getMessage());
      assertEquals("other_schema", connection.getSchema(), "a refused option changes nothing");
      connection.setCatalog("memory");
      assertEquals("memory", connection.getCatalog());
      assertEquals("main", connection.getSchema(), "setting the catalog resets the schema");
    }
  }

  @Test
  @Disabled(
      "warp-duckdb's session document keeps the old schema after SQL USE: GET /sessions/{id}"
          + " reports main while current_schema() is the new one (reported)")
  void schemaChangedInSqlIsSeenByGetSchema() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS used_in_sql");
      statement.execute("USE memory.used_in_sql");
      assertEquals("used_in_sql", connection.getSchema());
    }
  }

  @Test
  void transactionIsolationIsTheServers() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      int level = connection.getTransactionIsolation();
      assertEquals(connection.getMetaData().getDefaultTransactionIsolation(), level);
      connection.setTransactionIsolation(level);
      assertNull(connection.getWarnings());
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      assertNotNull(connection.getWarnings(), "a level the driver cannot set is a warning");
      connection.clearWarnings();
      assertNull(connection.getWarnings());
      assertThrows(SQLException.class, () -> connection.setTransactionIsolation(3));
    }
  }

  @Test
  void savepointsAreNotSupported() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertThrows(SQLFeatureNotSupportedException.class, connection::setSavepoint);
      assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("CALL x()"));
    }
  }

  @Test
  void onlyForwardOnlyReadOnlyResultSets() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertNotNull(
          connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
      assertThrows(
          SQLFeatureNotSupportedException.class,
          () ->
              connection.createStatement(
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
      assertThrows(
          SQLFeatureNotSupportedException.class,
          () ->
              connection.prepareStatement(
                  "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, connection.getHoldability());
    }
  }

  @Test
  void networkTimeoutAndClientInfoAreKept() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertEquals(0, connection.getNetworkTimeout());
      connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 30_000);
      assertEquals(30_000, connection.getNetworkTimeout());
      assertTrue(connection.isValid(5));
      connection.setClientInfo("ApplicationName", "tests");
      assertEquals("tests", connection.getClientInfo("ApplicationName"));
      assertEquals("tests", connection.getClientInfo().getProperty("ApplicationName"));
      connection.setReadOnly(true);
      assertTrue(connection.isReadOnly());
    }
  }

  @Test
  void networkTimeoutFromTheUrl() throws SQLException {
    try (Connection connection = SharedServer.connect("networkTimeout=12000")) {
      assertEquals(12_000, connection.getNetworkTimeout());
    }
  }

  @Test
  void closingTheConnectionClosesItsStatementsAndResults() throws SQLException {
    Connection connection = SharedServer.connect();
    Statement statement = connection.createStatement();
    ResultSet rows = statement.executeQuery("SELECT * FROM range(1000000) t(i)");
    assertTrue(rows.next());
    ResultSet tables = connection.getMetaData().getTables(null, null, null, null);
    connection.close();
    assertTrue(statement.isClosed());
    assertTrue(rows.isClosed());
    assertTrue(tables.isClosed());
  }

  @Test
  void abortClosesTheConnection() throws Exception {
    Connection connection = SharedServer.connect();
    connection.abort(Runnable::run);
    assertTrue(connection.isClosed());
  }

  @Test
  void unwrapsToTheDriverConnection() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertTrue(connection.isWrapperFor(WarpConnection.class));
      WarpConnection warp = connection.unwrap(WarpConnection.class);
      assertSame(connection, warp);
      assertTrue(warp.getSessionId().startsWith("s_"), warp.getSessionId());
      assertThrows(SQLException.class, () -> connection.unwrap(String.class));
    }
  }

  @Test
  void dataSourceConnects() throws SQLException {
    WarpServer server = SharedServer.get();
    WarpDataSource dataSource = new WarpDataSource();
    dataSource.setUrl(server.url("useEncryption=false"));
    dataSource.setUser(WarpServer.USER);
    dataSource.setPassword(server.password);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1")) {
      assertTrue(rows.next());
    }
    try (Connection connection = dataSource.getConnection(WarpServer.USER, server.password)) {
      assertTrue(connection.isValid(2));
    }
    assertThrows(SQLException.class, () -> dataSource.getConnection(WarpServer.USER, "wrong"));
  }
}
