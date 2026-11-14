package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireConnection;
import com.gizmodata.chargewire.jdbc.ChargewireDataSource;
import com.gizmodata.chargewire.jdbc.ChargewireProblem;
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
import org.junit.jupiter.api.Test;

/**
 * Connecting and the connection's own state (ported from {@code ConnectionTest}, {@code
 * TokenAuthenticationTest} and {@code ArrowFlightJdbcDriverTest}'s connecting cases).
 */
class ConnectionIT {
  @Test
  void connectsWithUserAndPasswordInTheUrl() throws SQLException {
    ChargewireServer server = SharedServer.get();
    String url =
        server.url(
            "useEncryption=false&user=chargewire&password="
                + URLEncoder.encode(server.password, StandardCharsets.UTF_8));
    try (Connection connection = DriverManager.getConnection(url)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void connectsWithSemicolonOptionsAndAnyKeyCase() throws SQLException {
    ChargewireServer server = SharedServer.get();
    Properties properties = new Properties();
    properties.setProperty("USER", "chargewire");
    properties.setProperty("PassWord", server.password);
    try (Connection connection =
        DriverManager.getConnection(server.url("") + ";USEENCRYPTION=false", properties)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void propertiesPutAsBooleansAndNumbers() throws SQLException {
    ChargewireServer server = SharedServer.get();
    Properties properties = new Properties();
    properties.put("user", "chargewire");
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
    ChargewireServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("useEncryption", "true");
    try (Connection connection =
        DriverManager.getConnection(server.url("useEncryption=false"), properties)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void wrongPasswordIsRefused() {
    ChargewireServer server = SharedServer.get();
    for (String password :
        new String[] {server.password + "x", server.password.split(":")[0], ""}) {
      Properties properties = server.properties();
      properties.setProperty("password", password);
      SQLInvalidAuthorizationSpecException error =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class,
              () -> DriverManager.getConnection(server.url(""), properties));
      assertEquals("28000", error.getSQLState());
      assertEquals("invalid_credentials", ChargewireProblem.codeOf(error));
      assertEquals(401, error.getErrorCode());
    }
  }

  @Test
  void aRefusedSignInShowsNeitherThePasswordNorTheToken() {
    ChargewireServer server = SharedServer.get();
    String secret = "Zq7Kv@Xw9Jp";
    for (String option : new String[] {"password", "token"}) {
      for (boolean inUrl : new boolean[] {true, false}) {
        Properties properties = server.properties();
        properties.remove("password");
        String url = server.url("");
        if (inUrl) {
          url += (url.contains("?") ? "&" : "?") + option + "=" + secret;
        } else {
          properties.setProperty(option, secret);
        }
        String connectUrl = url;
        SQLException error =
            assertThrows(
                SQLException.class, () -> DriverManager.getConnection(connectUrl, properties));
        assertEquals("28000", error.getSQLState(), error.getMessage());
        assertEquals(401, error.getErrorCode());
        StringBuilder messages = new StringBuilder();
        for (Throwable each = error; each != null; each = each.getCause()) {
          messages.append(each).append('\n');
          for (Throwable suppressed : each.getSuppressed()) {
            messages.append(suppressed).append('\n');
          }
        }
        assertFalse(messages.toString().contains("Zq7Kv"), messages.toString());
        assertFalse(messages.toString().contains("Xw9Jp"), messages.toString());
      }
    }
  }

  @Test
  void unknownUserIsRefused() {
    ChargewireServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("user", "nobody");
    SQLException error =
        assertThrows(
            SQLException.class, () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("invalid_credentials", ChargewireProblem.codeOf(error));
  }

  @Test
  void missingCredentialsAreReportedBeforeAnyRequest() {
    ChargewireServer server = SharedServer.get();
    SQLException none =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url("useEncryption=false")));
    assertEquals("28000", none.getSQLState());
    assertNull(ChargewireProblem.of(none), "the driver refuses before asking the server");
    SQLException noPassword =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url("useEncryption=false&user=chargewire")));
    assertEquals("28000", noPassword.getSQLState());
  }

  @Test
  void connectsWithAnAccessToken() throws Exception {
    ChargewireServer server = SharedServer.get();
    String token = new RawChargewire(server).token();
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
    ChargewireServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("password", "wrong");
    properties.setProperty("token", new RawChargewire(server).token());
    try (Connection connection = DriverManager.getConnection(server.url(""), properties)) {
      assertTrue(connection.isValid(2));
    }
  }

  @Test
  void aForgedTokenIsRefused() {
    ChargewireServer server = SharedServer.get();
    Properties properties = new Properties();
    properties.setProperty("token", "forged");
    properties.setProperty("useEncryption", "false");
    SQLException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("invalid_token", ChargewireProblem.codeOf(error));
  }

  @Test
  void nothingListeningIsAConnectionError() {
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () ->
                DriverManager.getConnection(
                    "jdbc:chargewire://127.0.0.1:1?useEncryption=false&user=u&password=p"));
    assertEquals("08001", error.getSQLState());
  }

  @Test
  void aServerThatIsNotChargewireIsRefused() throws SQLException {
    ChargewireServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("basePath", "/chargewire/v1/health");
    SQLException error =
        assertThrows(
            SQLException.class, () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("08001", error.getSQLState(), error.getMessage());
  }

  @Test
  void plainHttpToATlsDefaultFails() {
    ChargewireServer server = SharedServer.get();
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
    ChargewireServer server = SharedServer.get();
    Properties properties = server.properties();
    properties.setProperty("schema", "from_url");
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:chargewire://127.0.0.1:" + server.port + "/memory", properties)) {
      assertEquals("memory", connection.getCatalog());
      assertEquals("from_url", connection.getSchema());
    }
  }

  @Test
  void aMissingCatalogFailsToConnect() {
    ChargewireServer server = SharedServer.get();
    SQLException error =
        assertThrows(
            SQLException.class,
            () ->
                DriverManager.getConnection(
                    "jdbc:chargewire://127.0.0.1:" + server.port + "/no_such_catalog",
                    server.properties()));
    assertEquals("invalid_option_value", ChargewireProblem.codeOf(error));
  }

  /**
   * A key the driver has no property for is never silently ignored: refused in the URL, and in the
   * Properties when it resembles one of the driver's or names TLS or signing in; anything else in
   * the Properties (frameworks put their own there) connects, with a warning.
   */
  @Test
  void unknownConnectionPropertiesAreRefusedOrReported() throws SQLException {
    ChargewireServer server = SharedServer.get();
    SQLException inUrl =
        assertThrows(SQLException.class, () -> SharedServer.connect("resultPartition=4"));
    assertEquals("08001", inUrl.getSQLState());
    assertEquals(
        "Unknown connection property 'resultPartition' in the URL: did you mean"
            + " 'resultPartitions'?",
        inUrl.getMessage());

    Properties misspelled = new Properties();
    misspelled.setProperty("tlsRootCert", "/etc/ssl/ca.pem");
    SQLException inProperties =
        assertThrows(SQLException.class, () -> SharedServer.connect(misspelled));
    assertEquals("08001", inProperties.getSQLState());
    assertTrue(
        inProperties.getMessage().contains("did you mean 'tlsRootCerts'?"),
        inProperties.getMessage());
    assertTrue(!inProperties.getMessage().contains("/etc/ssl"), "no value in the message");

    Properties anotherDrivers = new Properties();
    anotherDrivers.setProperty("sslmode", "verify-full");
    SQLException security =
        assertThrows(SQLException.class, () -> SharedServer.connect(anotherDrivers));
    assertEquals("08001", security.getSQLState());
    assertTrue(security.getMessage().contains("it names TLS or signing in"), security.getMessage());

    Properties foreign = new Properties();
    foreign.setProperty("ApplicationName", "etl");
    foreign.setProperty("fetchsize", "1000");
    try (Connection connection = SharedServer.connect(foreign);
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 42")) {
      assertTrue(rows.next());
      java.sql.SQLWarning warning = connection.getWarnings();
      assertNotNull(warning);
      assertEquals("01S00", warning.getSQLState());
      assertEquals(
          "The connection property 'ApplicationName' is not one of this driver's and was ignored"
              + " (ChargewireProperty lists them)",
          warning.getMessage());
      assertTrue(warning.getNextWarning().getMessage().contains("'fetchsize'"));
      assertNull(warning.getNextWarning().getNextWarning());
    }
    try (Connection connection =
        DriverManager.getConnection(server.url("resultPartitions=4"), server.properties())) {
      assertNull(connection.getWarnings(), "the driver's own properties raise none");
    }
  }

  /**
   * A catalog and a schema given together must both land: the schema is one of that catalog, so the
   * catalog has to be applied first. Whether it is must not depend on the order the options were
   * written in, nor on chance (the Go driver applied them in a map's random order, and failed about
   * 33 connects in 40), so each form connects 40 times.
   */
  @Test
  void aCatalogAndASchemaGivenTogetherBothLandOnEveryConnect() throws SQLException {
    ChargewireServer server = SharedServer.get();
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("ATTACH IF NOT EXISTS ':memory:' AS together");
      statement.execute("CREATE SCHEMA IF NOT EXISTS together.reports");
    }
    String base = "jdbc:chargewire://127.0.0.1:" + server.port;
    Properties schemaFirst = server.properties();
    schemaFirst.setProperty("schema", "reports");
    schemaFirst.setProperty("catalog", "together");
    Properties schemaOnly = server.properties();
    schemaOnly.setProperty("schema", "reports");
    Object[][] forms = {
      {base + "/together?schema=reports", server.properties()},
      {base + "?schema=reports&catalog=together", server.properties()},
      {base + "?catalog=together&schema=reports", server.properties()},
      {base + ";schema=reports;catalog=together", server.properties()},
      {base, schemaFirst},
      {base + "/together", schemaOnly},
    };
    for (Object[] form : forms) {
      for (int attempt = 0; attempt < 40; attempt++) {
        try (Connection connection =
                DriverManager.getConnection((String) form[0], (Properties) form[1]);
            Statement statement = connection.createStatement();
            ResultSet rows =
                statement.executeQuery("SELECT current_catalog() || '.' || current_schema()")) {
          assertTrue(rows.next());
          assertEquals("together.reports", rows.getString(1), form[0] + ", connect " + attempt);
          assertEquals("together", connection.getCatalog());
          assertEquals("reports", connection.getSchema());
        }
      }
    }
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
  void schemaChangedInSqlIsSeenByGetSchema() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS used_in_sql");
      statement.execute("USE memory.used_in_sql");
      assertEquals("used_in_sql", connection.getSchema());
    }
  }

  @Test
  void theTransactionStateIsTheSessionDocuments() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      ChargewireConnection chargewire = connection.unwrap(ChargewireConnection.class);
      assertEquals(ChargewireConnection.TransactionState.NONE, chargewire.getTransactionState());
      statement.execute("BEGIN TRANSACTION");
      assertEquals(ChargewireConnection.TransactionState.OPEN, chargewire.getTransactionState());
      assertThrows(SQLException.class, () -> statement.execute("SELECT error('boom')"));
      assertEquals(
          ChargewireConnection.TransactionState.ABORTED,
          chargewire.getTransactionState(),
          "the engine refuses every statement until a rollback");
      statement.execute("ROLLBACK");
      assertEquals(ChargewireConnection.TransactionState.NONE, chargewire.getTransactionState());
      connection.setAutoCommit(false);
      assertEquals(ChargewireConnection.TransactionState.OPEN, chargewire.getTransactionState());
      connection.commit();
      assertEquals(
          ChargewireConnection.TransactionState.OPEN,
          chargewire.getTransactionState(),
          "with autocommit off a commit starts the next transaction");
      connection.setAutoCommit(true);
      assertEquals(ChargewireConnection.TransactionState.NONE, chargewire.getTransactionState());
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
  void setReadOnlyIsEnforcedAndOneWay() throws SQLException {
    try (Connection writer = SharedServer.connect();
        Statement setup = writer.createStatement();
        Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      setup.execute("CREATE OR REPLACE TABLE read_only_client (i INTEGER)");
      assertFalse(connection.isReadOnly());
      connection.setReadOnly(true);
      assertTrue(connection.isReadOnly());
      assertNull(connection.getWarnings());
      SQLException refused =
          assertThrows(
              SQLException.class,
              () -> statement.executeUpdate("INSERT INTO read_only_client VALUES (1)"));
      assertEquals("25006", refused.getSQLState());
      try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM read_only_client")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getLong(1));
      }
      assertEquals(
          "25006",
          assertThrows(SQLException.class, () -> connection.setReadOnly(false)).getSQLState());
      connection.setReadOnly(true);
      assertEquals(1, setup.executeUpdate("INSERT INTO read_only_client VALUES (2)"));
      setup.execute("DROP TABLE read_only_client");
    }
  }

  @Test
  void readOnlyPropertyAppliesFromCreation() throws SQLException {
    try (Connection connection = SharedServer.connect("readOnly=true");
        Statement statement = connection.createStatement()) {
      assertTrue(connection.isReadOnly());
      try (ResultSet rows = statement.executeQuery("SELECT 42 AS answer")) {
        assertTrue(rows.next());
        assertEquals(42, rows.getInt(1));
      }
      assertEquals(
          "25006",
          assertThrows(
                  SQLException.class,
                  () -> statement.execute("CREATE TABLE readonly_property_probe (i INTEGER)"))
              .getSQLState());
    }
  }

  @Test
  void sqlReadOnlyTransactionStillRefusesWrites() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE read_only_transaction (i INTEGER)");
      statement.execute("BEGIN TRANSACTION READ ONLY");
      assertThrows(
          SQLException.class,
          () -> statement.executeUpdate("INSERT INTO read_only_transaction VALUES (1)"));
      statement.execute("ROLLBACK");
      try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM read_only_transaction")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getLong(1));
      }
      statement.execute("DROP TABLE read_only_transaction");
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
      assertTrue(connection.isWrapperFor(ChargewireConnection.class));
      ChargewireConnection chargewire = connection.unwrap(ChargewireConnection.class);
      assertSame(connection, chargewire);
      assertTrue(chargewire.getSessionId().startsWith("s_"), chargewire.getSessionId());
      assertThrows(SQLException.class, () -> connection.unwrap(String.class));
    }
  }

  @Test
  void dataSourceConnects() throws SQLException {
    ChargewireServer server = SharedServer.get();
    ChargewireDataSource dataSource = new ChargewireDataSource();
    dataSource.setUrl(server.url("useEncryption=false"));
    dataSource.setUser(ChargewireServer.USER);
    dataSource.setPassword(server.password);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1")) {
      assertTrue(rows.next());
    }
    try (Connection connection = dataSource.getConnection(ChargewireServer.USER, server.password)) {
      assertTrue(connection.isValid(2));
    }
    assertThrows(
        SQLException.class, () -> dataSource.getConnection(ChargewireServer.USER, "wrong"));
  }
}
