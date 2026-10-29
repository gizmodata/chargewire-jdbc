package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.gizmodata.chargewire.jdbc.ChargewireProblem;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The driver against a server from before the protocol's rename, reached through today's URL and
 * default base path, and today's server never asked for the former root.
 *
 * <p>The former server is {@code CHARGEWIRE_JDBC_TEST_FORMER_SERVER_BINARY}, or else a sibling
 * checkout's {@code ../warp-sql-preRename/cpp/build/warp-duckdb}. Its tests are skipped only when
 * neither exists.
 */
class LegacyDialectIT {
  private static final String BINARY_VARIABLE = "CHARGEWIRE_JDBC_TEST_FORMER_SERVER_BINARY";
  private static final String ENDLESS = "SELECT count(*) FROM range(100000000000) t(i)";

  private static ChargewireServer former;

  private static Path formerBinary() {
    String configured = System.getenv(BINARY_VARIABLE);
    if (configured != null && !configured.isEmpty()) {
      return Path.of(configured);
    }
    return Path.of(System.getProperty("user.dir"), "..", "warp-sql-preRename", "cpp", "build")
        .resolve("warp-duckdb")
        .normalize();
  }

  @BeforeAll
  static void startFormerServer() {
    Path binary = formerBinary();
    if (Files.isExecutable(binary)) {
      former = ChargewireServer.startBinary(binary, "warp", "--username", "WARP_DUCKDB_PASSWORD");
    }
  }

  @AfterAll
  static void stopFormerServer() {
    if (former != null) {
      former.close();
    }
  }

  private static Connection connectFormer(String options) throws SQLException {
    assumeTrue(former != null, "no former server at " + formerBinary());
    return DriverManager.getConnection(former.url(options), former.properties());
  }

  @Test
  void connectsAndQueries() throws SQLException {
    try (Connection connection = connectFormer("");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 40 + 2 AS answer, 'ok' AS word")) {
      assertTrue(connection.isValid(2));
      assertEquals("warp-duckdb", connection.getMetaData().getDatabaseProductName());
      assertTrue(rows.next());
      assertEquals(42, rows.getInt("answer"));
      assertEquals("ok", rows.getString("word"));
      assertFalse(rows.next());
    }
  }

  @Test
  void aFormerUrlReachesAFormerServer() throws SQLException {
    assumeTrue(former != null, "no former server at " + formerBinary());
    String url = "jdbc:warp://127.0.0.1:" + former.port;
    try (Connection connection = DriverManager.getConnection(url, former.properties());
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1")) {
      assertTrue(rows.next());
      assertEquals(1, rows.getInt(1));
    }
  }

  @Test
  void preparedStatementsBindParameters() throws SQLException {
    try (Connection connection = connectFormer("");
        PreparedStatement statement =
            connection.prepareStatement("SELECT ? + 1 AS n, upper(?) AS s")) {
      statement.setInt(1, 41);
      statement.setString(2, "former");
      try (ResultSet rows = statement.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(42, rows.getInt("n"));
        assertEquals("FORMER", rows.getString("s"));
      }
    }
  }

  @Test
  void batchInsertsLand() throws SQLException {
    try (Connection connection = connectFormer("");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE legacy_batch (id INTEGER PRIMARY KEY, name VARCHAR)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO legacy_batch VALUES (?, ?)")) {
        for (int id = 0; id < 500; id++) {
          insert.setInt(1, id);
          insert.setString(2, id % 3 == 0 ? null : "row " + id);
          insert.addBatch();
        }
        assertEquals(500, insert.executeBatch().length);
      }
      try (ResultSet rows =
          statement.executeQuery("SELECT count(*), count(name), max(id) FROM legacy_batch")) {
        assertTrue(rows.next());
        assertEquals(500, rows.getInt(1));
        assertEquals(333, rows.getInt(2));
        assertEquals(499, rows.getInt(3));
      }
    }
  }

  @Test
  void sqlErrorsMapToTheirSqlState() throws SQLException {
    try (Connection connection = connectFormer("");
        Statement statement = connection.createStatement()) {
      SQLSyntaxErrorException missing =
          assertThrows(
              SQLSyntaxErrorException.class,
              () -> statement.executeQuery("SELECT * FROM nowhere_at_all"));
      assertEquals("sql_error", ChargewireProblem.codeOf(missing), missing.toString());
      assertEquals("42", missing.getSQLState().substring(0, 2), missing.toString());
      try (ResultSet rows = statement.executeQuery("SELECT 1")) {
        assertTrue(rows.next(), "the connection still works");
      }
    }
  }

  @Test
  void queryTimeoutReachesTheServer() throws SQLException {
    try (Connection connection = connectFormer("");
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(1);
      SQLTimeoutException error =
          assertTimeoutPreemptively(
              Duration.ofSeconds(30),
              () ->
                  assertThrows(
                      SQLTimeoutException.class,
                      () -> {
                        try (ResultSet rows = statement.executeQuery(ENDLESS)) {
                          while (rows.next()) {
                            rows.getObject(1);
                          }
                        }
                      }));
      assertEquals("statement_timeout", ChargewireProblem.codeOf(error), error.toString());
    }
  }

  @Test
  void partitionedResultsAreRead() throws SQLException {
    try (Connection connection = connectFormer("resultPartitions=4");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM range(300000) t(i)")) {
      long count = 0;
      long sum = 0;
      while (rows.next()) {
        count++;
        sum += rows.getLong(1);
      }
      assertEquals(300000, count);
      assertEquals(300000L * 299999 / 2, sum);
    }
  }

  @Test
  void tablesAndColumnsWithDefaults() throws SQLException {
    try (Connection connection = connectFormer("");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS legacy_meta"
              + " (id INTEGER DEFAULT 42, name VARCHAR NOT NULL, note VARCHAR)");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet tables = metaData.getTables(null, null, "legacy_meta", null)) {
        assertTrue(tables.next());
        assertEquals("legacy_meta", tables.getString("TABLE_NAME"));
      }
      Map<String, String> defaults = new HashMap<>();
      try (ResultSet columns = metaData.getColumns(null, null, "legacy_meta", null)) {
        while (columns.next()) {
          defaults.put(columns.getString("COLUMN_NAME"), columns.getString("COLUMN_DEF"));
        }
      }
      assertEquals(Set.of("id", "name", "note"), defaults.keySet());
      assertNotNull(defaults.get("id"), defaults.toString());
      assertTrue(defaults.get("id").contains("42"), defaults.toString());
    }
  }

  // ---- Today's server ----

  @Test
  void todaysServerIsNeverAskedForTheFormerRoot() throws Exception {
    ChargewireServer server = SharedServer.get();
    try (RecordingProxy proxy = new RecordingProxy(server.port)) {
      // ipcCompression=none: auto times discovery twice over a slow link, and the proxy is one.
      String url =
          "jdbc:chargewire://127.0.0.1:" + proxy.port() + "?resultPartitions=2&ipcCompression=none";
      try (Connection connection = DriverManager.getConnection(url, server.properties());
          Statement statement = connection.createStatement()) {
        statement.setQueryTimeout(30);
        try (ResultSet rows = statement.executeQuery("SELECT 1")) {
          assertTrue(rows.next());
        }
        try (PreparedStatement prepared = connection.prepareStatement("SELECT ? AS v")) {
          prepared.setInt(1, 7);
          try (ResultSet rows = prepared.executeQuery()) {
            assertTrue(rows.next());
          }
        }
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM nowhere"));
        try (ResultSet columns = connection.getMetaData().getColumns(null, null, "%", null)) {
          columns.next();
        }
      }
      List<String> paths = proxy.paths();
      assertTrue(paths.size() > 5, paths.toString());
      assertTrue(paths.stream().allMatch(p -> p.contains(" /chargewire")), paths.toString());
      assertTrue(
          paths.stream().noneMatch(p -> p.toLowerCase(Locale.ROOT).contains("warp")),
          paths.toString());
      assertEquals(1, paths.stream().filter(p -> p.equals("GET /chargewire")).count());
    }
  }

  /** Forwards HTTP to a server and records each request's method and path. */
  private static final class RecordingProxy implements AutoCloseable {
    private static final Set<String> UNFORWARDED =
        Set.of("connection", "content-length", "expect", "host", "upgrade", "transfer-encoding");

    private final HttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();
    private final List<String> paths = Collections.synchronizedList(new ArrayList<>());
    private final int target;

    RecordingProxy(int target) throws IOException {
      this.target = target;
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.setExecutor(Executors.newCachedThreadPool());
      server.createContext("/", this::forward);
      server.start();
    }

    int port() {
      return server.getAddress().getPort();
    }

    List<String> paths() {
      synchronized (paths) {
        return new ArrayList<>(paths);
      }
    }

    private void forward(HttpExchange exchange) throws IOException {
      URI uri = exchange.getRequestURI();
      paths.add(exchange.getRequestMethod() + " " + uri.getPath());
      byte[] body = exchange.getRequestBody().readAllBytes();
      HttpRequest.Builder request =
          HttpRequest.newBuilder(
                  URI.create(
                      "http://127.0.0.1:"
                          + target
                          + uri.getRawPath()
                          + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery())))
              .method(
                  exchange.getRequestMethod(),
                  body.length == 0
                      ? HttpRequest.BodyPublishers.noBody()
                      : HttpRequest.BodyPublishers.ofByteArray(body));
      exchange
          .getRequestHeaders()
          .forEach(
              (name, values) -> {
                if (!UNFORWARDED.contains(name.toLowerCase(Locale.ROOT))) {
                  values.forEach(value -> request.header(name, value));
                }
              });
      HttpResponse<byte[]> response;
      try {
        response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      response
          .headers()
          .map()
          .forEach(
              (name, values) -> {
                if (!UNFORWARDED.contains(name.toLowerCase(Locale.ROOT)) && !name.startsWith(":")) {
                  exchange.getResponseHeaders().put(name, values);
                }
              });
      byte[] answer = response.body();
      exchange.sendResponseHeaders(response.statusCode(), answer.length == 0 ? -1 : answer.length);
      if (answer.length > 0) {
        exchange.getResponseBody().write(answer);
      }
      exchange.close();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
