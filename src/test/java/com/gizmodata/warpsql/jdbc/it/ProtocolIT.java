package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpConnection;
import com.gizmodata.warpsql.jdbc.WarpProblem;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * The protocol behaviours behind the JDBC calls: cancellation, timeouts, the asynchronous path,
 * compression, resumption of retained results, incomplete streams and lost sessions (spec §5–§8).
 */
class ProtocolIT {
  /** Runs until stopped. */
  private static final String ENDLESS = "SELECT count(*) FROM range(100000000000) t(i)";

  /**
   * Runs a statement and reads its result: servers send the schema at once (spec §6.4), so a
   * statement's failure can surface either from {@code execute} or from {@code next}.
   */
  private static void runToEnd(Statement statement, String sql) throws SQLException {
    if (statement.execute(sql)) {
      try (ResultSet rows = statement.getResultSet()) {
        while (rows.next()) {
          rows.getObject(1);
        }
      }
    }
  }

  /** About 2 million rows in several batches; the sum of i is 1999999000000. */
  private static final String BIG = "SELECT i, i * 2 AS j FROM range(2000000) t(i)";

  private static void assertBigResult(ResultSet rows) throws SQLException {
    long count = 0;
    long sum = 0;
    while (rows.next()) {
      count++;
      sum += rows.getLong(1);
      if (rows.getLong(2) != rows.getLong(1) * 2) {
        throw new AssertionError("row " + count + " is wrong");
      }
    }
    assertEquals(2_000_000, count);
    assertEquals(1_999_999_000_000L, sum);
  }

  // ---- Cancel and timeout (spec §6.8, §6.9) ----

  @Test
  void cancelStopsARunningStatement() throws Exception {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<SQLException> running =
            executor.submit(
                () -> assertThrows(SQLException.class, () -> runToEnd(statement, ENDLESS)));
        Thread.sleep(700);
        statement.cancel();
        SQLException error = running.get();
        assertEquals("statement_cancelled", WarpProblem.codeOf(error), error.toString());
        assertEquals("57014", error.getSQLState());
      } finally {
        executor.shutdownNow();
      }
      try (ResultSet rows = statement.executeQuery("SELECT 1")) {
        assertTrue(rows.next(), "the statement is usable after a cancel");
      }
    }
  }

  @Test
  void cancelInAsyncMode() throws Exception {
    try (Connection connection = SharedServer.connect("async=true");
        Statement statement = connection.createStatement()) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<SQLException> running =
            executor.submit(
                () -> assertThrows(SQLException.class, () -> runToEnd(statement, ENDLESS)));
        Thread.sleep(700);
        statement.cancel();
        assertEquals("statement_cancelled", WarpProblem.codeOf(running.get()));
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void queryTimeoutIsEnforcedByTheServer() throws SQLException {
    for (String mode : List.of("", "async=true")) {
      try (Connection connection = SharedServer.connect(mode);
          Statement statement = connection.createStatement()) {
        statement.setQueryTimeout(1);
        assertEquals(1, statement.getQueryTimeout());
        SQLTimeoutException error =
            assertTimeoutPreemptively(
                Duration.ofSeconds(30),
                () -> assertThrows(SQLTimeoutException.class, () -> runToEnd(statement, ENDLESS)));
        assertEquals("statement_timeout", WarpProblem.codeOf(error), mode);
        statement.setQueryTimeout(0);
        try (ResultSet rows = statement.executeQuery("SELECT 1")) {
          assertTrue(rows.next());
        }
      }
    }
  }

  // ---- The asynchronous path and compression (spec §6.1, §6.3, §8.2) ----

  @Test
  void asyncModeSubmitsPollsAndFetches() throws SQLException {
    try (Connection connection = SharedServer.connect("async=true&pollWait=1");
        Statement statement = connection.createStatement()) {
      try (ResultSet rows = statement.executeQuery(BIG)) {
        assertBigResult(rows);
      }
      assertFalse(statement.execute("CREATE TEMP TABLE async_t AS SELECT * FROM range(10) t(i)"));
      assertEquals(10, statement.executeUpdate("INSERT INTO async_t SELECT * FROM range(10)"));
      try (PreparedStatement select =
          connection.prepareStatement("SELECT sum(i) FROM async_t WHERE i < ?")) {
        select.setInt(1, 5);
        try (ResultSet rows = select.executeQuery()) {
          assertTrue(rows.next());
          assertEquals(20, rows.getLong(1));
        }
      }
    }
  }

  /**
   * About 2 million rows of what compresses well: few distinct values. (The server sends a buffer
   * LZ4 would shrink by less than about half uncompressed, which BIG's consecutive integers are.)
   */
  private static final String COMPRESSIBLE =
      "SELECT i // 1000 AS g, 'status-' || (i % 3) AS s FROM range(2000000) t(i)";

  @Test
  void compressionShrinksResults() throws Exception {
    WarpServer server = SharedServer.get();
    String[] modes = {"none", "zstd", "lz4_frame"};
    long[] bytes = new long[modes.length];
    for (int i = 0; i < modes.length; i++) {
      try (ChaosProxy counter = new ChaosProxy(server.port, "$^", 0, 0);
          Connection connection =
              DriverManager.getConnection(
                  "jdbc:warp://127.0.0.1:" + counter.port() + "?ipcCompression=" + modes[i],
                  server.properties());
          Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery(COMPRESSIBLE)) {
        long count = 0;
        long sum = 0;
        while (rows.next()) {
          count++;
          sum += rows.getLong(1);
          if (!rows.getString(2).startsWith("status-")) {
            throw new AssertionError("row " + count + ": " + rows.getString(2));
          }
        }
        assertEquals(2_000_000, count);
        assertEquals(1_999_000_000L, sum);
        bytes[i] = counter.bytesFromServer();
      }
    }
    assertTrue(bytes[1] * 2 < bytes[0], "zstd " + bytes[1] + " bytes vs plain " + bytes[0]);
    assertTrue(bytes[2] * 2 < bytes[0], "lz4 " + bytes[2] + " bytes vs plain " + bytes[0]);
  }

  /** About 12 batches of 122,880 rows: enough for the server to split it four ways. */
  private static final long SPLIT_ROWS = 1_500_000;

  /**
   * Rows that exercise compression: {@code i} compresses well, {@code h} (53 random bits as a
   * DOUBLE) not at all, so the server may send its buffers uncompressed inside a compressed stream
   * (spec §8.2), and {@code n} and {@code s} have NULLs.
   */
  private static final String MIXED =
      "SELECT i, (hash(i) >> 11)::DOUBLE AS h,"
          + " CASE WHEN i % 5 = 0 THEN NULL ELSE i * 3 END AS n,"
          + " CASE WHEN i % 7 = 0 THEN NULL ELSE 'v' || (i % 1000) END AS s"
          + " FROM range("
          + SPLIT_ROWS
          + ") t(i) ORDER BY i";

  private static final java.util.regex.Pattern PARTITION_FETCH =
      java.util.regex.Pattern.compile("GET /warp/v1/sessions/\\S+/result\\?partition=\\d+");

  /**
   * A result split into partitions arrives compressed with the codec asked for, in every
   * partition's response, and decodes to exactly the rows the server computed.
   */
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"lz4_frame", "zstd"})
  void compressedPartitionsDecodeExactly(String codec) throws Exception {
    WarpServer server = SharedServer.get();
    long expectedBits;
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT bit_xor(hash(i) >> 11) FROM range(" + SPLIT_ROWS + ") t(i)")) {
      assertTrue(rows.next());
      expectedBits = rows.getLong(1);
    }
    try (ChaosProxy proxy = new ChaosProxy(server.port, "$^", 0, 0);
        Connection connection =
            DriverManager.getConnection(
                "jdbc:warp://127.0.0.1:"
                    + proxy.port()
                    + "?resultPartitions=4&ipcCompression="
                    + codec,
                server.properties());
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery(MIXED)) {
      long expected = 0;
      long bits = 0;
      while (rows.next()) {
        long i = rows.getLong(1);
        if (i != expected) {
          throw new AssertionError("row " + expected + " has i = " + i);
        }
        double h = rows.getDouble(2);
        bits ^= (long) h;
        long n = rows.getLong(3);
        if (rows.wasNull() != (i % 5 == 0) || (i % 5 != 0 && n != i * 3)) {
          throw new AssertionError("row " + i + " has n = " + rows.getObject(3));
        }
        String text = rows.getString(4);
        if ((text == null) != (i % 7 == 0) || (text != null && !text.equals("v" + (i % 1000)))) {
          throw new AssertionError("row " + i + " has s = " + text);
        }
        expected++;
      }
      assertEquals(SPLIT_ROWS, expected, "every row");
      assertEquals(expectedBits, bits, "the incompressible column came through bit for bit");
      long fetched =
          proxy.requests().stream().filter(r -> PARTITION_FETCH.matcher(r).matches()).count();
      assertTrue(fetched >= 1, "the result was not split: " + proxy.requests());
      List<String> asked = proxy.requestCompression();
      assertTrue(
          asked.size() >= fetched + 1 && asked.stream().allMatch(codec::equals),
          "requests asked for " + asked);
      List<String> used = proxy.responseCompression();
      assertEquals(
          fetched + 1,
          used.stream().filter(codec::equals).count(),
          "every partition's response says " + codec + ": " + used);
    }
  }

  // ---- Retention and resumption (spec §6.5, §6.6) ----

  @Test
  void retainedResultResumesAfterTheConnectionBreaks() throws Exception {
    WarpServer server = SharedServer.get();
    try (ChaosProxy proxy =
            new ChaosProxy(server.port, "GET /warp/v1/sessions/\\S+/result", 300_000, 1);
        Connection connection =
            DriverManager.getConnection(
                "jdbc:warp://127.0.0.1:" + proxy.port() + "?async=true", server.properties());
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery(BIG)) {
      assertBigResult(rows);
      assertEquals(1, proxy.cuts(), "the proxy broke the first result download");
    }
  }

  @Test
  void retainedStreamedResultResumesAfterTheConnectionBreaks() throws Exception {
    WarpServer server = SharedServer.get();
    try (ChaosProxy proxy =
            new ChaosProxy(server.port, "POST /warp/v1/sessions/\\S+/statements", 300_000, 1);
        Connection connection =
            DriverManager.getConnection(
                "jdbc:warp://127.0.0.1:" + proxy.port() + "?retainResults=true",
                server.properties());
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery(BIG)) {
      assertBigResult(rows);
      assertEquals(1, proxy.cuts());
    }
  }

  @Test
  void unretainedStreamedResultFailsWhenTheConnectionBreaks() throws Exception {
    WarpServer server = SharedServer.get();
    try (ChaosProxy proxy =
            new ChaosProxy(server.port, "POST /warp/v1/sessions/\\S+/statements", 300_000, 1);
        Connection connection =
            DriverManager.getConnection(
                "jdbc:warp://127.0.0.1:" + proxy.port(), server.properties());
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(
              SQLException.class,
              () -> {
                try (ResultSet rows = statement.executeQuery(BIG)) {
                  assertBigResult(rows);
                }
              });
      assertEquals("08006", error.getSQLState(), error.toString());
      assertTrue(error.getMessage().contains("retainResults"), error.getMessage());
      assertEquals(1, proxy.cuts());
      try (ResultSet rows = statement.executeQuery("SELECT 42")) {
        assertTrue(rows.next(), "the connection still works");
        assertEquals(42, rows.getInt(1));
      }
    }
  }

  @Test
  void closingAResultEarlyLetsAWriteFinish() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE early_close (i BIGINT)");
      try (ResultSet rows =
          statement.executeQuery(
              "INSERT INTO early_close SELECT i FROM range(2000000) t(i) RETURNING i")) {
        assertTrue(rows.next());
      }
      try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM early_close")) {
        assertTrue(rows.next());
        assertEquals(2_000_000, rows.getLong(1), "a write runs to its end (spec §6.8)");
      }
      statement.execute("DROP TABLE early_close");
    }
  }

  @Test
  void failureMidStreamRaisesTheStatementError() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      // The engine may fail before or after the first batch; either way the stream is
      // incomplete and the statement's own error is raised, promptly.
      SQLException error =
          assertTimeoutPreemptively(
              Duration.ofSeconds(5),
              () ->
                  assertThrows(
                      SQLException.class,
                      () -> {
                        try (ResultSet rows =
                            statement.executeQuery(
                                "SELECT CASE WHEN i < 300000 THEN i ELSE error('boom') END AS v"
                                    + " FROM range(1000000) t(i)")) {
                          while (rows.next()) {
                            rows.getLong(1);
                          }
                        }
                      }));
      assertEquals("execution_error", WarpProblem.codeOf(error));
      assertTrue(error.getMessage().contains("boom"), error.getMessage());
    }
  }

  @Test
  void resultsStreamWithoutLoadingEverything() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT i, repeat('x', 100) AS pad FROM range(20000000) t(i)")) {
      // About 2 GB of text in all: reading it only works batch by batch.
      long count = 0;
      while (rows.next() && count < 5_000_000) {
        count++;
      }
      assertEquals(5_000_000, count);
    }
  }

  @Test
  void closingAResultEarlyLeavesTheConnectionUsable() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 5; i++) {
        try (ResultSet rows = statement.executeQuery("SELECT * FROM range(100000000) t(i)")) {
          for (int r = 0; r < 10; r++) {
            assertTrue(rows.next());
          }
        }
      }
      assertTimeoutPreemptively(
          Duration.ofSeconds(10),
          () -> {
            try (ResultSet rows = statement.executeQuery("SELECT 7")) {
              assertTrue(rows.next());
              assertEquals(7, rows.getInt(1));
            }
          });
    }
  }

  @Test
  void maxRowsLimitsTheRowsRead() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.setMaxRows(3);
      assertEquals(3, statement.getMaxRows());
      try (ResultSet rows = statement.executeQuery("SELECT * FROM range(1000000) t(i)")) {
        int count = 0;
        while (rows.next()) {
          count++;
        }
        assertEquals(3, count);
      }
      statement.setMaxRows(0);
      try (ResultSet rows = statement.executeQuery("SELECT * FROM range(10) t(i)")) {
        int count = 0;
        while (rows.next()) {
          count++;
        }
        assertEquals(10, count);
      }
    }
  }

  @Test
  void closeOnCompletionClosesTheStatement() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      Statement statement = connection.createStatement();
      ResultSet rows = statement.executeQuery("SELECT * FROM range(5) t(i)");
      statement.closeOnCompletion();
      assertTrue(statement.isCloseOnCompletion());
      while (rows.next()) {
        assertFalse(statement.isClosed());
      }
      assertTrue(statement.isClosed());
      assertTrue(rows.isClosed());

      Statement another = connection.createStatement();
      another.setMaxRows(2);
      another.closeOnCompletion();
      ResultSet limited = another.executeQuery("SELECT * FROM range(5) t(i)");
      while (limited.next()) {
        assertFalse(another.isClosed());
      }
      assertTrue(another.isClosed(), "max rows ends the result too");

      Statement kept = connection.createStatement();
      kept.setMaxRows(2);
      ResultSet keptRows = kept.executeQuery("SELECT * FROM range(5) t(i)");
      while (keptRows.next()) {
        // Read to the end.
      }
      assertFalse(kept.isClosed(), "without closeOnCompletion the statement stays open");
      kept.close();
    }
  }

  @Test
  void manyStatementsRunConcurrentlyOnOneConnection() throws Exception {
    try (Connection connection = SharedServer.connect()) {
      ExecutorService executor = Executors.newFixedThreadPool(8);
      try {
        List<Future<Long>> sums = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
          int offset = t;
          sums.add(
              executor.submit(
                  () -> {
                    try (Statement statement = connection.createStatement();
                        ResultSet rows =
                            statement.executeQuery(
                                "SELECT i + " + offset + " FROM range(200000) t(i)")) {
                      long sum = 0;
                      while (rows.next()) {
                        sum += rows.getLong(1);
                      }
                      return sum;
                    }
                  }));
        }
        for (int t = 0; t < 8; t++) {
          assertEquals(199_999L * 200_000 / 2 + 200_000L * t, sums.get(t).get());
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  // ---- Several results open on one session ----

  @Test
  void nestedQueriesOnOneConnection() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement outer = connection.createStatement();
        Statement inner = connection.createStatement()) {
      assertTimeoutPreemptively(
          Duration.ofSeconds(20),
          () -> {
            long sum = 0;
            try (ResultSet rows = outer.executeQuery("SELECT i FROM range(300000) t(i)")) {
              while (rows.next()) {
                long i = rows.getLong(1);
                if (i % 50000 == 0) {
                  try (ResultSet one = inner.executeQuery("SELECT " + i + " * 2")) {
                    assertTrue(one.next());
                    assertEquals(i * 2, one.getLong(1));
                  }
                }
                sum += i;
              }
            }
            assertEquals(299_999L * 300_000 / 2, sum, "the outer result is complete and in order");
          });
    }
  }

  @Test
  void metadataAndOptionsWhileAResultIsOpen() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      assertTimeoutPreemptively(
          Duration.ofSeconds(20),
          () -> {
            try (ResultSet rows = statement.executeQuery("SELECT i FROM range(1000000) t(i)")) {
              assertTrue(rows.next());
              try (ResultSet catalogs = connection.getMetaData().getCatalogs()) {
                assertTrue(catalogs.next());
              }
              connection.setAutoCommit(false);
              connection.setAutoCommit(true);
              long count = 1;
              while (rows.next()) {
                count++;
              }
              assertEquals(1_000_000, count);
            }
          });
    }
  }

  @Test
  void anAbandonedHugeResultIsDroppedRatherThanBuffered() throws SQLException {
    try (Connection connection = SharedServer.connect("resultBufferSize=1048576");
        Statement abandoned = connection.createStatement();
        Statement next = connection.createStatement()) {
      ResultSet rows = abandoned.executeQuery("SELECT i FROM range(100000000) t(i)");
      assertTrue(rows.next());
      assertTimeoutPreemptively(
          Duration.ofSeconds(20),
          () -> {
            try (ResultSet other = next.executeQuery("SELECT 5")) {
              assertTrue(other.next());
              assertEquals(5, other.getInt(1));
            }
          });
      SQLException error =
          assertThrows(
              SQLException.class,
              () -> {
                while (rows.next()) {
                  rows.getLong(1);
                }
              });
      assertTrue(error.getMessage().contains("resultBufferSize"), error.getMessage());
      rows.close();
    }
  }

  // ---- Lost sessions (spec §5.6) ----

  private static String sessionOf(Connection connection) throws SQLException {
    return connection.unwrap(WarpConnection.class).getSessionId();
  }

  private static void endSessionBehindTheDriversBack(Connection connection) throws Exception {
    assertEquals(204, new RawWarp(SharedServer.get()).deleteSession(sessionOf(connection)));
  }

  private static String warnings(Connection connection) throws SQLException {
    StringBuilder text = new StringBuilder();
    for (SQLWarning warning = connection.getWarnings();
        warning != null;
        warning = warning.getNextWarning()) {
      text.append(warning.getMessage()).append('\n');
    }
    return text.toString();
  }

  @Test
  void lostSessionIsReopenedWithItsOptions() throws Exception {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS lost_options");
      connection.setSchema("lost_options");
      String before = sessionOf(connection);
      endSessionBehindTheDriversBack(connection);
      try (ResultSet rows = statement.executeQuery("SELECT current_schema()")) {
        assertTrue(rows.next());
        assertEquals("lost_options", rows.getString(1), "the schema was replayed");
      }
      assertNotEquals(before, sessionOf(connection));
      assertTrue(
          warnings(connection).contains("no longer had session " + before), warnings(connection));
      assertEquals("01000", connection.getWarnings().getSQLState());
      assertEquals("lost_options", connection.getSchema());
    }
  }

  @Test
  void lostSessionWithAnOpenTransactionFails() throws Exception {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE lost_tx (v INTEGER)");
      connection.setAutoCommit(false);
      statement.execute("INSERT INTO lost_tx VALUES (1)");
      endSessionBehindTheDriversBack(connection);
      SQLTransientConnectionException error =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> statement.execute("INSERT INTO lost_tx VALUES (2)"));
      assertEquals("08007", error.getSQLState());
      assertEquals("session_not_found", WarpProblem.codeOf(error));
      assertFalse(connection.getAutoCommit(), "autocommit was replayed");
      statement.execute("INSERT INTO lost_tx VALUES (3)");
      connection.commit();
      try (ResultSet rows = statement.executeQuery("SELECT list(v ORDER BY v) FROM lost_tx")) {
        assertTrue(rows.next());
        assertEquals(List.of(3), rows.getObject(1), "only the retried transaction committed");
      }
      connection.setAutoCommit(true);
      statement.execute("DROP TABLE lost_tx");
    }
  }

  @Test
  void preparedStatementsSurviveALostSession() throws Exception {
    try (Connection connection = SharedServer.connect();
        PreparedStatement select = connection.prepareStatement("SELECT ? * 2")) {
      select.setInt(1, 21);
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(42, rows.getInt(1));
      }
      endSessionBehindTheDriversBack(connection);
      select.setInt(1, 50);
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(100, rows.getInt(1), "prepared again in the new session");
      }
    }
  }

  @Test
  void isValidReopensALostSession() throws Exception {
    try (Connection connection = SharedServer.connect()) {
      endSessionBehindTheDriversBack(connection);
      assertTrue(connection.isValid(5));
      assertNotNull(connection.getWarnings());
    }
  }

  @Test
  void closeEndsTheServerSession() throws Exception {
    Connection connection = SharedServer.connect();
    String session = sessionOf(connection);
    RawWarp raw = new RawWarp(SharedServer.get());
    assertEquals(200, raw.get("/warp/v1/sessions/" + session).statusCode());
    connection.close();
    assertTrue(connection.isClosed());
    assertFalse(connection.isValid(1));
    assertEquals(404, raw.get("/warp/v1/sessions/" + session).statusCode());
    assertEquals(
        "08003", assertThrows(SQLException.class, connection::createStatement).getSQLState());
  }

  // ---- The JVM the driver runs in ----

  @Test
  void needsNoAddOpens() throws SQLException {
    List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    assertTrue(
        arguments.stream().noneMatch(argument -> argument.contains("add-opens")),
        "the tests must run without --add-opens: " + arguments);
    Properties zstd = new Properties();
    zstd.setProperty("ipcCompression", "zstd");
    try (Connection connection = SharedServer.connect(zstd);
        PreparedStatement select =
            connection.prepareStatement(
                "SELECT ?::DECIMAL(10,2) AS d, ?::BLOB AS b, [1, 2] AS l")) {
      select.setBigDecimal(1, new java.math.BigDecimal("1.25"));
      select.setBytes(2, new byte[] {1, 2, 3});
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(new java.math.BigDecimal("1.25"), rows.getBigDecimal(1));
        assertEquals(3, rows.getBytes(2).length);
        assertEquals(List.of(1, 2), rows.getObject(3));
      }
    }
  }
}
