package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpConnection;
import com.gizmodata.warpsql.jdbc.WarpProblem;
import com.gizmodata.warpsql.jdbc.WarpStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Results split into partitions and read over several connections at once (spec §6.10): order,
 * completeness, types, empty partitions, failure, cancel, early close, the asynchronous path and
 * resumption of a retained partition. Every test goes through a {@link ChaosProxy}, which shows
 * that the result really was split and its partitions read concurrently.
 */
class PartitionsIT {
  private static final long ROWS = 1_000_000;

  /** Far more rows than a test reads: still running when the test acts. */
  private static final String ENDLESS =
      "SELECT i, i % 7 AS m FROM range(3000000000) t(i) WHERE i % 3 = 0";

  private static final Pattern PARTITION_REQUEST =
      Pattern.compile(
          "GET /warp/v1/sessions/([^/]+)/statements/([^/]+)/result\\?partition=(\\d+)(&from_batch=\\d+)?");

  private static ChaosProxy proxy() throws Exception {
    return new ChaosProxy(SharedServer.get().port, "$^", 0, 0);
  }

  private static Connection connect(ChaosProxy proxy, String options) throws SQLException {
    WarpServer server = SharedServer.get();
    String url = "jdbc:warp://127.0.0.1:" + proxy.port();
    return DriverManager.getConnection(
        options.isEmpty() ? url : url + "?" + options, server.properties());
  }

  /** The partitions the driver fetched besides the submission's own stream. */
  private static TreeSet<Integer> partitionsFetched(ChaosProxy proxy) {
    TreeSet<Integer> indexes = new TreeSet<>();
    for (String request : proxy.requests()) {
      Matcher matcher = PARTITION_REQUEST.matcher(request);
      if (matcher.matches()) {
        indexes.add(Integer.parseInt(matcher.group(3)));
      }
    }
    return indexes;
  }

  private static String statementFetched(ChaosProxy proxy) {
    for (String request : proxy.requests()) {
      Matcher matcher = PARTITION_REQUEST.matcher(request);
      if (matcher.matches()) {
        return matcher.group(2);
      }
    }
    throw new AssertionError("no partition was fetched: " + proxy.requests());
  }

  /** Asserts the streamed result was split, and its other partitions read all at once. */
  private static void assertSplitAndConcurrent(ChaosProxy proxy, int asked) {
    TreeSet<Integer> fetched = partitionsFetched(proxy);
    assertFalse(fetched.isEmpty(), "the result was not split: " + proxy.requests());
    assertTrue(fetched.last() < asked, "more partitions than asked: " + fetched);
    assertEquals(fetched.last(), fetched.size(), "partitions 1 to k - 1, each once: " + fetched);
    assertTrue(
        proxy.mostOpenConnections() >= fetched.size() + 1,
        "only "
            + proxy.mostOpenConnections()
            + " connections were open at once for "
            + (fetched.size() + 1)
            + " partitions");
  }

  @Test
  void orderByKeepsItsOrderAcrossPartitions() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=4");
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT i FROM range(" + ROWS + ") t(i) ORDER BY i DESC")) {
      long expected = ROWS - 1;
      while (rows.next()) {
        assertEquals(expected--, rows.getLong(1));
      }
      assertEquals(-1, expected, "every row, in order");
      assertSplitAndConcurrent(proxy, 4);
    }
  }

  @Test
  void anyOrderReturnsEveryRowOnce() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=6&resultOrder=ANY");
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT i, i * 2 AS j FROM range(" + ROWS + ") t(i)")) {
      BitSet seen = new BitSet();
      long count = 0;
      while (rows.next()) {
        int i = rows.getInt(1);
        assertEquals(2L * i, rows.getLong(2));
        assertFalse(seen.get(i), "row " + i + " came twice");
        seen.set(i);
        count++;
      }
      assertEquals(ROWS, count);
      assertEquals(ROWS, seen.cardinality());
      assertSplitAndConcurrent(proxy, 6);
    }
  }

  @Test
  void typesAndNullsSurviveTheSplit() throws Exception {
    String sql =
        "SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END AS n, "
            + "CASE WHEN i % 5 = 0 THEN NULL ELSE 'row ' || i END AS s, i / 3.0 AS d, "
            + "(i % 1000)::DECIMAL(18, 3) AS dec, DATE '2026-01-01' + (i % 400)::INTEGER AS dt, "
            + "TIMESTAMP '2026-01-01' + to_seconds(i) AS ts, i % 2 = 0 AS b, "
            + "[i, NULL, i + 1] AS l, {'a': i, 'b': CASE WHEN i % 3 = 0 THEN NULL ELSE 'x' END}"
            + " AS st, encode('b' || i) AS bin FROM range(300000) t(i) ORDER BY i";
    try (ChaosProxy proxy = proxy();
        Connection whole = SharedServer.connect();
        Connection split = connect(proxy, "resultPartitions=5");
        Statement wholeStatement = whole.createStatement();
        Statement splitStatement = split.createStatement();
        ResultSet expected = wholeStatement.executeQuery(sql);
        ResultSet actual = splitStatement.executeQuery(sql)) {
      int columns = expected.getMetaData().getColumnCount();
      assertEquals(columns, actual.getMetaData().getColumnCount());
      long count = 0;
      long nulls = 0;
      while (expected.next()) {
        assertTrue(actual.next(), "the split result ended early, at row " + count);
        for (int c = 1; c <= columns; c++) {
          Object want = expected.getObject(c);
          Object got = actual.getObject(c);
          if (want instanceof byte[]) {
            assertArrayEquals((byte[]) want, (byte[]) got, "row " + count + " column " + c);
          } else {
            assertTrue(
                Objects.equals(want, got),
                "row " + count + " column " + c + ": " + want + " vs " + got);
          }
          nulls += want == null ? 1 : 0;
        }
        count++;
      }
      assertFalse(actual.next(), "the split result has extra rows");
      assertEquals(300_000, count);
      assertTrue(nulls > 0, "the result has NULLs to check");
      assertSplitAndConcurrent(proxy, 5);
    }
  }

  @Test
  void aSmallResultStaysInOnePartition() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=8");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 42 AS answer")) {
      assertTrue(rows.next());
      assertEquals(42, rows.getInt(1));
      assertFalse(rows.next());
      assertTrue(partitionsFetched(proxy).isEmpty(), proxy.requests().toString());
    }
  }

  @Test
  void emptyPartitionsEndTheResultCleanly() throws Exception {
    long batchRows =
        new RawWarp(SharedServer.get())
            .firstBatchRows("SELECT i, i * 2 AS j FROM range(" + ROWS + ") t(i)");
    assertTrue(batchRows > 0 && batchRows < ROWS, "batch size " + batchRows);
    // Two batches' worth of rows, split up to eight ways: some partitions get no batch.
    long total = 2 * batchRows;
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=8");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM range(" + total + ") t(i)")) {
      long expected = 0;
      while (rows.next()) {
        assertEquals(expected++, rows.getLong(1));
      }
      assertEquals(total, expected);
      TreeSet<Integer> fetched = partitionsFetched(proxy);
      assertTrue(
          fetched.size() + 1 > 2,
          (fetched.size() + 1) + " partitions for 2 batches: no empty partition to read");
    }
  }

  @Test
  void aFailureMidStreamFailsTheWholeResult() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=4");
        Statement statement = connection.createStatement()) {
      long[] read = new long[1];
      SQLException error =
          assertTimeoutPreemptively(
              Duration.ofSeconds(120),
              () ->
                  assertThrows(
                      SQLException.class,
                      () -> {
                        try (ResultSet rows =
                            statement.executeQuery(
                                "SELECT CASE WHEN i < 30000000 THEN i ELSE error('boom') END AS v"
                                    + " FROM range(100000000) t(i)")) {
                          while (rows.next()) {
                            rows.getLong(1);
                            read[0]++;
                          }
                        }
                      },
                      "the result must never end cleanly"));
      assertEquals("execution_error", WarpProblem.codeOf(error), error.toString());
      assertTrue(error.getMessage().contains("boom"), error.getMessage());
      assertTrue(read[0] < 100_000_000L);
      assertFalse(partitionsFetched(proxy).isEmpty(), "the result was not split");
      try (ResultSet rows = statement.executeQuery("SELECT 7")) {
        assertTrue(rows.next(), "the connection still works");
      }
    }
  }

  /** Asserts that nothing more arrives from the server: every download stopped. */
  private static void assertDownloadsStopped(ChaosProxy proxy) throws InterruptedException {
    Thread.sleep(1000);
    long before = proxy.bytesFromServer();
    Thread.sleep(1500);
    long after = proxy.bytesFromServer();
    assertTrue(after - before < 64 * 1024, (after - before) + " more bytes arrived");
  }

  private static String statementState(Connection connection, String statementId) throws Exception {
    String sessionId = connection.unwrap(WarpConnection.class).getSessionId();
    String body =
        new RawWarp(SharedServer.get())
            .get("/warp/v1/sessions/" + sessionId + "/statements/" + statementId)
            .body();
    Matcher state = Pattern.compile("\"state\"\\s*:\\s*\"(\\w+)\"").matcher(body);
    return state.find() ? state.group(1) : body;
  }

  @Test
  void cancelWhileReadingStopsEveryPartition() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=3");
        Statement statement = connection.createStatement()) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        AtomicLong read = new AtomicLong();
        Future<SQLException> reading =
            executor.submit(
                () ->
                    assertThrows(
                        SQLException.class,
                        () -> {
                          try (ResultSet rows = statement.executeQuery(ENDLESS)) {
                            while (rows.next()) {
                              read.incrementAndGet();
                            }
                          }
                        }));
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (read.get() < 100_000 && System.nanoTime() < deadline) {
          Thread.sleep(20);
        }
        assertTrue(read.get() > 0, "no rows arrived");
        statement.cancel();
        SQLException error = reading.get();
        assertEquals("statement_cancelled", WarpProblem.codeOf(error), error.toString());
        assertEquals("57014", error.getSQLState());
      } finally {
        executor.shutdownNow();
      }
      assertSplitAndConcurrent(proxy, 3);
      assertDownloadsStopped(proxy);
      String state = statementState(connection, statementFetched(proxy));
      assertTrue(state.equals("cancelled") || state.contains("not_found"), state);
      try (ResultSet rows = statement.executeQuery("SELECT 1")) {
        assertTrue(rows.next(), "the statement is usable after a cancel");
      }
    }
  }

  @Test
  void closingEarlyStopsEveryPartition() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "resultPartitions=4");
        Statement statement = connection.createStatement()) {
      ResultSet rows = statement.executeQuery(ENDLESS);
      for (int i = 0; i < 100_000; i++) {
        assertTrue(rows.next());
      }
      assertTimeoutPreemptively(Duration.ofSeconds(20), rows::close);
      assertSplitAndConcurrent(proxy, 4);
      assertDownloadsStopped(proxy);
      String state = statementState(connection, statementFetched(proxy));
      assertTrue(state.equals("cancelled") || state.contains("not_found"), state);
      assertTimeoutPreemptively(
          Duration.ofSeconds(10),
          () -> {
            try (ResultSet next = statement.executeQuery("SELECT 7")) {
              assertTrue(next.next());
              assertEquals(7, next.getInt(1));
            }
          });
    }
  }

  @Test
  void retainedAsyncResultIsReadInPartitions() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "async=true&resultPartitions=3");
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT i FROM range(" + ROWS + ") t(i) ORDER BY i")) {
      long expected = 0;
      while (rows.next()) {
        assertEquals(expected++, rows.getLong(1));
      }
      assertEquals(ROWS, expected);
      TreeSet<Integer> fetched = partitionsFetched(proxy);
      assertTrue(fetched.size() > 1 && fetched.first() == 0, "partitions fetched: " + fetched);
      assertTrue(fetched.last() < 3);
      assertTrue(proxy.mostOpenConnections() >= fetched.size(), "read concurrently");
    }
  }

  @Test
  void aRetainedPartitionResumesAfterItsConnectionBreaks() throws Exception {
    // The cut comes after more than one batch even of warp-duckdb's (about 1 MB here), so the
    // partition resumes from a later batch.
    WarpServer server = SharedServer.get();
    try (ChaosProxy proxy =
            new ChaosProxy(
                server.port, "GET /warp/v1/sessions/\\S+/result\\?partition=1 ", 1_200_000, 1);
        Connection connection = connect(proxy, "async=true&resultPartitions=3");
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT i FROM range(" + ROWS + ") t(i) ORDER BY i")) {
      long expected = 0;
      while (rows.next()) {
        assertEquals(expected++, rows.getLong(1));
      }
      assertEquals(ROWS, expected);
      assertEquals(1, proxy.cuts(), "the proxy broke partition 1's download");
      List<String> resumed =
          proxy.requests().stream()
              .filter(r -> r.contains("partition=1&from_batch="))
              .collect(Collectors.toList());
      assertEquals(1, resumed.size(), proxy.requests().toString());
    }
  }

  @Test
  void preparedStatementsAndPerStatementSettingsSplitToo() throws Exception {
    try (ChaosProxy proxy = proxy();
        Connection connection = connect(proxy, "");
        PreparedStatement statement =
            connection.prepareStatement("SELECT i FROM range(?) t(i) ORDER BY i")) {
      WarpStatement warp = statement.unwrap(WarpStatement.class);
      assertEquals(8, warp.getResultPartitions(), "8 by default");
      warp.setResultPartitions(4);
      assertEquals("preserve", warp.getResultOrder());
      assertThrows(SQLException.class, () -> warp.setResultPartitions(0));
      assertThrows(SQLException.class, () -> warp.setResultOrder("sorted"));
      statement.setLong(1, ROWS);
      try (ResultSet rows = statement.executeQuery()) {
        long expected = 0;
        while (rows.next()) {
          assertEquals(expected++, rows.getLong(1));
        }
        assertEquals(ROWS, expected);
      }
      assertSplitAndConcurrent(proxy, 4);
    }
  }

  @Test
  void badSettingsAreRefused() {
    for (String options : List.of("resultPartitions=0", "resultPartitions=257", "resultOrder=x")) {
      SQLException error =
          assertThrows(SQLException.class, () -> SharedServer.connect(options).close(), options);
      assertTrue(error.getMessage().contains(options.split("=")[0]), error.getMessage());
    }
  }
}
