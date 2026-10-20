package com.gizmodata.warpsql.jdbc;

import static com.gizmodata.warpsql.jdbc.FakeWarpServer.arrow;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.document;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.json;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.arrow.TestStreams;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The driver's client logic against a scripted server: what it does when tokens expire, sessions
 * disappear, streams break, requests are lost, and servers lack optional capabilities. These cases
 * are hard or impossible to provoke on a real server.
 */
class ClientBehaviourTest {
  private static final Schema INTS =
      new Schema(List.of(Field.nullable("i", new ArrowType.Int(64, true))));
  private final BufferAllocator allocator = new RootAllocator();
  private final FakeWarpServer server;

  ClientBehaviourTest() throws Exception {
    server = new FakeWarpServer();
  }

  @AfterEach
  void close() {
    server.close();
    allocator.close();
  }

  /** Batches of consecutive integers, starting at {@code first}. */
  private static List<List<Object[]>> batches(long first, int batches, int rows) {
    List<List<Object[]>> all = new ArrayList<>();
    long next = first;
    for (int b = 0; b < batches; b++) {
      List<Object[]> batch = new ArrayList<>();
      for (int r = 0; r < rows; r++) {
        batch.add(new Object[] {next++});
      }
      all.add(batch);
    }
    return all;
  }

  private byte[] stream(long first, int batches, int rows) throws Exception {
    return TestStreams.stream(allocator, INTS, batches(first, batches, rows));
  }

  private static List<Long> readAll(ResultSet rows) throws SQLException {
    List<Long> values = new ArrayList<>();
    while (rows.next()) {
      values.add(rows.getLong(1));
    }
    return values;
  }

  private static List<Long> range(long from, long to) {
    List<Long> values = new ArrayList<>();
    for (long i = from; i < to; i++) {
      values.add(i);
    }
    return values;
  }

  private Connection connect(String options) throws SQLException {
    return DriverManager.getConnection(server.url(options));
  }

  private static String updateDocument(String id, int rows) {
    return document(id, "succeeded", "{\"kind\":\"update\",\"rows_affected\":" + rows + "}");
  }

  // ---- Tokens (spec §4) ----

  @Test
  void anExpiredTokenIsRenewedAndTheRequestRepeatedOnce() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() == 1) {
            problem(exchange, 401, "invalid_token", "");
          } else {
            json(exchange, 200, updateDocument("q1", 3));
          }
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertEquals(3, statement.executeUpdate("UPDATE t SET x = 1"));
    }
    assertEquals(2, server.tokens.get());
    List<FakeWarpServer.Request> submits = server.requests("POST", "/v1/sessions/s1/statements");
    assertEquals("Bearer t1", submits.get(0).header("Authorization"));
    assertEquals("Bearer t2", submits.get(1).header("Authorization"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"token_expired", "token_retired_by_a_later_version"})
  void anExpiredOrUnexplainedTokenIsRenewedOnce(String reason) throws Exception {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() <= 2) {
            problem(exchange, 401, "invalid_token", ",\"reason\":\"" + reason + "\"");
          } else {
            json(exchange, 200, updateDocument("q1", 1));
          }
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class, () -> statement.execute("SELECT 1"));
      assertEquals(reason, WarpProblem.of(error).reason(), "renewed once, then the error");
      assertEquals(1, statement.executeUpdate("UPDATE t SET a = 1"));
    }
    assertEquals(2, server.tokens.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"token_revoked", "credentials_changed", "principal_dropped"})
  void aWithdrawnTokenIsNotRenewed(String reason) throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            problem(exchange, 401, "invalid_token", ",\"reason\":\"" + reason + "\""));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class, () -> statement.execute("SELECT 1"));
      assertEquals("invalid_token", WarpProblem.codeOf(error));
      assertEquals(reason, WarpProblem.of(error).reason());
      assertTrue(error.getMessage().contains(reason), "the reason is shown: " + error.getMessage());
    }
    assertEquals(1, server.tokens.get(), "no sign-in after " + reason);
    assertEquals(1, server.requests("POST", "/v1/sessions/s1/statements").size());
  }

  @Test
  void aTokenIsRenewedBeforeItExpires() throws Exception {
    server.expiresIn = 1;
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 0)));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    }
    assertTrue(server.tokens.get() >= 2, "renewed within 30 s of expiry: " + server.tokens.get());
  }

  @Test
  void aPreIssuedTokenCannotBeRenewed() throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> problem(exchange, 401, "invalid_token", ""));
    try (Connection connection = connect("token=issued-elsewhere");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class, () -> statement.execute("SELECT 1"));
      assertEquals("invalid_token", WarpProblem.codeOf(error));
    }
    assertEquals(0, server.tokens.get(), "a token property means no sign-in");
    assertEquals(
        "Bearer issued-elsewhere",
        server.requests("POST", "/v1/sessions").get(0).header("Authorization"));
  }

  @Test
  void theTokenExchangeGrant() throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:warp://127.0.0.1:"
                + server.base().replaceAll(".*:(\\d+)/.*", "$1")
                + "?useEncryption=false&subjectToken=idp-token")) {
      assertTrue(connection.isValid(1));
    }
    String body = server.requests("POST", "/v1/auth/token").get(0).body();
    assertTrue(body.contains("\"grant_type\":\"token_exchange\""), body);
    assertTrue(body.contains("\"subject_token\":\"idp-token\""), body);
  }

  @Test
  void passwordsTravelAsJsonFields() throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            server.url("").replace("password=pw", "password=a%3Ab%40c%22d"))) {
      assertTrue(connection.isValid(1));
    }
    String body = server.requests("POST", "/v1/auth/token").get(0).body();
    assertTrue(body.contains("\"password\":\"a:b@c\\\"d\""), body);
  }

  // ---- Retries (spec §6.7) ----

  @Test
  void aLostResponseIsRetriedWithTheSameIdempotencyKey() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() == 1) {
            exchange.close();
          } else {
            json(exchange, 200, updateDocument("q1", 1));
          }
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertEquals(1, statement.executeUpdate("INSERT INTO t VALUES (1)"));
    }
    List<FakeWarpServer.Request> submits = server.requests("POST", "/v1/sessions/s1/statements");
    assertEquals(2, submits.size());
    assertNotNull(submits.get(0).header("Idempotency-Key"));
    assertEquals(
        submits.get(0).header("Idempotency-Key"), submits.get(1).header("Idempotency-Key"));
  }

  @Test
  void commitsAreNotRetried() throws Exception {
    server.route("POST", "/v1/sessions/s1/commit", (exchange, request, path) -> exchange.close());
    server.route(
        "PATCH",
        "/v1/sessions/s1",
        (exchange, request, path) ->
            json(exchange, 200, "{\"options\":{\"autocommit\":false},\"errors\":{}}"));
    try (Connection connection = connect("")) {
      connection.setAutoCommit(false);
      SQLException error = assertThrows(SQLException.class, connection::commit);
      assertEquals("08006", error.getSQLState());
    }
    assertEquals(
        1,
        server.requests("POST", "/v1/sessions/s1/commit").size(),
        "a commit is never sent twice");
  }

  // ---- Lost sessions (spec §5.6) ----

  @Test
  void aLostSessionIsReopenedWithItsOptionsAndTheStatementRunAgain() throws Exception {
    server.route(
        "PATCH",
        "/v1/sessions/s1",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"options\":{\"catalog\":\"cat\",\"schema\":\"sales\"},\"errors\":{}}"));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> problem(exchange, 404, "session_not_found", ""));
    server.route(
        "POST",
        "/v1/sessions/s2/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 2)));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      connection.setSchema("sales");
      assertEquals(2, statement.executeUpdate("DELETE FROM t"));
      assertEquals("s2", connection.unwrap(WarpConnection.class).getSessionId());
      assertNotNull(connection.getWarnings());
      assertTrue(connection.getWarnings().getMessage().contains("no longer had session s1"));
    }
    String reopened = server.requests("POST", "/v1/sessions").get(1).body();
    assertTrue(reopened.contains("\"schema\":\"sales\""), reopened);
    assertTrue(reopened.contains("\"autocommit\":true"), reopened);
  }

  @Test
  void aLostSessionWithAnOpenTransactionIsReported() throws Exception {
    server.route(
        "PATCH",
        "/v1/sessions/s1",
        (exchange, request, path) ->
            json(exchange, 200, "{\"options\":{\"autocommit\":false},\"errors\":{}}"));
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() == 1) {
            json(exchange, 200, updateDocument("q1", 1));
          } else {
            problem(exchange, 404, "session_not_found", "");
          }
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.executeUpdate("INSERT INTO t VALUES (1)");
      SQLTransientConnectionException error =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> statement.executeUpdate("INSERT INTO t VALUES (2)"));
      assertEquals("08007", error.getSQLState());
    }
    assertTrue(
        server.requests("POST", "/v1/sessions").get(1).body().contains("\"autocommit\":false"));
    assertEquals(0, server.requests("POST", "/v1/sessions/s2/statements").size(), "not run again");
  }

  @Test
  void theTransactionStateComesFromTheSessionDocument() throws Exception {
    try (Connection connection = connect("")) {
      WarpConnection warp = connection.unwrap(WarpConnection.class);
      assertEquals(WarpConnection.TransactionState.NONE, warp.getTransactionState());
      server.transaction = "open";
      assertEquals(WarpConnection.TransactionState.OPEN, warp.getTransactionState());
      server.transaction = "aborted";
      assertEquals(WarpConnection.TransactionState.ABORTED, warp.getTransactionState());
      server.transaction = "suspended";
      assertEquals(
          WarpConnection.TransactionState.UNKNOWN,
          warp.getTransactionState(),
          "a value from a later version is unknown, not an error (spec §12)");
      server.transaction = null;
      assertEquals(WarpConnection.TransactionState.UNKNOWN, warp.getTransactionState());
    }
  }

  @Test
  void aLostSessionAfterASqlBeginIsReportedInAutocommitMode() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() == 1) {
            json(exchange, 200, updateDocument("q1", 0));
          } else {
            problem(exchange, 404, "session_not_found", "");
          }
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      statement.execute("BEGIN");
      server.transaction = "open";
      assertEquals(
          WarpConnection.TransactionState.OPEN,
          connection.unwrap(WarpConnection.class).getTransactionState());
      SQLTransientConnectionException error =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> statement.executeUpdate("INSERT INTO t VALUES (2)"));
      assertEquals("08007", error.getSQLState());
    }
    assertEquals(0, server.requests("POST", "/v1/sessions/s2/statements").size(), "not run again");
  }

  @Test
  void aKilledSessionClosesTheConnection() throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            problem(exchange, 401, "session_ended", ",\"reason\":\"killed\""));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      SQLNonTransientConnectionException error =
          assertThrows(
              SQLNonTransientConnectionException.class, () -> statement.execute("SELECT 1"));
      assertEquals("08003", error.getSQLState());
      assertTrue(error.getMessage().contains("killed"), error.getMessage());
      assertTrue(connection.isClosed());
    }
    assertEquals(1, server.sessions.get(), "no new session after a kill");
    assertEquals(0, server.requests("DELETE", "/v1/sessions/s1").size(), "nothing left to end");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"token_revoked", "credentials_changed", "principal_dropped", "evicted_by_magic"})
  void aSessionEndedForAnyOtherReasonClosesTheConnection(String reason) throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            problem(exchange, 401, "session_ended", ",\"reason\":\"" + reason + "\""));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      SQLNonTransientConnectionException error =
          assertThrows(
              SQLNonTransientConnectionException.class, () -> statement.execute("SELECT 1"));
      assertEquals("08003", error.getSQLState());
      assertTrue(error.getMessage().contains(reason), error.getMessage());
      assertTrue(connection.isClosed());
    }
    assertEquals(1, server.tokens.get(), "no sign-in with credentials that no longer work");
    assertEquals(1, server.sessions.get(), "no new session");
  }

  @Test
  void aSessionEndedByTokenExpiryIsReopenedWithANewToken() throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            problem(exchange, 401, "session_ended", ",\"reason\":\"token_expired\""));
    server.route(
        "POST",
        "/v1/sessions/s2/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 0)));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    }
    assertEquals(2, server.tokens.get());
    assertEquals(
        "Bearer t2", server.requests("POST", "/v1/sessions").get(1).header("Authorization"));
  }

  // ---- Results (spec §6.4–§6.6) ----

  private void retainedStatement(String id, boolean retained, String state, String cancelReason) {
    server.route(
        "GET",
        "/v1/sessions/s1/statements/" + id,
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"statement_id\":\""
                    + id
                    + "\",\"state\":\""
                    + state
                    + "\""
                    + (cancelReason == null
                        ? ""
                        : ",\"cancel_reason\":\""
                            + cancelReason
                            + "\",\"error\":{\"code\":\"statement_cancelled\"}")
                    + ",\"result\":{\"kind\":\"rows\",\"partitions\":[{\"index\":0}],\"retained\":"
                    + retained
                    + "}}"));
  }

  @Test
  void aBrokenRetainedStreamResumesFromTheNextBatch() throws Exception {
    byte[] firstThree = TestStreams.withoutEndMarker(stream(0, 3, 100));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> arrow(exchange, "q1", firstThree));
    retainedStatement("q1", true, "succeeded", null);
    byte[] rest = stream(300, 2, 100);
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1/result",
        (exchange, request, path) -> arrow(exchange, "q1", rest));
    try (Connection connection = connect("retainResults=true");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM big")) {
      assertEquals(range(0, 500), readAll(rows));
    }
    FakeWarpServer.Request resume =
        server.requests("GET", "/v1/sessions/s1/statements/q1/result").get(0);
    assertEquals("partition=0&from_batch=3", resume.query);
  }

  @Test
  void aBrokenUnretainedStreamIsAConnectionError() throws Exception {
    byte[] broken = TestStreams.withoutEndMarker(stream(0, 2, 10));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> arrow(exchange, "q1", broken));
    retainedStatement("q1", false, "cancelled", "client");
    try (Connection connection = connect("");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM big")) {
      SQLException error = assertThrows(SQLException.class, () -> readAll(rows));
      assertEquals("08006", error.getSQLState());
      assertTrue(error.getMessage().contains("not retained"), error.getMessage());
    }
    assertEquals(0, server.requests("GET", "/v1/sessions/s1/statements/q1/result").size());
  }

  @Test
  void aStatementThatFailsAfterItsSchemaFailsInExecute() throws Exception {
    byte[] schemaOnly = TestStreams.withoutEndMarker(TestStreams.schemaStream(allocator, INTS));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> arrow(exchange, "q1", schemaOnly));
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"statement_id\":\"q1\",\"state\":\"failed\",\"error\":{\"code\":"
                    + "\"execution_error\",\"detail\":\"boom\"}}"));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.executeQuery("SELECT error('boom')"));
      assertEquals("execution_error", WarpProblem.codeOf(error));
      assertTrue(error.getMessage().contains("boom"), error.getMessage());
      assertNull(statement.getResultSet());
    }
  }

  /** A status document whose result is split over {@code partitions}. */
  private void splitDocument(String partitions, String order) throws Exception {
    String schema = Base64.getEncoder().encodeToString(TestStreams.schemaStream(allocator, INTS));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            json(
                exchange,
                201,
                document(
                    "q1",
                    "succeeded",
                    "{\"kind\":\"rows\",\"schema\":\""
                        + schema
                        + "\",\"partitions\":"
                        + partitions
                        + (order == null ? "" : ",\"partition_order\":\"" + order + "\"")
                        + ",\"retained\":true}")));
  }

  /**
   * Answers a partition only once {@code together} partitions' requests are in flight at once, so a
   * driver that reads partitions one after another fails instead of passing.
   */
  private static FakeWarpServer.Route concurrently(CountDownLatch together, byte[] stream) {
    return (exchange, request, path) -> {
      together.countDown();
      try {
        if (!together.await(5, TimeUnit.SECONDS)) {
          problem(exchange, 500, "internal_error", "");
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      arrow(exchange, "q1", stream);
    };
  }

  @Test
  void partitionsOnOtherServersAreReadThereAllAtOnce() throws Exception {
    server.capabilities = "[\"long_poll\",\"result_partitions\",\"result_locations\"]";
    try (FakeWarpServer worker = new FakeWarpServer()) {
      splitDocument(
          "[{\"index\":0,\"location\":null},{\"index\":1,\"location\":\"" + worker.base() + "\"}]",
          "interleaved");
      CountDownLatch together = new CountDownLatch(2);
      byte[] first =
          TestStreams.stream(
              allocator, INTS, List.of(batches(0, 1, 5).get(0), batches(10, 1, 5).get(0)));
      server.route("GET", "/v1/sessions/s1/statements/q1/result", concurrently(together, first));
      worker.route(
          "GET", "/v1/sessions/s1/statements/q1/result", concurrently(together, stream(5, 1, 5)));
      try (Connection connection = connect("async=true");
          Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT i FROM distributed")) {
        assertEquals(range(0, 15), readAll(rows), "interleaved back into the result's order");
      }
      assertEquals(
          "remote",
          server
              .requests("POST", "/v1/sessions/s1/statements")
              .get(0)
              .header("Warp-Result-Locations"));
      assertEquals(
          "partition=0",
          server.requests("GET", "/v1/sessions/s1/statements/q1/result").get(0).query);
      FakeWarpServer.Request remote =
          worker.requests("GET", "/v1/sessions/s1/statements/q1/result").get(0);
      assertEquals("partition=1", remote.query);
      assertEquals("Bearer t1", remote.header("Authorization"), "the same access token");
    }
  }

  @Test
  void aLocationTheDriverDidNotAskForIsRefused() throws Exception {
    try (FakeWarpServer worker = new FakeWarpServer()) {
      splitDocument(
          "[{\"index\":0,\"location\":null},{\"index\":1,\"location\":\"" + worker.base() + "\"}]",
          "interleaved");
      byte[] local = stream(0, 1, 5);
      server.route(
          "GET",
          "/v1/sessions/s1/statements/q1/result",
          (exchange, request, path) -> arrow(exchange, "q1", local));
      try (Connection connection = connect("async=true");
          Statement statement = connection.createStatement()) {
        SQLException error =
            assertThrows(SQLException.class, () -> readAll(statement.executeQuery("SELECT 1")));
        assertEquals("client_protocol_error", WarpProblem.codeOf(error), error.toString());
      }
      assertNull(
          server
              .requests("POST", "/v1/sessions/s1/statements")
              .get(0)
              .header("Warp-Result-Locations"),
          "only with the result_locations capability");
      assertEquals(0, worker.requests.size(), "the token never went there");
    }
  }

  @Test
  void aSplitDocumentWithoutItsOrderIsAProtocolError() throws Exception {
    splitDocument("[{\"index\":0,\"location\":null},{\"index\":1,\"location\":null}]", null);
    try (Connection connection = connect("async=true");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
      assertEquals("client_protocol_error", WarpProblem.codeOf(error));
      assertEquals("08P01", error.getSQLState());
    }
    assertEquals(0, server.requests("GET", "/v1/sessions/s1/statements/q1/result").size());
  }

  @Test
  void aStreamedSplitWithoutItsOrderIsAProtocolError() throws Exception {
    server.capabilities = "[\"long_poll\",\"result_partitions\"]";
    byte[] first = stream(0, 2, 5);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          exchange.getResponseHeaders().set("Warp-Result-Partitions", "2");
          arrow(exchange, "q1", first);
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
      assertEquals("client_protocol_error", WarpProblem.codeOf(error));
      assertTrue(error.getMessage().contains("Warp-Result-Order"), error.getMessage());
    }
  }

  @Test
  void streamedPartitionsAreReadAllAtOnce() throws Exception {
    server.capabilities = "[\"long_poll\",\"result_partitions\"]";
    byte[] first = stream(0, 2, 5);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          exchange.getResponseHeaders().set("Warp-Result-Partitions", "3");
          exchange.getResponseHeaders().set("Warp-Result-Order", "interleaved");
          arrow(exchange, "q1", first);
        });
    CountDownLatch together = new CountDownLatch(2);
    byte[] second = stream(100, 2, 5);
    byte[] third = stream(200, 1, 5);
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1/result",
        (exchange, request, path) ->
            concurrently(together, request.query.equals("partition=1") ? second : third)
                .answer(exchange, request, path));
    try (Connection connection = connect("resultPartitions=3");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM t ORDER BY i")) {
      List<Long> expected = new ArrayList<>(range(0, 5));
      expected.addAll(range(100, 105));
      expected.addAll(range(200, 205));
      expected.addAll(range(5, 10));
      expected.addAll(range(105, 110));
      assertEquals(expected, readAll(rows));
    }
    FakeWarpServer.Request submit = server.requests("POST", "/v1/sessions/s1/statements").get(0);
    assertEquals("3", submit.header("Warp-Result-Partitions"));
    assertEquals("preserve", submit.header("Warp-Result-Order"));
    assertNull(submit.header("Warp-Result-Locations"), "only with result_locations");
    assertEquals(
        0,
        server.requests("GET", "/v1/sessions/s1/statements/q1").size(),
        "every partition is here: no document needed");
  }

  @Test
  void streamedPartitionsElsewhereAreFoundInTheDocument() throws Exception {
    server.capabilities = "[\"long_poll\",\"result_partitions\",\"result_locations\"]";
    try (FakeWarpServer worker = new FakeWarpServer()) {
      byte[] first = stream(0, 2, 5);
      server.route(
          "POST",
          "/v1/sessions/s1/statements",
          (exchange, request, path) -> {
            exchange.getResponseHeaders().set("Warp-Result-Partitions", "2");
            exchange.getResponseHeaders().set("Warp-Result-Order", "interleaved");
            arrow(exchange, "q1", first);
          });
      server.route(
          "GET",
          "/v1/sessions/s1/statements/q1",
          (exchange, request, path) ->
              json(
                  exchange,
                  200,
                  document(
                      "q1",
                      "running",
                      "{\"kind\":\"rows\",\"partitions\":[{\"index\":0,\"location\":null},"
                          + "{\"index\":1,\"location\":\""
                          + worker.base()
                          + "\"}],\"partition_order\":\"interleaved\",\"retained\":false}")));
      byte[] second = stream(100, 1, 5);
      worker.route(
          "GET",
          "/v1/sessions/s1/statements/q1/result",
          (exchange, request, path) -> arrow(exchange, "q1", second));
      try (Connection connection = connect("resultPartitions=2");
          Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT i FROM t ORDER BY i")) {
        List<Long> expected = new ArrayList<>(range(0, 5));
        expected.addAll(range(100, 105));
        expected.addAll(range(5, 10));
        assertEquals(expected, readAll(rows));
      }
      assertEquals(
          "remote",
          server
              .requests("POST", "/v1/sessions/s1/statements")
              .get(0)
              .header("Warp-Result-Locations"));
      assertEquals(
          "partition=1",
          worker.requests("GET", "/v1/sessions/s1/statements/q1/result").get(0).query);
      assertEquals(0, server.requests("GET", "/v1/sessions/s1/statements/q1/result").size());
    }
  }

  @Test
  void closingAPartlyReadResultFreesItOnTheServer() throws Exception {
    byte[] complete = stream(0, 3, 10);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> arrow(exchange, "q1", complete));
    server.route(
        "DELETE",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) -> FakeWarpServer.empty(exchange, 204));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      try (ResultSet rows = statement.executeQuery("SELECT i FROM t")) {
        assertTrue(rows.next());
      }
      assertEquals(1, server.requests("DELETE", "/v1/sessions/s1/statements/q1").size());
      try (ResultSet rows = statement.executeQuery("SELECT i FROM t")) {
        readAll(rows);
      }
      assertEquals(
          1,
          server.requests("DELETE", "/v1/sessions/s1/statements/q1").size(),
          "a result read to the end is already gone from an unretaining server");
    }
  }

  @Test
  void anUnreadResultIsBufferedBeforeTheSessionRunsAnythingElse() throws Exception {
    AtomicBoolean firstFinished = new AtomicBoolean();
    byte[] big = stream(0, 4, 1000);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (request.body().contains("big")) {
            arrow(exchange, "q1", big);
            firstFinished.set(true);
          } else {
            assertTrue(firstFinished.get());
            json(exchange, 200, updateDocument("q2", 0));
          }
        });
    try (Connection connection = connect("");
        Statement first = connection.createStatement();
        Statement second = connection.createStatement();
        ResultSet rows = first.executeQuery("SELECT i FROM big")) {
      assertTrue(rows.next());
      assertEquals(0, rows.getLong(1));
      second.execute("SELECT 2");
      List<Long> values = new ArrayList<>(List.of(0L));
      values.addAll(readAll(rows));
      assertEquals(range(0, 4000), values, "the buffered rest reads like the stream would have");
    }
  }

  // ---- Servers without optional capabilities (spec §3.1) ----

  @Test
  void withoutPreparedStatementsParametersTravelAsJson() throws Exception {
    server.capabilities = "[\"transactions\"]";
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 1)));
    try (Connection connection = connect("");
        PreparedStatement statement =
            connection.prepareStatement("UPDATE t SET a = ? WHERE b = $2")) {
      statement.setInt(1, 7);
      statement.setString(2, "x");
      assertEquals(1, statement.executeUpdate());
      assertEquals(2, statement.getParameterMetaData().getParameterCount());
    }
    assertEquals(0, server.requests("POST", "/v1/sessions/s1/prepared").size());
    String body = server.requests("POST", "/v1/sessions/s1/statements").get(0).body();
    assertEquals("{\"sql\":\"UPDATE t SET a = ? WHERE b = $2\",\"parameters\":[7,\"x\"]}", body);
  }

  @Test
  void withoutLongPollStatusIsPolled() throws Exception {
    server.capabilities = "[\"prepared_statements\"]";
    AtomicInteger polls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 201, document("q1", "running", null)));
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                polls.incrementAndGet() < 3
                    ? document("q1", "running", null)
                    : updateDocument("q1", 4)));
    try (Connection connection = connect("async=true");
        Statement statement = connection.createStatement()) {
      assertEquals(4, statement.executeUpdate("UPDATE t SET a = 1"));
    }
    for (FakeWarpServer.Request poll : server.requests("GET", "/v1/sessions/s1/statements/q1")) {
      assertNull(poll.header("Prefer"), "no long-poll without the capability");
    }
    assertEquals(3, polls.get());
  }

  @Test
  void aQuickStatementIsPolledBrieflyNotLongPolled() throws Exception {
    AtomicInteger polls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 201, document("q1", "queued", null)));
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                polls.incrementAndGet() < 4
                    ? document("q1", "running", null)
                    : updateDocument("q1", 1)));
    try (Connection connection = connect("async=true");
        Statement statement = connection.createStatement()) {
      assertEquals(1, statement.executeUpdate("UPDATE t SET a = 1"));
    }
    for (FakeWarpServer.Request poll : server.requests("GET", "/v1/sessions/s1/statements/q1")) {
      assertNull(
          poll.header("Prefer"),
          "a long-poll on a statement that just finished may be held for its whole wait");
    }
  }

  @Test
  void aLongStatementIsLongPolledWithGrowingWaits() throws Exception {
    long finishes = System.nanoTime() + Duration.ofMillis(900).toNanos();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 201, document("q1", "running", null)));
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) -> {
          if (request.header("Prefer") != null) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          json(
              exchange,
              200,
              System.nanoTime() - finishes > 0
                  ? updateDocument("q1", 7)
                  : document("q1", "running", null));
        });
    try (Connection connection = connect("async=true&pollWait=4");
        Statement statement = connection.createStatement()) {
      assertEquals(7, statement.executeUpdate("UPDATE t SET a = 1"));
    }
    List<String> waits = new ArrayList<>();
    for (FakeWarpServer.Request poll : server.requests("GET", "/v1/sessions/s1/statements/q1")) {
      if (poll.header("Prefer") != null) {
        waits.add(poll.header("Prefer"));
      }
    }
    assertTrue(waits.size() >= 2, waits.toString());
    assertEquals(List.of("wait=1", "wait=2"), waits.subList(0, 2), "waits grow toward pollWait");
  }

  @Test
  void typeInfoMapsFlightSqlColumnsToJdbcs() throws Exception {
    Schema schema =
        new Schema(
            List.of(
                Field.notNullable("type_name", ArrowType.Utf8.INSTANCE),
                Field.notNullable("data_type", new ArrowType.Int(32, true)),
                Field.nullable("column_size", new ArrowType.Int(32, true)),
                Field.nullable("literal_prefix", ArrowType.Utf8.INSTANCE),
                Field.nullable("literal_suffix", ArrowType.Utf8.INSTANCE),
                new Field(
                    "create_params",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    List.of(Field.notNullable("item", ArrowType.Utf8.INSTANCE))),
                Field.notNullable("nullable", new ArrowType.Int(32, true)),
                Field.notNullable("case_sensitive", ArrowType.Bool.INSTANCE),
                Field.notNullable("searchable", new ArrowType.Int(32, true)),
                Field.nullable("unsigned_attribute", ArrowType.Bool.INSTANCE),
                Field.notNullable("fixed_prec_scale", ArrowType.Bool.INSTANCE),
                Field.nullable("auto_increment", ArrowType.Bool.INSTANCE),
                Field.nullable("local_type_name", ArrowType.Utf8.INSTANCE),
                Field.nullable("minimum_scale", new ArrowType.Int(32, true)),
                Field.nullable("maximum_scale", new ArrowType.Int(32, true)),
                Field.notNullable("sql_data_type", new ArrowType.Int(32, true)),
                Field.nullable("datetime_subcode", new ArrowType.Int(32, true)),
                Field.nullable("num_prec_radix", new ArrowType.Int(32, true)),
                Field.nullable("interval_precision", new ArrowType.Int(32, true))));
    byte[] stream =
        TestStreams.stream(
            allocator,
            schema,
            List.of(
                List.<Object[]>of(
                    new Object[] {
                      "DECIMAL",
                      Types.DECIMAL,
                      38,
                      null,
                      null,
                      List.of("precision", "scale"),
                      1,
                      false,
                      3,
                      false,
                      true,
                      false,
                      "DECIMAL",
                      0,
                      38,
                      Types.DECIMAL,
                      null,
                      10,
                      null
                    })));
    server.route(
        "GET",
        "/v1/sessions/s1/metadata/type-info",
        (exchange, request, path) -> arrow(exchange, "", stream));
    try (Connection connection = connect("");
        ResultSet rows = connection.getMetaData().getTypeInfo()) {
      assertTrue(rows.next());
      assertEquals("DECIMAL", rows.getString("TYPE_NAME"));
      assertEquals(Types.DECIMAL, rows.getInt("DATA_TYPE"));
      assertEquals(38, rows.getInt("PRECISION"));
      assertEquals("precision,scale", rows.getString("CREATE_PARAMS"));
      assertEquals((short) 1, rows.getObject("NULLABLE"));
      assertFalse(rows.getBoolean("CASE_SENSITIVE"));
      assertEquals(3, rows.getShort("SEARCHABLE"));
      assertTrue(rows.getBoolean("FIXED_PREC_SCALE"));
      assertEquals(38, rows.getShort("MAXIMUM_SCALE"));
      assertEquals(10, rows.getInt("NUM_PREC_RADIX"));
      assertFalse(rows.next());
    }
  }

  // ---- Requests the driver sends ----

  @Test
  void submissionsCarryTimeoutRetentionCompressionAndAKey() throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 0)));
    try (Connection connection = connect("retainResults=true&ipcCompression=zstd");
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(5);
      statement.execute("CREATE TABLE t (i INT)");
    }
    FakeWarpServer.Request submit = server.requests("POST", "/v1/sessions/s1/statements").get(0);
    assertEquals("5000", submit.header("Warp-Statement-Timeout"));
    assertEquals("true", submit.header("Warp-Retain-Result"));
    assertEquals("zstd", submit.header("Warp-IPC-Compression"));
    assertEquals("application/vnd.apache.arrow.stream", submit.header("Accept"));
    assertTrue(
        submit.header("User-Agent").startsWith("warp-sql-jdbc/"), submit.header("User-Agent"));
    assertEquals(36, submit.header("Idempotency-Key").length());
    assertEquals("{\"sql\":\"CREATE TABLE t (i INT)\"}", submit.body());
  }

  @Test
  void compressionIsOnlyAskedOfServersThatOfferIt() throws Exception {
    server.capabilities = "[\"prepared_statements\",\"long_poll\"]";
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 0)));
    try (Connection connection = connect("ipcCompression=zstd&async=true");
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    }
    FakeWarpServer.Request submit = server.requests("POST", "/v1/sessions/s1/statements").get(0);
    assertNull(submit.header("Warp-IPC-Compression"));
    assertEquals("respond-async", submit.header("Prefer"));
    assertEquals("application/json", submit.header("Accept"));
  }

  private static final String BOTH_CODECS =
      "[\"prepared_statements\",\"long_poll\",\"result_partitions\",\"result_retention\","
          + "\"ipc_compression_zstd\",\"ipc_compression_lz4_frame\"]";

  /** One batch of the integers from {@code first} to {@code first + rows - 1}. */
  private static List<Object[]> batch(long first, int rows) {
    return batches(first, 1, rows).get(0);
  }

  /**
   * A streamed result split three ways (spec §6.10): the submission's own stream is partition 0,
   * and the driver fetches partitions 1 and 2. Every one of those requests must ask for the codec,
   * since each partition's header applies to that partition alone (§8.2).
   */
  @Test
  void everyPartitionsRequestAsksForTheCodec() throws Exception {
    server.capabilities = BOTH_CODECS;
    byte[] partition0 = TestStreams.stream(allocator, INTS, List.of(batch(0, 5), batch(15, 5)));
    byte[] partition1 = TestStreams.stream(allocator, INTS, List.of(batch(5, 5), batch(20, 5)));
    byte[] partition2 = TestStreams.stream(allocator, INTS, List.of(batch(10, 5)));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          exchange.getResponseHeaders().set("Warp-Result-Partitions", "3");
          exchange.getResponseHeaders().set("Warp-Result-Order", "interleaved");
          exchange.getResponseHeaders().set("Warp-IPC-Compression", "none");
          arrow(exchange, "q1", partition0);
        });
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1/result",
        (exchange, request, path) ->
            arrow(exchange, "q1", request.query.equals("partition=1") ? partition1 : partition2));
    try (Connection connection = connect("ipcCompression=LZ4_FRAME");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM big ORDER BY i")) {
      assertEquals(range(0, 25), readAll(rows));
    }
    FakeWarpServer.Request submit = server.requests("POST", "/v1/sessions/s1/statements").get(0);
    assertEquals("lz4_frame", submit.header("Warp-IPC-Compression"));
    List<FakeWarpServer.Request> fetches =
        server.requests("GET", "/v1/sessions/s1/statements/q1/result");
    List<String> partitions = new ArrayList<>();
    for (FakeWarpServer.Request fetch : fetches) {
      partitions.add(fetch.query);
      assertEquals("lz4_frame", fetch.header("Warp-IPC-Compression"), fetch.query);
    }
    partitions.sort(null);
    assertEquals(List.of("partition=1", "partition=2"), partitions);
  }

  /** A fetch that resumes a broken stream (spec §6.5) asks for the codec too. */
  @Test
  void aResumedFetchAsksForTheCodec() throws Exception {
    server.capabilities = BOTH_CODECS;
    byte[] firstThree = TestStreams.withoutEndMarker(stream(0, 3, 100));
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> arrow(exchange, "q1", firstThree));
    retainedStatement("q1", true, "succeeded", null);
    byte[] rest = stream(300, 2, 100);
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1/result",
        (exchange, request, path) -> arrow(exchange, "q1", rest));
    try (Connection connection = connect("retainResults=true&ipcCompression=zstd");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM big")) {
      assertEquals(range(0, 500), readAll(rows));
    }
    FakeWarpServer.Request resume =
        server.requests("GET", "/v1/sessions/s1/statements/q1/result").get(0);
    assertEquals("partition=0&from_batch=3", resume.query);
    assertEquals("zstd", resume.header("Warp-IPC-Compression"));
  }

  private String codecChosen(String options) throws SQLException {
    try (Connection connection = connect(options)) {
      return connection.unwrap(WarpConnection.class).client().resultCompression();
    }
  }

  private String headerSent(String options) throws SQLException {
    server.requests.clear();
    server.route(
        "POST",
        "/v1/sessions/[^/]+/statements",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 0)));
    try (Connection connection = connect(options);
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    }
    return server
        .requests("POST", "/v1/sessions/[^/]+/statements")
        .get(0)
        .header("Warp-IPC-Compression");
  }

  @Test
  void autoChoosesLz4OnANearLink() throws Exception {
    server.capabilities = BOTH_CODECS;
    assertEquals("lz4_frame", headerSent(""), "auto is the default");
    assertEquals("lz4_frame", headerSent("ipcCompression=auto"));
    assertTrue(
        codecChosenRoundTrip("ipcCompression=auto").compareTo(Duration.ofMillis(10)) < 0,
        "a loopback round trip");
  }

  private Duration codecChosenRoundTrip(String options) throws SQLException {
    try (Connection connection = connect(options)) {
      return connection.unwrap(WarpConnection.class).client().discoveryRoundTrip();
    }
  }

  @Test
  void autoChoosesZstdOnAFarLink() throws Exception {
    server.capabilities = BOTH_CODECS;
    server.discoveryDelayMillis = 25;
    assertEquals("zstd", headerSent("ipcCompression=auto"));
    assertEquals(
        2,
        server.requests("GET", "").size(),
        "a slow first discovery is timed again on the open connection");
    assertTrue(
        codecChosenRoundTrip("").compareTo(Duration.ofMillis(25)) >= 0,
        "the round trip includes the delay");
    // An explicit codec is not second-guessed, and costs no second discovery.
    server.requests.clear();
    assertEquals("lz4_frame", headerSent("ipcCompression=lz4_frame"));
    assertEquals(1, server.requests("GET", "").size());
  }

  @Test
  void aCodecTheServerDoesNotListIsNeverUsed() throws Exception {
    server.capabilities = "[\"prepared_statements\",\"ipc_compression_zstd\"]";
    assertNull(headerSent("ipcCompression=auto"), "auto on a near link wants lz4_frame");
    assertNull(headerSent("ipcCompression=lz4_frame"));
    assertEquals("zstd", headerSent("ipcCompression=zstd"));
    assertEquals("none", codecChosen("ipcCompression=lz4_frame"));
    server.capabilities = "[\"prepared_statements\",\"ipc_compression_lz4_frame\"]";
    server.discoveryDelayMillis = 25;
    assertNull(headerSent("ipcCompression=auto"), "auto on a far link wants zstd");
    assertNull(headerSent("ipcCompression=zstd"));
    assertEquals("lz4_frame", headerSent("ipcCompression=lz4_frame"));
    server.discoveryDelayMillis = 0;
    assertNull(headerSent("ipcCompression=none"));
  }

  @Test
  void aServerMountedElsewhere() throws Exception {
    try (FakeWarpServer mounted = new FakeWarpServer("/db/warp");
        Connection connection = DriverManager.getConnection(mounted.url(""))) {
      assertTrue(connection.isValid(1));
      assertEquals(1, mounted.requests("POST", "/v1/sessions").size());
    }
  }

  @Test
  void aLostPreparedStatementIsPreparedAgain() throws Exception {
    String schema =
        Base64.getEncoder()
            .encodeToString(
                TestStreams.schemaStream(
                    allocator,
                    new Schema(List.of(Field.nullable("1", new ArrowType.Int(32, true))))));
    AtomicInteger prepares = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/prepared",
        (exchange, request, path) ->
            json(
                exchange,
                201,
                "{\"prepared_id\":\"p"
                    + prepares.incrementAndGet()
                    + "\",\"parameter_schema\":\""
                    + schema
                    + "\",\"result_schema\":null}"));
    server.route(
        "POST",
        "/v1/sessions/s1/prepared/p1/execute",
        (exchange, request, path) -> problem(exchange, 404, "prepared_not_found", ""));
    server.route(
        "POST",
        "/v1/sessions/s1/prepared/p2/execute",
        (exchange, request, path) -> json(exchange, 200, updateDocument("q1", 1)));
    try (Connection connection = connect("");
        PreparedStatement statement = connection.prepareStatement("DELETE FROM t WHERE i = ?")) {
      assertNull(statement.getMetaData(), "the server sent no result schema");
      statement.setInt(1, 5);
      assertEquals(1, statement.executeUpdate());
    }
    assertEquals(2, prepares.get());
    FakeWarpServer.Request execute =
        server.requests("POST", "/v1/sessions/s1/prepared/p2/execute").get(0);
    assertEquals("application/vnd.apache.arrow.stream", execute.header("Content-Type"));
    assertEquals(
        1, server.requests("DELETE", "/v1/sessions/s1/prepared/p2").size(), "closing frees it");
  }

  @Test
  void cancelPostsToTheRunningStatement() throws Exception {
    AtomicBoolean cancelled = new AtomicBoolean();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 201, document("q1", "running", null)));
    server.route(
        "POST",
        "/v1/sessions/s1/statements/q1/cancel",
        (exchange, request, path) -> {
          cancelled.set(true);
          FakeWarpServer.empty(exchange, 202);
        });
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) -> {
          try {
            Thread.sleep(20);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          json(
              exchange,
              200,
              cancelled.get()
                  ? "{\"statement_id\":\"q1\",\"state\":\"cancelled\",\"cancel_reason\":\"client\","
                      + "\"error\":{\"code\":\"statement_cancelled\",\"detail\":\"cancelled\"}}"
                  : document("q1", "running", null));
        });
    try (Connection connection = connect("async=true");
        Statement statement = connection.createStatement()) {
      Thread canceller =
          new Thread(
              () -> {
                try {
                  Thread.sleep(300);
                  statement.cancel();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      canceller.start();
      SQLException error =
          assertThrows(SQLException.class, () -> statement.execute("SELECT slow()"));
      canceller.join();
      assertEquals("statement_cancelled", WarpProblem.codeOf(error));
      assertEquals("57014", error.getSQLState());
    }
  }

  @Test
  void cancelBeforeTheSubmissionAnswersCancelsTheSession() throws Exception {
    CountDownLatch sessionCancelled = new CountDownLatch(1);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          try {
            if (!sessionCancelled.await(10, TimeUnit.SECONDS)) {
              json(exchange, 201, document("q1", "succeeded", null));
              return;
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          exchange.getResponseHeaders().set("Warp-Statement-Id", "q1");
          problem(exchange, 409, "statement_cancelled", ",\"cancel_reason\":\"client\"");
        });
    server.route(
        "POST",
        "/v1/sessions/s1/cancel",
        (exchange, request, path) -> {
          sessionCancelled.countDown();
          FakeWarpServer.empty(exchange, 202);
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      Thread canceller =
          new Thread(
              () -> {
                try {
                  Thread.sleep(300);
                  statement.cancel();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      canceller.start();
      SQLException error =
          assertThrows(SQLException.class, () -> statement.execute("SELECT slow()"));
      canceller.join();
      assertEquals("statement_cancelled", WarpProblem.codeOf(error));
      assertEquals("57014", error.getSQLState());
      assertEquals("client", WarpProblem.of(error).cancelReason());
    }
    assertEquals(1, server.requests("POST", "/v1/sessions/s1/cancel").size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "async=true"})
  void aStatementCancelledByTheTimeItsSubmitIsAnsweredIsAnError(String options) throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          exchange.getResponseHeaders().set("Warp-Statement-Id", "q1");
          problem(exchange, 409, "statement_cancelled", ",\"cancel_reason\":\"killed\"");
        });
    try (Connection connection = connect(options);
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.execute("SELECT slow()"));
      assertEquals("statement_cancelled", WarpProblem.codeOf(error));
      assertEquals("killed", WarpProblem.of(error).cancelReason());
      assertEquals("57014", error.getSQLState());
      assertEquals(409, error.getErrorCode());
      assertTrue(error.getMessage().contains("killed"), error.getMessage());
      assertTrue(connection.isValid(1), "the session goes on");
    }
    assertEquals(0, server.requests("GET", "/v1/sessions/s1/statements/q1").size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "async=true"})
  void aFailedDocumentIsAnErrorWhateverTheStatus(String options) throws Exception {
    // An idempotent replay answers 200 with the statement's document in any state (spec §6.7).
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"statement_id\":\"q1\",\"state\":\"failed\",\"error\":{\"code\":"
                    + "\"execution_error\",\"status\":422,\"detail\":\"division by zero\"}}"));
    try (Connection connection = connect(options);
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.execute("SELECT 1 / 0"));
      assertEquals("execution_error", WarpProblem.codeOf(error));
      assertTrue(error.getMessage().contains("division by zero"), error.getMessage());
    }
  }

  @Test
  void anUnknownStateKeepsPolling() throws Exception {
    AtomicInteger polls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> json(exchange, 201, document("q1", "paused", null)));
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                polls.incrementAndGet() < 3
                    ? document("q1", "warming_up", null)
                    : updateDocument("q1", 5)));
    try (Connection connection = connect("async=true");
        Statement statement = connection.createStatement()) {
      assertEquals(5, statement.executeUpdate("UPDATE t SET a = 1"));
    }
    assertEquals(3, polls.get(), "a state from a later version is not terminal (spec §12)");
  }

  @Test
  void refusedSessionOptionsAreErrors() throws Exception {
    server.route(
        "PATCH",
        "/v1/sessions/s1",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"options\":{\"schema\":\"main\"},\"errors\":{\"schema\":{\"code\":\"invalid_option_value\","
                    + "\"detail\":\"no schema x\"}}}"));
    try (Connection connection = connect("")) {
      SQLException error = assertThrows(SQLException.class, () -> connection.setSchema("x"));
      assertEquals("HY024", error.getSQLState());
      assertEquals("no schema x [invalid_option_value]", error.getMessage());
    }
  }

  @Test
  void updateCountsBeyondAnIntSaturate() throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            json(exchange, 200, updateDocument("q1", 0).replace("0}", "5000000000}")));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertEquals(-1, statement.getUpdateCount(), "nothing ran yet");
      assertNull(statement.getResultSet());
      assertEquals(Integer.MAX_VALUE, statement.executeUpdate("UPDATE huge SET a = 1"));
      assertEquals(5_000_000_000L, statement.getLargeUpdateCount());
      assertEquals(5_000_000_000L, statement.executeLargeUpdate("UPDATE huge SET a = 1"));
    }
  }

  @Test
  void statementArgumentsAreChecked() throws Exception {
    try (Connection connection = connect("")) {
      Statement statement = connection.createStatement();
      assertEquals(
          "HY009", assertThrows(SQLException.class, () -> statement.execute(null)).getSQLState());
      assertEquals(
          "HY009",
          assertThrows(SQLException.class, () -> connection.prepareStatement(null)).getSQLState());
      assertThrows(
          java.sql.SQLFeatureNotSupportedException.class,
          () -> statement.executeUpdate("INSERT INTO t VALUES (1)", new int[] {1}));
      assertThrows(
          java.sql.SQLFeatureNotSupportedException.class,
          () -> statement.execute("INSERT INTO t VALUES (1)", Statement.RETURN_GENERATED_KEYS));
      assertThrows(
          java.sql.SQLFeatureNotSupportedException.class,
          () -> connection.prepareStatement("INSERT INTO t VALUES (1)", new String[] {"id"}));
      assertThrows(java.sql.SQLFeatureNotSupportedException.class, statement::getGeneratedKeys);
      statement.close();
      assertEquals(
          "HY010",
          assertThrows(SQLException.class, () -> statement.execute("SELECT 1")).getSQLState());
      assertTrue(statement.isClosed());
    }
  }

  @Test
  void serverInfoKeysDriveDatabaseMetaData() throws Exception {
    server.route(
        "GET",
        "/v1/sessions/s1/metadata/server-info",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"server_name\":\"Acme SQL\",\"server_version\":\"3.14.2-beta\",\"sql_identifier_quote_char\":\"`\","
                    + "\"sql_keywords\":[\"QUALIFY\",\"PIVOT\"],\"sql_identifier_case\":3,"
                    + "\"sql_null_ordering\":3,\"sql_default_transaction_isolation\":4,"
                    + "\"sql_transactions_supported\":false,\"server_read_only\":true}"));
    try (Connection connection = connect("")) {
      java.sql.DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("Acme SQL", metaData.getDatabaseProductName());
      assertEquals("3.14.2-beta", metaData.getDatabaseProductVersion());
      assertEquals(3, metaData.getDatabaseMajorVersion());
      assertEquals(14, metaData.getDatabaseMinorVersion());
      assertEquals("`", metaData.getIdentifierQuoteString());
      assertEquals("QUALIFY,PIVOT", metaData.getSQLKeywords());
      assertTrue(metaData.storesLowerCaseIdentifiers());
      assertFalse(metaData.storesMixedCaseIdentifiers());
      assertTrue(metaData.nullsAreSortedAtEnd());
      assertFalse(metaData.nullsAreSortedHigh());
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, metaData.getDefaultTransactionIsolation());
      assertFalse(metaData.supportsTransactions());
      assertTrue(metaData.isReadOnly());
    }
    assertEquals(
        1, server.requests("GET", "/v1/sessions/s1/metadata/server-info").size(), "read once");
  }

  @Test
  void retiredServerInfoKeysAreIgnored() throws Exception {
    server.route(
        "GET",
        "/v1/sessions/s1/metadata/server-info",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"read_only\":true,\"transactions_supported\":false,"
                    + "\"sql_max_schema_name_length\":9,\"duckdb_version\":\"v2.0.0\"}"));
    try (Connection connection = connect("")) {
      java.sql.DatabaseMetaData metaData = connection.getMetaData();
      assertFalse(metaData.isReadOnly(), "only server_read_only says so");
      assertTrue(metaData.supportsTransactions(), "only sql_transactions_supported says no");
      assertEquals(0, metaData.getMaxSchemaNameLength(), "only sql_db_schema_name_length");
    }
  }

  @Test
  void withoutServerInfoKeysTheDefaultsHold() throws Exception {
    server.route(
        "GET",
        "/v1/sessions/s1/metadata/server-info",
        (exchange, request, path) -> json(exchange, 200, "{}"));
    try (Connection connection = connect("")) {
      java.sql.DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("fake", metaData.getDatabaseProductName(), "the discovery document's name");
      assertEquals("9.8.7", metaData.getDatabaseProductVersion());
      assertEquals("\"", metaData.getIdentifierQuoteString());
      assertTrue(metaData.supportsTransactions(), "from the transactions capability");
      assertEquals(
          Connection.TRANSACTION_READ_COMMITTED, metaData.getDefaultTransactionIsolation());
      assertTrue(metaData.nullsAreSortedHigh());
    }
  }

  // ---- Discovery and connecting (spec §3) ----

  @Test
  void anotherProtocolVersionIsRefused() {
    server.protocolVersions = "[\"2\"]";
    SQLException error = assertThrows(SQLNonTransientConnectionException.class, () -> connect(""));
    assertEquals("08001", error.getSQLState());
    assertTrue(error.getMessage().contains("version 1"), error.getMessage());
  }

  @Test
  void aProxyErrorPageIsAnHttpError() {
    server.route(
        "GET",
        "",
        (exchange, request, path) ->
            FakeWarpServer.send(
                exchange,
                502,
                "text/html",
                "<h1>Bad Gateway</h1>".getBytes(StandardCharsets.UTF_8)));
    SQLException error = assertThrows(SQLException.class, () -> connect(""));
    assertEquals("client_http_error", WarpProblem.codeOf(error));
    assertEquals(502, error.getErrorCode());
    assertTrue(error.getMessage().contains("Bad Gateway"), error.getMessage());
  }

  @Test
  void aSilentServerTimesOutWhileConnecting() {
    server.route(
        "GET",
        "",
        (exchange, request, path) -> {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
    SQLTimeoutException error =
        assertTimeoutPreemptively(
            Duration.ofSeconds(4),
            () -> assertThrows(SQLTimeoutException.class, () -> connect("connectTimeout=300")));
    assertEquals("HYT01", error.getSQLState());
  }

  @Test
  void wrongCredentialsAreRefused() {
    SQLException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () ->
                DriverManager.getConnection(
                    server.url("").replace("password=pw", "password=wrong")));
    assertEquals("invalid_credentials", WarpProblem.codeOf(error));
  }

  @Test
  void closeEndsTheSession() throws Exception {
    Connection connection = connect("");
    connection.close();
    connection.close();
    assertEquals(1, server.requests("DELETE", "/v1/sessions/s1").size());
  }
}
