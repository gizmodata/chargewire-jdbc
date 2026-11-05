package com.gizmodata.chargewire.jdbc;

import static com.gizmodata.chargewire.jdbc.FakeChargewireServer.document;
import static com.gizmodata.chargewire.jdbc.FakeChargewireServer.json;
import static com.gizmodata.chargewire.jdbc.FakeChargewireServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.client.ChargewireClient;
import com.gizmodata.chargewire.jdbc.client.ResumeClock;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A server starting again after a stop answers {@code server_resuming} (HTTP 503 with {@code
 * Retry-After}) and may hold the login: the driver waits and repeats the same request until the
 * {@code resumeTimeout} deadline. The pauses go through a fake clock, so these tests take
 * milliseconds; one test holds a real login for 25 s, and a few wait a few real seconds.
 */
class ServerResumingTest {
  /** Records every pause and moves its time forward by it, instead of sleeping. */
  static class FakeClock implements ResumeClock {
    final AtomicLong now = new AtomicLong(1_000_000_000L);
    final List<Duration> pauses = new CopyOnWriteArrayList<>();

    @Override
    public long nanoTime() {
      return now.get();
    }

    @Override
    public void sleep(Duration pause) {
      pauses.add(pause);
      now.addAndGet(pause.toNanos());
    }

    Duration waited() {
      return pauses.stream().reduce(Duration.ZERO, Duration::plus);
    }
  }

  private final FakeChargewireServer server;
  private final FakeClock clock = new FakeClock();
  private ResumeClock previous;

  ServerResumingTest() throws Exception {
    server = new FakeChargewireServer();
  }

  @BeforeEach
  void useFakeClock() {
    previous = ChargewireClient.setResumeClockForTesting(clock);
  }

  @AfterEach
  void close() {
    ChargewireClient.setResumeClockForTesting(previous);
    server.close();
  }

  private Connection connect(String options) throws SQLException {
    return DriverManager.getConnection(server.url(options));
  }

  /** Answers {@code server_resuming}, with {@code Retry-After} unless it is {@code null}. */
  private static void resuming(HttpExchange exchange, String retryAfter) throws IOException {
    if (retryAfter != null) {
      exchange.getResponseHeaders().set("Retry-After", retryAfter);
    }
    problem(exchange, 503, "server_resuming", "");
  }

  /** Makes the first {@code times} logins answer {@code server_resuming}. */
  private AtomicInteger resumingLogins(int times, String retryAfter) {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/auth/token",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() <= times) {
            resuming(exchange, retryAfter);
          } else {
            json(exchange, 200, "{\"access_token\":\"t1\",\"expires_in\":3600}");
          }
        });
    return calls;
  }

  private static void assertResumingError(SQLException error) {
    assertInstanceOf(SQLTransientConnectionException.class, error);
    assertEquals("08001", error.getSQLState());
    assertEquals(503, error.getErrorCode());
    assertEquals("server_resuming", ChargewireProblem.codeOf(error));
    assertTrue(
        error.getMessage().startsWith("fake server_resuming [server_resuming]"),
        error.getMessage());
  }

  @Test
  void theLoginIsRepeatedUntilTheServerHasResumed() throws Exception {
    AtomicInteger logins = resumingLogins(3, "5");
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertFalse(connection.isClosed());
    }
    assertEquals(4, logins.get());
    assertEquals(List.of(secs(5), secs(5), secs(5)), clock.pauses);
    // The same request each time: same body, so the same credential.
    List<FakeChargewireServer.Request> tokens = server.requests("POST", "/v1/auth/token");
    assertEquals(4, tokens.size());
    for (FakeChargewireServer.Request token : tokens) {
      assertEquals(tokens.get(0).body(), token.body());
      assertTrue(token.body().contains("\"password\":\"pw\""), token.body());
    }
  }

  @Test
  void aStatementIsRepeatedUntilTheServerHasResumed() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() <= 2) {
            resuming(exchange, "2");
          } else {
            json(
                exchange,
                200,
                document("q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":3}"));
          }
        });
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertEquals(3, statement.executeUpdate("UPDATE t SET x = 1"));
    }
    assertEquals(List.of(secs(2), secs(2)), clock.pauses);
    List<FakeChargewireServer.Request> submits =
        server.requests("POST", "/v1/sessions/s1/statements");
    assertEquals(3, submits.size());
    // Repeated as it was: the same statement under the same Idempotency-Key.
    for (FakeChargewireServer.Request submit : submits) {
      assertEquals(submits.get(0).header("Idempotency-Key"), submit.header("Idempotency-Key"));
      assertEquals(submits.get(0).body(), submit.body());
    }
    assertEquals(1, server.tokens.get(), "the login was not repeated");
  }

  @Test
  void theDeadlineReportsTheServersError() throws Exception {
    AtomicInteger logins = resumingLogins(Integer.MAX_VALUE, "4");
    SQLException error = assertThrows(SQLException.class, () -> connect("resumeTimeout=10"));
    assertResumingError(error);
    assertTrue(error.getMessage().contains("still resuming after 10 s"), error.getMessage());
    // 4 + 4, then the 2 s left before the deadline, then one last try.
    assertEquals(List.of(secs(4), secs(4), secs(2)), clock.pauses);
    assertEquals(4, logins.get());
  }

  @Test
  void theDeadlineAlsoEndsAStatementsRetries() throws Exception {
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> resuming(exchange, "60"));
    try (Connection connection = connect("resumeTimeout=120");
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.executeUpdate("UPDATE t SET x = 1"));
      assertResumingError(error);
      assertTrue(error.getMessage().contains("still resuming after 120 s"), error.getMessage());
    }
    assertEquals(secs(120), clock.waited());
    assertEquals(3, server.requests("POST", "/v1/sessions/s1/statements").size());
  }

  @Test
  void withoutRetryAfterTheDriverWaitsThreeSeconds() throws Exception {
    resumingLogins(1, null);
    try (Connection connection = connect("")) {
      assertFalse(connection.isClosed());
    }
    assertEquals(List.of(secs(3)), clock.pauses);
  }

  @Test
  void retryAfterZeroStillWaitsASecondSoTheRetriesNeverSpin() throws Exception {
    resumingLogins(2, "0");
    try (Connection connection = connect("")) {
      assertFalse(connection.isClosed());
    }
    assertEquals(List.of(secs(1), secs(1)), clock.pauses);
  }

  @Test
  void aLongRetryAfterIsCappedAtSixtySeconds() throws Exception {
    resumingLogins(2, "3600");
    try (Connection connection = connect("")) {
      assertFalse(connection.isClosed());
    }
    assertEquals(List.of(secs(60), secs(60)), clock.pauses);
  }

  @Test
  void zeroResumeTimeoutReportsTheFirstAnswer() throws Exception {
    AtomicInteger logins = resumingLogins(1, "1");
    SQLException error = assertThrows(SQLException.class, () -> connect("resumeTimeout=0"));
    assertResumingError(error);
    assertFalse(error.getMessage().contains("still resuming"), error.getMessage());
    assertEquals(1, logins.get());
    assertEquals(List.of(), clock.pauses);
  }

  @Test
  void anotherUnavailableAnswerIsNotRetried() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/auth/token",
        (exchange, request, path) -> {
          calls.incrementAndGet();
          exchange.getResponseHeaders().set("Retry-After", "1");
          problem(exchange, 503, "server_draining", "");
        });
    SQLException error = assertThrows(SQLException.class, () -> connect(""));
    assertEquals("server_draining", ChargewireProblem.codeOf(error));
    assertEquals("08004", error.getSQLState());
    assertEquals(1, calls.get());
    assertEquals(List.of(), clock.pauses);
  }

  @Test
  void aLoginHeldForTwentyFiveSecondsSucceedsDespiteAShortConnectTimeout() throws Exception {
    ChargewireClient.setResumeClockForTesting(previous);
    server.route(
        "POST",
        "/v1/auth/token",
        (exchange, request, path) -> {
          try {
            Thread.sleep(25_000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          json(exchange, 200, "{\"access_token\":\"t1\",\"expires_in\":3600}");
        });
    long started = System.nanoTime();
    assertTimeoutPreemptively(
        Duration.ofSeconds(45),
        () -> {
          // connectTimeout (10 s here) bounds TCP and the other requests while connecting, not a
          // login a resuming server holds.
          try (Connection connection = connect("connectTimeout=10000")) {
            assertFalse(connection.isClosed());
          }
        });
    assertTrue(Duration.ofNanos(System.nanoTime() - started).toSeconds() >= 25);
  }

  @Test
  void aShortNetworkTimeoutDoesNotCutAHeldStatementShort() throws Exception {
    // A resuming server may hold any answer for up to 25 s (spec §11.2), so every request waits
    // at least 30 s for its answer, whatever networkTimeout says (held for 3 s here, with 1 s).
    ChargewireClient.setResumeClockForTesting(previous);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          try {
            Thread.sleep(3_000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          json(
              exchange,
              200,
              document("q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":1}"));
        });
    assertTimeoutPreemptively(
        Duration.ofSeconds(20),
        () -> {
          try (Connection connection = connect("networkTimeout=1000");
              Statement statement = connection.createStatement()) {
            assertEquals(1000, connection.getNetworkTimeout(), "the setting itself is kept");
            assertEquals(1, statement.executeUpdate("UPDATE t SET x = 1"));
          }
        });
  }

  @Test
  void aCommitIsRepeatedUntilTheServerHasResumed() throws Exception {
    // server_resuming means the request had no effect, so even a commit is sent again (spec
    // §11.2), although a commit whose answer was lost never is.
    AtomicInteger calls = new AtomicInteger();
    server.route(
        "POST",
        "/v1/sessions/s1/commit",
        (exchange, request, path) -> {
          if (calls.incrementAndGet() <= 2) {
            resuming(exchange, "2");
          } else {
            FakeChargewireServer.empty(exchange, 204);
          }
        });
    server.route(
        "PATCH",
        "/v1/sessions/s1",
        (exchange, request, path) ->
            json(exchange, 200, "{\"options\":{\"autocommit\":false},\"errors\":{}}"));
    try (Connection connection = connect("")) {
      connection.setAutoCommit(false);
      connection.commit();
    }
    assertEquals(List.of(secs(2), secs(2)), clock.pauses);
    assertEquals(3, calls.get());
  }

  @Test
  void isValidStopsRetryingAtItsOwnTimeout() throws Exception {
    try (Connection connection = connect("")) {
      server.route("GET", "/v1/sessions/s1", (exchange, request, path) -> resuming(exchange, "5"));
      assertFalse(connection.isValid(3));
      // A pool's probe must answer within its timeout, not wait out resumeTimeout (600 s).
      assertEquals(List.of(secs(3)), clock.pauses);
      assertEquals(2, server.requests("GET", "/v1/sessions/s1").size());
    }
  }

  @Test
  void closingAConnectionOnAResumingServerDoesNotWait() throws Exception {
    Connection connection = connect("");
    server.route("DELETE", "/v1/sessions/s1", (exchange, request, path) -> resuming(exchange, "5"));
    connection.close();
    assertTrue(connection.isClosed());
    // No session survives a stop: ending it again after the resume would be pointless.
    assertEquals(List.of(), clock.pauses);
    assertEquals(1, server.requests("DELETE", "/v1/sessions/s1").size());
  }

  @Test
  void cleanupRequestsAreNotRepeatedWhileTheServerResumes() throws Exception {
    try (Connection connection = connect("")) {
      ChargewireClient client = ((ChargewireConnection) connection).client();
      server.route(
          "DELETE", "/v1/sessions/s1/.+", (exchange, request, path) -> resuming(exchange, "5"));
      server.route(
          "POST", "/v1/sessions/s1/.*cancel", (exchange, request, path) -> resuming(exchange, "5"));
      // Freeing a result or a prepared statement: gone with the stop, so already done.
      client.deleteStatement("s1", "q1");
      client.closePrepared("s1", "p1");
      // A cancel reports the answer at once: the statement it was for did not survive the stop.
      assertEquals(
          "server_resuming",
          ChargewireProblem.codeOf(
              assertThrows(SQLException.class, () -> client.cancel("s1", "q1"))));
      assertEquals(
          "server_resuming",
          ChargewireProblem.codeOf(
              assertThrows(SQLException.class, () -> client.cancelSession("s1"))));
      assertEquals(List.of(), clock.pauses);
      assertEquals(2, server.requests("DELETE", "/v1/sessions/s1/.+").size());
      assertEquals(2, server.requests("POST", "/v1/sessions/s1/.*cancel").size());
    }
  }

  @Test
  void theQueryTimeoutEndsAStatementsRetries() throws Exception {
    server.route(
        "POST", "/v1/sessions/s1/statements", (exchange, request, path) -> resuming(exchange, "5"));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(12);
      SQLTimeoutException error =
          assertThrows(
              SQLTimeoutException.class, () -> statement.executeUpdate("UPDATE t SET x = 1"));
      assertEquals("HYT00", error.getSQLState());
      assertEquals("server_resuming", ChargewireProblem.codeOf(error));
      assertTrue(error.getMessage().contains("query timeout"), error.getMessage());
    }
    assertEquals(List.of(secs(5), secs(5), secs(2)), clock.pauses);
    assertEquals(4, server.requests("POST", "/v1/sessions/s1/statements").size());
  }

  @Test
  void aCancelEndsAStatementsRetries() throws Exception {
    AtomicReference<Statement> running = new AtomicReference<>();
    // Statement.cancel() from another thread, arriving during the first pause.
    ChargewireClient.setResumeClockForTesting(
        new FakeClock() {
          @Override
          public void sleep(Duration pause) {
            super.sleep(pause);
            try {
              running.get().cancel();
            } catch (SQLException e) {
              throw new AssertionError(e);
            }
          }
        });
    server.route(
        "POST", "/v1/sessions/s1/statements", (exchange, request, path) -> resuming(exchange, "5"));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      running.set(statement);
      SQLException error =
          assertThrows(SQLException.class, () -> statement.executeUpdate("UPDATE t SET x = 1"));
      assertEquals("57014", error.getSQLState());
      assertTrue(error.getMessage().contains("cancelled"), error.getMessage());
    }
    assertEquals(1, server.requests("POST", "/v1/sessions/s1/statements").size());
  }

  @Test
  void aCancelEndsARealPauseEarly() throws Exception {
    ChargewireClient.setResumeClockForTesting(previous);
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> resuming(exchange, "60"));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      Thread canceller =
          new Thread(
              () -> {
                try {
                  while (server.requests("POST", "/v1/sessions/s1/statements").isEmpty()) {
                    Thread.sleep(20);
                  }
                  Thread.sleep(300);
                  statement.cancel();
                } catch (InterruptedException | SQLException e) {
                  throw new AssertionError(e);
                }
              });
      canceller.start();
      long started = System.nanoTime();
      SQLException error =
          assertTimeoutPreemptively(
              Duration.ofSeconds(10),
              () ->
                  assertThrows(
                      SQLException.class, () -> statement.executeUpdate("UPDATE t SET x = 1")));
      canceller.join();
      assertEquals("57014", error.getSQLState());
      // Not the 60 s Retry-After: the pause looks at the cancel every 100 ms.
      assertTrue(Duration.ofNanos(System.nanoTime() - started).toSeconds() < 5);
    }
  }

  private static Duration secs(long seconds) {
    return Duration.ofSeconds(seconds);
  }
}
