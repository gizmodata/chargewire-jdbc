package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireProblem;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A server starting again after a stop (spec §11.2), end to end: a stand-in in front of a real
 * server answers {@code 503 server_resuming} with {@code Retry-After: 1} a few times, holding some
 * answers, before letting requests through. The driver must repeat the same sign-in or statement
 * until the server answers, with real waits.
 */
class ServerResumingIT {
  private static final String TOKEN = "POST /chargewire/v1/auth/token";
  private static final String STATEMENTS = "POST /chargewire/v1/sessions/[^/]+/statements";

  private ChargewireServer server;
  private ResumingStandIn standIn;

  @BeforeEach
  void start() throws Exception {
    server = SharedServer.get();
    standIn = new ResumingStandIn(server.httpBase());
  }

  @AfterEach
  void stop() {
    standIn.close();
  }

  private Connection connect(String options) throws SQLException {
    return DriverManager.getConnection(standIn.url(options), server.properties());
  }

  private static long elapsedMillis(long startedNanos) {
    return Duration.ofNanos(System.nanoTime() - startedNanos).toMillis();
  }

  @Test
  void theSignInIsRepeatedUntilTheServerHasResumed() throws Exception {
    // One answer held 3 s: longer than this connection's connectTimeout and networkTimeout (1 s),
    // which must not cut a held answer short.
    standIn.resume(TOKEN, 1, "1", Duration.ofSeconds(3));
    long started = System.nanoTime();
    assertTimeoutPreemptively(
        Duration.ofSeconds(60),
        () -> {
          try (Connection connection = connect("connectTimeout=1000&networkTimeout=1000");
              Statement statement = connection.createStatement();
              ResultSet rows = statement.executeQuery("SELECT 41 + 1 AS answer")) {
            assertTrue(rows.next());
            assertEquals(42, rows.getInt(1));
          }
        });
    // The held answer (3 s), then a 1 s wait before the sign-in that got through.
    assertTrue(elapsedMillis(started) >= 4_000, "waited " + elapsedMillis(started) + " ms");
    List<ResumingStandIn.Seen> tokens = standIn.seen(TOKEN);
    assertEquals(2, tokens.size(), "one held server_resuming, then the real sign-in");
    assertTrue(tokens.get(0).answeredResuming());
    assertFalse(tokens.get(1).answeredResuming());
    assertEquals(tokens.get(0).body(), tokens.get(1).body(), "the same sign-in, sent again");
  }

  @Test
  void severalAnswersAreWaitedOut() throws Exception {
    standIn.resume(TOKEN, 3, "1", Duration.ZERO);
    long started = System.nanoTime();
    try (Connection connection = connect("");
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 'resumed' AS state")) {
      assertTrue(rows.next());
      assertEquals("resumed", rows.getString(1));
    }
    assertTrue(elapsedMillis(started) >= 3_000, "waited " + elapsedMillis(started) + " ms");
    List<ResumingStandIn.Seen> tokens = standIn.seen(TOKEN);
    assertEquals(4, tokens.size());
    for (ResumingStandIn.Seen token : tokens) {
      assertEquals(tokens.get(0).body(), token.body());
    }
  }

  @Test
  void aStatementIsRepeatedUntilTheServerHasResumed() throws Exception {
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE resumed_writes (i INTEGER)");
      standIn.resume(STATEMENTS, 2, "1", Duration.ZERO);
      long started = System.nanoTime();
      assertEquals(1, statement.executeUpdate("INSERT INTO resumed_writes VALUES (7)"));
      assertTrue(elapsedMillis(started) >= 2_000, "waited " + elapsedMillis(started) + " ms");
      // Answered server_resuming twice, so not run then: the row is there once.
      try (ResultSet rows = statement.executeQuery("SELECT count(*), sum(i) FROM resumed_writes")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getLong(1));
        assertEquals(7, rows.getLong(2));
      }
    }
    List<ResumingStandIn.Seen> inserts =
        standIn.seen(STATEMENTS).stream().filter(s -> s.body().contains("INSERT")).toList();
    assertEquals(3, inserts.size());
    assertEquals(inserts.get(0).body(), inserts.get(2).body());
  }

  @Test
  void withResumeTimeoutZeroTheFirstAnswerIsTheError() throws Exception {
    standIn.resume(TOKEN, 1, "1", Duration.ZERO);
    SQLException error = assertThrows(SQLException.class, () -> connect("resumeTimeout=0"));
    assertInstanceOf(SQLTransientConnectionException.class, error);
    assertEquals("08001", error.getSQLState());
    assertEquals("server_resuming", ChargewireProblem.codeOf(error));
    assertTrue(error.getMessage().contains("the server is resuming"), error.getMessage());
    assertEquals(1, standIn.seen(TOKEN).size());
  }
}
