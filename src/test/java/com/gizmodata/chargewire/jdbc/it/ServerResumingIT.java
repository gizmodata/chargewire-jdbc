package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireConnection;
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

  // ---- A connection made to a stopped server: where its session opens ----

  private static String place(Statement statement) throws SQLException {
    try (ResultSet rows =
        statement.executeQuery("SELECT current_catalog() || '.' || current_schema()")) {
      assertTrue(rows.next());
      return rows.getString(1);
    }
  }

  /** Two catalogs: {@code place_a} with the schema {@code only_a}, {@code place_b} without it. */
  private static void createPlaces() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("ATTACH IF NOT EXISTS ':memory:' AS place_a");
      statement.execute("ATTACH IF NOT EXISTS ':memory:' AS place_b");
      statement.execute("CREATE SCHEMA IF NOT EXISTS place_a.only_a");
      statement.execute("CREATE SCHEMA IF NOT EXISTS place_a.both");
      statement.execute("CREATE SCHEMA IF NOT EXISTS place_b.both");
    }
  }

  /** What a connection does before its first statement. */
  @FunctionalInterface
  private interface Settings {
    void apply(Connection connection) throws SQLException;
  }

  /**
   * The same settings on a connection to a serving server (each one sent as it is made) and on one
   * to a stopped server (kept, and sent when the session opens) must leave the session in the same
   * place; and the session that replaces a lost one must open there too.
   */
  private void assertSamePlace(String connectOptions, Settings settings, String expected)
      throws Exception {
    createPlaces();
    for (boolean stopped : new boolean[] {false, true}) {
      standIn.stopped(stopped);
      String how = stopped ? "to a stopped server" : "to a serving server";
      try (Connection connection = connect(connectOptions);
          Statement statement = connection.createStatement()) {
        String sessionAtConnect = connection.unwrap(ChargewireConnection.class).getSessionId();
        assertEquals(stopped, sessionAtConnect == null, how);
        settings.apply(connection);
        assertEquals(expected, place(statement), "the first statement " + how);
        assertEquals(expected, connection.getCatalog() + "." + connection.getSchema(), how);
        // The driver replaces a lost session only when it knows no transaction was open: it read
        // the session (through isValid) after the last statement.
        assertTrue(connection.isValid(5));
        String lost = connection.unwrap(ChargewireConnection.class).getSessionId();
        assertEquals(204, new RawChargewire(server).deleteSession(lost));
        assertEquals(expected, place(statement), "after the session was lost, " + how);
        assertNotEquals(lost, connection.unwrap(ChargewireConnection.class).getSessionId());
        assertEquals(expected, place(statement), "and the statement after, " + how);
      }
    }
  }

  /**
   * Setting the catalog moves the session to that catalog's default schema, as {@code USE} does: a
   * schema set before it must not be sent with the session. {@code place_b} has no schema {@code
   * only_a}, so sending both would refuse every statement.
   */
  @Test
  void aCatalogSetAfterASchemaOpensInTheCatalogsDefaultSchema() throws Exception {
    assertSamePlace(
        "catalog=place_a",
        connection -> {
          connection.setSchema("only_a");
          connection.setCatalog("place_b");
        },
        "place_b.main");
  }

  /** Both catalogs have the schema: sending both would open in it, with no error to notice. */
  @Test
  void aCatalogSetAfterASchemaBothCatalogsHaveDoesNotOpenInThatSchema() throws Exception {
    assertSamePlace(
        "catalog=place_a",
        connection -> {
          connection.setSchema("both");
          connection.setCatalog("place_b");
        },
        "place_b.main");
  }

  @Test
  void aSchemaGivenAtConnectIsForgottenWhenTheCatalogIsSet() throws Exception {
    assertSamePlace(
        "catalog=place_a&schema=only_a",
        connection -> connection.setCatalog("place_b"),
        "place_b.main");
  }

  @Test
  void aSchemaSetAfterTheCatalogIsKept() throws Exception {
    assertSamePlace(
        "",
        connection -> {
          connection.setCatalog("place_a");
          connection.setSchema("only_a");
        },
        "place_a.only_a");
    assertSamePlace("catalog=place_a&schema=both", connection -> {}, "place_a.both");
    assertSamePlace(
        "catalog=place_a&schema=both",
        connection -> {
          connection.setCatalog("place_b");
          connection.setSchema("both");
          connection.setCatalog("place_a");
          connection.setSchema("only_a");
        },
        "place_a.only_a");
  }

  /** A schema the server refuses fails where it is sent, and leaves the place as it was. */
  @Test
  void aRefusedSchemaDoesNotMoveTheSession() throws Exception {
    createPlaces();
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      connection.setCatalog("place_b");
      SQLException refused = assertThrows(SQLException.class, () -> connection.setSchema("only_a"));
      assertEquals("HY024", refused.getSQLState());
      assertTrue(refused.getMessage().contains("place_b.only_a"), refused.getMessage());
      assertEquals("place_b.main", place(statement));
      assertTrue(connection.isValid(5));
      assertEquals(
          204,
          new RawChargewire(server)
              .deleteSession(connection.unwrap(ChargewireConnection.class).getSessionId()));
      assertEquals("place_b.main", place(statement), "the replacement opens where it was");
    }
    // To a stopped server nothing is sent until the first statement, which then fails with the
    // error the setting would have given; setting a schema that exists lets the connection open.
    standIn.stopped(true);
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      connection.setCatalog("place_b");
      connection.setSchema("only_a");
      SQLException refused = assertThrows(SQLException.class, () -> place(statement));
      assertTrue(refused.getMessage().contains("place_b.only_a"), refused.getMessage());
      connection.setSchema("both");
      assertEquals("place_b.both", place(statement));
    }
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
