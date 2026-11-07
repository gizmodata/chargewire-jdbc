package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireConnection;
import com.gizmodata.chargewire.jdbc.ChargewireProblem;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Sessions the server ends on its own (spec §5.6, §5.7): the idle sweep and the idle-in-transaction
 * limit. A session that held a transaction open ends with {@code session_ended}, reason {@code
 * idle_in_transaction}, either way; one evicted without a transaction is gone ({@code
 * session_not_found}), and the driver can't always tell whether it had one. The transaction's work
 * is rolled back, so the statement that finds the session gone must never run again on a new
 * session in autocommit, where it would commit on its own. Each test runs its own server with a
 * one-second limit.
 */
class IdleSessionsIT {
  /** A one-second limit, the server's sweep (every few seconds), and room to spare. */
  private static final long PAST_THE_LIMIT_MS = 8_000;

  private static Connection connect(ChargewireServer server) throws SQLException {
    return DriverManager.getConnection(server.url(""), server.properties());
  }

  private static String sessionOf(Connection connection) throws SQLException {
    return connection.unwrap(ChargewireConnection.class).getSessionId();
  }

  /** The table's rows, read on a connection of its own. */
  private static List<String> rows(ChargewireServer server, String table) throws SQLException {
    List<String> rows = new ArrayList<>();
    try (Connection connection = connect(server);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery("SELECT who, v FROM " + table + " ORDER BY ALL")) {
      while (result.next()) {
        rows.add(result.getString(1) + " " + result.getInt(2));
      }
    }
    return rows;
  }

  @Test
  void aTransactionLeftIdleIsRolledBackAndTheStatementIsNotRunAgain() throws Exception {
    try (ChargewireServer server =
            ChargewireServer.start(ChargewireServer.IDLE_IN_TRANSACTION_TIMEOUT_FLAG, "1");
        Connection sqlBegin = connect(server);
        Connection autocommitOff = connect(server);
        Statement begun = sqlBegin.createStatement();
        Statement off = autocommitOff.createStatement()) {
      begun.execute("CREATE TABLE idle_tx (who VARCHAR, v INTEGER)");
      // A transaction begun in SQL, which the driver sees only in the session document.
      begun.execute("BEGIN");
      begun.execute("INSERT INTO idle_tx VALUES ('sql', 1)");
      autocommitOff.setAutoCommit(false);
      off.execute("INSERT INTO idle_tx VALUES ('off', 1)");
      String sqlSession = sessionOf(sqlBegin);
      String offSession = sessionOf(autocommitOff);

      Thread.sleep(PAST_THE_LIMIT_MS);

      for (Connection connection : List.of(sqlBegin, autocommitOff)) {
        Statement statement = connection == sqlBegin ? begun : off;
        String who = connection == sqlBegin ? "sql" : "off";
        SQLTransactionRollbackException error =
            assertThrows(
                SQLTransactionRollbackException.class,
                () -> statement.execute("INSERT INTO idle_tx VALUES ('" + who + "', 2)"),
                who);
        assertEquals("40000", error.getSQLState(), who);
        assertTrue(error.getMessage().contains("idle_in_transaction"), error.getMessage());
        assertEquals("session_ended", ChargewireProblem.codeOf(error), who);
        assertEquals("idle_in_transaction", ChargewireProblem.of(error).reason(), who);
        assertFalse(connection.isClosed(), "the session is replaced, the connection stays open");
        assertNotNull(connection.getWarnings(), who);
      }
      assertNotEquals(sqlSession, sessionOf(sqlBegin));
      assertNotEquals(offSession, sessionOf(autocommitOff));
      assertEquals(
          List.of(),
          rows(server, "idle_tx"),
          "the rolled-back inserts are gone, and the failing ones did not run on the new session");

      // Both connections work again, each with its own autocommit.
      begun.execute("INSERT INTO idle_tx VALUES ('sql', 3)");
      assertFalse(autocommitOff.getAutoCommit());
      off.execute("INSERT INTO idle_tx VALUES ('off', 3)");
      assertEquals(List.of("sql 3"), rows(server, "idle_tx"), "autocommit off: not yet committed");
      autocommitOff.commit();
      assertEquals(List.of("off 3", "sql 3"), rows(server, "idle_tx"));
    }
  }

  @Test
  void anEvictedSessionsStatementRunsAgainOnlyWhenNoTransactionWasOpen() throws Exception {
    try (ChargewireServer server = ChargewireServer.start(ChargewireServer.IDLE_TIMEOUT_FLAG, "1");
        Connection inTransaction = connect(server);
        Connection known = connect(server);
        Connection unread = connect(server);
        Statement begun = inTransaction.createStatement();
        Statement plain = known.createStatement();
        Statement other = unread.createStatement()) {
      begun.execute("CREATE TABLE evicted (who VARCHAR, v INTEGER)");
      begun.execute("BEGIN");
      begun.execute("INSERT INTO evicted VALUES ('tx', 1)");
      // The driver knows this session had no transaction: it read the session after the
      // statement.
      plain.execute("SELECT 1");
      assertTrue(known.isValid(5));
      // This one ran a statement since it last read its session: it can't tell.
      other.execute("SELECT 1");

      Thread.sleep(PAST_THE_LIMIT_MS);

      // The BEGIN came in SQL after the session was last read, so only the server knows a
      // transaction was open: an evicted session that held one ends with idle_in_transaction
      // (spec §5.7), and the statement is refused, not run alone on the new session.
      SQLTransactionRollbackException lost =
          assertThrows(
              SQLTransactionRollbackException.class,
              () -> begun.execute("INSERT INTO evicted VALUES ('tx', 2)"));
      assertEquals("40000", lost.getSQLState());
      assertEquals("session_ended", ChargewireProblem.codeOf(lost));
      assertEquals("idle_in_transaction", ChargewireProblem.of(lost).reason());
      assertFalse(inTransaction.isClosed(), "the session is replaced, the connection stays open");

      // No transaction was open here, so the server says only session_not_found; the driver
      // can't tell whether one was, and refuses too.

      SQLTransientConnectionException unknown =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> other.execute("INSERT INTO evicted VALUES ('unread', 1)"));
      assertEquals("08007", unknown.getSQLState());
      assertEquals("session_not_found", ChargewireProblem.codeOf(unknown));
      assertTrue(unknown.getMessage().contains("cannot tell"), unknown.getMessage());

      String before = sessionOf(known);
      assertEquals(1, plain.executeUpdate("INSERT INTO evicted VALUES ('known', 1)"));
      assertNotEquals(before, sessionOf(known), "run again on a new session");

      assertEquals(
          List.of("known 1"),
          rows(server, "evicted"),
          "the evicted transaction was rolled back, and no refused statement ran");
      // The refusing connections are on new sessions and work again.
      begun.execute("INSERT INTO evicted VALUES ('tx', 3)");
      other.execute("INSERT INTO evicted VALUES ('unread', 3)");
      assertEquals(List.of("known 1", "tx 3", "unread 3"), rows(server, "evicted"));
    }
  }

  @Test
  void anEvictedSessionsStatementDoesNotRunAgainWhereUseOrSetWouldNotHoldAnyMore()
      throws Exception {
    try (ChargewireServer server = ChargewireServer.start(ChargewireServer.IDLE_TIMEOUT_FLAG, "1");
        Connection setup = connect(server);
        Connection used = connect(server);
        Connection readBack = connect(server);
        Connection set = connect(server);
        Statement admin = setup.createStatement();
        Statement afterUse = used.createStatement();
        Statement afterRead = readBack.createStatement();
        Statement afterSet = set.createStatement()) {
      // The same table name in two schemas: on a new session opened in the old schema, the
      // unqualified insert would land in the wrong one.
      admin.execute("CREATE SCHEMA moved");
      admin.execute("CREATE TABLE main.placed (who VARCHAR, v INTEGER)");
      admin.execute("CREATE TABLE moved.placed (who VARCHAR, v INTEGER)");

      afterUse.execute("USE moved");
      // Read right after the USE: on servers up to Chargewire 2.2.0 it may not show it yet, so it
      // doesn't count, whatever it shows.
      assertTrue(used.isValid(5));
      afterRead.execute("USE moved");
      afterRead.execute("SELECT 1");
      // Read after a later statement's answer: it shows the USE on every server.
      assertTrue(readBack.isValid(5));
      assertEquals("moved", readBack.getSchema());
      afterSet.execute("SET threads = 2");
      afterSet.execute("SELECT 1");
      assertTrue(set.isValid(5));

      Thread.sleep(PAST_THE_LIMIT_MS);

      SQLTransientConnectionException lostUse =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> afterUse.execute("INSERT INTO placed VALUES ('use', 1)"));
      assertEquals("08007", lostUse.getSQLState());
      assertEquals("session_not_found", ChargewireProblem.codeOf(lostUse));
      assertTrue(lostUse.getMessage().contains("after it ran USE"), lostUse.getMessage());

      SQLTransientConnectionException lostSet =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> afterSet.execute("INSERT INTO placed VALUES ('set', 1)"));
      assertEquals("08007", lostSet.getSQLState());
      assertTrue(lostSet.getMessage().contains("SET, RESET or PRAGMA"), lostSet.getMessage());

      String before = sessionOf(readBack);
      afterRead.execute("INSERT INTO placed VALUES ('read back', 1)");
      assertNotEquals(before, sessionOf(readBack), "run again on a new session");
      assertEquals("moved", readBack.getSchema(), "opened in the schema read back");

      assertEquals(List.of(), rows(server, "main.placed"), "nothing landed in the old schema");
      assertEquals(
          List.of("read back 1"),
          rows(server, "moved.placed"),
          "only the statement whose USE was read back ran again");
    }
  }
}
