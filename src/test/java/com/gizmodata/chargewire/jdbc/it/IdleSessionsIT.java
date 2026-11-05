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
 * Sessions the server ends on its own while a transaction is open (spec §5.6, §5.7): the idle sweep
 * ({@code session_not_found}) and the idle-in-transaction limit ({@code session_ended}, reason
 * {@code idle_in_transaction}). The transaction's work is rolled back, so the statement that finds
 * the session gone must never run again on a new session in autocommit, where it would commit on
 * its own. Each test runs its own server with a one-second limit.
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

      SQLTransientConnectionException lost =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> begun.execute("INSERT INTO evicted VALUES ('tx', 2)"));
      assertEquals("08007", lost.getSQLState());
      assertEquals("session_not_found", ChargewireProblem.codeOf(lost));
      // The BEGIN came in SQL after the session was last read, so the driver can't tell a
      // transaction was open; either way the statement is refused.
      assertTrue(lost.getMessage().contains("was not run"), lost.getMessage());

      SQLTransientConnectionException unknown =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> other.execute("INSERT INTO evicted VALUES ('unread', 1)"));
      assertEquals("08007", unknown.getSQLState());
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
}
