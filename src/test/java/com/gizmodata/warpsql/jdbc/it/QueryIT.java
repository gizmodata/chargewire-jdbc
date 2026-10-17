package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpProblem;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Queries, updates, prepared statements and transactions: GizmoSQL 1.x's JDBC integration tests
 * ({@code GizmoSqlIntegrationIT}) ported to Warp SQL, plus the cases around them.
 */
class QueryIT {
  @Test
  void connects() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertFalse(connection.isClosed());
      assertTrue(connection.isValid(5));
    }
  }

  @Test
  void simpleQuery() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1 AS value")) {
      assertTrue(rows.next());
      assertEquals(1, rows.getInt("value"));
      assertFalse(rows.next());
    }
  }

  @Test
  void queryWithMultipleRows() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name) ORDER BY id")) {
      String names = "";
      int count = 0;
      while (rows.next()) {
        count++;
        assertEquals(count, rows.getInt(1));
        names += rows.getString(2);
      }
      assertEquals(3, count);
      assertEquals("abc", names);
    }
  }

  @Test
  void createInsertAndQueryATable() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)");
      statement.execute("INSERT INTO test_table VALUES (1, 'Alice', 10.5)");
      statement.execute("INSERT INTO test_table VALUES (2, 'Bob', 20.0)");
      statement.execute("INSERT INTO test_table VALUES (3, 'Charlie', 30.5)");
      try (ResultSet rows = statement.executeQuery("SELECT * FROM test_table ORDER BY id")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt("id"));
        assertEquals("Alice", rows.getString("name"));
        assertEquals(10.5, rows.getDouble("value"), 0.001);
        assertTrue(rows.next());
        assertEquals("Bob", rows.getString("name"));
        assertTrue(rows.next());
        assertEquals("Charlie", rows.getString("name"));
        assertFalse(rows.next());
      }
      statement.execute("DROP TABLE test_table");
    }
  }

  @Test
  void preparedQueryWithAParameter() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE prepared_test (id INTEGER, name VARCHAR)");
      statement.execute(
          "INSERT INTO prepared_test VALUES (1, 'Test1'), (2, 'Test2'), (3, 'Test3')");
      try (PreparedStatement prepared =
          connection.prepareStatement("SELECT * FROM prepared_test WHERE id > ? ORDER BY id")) {
        prepared.setInt(1, 1);
        try (ResultSet rows = prepared.executeQuery()) {
          assertTrue(rows.next());
          assertEquals(2, rows.getInt("id"));
          assertTrue(rows.next());
          assertEquals(3, rows.getInt("id"));
          assertFalse(rows.next());
        }
        prepared.setInt(1, 2);
        try (ResultSet rows = prepared.executeQuery()) {
          assertTrue(rows.next());
          assertEquals("Test3", rows.getString("name"));
          assertFalse(rows.next());
        }
      }
    }
  }

  @Test
  void resultSetMetaDataNamesTheColumns() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT 42 AS int_col, 'hello' AS str_col, 3.14 AS double_col")) {
      ResultSetMetaData metaData = rows.getMetaData();
      assertEquals(3, metaData.getColumnCount());
      assertEquals("int_col", metaData.getColumnName(1));
      assertEquals("str_col", metaData.getColumnName(2));
      assertEquals("double_col", metaData.getColumnName(3));
      assertEquals(java.sql.Types.INTEGER, metaData.getColumnType(1));
      assertEquals(java.sql.Types.VARCHAR, metaData.getColumnType(2));
      assertEquals(java.sql.Types.DECIMAL, metaData.getColumnType(3));
    }
  }

  @Test
  void nullsReadAsNullAndSetWasNull() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT NULL AS null_col, 42 AS int_col, NULL::INTEGER AS n")) {
      assertTrue(rows.next());
      assertNull(rows.getObject("null_col"));
      assertTrue(rows.wasNull());
      assertEquals(42, rows.getInt("int_col"));
      assertFalse(rows.wasNull());
      assertEquals(0, rows.getInt("n"));
      assertTrue(rows.wasNull());
      assertNull(rows.getString("n"));
      assertNull(rows.getBigDecimal("n"));
    }
  }

  @Test
  void largeResultSet() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT * FROM range(10000) AS t(i)")) {
      int count = 0;
      while (rows.next()) {
        count++;
      }
      assertEquals(10000, count);
    }
  }

  @Test
  void aggregation() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT COUNT(*) AS cnt, SUM(i) AS total, AVG(i) AS average FROM range(100) AS t(i)")) {
      assertTrue(rows.next());
      assertEquals(100, rows.getLong("cnt"));
      assertEquals(4950, rows.getLong("total"));
      assertEquals(49.5, rows.getDouble("average"), 0.001);
    }
  }

  @Test
  void transactionIsolationIsAValidLevel() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      int isolation = connection.getTransactionIsolation();
      assertTrue(
          List.of(
                  Connection.TRANSACTION_NONE,
                  Connection.TRANSACTION_READ_UNCOMMITTED,
                  Connection.TRANSACTION_READ_COMMITTED,
                  Connection.TRANSACTION_REPEATABLE_READ,
                  Connection.TRANSACTION_SERIALIZABLE)
              .contains(isolation));
    }
  }

  // ---- Arrays (GizmoSQL issue #95) ----

  @Test
  void arrayLiteralBracketSyntax() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT [2, 3, 4] AS arr")) {
      assertTrue(rows.next());
      assertEquals(List.of(2, 3, 4), rows.getObject("arr"));
      Array array = rows.getArray("arr");
      assertArrayEquals(new Object[] {2, 3, 4}, (Object[]) array.getArray());
      assertEquals(java.sql.Types.INTEGER, array.getBaseType());
      assertEquals("[2, 3, 4]", rows.getString("arr"));
    }
  }

  @Test
  void arrayLiteralListSyntax() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT list_value(2, 3, 4) AS arr")) {
      assertTrue(rows.next());
      assertEquals(List.of(2, 3, 4), rows.getObject("arr"));
    }
  }

  @Test
  void listHasAnyFunction() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE list_test (id INTEGER, anomalies INTEGER[])");
      statement.execute(
          "INSERT INTO list_test VALUES (1, [0, 1, 2]), (2, [3, 4, 5]), (3, [0, 6, 7])");
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT id FROM list_test WHERE list_has_any(anomalies, [0]) ORDER BY id")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt("id"));
        assertTrue(rows.next());
        assertEquals(3, rows.getInt("id"));
        assertFalse(rows.next());
      }
    }
  }

  @Test
  void nestedArrays() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT [[1, 2], [3, 4]] AS nested_arr")) {
      assertTrue(rows.next());
      assertEquals(List.of(List.of(1, 2), List.of(3, 4)), rows.getObject("nested_arr"));
    }
  }

  @Test
  void arrayAggregation() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT list(i ORDER BY i) AS arr FROM (SELECT unnest([1,2,3,4,5]) AS i)")) {
      assertTrue(rows.next());
      assertEquals(List.of(1, 2, 3, 4, 5), rows.getObject("arr"));
    }
  }

  @Test
  void stringArrays() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT ['a', 'b', NULL] AS str_arr")) {
      assertTrue(rows.next());
      assertEquals(java.util.Arrays.asList("a", "b", null), rows.getObject("str_arr"));
      assertEquals("[\"a\", \"b\", null]", rows.getString("str_arr"));
    }
  }

  @Test
  void structsAndMaps() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT {'a': 1, 'b': 'x'} AS s, MAP {'k': 2} AS m")) {
      assertTrue(rows.next());
      assertEquals(Map.of("a", 1, "b", "x"), rows.getObject("s"));
      assertArrayEquals(
          new Object[] {1, "x"}, rows.getObject("s", java.sql.Struct.class).getAttributes());
      assertEquals(Map.of("k", 2), rows.getObject("m"));
      assertEquals("{\"a\": 1, \"b\": \"x\"}", rows.getString("s"));
    }
  }

  // ---- Decimals (GizmoSQL v1.6.0 regressions) ----

  @Test
  void decimalParameterRoundTrip() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_dec(id INT, amount DECIMAL(10,2))");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_dec VALUES (?, ?)")) {
        insert.setInt(1, 1);
        insert.setBigDecimal(2, new BigDecimal("123.45"));
        assertEquals(1, insert.executeUpdate());
      }
      try (ResultSet rows = statement.executeQuery("SELECT amount FROM it_dec WHERE id = 1")) {
        assertTrue(rows.next());
        assertEquals(new BigDecimal("123.45"), rows.getBigDecimal(1));
      }
    }
  }

  @Test
  void decimalBindWithOtherJavaTypes() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_decbind(id INT, amount DECIMAL(10,3))");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_decbind VALUES (?, ?)")) {
        insert.setInt(1, 1);
        insert.setDouble(2, 3.0);
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 2);
        insert.setObject(2, "12.345");
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 3);
        insert.setBigDecimal(2, new BigDecimal("9.87654"));
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 4);
        insert.setObject(2, "1.0005", java.sql.Types.DECIMAL, 3);
        assertEquals(1, insert.executeUpdate());
      }
      try (ResultSet rows = statement.executeQuery("SELECT amount FROM it_decbind ORDER BY id")) {
        assertTrue(rows.next());
        assertEquals(new BigDecimal("3.000"), rows.getBigDecimal(1));
        assertTrue(rows.next());
        assertEquals(new BigDecimal("12.345"), rows.getBigDecimal(1));
        assertTrue(rows.next());
        assertEquals(new BigDecimal("9.877"), rows.getBigDecimal(1), "9.87654 rounds half up");
        assertTrue(rows.next());
        assertEquals(new BigDecimal("1.001"), rows.getBigDecimal(1), "1.0005 rounds half up");
      }
    }
  }

  // ---- Update counts and RETURNING ----

  @Test
  void dmlReportsUpdateCounts() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_upd(id INT, v VARCHAR)");
      assertFalse(statement.execute("INSERT INTO it_upd VALUES (1,'a'),(2,'b'),(3,'c')"));
      assertEquals(3, statement.getUpdateCount());
      assertNull(statement.getResultSet());
      assertFalse(statement.execute("UPDATE it_upd SET v = 'x' WHERE id < 3"));
      assertEquals(2, statement.getUpdateCount());
      assertFalse(statement.execute("DELETE FROM it_upd WHERE id = 1"));
      assertEquals(1, statement.getUpdateCount());
      assertEquals(1, statement.executeUpdate("DELETE FROM it_upd WHERE id = 2"));
      assertEquals(1L, statement.executeLargeUpdate("DELETE FROM it_upd WHERE id = 3"));
      assertTrue(statement.execute("SELECT count(*) FROM it_upd"));
      assertEquals(-1, statement.getUpdateCount());
      assertFalse(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
    }
  }

  @Test
  void ddlHasAnUpdateCountOfZero() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      assertFalse(statement.execute("CREATE TEMP TABLE it_ddl(id INT)"));
      assertEquals(0, statement.getUpdateCount());
      assertEquals(0, statement.executeUpdate("DROP TABLE it_ddl"));
    }
  }

  @Test
  void insertReturningProducesRows() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SEQUENCE it_returning_seq");
      statement.execute(
          "CREATE TABLE it_returning (id INTEGER DEFAULT nextval('it_returning_seq') PRIMARY KEY,"
              + " name VARCHAR)");
      try {
        try (ResultSet rows =
            statement.executeQuery(
                "INSERT INTO it_returning (name) VALUES ('alice'),('bob') RETURNING id, name")) {
          assertEquals(2, rows.getMetaData().getColumnCount());
          int count = 0;
          while (rows.next()) {
            count++;
            assertTrue(rows.getInt("id") > 0);
            assertTrue(List.of("alice", "bob").contains(rows.getString("name")));
          }
          assertEquals(2, count);
        }
        try (PreparedStatement update =
            connection.prepareStatement(
                "UPDATE it_returning SET name = ? WHERE name = ? RETURNING id, name")) {
          update.setString(1, "CAROL");
          update.setString(2, "alice");
          try (ResultSet rows = update.executeQuery()) {
            assertTrue(rows.next());
            assertEquals("CAROL", rows.getString("name"));
            assertFalse(rows.next());
          }
        }
        try (ResultSet rows =
            statement.executeQuery("DELETE FROM it_returning WHERE name = 'bob' RETURNING id")) {
          assertTrue(rows.next());
          assertFalse(rows.next());
        }
        // The word "returning" inside a literal is just text.
        assertEquals(
            1, statement.executeUpdate("INSERT INTO it_returning (name) VALUES ('returning')"));
        try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM it_returning")) {
          assertTrue(rows.next());
          assertEquals(2, rows.getInt(1));
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS it_returning");
        statement.execute("DROP SEQUENCE IF EXISTS it_returning_seq");
      }
    }
  }

  @Test
  void executeUpdateOnAQueryFailsAfterRunningIt() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_eu(id INT)");
      SQLException error =
          assertThrows(
              SQLException.class,
              () -> statement.executeUpdate("INSERT INTO it_eu VALUES (1), (2) RETURNING id"));
      assertEquals("07000", error.getSQLState());
      try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM it_eu")) {
        assertTrue(rows.next());
        assertEquals(2, rows.getInt(1), "the statement ran to completion before the error");
      }
    }
  }

  @Test
  void executeQueryOnAnUpdateFails() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(
              SQLException.class, () -> statement.executeQuery("CREATE TEMP TABLE it_eq(i INT)"));
      assertEquals("02000", error.getSQLState());
    }
  }

  // ---- Prepared reuse, batches and transactions ----

  @Test
  void preparedReuseAndBatchLedger() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TEMP TABLE it_reuse_ledger (id INTEGER PRIMARY KEY, value VARCHAR)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_reuse_ledger VALUES (?, ?)")) {
        for (int id = 0; id < 20; id++) {
          insert.setInt(1, id);
          insert.setString(2, "bound ' ; -- value");
          assertEquals(1, insert.executeUpdate());
        }
        for (int id = 20; id < 25; id++) {
          insert.setInt(1, id);
          insert.setString(2, "batch value");
          insert.addBatch();
        }
        int[] counts = insert.executeBatch();
        assertEquals(5, counts.length);
        for (int count : counts) {
          assertEquals(Statement.SUCCESS_NO_INFO, count);
        }
        assertEquals(0, insert.executeBatch().length);
        insert.setInt(1, 25);
        insert.setString(2, "single");
        insert.addBatch();
        assertArrayEquals(new int[] {1}, insert.executeBatch(), "a batch of one keeps its count");
      }
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT count(*), sum(id), max(value) FROM it_reuse_ledger WHERE id < 20")) {
        assertTrue(rows.next());
        assertEquals(20, rows.getInt(1));
        assertEquals(190, rows.getInt(2));
        assertEquals("bound ' ; -- value", rows.getString(3));
      }
      try (ResultSet rows =
          statement.executeQuery("SELECT count(*), sum(id) FROM it_reuse_ledger")) {
        assertTrue(rows.next());
        assertEquals(26, rows.getInt(1));
        assertEquals(325, rows.getInt(2));
      }
    }
  }

  @Test
  void largeBatchIsOneRequestAndWritesEveryRow() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TEMP TABLE it_big_batch (id BIGINT, amount DECIMAL(12,2), ts TIMESTAMP, note VARCHAR)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_big_batch VALUES (?, ?, ?, ?)")) {
        for (long id = 0; id < 5000; id++) {
          insert.setLong(1, id);
          insert.setBigDecimal(2, BigDecimal.valueOf(id, 2));
          insert.setTimestamp(3, java.sql.Timestamp.valueOf("2026-01-01 00:00:00"));
          if (id % 7 == 0) {
            insert.setNull(4, java.sql.Types.VARCHAR);
          } else {
            insert.setString(4, "n" + id);
          }
          insert.addBatch();
        }
        assertEquals(5000, insert.executeBatch().length);
      }
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT count(*), sum(id), sum(amount), count(note) FROM it_big_batch")) {
        assertTrue(rows.next());
        assertEquals(5000, rows.getInt(1));
        assertEquals(5000L * 4999 / 2, rows.getLong(2));
        assertEquals(new BigDecimal("124975.00"), rows.getBigDecimal(3));
        assertEquals(5000 - 715, rows.getInt(4));
      }
    }
  }

  @Test
  void failedBatchReportsTheServerError() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_bad_batch (id INTEGER PRIMARY KEY)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_bad_batch VALUES (?)")) {
        insert.setInt(1, 1);
        insert.addBatch();
        insert.setInt(1, 1);
        insert.addBatch();
        BatchUpdateException error = assertThrows(BatchUpdateException.class, insert::executeBatch);
        assertEquals("23", error.getSQLState().substring(0, 2), error.getMessage());
        assertNotNull(WarpProblem.of(error));
      }
    }
  }

  @Test
  void statementBatchRunsEachStatement() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_sql_batch (id INTEGER)");
      statement.addBatch("INSERT INTO it_sql_batch VALUES (1), (2)");
      statement.addBatch("INSERT INTO it_sql_batch VALUES (3)");
      statement.addBatch("UPDATE it_sql_batch SET id = id + 10");
      assertArrayEquals(new int[] {2, 1, 3}, statement.executeBatch());
      statement.addBatch("INSERT INTO it_sql_batch VALUES (4)");
      statement.addBatch("INSERT INTO no_such_table VALUES (5)");
      BatchUpdateException error =
          assertThrows(BatchUpdateException.class, statement::executeBatch);
      assertArrayEquals(new int[] {1}, error.getUpdateCounts());
      assertEquals("sql_error", WarpProblem.codeOf(error));
    }
  }

  @Test
  void commitRollbackAndAutocommitTransition() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_transaction_ledger (id INTEGER)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_transaction_ledger VALUES (?)")) {
        connection.setAutoCommit(false);
        assertFalse(connection.getAutoCommit());
        insert.setInt(1, 1);
        assertEquals(1, insert.executeUpdate());
        connection.rollback();
        try (ResultSet rows =
            statement.executeQuery("SELECT count(*) FROM it_transaction_ledger")) {
          assertTrue(rows.next());
          assertEquals(0, rows.getInt(1));
        }
        insert.setInt(1, 2);
        assertEquals(1, insert.executeUpdate());
        connection.commit();
        insert.setInt(1, 3);
        assertEquals(1, insert.executeUpdate());
        connection.rollback();
        insert.setInt(1, 4);
        assertEquals(1, insert.executeUpdate());
        connection.setAutoCommit(true);
      }
      try (ResultSet rows =
          statement.executeQuery("SELECT count(*), sum(id) FROM it_transaction_ledger")) {
        assertTrue(rows.next());
        assertEquals(2, rows.getInt(1));
        assertEquals(6, rows.getInt(2));
      }
    }
  }

  @Test
  void writesPreparedInsideATransactionRun() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE it_prepared_in_transaction (id INTEGER)");
      connection.setAutoCommit(false);
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO it_prepared_in_transaction VALUES (?)")) {
        insert.setInt(1, 1);
        assertEquals(1, insert.executeUpdate());
      }
      connection.commit();
      try (ResultSet rows =
          statement.executeQuery("SELECT count(*) FROM it_prepared_in_transaction")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt(1));
      }
    }
  }

  @Test
  void committedWorkIsVisibleToOtherConnections() throws SQLException {
    try (Connection writer = SharedServer.connect();
        Connection reader = SharedServer.connect();
        Statement write = writer.createStatement();
        Statement read = reader.createStatement()) {
      write.execute("CREATE OR REPLACE TABLE it_visibility (id INTEGER)");
      writer.setAutoCommit(false);
      write.execute("INSERT INTO it_visibility VALUES (1)");
      try (ResultSet rows = read.executeQuery("SELECT count(*) FROM it_visibility")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getInt(1), "uncommitted work is invisible");
      }
      writer.commit();
      try (ResultSet rows = read.executeQuery("SELECT count(*) FROM it_visibility")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt(1));
      }
      write.execute("DROP TABLE it_visibility");
      writer.commit();
    }
  }

  @Test
  void commitInAutocommitModeFails() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertEquals("25000", assertThrows(SQLException.class, connection::commit).getSQLState());
      assertEquals("25000", assertThrows(SQLException.class, connection::rollback).getSQLState());
    }
  }

  // ---- Errors ----

  @Test
  void syntaxErrorsAreSyntaxErrorExceptions() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      SQLSyntaxErrorException error =
          assertThrows(SQLSyntaxErrorException.class, () -> statement.executeQuery("SELEC 1"));
      assertEquals("sql_error", WarpProblem.codeOf(error));
      assertEquals("42", error.getSQLState().substring(0, 2));
      assertEquals(400, error.getErrorCode());
      SQLSyntaxErrorException missing =
          assertThrows(
              SQLSyntaxErrorException.class,
              () -> statement.executeQuery("SELECT * FROM nowhere_at_all"));
      assertEquals("sql_error", WarpProblem.codeOf(missing));
      // The statement and connection still work.
      try (ResultSet rows = statement.executeQuery("SELECT 1")) {
        assertTrue(rows.next());
      }
    }
  }

  @Test
  void prepareErrorsSurfaceAtPrepareTime() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      SQLException error =
          assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement("SELEC 1"));
      assertEquals("sql_error", WarpProblem.codeOf(error));
    }
  }

  @Test
  void executionErrorsCarryTheServerCode() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      SQLException error =
          assertThrows(SQLException.class, () -> statement.executeQuery("SELECT error('boom')"));
      assertEquals("execution_error", WarpProblem.codeOf(error));
      assertTrue(error.getMessage().contains("boom"), error.getMessage());
    }
  }

  @Test
  void parametersAreValuesNotSql() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE victims (name VARCHAR)");
      String hostile = "x'); DROP TABLE victims; --";
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO victims VALUES (?)")) {
        insert.setString(1, hostile);
        insert.executeUpdate();
      }
      try (ResultSet rows = statement.executeQuery("SELECT name FROM victims")) {
        assertTrue(rows.next());
        assertEquals(hostile, rows.getString(1));
      }
    }
  }

  @Test
  void unsetParametersAreReported() throws SQLException {
    try (Connection connection = SharedServer.connect();
        PreparedStatement select = connection.prepareStatement("SELECT ? + ?")) {
      select.setInt(1, 1);
      assertEquals("07001", assertThrows(SQLException.class, select::executeQuery).getSQLState());
      assertEquals(
          "07009", assertThrows(SQLException.class, () -> select.setInt(3, 1)).getSQLState());
    }
  }

  @Test
  void typedNullParameters() throws SQLException {
    try (Connection connection = SharedServer.connect();
        PreparedStatement select = connection.prepareStatement("SELECT ? + 1 AS plus")) {
      select.setNull(1, java.sql.Types.INTEGER);
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertNull(rows.getObject(1));
      }
    }
  }
}
