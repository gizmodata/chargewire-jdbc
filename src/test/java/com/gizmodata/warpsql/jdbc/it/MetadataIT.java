package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpDriver;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@link DatabaseMetaData} against a real server (ported from {@code ArrowDatabaseMetadataTest},
 * whose cases ran against a mock Flight SQL producer, and GizmoSQL's metadata integration tests).
 */
class MetadataIT {
  @BeforeAll
  static void createObjects() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS meta");
      statement.execute(
          "CREATE OR REPLACE TABLE meta.parent (a INTEGER, b VARCHAR, PRIMARY KEY (a, b))");
      statement.execute(
          "CREATE OR REPLACE TABLE meta.child (id BIGINT PRIMARY KEY, pa INTEGER, pb VARCHAR,"
              + " amount DECIMAL(12,3), at_time TIMESTAMP, note VARCHAR NOT NULL,"
              + " FOREIGN KEY (pa, pb) REFERENCES meta.parent (a, b))");
      statement.execute(
          "CREATE OR REPLACE VIEW meta.child_view AS SELECT id, note FROM meta.child");
    }
  }

  private static List<String> column(ResultSet rows, String name) throws SQLException {
    List<String> values = new ArrayList<>();
    while (rows.next()) {
      values.add(rows.getString(name));
    }
    rows.close();
    return values;
  }

  private static List<String> labels(ResultSet rows) throws SQLException {
    ResultSetMetaData metaData = rows.getMetaData();
    List<String> names = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      names.add(metaData.getColumnLabel(i));
    }
    return names;
  }

  @Test
  void productAndDriver() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals(WarpServer.NAME, metaData.getDatabaseProductName());
      String version = metaData.getDatabaseProductVersion();
      java.util.regex.Matcher numbers =
          java.util.regex.Pattern.compile("(\\d+)\\.(\\d+)").matcher(version);
      assertTrue(numbers.find(), version);
      assertEquals(Integer.parseInt(numbers.group(1)), metaData.getDatabaseMajorVersion());
      assertEquals(Integer.parseInt(numbers.group(2)), metaData.getDatabaseMinorVersion());
      assertEquals(WarpDriver.NAME, metaData.getDriverName());
      assertEquals(WarpDriver.version(), metaData.getDriverVersion());
      assertEquals(4, metaData.getJDBCMajorVersion());
      assertEquals("\"", metaData.getIdentifierQuoteString());
      assertEquals("warp", metaData.getUserName());
      assertFalse(metaData.getURL().contains(SharedServer.get().password));
      assertTrue(metaData.supportsTransactions());
      assertFalse(metaData.isReadOnly(), "server_read_only");
      assertTrue(metaData.supportsBatchUpdates());
      assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
      assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
      assertEquals(DatabaseMetaData.sqlStateSQL, metaData.getSQLStateType());
      assertEquals("\\", metaData.getSearchStringEscape());
      assertTrue(metaData.getConnection() == connection);
    }
  }

  @Test
  void catalogs() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      ResultSet rows = connection.getMetaData().getCatalogs();
      assertEquals(List.of("TABLE_CAT"), labels(rows));
      assertTrue(column(rows, "TABLE_CAT").contains("memory"));
    }
  }

  @Test
  void schemas() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet all = metaData.getSchemas();
      assertEquals(List.of("TABLE_SCHEM", "TABLE_CATALOG"), labels(all));
      assertTrue(column(all, "TABLE_SCHEM").containsAll(List.of("main", "meta")));
      List<String> filtered = column(metaData.getSchemas("memory", "me%"), "TABLE_SCHEM");
      assertEquals(List.of("meta"), filtered);
      assertEquals(List.of(), column(metaData.getSchemas("no_such_catalog", null), "TABLE_SCHEM"));
    }
  }

  @Test
  void tableTypes() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      ResultSet rows = connection.getMetaData().getTableTypes();
      assertEquals(List.of("TABLE_TYPE"), labels(rows));
      assertTrue(column(rows, "TABLE_TYPE").containsAll(List.of("BASE TABLE", "VIEW")));
    }
  }

  @Test
  void tables() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet rows = metaData.getTables(null, "meta", null, null);
      assertEquals(
          List.of(
              "TABLE_CAT",
              "TABLE_SCHEM",
              "TABLE_NAME",
              "TABLE_TYPE",
              "REMARKS",
              "TYPE_CAT",
              "TYPE_SCHEM",
              "TYPE_NAME",
              "SELF_REFERENCING_COL_NAME",
              "REF_GENERATION"),
          labels(rows));
      Map<String, String> types = new HashMap<>();
      while (rows.next()) {
        assertEquals("memory", rows.getString("TABLE_CAT"));
        assertEquals("meta", rows.getString(2));
        types.put(rows.getString("TABLE_NAME"), rows.getString("TABLE_TYPE"));
      }
      rows.close();
      assertEquals(
          Map.of("parent", "BASE TABLE", "child", "BASE TABLE", "child_view", "VIEW"), types);
      assertEquals(
          List.of("child_view"),
          column(metaData.getTables(null, "meta", "%", new String[] {"VIEW"}), "TABLE_NAME"));
      assertEquals(
          List.of("child", "child_view"),
          column(metaData.getTables("memory", "meta", "child%", null), "TABLE_NAME"));
      assertEquals(
          List.of(),
          column(metaData.getTables(null, null, "x' OR '1'='1", null), "TABLE_NAME"),
          "patterns are values, not SQL");
    }
  }

  @Test
  void temporaryTablesAreListed() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE get_tables_test (id INTEGER)");
      List<String> names =
          column(
              connection.getMetaData().getTables(null, null, "get_tables_test", null),
              "TABLE_NAME");
      assertEquals(List.of("get_tables_test"), names);
    }
  }

  @Test
  void columns() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      ResultSet rows = connection.getMetaData().getColumns(null, "meta", "child", null);
      assertEquals(24, rows.getMetaData().getColumnCount());
      assertEquals("IS_GENERATEDCOLUMN", rows.getMetaData().getColumnLabel(24));
      List<String> names = new ArrayList<>();
      Map<String, Integer> dataTypes = new HashMap<>();
      while (rows.next()) {
        String name = rows.getString("COLUMN_NAME");
        names.add(name);
        dataTypes.put(name, rows.getInt("DATA_TYPE"));
        assertEquals(names.size(), rows.getInt("ORDINAL_POSITION"));
        assertEquals("child", rows.getString("TABLE_NAME"));
        if (name.equals("amount")) {
          // The base type, as GizmoSQL 1.x reports it; the digits are separate.
          assertEquals("DECIMAL", rows.getString("TYPE_NAME"));
          assertEquals(12, rows.getInt("COLUMN_SIZE"));
          assertEquals(3, rows.getInt("DECIMAL_DIGITS"));
          assertEquals(10, rows.getInt("NUM_PREC_RADIX"));
        }
        if (name.equals("at_time")) {
          assertEquals("TIMESTAMP", rows.getString("TYPE_NAME"));
          assertEquals(6, rows.getInt("DECIMAL_DIGITS"));
        }
      }
      rows.close();
      assertEquals(List.of("id", "pa", "pb", "amount", "at_time", "note"), names);
      assertEquals(Types.BIGINT, dataTypes.get("id"));
      assertEquals(Types.INTEGER, dataTypes.get("pa"));
      assertEquals(Types.VARCHAR, dataTypes.get("pb"));
      assertEquals(Types.DECIMAL, dataTypes.get("amount"));
      assertEquals(Types.TIMESTAMP, dataTypes.get("at_time"));
      List<String> filtered =
          column(connection.getMetaData().getColumns(null, "meta", "child", "p_"), "COLUMN_NAME");
      assertEquals(List.of("pa", "pb"), filtered, "column patterns use LIKE");
      List<String> escaped =
          column(
              connection.getMetaData().getColumns(null, "meta", "child", "at\\_time"),
              "COLUMN_NAME");
      assertEquals(List.of("at_time"), escaped);
    }
  }

  private static Map<String, Map<String, String>> describeMetaTable(Connection connection)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE SEQUENCE IF NOT EXISTS it_meta_seq START 1");
      statement.execute(
          "CREATE OR REPLACE TABLE it_meta (id INT PRIMARY KEY DEFAULT nextval('it_meta_seq'),"
              + " name VARCHAR NOT NULL, salary DECIMAL(10,2) DEFAULT 50000.00, note VARCHAR)");
      statement.execute("COMMENT ON COLUMN it_meta.name IS 'employee name'");
    }
    Map<String, Map<String, String>> columns = new HashMap<>();
    try (ResultSet rows = connection.getMetaData().getColumns(null, null, "it_meta", null)) {
      while (rows.next()) {
        Map<String, String> column = new HashMap<>();
        for (String label :
            List.of(
                "IS_NULLABLE",
                "NULLABLE",
                "IS_AUTOINCREMENT",
                "REMARKS",
                "COLUMN_DEF",
                "COLUMN_SIZE",
                "DECIMAL_DIGITS")) {
          column.put(label, String.valueOf(rows.getString(label)));
        }
        columns.put(rows.getString("COLUMN_NAME"), column);
      }
    }
    return columns;
  }

  @Test
  void columnsCarryNullabilityAutoIncrementAndPrecision() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      Map<String, Map<String, String>> columns = describeMetaTable(connection);
      assertEquals("NO", columns.get("id").get("IS_NULLABLE"));
      assertEquals("0", columns.get("id").get("NULLABLE"));
      assertEquals("NO", columns.get("name").get("IS_NULLABLE"));
      assertEquals("YES", columns.get("salary").get("IS_NULLABLE"));
      assertEquals("YES", columns.get("note").get("IS_NULLABLE"));
      assertEquals("YES", columns.get("id").get("IS_AUTOINCREMENT"));
      assertEquals("NO", columns.get("name").get("IS_AUTOINCREMENT"));
      assertEquals("10", columns.get("salary").get("COLUMN_SIZE"));
      assertEquals("2", columns.get("salary").get("DECIMAL_DIGITS"));
      assertEquals(
          "10", columns.get("id").get("COLUMN_SIZE"), "an INTEGER's size is in digits, not bits");
    }
  }

  @Test
  void columnsCarryCommentsAndDefaults() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      Map<String, Map<String, String>> columns = describeMetaTable(connection);
      assertEquals("employee name", columns.get("name").get("REMARKS"));
      assertTrue(columns.get("id").get("COLUMN_DEF").contains("nextval"));
      assertTrue(columns.get("salary").get("COLUMN_DEF").contains("50000"));
    }
  }

  @Test
  void primaryKeys() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      ResultSet rows = connection.getMetaData().getPrimaryKeys(null, "meta", "parent");
      assertEquals(
          List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
          labels(rows));
      assertTrue(rows.next());
      assertEquals("a", rows.getString("COLUMN_NAME"));
      assertEquals((short) 1, rows.getObject("KEY_SEQ"));
      assertEquals("parent", rows.getString("TABLE_NAME"));
      assertTrue(rows.next());
      assertEquals("b", rows.getString("COLUMN_NAME"));
      assertEquals(2, rows.getShort("KEY_SEQ"));
      assertFalse(rows.next());
      rows.close();
      assertEquals(
          List.of(),
          column(
              connection.getMetaData().getPrimaryKeys(null, "meta", "child_view"), "COLUMN_NAME"));
    }
  }

  private static void assertForeignKey(ResultSet rows) throws SQLException {
    assertEquals(14, rows.getMetaData().getColumnCount());
    assertEquals("DEFERRABILITY", rows.getMetaData().getColumnLabel(14));
    String[] pk = {"a", "b"};
    String[] fk = {"pa", "pb"};
    for (int i = 0; i < 2; i++) {
      assertTrue(rows.next());
      assertEquals("parent", rows.getString("PKTABLE_NAME"));
      assertEquals(pk[i], rows.getString("PKCOLUMN_NAME"));
      assertEquals("child", rows.getString("FKTABLE_NAME"));
      assertEquals(fk[i], rows.getString("FKCOLUMN_NAME"));
      assertEquals(i + 1, rows.getShort("KEY_SEQ"));
      assertEquals("meta", rows.getString("PKTABLE_SCHEM"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, rows.getShort("DEFERRABILITY"));
      short updateRule = rows.getShort("UPDATE_RULE");
      assertTrue(updateRule >= 0 && updateRule <= 4, "a JDBC rule code: " + updateRule);
    }
    assertFalse(rows.next());
    rows.close();
  }

  @Test
  void importedKeys() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertForeignKey(connection.getMetaData().getImportedKeys(null, "meta", "child"));
    }
  }

  @Test
  void exportedKeys() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertForeignKey(connection.getMetaData().getExportedKeys(null, "meta", "parent"));
    }
  }

  @Test
  void crossReference() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      assertForeignKey(
          connection
              .getMetaData()
              .getCrossReference(null, "meta", "parent", null, "meta", "child"));
    }
  }

  @Test
  void typeInfoListsTheServersTypesOrderedByDataType() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      ResultSet rows = connection.getMetaData().getTypeInfo();
      assertEquals(18, rows.getMetaData().getColumnCount());
      assertEquals("TYPE_NAME", rows.getMetaData().getColumnLabel(1));
      Map<String, Integer> dataTypes = new HashMap<>();
      int previous = Integer.MIN_VALUE;
      while (rows.next()) {
        int dataType = rows.getInt("DATA_TYPE");
        assertTrue(dataType >= previous, "getTypeInfo is ordered by DATA_TYPE");
        previous = dataType;
        dataTypes.putIfAbsent(rows.getString("TYPE_NAME"), dataType);
      }
      rows.close();
      assertEquals(Types.INTEGER, dataTypes.get("integer"));
      assertEquals(Types.BIGINT, dataTypes.get("bigint"));
      assertEquals(Types.VARCHAR, dataTypes.get("varchar"));
      assertEquals(Types.DECIMAL, dataTypes.get("decimal"));
    }
  }

  @Test
  void metadataWithoutAnEndpointIsEmptyWithJdbcColumns() throws SQLException {
    try (Connection connection = SharedServer.connect()) {
      DatabaseMetaData metaData = connection.getMetaData();
      List<ResultSet> results =
          List.of(
              metaData.getIndexInfo(null, null, "child", false, false),
              metaData.getProcedures(null, null, null),
              metaData.getProcedureColumns(null, null, null, null),
              metaData.getFunctions(null, null, null),
              metaData.getFunctionColumns(null, null, null, null),
              metaData.getTablePrivileges(null, null, null),
              metaData.getColumnPrivileges(null, null, "child", null),
              metaData.getBestRowIdentifier(null, null, "child", 0, true),
              metaData.getVersionColumns(null, null, "child"),
              metaData.getUDTs(null, null, null, null),
              metaData.getSuperTypes(null, null, null),
              metaData.getSuperTables(null, null, null),
              metaData.getAttributes(null, null, null, null),
              metaData.getClientInfoProperties(),
              metaData.getPseudoColumns(null, null, null, null));
      int[] columnCounts = {13, 9, 20, 6, 17, 7, 8, 8, 8, 7, 6, 4, 21, 4, 12};
      for (int i = 0; i < results.size(); i++) {
        ResultSet rows = results.get(i);
        assertEquals(columnCounts[i], rows.getMetaData().getColumnCount(), "result " + i);
        assertFalse(rows.next());
        assertNull(rows.getStatement());
        rows.close();
      }
      try (ResultSet index = metaData.getIndexInfo(null, null, "child", false, false)) {
        assertEquals("INDEX_NAME", index.getMetaData().getColumnLabel(6));
        assertEquals(Types.BOOLEAN, index.getMetaData().getColumnType(4));
      }
    }
  }

  @Test
  void metadataResultsReadLikeAnyOther() throws SQLException {
    try (Connection connection = SharedServer.connect();
        ResultSet rows = connection.getMetaData().getColumns(null, "meta", "parent", null)) {
      assertTrue(rows.isBeforeFirst());
      assertTrue(rows.next());
      assertNotNull(rows.getObject(1));
      assertEquals(rows.getString("TABLE_NAME"), rows.getString(3), "by index and by name");
      assertEquals(Integer.class, rows.getObject("DATA_TYPE").getClass());
      assertNull(rows.getObject("SOURCE_DATA_TYPE"));
      assertTrue(rows.wasNull());
    }
  }
}
