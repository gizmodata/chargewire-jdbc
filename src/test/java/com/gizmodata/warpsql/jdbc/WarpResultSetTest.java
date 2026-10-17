package com.gizmodata.warpsql.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.arrow.Rows;
import com.gizmodata.warpsql.jdbc.arrow.SqlTypes;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The result set's cursor and metadata over in-memory batches (ported from {@code ResultSetTest},
 * {@code ResultSetMetadataTest} and {@code ArrowFlightJdbcCursorTest}).
 */
class WarpResultSetTest {
  private final BufferAllocator allocator = new RootAllocator();

  @AfterEach
  void close() {
    allocator.close();
  }

  private static Field column(
      String name, ArrowType type, boolean nullable, Map<String, String> metadata) {
    return new Field(name, new FieldType(nullable, type, null, metadata), null);
  }

  private WarpResultSet result(List<Object[]> rows) throws SQLException {
    Schema schema =
        new Schema(
            List.of(
                column(
                    "id",
                    new ArrowType.Int(32, true),
                    false,
                    Map.of(
                        SqlTypes.CATALOG_NAME,
                        "lake",
                        SqlTypes.SCHEMA_NAME,
                        "sales",
                        SqlTypes.TABLE_NAME,
                        "orders",
                        SqlTypes.TYPE_NAME,
                        "INT4",
                        SqlTypes.IS_AUTO_INCREMENT,
                        "1",
                        SqlTypes.IS_READ_ONLY,
                        "1",
                        SqlTypes.IS_SEARCHABLE,
                        "0",
                        SqlTypes.IS_CASE_SENSITIVE,
                        "0")),
                column("Name", ArrowType.Utf8.INSTANCE, true, null),
                column(
                    "amount",
                    new ArrowType.Decimal(10, 2, 128),
                    true,
                    Map.of(SqlTypes.PRECISION, "8", SqlTypes.SCALE, "1")),
                column(
                    "ratio",
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                    true,
                    null)));
    return new WarpResultSet(
        null, new ResultBatches.InMemory(Rows.toRoot(allocator, schema, rows)), 0, null);
  }

  private WarpResultSet sample() throws SQLException {
    return result(
        List.of(
            new Object[] {1, "one", new BigDecimal("1.50"), 0.5},
            new Object[] {2, null, null, null},
            new Object[] {3, "three", new BigDecimal("3.25"), 2.0}));
  }

  @Test
  void walksForwardThroughTheRows() throws SQLException {
    try (WarpResultSet rows = sample()) {
      assertTrue(rows.isBeforeFirst());
      assertFalse(rows.isFirst());
      assertEquals(0, rows.getRow());
      assertTrue(rows.next());
      assertTrue(rows.isFirst());
      assertEquals(1, rows.getRow());
      assertEquals(1, rows.getInt("id"));
      assertTrue(rows.next());
      assertNull(rows.getString("name"));
      assertTrue(rows.wasNull());
      assertEquals(0.0, rows.getDouble(4));
      assertTrue(rows.wasNull());
      assertTrue(rows.next());
      assertEquals("three", rows.getString(2));
      assertFalse(rows.wasNull());
      assertFalse(rows.next());
      assertTrue(rows.isAfterLast());
      assertEquals(0, rows.getRow());
      assertFalse(rows.next(), "stays after the last row");
    }
  }

  @Test
  void anEmptyResultIsNeitherBeforeFirstNorAfterLast() throws SQLException {
    try (WarpResultSet rows = result(List.of())) {
      assertFalse(rows.isBeforeFirst());
      assertFalse(rows.next());
      assertFalse(rows.isAfterLast());
      assertEquals(4, rows.getMetaData().getColumnCount(), "the columns are known without rows");
    }
  }

  @Test
  void columnsAreFoundByLabelInAnyCase() throws SQLException {
    try (WarpResultSet rows = sample()) {
      assertEquals(2, rows.findColumn("name"));
      assertEquals(2, rows.findColumn("NAME"));
      assertEquals(
          "42S22",
          assertThrows(SQLException.class, () -> rows.findColumn("missing")).getSQLState());
      assertTrue(rows.next());
      assertEquals("one", rows.getString("NaMe"));
      assertEquals(
          "07009", assertThrows(SQLException.class, () -> rows.getString(5)).getSQLState());
      assertEquals(
          "07009", assertThrows(SQLException.class, () -> rows.getString(0)).getSQLState());
    }
  }

  @Test
  void readingWithoutARowFails() throws SQLException {
    try (WarpResultSet rows = sample()) {
      assertEquals("24000", assertThrows(SQLException.class, () -> rows.getInt(1)).getSQLState());
      while (rows.next()) {
        rows.getInt(1);
      }
      assertEquals("24000", assertThrows(SQLException.class, () -> rows.getInt(1)).getSQLState());
    }
  }

  @Test
  void aClosedResultFailsEveryCall() throws SQLException {
    WarpResultSet rows = sample();
    rows.close();
    rows.close();
    assertTrue(rows.isClosed());
    assertEquals("24000", assertThrows(SQLException.class, rows::next).getSQLState());
    assertEquals("24000", assertThrows(SQLException.class, rows::getMetaData).getSQLState());
    assertEquals("24000", assertThrows(SQLException.class, () -> rows.getString(1)).getSQLState());
    assertEquals("24000", assertThrows(SQLException.class, rows::wasNull).getSQLState());
  }

  @Test
  void itIsForwardOnlyAndReadOnly() throws SQLException {
    try (WarpResultSet rows = sample()) {
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, rows.getType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, rows.getConcurrency());
      assertEquals(ResultSet.FETCH_FORWARD, rows.getFetchDirection());
      assertEquals("24000", assertThrows(SQLException.class, rows::previous).getSQLState());
      assertEquals("24000", assertThrows(SQLException.class, () -> rows.absolute(1)).getSQLState());
      assertEquals("24000", assertThrows(SQLException.class, rows::beforeFirst).getSQLState());
      assertEquals(
          "24000",
          assertThrows(SQLException.class, () -> rows.setFetchDirection(ResultSet.FETCH_REVERSE))
              .getSQLState());
      assertThrows(SQLFeatureNotSupportedException.class, rows::isLast);
      assertTrue(rows.next());
      assertThrows(SQLFeatureNotSupportedException.class, () -> rows.updateInt(1, 5));
      assertThrows(SQLFeatureNotSupportedException.class, () -> rows.updateString("Name", "x"));
      assertThrows(SQLFeatureNotSupportedException.class, rows::insertRow);
      assertThrows(SQLFeatureNotSupportedException.class, rows::deleteRow);
      assertFalse(rows.rowUpdated());
      rows.setFetchSize(100);
      assertEquals(100, rows.getFetchSize());
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  void streamsAndLegacyGetters() throws Exception {
    try (WarpResultSet rows = sample()) {
      assertTrue(rows.next());
      try (InputStream unicode = rows.getUnicodeStream(2)) {
        assertEquals("one", new String(unicode.readAllBytes(), StandardCharsets.UTF_16BE));
      }
      assertEquals(new BigDecimal("1.5"), rows.getBigDecimal(3, 1), "rounded half up to the scale");
      assertEquals("one", rows.getClob(2).getSubString(1, 3));
      assertEquals("one", rows.getNString(2));
      assertEquals(
          new ByteArrayInputStream("one".getBytes(StandardCharsets.UTF_8)).available(),
          rows.getBinaryStream(2).available());
      assertThrows(
          SQLFeatureNotSupportedException.class,
          () -> rows.getObject(1, Map.of("x", String.class)));
      assertEquals(1, rows.getObject(1, Map.of()));
      assertEquals(
          "HY009",
          assertThrows(SQLException.class, () -> rows.getObject(1, (Class<?>) null)).getSQLState());
      assertTrue(rows.next());
      assertNull(rows.getBlob(2));
      assertNull(rows.getClob(2));
      assertNull(rows.getURL(2));
    }
  }

  @Test
  void metaDataReadsTheFlightSqlColumnKeys() throws SQLException {
    try (WarpResultSet rows = sample()) {
      ResultSetMetaData metaData = rows.getMetaData();
      assertEquals(4, metaData.getColumnCount());
      assertEquals("id", metaData.getColumnName(1));
      assertEquals("Name", metaData.getColumnLabel(2));
      assertEquals("lake", metaData.getCatalogName(1));
      assertEquals("sales", metaData.getSchemaName(1));
      assertEquals("orders", metaData.getTableName(1));
      assertEquals("", metaData.getTableName(2), "unknown is empty, as JDBC asks");
      assertEquals("INT4", metaData.getColumnTypeName(1));
      assertEquals(Types.INTEGER, metaData.getColumnType(1));
      assertTrue(metaData.isAutoIncrement(1));
      assertFalse(metaData.isAutoIncrement(2));
      assertTrue(metaData.isReadOnly(1));
      assertFalse(metaData.isWritable(1));
      assertTrue(metaData.isWritable(2));
      assertFalse(metaData.isSearchable(1));
      assertTrue(metaData.isSearchable(2));
      assertFalse(metaData.isCaseSensitive(1));
      assertTrue(
          metaData.isCaseSensitive(2), "text compares case-sensitively unless the server says");
      assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(1));
      assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(2));
      assertEquals(8, metaData.getPrecision(3), "the server's PRECISION wins over the Arrow type");
      assertEquals(1, metaData.getScale(3));
      assertEquals(10, metaData.getPrecision(1));
      assertEquals(15, metaData.getPrecision(4));
      assertTrue(metaData.isSigned(1));
      assertFalse(metaData.isSigned(2));
      assertFalse(metaData.isCurrency(3));
      assertEquals(11, metaData.getColumnDisplaySize(1), "ten digits and a sign");
      assertEquals(SqlTypes.UNBOUNDED, metaData.getColumnDisplaySize(2));
      assertEquals(Integer.class.getName(), metaData.getColumnClassName(1));
      assertEquals(BigDecimal.class.getName(), metaData.getColumnClassName(3));
      assertEquals(
          "07009", assertThrows(SQLException.class, () -> metaData.getColumnName(5)).getSQLState());
      assertTrue(metaData.isWrapperFor(WarpResultSetMetaData.class));
    }
  }

  @Test
  void theStatementOfADriverMadeResultIsNull() throws SQLException {
    try (WarpResultSet rows = sample()) {
      assertNull(rows.getStatement());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rows.getHoldability());
      assertNull(rows.getWarnings());
    }
  }
}
