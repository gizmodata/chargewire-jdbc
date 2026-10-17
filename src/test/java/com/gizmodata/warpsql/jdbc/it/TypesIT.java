package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpInterval;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Every common DuckDB type, read through the getters JDBC applications use, and bound back. */
class TypesIT {
  private static ResultSet one(Statement statement, String sql) throws SQLException {
    ResultSet rows = statement.executeQuery(sql);
    assertTrue(rows.next());
    return rows;
  }

  @Test
  void integers() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(
                statement,
                "SELECT -1::TINYINT a, -2::SMALLINT b, -3::INTEGER c, -4::BIGINT d,"
                    + " 255::UTINYINT e, 65535::USMALLINT f, 4294967295::UINTEGER g,"
                    + " 18446744073709551615::UBIGINT h, 170141183460469231731687303715884105727::HUGEINT i")) {
      assertEquals((byte) -1, rows.getObject("a"));
      assertEquals((short) -2, rows.getObject("b"));
      assertEquals(-3, rows.getObject("c"));
      assertEquals(-4L, rows.getObject("d"));
      assertEquals((short) 255, rows.getObject("e"), "unsigned types widen, so values stay exact");
      assertEquals(65535, rows.getObject("f"));
      assertEquals(4294967295L, rows.getObject("g"));
      assertEquals(new BigDecimal("18446744073709551615"), rows.getObject("h"));
      assertEquals("18446744073709551615", rows.getString("h"));
      SQLDataException overflow = assertThrows(SQLDataException.class, () -> rows.getLong("h"));
      assertEquals("22003", overflow.getSQLState());
      assertEquals(new BigInteger("18446744073709551615"), rows.getObject("h", BigInteger.class));
      assertEquals(new BigDecimal("170141183460469231731687303715884105727"), rows.getBigDecimal("i"));
      assertEquals(-1, rows.getInt("a"));
      assertEquals(255, rows.getInt("e"));
      assertEquals("22003", assertThrows(SQLDataException.class, () -> rows.getByte("e")).getSQLState());
      assertTrue(rows.getBoolean("a"));
      assertEquals(-3.0, rows.getDouble("c"));
      assertEquals(BigDecimal.valueOf(-4), rows.getBigDecimal("d"));
      ResultSetMetaData metaData = rows.getMetaData();
      assertEquals(Types.TINYINT, metaData.getColumnType(1));
      assertEquals(Types.SMALLINT, metaData.getColumnType(5));
      assertEquals(Types.NUMERIC, metaData.getColumnType(8));
      assertEquals(20, metaData.getPrecision(8));
      assertFalse(metaData.isSigned(8));
      assertTrue(metaData.isSigned(4));
    }
  }

  @Test
  void floatingPointAndDecimals() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(
                statement,
                "SELECT 1.5::FLOAT r, 2.25::DOUBLE d, 'NaN'::DOUBLE nan, 123.45::DECIMAL(5,2) dec,"
                    + " 12345678901234567890.123456789::DECIMAL(38,9) big, -0.001::DECIMAL(4,3) small")) {
      assertEquals(1.5f, rows.getObject("r"));
      assertEquals(1.5f, rows.getFloat("r"));
      assertEquals(2.25, rows.getObject("d"));
      assertEquals(new BigDecimal("2.25"), rows.getBigDecimal("d"));
      assertTrue(Double.isNaN(rows.getDouble("nan")));
      assertEquals("22003", assertThrows(SQLDataException.class, () -> rows.getBigDecimal("nan")).getSQLState());
      assertEquals(new BigDecimal("123.45"), rows.getObject("dec"));
      assertEquals("123.45", rows.getString("dec"));
      assertEquals(123, rows.getInt("dec"), "the fraction is dropped toward zero");
      assertEquals(new BigDecimal("12345678901234567890.123456789"), rows.getBigDecimal("big"));
      assertEquals("-0.001", rows.getString("small"));
      ResultSetMetaData metaData = rows.getMetaData();
      assertEquals(Types.REAL, metaData.getColumnType(1));
      assertEquals(Types.DOUBLE, metaData.getColumnType(2));
      assertEquals(Types.DECIMAL, metaData.getColumnType(4));
      assertEquals(5, metaData.getPrecision(4));
      assertEquals(2, metaData.getScale(4));
      assertEquals(38, metaData.getPrecision(5));
      assertEquals(9, metaData.getScale(5));
    }
  }

  @Test
  void textBinaryBooleanAndUuid() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(
                statement,
                "SELECT 'héllo ✓' s, '\\x00\\xff'::BLOB b, true t, false f, '42' n,"
                    + " 'a1b2c3d4-0000-4000-8000-000000000001'::UUID u, 'ab'::ENUM('ab', 'cd') e")) {
      assertEquals("héllo ✓", rows.getString("s"));
      assertEquals("héllo ✓", rows.getObject("s"));
      assertArrayEquals("héllo ✓".getBytes(java.nio.charset.StandardCharsets.UTF_8), rows.getBytes("s"));
      assertArrayEquals(new byte[] {0, (byte) 0xff}, rows.getBytes("b"));
      assertArrayEquals(new byte[] {0, (byte) 0xff}, (byte[]) rows.getObject("b"));
      assertArrayEquals(new byte[] {0, (byte) 0xff}, rows.getBlob("b").getBytes(1, 2));
      assertTrue(rows.getBoolean("t"));
      assertFalse(rows.getBoolean("f"));
      assertEquals(1, rows.getInt("t"));
      assertEquals("true", rows.getString("t"));
      assertEquals(42, rows.getInt("n"), "numeric text converts");
      assertEquals("22018", assertThrows(SQLDataException.class, () -> rows.getInt("s")).getSQLState());
      UUID uuid = UUID.fromString("a1b2c3d4-0000-4000-8000-000000000001");
      assertEquals(uuid.toString(), rows.getString("u"));
      assertEquals(uuid, rows.getObject("u", UUID.class));
      assertEquals("ab", rows.getString("e"), "an enum is dictionary-encoded text");
      assertEquals("ab", rows.getObject("e"));
      assertEquals(Types.VARCHAR, rows.getMetaData().getColumnType(7));
      assertEquals(Types.VARBINARY, rows.getMetaData().getColumnType(2));
      assertEquals(Types.BOOLEAN, rows.getMetaData().getColumnType(3));
    }
  }

  @Test
  void datesAndTimes() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(
                statement,
                "SELECT DATE '2024-02-29' d, TIME '13:14:15.123456' t, TIME '00:00:00' midnight,"
                    + " TIMESTAMP '2024-02-29 13:14:15.123456' ts, TIMESTAMP_NS '2024-02-29 13:14:15.123456789' ns,"
                    + " TIMESTAMP_S '1969-12-31 23:59:59' before_epoch")) {
      assertEquals(Date.valueOf("2024-02-29"), rows.getObject("d"));
      assertEquals(LocalDate.of(2024, 2, 29), rows.getObject("d", LocalDate.class));
      assertEquals("2024-02-29", rows.getString("d"));
      assertEquals(Timestamp.valueOf("2024-02-29 00:00:00"), rows.getTimestamp("d"));
      assertEquals(LocalTime.of(13, 14, 15, 123_456_000), rows.getObject("t", LocalTime.class));
      assertEquals("13:14:15.123456", rows.getString("t"));
      assertEquals(Time.valueOf("13:14:15").getTime() + 123, rows.getTime("t").getTime());
      assertEquals("00:00:00", rows.getString("midnight"));
      assertEquals(Timestamp.valueOf("2024-02-29 13:14:15.123456"), rows.getTimestamp("ts"));
      assertEquals(LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000), rows.getObject("ts", LocalDateTime.class));
      assertEquals("2024-02-29 13:14:15.123456", rows.getString("ts"));
      assertEquals(Date.valueOf("2024-02-29"), rows.getDate("ts"));
      assertEquals(123_456_789, rows.getTimestamp("ns").getNanos(), "nanoseconds are kept");
      assertEquals("2024-02-29 13:14:15.123456789", rows.getString("ns"));
      assertEquals(LocalDateTime.of(1969, 12, 31, 23, 59, 59), rows.getObject("before_epoch", LocalDateTime.class));
      assertThrows(SQLException.class, () -> rows.getObject("ts", OffsetDateTime.class),
          "a wall-clock timestamp has no offset");

      Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      assertEquals(
          Instant.parse("2024-02-29T13:14:15.123456Z"), rows.getTimestamp("ts", utc).toInstant(),
          "a calendar says which zone the wall clock is in");
      Calendar tokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
      assertEquals(Instant.parse("2024-02-29T04:14:15.123456Z"), rows.getTimestamp("ts", tokyo).toInstant());
      assertEquals(Instant.parse("2024-02-28T15:00:00Z").toEpochMilli(), rows.getDate("d", tokyo).getTime());
      ResultSetMetaData metaData = rows.getMetaData();
      assertEquals(Types.DATE, metaData.getColumnType(1));
      assertEquals(Types.TIME, metaData.getColumnType(2));
      assertEquals(Types.TIMESTAMP, metaData.getColumnType(4));
      assertEquals(6, metaData.getScale(4));
      assertEquals(9, metaData.getScale(5));
    }
  }

  @Test
  void timestampsWithTimeZoneAreInstants() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'America/New_York'");
      try (ResultSet rows = one(statement, "SELECT TIMESTAMPTZ '2024-07-01 12:00:00.5+00' tz")) {
        Instant instant = Instant.parse("2024-07-01T12:00:00.500Z");
        assertEquals(instant, rows.getTimestamp("tz").toInstant());
        assertEquals(instant, rows.getObject("tz", Instant.class));
        Calendar tokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
        assertEquals(instant, rows.getTimestamp("tz", tokyo).toInstant(), "a calendar never shifts an instant");
        OffsetDateTime offset = rows.getObject("tz", OffsetDateTime.class);
        assertEquals(instant, offset.toInstant());
        assertEquals(ZoneOffset.ofHours(-4), offset.getOffset(), "in the session's time zone");
        assertEquals("2024-07-01 08:00:00.5-04:00", rows.getString("tz"));
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, rows.getMetaData().getColumnType(1));
      }
    }
  }

  @Test
  void intervals() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(statement, "SELECT INTERVAL '1 year 2 months 3 days 04:05:06.5' i, INTERVAL 90 MINUTE m")) {
      assertEquals(new WarpInterval(14, 3, 14_706_500_000_000L), rows.getObject("i"));
      assertEquals("P1Y2M3DT4H5M6.5S", rows.getString("i"));
      assertEquals(Duration.ofMinutes(90), rows.getObject("m", Duration.class));
      assertEquals(Types.OTHER, rows.getMetaData().getColumnType(1));
    }
  }

  @Test
  void nestedValues() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(
                statement,
                "SELECT [DATE '2024-01-01', NULL] dates, {'n': 1, 'l': [1.5, 2.5]} s,"
                    + " [{'k': 'a'}, {'k': 'b'}] structs, [1, 2]::INTEGER[2] fixed")) {
      assertEquals(java.util.Arrays.asList(Date.valueOf("2024-01-01"), null), rows.getObject("dates"));
      assertEquals("[\"2024-01-01\", null]", rows.getString("dates"));
      assertEquals(
          "{\"n\": 1, \"l\": [1.5, 2.5]}", rows.getString("s"));
      assertEquals(List.of(java.util.Map.of("k", "a"), java.util.Map.of("k", "b")), rows.getObject("structs"));
      assertEquals(List.of(1, 2), rows.getObject("fixed"));
      assertEquals(Types.ARRAY, rows.getMetaData().getColumnType(1));
      assertEquals(Types.STRUCT, rows.getMetaData().getColumnType(2));
      assertEquals("DATE[]", rows.getMetaData().getColumnTypeName(1));
    }
  }

  @Test
  void nullsOfEveryType() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        ResultSet rows =
            one(
                statement,
                "SELECT NULL::INTEGER, NULL::DOUBLE, NULL::DECIMAL(5,2), NULL::VARCHAR, NULL::BLOB,"
                    + " NULL::BOOLEAN, NULL::DATE, NULL::TIME, NULL::TIMESTAMP, NULL::TIMESTAMPTZ,"
                    + " NULL::INTERVAL, NULL::INTEGER[], NULL::STRUCT(a INT), NULL::UUID")) {
      for (int column = 1; column <= 14; column++) {
        assertNull(rows.getObject(column), "column " + column);
        assertTrue(rows.wasNull());
        assertNull(rows.getString(column));
      }
      assertFalse(rows.getBoolean(6));
      assertEquals(0, rows.getInt(1));
      assertEquals(0.0, rows.getDouble(2));
      assertNull(rows.getBigDecimal(3));
      assertNull(rows.getBytes(5));
      assertNull(rows.getDate(7));
      assertNull(rows.getTime(8));
      assertNull(rows.getTimestamp(9));
      assertNull(rows.getArray(12));
      assertNull(rows.getObject(9, LocalDateTime.class));
    }
  }

  // ---- Parameters ----

  @Test
  void parametersRoundTripWithExactTypes() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TEMP TABLE every_type (i INTEGER, b BIGINT, s SMALLINT, d DOUBLE, r REAL,"
              + " dec DECIMAL(18,4), v VARCHAR, bin BLOB, flag BOOLEAN, day DATE, tm TIME,"
              + " ts TIMESTAMP, tz TIMESTAMPTZ, u UUID)");
      try (PreparedStatement insert =
          connection.prepareStatement(
              "INSERT INTO every_type VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        insert.setInt(1, Integer.MIN_VALUE);
        insert.setLong(2, Long.MAX_VALUE);
        insert.setShort(3, (short) -7);
        insert.setDouble(4, Math.PI);
        insert.setFloat(5, 0.25f);
        insert.setBigDecimal(6, new BigDecimal("-12345678901234.5678"));
        insert.setString(7, "it's \"quoted\" ✓");
        insert.setBytes(8, new byte[] {0, 1, (byte) 0xfe, (byte) 0xff});
        insert.setBoolean(9, true);
        insert.setDate(10, Date.valueOf("1999-12-31"));
        insert.setObject(11, LocalTime.of(23, 59, 58, 999_999_000));
        insert.setTimestamp(12, Timestamp.valueOf("2026-10-16 21:04:05.123456"));
        insert.setObject(13, OffsetDateTime.parse("2026-10-16T21:04:05.5+02:00"));
        insert.setObject(14, UUID.fromString("00000000-0000-4000-8000-00000000abcd"));
        assertEquals(1, insert.executeUpdate());
      }
      try (ResultSet rows = one(statement, "SELECT * FROM every_type")) {
        assertEquals(Integer.MIN_VALUE, rows.getInt("i"));
        assertEquals(Long.MAX_VALUE, rows.getLong("b"));
        assertEquals(-7, rows.getShort("s"));
        assertEquals(Math.PI, rows.getDouble("d"));
        assertEquals(0.25f, rows.getFloat("r"));
        assertEquals(new BigDecimal("-12345678901234.5678"), rows.getBigDecimal("dec"));
        assertEquals("it's \"quoted\" ✓", rows.getString("v"));
        assertArrayEquals(new byte[] {0, 1, (byte) 0xfe, (byte) 0xff}, rows.getBytes("bin"));
        assertTrue(rows.getBoolean("flag"));
        assertEquals(LocalDate.of(1999, 12, 31), rows.getObject("day", LocalDate.class));
        assertEquals(LocalTime.of(23, 59, 58, 999_999_000), rows.getObject("tm", LocalTime.class));
        assertEquals(LocalDateTime.of(2026, 10, 16, 21, 4, 5, 123_456_000), rows.getObject("ts", LocalDateTime.class));
        assertEquals(Instant.parse("2026-10-16T19:04:05.5Z"), rows.getObject("tz", Instant.class));
        assertEquals(UUID.fromString("00000000-0000-4000-8000-00000000abcd"), rows.getObject("u", UUID.class));
      }
    }
  }

  @Test
  @Disabled(
      "warp-duckdb refuses list-typed Arrow parameters with unsupported_arrow_type, though spec"
          + " §6.1 sends nested values through Arrow parameters (reported)")
  void listParameters() throws SQLException {
    try (Connection connection = SharedServer.connect();
        PreparedStatement select = connection.prepareStatement("SELECT ?::INTEGER[] AS l")) {
      select.setArray(1, connection.createArrayOf("INTEGER", new Object[] {3, null, 5}));
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(java.util.Arrays.asList(3, null, 5), rows.getObject("l"));
      }
    }
  }

  @Test
  @Disabled(
      "warp-duckdb refuses month_day_nano_interval Arrow parameters with unsupported_arrow_type,"
          + " though it returns INTERVAL results in that type (reported)")
  void intervalParameters() throws SQLException {
    try (Connection connection = SharedServer.connect();
        PreparedStatement select = connection.prepareStatement("SELECT ?::INTERVAL AS i")) {
      select.setObject(1, Duration.ofHours(25).plusMillis(1));
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(new WarpInterval(0, 0, Duration.ofHours(25).plusMillis(1).toNanos()), rows.getObject("i"));
      }
    }
  }

  @Test
  void timestampParametersHonourTheCalendar() throws SQLException {
    try (Connection connection = SharedServer.connect();
        PreparedStatement select =
            connection.prepareStatement("SELECT ?::TIMESTAMP AS wall, ?::TIMESTAMPTZ AS instant")) {
      Timestamp value = Timestamp.from(Instant.parse("2026-01-02T03:04:05Z"));
      Calendar tokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
      select.setTimestamp(1, value, tokyo);
      select.setTimestamp(2, value, tokyo);
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(
            LocalDateTime.of(2026, 1, 2, 12, 4, 5), rows.getObject("wall", LocalDateTime.class),
            "the wall clock in the calendar's zone");
        assertEquals(Instant.parse("2026-01-02T03:04:05Z"), rows.getObject("instant", Instant.class));
      }
    }
  }

  @Test
  void parametersConvertToTheDeclaredType() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE declared (i INTEGER, v VARCHAR, d DATE)");
      try (PreparedStatement insert = connection.prepareStatement("INSERT INTO declared VALUES (?, ?, ?)")) {
        insert.setString(1, "12");
        insert.setInt(2, 34);
        insert.setString(3, "2026-10-17");
        assertEquals(1, insert.executeUpdate());
        insert.setLong(1, 1L << 40);
        insert.setNull(2, Types.VARCHAR);
        insert.setNull(3, Types.DATE);
        SQLDataException error = assertThrows(SQLDataException.class, insert::executeUpdate);
        assertEquals("22003", error.getSQLState(), "an INTEGER parameter cannot hold 2^40");
        insert.setDouble(1, 1.5);
        assertEquals("22003", assertThrows(SQLDataException.class, insert::executeUpdate).getSQLState());
      }
      try (ResultSet rows = one(statement, "SELECT * FROM declared")) {
        assertEquals(12, rows.getInt(1));
        assertEquals("34", rows.getString(2));
        assertEquals(LocalDate.of(2026, 10, 17), rows.getObject(3, LocalDate.class));
        assertFalse(rows.next());
      }
    }
  }

  @Test
  void parameterMetaDataDescribesDeclaredTypes() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TEMP TABLE pmd (i INTEGER, v VARCHAR, ts TIMESTAMP)");
      try (PreparedStatement insert = connection.prepareStatement("INSERT INTO pmd VALUES (?, ?, ?)")) {
        java.sql.ParameterMetaData metaData = insert.getParameterMetaData();
        assertEquals(3, metaData.getParameterCount());
        assertEquals(Types.INTEGER, metaData.getParameterType(1));
        assertEquals(Types.VARCHAR, metaData.getParameterType(2));
        assertEquals(Types.TIMESTAMP, metaData.getParameterType(3));
        assertEquals(java.sql.ParameterMetaData.parameterModeIn, metaData.getParameterMode(1));
      }
      try (PreparedStatement select = connection.prepareStatement("SELECT ?")) {
        assertEquals(Types.OTHER, select.getParameterMetaData().getParameterType(1),
            "the server does not know an unconstrained parameter's type");
      }
    }
  }
}
