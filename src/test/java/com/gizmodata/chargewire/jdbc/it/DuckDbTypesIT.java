package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireInterval;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Every DuckDB type, as an application meets it without choosing anything: its JDBC type, type name
 * and {@code getObject} class, from a nullable column, a NOT NULL column and an empty result, and
 * back to the server as a parameter.
 *
 * <p>The driver asks a DuckDB server for its exact Arrow types ({@code arrowLosslessConversion}, on
 * by default), so UUID is a UUID, TIME WITH TIME ZONE keeps its offset, BIT is its bits and HUGEINT
 * is exact through its whole range. With the property off, or after {@code SET SESSION
 * arrow_lossless_conversion = false}, the same columns read as the server's plainer Arrow types.
 */
class DuckDbTypesIT {
  private static final String HUGEINT_MAX = "170141183460469231731687303715884105727";
  private static final String HUGEINT_MIN = "-170141183460469231731687303715884105728";
  private static final String UHUGEINT_MAX = "340282366920938463463374607431768211455";
  private static final String BIGNUM = "-123456789012345678901234567890123456789012345";
  private static final UUID ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

  /** One column: what DuckDB calls it, a value, and what JDBC must say about it. */
  private record Column(
      String name,
      String sqlType,
      String literal,
      String typeName,
      int jdbcType,
      Class<?> javaClass,
      Object value,
      String text,
      boolean bindable) {}

  private static Column column(
      String name,
      String sqlType,
      String literal,
      String typeName,
      int jdbcType,
      Class<?> javaClass,
      Object value,
      String text) {
    return new Column(name, sqlType, literal, typeName, jdbcType, javaClass, value, text, true);
  }

  /** A column whose value has no parameter form (README, "Not supported": struct parameters). */
  private static Column readOnly(
      String name,
      String sqlType,
      String literal,
      String typeName,
      int jdbcType,
      Class<?> javaClass,
      Object value,
      String text) {
    return new Column(name, sqlType, literal, typeName, jdbcType, javaClass, value, text, false);
  }

  private static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  private static List<Column> columns() {
    return List.of(
        column("c_bool", "BOOLEAN", "true", "BOOLEAN", Types.BOOLEAN, Boolean.class, true, "true"),
        column("c_tinyint", "TINYINT", "-1", "TINYINT", Types.TINYINT, Byte.class, (byte) -1, "-1"),
        column(
            "c_smallint",
            "SMALLINT",
            "-2",
            "SMALLINT",
            Types.SMALLINT,
            Short.class,
            (short) -2,
            "-2"),
        column("c_int", "INTEGER", "-3", "INTEGER", Types.INTEGER, Integer.class, -3, "-3"),
        column("c_bigint", "BIGINT", "-4", "BIGINT", Types.BIGINT, Long.class, -4L, "-4"),
        column(
            "c_hugeint",
            "HUGEINT",
            HUGEINT_MAX,
            "HUGEINT",
            Types.NUMERIC,
            BigDecimal.class,
            new BigDecimal(HUGEINT_MAX),
            HUGEINT_MAX),
        column(
            "c_utinyint",
            "UTINYINT",
            "255",
            "UTINYINT",
            Types.SMALLINT,
            Short.class,
            (short) 255,
            "255"),
        column(
            "c_ubigint",
            "UBIGINT",
            "18446744073709551615",
            "UBIGINT",
            Types.NUMERIC,
            BigDecimal.class,
            new BigDecimal("18446744073709551615"),
            "18446744073709551615"),
        column(
            "c_uhugeint",
            "UHUGEINT",
            UHUGEINT_MAX,
            "UHUGEINT",
            Types.NUMERIC,
            BigDecimal.class,
            new BigDecimal(UHUGEINT_MAX),
            UHUGEINT_MAX),
        column("c_float", "FLOAT", "1.5", "REAL", Types.REAL, Float.class, 1.5f, "1.5"),
        column("c_double", "DOUBLE", "2.5", "DOUBLE", Types.DOUBLE, Double.class, 2.5, "2.5"),
        column(
            "c_dec",
            "DECIMAL(18,3)",
            "12345.678",
            "DECIMAL(18,3)",
            Types.DECIMAL,
            BigDecimal.class,
            new BigDecimal("12345.678"),
            "12345.678"),
        column(
            "c_bignum",
            "BIGNUM",
            BIGNUM + "::BIGNUM",
            "BIGNUM",
            Types.NUMERIC,
            BigDecimal.class,
            new BigDecimal(BIGNUM),
            BIGNUM),
        column(
            "c_varchar",
            "VARCHAR",
            "'hello'",
            "VARCHAR",
            Types.VARCHAR,
            String.class,
            "hello",
            "hello"),
        column(
            "c_blob",
            "BLOB",
            "'\\xAA\\xBB'::BLOB",
            "VARBINARY",
            Types.VARBINARY,
            byte[].class,
            new byte[] {(byte) 0xAA, (byte) 0xBB},
            null),
        column("c_bit", "BIT", "'10101'::BIT", "BIT", Types.OTHER, String.class, "10101", "10101"),
        column(
            "c_uuid", "UUID", "'" + ID + "'", "UUID", Types.OTHER, UUID.class, ID, ID.toString()),
        column(
            "c_date",
            "DATE",
            "DATE '2026-11-13'",
            "DATE",
            Types.DATE,
            Date.class,
            Date.valueOf("2026-11-13"),
            "2026-11-13"),
        column(
            "c_time",
            "TIME",
            "TIME '13:14:15.123'",
            "TIME",
            Types.TIME,
            Time.class,
            Time.valueOf("13:14:15"),
            "13:14:15.123"),
        column(
            "c_timetz",
            "TIMETZ",
            "TIMETZ '13:14:15.123456+02:00'",
            "TIME WITH TIME ZONE",
            Types.TIME_WITH_TIMEZONE,
            OffsetTime.class,
            OffsetTime.of(13, 14, 15, 123_456_000, ZoneOffset.ofHours(2)),
            "13:14:15.123456+02:00"),
        column(
            "c_time_ns",
            "TIME_NS",
            "TIME_NS '13:14:15.123'",
            "TIME",
            Types.TIME,
            Time.class,
            Time.valueOf("13:14:15"),
            "13:14:15.123"),
        column(
            "c_ts",
            "TIMESTAMP",
            "TIMESTAMP '2026-11-13 13:14:15.123456'",
            "TIMESTAMP",
            Types.TIMESTAMP,
            Timestamp.class,
            Timestamp.valueOf("2026-11-13 13:14:15.123456"),
            "2026-11-13 13:14:15.123456"),
        column(
            "c_ts_s",
            "TIMESTAMP_S",
            "TIMESTAMP_S '2026-11-13 13:14:15'",
            "TIMESTAMP",
            Types.TIMESTAMP,
            Timestamp.class,
            Timestamp.valueOf("2026-11-13 13:14:15"),
            "2026-11-13 13:14:15"),
        column(
            "c_ts_ms",
            "TIMESTAMP_MS",
            "TIMESTAMP_MS '2026-11-13 13:14:15.123'",
            "TIMESTAMP",
            Types.TIMESTAMP,
            Timestamp.class,
            Timestamp.valueOf("2026-11-13 13:14:15.123"),
            "2026-11-13 13:14:15.123"),
        column(
            "c_ts_ns",
            "TIMESTAMP_NS",
            "TIMESTAMP_NS '2026-11-13 13:14:15.123456789'",
            "TIMESTAMP",
            Types.TIMESTAMP,
            Timestamp.class,
            Timestamp.valueOf("2026-11-13 13:14:15.123456789"),
            "2026-11-13 13:14:15.123456789"),
        column(
            "c_tstz",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ '2026-11-13 13:14:15.123456+00'",
            "TIMESTAMP WITH TIME ZONE",
            Types.TIMESTAMP_WITH_TIMEZONE,
            Timestamp.class,
            Timestamp.from(Instant.parse("2026-11-13T13:14:15.123456Z")),
            null),
        column(
            "c_interval",
            "INTERVAL",
            "INTERVAL '1 year 2 months 3 days 4 hours'",
            "INTERVAL",
            Types.OTHER,
            ChargewireInterval.class,
            new ChargewireInterval(14, 3, 4L * 3600 * 1_000_000_000L),
            "P1Y2M3DT4H"),
        column(
            "c_enum",
            "ENUM('sad', 'ok', 'happy')",
            "'ok'",
            "VARCHAR",
            Types.VARCHAR,
            String.class,
            "ok",
            "ok"),
        column(
            "c_list",
            "INTEGER[]",
            "[1, 2, 3]",
            "INTEGER[]",
            Types.ARRAY,
            List.class,
            List.of(1, 2, 3),
            "[1, 2, 3]"),
        column(
            "c_boollist",
            "BOOLEAN[]",
            "[true, false, NULL]",
            "BOOLEAN[]",
            Types.ARRAY,
            List.class,
            Arrays.asList(true, false, null),
            "[true, false, null]"),
        column(
            "c_uuidlist",
            "UUID[]",
            "['" + ID + "'::UUID]",
            "UUID[]",
            Types.ARRAY,
            List.class,
            List.of(ID),
            "[\"" + ID + "\"]"),
        column(
            "c_json",
            "JSON",
            "'{\"a\": [1,2]}'",
            "JSON",
            Types.VARCHAR,
            String.class,
            "{\"a\": [1,2]}",
            "{\"a\": [1,2]}"),
        column(
            "c_arr",
            "INTEGER[3]",
            "[4, 5, 6]",
            "INTEGER[]",
            Types.ARRAY,
            List.class,
            List.of(4, 5, 6),
            "[4, 5, 6]"),
        readOnly(
            "c_struct",
            "STRUCT(a INTEGER, b BOOLEAN, u UUID, t TIMETZ)",
            "{'a': 1, 'b': true, 'u': '" + ID + "', 't': TIMETZ '01:02:03-05:30'}",
            "STRUCT",
            Types.STRUCT,
            Map.class,
            map(
                "a",
                1,
                "b",
                true,
                "u",
                ID,
                "t",
                OffsetTime.of(1, 2, 3, 0, ZoneOffset.ofHoursMinutes(-5, -30))),
            null),
        readOnly(
            "c_map",
            "MAP(VARCHAR, BOOLEAN)",
            "MAP {'k': true}",
            "MAP",
            Types.JAVA_OBJECT,
            Map.class,
            map("k", true),
            "{\"k\": true}"),
        readOnly(
            "c_union",
            "UNION(num INTEGER, flag BOOLEAN)",
            "true::UNION(num INTEGER, flag BOOLEAN)",
            "UNION",
            Types.JAVA_OBJECT,
            Object.class,
            true,
            "true"));
  }

  private static void createTables(Statement statement, List<Column> columns) throws SQLException {
    StringBuilder nullable = new StringBuilder();
    StringBuilder notNull = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (Column column : columns) {
      String separator = nullable.length() == 0 ? "" : ", ";
      nullable.append(separator).append(column.name()).append(' ').append(column.sqlType());
      notNull
          .append(separator)
          .append(column.name())
          .append(' ')
          .append(column.sqlType())
          .append(" NOT NULL");
      values.append(separator).append(column.literal());
    }
    statement.execute("CREATE OR REPLACE TEMP TABLE all_types (id INTEGER, " + nullable + ")");
    statement.execute("CREATE OR REPLACE TEMP TABLE all_types_nn (id INTEGER, " + notNull + ")");
    statement.execute("CREATE OR REPLACE TEMP TABLE all_types_back (id INTEGER, " + nullable + ")");
    statement.execute("INSERT INTO all_types VALUES (1, " + values + ")");
    statement.execute("INSERT INTO all_types_nn VALUES (1, " + values + ")");
    statement.execute(
        "INSERT INTO all_types (id) VALUES (2)"); // every other column NULL in this row
  }

  private static void assertValue(Column column, Object expected, Object actual, String where) {
    if (expected instanceof byte[]) {
      assertArrayEquals((byte[]) expected, (byte[]) actual, column.name() + where);
    } else if (expected instanceof Time) {
      // java.sql.Time is to the millisecond, and its date is 1970-01-01 in the JVM's zone.
      assertEquals(expected.toString(), actual.toString(), column.name() + where);
    } else {
      assertEquals(expected, actual, column.name() + where);
    }
  }

  private static void assertMetadata(ResultSetMetaData metaData, List<Column> columns, String where)
      throws SQLException {
    assertEquals(columns.size() + 1, metaData.getColumnCount());
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      String at = column.name() + " in " + where;
      assertEquals(column.name(), metaData.getColumnLabel(i + 2), at);
      assertEquals(column.typeName(), metaData.getColumnTypeName(i + 2), at);
      assertEquals(column.jdbcType(), metaData.getColumnType(i + 2), at);
      assertEquals(column.javaClass().getName(), metaData.getColumnClassName(i + 2), at);
    }
  }

  @Test
  void everyTypeReadsWithItsJdbcTypeAndClassNullableNotNullAndEmpty() throws SQLException {
    List<Column> columns = columns();
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      createTables(statement, columns);
      for (String table : List.of("all_types", "all_types_nn")) {
        try (ResultSet rows = statement.executeQuery("SELECT * FROM " + table + " ORDER BY id")) {
          assertMetadata(rows.getMetaData(), columns, table);
          assertTrue(rows.next());
          for (Column column : columns) {
            String at = " in " + table;
            Object value = rows.getObject(column.name());
            assertFalse(rows.wasNull(), column.name() + at);
            assertTrue(
                column.javaClass().isInstance(value),
                column.name() + at + " is a " + value.getClass().getName());
            assertValue(column, column.value(), value, at);
            if (column.text() != null) {
              assertEquals(column.text(), rows.getString(column.name()), column.name() + at);
            }
          }
          if (table.equals("all_types")) {
            assertTrue(rows.next());
            for (Column column : columns) {
              assertNull(rows.getObject(column.name()), column.name());
              assertTrue(rows.wasNull(), column.name());
              assertNull(rows.getString(column.name()), column.name());
            }
          }
          assertFalse(rows.next());
        }
        try (ResultSet rows = statement.executeQuery("SELECT * FROM " + table + " WHERE false")) {
          assertMetadata(rows.getMetaData(), columns, "an empty result of " + table);
          assertFalse(rows.next());
        }
      }
      try (PreparedStatement prepared = connection.prepareStatement("SELECT * FROM all_types")) {
        assertMetadata(prepared.getMetaData(), columns, "a prepared statement, before it runs");
      }
    }
  }

  /**
   * What {@code getObject} returned goes back with {@code setObject}, and NULL with {@code
   * setObject(null)} and with {@code setNull} of the column's own JDBC type: the server then holds
   * the same values, column by column.
   */
  @Test
  void whatWasReadGoesBackAsAParameter() throws SQLException {
    List<Column> columns = new ArrayList<>();
    for (Column column : columns()) {
      if (column.bindable()) {
        columns.add(column);
      }
    }
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      createTables(statement, columns);
      StringBuilder marks = new StringBuilder("?");
      for (int i = 0; i < columns.size(); i++) {
        marks.append(", ?");
      }
      try (PreparedStatement insert =
              connection.prepareStatement("INSERT INTO all_types_back VALUES (" + marks + ")");
          ResultSet rows = statement.executeQuery("SELECT * FROM all_types ORDER BY id")) {
        ParameterMetaData parameters = insert.getParameterMetaData();
        assertTrue(rows.next());
        insert.setInt(1, 1);
        for (int i = 0; i < columns.size(); i++) {
          Column column = columns.get(i);
          assertEquals(column.jdbcType(), parameters.getParameterType(i + 2), column.name());
          assertEquals(column.typeName(), parameters.getParameterTypeName(i + 2), column.name());
          assertEquals(
              column.javaClass().getName(), parameters.getParameterClassName(i + 2), column.name());
          insert.setObject(i + 2, rows.getObject(column.name()));
        }
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 2);
        for (int i = 0; i < columns.size(); i++) {
          insert.setObject(i + 2, null);
        }
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 3);
        for (int i = 0; i < columns.size(); i++) {
          insert.setNull(i + 2, columns.get(i).jdbcType());
        }
        assertEquals(1, insert.executeUpdate());
        // And both rows in one batch: one Arrow column per parameter, a NULL first.
        insert.setInt(1, 4);
        for (int i = 0; i < columns.size(); i++) {
          insert.setObject(i + 2, null);
        }
        insert.addBatch();
        insert.setInt(1, 5);
        for (int i = 0; i < columns.size(); i++) {
          insert.setObject(i + 2, rows.getObject(columns.get(i).name()));
        }
        insert.addBatch();
        assertEquals(2, insert.executeBatch().length);
      }
      for (Column column : columns) {
        // TIME reads as java.sql.Time, which is to the millisecond: the values here are too.
        try (ResultSet same =
            statement.executeQuery(
                "SELECT b.id, b."
                    + column.name()
                    + " IS NOT DISTINCT FROM s."
                    + column.name()
                    + " FROM all_types_back b JOIN all_types s"
                    + " ON s.id = CASE WHEN b.id IN (1, 5) THEN 1 ELSE 2 END ORDER BY b.id")) {
          for (int id = 1; id <= 5; id++) {
            assertTrue(same.next());
            assertTrue(same.getBoolean(2), column.name() + " of row " + id + " came back as sent");
          }
        }
      }
    }
  }

  /** The ends of each range, and the forms a parameter may take, for the types DuckDB encodes. */
  @Test
  void theEncodedTypesTakeAndGiveTheirWholeRange() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE OR REPLACE TEMP TABLE ranges (id INTEGER, h HUGEINT, u UHUGEINT, n BIGNUM,"
              + " b BIT, t TIMETZ, f BOOLEAN)");
      String huge = "9".repeat(300);
      Object[][] rows = {
        {
          new BigInteger(HUGEINT_MAX),
          new BigInteger(UHUGEINT_MAX),
          new BigInteger(huge),
          "1",
          OffsetTime.of(23, 59, 59, 999_999_000, ZoneOffset.ofHoursMinutesSeconds(15, 59, 59)),
          true
        },
        {
          new BigDecimal(HUGEINT_MIN),
          BigDecimal.ZERO,
          new BigInteger("-" + huge),
          "0",
          OffsetTime.of(0, 0, 0, 0, ZoneOffset.ofHoursMinutesSeconds(-15, -59, -59)),
          false
        },
        {0L, 1, 0, "0000000011", OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC), "true"},
        {-1, (short) 255, -1L, "1".repeat(64) + "0", "01:02:03.5+05:30", 0},
        {
          HUGEINT_MAX,
          UHUGEINT_MAX,
          "12345678901234567890",
          "101010101",
          LocalTime.of(1, 2, 3),
          "false"
        },
      };
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO ranges VALUES (?, ?, ?, ?, ?, ?, ?)")) {
        for (int id = 0; id < rows.length; id++) {
          insert.setInt(1, id);
          for (int c = 0; c < rows[id].length; c++) {
            insert.setObject(c + 2, rows[id][c]);
          }
          assertEquals(1, insert.executeUpdate());
        }
        // The same rows again as one batch: numbers, text and times mixed in each column.
        for (int id = 0; id < rows.length; id++) {
          insert.setInt(1, 100 + id);
          for (int c = 0; c < rows[id].length; c++) {
            insert.setObject(c + 2, rows[id][c]);
          }
          insert.addBatch();
        }
        assertEquals(rows.length, insert.executeBatch().length);
      }
      String[][] stored = {
        {HUGEINT_MAX, UHUGEINT_MAX, huge, "1", "23:59:59.999999+15:59:59", "true"},
        {HUGEINT_MIN, "0", "-" + huge, "0", "00:00:00-15:59:59", "false"},
        {"0", "1", "0", "0000000011", "12:00:00+00", "true"},
        {"-1", "255", "-1", "1".repeat(64) + "0", "01:02:03.5+05:30", "false"},
        {HUGEINT_MAX, UHUGEINT_MAX, "12345678901234567890", "101010101", null, "false"},
      };
      try (ResultSet text =
              statement.executeQuery(
                  "SELECT id, h::VARCHAR, u::VARCHAR, n::VARCHAR, b::VARCHAR, t::VARCHAR,"
                      + " f::VARCHAR, t = TIME '01:02:03'::TIMETZ FROM ranges ORDER BY id");
          Statement second = connection.createStatement();
          ResultSet values = second.executeQuery("SELECT * FROM ranges ORDER BY id")) {
        for (int pass = 0; pass < 2; pass++) {
          for (int id = 0; id < rows.length; id++) {
            assertTrue(text.next());
            assertTrue(values.next());
            String at = "row " + text.getInt(1);
            for (int c = 0; c < 6; c++) {
              if (stored[id][c] != null) {
                assertEquals(stored[id][c], text.getString(c + 2), at + ", column " + (c + 2));
              }
            }
            if (id == 4) {
              assertTrue(text.getBoolean(8), "a LocalTime means what TIME '…'::TIMETZ means");
            }
            // And read back through the driver.
            assertEquals(new BigDecimal(stored[id][0]), values.getBigDecimal("h"), at);
            assertEquals(
                new BigInteger(stored[id][1]), values.getObject("u", BigInteger.class), at);
            assertEquals(new BigDecimal(stored[id][2]), values.getObject("n"), at);
            assertEquals(stored[id][2], values.getString("n"), at);
            assertEquals(stored[id][3], values.getObject("b"), at);
            assertEquals(Boolean.valueOf(stored[id][5]), values.getObject("f"), at);
            OffsetTime time = values.getObject("t", OffsetTime.class);
            assertEquals(time, values.getObject("t"), at);
            assertEquals(time.toLocalTime(), values.getObject("t", LocalTime.class), at);
            if (rows[id][4] instanceof OffsetTime) {
              assertEquals(rows[id][4], time, at);
            }
            assertEquals(
                java.time.LocalDate.EPOCH
                    .atTime(time.toLocalTime())
                    .toInstant(time.getOffset())
                    .toEpochMilli(),
                values.getTime("t").getTime(),
                at + ": getTime is that moment on 1970-01-01");
          }
        }
        assertFalse(text.next());
      }
      // A number that is no integer, and a value that is no bit string, are refused.
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO ranges (h, b) VALUES (?, ?)")) {
        insert.setObject(1, new BigDecimal("1.5"));
        insert.setString(2, "1");
        assertEquals(
            "22003", assertThrows(SQLDataException.class, insert::executeUpdate).getSQLState());
        insert.setObject(1, 1);
        insert.setObject(2, 5);
        SQLDataException bits = assertThrows(SQLDataException.class, insert::executeUpdate);
        assertEquals("22018", bits.getSQLState());
        assertEquals(
            "Parameter 2: Cannot convert 'Integer' to BIT (give the bits as text)",
            bits.getMessage());
      }
    }
  }

  /**
   * VARIANT reads as the Java value it holds, whatever that is, and as JSON text for an object or
   * an array; a scalar or a list bound to a VARIANT parameter becomes a Variant of its own type.
   */
  @Test
  void variantsReadAsTheValueTheyHold() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TEMP TABLE variants (id INTEGER, v VARIANT)");
      statement.execute(
          "INSERT INTO variants VALUES"
              + " (1, {'a': 1, 'b': [true, false], 'c': 'x', 'd': {'e': NULL}}::VARIANT),"
              + " (2, 42::VARIANT), (3, 'text'::VARIANT), (4, NULL),"
              + " (5, [1.5, 2.25]::DECIMAL(5,2)[]::VARIANT), (6, DATE '2026-01-02'::VARIANT),"
              + " (7, TIMESTAMPTZ '2026-01-02 03:04:05+00'::VARIANT),"
              + " (8, '"
              + ID
              + "'::UUID::VARIANT), (9, true::VARIANT), (10, 1.5::DOUBLE::VARIANT)");
      Map<String, Object> inner = new LinkedHashMap<>();
      inner.put("e", null);
      Object[] expected = {
        map("a", 1, "b", List.of(true, false), "c", "x", "d", inner),
        42,
        "text",
        null,
        List.of(new BigDecimal("1.50"), new BigDecimal("2.25")),
        java.time.LocalDate.of(2026, 1, 2),
        java.time.OffsetDateTime.parse("2026-01-02T03:04:05Z"),
        ID,
        true,
        1.5
      };
      String[] text = {
        "{\"a\": 1, \"b\": [true, false], \"c\": \"x\", \"d\": {\"e\": null}}",
        "42",
        "text",
        null,
        "[1.50, 2.25]",
        "2026-01-02",
        "2026-01-02T03:04:05Z",
        ID.toString(),
        "true",
        "1.5"
      };
      try (ResultSet rows = statement.executeQuery("SELECT v FROM variants ORDER BY id")) {
        ResultSetMetaData metaData = rows.getMetaData();
        assertEquals("VARIANT", metaData.getColumnTypeName(1));
        assertEquals(Types.OTHER, metaData.getColumnType(1));
        assertEquals("java.lang.Object", metaData.getColumnClassName(1));
        for (int i = 0; i < expected.length; i++) {
          assertTrue(rows.next());
          assertEquals(expected[i], rows.getObject(1), "row " + (i + 1));
          assertEquals(text[i], rows.getString(1), "row " + (i + 1));
          assertEquals(expected[i] == null, rows.wasNull());
        }
        assertFalse(rows.next());
      }
      try (ResultSet rows = statement.executeQuery("SELECT v FROM variants WHERE id = 2")) {
        assertTrue(rows.next());
        assertEquals(42L, rows.getLong(1));
        assertEquals(42, rows.getObject(1, Integer.class));
        assertEquals(new BigDecimal("42"), rows.getBigDecimal(1));
      }
      // Back as parameters: each becomes a Variant of the value's own type.
      Object[] bound = {42, "text", null, List.of(1, 2), true, 1.5, new BigDecimal("1.50"), -7L};
      String[] types = {
        "INT32",
        "VARCHAR",
        "VARIANT_NULL",
        "ARRAY(2)",
        "BOOL_TRUE",
        "DOUBLE",
        "DECIMAL(3, 2)",
        "INT64"
      };
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO variants VALUES (?, ?)")) {
        assertEquals(Types.OTHER, insert.getParameterMetaData().getParameterType(2));
        assertEquals("VARIANT", insert.getParameterMetaData().getParameterTypeName(2));
        for (int i = 0; i < bound.length; i++) {
          insert.setInt(1, 100 + i);
          insert.setObject(2, bound[i]);
          assertEquals(1, insert.executeUpdate());
        }
      }
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT v, variant_typeof(v) FROM variants WHERE id >= 100 ORDER BY id")) {
        for (int i = 0; i < bound.length; i++) {
          assertTrue(rows.next());
          assertEquals(bound[i], rows.getObject(1), "parameter " + i);
          assertEquals(types[i], rows.getString(2), "parameter " + i);
        }
      }
    }
  }

  /**
   * {@code arrowLosslessConversion=false} leaves the session as the server opens it, which is what
   * the driver did before the property existed: UUID as text, TIME WITH TIME ZONE without its
   * offset, BIT as bytes, HUGEINT as DECIMAL(38, 0), refused above 38 digits.
   */
  @Test
  void withTheOptionOffTheTypesAreTheServersPlainerOnes() throws SQLException {
    Properties off = new Properties();
    off.setProperty("arrowLosslessConversion", "false");
    try (Connection connection = SharedServer.connect(off);
        Statement statement = connection.createStatement()) {
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT current_setting('arrow_lossless_conversion')::VARCHAR AS setting, true AS f,"
                  + " '"
                  + ID
                  + "'::UUID AS u, TIMETZ '13:14:15+02:00' AS t, '10101'::BIT AS b,"
                  + " 12345::HUGEINT AS h, "
                  + BIGNUM
                  + "::BIGNUM AS n, '{\"a\": 1}'::JSON AS j")) {
        assertTrue(rows.next());
        assertEquals("false", rows.getString("setting"));
        ResultSetMetaData metaData = rows.getMetaData();
        assertEquals(true, rows.getObject("f"));
        assertEquals(ID.toString(), rows.getObject("u"));
        assertEquals(Types.VARCHAR, metaData.getColumnType(3));
        assertEquals(ID, rows.getObject("u", UUID.class), "still convertible");
        assertEquals("13:14:15", rows.getObject("t").toString());
        assertEquals(Types.TIME, metaData.getColumnType(4));
        assertArrayEquals(new byte[] {3, (byte) 0xF5}, (byte[]) rows.getObject("b"));
        assertEquals(Types.VARBINARY, metaData.getColumnType(5));
        assertEquals(new BigDecimal("12345"), rows.getObject("h"));
        assertEquals("DECIMAL(38,0)", metaData.getColumnTypeName(6));
        // BIGNUM arrives in DuckDB's own encoding either way, and reads as a number either way.
        assertEquals(new BigDecimal(BIGNUM), rows.getObject("n"));
        assertEquals("BIGNUM", metaData.getColumnTypeName(7));
        assertEquals("VARCHAR", metaData.getColumnTypeName(8));
      }
      if (ChargewireServer.NAME.equals("Hardcharger")) {
        // Hardcharger refuses a HUGEINT that DECIMAL(38, 0) cannot hold rather than send it
        // (TypesIT has the test of that); chargewire-duckdb sends it.
        SQLException refused =
            assertThrows(
                SQLException.class,
                () -> statement.executeQuery("SELECT " + HUGEINT_MAX + "::HUGEINT"));
        assertTrue(
            refused.getMessage().contains("arrow_lossless_conversion"), refused.getMessage());
      }
    }
  }

  /** A statement can switch the mode; the driver reads whichever form arrives. */
  @Test
  void theModeCanBeSwitchedInSql() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      String query = "SELECT true AS f, '" + ID + "'::UUID AS u, TIMETZ '13:14:15+02:00' AS t";
      try (ResultSet rows = statement.executeQuery(query)) {
        assertTrue(rows.next());
        assertEquals(ID, rows.getObject("u"), "the driver asked for exact types");
        assertEquals(OffsetTime.of(13, 14, 15, 0, ZoneOffset.ofHours(2)), rows.getObject("t"));
      }
      statement.execute("SET SESSION arrow_lossless_conversion = false");
      try (ResultSet rows = statement.executeQuery(query)) {
        assertTrue(rows.next());
        assertEquals(true, rows.getObject("f"));
        assertEquals(ID.toString(), rows.getObject("u"));
        assertEquals("13:14:15", rows.getObject("t").toString());
      }
      statement.execute("SET SESSION arrow_lossless_conversion = true");
      try (ResultSet rows = statement.executeQuery(query)) {
        assertTrue(rows.next());
        assertEquals(true, rows.getObject("f"));
        assertEquals(Types.BOOLEAN, rows.getMetaData().getColumnType(1));
        assertEquals(ID, rows.getObject("u"));
      }
    }
  }
}
