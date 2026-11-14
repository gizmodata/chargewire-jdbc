package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * {@code executeBatch} stores what {@code executeUpdate} stores for the same rows one by one: for
 * every type of parameter, and for every Java value the type takes.
 *
 * <p>A batch goes to the server as Arrow, and an Arrow column has one type. Where the server
 * declares a parameter's type, every row is converted to it by the same rule alone or in a batch.
 * Where what is stored follows the type that is sent (a VARIANT, and a parameter the server gives
 * no type), a batch whose rows differ in type cannot be one column, and each run of rows of one
 * type is a request of its own.
 */
class BatchesIT {
  /** One way of setting a parameter. */
  private interface Setter {
    void set(PreparedStatement statement, int index) throws SQLException;
  }

  private static void put(Map<String, Setter> values, String label, Object value) {
    values.put(label, (statement, index) -> statement.setObject(index, value));
  }

  /** A value of every class a setter takes, and text in every form a column reads. */
  private static Map<String, Setter> values() {
    Map<String, Setter> values = new LinkedHashMap<>();
    put(values, "null", null);
    values.put("setNull VARCHAR", (statement, index) -> statement.setNull(index, Types.VARCHAR));
    values.put("setNull INTEGER", (statement, index) -> statement.setNull(index, Types.INTEGER));
    put(values, "Boolean", true);
    put(values, "Byte", (byte) 1);
    put(values, "Short", (short) 2);
    put(values, "Integer", 3);
    put(values, "Long", 4L);
    put(values, "Long, large", 4_000_000_000_000L);
    put(values, "Float", 1.5f);
    put(values, "Double", 2.5d);
    put(values, "BigInteger", new BigInteger("12345678901234567890"));
    put(values, "BigInteger, small", BigInteger.valueOf(5));
    put(values, "BigDecimal 1.5", new BigDecimal("1.5"));
    put(values, "BigDecimal 12345.678", new BigDecimal("12345.678"));
    put(values, "BigDecimal 7", new BigDecimal("7"));
    put(values, "text 1", "1");
    put(values, "text abc", "abc");
    put(values, "text true", "true");
    put(values, "text date", "2026-01-15");
    put(values, "text timestamp", "2026-01-15 12:34:56");
    put(values, "text timestamp, offset", "2026-01-15 12:34:56+02");
    put(values, "text time", "12:34:56");
    put(values, "text time, offset", "12:34:56+02:00");
    put(values, "text bits", "10101");
    put(values, "text json", "{\"a\": 1}");
    put(values, "text uuid", "00112233-4455-6677-8899-aabbccddeeff");
    put(values, "text interval", "3 days");
    put(values, "text list", "[1, 2]");
    put(values, "text enum", "happy");
    put(values, "byte[]", new byte[] {1, 2});
    put(values, "UUID", UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"));
    put(values, "LocalDate", LocalDate.of(2026, 1, 15));
    put(values, "LocalTime", LocalTime.of(12, 34, 56));
    put(values, "LocalTime, nanos", LocalTime.of(12, 34, 56, 123_456_789));
    put(values, "LocalDateTime", LocalDateTime.of(2026, 1, 15, 12, 34, 56, 123_456_000));
    put(values, "LocalDateTime, nanos", LocalDateTime.of(2026, 1, 15, 12, 34, 56, 123_456_789));
    put(values, "LocalDateTime, in a gap", LocalDateTime.of(2026, 3, 8, 2, 30));
    put(values, "Instant", Instant.parse("2026-01-15T12:34:56.123456Z"));
    put(values, "Instant, nanos", Instant.parse("2026-01-15T12:34:56.123456789Z"));
    put(values, "OffsetDateTime", OffsetDateTime.parse("2026-01-15T12:34:56+02:00"));
    put(
        values,
        "ZonedDateTime",
        ZonedDateTime.of(2026, 7, 15, 12, 34, 56, 0, ZoneId.of("Europe/Berlin")));
    put(values, "java.sql.Date", java.sql.Date.valueOf("2026-01-15"));
    put(values, "java.sql.Time", Time.valueOf("12:34:56"));
    put(values, "java.sql.Timestamp", Timestamp.valueOf("2026-01-15 12:34:56.123456"));
    put(values, "java.sql.Timestamp, nanos", Timestamp.valueOf("2026-01-15 12:34:56.123456789"));
    put(values, "java.util.Date", new java.util.Date(1_768_480_496_000L));
    put(values, "OffsetTime", OffsetTime.of(12, 34, 56, 0, ZoneOffset.ofHours(2)));
    put(values, "Duration", java.time.Duration.ofSeconds(18_000, 500_000_000));
    put(values, "Period", java.time.Period.of(1, 2, 3));
    Calendar tokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    values.put(
        "setTimestamp, Calendar",
        (statement, index) ->
            statement.setTimestamp(index, Timestamp.valueOf("2026-01-15 12:34:56"), tokyo));
    values.put(
        "setDate, Calendar",
        (statement, index) -> statement.setDate(index, java.sql.Date.valueOf("2026-01-15"), tokyo));
    values.put(
        "setTime, Calendar",
        (statement, index) -> statement.setTime(index, Time.valueOf("12:34:56"), tokyo));
    values.put(
        "setObject, TIMESTAMP",
        (statement, index) ->
            statement.setObject(index, Instant.parse("2026-01-15T12:34:56Z"), Types.TIMESTAMP));
    values.put(
        "setObject, TIMESTAMP WITH TIME ZONE",
        (statement, index) ->
            statement.setObject(
                index, LocalDateTime.of(2026, 1, 15, 12, 34, 56), Types.TIMESTAMP_WITH_TIMEZONE));
    values.put(
        "setObject, DECIMAL scale 2",
        (statement, index) -> statement.setObject(index, "1.005", Types.DECIMAL, 2));
    put(values, "List of Integer", List.of(1, 2));
    put(values, "List of Integer, three", List.of(1, 2, 3));
    put(values, "List of Long", List.of(1L, 2L));
    put(values, "List of String", List.of("sad", "happy"));
    put(values, "List of Boolean", List.of(true, false));
    put(values, "List of UUID", List.of(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")));
    put(values, "List of LocalDateTime", List.of(LocalDateTime.of(2026, 1, 15, 12, 34, 56)));
    put(values, "List of Instant", List.of(Instant.parse("2026-01-15T12:34:56Z")));
    put(values, "List, empty", List.of());
    put(values, "List of Integer and Long", List.of(1, 2L));
    put(values, "List of Integer and String", List.of(1, "2"));
    return values;
  }

  /** Every type of parameter: what DuckDB calls it. */
  private static final List<String> TYPES =
      List.of(
          "BOOLEAN",
          "TINYINT",
          "SMALLINT",
          "INTEGER",
          "BIGINT",
          "HUGEINT",
          "UTINYINT",
          "USMALLINT",
          "UINTEGER",
          "UBIGINT",
          "UHUGEINT",
          "FLOAT",
          "DOUBLE",
          "DECIMAL(18,3)",
          "DECIMAL(38,10)",
          "BIGNUM",
          "VARCHAR",
          "BLOB",
          "BIT",
          "UUID",
          "JSON",
          "DATE",
          "TIME",
          "TIME_NS",
          "TIMETZ",
          "TIMESTAMP",
          "TIMESTAMP_S",
          "TIMESTAMP_MS",
          "TIMESTAMP_NS",
          "TIMESTAMPTZ",
          "INTERVAL",
          "ENUM('sad', 'ok', 'happy')",
          "INTEGER[]",
          "BIGINT[]",
          "VARCHAR[]",
          "BOOLEAN[]",
          "UUID[]",
          "TIMESTAMP[]",
          "TIMESTAMPTZ[]",
          "ENUM('sad', 'ok', 'happy')[]",
          "INTEGER[3]",
          "VARIANT",
          "VARIANT[]");

  /** What a table holds: each row's value as text, and its type where a value has its own. */
  private static List<String> stored(Statement statement, String table, String type)
      throws SQLException {
    String own =
        type.equals("VARIANT")
            ? "variant_typeof(v)"
            : type.equals("VARIANT[]") ? "list_transform(v, lambda x: variant_typeof(x))" : "''";
    List<String> rows = new ArrayList<>();
    try (ResultSet result =
        statement.executeQuery(
            "SELECT id, typeof(v), CAST("
                + own
                + " AS VARCHAR), CAST(v AS VARCHAR) FROM "
                + table
                + " ORDER BY id")) {
      while (result.next()) {
        rows.add(
            result.getInt(1)
                + " | "
                + result.getString(2)
                + " | "
                + result.getString(3)
                + " | "
                + result.getString(4));
      }
    }
    return rows;
  }

  @Test
  void aBatchStoresWhatItsRowsStoreOneByOneForEveryTypeOfParameter() throws SQLException {
    Map<String, Setter> values = values();
    List<String> labels = new ArrayList<>(values.keySet());
    int rows = 0;
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      // Not the JVM's zone: what a value without a zone means must not depend on the path.
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      for (String type : TYPES) {
        statement.execute("CREATE OR REPLACE TEMP TABLE one_by_one (id INTEGER, v " + type + ")");
        statement.execute("CREATE OR REPLACE TEMP TABLE in_a_batch (id INTEGER, v " + type + ")");
        List<Integer> taken = new ArrayList<>();
        try (PreparedStatement single =
            connection.prepareStatement("INSERT INTO one_by_one VALUES (?, ?)")) {
          for (int i = 0; i < labels.size(); i++) {
            single.setInt(1, i);
            try {
              values.get(labels.get(i)).set(single, 2);
              single.executeUpdate();
              taken.add(i);
            } catch (SQLException e) {
              // Not a value for this type: it is not in the batch either.
            }
          }
        }
        assertTrue(taken.size() >= 4, type + " takes " + taken);
        try (PreparedStatement batch =
            connection.prepareStatement("INSERT INTO in_a_batch VALUES (?, ?)")) {
          for (int i : taken) {
            batch.setInt(1, i);
            values.get(labels.get(i)).set(batch, 2);
            batch.addBatch();
          }
          assertEquals(taken.size(), batch.executeBatch().length, type);
        } catch (SQLException e) {
          List<String> sent = new ArrayList<>();
          taken.forEach(i -> sent.add(labels.get(i)));
          throw new AssertionError(
              type + " took each of " + sent + " alone, and their batch failed: " + e, e);
        }
        List<String> one = stored(statement, "one_by_one", type);
        List<String> together = stored(statement, "in_a_batch", type);
        assertEquals(taken.size(), one.size(), type);
        for (int row = 0; row < one.size(); row++) {
          assertEquals(
              one.get(row),
              together.size() > row ? together.get(row) : null,
              type + ", " + labels.get(taken.get(row)) + " (id | typeof | own type | value)");
        }
        assertEquals(one, together, type);
        rows += taken.size();
      }
    }
    assertTrue(rows > 600, "rows compared: " + rows);
  }

  /**
   * The same for a parameter the server gives no type: an argument of a function that takes any
   * type. What arrives is what the function sees, so the stored text names the type that was sent.
   */
  @Test
  void aBatchSendsAnUntypedParameterAsItsRowsSendItOneByOne() throws SQLException {
    Map<String, Setter> values = values();
    List<String> labels = new ArrayList<>(values.keySet());
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      String columns = " (id INTEGER, type VARCHAR, v VARCHAR)";
      statement.execute("CREATE OR REPLACE TEMP TABLE one_by_one" + columns);
      statement.execute("CREATE OR REPLACE TEMP TABLE in_a_batch" + columns);
      String insert = " VALUES (?, typeof(?), format('{}', ?))";
      List<Integer> taken = new ArrayList<>();
      try (PreparedStatement single =
          connection.prepareStatement("INSERT INTO one_by_one" + insert)) {
        assertEquals("UNKNOWN", single.getParameterMetaData().getParameterTypeName(2));
        for (int i = 0; i < labels.size(); i++) {
          single.setInt(1, i);
          try {
            values.get(labels.get(i)).set(single, 2);
            values.get(labels.get(i)).set(single, 3);
            single.executeUpdate();
            taken.add(i);
          } catch (SQLException e) {
            // Not a value an untyped parameter takes.
          }
        }
      }
      assertTrue(taken.size() >= 40, "an untyped parameter takes " + taken);
      try (PreparedStatement batch =
          connection.prepareStatement("INSERT INTO in_a_batch" + insert)) {
        for (int i : taken) {
          batch.setInt(1, i);
          values.get(labels.get(i)).set(batch, 2);
          values.get(labels.get(i)).set(batch, 3);
          batch.addBatch();
        }
        assertEquals(taken.size(), batch.executeBatch().length);
      }
      List<String> one = new ArrayList<>();
      List<String> together = new ArrayList<>();
      for (String table : List.of("one_by_one", "in_a_batch")) {
        try (ResultSet result =
            statement.executeQuery("SELECT id, type, v FROM " + table + " ORDER BY id")) {
          while (result.next()) {
            (table.equals("one_by_one") ? one : together)
                .add(
                    labels.get(result.getInt(1))
                        + " | "
                        + result.getString(2)
                        + " | "
                        + result.getString(3));
          }
        }
      }
      assertEquals(taken.size(), one.size());
      for (int row = 0; row < one.size(); row++) {
        assertEquals(one.get(row), together.size() > row ? together.get(row) : null);
      }
    }
  }

  /**
   * What a VARIANT stores for a value of each Java class, alone or in a batch: the type the value
   * has, which reads back as the same class and goes back as the same type.
   */
  @Test
  void aVariantKeepsTheTypeOfEachJavaValue() throws SQLException {
    Map<Object, String> types = new LinkedHashMap<>();
    types.put(true, "BOOL_TRUE | true");
    types.put((byte) 1, "INT8 | 1");
    types.put((short) 2, "INT16 | 2");
    types.put(3, "INT32 | 3");
    types.put(4L, "INT64 | 4");
    types.put(1.5f, "FLOAT | 1.5");
    types.put(2.5d, "DOUBLE | 2.5");
    types.put(new BigDecimal("12345.678"), "DECIMAL(8, 3) | 12345.678");
    types.put(new BigInteger("12345678901234567890"), "DECIMAL(20, 0) | 12345678901234567890");
    types.put("abc", "VARCHAR | abc");
    types.put(new byte[] {1, 2}, "BLOB | \\x01\\x02");
    types.put(LocalDate.of(2026, 1, 15), "DATE | 2026-01-15");
    types.put(LocalTime.of(12, 34, 56), "TIME_MICROS | 12:34:56");
    types.put(
        LocalDateTime.of(2026, 1, 15, 12, 34, 56, 123_456_000),
        "TIMESTAMP_MICROS | 2026-01-15 12:34:56.123456");
    types.put(
        LocalDateTime.of(2026, 1, 15, 12, 34, 56, 123_456_789),
        "TIMESTAMP_NANOS | 2026-01-15 12:34:56.123456789");
    types.put(
        Instant.parse("2026-01-15T12:34:56Z"), "TIMESTAMP_MICROS_TZ | 2026-01-15 18:04:56+05:30");
    types.put(
        OffsetDateTime.parse("2026-01-15T12:34:56+02:00"),
        "TIMESTAMP_MICROS_TZ | 2026-01-15 16:04:56+05:30");
    types.put(List.of(1, 2), "ARRAY(2) | [1, 2]");
    types.put(List.of(List.of("a"), List.of("b", "c")), "ARRAY(2) | [[a], [b, c]]");
    // These two are stored as their text. CAST(? AS UUID) in the statement stores a UUID (below);
    // a Variant has no time with an offset that the server can send back.
    types.put(
        UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"),
        "VARCHAR | 00112233-4455-6677-8899-aabbccddeeff");
    types.put(OffsetTime.of(12, 34, 56, 0, ZoneOffset.ofHours(2)), "VARCHAR | 12:34:56+02:00");
    List<Object> values = new ArrayList<>(types.keySet());
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      statement.execute("CREATE OR REPLACE TEMP TABLE variants (id INTEGER, v VARIANT)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO variants VALUES (?, ?)")) {
        for (int i = 0; i < values.size(); i++) {
          insert.setInt(1, i);
          insert.setObject(2, values.get(i));
          assertEquals(1, insert.executeUpdate());
        }
        for (int i = 0; i < values.size(); i++) {
          insert.setInt(1, 100 + i);
          insert.setObject(2, values.get(i));
          insert.addBatch();
        }
        assertEquals(values.size(), insert.executeBatch().length);
        // What getObject returns for each goes back as the same type and value.
        try (ResultSet rows = statement.executeQuery("SELECT v FROM variants WHERE id < 100");
            PreparedStatement again =
                connection.prepareStatement("INSERT INTO variants VALUES (?, ?)")) {
          for (int i = 0; i < values.size(); i++) {
            assertTrue(rows.next());
            again.setInt(1, 200 + i);
            again.setObject(2, rows.getObject(1));
            again.addBatch();
          }
          assertEquals(values.size(), again.executeBatch().length);
        }
      }
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT id, variant_typeof(v) || ' | ' || CAST(v AS VARCHAR) FROM variants"
                  + " ORDER BY id")) {
        for (String how : List.of("alone", "in a batch", "as getObject returned it")) {
          for (Object value : values) {
            assertTrue(rows.next());
            assertEquals(
                types.get(value),
                rows.getString(2),
                value.getClass().getSimpleName() + " " + how + " (row " + rows.getInt(1) + ")");
          }
        }
      }
      UUID id = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
      try (PreparedStatement insert =
              connection.prepareStatement("INSERT INTO variants VALUES (400, CAST(? AS UUID))");
          PreparedStatement find =
              connection.prepareStatement(
                  "SELECT variant_typeof(v), v FROM variants WHERE v = CAST(? AS UUID)")) {
        insert.setObject(1, id);
        assertEquals(1, insert.executeUpdate());
        find.setObject(1, id);
        try (ResultSet rows = find.executeQuery()) {
          assertTrue(rows.next());
          assertEquals("UUID", rows.getString(1));
          assertEquals(id, rows.getObject(2));
          assertTrue(!rows.next(), "the UUIDs stored as text are not equal to a UUID");
        }
      }
      // A list whose values only text could hold together is refused, alone and in a batch.
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO variants VALUES (?, ?)")) {
        insert.setInt(1, 300);
        insert.setObject(2, List.of(1, "a"));
        SQLException refused = assertThrows(SQLException.class, insert::executeUpdate);
        assertEquals("07006", refused.getSQLState());
        assertTrue(
            refused.getMessage().contains("CAST(CAST(? AS JSON) AS VARIANT)"),
            refused.getMessage());
      }
    }
  }

  /**
   * A batch that takes several requests fails where a request fails: the requests before it stay
   * applied, and the exception has the counts of their entries. The server runs a request's rows
   * one after another, so with autocommit on the rows of the failing request before the one that
   * failed may be applied too (Hardcharger 2.12.0 applies them), as in a batch of one request. With
   * autocommit off, rolling back undoes them all.
   */
  @Test
  void aBatchOfSeveralRequestsReportsTheEntriesAppliedBeforeAFailure() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE batch_runs (id INTEGER PRIMARY KEY, v VARIANT)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO batch_runs VALUES (?, ?)")) {
        for (boolean autoCommit : new boolean[] {true, false}) {
          connection.setAutoCommit(autoCommit);
          // Two requests: ids 1 and 2 hold numbers, 3 and 1 text; the second has the duplicate.
          Object[] values = {10, 20, "thirty", "again"};
          int[] ids = {1, 2, 3, 1};
          for (int i = 0; i < ids.length; i++) {
            insert.setInt(1, ids[i]);
            insert.setObject(2, values[i]);
            insert.addBatch();
          }
          java.sql.BatchUpdateException failed =
              assertThrows(java.sql.BatchUpdateException.class, insert::executeBatch);
          assertEquals("23000", failed.getSQLState(), failed.getMessage());
          assertEquals(
              List.of(Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO),
              java.util.Arrays.stream(failed.getUpdateCounts()).boxed().toList());
          if (!autoCommit) {
            connection.rollback();
          }
          try (ResultSet rows =
              statement.executeQuery("SELECT list(id ORDER BY id) FROM batch_runs")) {
            assertTrue(rows.next());
            Object applied = rows.getObject(1);
            if (autoCommit) {
              assertTrue(
                  List.of(1, 2).equals(applied) || List.of(1, 2, 3).equals(applied),
                  "applied: " + applied);
            } else {
              assertNull(applied);
            }
          }
          if (!autoCommit) {
            connection.commit();
          }
          connection.setAutoCommit(true);
          statement.execute("DELETE FROM batch_runs");
        }
      } finally {
        connection.setAutoCommit(true);
        statement.execute("DROP TABLE batch_runs");
      }
    }
  }
}
