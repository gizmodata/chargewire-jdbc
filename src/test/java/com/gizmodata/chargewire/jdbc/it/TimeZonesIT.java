package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Parameters bound to {@code TIMESTAMP WITH TIME ZONE}, and such columns read back, under several
 * session time zones, with the JVM's default zone different from each of them but one.
 *
 * <p>The rule under test: a value without a zone ({@code LocalDateTime}, {@code LocalDate}, {@code
 * java.sql.Date}, text without an offset) means what the same SQL literal means in that session, so
 * every expectation for one is computed by the server from the literal ({@code TIMESTAMP
 * '…'::TIMESTAMPTZ}, {@code DATE '…'::TIMESTAMPTZ}). A value that is a point in time ({@code
 * Instant}, {@code OffsetDateTime}, {@code ZonedDateTime}, {@code java.sql.Timestamp}, {@code
 * java.util.Date}, {@code Calendar}, text with an offset) is that instant in every session. The
 * wall clocks cross daylight-saving changes of the session zones and of the JVM's zone, including
 * times a zone skips and times it has twice.
 */
class TimeZonesIT {
  /** The JVM's zone for these tests: far from the session zones, with its own daylight saving. */
  private static final String JVM_ZONE = "Pacific/Auckland";

  /**
   * UTC; New York (daylight saving on 2026-03-08 and 2026-11-01); Kolkata (a half-hour offset, no
   * daylight saving); Havana (clocks change at midnight, so 2026-03-08 has no 00:00 there); and the
   * JVM's own zone.
   */
  private static final List<String> SESSION_ZONES =
      List.of("UTC", "America/New_York", "Asia/Kolkata", "America/Havana", JVM_ZONE);

  private static final List<String> WALL_CLOCKS =
      List.of(
          "2026-01-15 12:34:56.123456",
          "2026-07-15 23:59:59.999999",
          // New York, 2026-03-08: 02:00 to 03:00 does not exist. Havana: 00:00 to 01:00.
          "2026-03-08 00:00:00",
          "2026-03-08 00:30:00",
          "2026-03-08 01:59:59",
          "2026-03-08 02:30:00",
          "2026-03-08 03:00:00",
          // New York, 2026-11-01: 01:00 to 02:00 happens twice. Havana: 00:00 to 01:00.
          "2026-11-01 00:30:00",
          "2026-11-01 01:30:00",
          "2026-11-01 02:00:00",
          // Auckland (the JVM): 2026-09-27 02:00 to 03:00 does not exist, 2026-04-05 02:00 to 03:00
          // happens twice.
          "2026-09-27 02:30:00",
          "2026-04-05 02:30:00",
          "1969-12-31 23:59:59.5");

  private static TimeZone jvmZoneBefore;

  @BeforeAll
  static void setJvmZone() {
    jvmZoneBefore = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(JVM_ZONE));
  }

  @AfterAll
  static void restoreJvmZone() {
    TimeZone.setDefault(jvmZoneBefore);
  }

  /** Binds parameter {@code index} one way. */
  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement statement, int index) throws SQLException;
  }

  private static LocalDateTime local(String wallClock) {
    return LocalDateTime.parse(wallClock.replace(' ', 'T'));
  }

  private static long epochMicros(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  private static long serverMicros(Statement statement, String expression) throws SQLException {
    try (ResultSet rows = statement.executeQuery("SELECT epoch_us(" + expression + ")")) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static Calendar calendar(String zone) {
    return Calendar.getInstance(TimeZone.getTimeZone(zone));
  }

  /** Every way to bind a date and time without a zone: each must mean the TIMESTAMP literal. */
  private static Map<String, Binder> withoutZone(String wallClock) {
    LocalDateTime dateTime = local(wallClock);
    Map<String, Binder> binders = new LinkedHashMap<>();
    binders.put("setObject(LocalDateTime)", (s, i) -> s.setObject(i, dateTime));
    binders.put(
        "setObject(LocalDateTime, TIMESTAMP)", (s, i) -> s.setObject(i, dateTime, Types.TIMESTAMP));
    binders.put(
        "setObject(LocalDateTime, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, dateTime, Types.TIMESTAMP_WITH_TIMEZONE));
    binders.put(
        "setObject(LocalDateTime, JDBCType.TIMESTAMP)",
        (s, i) -> s.setObject(i, dateTime, java.sql.JDBCType.TIMESTAMP));
    binders.put(
        "setObject(String, TIMESTAMP)", (s, i) -> s.setObject(i, wallClock, Types.TIMESTAMP));
    binders.put(
        "setObject(String, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, wallClock, Types.TIMESTAMP_WITH_TIMEZONE));
    return binders;
  }

  /** Every way to bind text: each must mean what the server makes of the same text. */
  private static Map<String, Binder> text(String text) {
    Map<String, Binder> binders = new LinkedHashMap<>();
    binders.put("setString", (s, i) -> s.setString(i, text));
    binders.put("setNString", (s, i) -> s.setNString(i, text));
    binders.put("setObject(String)", (s, i) -> s.setObject(i, text));
    binders.put("setObject(String, VARCHAR)", (s, i) -> s.setObject(i, text, Types.VARCHAR));
    binders.put("setCharacterStream", (s, i) -> s.setCharacterStream(i, new StringReader(text)));
    return binders;
  }

  /** Every way to bind a date without a zone: each must mean the DATE literal. */
  private static Map<String, Binder> dates(LocalDate date) {
    Map<String, Binder> binders = new LinkedHashMap<>();
    binders.put("setObject(LocalDate)", (s, i) -> s.setObject(i, date));
    binders.put("setObject(LocalDate, DATE)", (s, i) -> s.setObject(i, date, Types.DATE));
    binders.put(
        "setObject(LocalDate, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, date, Types.TIMESTAMP_WITH_TIMEZONE));
    binders.put(
        "setObject(LocalDateTime, DATE)",
        (s, i) -> s.setObject(i, date.atTime(13, 14, 15), Types.DATE));
    binders.put("setDate(java.sql.Date)", (s, i) -> s.setDate(i, Date.valueOf(date)));
    binders.put("setObject(java.sql.Date)", (s, i) -> s.setObject(i, Date.valueOf(date)));
    binders.put(
        "setObject(java.sql.Date, DATE)", (s, i) -> s.setObject(i, Date.valueOf(date), Types.DATE));
    binders.put(
        "setObject(java.sql.Date, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, Date.valueOf(date), Types.TIMESTAMP_WITH_TIMEZONE));
    binders.put(
        "setObject(String date, DATE)", (s, i) -> s.setObject(i, date.toString(), Types.DATE));
    return binders;
  }

  /** Every way to bind a point in time: each must be that instant, whatever the zones. */
  private static Map<String, Binder> instants(Instant instant, String sessionZone) {
    Timestamp timestamp = Timestamp.from(instant);
    java.util.Date utilDate = java.util.Date.from(instant.truncatedTo(ChronoUnit.MILLIS));
    OffsetDateTime offset = instant.atOffset(ZoneOffset.ofHoursMinutes(9, 30));
    ZonedDateTime paris = instant.atZone(ZoneId.of("Europe/Paris"));
    Map<String, Binder> binders = new LinkedHashMap<>();
    binders.put("setTimestamp", (s, i) -> s.setTimestamp(i, timestamp));
    binders.put(
        "setTimestamp(Calendar session zone)",
        (s, i) -> s.setTimestamp(i, timestamp, calendar(sessionZone)));
    binders.put(
        "setTimestamp(Calendar Tokyo)",
        (s, i) -> s.setTimestamp(i, timestamp, calendar("Asia/Tokyo")));
    binders.put("setObject(Timestamp)", (s, i) -> s.setObject(i, timestamp));
    binders.put(
        "setObject(Timestamp, TIMESTAMP)", (s, i) -> s.setObject(i, timestamp, Types.TIMESTAMP));
    binders.put(
        "setObject(Timestamp, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, timestamp, Types.TIMESTAMP_WITH_TIMEZONE));
    binders.put("setObject(Instant)", (s, i) -> s.setObject(i, instant));
    binders.put(
        "setObject(Instant, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, instant, Types.TIMESTAMP_WITH_TIMEZONE));
    binders.put(
        "setObject(Instant, TIMESTAMP)", (s, i) -> s.setObject(i, instant, Types.TIMESTAMP));
    binders.put("setObject(OffsetDateTime)", (s, i) -> s.setObject(i, offset));
    binders.put(
        "setObject(OffsetDateTime, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, offset, Types.TIMESTAMP_WITH_TIMEZONE));
    binders.put(
        "setObject(OffsetDateTime, TIMESTAMP)", (s, i) -> s.setObject(i, offset, Types.TIMESTAMP));
    binders.put("setObject(ZonedDateTime)", (s, i) -> s.setObject(i, paris));
    binders.put(
        "setObject(ZonedDateTime, TIMESTAMP)", (s, i) -> s.setObject(i, paris, Types.TIMESTAMP));
    binders.put("setString(ISO with offset)", (s, i) -> s.setString(i, offset.toString()));
    binders.put(
        "setString(with offset, space)",
        (s, i) -> s.setString(i, offset.toString().replace('T', ' ')));
    binders.put("setString(ISO Z)", (s, i) -> s.setString(i, instant.toString()));
    binders.put(
        "setObject(String with offset, TIMESTAMP_WITH_TIMEZONE)",
        (s, i) -> s.setObject(i, offset.toString(), Types.TIMESTAMP_WITH_TIMEZONE));
    if (instant.equals(utilDate.toInstant())) {
      // java.util.Date and Calendar hold milliseconds.
      binders.put("setObject(java.util.Date)", (s, i) -> s.setObject(i, utilDate));
      binders.put(
          "setObject(java.util.Date, TIMESTAMP)",
          (s, i) -> s.setObject(i, utilDate, Types.TIMESTAMP));
      binders.put("setObject(Calendar)", (s, i) -> s.setObject(i, GregorianCalendar.from(paris)));
    }
    return binders;
  }

  /** One expectation: what a binding must store, in microseconds since the epoch. */
  private record Case(String name, Binder binder, long expected) {}

  /**
   * The cases for one session and one wall clock, the expectations for values without a zone
   * computed by the server from the same literal in this session.
   */
  private static List<Case> cases(Statement statement, String zone, String wallClock)
      throws SQLException {
    List<Case> cases = new ArrayList<>();
    LocalDateTime dateTime = local(wallClock);
    long literal = serverMicros(statement, "TIMESTAMP '" + wallClock + "'::TIMESTAMPTZ");
    for (Map.Entry<String, Binder> binder : withoutZone(wallClock).entrySet()) {
      cases.add(new Case(binder.getKey(), binder.getValue(), literal));
    }
    LocalDate date = dateTime.toLocalDate();
    for (String text : List.of(wallClock, dateTime.toString(), date.toString())) {
      long textLiteral = serverMicros(statement, "'" + text + "'::TIMESTAMPTZ");
      String kind =
          text.equals(date.toString()) ? "date" : text.contains("T") ? "ISO" : "wall clock";
      for (Map.Entry<String, Binder> binder : text(text).entrySet()) {
        cases.add(new Case(binder.getKey() + "(" + kind + ")", binder.getValue(), textLiteral));
      }
      if (!kind.equals("date")) {
        assertEquals(literal, textLiteral, "text without an offset reads as the TIMESTAMP literal");
      }
    }
    long dateLiteral = serverMicros(statement, "DATE '" + date + "'::TIMESTAMPTZ");
    for (Map.Entry<String, Binder> binder : dates(date).entrySet()) {
      cases.add(new Case(binder.getKey(), binder.getValue(), dateLiteral));
    }
    cases.add(
        new Case(
            "setObject(LocalDate, TIMESTAMP)",
            (s, i) -> s.setObject(i, date, Types.TIMESTAMP),
            serverMicros(statement, "TIMESTAMP '" + date + " 00:00:00'::TIMESTAMPTZ")));
    // Points in time: the wall clock in the JVM's zone, as Timestamp.valueOf reads it, and the
    // same wall clock at a fixed offset.
    for (Instant instant :
        List.of(
            Timestamp.valueOf(dateTime).toInstant(),
            dateTime.toInstant(ZoneOffset.ofHoursMinutes(-3, -30)))) {
      long expected = epochMicros(instant);
      assertEquals(
          expected,
          serverMicros(
              statement, "TIMESTAMPTZ '" + instant.toString().replace("Z", "+00:00") + "'"),
          "the instant, as a literal with an offset");
      for (Map.Entry<String, Binder> binder : instants(instant, zone).entrySet()) {
        cases.add(new Case(binder.getKey() + " of " + instant, binder.getValue(), expected));
      }
    }
    // A java.sql.Date with a Calendar: that date as the Calendar's zone sees it, at its midnight.
    Date sqlDate = Date.valueOf(date);
    for (String calendarZone : List.of(zone, "Asia/Tokyo")) {
      ZoneId id = ZoneId.of(calendarZone);
      Instant midnight =
          Instant.ofEpochMilli(sqlDate.getTime())
              .atZone(id)
              .toLocalDate()
              .atStartOfDay(id)
              .toInstant();
      cases.add(
          new Case(
              "setDate(Calendar " + calendarZone + ")",
              (s, i) -> s.setDate(i, sqlDate, calendar(calendarZone)),
              epochMicros(midnight)));
    }
    return cases;
  }

  @Test
  void everySetterMeansTheSameAsItsLiteralInEverySessionZone() throws SQLException {
    assertEquals(JVM_ZONE, ZoneId.systemDefault().getId());
    int checked = 0;
    for (String zone : SESSION_ZONES) {
      try (Connection connection = SharedServer.connect();
          Statement statement = connection.createStatement()) {
        statement.execute("SET TimeZone = '" + zone + "'");
        try (PreparedStatement cast =
            connection.prepareStatement("SELECT epoch_us(CAST(? AS TIMESTAMPTZ))")) {
          assertEquals(
              Types.TIMESTAMP_WITH_TIMEZONE, cast.getParameterMetaData().getParameterType(1));
          for (String wallClock : WALL_CLOCKS) {
            for (Case c : cases(statement, zone, wallClock)) {
              c.binder().bind(cast, 1);
              try (ResultSet rows = cast.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(
                    c.expected(),
                    rows.getLong(1),
                    c.name() + " of " + wallClock + " in a " + zone + " session");
                checked++;
              }
            }
          }
        }
      }
    }
    assertTrue(checked > 3000, "checked " + checked);
  }

  /** The same through an INSERT into a TIMESTAMPTZ column and through a filter on one. */
  @Test
  void insertsAndFiltersAgreeWithTheLiteral() throws SQLException {
    for (String zone : SESSION_ZONES) {
      try (Connection connection = SharedServer.connect();
          Statement statement = connection.createStatement()) {
        statement.execute("SET TimeZone = '" + zone + "'");
        statement.execute("CREATE OR REPLACE TEMP TABLE tz_params (id INTEGER, ts TIMESTAMPTZ)");
        try (PreparedStatement insert =
                connection.prepareStatement("INSERT INTO tz_params VALUES (?, ?)");
            PreparedStatement filter =
                connection.prepareStatement(
                    "SELECT count(*), min(id), max(id) FROM tz_params WHERE ts = ? AND id = ?")) {
          List<Case> all = new ArrayList<>();
          for (String wallClock :
              List.of("2026-01-15 12:34:56.123456", "2026-03-08 02:30:00", "2026-11-01 01:30:00")) {
            all.addAll(cases(statement, zone, wallClock));
          }
          for (int id = 0; id < all.size(); id++) {
            insert.setInt(1, id);
            all.get(id).binder().bind(insert, 2);
            assertEquals(1, insert.executeUpdate());
          }
          try (ResultSet rows =
              statement.executeQuery("SELECT id, epoch_us(ts) FROM tz_params ORDER BY id")) {
            for (Case c : all) {
              assertTrue(rows.next());
              assertEquals(c.expected(), rows.getLong(2), "stored by " + c.name() + " in " + zone);
            }
          }
          for (int id = 0; id < all.size(); id++) {
            all.get(id).binder().bind(filter, 1);
            filter.setInt(2, id);
            try (ResultSet rows = filter.executeQuery()) {
              assertTrue(rows.next());
              assertEquals(1, rows.getLong(1), "found by " + all.get(id).name() + " in " + zone);
            }
          }
        }
      }
    }
  }

  /**
   * A batch is one Arrow column per parameter: rows that mix points in time, values without a zone
   * and text must each keep their own meaning, whatever comes first.
   */
  @Test
  void aBatchKeepsEachRowsMeaning() throws SQLException {
    for (String zone : SESSION_ZONES) {
      try (Connection connection = SharedServer.connect();
          Statement statement = connection.createStatement()) {
        statement.execute("SET TimeZone = '" + zone + "'");
        List<List<Case>> batches = new ArrayList<>();
        List<Case> mixed = new ArrayList<>();
        for (String wallClock : List.of("2026-11-01 01:30:00", "2026-01-15 12:34:56.123456")) {
          // One of each kind per wall clock, so every kind meets every other in one column.
          List<Case> cases = cases(statement, zone, wallClock);
          mixed.addAll(cases);
          batches.add(
              pick(cases, "setObject(LocalDateTime)", "setObject(LocalDateTime, TIMESTAMP)"));
          batches.add(pick(cases, "setObject(LocalDate)", "setDate(java.sql.Date)"));
          // A date first must not cut the time off the date-times after it, nor the reverse.
          batches.add(pick(cases, "setObject(LocalDate)", "setObject(LocalDateTime)"));
          batches.add(pick(cases, "setObject(LocalDateTime)", "setObject(LocalDate)"));
          batches.add(
              pick(cases, "setObject(Instant)", "setTimestamp", "setObject(OffsetDateTime)"));
          batches.add(pick(cases, "setObject(Instant)", "setObject(LocalDateTime)"));
          batches.add(pick(cases, "setObject(LocalDateTime)", "setObject(Instant)"));
          batches.add(pick(cases, "setString(wall clock)", "setObject(OffsetDateTime)"));
          batches.add(pick(cases, "setString(ISO Z)", "setString(wall clock)", "setString(date)"));
          batches.add(pick(cases, "setTimestamp", "setDate(java.sql.Date)", "setString(date)"));
        }
        batches.add(mixed);
        for (List<Case> batch : batches) {
          statement.execute("CREATE OR REPLACE TEMP TABLE tz_batch (id INTEGER, ts TIMESTAMPTZ)");
          try (PreparedStatement insert =
              connection.prepareStatement("INSERT INTO tz_batch VALUES (?, ?)")) {
            // A NULL first, and one between: neither decides the column's type.
            insert.setInt(1, -1);
            insert.setNull(2, Types.TIMESTAMP_WITH_TIMEZONE);
            insert.addBatch();
            for (int id = 0; id < batch.size(); id++) {
              insert.setInt(1, id);
              batch.get(id).binder().bind(insert, 2);
              insert.addBatch();
              if (id == 0) {
                insert.setInt(1, -2);
                insert.setObject(2, null);
                insert.addBatch();
              }
            }
            assertEquals(batch.size() + 2, insert.executeBatch().length);
          }
          try (ResultSet rows =
              statement.executeQuery(
                  "SELECT id, epoch_us(ts), ts IS NULL FROM tz_batch ORDER BY id")) {
            assertTrue(rows.next());
            assertTrue(rows.getBoolean(3), "NULL stays NULL");
            assertTrue(rows.next());
            assertTrue(rows.getBoolean(3), "NULL stays NULL");
            for (Case c : batch) {
              assertTrue(rows.next());
              assertEquals(
                  c.expected(),
                  rows.getLong(2),
                  c.name() + " in a batch of " + batch.size() + " in " + zone);
            }
          }
        }
      }
    }
  }

  private static List<Case> pick(List<Case> cases, String... prefixes) {
    List<Case> picked = new ArrayList<>();
    for (String prefix : prefixes) {
      for (Case c : cases) {
        if (c.name().equals(prefix) || c.name().startsWith(prefix + " of ")) {
          picked.add(c);
        }
      }
    }
    assertTrue(picked.size() >= prefixes.length, "found " + picked.size());
    return picked;
  }

  /**
   * A mixed column travels as text: the years the text form must get right, against the server's
   * own literals.
   */
  @Test
  void textForAMixedBatchReadsBackExactlyAtTheExtremes() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      statement.execute("CREATE OR REPLACE TEMP TABLE tz_extremes (id INTEGER, ts TIMESTAMPTZ)");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO tz_extremes VALUES (?, ?)")) {
        Object[] values = {
          LocalDateTime.of(1, 1, 1, 0, 0, 0),
          Instant.parse("9999-12-31T23:59:59.999999Z"),
          LocalDateTime.of(-43, 3, 15, 12, 0, 0, 1000),
          Instant.parse("-0043-03-15T12:00:00Z"),
          LocalDate.of(12345, 6, 7),
          LocalDateTime.of(12345, 6, 7, 8, 9, 10, 999_999_000),
          Instant.parse("1969-12-31T23:59:59.999999Z"),
        };
        for (int id = 0; id < values.length; id++) {
          insert.setInt(1, id);
          insert.setObject(2, values[id]);
          insert.addBatch();
        }
        insert.executeBatch();
      }
      String[] literals = {
        "TIMESTAMP '0001-01-01 00:00:00'::TIMESTAMPTZ",
        "TIMESTAMPTZ '9999-12-31 23:59:59.999999+00'",
        "TIMESTAMP '0044-03-15 (BC) 12:00:00.000001'::TIMESTAMPTZ",
        "TIMESTAMPTZ '0044-03-15 (BC) 12:00:00+00'",
        "DATE '12345-06-07'::TIMESTAMPTZ",
        "TIMESTAMP '12345-06-07 08:09:10.999999'::TIMESTAMPTZ",
        "TIMESTAMPTZ '1969-12-31 23:59:59.999999+00'",
      };
      for (int id = 0; id < literals.length; id++) {
        try (ResultSet rows =
            statement.executeQuery(
                "SELECT epoch_us(ts), epoch_us("
                    + literals[id]
                    + ") FROM tz_extremes WHERE id = "
                    + id)) {
          assertTrue(rows.next());
          assertEquals(rows.getLong(2), rows.getLong(1), literals[id]);
        }
      }
    }
  }

  /**
   * The session's zone at execution decides, not the one at prepare (the server declares the
   * parameter with the zone the session had then), and not the JVM's.
   */
  @Test
  void theZoneAtExecutionDecides() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'UTC'");
      try (PreparedStatement cast =
          connection.prepareStatement("SELECT epoch_us(CAST(? AS TIMESTAMPTZ))")) {
        LocalDateTime wallClock = LocalDateTime.of(2026, 7, 1, 12, 0);
        long[] seen = new long[3];
        String[] zones = {"UTC", "Asia/Kolkata", "America/New_York"};
        for (int i = 0; i < zones.length; i++) {
          statement.execute("SET TimeZone = '" + zones[i] + "'");
          cast.setObject(1, wallClock);
          try (ResultSet rows = cast.executeQuery()) {
            assertTrue(rows.next());
            seen[i] = rows.getLong(1);
          }
          assertEquals(
              serverMicros(statement, "TIMESTAMP '2026-07-01 12:00:00'::TIMESTAMPTZ"),
              seen[i],
              zones[i]);
        }
        assertEquals(epochMicros(Instant.parse("2026-07-01T12:00:00Z")), seen[0]);
        assertEquals(epochMicros(Instant.parse("2026-07-01T06:30:00Z")), seen[1]);
        assertEquals(epochMicros(Instant.parse("2026-07-01T16:00:00Z")), seen[2]);
      }
    }
  }

  /** Lists of TIMESTAMPTZ follow the same rule, element by element. */
  @Test
  void listElementsFollowTheSameRule() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      statement.execute("CREATE OR REPLACE TEMP TABLE tz_lists (id INTEGER, l TIMESTAMPTZ[])");
      LocalDateTime wallClock = LocalDateTime.of(2026, 1, 15, 12, 34, 56);
      Instant instant = Instant.parse("2026-01-15T00:00:00Z");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO tz_lists VALUES (?, ?)")) {
        insert.setInt(1, 1);
        insert.setObject(2, List.of(wallClock, wallClock.plusDays(1)));
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 2);
        insert.setObject(2, List.of(instant, Timestamp.from(instant)));
        assertEquals(1, insert.executeUpdate());
        insert.setInt(1, 3);
        insert.setObject(2, new Object[] {wallClock, instant, LocalDate.of(2026, 1, 15)});
        assertEquals(1, insert.executeUpdate());
      }
      long literal = serverMicros(statement, "TIMESTAMP '2026-01-15 12:34:56'::TIMESTAMPTZ");
      long dateLiteral = serverMicros(statement, "DATE '2026-01-15'::TIMESTAMPTZ");
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT id, list_transform(l, lambda x: epoch_us(x)) FROM tz_lists ORDER BY id")) {
        assertTrue(rows.next());
        assertEquals(List.of(literal, literal + 86_400_000_000L), rows.getObject(2));
        assertTrue(rows.next());
        assertEquals(List.of(epochMicros(instant), epochMicros(instant)), rows.getObject(2));
        assertTrue(rows.next());
        assertEquals(List.of(literal, epochMicros(instant), dateLiteral), rows.getObject(2));
      }
    }
  }

  /** A time of day has no date: refused, as the server refuses TIME for a TIMESTAMPTZ. */
  @Test
  void aTimeOfDayIsRefused() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        PreparedStatement cast =
            connection.prepareStatement("SELECT epoch_us(CAST(? AS TIMESTAMPTZ))")) {
      SQLException server =
          assertThrows(
              SQLException.class,
              () -> statement.executeQuery("SELECT TIME '12:34:56'::TIMESTAMPTZ"));
      assertTrue(
          server.getMessage().contains("TIME -> TIMESTAMP WITH TIME ZONE"), server.getMessage());
      List<String> classes = List.of("Time", "Time", "LocalTime", "LocalTime", "Integer");
      List<Binder> binders =
          List.of(
              (s, i) -> s.setTime(i, Time.valueOf("12:34:56")),
              (s, i) -> s.setTime(i, Time.valueOf("12:34:56"), calendar("UTC")),
              (s, i) -> s.setObject(i, LocalTime.of(12, 34, 56)),
              (s, i) -> s.setObject(i, LocalTime.of(12, 34, 56), Types.TIME),
              (s, i) -> s.setInt(i, 5));
      for (int b = 0; b < binders.size(); b++) {
        binders.get(b).bind(cast, 1);
        SQLDataException refused = assertThrows(SQLDataException.class, cast::executeQuery);
        assertEquals("22018", refused.getSQLState());
        assertEquals(
            "Parameter 1: Cannot convert '" + classes.get(b) + "' to TIMESTAMP WITH TIME ZONE",
            refused.getMessage());
      }
    }
  }

  /** Text the server cannot read as a timestamp is the server's error, as for the literal. */
  @Test
  void textThatIsNoTimestampIsTheServersError() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement();
        PreparedStatement cast =
            connection.prepareStatement("SELECT epoch_us(CAST(? AS TIMESTAMPTZ))")) {
      SQLException literal =
          assertThrows(
              SQLException.class,
              () -> statement.executeQuery("SELECT epoch_us('yesterday-ish'::TIMESTAMPTZ)"));
      cast.setString(1, "yesterday-ish");
      SQLException bound = assertThrows(SQLException.class, cast::executeQuery);
      assertTrue(bound.getMessage().contains("yesterday-ish"), bound.getMessage());
      assertTrue(bound.getMessage().contains("Conversion Error"), bound.getMessage());
      assertTrue(literal.getMessage().contains("Conversion Error"), literal.getMessage());
      // Forms the server reads and ISO does not: a date alone, an offset in hours, a zone's name.
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      for (String text :
          List.of("2026-01-15", "2026-01-15 12:00:00+05", "2026-01-15 12:00:00 America/New_York")) {
        cast.setString(1, text);
        try (ResultSet rows = cast.executeQuery()) {
          assertTrue(rows.next());
          assertEquals(serverMicros(statement, "'" + text + "'::TIMESTAMPTZ"), rows.getLong(1));
        }
      }
    }
  }

  /** A finer fraction than the column's microseconds is refused, not rounded, as before. */
  @Test
  void aFinerFractionThanTheParameterHoldsIsRefused() throws SQLException {
    try (Connection connection = SharedServer.connect();
        PreparedStatement insert =
            connection.prepareStatement("SELECT epoch_us(CAST(? AS TIMESTAMPTZ))")) {
      insert.setObject(1, LocalDateTime.of(2026, 1, 15, 12, 0, 0, 1));
      assertEquals(
          "22008", assertThrows(SQLDataException.class, insert::executeQuery).getSQLState());
      insert.setObject(1, Instant.parse("2026-01-15T12:00:00.000000001Z"));
      assertEquals(
          "22008", assertThrows(SQLDataException.class, insert::executeQuery).getSQLState());
      insert.setObject(1, LocalDateTime.of(2026, 1, 15, 12, 0, 0, 1));
      insert.addBatch();
      insert.setObject(1, Instant.parse("2026-01-15T12:00:00Z"));
      insert.addBatch();
      SQLException batch = assertThrows(SQLException.class, insert::executeBatch);
      assertEquals("22008", batch.getSQLState());
    }
  }

  /**
   * Reading: a TIMESTAMPTZ is an instant, shown in the session's zone. {@code LocalDateTime} is the
   * wall clock the server itself shows for it in this session; binding what was read stores the
   * same instant.
   */
  @Test
  void readingShowsTheInstantInTheSessionsZone() throws SQLException {
    for (String zone : SESSION_ZONES) {
      ZoneId zoneId = ZoneId.of(zone);
      try (Connection connection = SharedServer.connect();
          Statement statement = connection.createStatement()) {
        statement.execute("SET TimeZone = '" + zone + "'");
        statement.execute("CREATE OR REPLACE TEMP TABLE tz_read (id INTEGER, ts TIMESTAMPTZ)");
        List<Instant> instants = new ArrayList<>();
        for (String wallClock : WALL_CLOCKS) {
          instants.add(local(wallClock).toInstant(ZoneOffset.UTC));
          instants.add(local(wallClock).toInstant(ZoneOffset.ofHoursMinutes(5, 30)));
          instants.add(local(wallClock).toInstant(ZoneOffset.ofHours(-5)));
          instants.add(local(wallClock).toInstant(ZoneOffset.ofHours(13)));
        }
        try (PreparedStatement insert =
            connection.prepareStatement("INSERT INTO tz_read VALUES (?, ?)")) {
          for (int id = 0; id < instants.size(); id++) {
            insert.setInt(1, id);
            insert.setObject(2, instants.get(id));
            insert.addBatch();
          }
          insert.executeBatch();
        }
        try (ResultSet rows =
                statement.executeQuery(
                    "SELECT id, ts, CAST(ts AS TIMESTAMP) AS wall, CAST(ts AS VARCHAR) AS text"
                        + " FROM tz_read ORDER BY id");
            Statement literals = connection.createStatement();
            PreparedStatement back =
                connection.prepareStatement("SELECT epoch_us(CAST(? AS TIMESTAMPTZ))")) {
          assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, rows.getMetaData().getColumnType(2));
          for (Instant instant : instants) {
            assertTrue(rows.next());
            String at = instant + " in " + zone;
            assertEquals(instant, rows.getObject("ts", Instant.class), at);
            OffsetDateTime offset = rows.getObject("ts", OffsetDateTime.class);
            assertEquals(instant, offset.toInstant(), at);
            assertEquals(zoneId.getRules().getOffset(instant), offset.getOffset(), at);
            ZonedDateTime zoned = rows.getObject("ts", ZonedDateTime.class);
            assertEquals(instant, zoned.toInstant(), at);
            assertEquals(zoneId, zoned.getZone(), at);
            // The wall clock is the server's own for this session, not the JVM's.
            LocalDateTime wall = rows.getObject("wall", LocalDateTime.class);
            assertEquals(wall, rows.getObject("ts", LocalDateTime.class), at);
            assertEquals(wall.toLocalDate(), rows.getObject("ts", LocalDate.class), at);
            assertEquals(wall.toLocalTime(), rows.getObject("ts", LocalTime.class), at);
            // java.sql.Timestamp is the instant, with or without a Calendar.
            assertInstanceOf(Timestamp.class, rows.getObject("ts"));
            assertEquals(instant, ((Timestamp) rows.getObject("ts")).toInstant(), at);
            assertEquals(instant, rows.getTimestamp("ts").toInstant(), at);
            assertEquals(instant, rows.getTimestamp("ts", calendar("Asia/Tokyo")).toInstant(), at);
            assertEquals(instant, rows.getTimestamp("ts", calendar(zone)).toInstant(), at);
            // getString: the session's wall clock and its offset; the same instant as the
            // server's own text.
            String text = rows.getString("ts");
            OffsetDateTime parsed = OffsetDateTime.parse(text.replace(' ', 'T'));
            assertEquals(instant, parsed.toInstant(), text);
            assertEquals(wall, parsed.toLocalDateTime(), text);
            assertEquals(offset.getOffset(), parsed.getOffset(), text);
            assertTrue(rows.getString("text").startsWith(text.substring(0, 19)), text);
            // What was read, bound back, is the same instant.
            long micros = epochMicros(instant);
            Map<String, Binder> binders = new LinkedHashMap<>();
            binders.put("OffsetDateTime", (s, i) -> s.setObject(i, offset));
            binders.put("ZonedDateTime", (s, i) -> s.setObject(i, zoned));
            binders.put("Instant", (s, i) -> s.setObject(i, rows.getObject("ts", Instant.class)));
            binders.put("getTimestamp", (s, i) -> s.setTimestamp(i, rows.getTimestamp("ts")));
            binders.put("getObject", (s, i) -> s.setObject(i, rows.getObject("ts")));
            binders.put("getString", (s, i) -> s.setString(i, text));
            binders.put("the server's text", (s, i) -> s.setString(i, rows.getString("text")));
            for (Map.Entry<String, Binder> binder : binders.entrySet()) {
              binder.getValue().bind(back, 1);
              try (ResultSet again = back.executeQuery()) {
                assertTrue(again.next());
                assertEquals(micros, again.getLong(1), binder.getKey() + " of " + at);
              }
            }
            // The wall clock bound back means that wall clock in this session: the same instant,
            // except in the hour a zone has twice, where the server takes one of the two.
            back.setObject(1, wall);
            try (ResultSet again = back.executeQuery()) {
              assertTrue(again.next());
              long literal =
                  serverMicros(
                      literals,
                      "TIMESTAMP '" + wall.toString().replace('T', ' ') + "'::TIMESTAMPTZ");
              assertEquals(literal, again.getLong(1), "the wall clock of " + at);
              if (zoneId.getRules().getValidOffsets(wall).size() == 1) {
                assertEquals(micros, again.getLong(1), "the wall clock of " + at);
              }
            }
          }
        }
      }
    }
  }

  /**
   * The server takes zone names java.time does not know by that name: the three-letter IDs of old
   * JDKs ({@code PST}, {@code IST}, {@code EST}) and names the time zone database has dropped. The
   * wall clock must still be the session's, not UTC's.
   */
  @Test
  void sessionZonesJavaTimeDoesNotKnowByNameStillShowTheSessionsWallClock() throws SQLException {
    List<String> legacy =
        List.of(
            "ACT",
            "AET",
            "AGT",
            "ART",
            "AST",
            "BET",
            "BST",
            "CAT",
            "CNT",
            "CST",
            "CTT",
            "EAT",
            "ECT",
            "EST",
            "HST",
            "IET",
            "IST",
            "JST",
            "MIT",
            "MST",
            "NET",
            "NST",
            "PLT",
            "PNT",
            "PRT",
            "PST",
            "SST",
            "VST",
            "Canada/East-Saskatchewan",
            "US/Pacific-New",
            "Factory",
            "ROC");
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      int unknownToJavaTime = 0;
      for (String zone : legacy) {
        try {
          ZoneId.of(zone);
        } catch (java.time.DateTimeException e) {
          unknownToJavaTime++;
        }
        statement.execute("SET TimeZone = '" + zone + "'");
        try (ResultSet rows =
            statement.executeQuery(
                "SELECT ts, CAST(ts AS TIMESTAMP) AS wall, CAST(ts AS VARCHAR) AS text FROM (VALUES"
                    + " (TIMESTAMPTZ '2026-01-15 12:00:00+00'),"
                    + " (TIMESTAMPTZ '2026-07-15 12:00:00+00')) t(ts)")) {
          for (String instant : List.of("2026-01-15T12:00:00Z", "2026-07-15T12:00:00Z")) {
            assertTrue(rows.next());
            LocalDateTime wall = rows.getObject("wall", LocalDateTime.class);
            assertEquals(wall, rows.getObject("ts", LocalDateTime.class), zone);
            assertEquals(Instant.parse(instant), rows.getObject("ts", Instant.class), zone);
            OffsetDateTime offset = rows.getObject("ts", OffsetDateTime.class);
            assertEquals(Instant.parse(instant), offset.toInstant(), zone);
            assertEquals(wall, offset.toLocalDateTime(), zone);
            assertEquals(wall, rows.getObject("ts", ZonedDateTime.class).toLocalDateTime(), zone);
            assertEquals(
                wall,
                OffsetDateTime.parse(rows.getString("ts").replace(' ', 'T')).toLocalDateTime());
            assertTrue(
                rows.getString("text").startsWith(wall.toString().replace('T', ' ')),
                zone + ": " + rows.getString("text") + " and " + wall);
          }
        }
      }
      assertTrue(unknownToJavaTime > 20, "the test still covers names java.time does not know");
    }
  }

  /** Where the JVM's zone and the session's differ, the JVM's must not show anywhere. */
  @Test
  void theJvmZoneIsNotTheSessionsZone() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      LocalDateTime wallClock = LocalDateTime.of(2026, 1, 1, 0, 0);
      try (PreparedStatement cast =
          connection.prepareStatement("SELECT CAST(? AS TIMESTAMPTZ) AS ts")) {
        cast.setObject(1, wallClock);
        try (ResultSet rows = cast.executeQuery()) {
          assertTrue(rows.next());
          Instant instant = rows.getObject(1, Instant.class);
          assertEquals(Instant.parse("2025-12-31T18:30:00Z"), instant, "midnight in Kolkata");
          assertNotEquals(wallClock.atZone(ZoneId.systemDefault()).toInstant(), instant);
          assertEquals(wallClock, rows.getObject(1, LocalDateTime.class));
        }
      }
    }
  }

  /**
   * Inside a list, a struct, a map or a union, a TIMESTAMP WITH TIME ZONE and a TIME WITH TIME ZONE
   * have the text they have as a column: the session's wall clock with its offset, and the time
   * with its seconds and its offset. The JVM's zone ({@link #JVM_ZONE} here) is in none.
   */
  @Test
  void nestedValuesWithAZoneHaveTheTextTheyHaveAsAColumn() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      for (String zone : List.of("Asia/Kolkata", "America/New_York", "UTC")) {
        statement.execute("SET TimeZone = '" + zone + "'");
        try (ResultSet rows =
            statement.executeQuery(
                "SELECT t, o, [t, NULL, t] AS l, {'t': t, 'o': o} AS s, MAP {'k': t} AS m,"
                    + " MAP {t: o} AS keyed, [[o]] AS ll, [{'t': t}] AS ls,"
                    + " union_value(t := t)::UNION(n INTEGER, t TIMESTAMPTZ) AS u,"
                    + " [union_value(t := t)::UNION(n INTEGER, t TIMESTAMPTZ)] AS lu,"
                    + " FROM (SELECT TIMESTAMPTZ '2026-01-15 12:00:00+00' AS t,"
                    + " TIMETZ '12:00:00+05:30' AS o)")) {
          assertTrue(rows.next());
          String t = rows.getString("t");
          String o = rows.getString("o");
          assertEquals("12:00:00+05:30", o);
          assertEquals(
              Map.of(
                      "Asia/Kolkata", "2026-01-15 17:30:00+05:30",
                      "America/New_York", "2026-01-15 07:00:00-05:00",
                      "UTC", "2026-01-15 12:00:00+00:00")
                  .get(zone),
              t);
          assertEquals("[\"" + t + "\", null, \"" + t + "\"]", rows.getString("l"), zone);
          assertEquals("{\"t\": \"" + t + "\", \"o\": \"" + o + "\"}", rows.getString("s"), zone);
          assertEquals("{\"k\": \"" + t + "\"}", rows.getString("m"), zone);
          assertEquals("{\"" + t + "\": \"" + o + "\"}", rows.getString("keyed"), zone);
          assertEquals("[[\"" + o + "\"]]", rows.getString("ll"), zone);
          assertEquals("[{\"t\": \"" + t + "\"}]", rows.getString("ls"), zone);
          assertEquals(t, rows.getString("u"), zone);
          assertEquals("[\"" + t + "\"]", rows.getString("lu"), zone);
          // The values themselves are the instant and the time, as before.
          assertEquals(
              Instant.parse("2026-01-15T12:00:00Z"),
              ((Timestamp) ((List<?>) rows.getObject("l")).get(0)).toInstant());
        }
      }
    }
  }

  /**
   * Where the server gives a parameter no type (an operand of arithmetic, an argument of a
   * function), the driver cannot know which column the value is for, and sends it as its own type:
   * a {@code java.sql.Timestamp} as a TIMESTAMP holding its wall clock in the JVM's zone, as it is
   * stored in a TIMESTAMP column, and an {@code Instant} or {@code OffsetDateTime} as the instant.
   * So each finds the rows of one kind of column and not of the other, when the session's zone is
   * not the JVM's. The README says so, with the two ways out that are the last assertions here.
   */
  @Test
  void whereTheServerGivesAParameterNoTypeAValueGoesAsItsOwnType() throws SQLException {
    try (Connection connection = SharedServer.connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET TimeZone = 'Asia/Kolkata'");
      statement.execute(
          "CREATE OR REPLACE TEMP TABLE moments (naive TIMESTAMP, zoned TIMESTAMPTZ)");
      Timestamp moment = Timestamp.valueOf("2026-01-15 12:00:00");
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO moments VALUES (?, ?)")) {
        insert.setTimestamp(1, moment);
        insert.setTimestamp(2, moment);
        assertEquals(1, insert.executeUpdate());
      }
      Object[] instants = {moment.toInstant(), moment.toInstant().atOffset(ZoneOffset.UTC)};
      // Typed by the column it is compared with: every value means what it means for that type.
      assertEquals("TIMESTAMP", typeOfParameter(connection, "naive = ?"));
      assertEquals("TIMESTAMP WITH TIME ZONE", typeOfParameter(connection, "zoned = ?"));
      assertEquals(1, found(connection, "naive = ?", moment));
      assertEquals(1, found(connection, "zoned = ?", moment));
      for (Object instant : instants) {
        assertEquals(1, found(connection, "zoned = ?", instant));
      }
      for (String column : List.of("naive", "zoned")) {
        boolean zoned = column.equals("zoned");
        for (String untyped :
            List.of(
                "% >= ? - INTERVAL 1 MINUTE AND % <= ? + INTERVAL 1 MINUTE",
                "date_trunc('minute', %) = date_trunc('minute', ?)",
                "% = CASE WHEN true THEN ? ELSE ? END")) {
          String predicate = untyped.replace("%", column);
          assertEquals("UNKNOWN", typeOfParameter(connection, predicate), predicate);
          // The Timestamp's wall clock: the TIMESTAMP column's, and another instant for the
          // TIMESTAMPTZ column, which reads it in the session's zone.
          assertEquals(zoned ? 0 : 1, found(connection, predicate, moment), predicate);
          assertEquals(
              zoned ? 0 : 1, found(connection, predicate, moment.toLocalDateTime()), predicate);
          for (Object instant : instants) {
            assertEquals(zoned ? 1 : 0, found(connection, predicate, instant), predicate);
          }
        }
      }
      // The ways out for a TIMESTAMPTZ in such an expression: an Instant (above), or the type.
      String typed =
          "zoned >= CAST(? AS TIMESTAMPTZ) - INTERVAL 1 MINUTE"
              + " AND zoned <= CAST(? AS TIMESTAMPTZ) + INTERVAL 1 MINUTE";
      assertEquals("TIMESTAMP WITH TIME ZONE", typeOfParameter(connection, typed));
      assertEquals(1, found(connection, typed, moment));
    }
  }

  private static String typeOfParameter(Connection connection, String predicate)
      throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement("SELECT count(*) FROM moments WHERE " + predicate)) {
      return select.getParameterMetaData().getParameterTypeName(1);
    }
  }

  /** The rows a predicate finds with the same value for each of its parameters. */
  private static int found(Connection connection, String predicate, Object value)
      throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement("SELECT count(*) FROM moments WHERE " + predicate)) {
      for (int i = 1; i <= select.getParameterMetaData().getParameterCount(); i++) {
        if (value instanceof Timestamp) {
          select.setTimestamp(i, (Timestamp) value);
        } else {
          select.setObject(i, value);
        }
      }
      try (ResultSet rows = select.executeQuery()) {
        assertTrue(rows.next());
        return rows.getInt(1);
      }
    }
  }
}
