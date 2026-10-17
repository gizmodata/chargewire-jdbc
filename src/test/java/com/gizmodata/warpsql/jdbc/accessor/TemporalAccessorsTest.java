package com.gizmodata.warpsql.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.gizmodata.warpsql.jdbc.WarpInterval;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Dates, times, timestamps, intervals and durations (ported from the {@code Date}, {@code Time},
 * {@code TimeStamp}, {@code Interval} and {@code Duration} accessor tests and {@code
 * TimestampResultSetTest}).
 */
class TemporalAccessorsTest extends AccessorFixture {
  private static final Calendar SAO_PAULO =
      Calendar.getInstance(TimeZone.getTimeZone("America/Sao_Paulo"));
  private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  @Test
  void datesInDaysAndMilliseconds() throws SQLException {
    DateDayVector days = vector(new ArrowType.Date(DateUnit.DAY), 2, v -> v.setSafe(0, -1));
    DateMilliVector millis =
        vector(
            new ArrowType.Date(DateUnit.MILLISECOND),
            1,
            v -> v.setSafe(0, LocalDate.of(2026, 10, 17).toEpochDay() * 86_400_000L));
    Accessor accessor = accessor(days);
    assertEquals(LocalDate.of(1969, 12, 31), accessor.getLocalDate(0));
    assertEquals(Date.valueOf("1969-12-31"), accessor.getObject(0));
    assertEquals("1969-12-31", accessor.getString(0));
    assertEquals(Timestamp.valueOf("1969-12-31 00:00:00"), accessor.getTimestamp(0, null));
    assertEquals(LocalDate.of(2026, 10, 17), accessor(millis).getLocalDate(0));
    assertEquals(
        "07006", assertThrows(SQLException.class, () -> accessor.getTime(0, null)).getSQLState());
  }

  @Test
  void datesWithACalendarAreMidnightInItsZone() throws SQLException {
    DateDayVector days =
        vector(
            new ArrowType.Date(DateUnit.DAY),
            1,
            v -> v.setSafe(0, (int) LocalDate.of(2026, 10, 17).toEpochDay()));
    Date date = accessor(days).getDate(0, SAO_PAULO);
    assertEquals(
        LocalDate.of(2026, 10, 17)
            .atStartOfDay(ZoneId.of("America/Sao_Paulo"))
            .toInstant()
            .toEpochMilli(),
        date.getTime());
  }

  @ParameterizedTest
  @EnumSource(TimeUnit.class)
  void timesInEveryUnit(TimeUnit unit) throws SQLException {
    LocalTime time = LocalTime.of(13, 14, 15, 123_456_789);
    long nanos = time.toNanoOfDay();
    Accessor accessor;
    String text;
    switch (unit) {
      case SECOND:
        accessor =
            accessor(
                vector(
                    new ArrowType.Time(unit, 32),
                    1,
                    v -> ((TimeSecVector) v).setSafe(0, (int) (nanos / 1_000_000_000L))));
        text = "13:14:15";
        break;
      case MILLISECOND:
        accessor =
            accessor(
                vector(
                    new ArrowType.Time(unit, 32),
                    1,
                    v -> ((TimeMilliVector) v).setSafe(0, (int) (nanos / 1_000_000L))));
        text = "13:14:15.123";
        break;
      case MICROSECOND:
        accessor =
            accessor(
                vector(
                    new ArrowType.Time(unit, 64),
                    1,
                    v -> ((TimeMicroVector) v).setSafe(0, nanos / 1_000L)));
        text = "13:14:15.123456";
        break;
      default:
        accessor =
            accessor(
                vector(
                    new ArrowType.Time(unit, 64), 1, v -> ((TimeNanoVector) v).setSafe(0, nanos)));
        text = "13:14:15.123456789";
    }
    assertEquals(text, accessor.getString(0));
    assertEquals(LocalTime.parse(text), accessor.getLocalTime(0));
    assertEquals(
        Time.valueOf("13:14:15").getTime() + (unit == TimeUnit.SECOND ? 0 : 123),
        accessor.getTime(0, null).getTime());
    assertEquals(LocalDate.EPOCH.atTime(LocalTime.parse(text)), accessor.getLocalDateTime(0));
  }

  @Test
  void twentyFourOClockReadsAsTextAndTime() throws SQLException {
    TimeMicroVector times =
        vector(new ArrowType.Time(TimeUnit.MICROSECOND, 64), 1, v -> v.setSafe(0, 86_400_000_000L));
    Accessor accessor = accessor(times);
    assertEquals("24:00:00", accessor.getString(0));
    assertEquals(86_400_000L, accessor.getTime(0, UTC).getTime());
    assertEquals(
        "22008",
        assertThrows(SQLDataException.class, () -> accessor.getLocalTime(0)).getSQLState());
  }

  @Test
  void timesWithACalendar() throws SQLException {
    TimeSecVector times =
        vector(new ArrowType.Time(TimeUnit.SECOND, 32), 1, v -> v.setSafe(0, 3600));
    assertEquals(3_600_000L, accessor(times).getTime(0, UTC).getTime());
    assertEquals(
        3_600_000L + 3 * 3_600_000L,
        accessor(times).getTime(0, SAO_PAULO).getTime(),
        "01:00 in UTC-3");
  }

  private TimeStampVector timestamps(TimeUnit unit, String zone, long... values) {
    return vector(
        new ArrowType.Timestamp(unit, zone),
        values.length,
        v -> {
          TimeStampVector timestamps = v;
          for (int i = 0; i < values.length; i++) {
            timestamps.setSafe(i, values[i]);
          }
        });
  }

  private static long epoch(Instant instant, TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return instant.getEpochSecond();
      case MILLISECOND:
        return instant.toEpochMilli();
      case MICROSECOND:
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000;
      default:
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }
  }

  private static int truncated(int nanos, TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return 0;
      case MILLISECOND:
        return nanos / 1_000_000 * 1_000_000;
      case MICROSECOND:
        return nanos / 1_000 * 1_000;
      default:
        return nanos;
    }
  }

  @ParameterizedTest
  @EnumSource(TimeUnit.class)
  void wallClockTimestamps(TimeUnit unit) throws SQLException {
    Instant instant = Instant.parse("2026-03-08T02:30:15.123456789Z");
    Accessor accessor = accessor(timestamps(unit, null, epoch(instant, unit)));
    LocalDateTime expected =
        LocalDateTime.ofEpochSecond(
            instant.getEpochSecond(), truncated(instant.getNano(), unit), java.time.ZoneOffset.UTC);
    assertEquals(expected, accessor.getLocalDateTime(0));
    assertEquals(
        Timestamp.valueOf(expected),
        accessor.getTimestamp(0, null),
        "the wall clock in the JVM's zone");
    assertEquals(Timestamp.valueOf(expected), accessor.getObject(0));
    assertEquals(expected.toLocalDate(), accessor.getLocalDate(0));
    assertEquals(
        expected.atZone(ZoneId.of("America/Sao_Paulo")).toInstant(),
        accessor.getTimestamp(0, SAO_PAULO).toInstant(),
        "the wall clock in the calendar's zone");
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getOffsetDateTime(0)).getSQLState());
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getObject(0, Instant.class)).getSQLState());
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getObject(0, ZonedDateTime.class))
            .getSQLState());
  }

  @Test
  void timestampsBeforeTheEpoch() throws SQLException {
    Accessor micros = accessor(timestamps(TimeUnit.MICROSECOND, null, -1L));
    assertEquals(
        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_000), micros.getLocalDateTime(0));
    assertEquals("1969-12-31 23:59:59.999999", micros.getString(0));
    Accessor nanos = accessor(timestamps(TimeUnit.NANOSECOND, null, -1_500_000_000L));
    assertEquals(
        LocalDateTime.of(1969, 12, 31, 23, 59, 58, 500_000_000), nanos.getLocalDateTime(0));
  }

  @Test
  void timestampTextKeepsOnlyTheFractionItNeeds() throws SQLException {
    Accessor accessor =
        accessor(timestamps(TimeUnit.MICROSECOND, null, 0L, 1_500_000L, 1_000_010L));
    assertEquals("1970-01-01 00:00:00", accessor.getString(0));
    assertEquals("1970-01-01 00:00:01.5", accessor.getString(1));
    assertEquals("1970-01-01 00:00:01.00001", accessor.getString(2));
  }

  @ParameterizedTest
  @EnumSource(TimeUnit.class)
  void zonedTimestampsAreInstants(TimeUnit unit) throws SQLException {
    Instant instant = Instant.parse("2026-07-01T12:00:00Z");
    Accessor accessor = accessor(timestamps(unit, "America/Vancouver", epoch(instant, unit)));
    assertEquals(instant, accessor.getTimestamp(0, null).toInstant());
    assertEquals(
        instant,
        accessor.getTimestamp(0, SAO_PAULO).toInstant(),
        "a calendar never shifts an instant");
    assertEquals(instant, accessor.getObject(0, Instant.class));
    assertEquals(OffsetDateTime.parse("2026-07-01T05:00:00-07:00"), accessor.getOffsetDateTime(0));
    assertEquals(
        ZonedDateTime.ofInstant(instant, ZoneId.of("America/Vancouver")),
        accessor.getObject(0, ZonedDateTime.class));
    assertEquals(
        LocalDateTime.of(2026, 7, 1, 5, 0),
        accessor.getLocalDateTime(0),
        "the wall clock in the column's zone");
    assertEquals("2026-07-01 05:00:00-07:00", accessor.getString(0));
    assertEquals(instant.toEpochMilli(), accessor.getDate(0, SAO_PAULO).getTime());
    assertEquals(instant.toEpochMilli(), accessor.getTime(0, null).getTime());
  }

  @Test
  void zonedTimestampTextInUtcAndOffsets() throws SQLException {
    Accessor utc = accessor(timestamps(TimeUnit.SECOND, "UTC", 0L));
    assertEquals("1970-01-01 00:00:00+00:00", utc.getString(0));
    Accessor offset = accessor(timestamps(TimeUnit.SECOND, "+05:30", 0L));
    assertEquals("1970-01-01 05:30:00+05:30", offset.getString(0));
  }

  @Test
  void intervals() throws SQLException {
    IntervalYearVector years =
        vector(new ArrowType.Interval(IntervalUnit.YEAR_MONTH), 1, v -> v.setSafe(0, 14));
    IntervalDayVector days =
        vector(new ArrowType.Interval(IntervalUnit.DAY_TIME), 1, v -> v.setSafe(0, 2, 1500));
    IntervalMonthDayNanoVector all =
        vector(
            new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO),
            3,
            v -> {
              v.setSafe(0, 1, 2, 3_000_000_004L);
              v.setSafe(1, 0, 3, 0);
              v.setSafe(2, 0, 0, 0);
            });
    assertEquals(Period.of(1, 2, 0), accessor(years).getObject(0));
    assertEquals("P1Y2M", accessor(years).getString(0));
    assertEquals(Duration.ofDays(2).plusMillis(1500), accessor(days).getObject(0));
    assertEquals("PT48H1.5S", accessor(days).getString(0));
    Accessor accessor = accessor(all);
    assertEquals(new WarpInterval(1, 2, 3_000_000_004L), accessor.getObject(0));
    assertEquals("P1M2DT3.000000004S", accessor.getString(0));
    assertEquals(Period.ofDays(3), accessor.getObject(1, Period.class));
    assertEquals(Duration.ofDays(3), accessor.getObject(1, Duration.class));
    assertEquals("PT0S", accessor.getString(2));
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getObject(0, Duration.class)).getSQLState(),
        "months have no fixed length");
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getObject(0, Period.class)).getSQLState());
  }

  @Test
  void durations() throws SQLException {
    DurationVector durations =
        vector(new ArrowType.Duration(TimeUnit.MICROSECOND), 2, v -> v.setSafe(0, 1_500_000L));
    Accessor accessor = accessor(durations);
    assertEquals(Duration.ofMillis(1500), accessor.getObject(0));
    assertEquals("PT1.5S", accessor.getString(0));
    assertEquals(Duration.ofMillis(1500), accessor.getObject(0, Duration.class));
  }
}
