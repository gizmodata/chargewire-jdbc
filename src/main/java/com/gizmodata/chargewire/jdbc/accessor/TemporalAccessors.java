package com.gizmodata.chargewire.jdbc.accessor;

import com.gizmodata.chargewire.jdbc.ChargewireInterval;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import org.apache.arrow.memory.ArrowBuf;
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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Accessors for dates, times, timestamps, intervals and durations. */
final class TemporalAccessors {
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;

  private TemporalAccessors() {}

  /** Formats {@code HH:mm:ss}, then a fraction only as long as needed. */
  static String formatTime(long nanoOfDay) {
    long seconds = nanoOfDay / 1_000_000_000L;
    int nanos = (int) (nanoOfDay % 1_000_000_000L);
    return String.format("%02d:%02d:%02d", seconds / 3600, (seconds / 60) % 60, seconds % 60)
        + fraction(nanos);
  }

  static String fraction(int nanos) {
    if (nanos == 0) {
      return "";
    }
    String digits = String.format("%09d", nanos);
    int end = digits.length();
    while (digits.charAt(end - 1) == '0') {
      end--;
    }
    return "." + digits.substring(0, end);
  }

  static String formatDateTime(LocalDateTime dateTime) {
    return dateTime.toLocalDate() + " " + formatTime(dateTime.toLocalTime().toNanoOfDay());
  }

  /** Dates, as days or milliseconds since the epoch. */
  static final class DateAccessor extends Accessor {
    DateAccessor(ValueVector vector) {
      super(vector);
    }

    @Override
    public LocalDate getLocalDate(int row) {
      if (vector instanceof DateDayVector) {
        return LocalDate.ofEpochDay(((DateDayVector) vector).get(row));
      }
      long millis = ((DateMilliVector) vector).get(row);
      return LocalDate.ofEpochDay(Math.floorDiv(millis, 86_400_000L));
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) {
      return getLocalDate(row).atStartOfDay();
    }

    @Override
    public Object getObject(int row) throws SQLException {
      return getDate(row, null);
    }

    @Override
    public String getString(int row) {
      return getLocalDate(row).toString();
    }
  }

  /** Times of day in seconds, milliseconds, microseconds or nanoseconds. */
  static final class TimeAccessor extends Accessor {
    TimeAccessor(ValueVector vector) {
      super(vector);
    }

    private long nanoOfDay(int row) {
      if (vector instanceof TimeSecVector) {
        return ((TimeSecVector) vector).get(row) * 1_000_000_000L;
      } else if (vector instanceof TimeMilliVector) {
        return ((TimeMilliVector) vector).get(row) * 1_000_000L;
      } else if (vector instanceof TimeMicroVector) {
        return ((TimeMicroVector) vector).get(row) * 1_000L;
      }
      return ((TimeNanoVector) vector).get(row);
    }

    @Override
    public LocalTime getLocalTime(int row) throws SQLException {
      long nanos = nanoOfDay(row);
      if (nanos < 0 || nanos >= NANOS_PER_DAY) {
        throw new SQLDataException(
            "The time " + formatTime(nanos) + " has no java.time.LocalTime form", "22008");
      }
      return LocalTime.ofNanoOfDay(nanos);
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) {
      return LocalDate.EPOCH.atStartOfDay().plusNanos(nanoOfDay(row));
    }

    /** Built from the nanoseconds directly, so that {@code 24:00:00} reads too. */
    @Override
    public Time getTime(int row, Calendar calendar) {
      return new Time(
          LocalDate.EPOCH
              .atStartOfDay(zone(calendar))
              .plusNanos(nanoOfDay(row))
              .toInstant()
              .toEpochMilli());
    }

    @Override
    public Object getObject(int row) {
      return getTime(row, null);
    }

    @Override
    public String getString(int row) {
      return formatTime(nanoOfDay(row));
    }
  }

  /**
   * Timestamps of any unit. Without a time zone they are wall-clock values; with one they are
   * absolute instants, which a {@link Calendar} never shifts.
   */
  static final class TimestampAccessor extends Accessor {
    private final TimeUnit unit;
    private final ZoneId zone;

    TimestampAccessor(TimeStampVector vector) {
      super(vector);
      ArrowType.Timestamp type = (ArrowType.Timestamp) vector.getField().getType();
      this.unit = type.getUnit();
      this.zone = type.getTimezone() == null ? null : zoneOf(type.getTimezone());
    }

    private static ZoneId zoneOf(String timezone) {
      try {
        return ZoneId.of(timezone);
      } catch (DateTimeException e) {
        return ZoneOffset.UTC;
      }
    }

    private Instant instant(int row) {
      long value = ((TimeStampVector) vector).get(row);
      switch (unit) {
        case SECOND:
          return Instant.ofEpochSecond(value);
        case MILLISECOND:
          return Instant.ofEpochMilli(value);
        case MICROSECOND:
          return Instant.ofEpochSecond(
              Math.floorDiv(value, 1_000_000L), Math.floorMod(value, 1_000_000L) * 1_000L);
        default:
          return Instant.ofEpochSecond(
              Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L));
      }
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) {
      return LocalDateTime.ofInstant(instant(row), zone == null ? ZoneOffset.UTC : zone);
    }

    @Override
    public LocalDate getLocalDate(int row) {
      return getLocalDateTime(row).toLocalDate();
    }

    @Override
    public LocalTime getLocalTime(int row) {
      return getLocalDateTime(row).toLocalTime();
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int row) throws SQLException {
      if (zone == null) {
        return super.getOffsetDateTime(row);
      }
      return instant(row).atZone(zone).toOffsetDateTime();
    }

    @Override
    public ZonedDateTime getZonedDateTime(int row) throws SQLException {
      if (zone == null) {
        return super.getZonedDateTime(row);
      }
      return instant(row).atZone(zone);
    }

    @Override
    public Timestamp getTimestamp(int row, Calendar calendar) throws SQLException {
      return zone == null ? super.getTimestamp(row, calendar) : Timestamp.from(instant(row));
    }

    @Override
    public Date getDate(int row, Calendar calendar) throws SQLException {
      return zone == null ? super.getDate(row, calendar) : new Date(instant(row).toEpochMilli());
    }

    @Override
    public Time getTime(int row, Calendar calendar) throws SQLException {
      return zone == null ? super.getTime(row, calendar) : new Time(instant(row).toEpochMilli());
    }

    @Override
    public Object getObject(int row) throws SQLException {
      return getTimestamp(row, null);
    }

    @Override
    public String getString(int row) {
      if (zone == null) {
        return formatDateTime(getLocalDateTime(row));
      }
      ZonedDateTime value = instant(row).atZone(zone);
      return formatDateTime(value.toLocalDateTime())
          + value.getOffset().getId().replace("Z", "+00:00");
    }
  }

  /** Intervals: year-month as {@link Period}, day-time as {@link Duration}, and month-day-nano. */
  static final class IntervalAccessor extends Accessor {
    IntervalAccessor(ValueVector vector) {
      super(vector);
    }

    @Override
    public Object getObject(int row) {
      if (vector instanceof IntervalYearVector) {
        return Period.ofMonths(((IntervalYearVector) vector).get(row)).normalized();
      }
      if (vector instanceof IntervalDayVector) {
        return ((IntervalDayVector) vector).getObject(row);
      }
      ArrowBuf data = ((IntervalMonthDayNanoVector) vector).getDataBuffer();
      return new ChargewireInterval(
          IntervalMonthDayNanoVector.getMonths(data, row),
          IntervalMonthDayNanoVector.getDays(data, row),
          IntervalMonthDayNanoVector.getNanoseconds(data, row));
    }

    @Override
    public <T> T getObject(int row, Class<T> type) throws SQLException {
      Object value = getObject(row);
      if (value instanceof ChargewireInterval) {
        ChargewireInterval interval = (ChargewireInterval) value;
        if (type == Period.class && interval.nanos() == 0) {
          return type.cast(interval.period());
        }
        if (type == Duration.class && interval.months() == 0) {
          return type.cast(Duration.ofDays(interval.days()).plusNanos(interval.nanos()));
        }
      }
      return super.getObject(row, type);
    }
  }

  /** Durations of any unit, as {@link Duration}. */
  static final class DurationAccessor extends Accessor {
    DurationAccessor(DurationVector vector) {
      super(vector);
    }

    @Override
    public Duration getObject(int row) {
      return ((DurationVector) vector).getObject(row);
    }
  }
}
