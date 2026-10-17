package com.gizmodata.warpsql.jdbc.accessor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.vector.ValueVector;

/**
 * Reads one column of a batch as JDBC values.
 *
 * <p>Getters are only called for rows that are not null ({@link #isNull(int)} first). Each
 * subclass implements the conversions its Arrow type supports; the rest raise {@code 07006}
 * ("restricted data type attribute violation"). Numbers that do not fit the requested type raise
 * {@code 22003} rather than wrap around, and text that does not parse raises {@code 22018}.
 *
 * <p>Internal API.
 */
public abstract class Accessor {
  /** The vector this accessor reads. */
  protected final ValueVector vector;

  /**
   * Creates an accessor.
   *
   * @param vector the column's vector
   */
  protected Accessor(ValueVector vector) {
    this.vector = vector;
  }

  /**
   * Returns whether a row's value is null.
   *
   * @param row the row in the batch
   * @return {@code true} for null
   */
  public boolean isNull(int row) {
    return vector.isNull(row);
  }

  /**
   * Returns the value as its natural Java object.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if the value cannot be read
   */
  public abstract Object getObject(int row) throws SQLException;

  /**
   * Returns the value as text.
   *
   * @param row a non-null row
   * @return the text
   * @throws SQLException if the value cannot be read
   */
  public String getString(int row) throws SQLException {
    return String.valueOf(getObject(row));
  }

  /**
   * Returns the value as a boolean.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it has no boolean form
   */
  public boolean getBoolean(int row) throws SQLException {
    throw cannotConvert("boolean");
  }

  /**
   * Returns the value as a {@code long}; integer getters narrow it with a range check.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number or does not fit
   */
  public long getLong(int row) throws SQLException {
    throw cannotConvert("long");
  }

  /**
   * Returns the value as a byte.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number or does not fit
   */
  public byte getByte(int row) throws SQLException {
    long value = getLong(row);
    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
      throw outOfRange(value, "byte");
    }
    return (byte) value;
  }

  /**
   * Returns the value as a short.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number or does not fit
   */
  public short getShort(int row) throws SQLException {
    long value = getLong(row);
    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
      throw outOfRange(value, "short");
    }
    return (short) value;
  }

  /**
   * Returns the value as an int.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number or does not fit
   */
  public int getInt(int row) throws SQLException {
    long value = getLong(row);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw outOfRange(value, "int");
    }
    return (int) value;
  }

  /**
   * Returns the value as a double.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number
   */
  public double getDouble(int row) throws SQLException {
    throw cannotConvert("double");
  }

  /**
   * Returns the value as a float.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number or overflows a float
   */
  public float getFloat(int row) throws SQLException {
    double value = getDouble(row);
    float narrowed = (float) value;
    if (Float.isInfinite(narrowed) && !Double.isInfinite(value)) {
      throw outOfRange(value, "float");
    }
    return narrowed;
  }

  /**
   * Returns the value as a decimal.
   *
   * @param row a non-null row
   * @return the value
   * @throws SQLException if it is not a number
   */
  public BigDecimal getBigDecimal(int row) throws SQLException {
    throw cannotConvert("BigDecimal");
  }

  /**
   * Returns the value's bytes.
   *
   * @param row a non-null row
   * @return the bytes
   * @throws SQLException if it has no byte form
   */
  public byte[] getBytes(int row) throws SQLException {
    throw cannotConvert("byte[]");
  }

  /**
   * Returns the value as a date.
   *
   * @param row a non-null row
   * @return the date
   * @throws SQLException if it has no date form
   */
  public LocalDate getLocalDate(int row) throws SQLException {
    throw cannotConvert("date");
  }

  /**
   * Returns the value as a time of day.
   *
   * @param row a non-null row
   * @return the time
   * @throws SQLException if it has no time form
   */
  public LocalTime getLocalTime(int row) throws SQLException {
    throw cannotConvert("time");
  }

  /**
   * Returns the value as a wall-clock date and time. For a timestamp with a time zone, this is the
   * wall clock in that zone.
   *
   * @param row a non-null row
   * @return the date and time
   * @throws SQLException if it has no date-time form
   */
  public LocalDateTime getLocalDateTime(int row) throws SQLException {
    throw cannotConvert("timestamp");
  }

  /**
   * Returns the value as an absolute point in time with its offset.
   *
   * @param row a non-null row
   * @return the offset date-time
   * @throws SQLException unless the value is a timestamp with a time zone
   */
  public OffsetDateTime getOffsetDateTime(int row) throws SQLException {
    throw new SQLException(
        "Only a timestamp with a time zone converts to an absolute time; this column has none",
        "07006");
  }

  /**
   * Returns the value as a {@link Date}. A date without a zone is taken at midnight in the
   * calendar's zone, or the JVM's when there is no calendar.
   *
   * @param row a non-null row
   * @param calendar the zone to interpret wall-clock values in, or {@code null}
   * @return the date
   * @throws SQLException if it has no date form
   */
  public Date getDate(int row, Calendar calendar) throws SQLException {
    LocalDate date = getLocalDate(row);
    if (calendar == null) {
      return Date.valueOf(date);
    }
    return new Date(date.atStartOfDay(zone(calendar)).toInstant().toEpochMilli());
  }

  /**
   * Returns the value as a {@link Time}, to the millisecond.
   *
   * @param row a non-null row
   * @param calendar the zone to interpret wall-clock values in, or {@code null}
   * @return the time
   * @throws SQLException if it has no time form
   */
  public Time getTime(int row, Calendar calendar) throws SQLException {
    LocalTime time = getLocalTime(row);
    return new Time(
        LocalDate.EPOCH.atTime(time).atZone(zone(calendar)).toInstant().toEpochMilli());
  }

  /**
   * Returns the value as a {@link Timestamp}, keeping nanoseconds.
   *
   * @param row a non-null row
   * @param calendar the zone to interpret wall-clock values in, or {@code null}
   * @return the timestamp
   * @throws SQLException if it has no date-time form
   */
  public Timestamp getTimestamp(int row, Calendar calendar) throws SQLException {
    LocalDateTime dateTime = getLocalDateTime(row);
    if (calendar == null) {
      return Timestamp.valueOf(dateTime);
    }
    return Timestamp.from(dateTime.atZone(zone(calendar)).toInstant());
  }

  /**
   * Returns the value as a JDBC array.
   *
   * @param row a non-null row
   * @return the array
   * @throws SQLException unless the column is a list
   */
  public Array getArray(int row) throws SQLException {
    throw cannotConvert("Array");
  }

  /**
   * Returns the value as a JDBC struct.
   *
   * @param row a non-null row
   * @return the struct
   * @throws SQLException unless the column is a struct
   */
  public Struct getStruct(int row) throws SQLException {
    throw cannotConvert("Struct");
  }

  /**
   * Returns the value as a character stream.
   *
   * @param row a non-null row
   * @return a reader over {@link #getString(int)}
   * @throws SQLException if the value cannot be read
   */
  public Reader getCharacterStream(int row) throws SQLException {
    return new StringReader(getString(row));
  }

  /**
   * Returns the value as a byte stream.
   *
   * @param row a non-null row
   * @return a stream over {@link #getBytes(int)}
   * @throws SQLException if it has no byte form
   */
  public InputStream getBinaryStream(int row) throws SQLException {
    return new ByteArrayInputStream(getBytes(row));
  }

  /**
   * Returns the value as ASCII bytes.
   *
   * @param row a non-null row
   * @return a stream over the text in US-ASCII
   * @throws SQLException if the value cannot be read
   */
  public InputStream getAsciiStream(int row) throws SQLException {
    return new ByteArrayInputStream(getString(row).getBytes(StandardCharsets.US_ASCII));
  }

  /**
   * Returns the value converted to a class (JDBC 4.1 {@code getObject(int, Class)}).
   *
   * @param row a non-null row
   * @param type the class wanted
   * @param <T> the class
   * @return the value
   * @throws SQLException if the value has no form of that class
   */
  public <T> T getObject(int row, Class<T> type) throws SQLException {
    Object value;
    if (type == Object.class) {
      value = getObject(row);
    } else if (type == String.class) {
      value = getString(row);
    } else if (type == Boolean.class) {
      value = getBoolean(row);
    } else if (type == Byte.class) {
      value = getByte(row);
    } else if (type == Short.class) {
      value = getShort(row);
    } else if (type == Integer.class) {
      value = getInt(row);
    } else if (type == Long.class) {
      value = getLong(row);
    } else if (type == Float.class) {
      value = getFloat(row);
    } else if (type == Double.class) {
      value = getDouble(row);
    } else if (type == BigDecimal.class) {
      value = getBigDecimal(row);
    } else if (type == BigInteger.class) {
      value = toBigInteger(getBigDecimal(row));
    } else if (type == byte[].class) {
      value = getBytes(row);
    } else if (type == LocalDate.class) {
      value = getLocalDate(row);
    } else if (type == LocalTime.class) {
      value = getLocalTime(row);
    } else if (type == LocalDateTime.class) {
      value = getLocalDateTime(row);
    } else if (type == OffsetDateTime.class) {
      value = getOffsetDateTime(row);
    } else if (type == Instant.class) {
      value = getOffsetDateTime(row).toInstant();
    } else if (type == ZonedDateTime.class) {
      value = getZonedDateTime(row);
    } else if (type == OffsetTime.class) {
      value = getOffsetDateTime(row).toOffsetTime();
    } else if (type == Date.class) {
      value = getDate(row, null);
    } else if (type == Time.class) {
      value = getTime(row, null);
    } else if (type == Timestamp.class) {
      value = getTimestamp(row, null);
    } else if (type == Array.class) {
      value = getArray(row);
    } else if (type == Struct.class) {
      value = getStruct(row);
    } else {
      value = getObject(row);
      if (type == UUID.class && value instanceof String) {
        value = parseUuid((String) value);
      } else if ((type == Duration.class || type == Period.class) && !type.isInstance(value)) {
        throw cannotConvert(type.getSimpleName());
      } else if ((type == List.class || type == Map.class) && !type.isInstance(value)) {
        throw cannotConvert(type.getSimpleName());
      }
      if (!type.isInstance(value)) {
        throw cannotConvert(type.getName());
      }
    }
    return type.cast(value);
  }

  /**
   * Returns the value as a zoned date-time, for a timestamp with a time zone.
   *
   * @param row a non-null row
   * @return the value in its own zone
   * @throws SQLException unless the value is a timestamp with a time zone
   */
  public ZonedDateTime getZonedDateTime(int row) throws SQLException {
    return getOffsetDateTime(row).toZonedDateTime();
  }

  /**
   * Returns a description of the column's type for messages.
   *
   * @return the vector's Arrow type
   */
  protected String typeDescription() {
    return vector.getField().getType().toString();
  }

  /**
   * Builds the error for a conversion the column's type does not support.
   *
   * @param target what was asked for
   * @return a {@code 07006} exception
   */
  protected SQLException cannotConvert(String target) {
    return new SQLException(
        "Cannot read a " + typeDescription() + " column as " + target, "07006");
  }

  static SQLDataException outOfRange(Object value, String target) {
    return new SQLDataException(
        "The value " + value + " does not fit in a " + target, "22003");
  }

  static SQLDataException unparseable(String text, String target, Throwable cause) {
    String shown = text.length() > 100 ? text.substring(0, 100) + "..." : text;
    return new SQLDataException("Cannot read '" + shown + "' as " + target, "22018", cause);
  }

  static long toLong(BigDecimal value) throws SQLException {
    try {
      return value.toBigInteger().longValueExact();
    } catch (ArithmeticException e) {
      throw outOfRange(value, "long");
    }
  }

  static BigInteger toBigInteger(BigDecimal value) {
    return value.toBigInteger();
  }

  static UUID parseUuid(String text) throws SQLException {
    try {
      return UUID.fromString(text);
    } catch (IllegalArgumentException e) {
      throw unparseable(text, "UUID", e);
    }
  }

  static ZoneId zone(Calendar calendar) {
    return calendar == null ? ZoneId.systemDefault() : calendar.getTimeZone().toZoneId();
  }
}
