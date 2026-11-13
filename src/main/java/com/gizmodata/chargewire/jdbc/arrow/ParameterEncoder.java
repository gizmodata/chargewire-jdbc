package com.gizmodata.chargewire.jdbc.arrow;

import com.gizmodata.chargewire.jdbc.ChargewireInterval;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
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
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Encodes JDBC parameter values as an Arrow IPC stream, one row per execution (spec §7.2).
 *
 * <p>A parameter whose type the server declared is converted to that type; a conversion that would
 * lose information other than decimal rounding fails with {@code 22003} or {@code 22018}. Where the
 * server declared no type (Arrow's null type), the type follows from the Java values, widened
 * across the rows of a batch, or from the {@link Types} hint of a {@code setNull}.
 *
 * <p>A {@code TIMESTAMP WITH TIME ZONE} parameter takes a value that is a point in time ({@link
 * Instant}, {@link OffsetDateTime}, {@link ZonedDateTime}, {@link Timestamp}, {@link
 * java.util.Date}, {@link Calendar}, text with an offset) as that instant. A value without a zone
 * ({@link LocalDateTime}, {@link LocalDate}, {@link java.sql.Date}, text without an offset) is not
 * converted here: it travels in its own type (a timestamp without a zone, a date, text), and the
 * server reads it in the session's time zone, exactly as it reads the same literal in SQL text. The
 * JVM's default zone plays no part in it.
 *
 * <p>Internal API.
 */
public final class ParameterEncoder {
  /** No {@link Types} hint. */
  public static final int NO_TYPE = Integer.MIN_VALUE;

  private static final ArrowType NULL = ArrowType.Null.INSTANCE;
  private static final ArrowType UTF8 = ArrowType.Utf8.INSTANCE;

  private ParameterEncoder() {}

  /** One bound parameter: the value and how it was set. */
  public static final class Value {
    final Object value;
    final int sqlType;
    final Calendar calendar;

    /** The value as the application gave it, before {@link #coerce} converted it. */
    final Object source;

    /**
     * Creates a bound value.
     *
     * @param value the value, or {@code null}
     * @param sqlType the {@link Types} hint, or {@link #NO_TYPE}
     * @param calendar the zone for wall-clock values, or {@code null}
     */
    public Value(Object value, int sqlType, Calendar calendar) {
      this(value, sqlType, calendar, value);
    }

    /**
     * Creates a bound value that {@link #coerce} converted to its {@link Types} hint.
     *
     * @param value the converted value, or {@code null}
     * @param sqlType the {@link Types} hint, or {@link #NO_TYPE}
     * @param calendar the zone for wall-clock values, or {@code null}
     * @param source the value before the conversion: a point in time converted to a {@code
     *     TIMESTAMP}'s wall clock is still sent as that instant to a parameter the server types
     *     {@code TIMESTAMP WITH TIME ZONE}
     */
    public Value(Object value, int sqlType, Calendar calendar, Object source) {
      this.value = value;
      this.sqlType = sqlType;
      this.calendar = calendar;
      this.source = source;
    }

    /**
     * Returns the value.
     *
     * @return the value, or {@code null}
     */
    public Object value() {
      return value;
    }
  }

  /**
   * Encodes rows of parameters.
   *
   * @param allocator where vectors are allocated while encoding
   * @param declared the server's parameter schema, or {@code null} when it sent none
   * @param rows one array per execution, each as long as the parameter count
   * @return the Arrow IPC stream
   * @throws SQLException if a value cannot be converted
   */
  public static byte[] encode(BufferAllocator allocator, Schema declared, List<Value[]> rows)
      throws SQLException {
    int columns = rows.get(0).length;
    List<Field> fields = new ArrayList<>(columns);
    for (int c = 0; c < columns; c++) {
      Field declaredField =
          declared != null && c < declared.getFields().size() ? declared.getFields().get(c) : null;
      String name = declaredField != null ? declaredField.getName() : Integer.toString(c + 1);
      ArrowType type =
          declaredField != null && !(declaredField.getType() instanceof ArrowType.Null)
              ? declaredField.getType()
              : null;
      fields.add(
          type != null ? sentField(declaredField, column(rows, c)) : inferField(name, rows, c));
    }
    try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator)) {
      root.allocateNew();
      for (int c = 0; c < columns; c++) {
        FieldVector vector = root.getVector(c);
        Field declaredField =
            declared != null && c < declared.getFields().size()
                ? declared.getFields().get(c)
                : null;
        for (int r = 0; r < rows.size(); r++) {
          Value value = rows.get(r)[c];
          try {
            set(vector, r, sent(declaredField, value), value.calendar, declaredField);
          } catch (SQLException e) {
            throw new SQLDataException(
                "Parameter "
                    + (c + 1)
                    + (rows.size() > 1 ? " of batch row " + (r + 1) : "")
                    + ": "
                    + e.getMessage(),
                e.getSQLState(),
                e);
          }
        }
        vector.setValueCount(rows.size());
      }
      root.setRowCount(rows.size());
      return ArrowIpc.writeStream(root);
    } catch (IOException e) {
      throw new SQLException("Cannot encode parameters: " + e.getMessage(), "HY000", e);
    }
  }

  /**
   * Converts a value to the Java class of a JDBC type, for {@code setObject(index, value,
   * targetSqlType[, scale])}.
   *
   * @param value the value, not {@code null}
   * @param sqlType the {@link Types} code to convert to
   * @param scale the scale for {@code DECIMAL} and {@code NUMERIC}, or a negative number
   * @return the converted value
   * @throws SQLException if the value has no form of that type
   */
  public static Object coerce(Object value, int sqlType, int scale) throws SQLException {
    switch (sqlType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return toBoolean(value);
      case Types.TINYINT:
        return (byte) checkedLong(value, Byte.MIN_VALUE, Byte.MAX_VALUE);
      case Types.SMALLINT:
        return (short) checkedLong(value, Short.MIN_VALUE, Short.MAX_VALUE);
      case Types.INTEGER:
        return (int) checkedLong(value, Integer.MIN_VALUE, Integer.MAX_VALUE);
      case Types.BIGINT:
        return checkedLong(value, Long.MIN_VALUE, Long.MAX_VALUE);
      case Types.REAL:
        return (float) toDouble(value);
      case Types.FLOAT:
      case Types.DOUBLE:
        return toDouble(value);
      case Types.DECIMAL:
      case Types.NUMERIC:
        BigDecimal decimal = toBigDecimal(value);
        return scale >= 0 ? decimal.setScale(scale, RoundingMode.HALF_UP) : decimal;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
      case Types.NCLOB:
        return toText(value);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return toBytes(value);
      case Types.DATE:
        return toDate(value, null);
      case Types.TIME:
        return toTime(value, null);
      case Types.TIMESTAMP:
        return toLocalDateTime(value, null, "TIMESTAMP");
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return toZoned(value);
      default:
        return value;
    }
  }

  /**
   * The form of a value for a {@code TIMESTAMP WITH TIME ZONE}: a point in time as an {@link
   * Instant}; a value without a zone stays without one ({@link LocalDateTime}, {@link LocalDate},
   * or its text), for the server to read in the session's time zone.
   */
  private static Object toZoned(Object value) throws SQLException {
    switch (zonedKind(value, null)) {
      case DATE:
        return toDate(value, null);
      case DATE_TIME:
        return value;
      case TEXT:
        String text = value.toString().trim();
        try {
          return LocalDateTime.parse(text.replace(' ', 'T'));
        } catch (DateTimeParseException e) {
          // Not ISO: the server reads the text, as it reads a literal.
          return text;
        }
      default:
        return toInstant(value, null, true);
    }
  }

  /**
   * Returns the JSON form of a parameter, for servers without prepared statements (spec §6.1:
   * numbers, strings, booleans and {@code null}).
   *
   * @param value the value, or {@code null}
   * @return a {@link Boolean}, {@link Number} or {@link String}, or {@code null}
   * @throws SQLException if the value has no JSON form
   */
  public static Object jsonValue(Object value) throws SQLException {
    if (value == null
        || value instanceof Boolean
        || value instanceof String
        || value instanceof Number) {
      return value;
    }
    if (value instanceof Character || value instanceof UUID) {
      return value.toString();
    }
    if (value instanceof byte[] || value instanceof Array || elements(value) != null) {
      throw new SQLException(
          "Binary and array parameters need a server with prepared statements", "0A000");
    }
    if (value instanceof java.sql.Date || value instanceof LocalDate) {
      return toDate(value, null).toString();
    }
    if (value instanceof Time || value instanceof LocalTime) {
      return toTime(value, null).toString();
    }
    if (value instanceof OffsetDateTime
        || value instanceof ZonedDateTime
        || value instanceof Instant) {
      return toInstant(value, null, true).toString();
    }
    if (value instanceof Duration
        || value instanceof Period
        || value instanceof ChargewireInterval) {
      return value.toString();
    }
    return toLocalDateTime(value, null, "TIMESTAMP").toString().replace('T', ' ');
  }

  // ---- Type inference ----

  private static Field inferField(String name, List<Value[]> rows, int column) throws SQLException {
    Field inferred = null;
    int hint = NO_TYPE;
    for (Value[] row : rows) {
      Value value = row[column];
      if (value.value == null) {
        if (hint == NO_TYPE) {
          hint = value.sqlType;
        }
        continue;
      }
      Field field = fieldOf(name, value.value);
      inferred = inferred == null ? field : unify(name, inferred, field);
    }
    if (inferred != null) {
      return inferred;
    }
    ArrowType hinted = hint == NO_TYPE ? null : SqlTypes.arrowType(hint);
    return Field.nullable(name, hinted == null ? NULL : hinted);
  }

  private static Field fieldOf(String name, Object value) throws SQLException {
    if (value instanceof Array) {
      value = ((Array) value).getArray();
    }
    List<?> elements = elements(value);
    if (elements != null) {
      Field child = null;
      for (Object element : elements) {
        if (element != null) {
          Field field = fieldOf("item", element);
          child = child == null ? field : unify("item", child, field);
        }
      }
      if (child == null) {
        child = Field.nullable("item", NULL);
      }
      return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(child));
    }
    return Field.nullable(name, typeOf(value));
  }

  /** The natural Arrow type of a Java value. */
  static ArrowType typeOf(Object value) throws SQLException {
    if (value instanceof Boolean) {
      return ArrowType.Bool.INSTANCE;
    } else if (value instanceof Byte) {
      return new ArrowType.Int(8, true);
    } else if (value instanceof Short) {
      return new ArrowType.Int(16, true);
    } else if (value instanceof Integer) {
      return new ArrowType.Int(32, true);
    } else if (value instanceof Long) {
      return new ArrowType.Int(64, true);
    } else if (value instanceof Float) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (value instanceof Double) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (value instanceof BigInteger) {
      return decimalType(new BigDecimal((BigInteger) value));
    } else if (value instanceof BigDecimal) {
      return decimalType((BigDecimal) value);
    } else if (value instanceof String || value instanceof Character || value instanceof UUID) {
      return UTF8;
    } else if (value instanceof byte[]) {
      return ArrowType.Binary.INSTANCE;
    } else if (value instanceof java.sql.Date || value instanceof LocalDate) {
      return new ArrowType.Date(DateUnit.DAY);
    } else if (value instanceof Time) {
      return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
    } else if (value instanceof LocalTime) {
      return new ArrowType.Time(unitFor(((LocalTime) value).getNano()), 64);
    } else if (value instanceof Timestamp) {
      return new ArrowType.Timestamp(unitFor(((Timestamp) value).getNanos()), null);
    } else if (value instanceof LocalDateTime) {
      return new ArrowType.Timestamp(unitFor(((LocalDateTime) value).getNano()), null);
    } else if (value instanceof Instant) {
      return new ArrowType.Timestamp(unitFor(((Instant) value).getNano()), "UTC");
    } else if (value instanceof OffsetDateTime) {
      return new ArrowType.Timestamp(unitFor(((OffsetDateTime) value).getNano()), "UTC");
    } else if (value instanceof ZonedDateTime) {
      return new ArrowType.Timestamp(unitFor(((ZonedDateTime) value).getNano()), "UTC");
    } else if (value instanceof java.util.Date || value instanceof Calendar) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    } else if (value instanceof Duration
        || value instanceof Period
        || value instanceof ChargewireInterval) {
      return new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO);
    }
    throw new SQLException("Unsupported parameter type " + value.getClass().getName(), "07006");
  }

  private static TimeUnit unitFor(int nanos) {
    return nanos % 1000 == 0 ? TimeUnit.MICROSECOND : TimeUnit.NANOSECOND;
  }

  private static ArrowType decimalType(BigDecimal value) {
    BigDecimal normal = value.scale() < 0 ? value.setScale(0) : value;
    int scale = normal.scale();
    int precision = Math.max(Math.max(normal.precision(), scale), 1);
    if (precision <= 38) {
      return new ArrowType.Decimal(precision, scale, 128);
    }
    return precision <= 76 ? new ArrowType.Decimal(precision, scale, 256) : UTF8;
  }

  /** The type both values fit in; text when there is no better one. */
  private static Field unify(String name, Field a, Field b) {
    if (a.equals(b)) {
      return a;
    }
    if (a.getType() instanceof ArrowType.List && b.getType() instanceof ArrowType.List) {
      Field child = unify("item", a.getChildren().get(0), b.getChildren().get(0));
      return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(child));
    }
    if (a.getType() instanceof ArrowType.Null) {
      return b;
    }
    if (b.getType() instanceof ArrowType.Null) {
      return a;
    }
    return Field.nullable(name, unify(a.getType(), b.getType()));
  }

  private static ArrowType unify(ArrowType a, ArrowType b) {
    if (a.equals(b)) {
      return a;
    }
    if (a instanceof ArrowType.Int && b instanceof ArrowType.Int) {
      return new ArrowType.Int(
          Math.max(((ArrowType.Int) a).getBitWidth(), ((ArrowType.Int) b).getBitWidth()), true);
    }
    boolean aFloat = a instanceof ArrowType.FloatingPoint;
    boolean bFloat = b instanceof ArrowType.FloatingPoint;
    if ((aFloat || SqlTypes.isNumeric(a))
        && (bFloat || SqlTypes.isNumeric(b))
        && (aFloat || bFloat)) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }
    if (SqlTypes.isNumeric(a) && SqlTypes.isNumeric(b)) {
      int scale = Math.max(scaleOf(a), scaleOf(b));
      int integerDigits = Math.max(integerDigitsOf(a), integerDigitsOf(b));
      int precision = integerDigits + scale;
      return precision <= 38
          ? new ArrowType.Decimal(precision, scale, 128)
          : precision <= 76 ? new ArrowType.Decimal(precision, scale, 256) : UTF8;
    }
    if (a instanceof ArrowType.Timestamp && b instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp ta = (ArrowType.Timestamp) a;
      ArrowType.Timestamp tb = (ArrowType.Timestamp) b;
      if (ta.getTimezone() == null && tb.getTimezone() == null
          || ta.getTimezone() != null && tb.getTimezone() != null) {
        TimeUnit unit =
            ta.getUnit() == TimeUnit.NANOSECOND || tb.getUnit() == TimeUnit.NANOSECOND
                ? TimeUnit.NANOSECOND
                : TimeUnit.MICROSECOND;
        return new ArrowType.Timestamp(unit, ta.getTimezone());
      }
    }
    if (a instanceof ArrowType.Time && b instanceof ArrowType.Time) {
      return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
    }
    return UTF8;
  }

  private static int scaleOf(ArrowType type) {
    return type instanceof ArrowType.Decimal ? ((ArrowType.Decimal) type).getScale() : 0;
  }

  private static int integerDigitsOf(ArrowType type) {
    if (type instanceof ArrowType.Decimal) {
      ArrowType.Decimal decimal = (ArrowType.Decimal) type;
      return decimal.getPrecision() - decimal.getScale();
    }
    return SqlTypes.columnSize(type);
  }

  // ---- Parameters the server types TIMESTAMP WITH TIME ZONE ----

  /** What a value bound to a {@code TIMESTAMP WITH TIME ZONE} parameter is. */
  private enum Zoned {
    /** A point in time: sent as that instant. */
    INSTANT,
    /** A date without a zone: the server takes its midnight in the session's time zone. */
    DATE,
    /** A date and time without a zone: the server reads it in the session's time zone. */
    DATE_TIME,
    /** Text without an offset the driver can read: the server reads it, as it reads a literal. */
    TEXT,
    /** Anything else: the declared type's own conversion refuses it. */
    OTHER
  }

  private static boolean isZoned(ArrowType type) {
    return type instanceof ArrowType.Timestamp
        && ((ArrowType.Timestamp) type).getTimezone() != null;
  }

  /**
   * Classifies a value for a {@code TIMESTAMP WITH TIME ZONE} parameter. A {@link Timestamp} is the
   * instant it wraps, as in {@code new Timestamp(millis)} and as {@code getTimestamp} returns one
   * for such a column; a {@link java.sql.Date} is a calendar date, unless a {@link Calendar} names
   * the zone whose midnight is meant.
   */
  private static Zoned zonedKind(Object value, Calendar calendar) {
    if (value instanceof Instant
        || value instanceof OffsetDateTime
        || value instanceof ZonedDateTime
        || value instanceof Timestamp
        || value instanceof Calendar) {
      return Zoned.INSTANT;
    }
    if (value instanceof java.sql.Date) {
      return calendar == null ? Zoned.DATE : Zoned.INSTANT;
    }
    if (value instanceof Time) {
      return Zoned.OTHER;
    }
    if (value instanceof java.util.Date) {
      return Zoned.INSTANT;
    }
    if (value instanceof LocalDate) {
      return Zoned.DATE;
    }
    if (value instanceof LocalDateTime) {
      return Zoned.DATE_TIME;
    }
    if (value instanceof String || value instanceof Character) {
      return offsetText(value.toString()) != null ? Zoned.INSTANT : Zoned.TEXT;
    }
    return Zoned.OTHER;
  }

  /**
   * The instant of ISO text with an offset ({@code 2026-01-02T03:04:05+01:00}), or {@code null}.
   */
  private static Instant offsetText(String text) {
    try {
      return OffsetDateTime.parse(text.trim().replace(' ', 'T')).toInstant();
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static List<Value> column(List<Value[]> rows, int column) {
    List<Value> values = new ArrayList<>(rows.size());
    for (Value[] row : rows) {
      values.add(row[column]);
    }
    return values;
  }

  private static Field child(Field field) {
    return field == null || field.getChildren().isEmpty() ? null : field.getChildren().get(0);
  }

  /**
   * The value to send for a declared field: for a {@code TIMESTAMP WITH TIME ZONE} parameter, a
   * point in time that {@code setObject(…, Types.TIMESTAMP)} converted to a wall clock is sent as
   * the instant it was.
   */
  private static Object sent(Field declared, Value value) {
    if (declared != null
        && isZoned(declared.getType())
        && value.sqlType == Types.TIMESTAMP
        && value.source != value.value
        && !(value.source instanceof String)
        && zonedKind(value.source, value.calendar) == Zoned.INSTANT) {
      return value.source;
    }
    return value.value;
  }

  /**
   * The field a declared parameter is sent as. It is the declared one, except where the server
   * types the parameter (or a list's elements) {@code TIMESTAMP WITH TIME ZONE} and a value has no
   * zone: then the column is sent without a zone too (a timestamp without a zone, or a date when
   * every value is one), or as text when points in time and values without a zone are mixed or a
   * value is text, and the server converts each row as it converts the same literal. A batch column
   * has one Arrow type, so the choice is made over all its rows.
   */
  private static Field sentField(Field declared, List<Value> values) throws SQLException {
    ArrowType type = declared.getType();
    if (type instanceof ArrowType.List && !declared.getChildren().isEmpty()) {
      Field element = declared.getChildren().get(0);
      List<Value> elements = new ArrayList<>();
      for (Value value : values) {
        Object list = value.value instanceof Array ? ((Array) value.value).getArray() : value.value;
        List<?> items = elements(list);
        if (items != null) {
          for (Object item : items) {
            elements.add(new Value(item, NO_TYPE, value.calendar));
          }
        }
      }
      Field sentElement = sentField(element, elements);
      return sentElement == element
          ? declared
          : new Field(declared.getName(), declared.getFieldType(), List.of(sentElement));
    }
    if (!isZoned(type)) {
      return declared;
    }
    boolean instant = false;
    boolean date = false;
    boolean dateTime = false;
    boolean text = false;
    for (Value value : values) {
      Object given = sent(declared, value);
      if (given == null) {
        continue;
      }
      switch (zonedKind(given, value.calendar)) {
        case DATE:
          date = true;
          break;
        case DATE_TIME:
          dateTime = true;
          break;
        case TEXT:
          text = true;
          break;
        default:
          instant = true;
          break;
      }
    }
    if (!date && !dateTime && !text) {
      return declared;
    }
    if (text || instant) {
      return Field.nullable(declared.getName(), UTF8);
    }
    return Field.nullable(
        declared.getName(),
        dateTime
            ? new ArrowType.Timestamp(((ArrowType.Timestamp) type).getUnit(), null)
            : new ArrowType.Date(DateUnit.DAY));
  }

  /**
   * The text of a value for a {@code TIMESTAMP WITH TIME ZONE} parameter whose column is sent as
   * text: a point in time with its offset from UTC, a value without a zone without one, and text as
   * it was given.
   */
  private static String zonedText(Object value, Calendar calendar, TimeUnit unit)
      throws SQLException {
    switch (zonedKind(value, calendar)) {
      case TEXT:
        return value.toString();
      case DATE:
        return dateText(toDate(value, calendar));
      case DATE_TIME:
        LocalDateTime dateTime = (LocalDateTime) value;
        epoch(dateTime.toInstant(ZoneOffset.UTC), unit);
        return dateTimeText(dateTime);
      default:
        Instant instant = toInstant(value, calendar, true);
        epoch(instant, unit);
        return dateTimeText(LocalDateTime.ofInstant(instant, ZoneOffset.UTC)) + "+00:00";
    }
  }

  /** {@code yyyy-MM-dd}, the year with a sign only when negative, as the server reads it. */
  private static String dateText(LocalDate date) {
    return (date.getYear() < 0 ? "-" : "")
        + String.format(
            "%04d-%02d-%02d", Math.abs(date.getYear()), date.getMonthValue(), date.getDayOfMonth());
  }

  private static String dateTimeText(LocalDateTime dateTime) {
    String text =
        dateText(dateTime.toLocalDate())
            + String.format(
                " %02d:%02d:%02d", dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    if (dateTime.getNano() == 0) {
      return text;
    }
    String fraction = String.format("%09d", dateTime.getNano());
    int end = fraction.length();
    while (fraction.charAt(end - 1) == '0') {
      end--;
    }
    return text + "." + fraction.substring(0, end);
  }

  // ---- Writing values ----

  static void set(FieldVector vector, int row, Object value, Calendar calendar)
      throws SQLException {
    set(vector, row, value, calendar, null);
  }

  /**
   * Writes one value.
   *
   * @param declared the server's declared field for this position, or {@code null} when it declared
   *     none: text written for a {@code TIMESTAMP WITH TIME ZONE} parameter is written in the form
   *     the server reads ({@link #zonedText})
   */
  private static void set(
      FieldVector vector, int row, Object value, Calendar calendar, Field declared)
      throws SQLException {
    if (value instanceof Array) {
      value = ((Array) value).getArray();
    }
    if (value == null) {
      setNull(vector, row);
      return;
    }
    if (vector instanceof NullVector) {
      throw new SQLDataException("The parameter's type is NULL, but a value was given", "22023");
    } else if (vector instanceof TinyIntVector) {
      ((TinyIntVector) vector)
          .setSafe(row, (byte) checkedLong(value, Byte.MIN_VALUE, Byte.MAX_VALUE));
    } else if (vector instanceof SmallIntVector) {
      ((SmallIntVector) vector)
          .setSafe(row, (short) checkedLong(value, Short.MIN_VALUE, Short.MAX_VALUE));
    } else if (vector instanceof IntVector) {
      ((IntVector) vector)
          .setSafe(row, (int) checkedLong(value, Integer.MIN_VALUE, Integer.MAX_VALUE));
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(row, checkedLong(value, Long.MIN_VALUE, Long.MAX_VALUE));
    } else if (vector instanceof UInt1Vector) {
      ((UInt1Vector) vector).setSafe(row, (int) checkedLong(value, 0, 0xFF));
    } else if (vector instanceof UInt2Vector) {
      ((UInt2Vector) vector).setSafe(row, (char) checkedLong(value, 0, 0xFFFF));
    } else if (vector instanceof UInt4Vector) {
      ((UInt4Vector) vector).setSafe(row, (int) checkedLong(value, 0, 0xFFFFFFFFL));
    } else if (vector instanceof UInt8Vector) {
      BigInteger integer = exactInteger(value);
      if (integer.signum() < 0 || integer.bitLength() > 64) {
        throw outOfRange(value, "UBIGINT");
      }
      ((UInt8Vector) vector).setSafe(row, integer.longValue());
    } else if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).setSafe(row, (float) toDouble(value));
    } else if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(row, toDouble(value));
    } else if (vector instanceof DecimalVector) {
      ArrowType.Decimal type = (ArrowType.Decimal) vector.getField().getType();
      ((DecimalVector) vector).setSafe(row, toDecimal(value, type));
    } else if (vector instanceof Decimal256Vector) {
      ArrowType.Decimal type = (ArrowType.Decimal) vector.getField().getType();
      ((Decimal256Vector) vector).setSafe(row, toDecimal(value, type));
    } else if (vector instanceof BitVector) {
      ((BitVector) vector).setSafe(row, toBoolean(value) ? 1 : 0);
    } else if (vector instanceof VarCharVector) {
      String text =
          declared != null && isZoned(declared.getType())
              ? zonedText(value, calendar, ((ArrowType.Timestamp) declared.getType()).getUnit())
              : toText(value);
      ((VarCharVector) vector).setSafe(row, text.getBytes(StandardCharsets.UTF_8));
    } else if (vector instanceof LargeVarCharVector) {
      ((LargeVarCharVector) vector).setSafe(row, toText(value).getBytes(StandardCharsets.UTF_8));
    } else if (vector instanceof VarBinaryVector) {
      ((VarBinaryVector) vector).setSafe(row, toBytes(value));
    } else if (vector instanceof LargeVarBinaryVector) {
      ((LargeVarBinaryVector) vector).setSafe(row, toBytes(value));
    } else if (vector instanceof UuidVector) {
      ((UuidVector) vector).setSafe(row, toUuid(value));
    } else if (vector instanceof FixedSizeBinaryVector) {
      FixedSizeBinaryVector binary = (FixedSizeBinaryVector) vector;
      byte[] bytes = toBytes(value);
      if (bytes.length != binary.getByteWidth()) {
        throw new SQLDataException(
            "Expected " + binary.getByteWidth() + " bytes, got " + bytes.length, "22001");
      }
      binary.setSafe(row, bytes);
    } else if (vector instanceof DateDayVector) {
      ((DateDayVector) vector).setSafe(row, Math.toIntExact(toDate(value, calendar).toEpochDay()));
    } else if (vector instanceof DateMilliVector) {
      ((DateMilliVector) vector).setSafe(row, toDate(value, calendar).toEpochDay() * 86_400_000L);
    } else if (vector instanceof TimeSecVector
        || vector instanceof TimeMilliVector
        || vector instanceof TimeMicroVector
        || vector instanceof TimeNanoVector) {
      setTime(vector, row, toTime(value, calendar).toNanoOfDay());
    } else if (vector instanceof TimeStampVector) {
      ArrowType.Timestamp type = (ArrowType.Timestamp) vector.getField().getType();
      Instant instant = toInstant(value, calendar, type.getTimezone() != null);
      ((TimeStampVector) vector).setSafe(row, epoch(instant, type.getUnit()));
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      ChargewireInterval interval = toInterval(value);
      ((IntervalMonthDayNanoVector) vector)
          .setSafe(row, interval.months(), interval.days(), interval.nanos());
    } else if (vector instanceof ListVector) {
      setList((ListVector) vector, row, value, calendar, child(declared));
    } else {
      throw new SQLException(
          "Parameters of type " + vector.getField().getType() + " are not supported", "0A000");
    }
  }

  private static void setNull(FieldVector vector, int row) {
    if (vector instanceof UuidVector) {
      ((UuidVector) vector).getUnderlyingVector().setNull(row);
    } else if (vector instanceof BaseFixedWidthVector) {
      ((BaseFixedWidthVector) vector).setNull(row);
    } else if (vector instanceof BaseVariableWidthVector) {
      ((BaseVariableWidthVector) vector).setNull(row);
    } else if (vector instanceof ListVector) {
      ((ListVector) vector).setNull(row);
    }
  }

  private static void setTime(FieldVector vector, int row, long nanoOfDay) throws SQLException {
    if (vector instanceof TimeNanoVector) {
      ((TimeNanoVector) vector).setSafe(row, nanoOfDay);
    } else if (vector instanceof TimeMicroVector) {
      ((TimeMicroVector) vector).setSafe(row, exactDivide(nanoOfDay, 1_000L, "microsecond"));
    } else if (vector instanceof TimeMilliVector) {
      ((TimeMilliVector) vector)
          .setSafe(row, (int) exactDivide(nanoOfDay, 1_000_000L, "millisecond"));
    } else {
      ((TimeSecVector) vector).setSafe(row, (int) exactDivide(nanoOfDay, 1_000_000_000L, "second"));
    }
  }

  private static void setList(
      ListVector vector, int row, Object value, Calendar calendar, Field declaredChild)
      throws SQLException {
    List<?> elements = elements(value);
    if (elements == null) {
      throw new SQLDataException(
          "A list parameter needs an array, a java.sql.Array or a List", "22023");
    }
    FieldVector data = vector.getDataVector();
    int start = vector.startNewValue(row);
    for (int i = 0; i < elements.size(); i++) {
      set(data, start + i, elements.get(i), calendar, declaredChild);
    }
    vector.endValue(row, elements.size());
    data.setValueCount(start + elements.size());
  }

  /** The elements of an array or list value, or {@code null} for any other value. */
  private static List<?> elements(Object value) {
    if (value instanceof List) {
      return (List<?>) value;
    }
    if (value instanceof Object[]) {
      return Arrays.asList((Object[]) value);
    }
    if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
      int length = java.lang.reflect.Array.getLength(value);
      List<Object> list = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        list.add(java.lang.reflect.Array.get(value, i));
      }
      return list;
    }
    return null;
  }

  // ---- Conversions ----

  private static SQLDataException outOfRange(Object value, String target) {
    return new SQLDataException("The value " + value + " does not fit in " + target, "22003");
  }

  private static SQLDataException unparseable(Object value, String target, Throwable cause) {
    return new SQLDataException("Cannot convert '" + value + "' to " + target, "22018", cause);
  }

  private static BigInteger exactInteger(Object value) throws SQLException {
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      return BigInteger.valueOf(((Number) value).longValue());
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? BigInteger.ONE : BigInteger.ZERO;
    }
    BigDecimal decimal;
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    } else if (value instanceof BigDecimal) {
      decimal = (BigDecimal) value;
    } else if (value instanceof Double || value instanceof Float) {
      double number = ((Number) value).doubleValue();
      if (Double.isNaN(number) || Double.isInfinite(number)) {
        throw outOfRange(value, "an integer");
      }
      decimal = new BigDecimal(value.toString());
    } else if (value instanceof String) {
      try {
        decimal = new BigDecimal(((String) value).trim());
      } catch (NumberFormatException e) {
        throw unparseable(value, "an integer", e);
      }
    } else {
      throw unparseable(value, "an integer", null);
    }
    try {
      return decimal.toBigIntegerExact();
    } catch (ArithmeticException e) {
      throw new SQLDataException(
          "The value " + value + " has a fraction; an integer parameter cannot hold it", "22003");
    }
  }

  private static long checkedLong(Object value, long min, long max) throws SQLException {
    BigInteger integer = exactInteger(value);
    if (integer.bitLength() > 63 || integer.longValue() < min || integer.longValue() > max) {
      throw outOfRange(value, "the parameter's integer type");
    }
    return integer.longValue();
  }

  private static double toDouble(Object value) throws SQLException {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1 : 0;
    }
    if (value instanceof String) {
      try {
        return Double.parseDouble(((String) value).trim());
      } catch (NumberFormatException e) {
        throw unparseable(value, "a floating-point number", e);
      }
    }
    throw unparseable(value, "a floating-point number", null);
  }

  private static BigDecimal toBigDecimal(Object value) throws SQLException {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    } else if (value instanceof Double || value instanceof Float) {
      double number = ((Number) value).doubleValue();
      if (Double.isNaN(number) || Double.isInfinite(number)) {
        throw outOfRange(value, "DECIMAL");
      }
      return new BigDecimal(value.toString());
    } else if (value instanceof Number) {
      return BigDecimal.valueOf(((Number) value).longValue());
    } else if (value instanceof Boolean) {
      return (Boolean) value ? BigDecimal.ONE : BigDecimal.ZERO;
    } else if (value instanceof String) {
      try {
        return new BigDecimal(((String) value).trim());
      } catch (NumberFormatException e) {
        throw unparseable(value, "DECIMAL", e);
      }
    }
    throw unparseable(value, "DECIMAL", null);
  }

  /** Rounds half up to the column's scale, as SQL assignment to a DECIMAL column does. */
  private static BigDecimal toDecimal(Object value, ArrowType.Decimal type) throws SQLException {
    BigDecimal scaled = toBigDecimal(value).setScale(type.getScale(), RoundingMode.HALF_UP);
    if (scaled.precision() > type.getPrecision()) {
      throw outOfRange(value, "DECIMAL(" + type.getPrecision() + "," + type.getScale() + ")");
    }
    return scaled;
  }

  private static boolean toBoolean(Object value) throws SQLException {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue() != 0;
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      if ("true".equalsIgnoreCase(text) || "1".equals(text)) {
        return true;
      }
      if ("false".equalsIgnoreCase(text) || "0".equals(text)) {
        return false;
      }
    }
    throw unparseable(value, "BOOLEAN", null);
  }

  private static String toText(Object value) throws SQLException {
    if (value instanceof byte[]) {
      throw unparseable("binary data", "text", null);
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    return value.toString();
  }

  private static byte[] toBytes(Object value) throws SQLException {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof UUID) {
      UUID uuid = (UUID) value;
      return ByteBuffer.allocate(16)
          .putLong(uuid.getMostSignificantBits())
          .putLong(uuid.getLeastSignificantBits())
          .array();
    }
    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }
    throw unparseable(value.getClass().getSimpleName(), "binary", null);
  }

  private static UUID toUuid(Object value) throws SQLException {
    if (value instanceof UUID) {
      return (UUID) value;
    }
    if (value instanceof String) {
      try {
        return UUID.fromString(((String) value).trim());
      } catch (IllegalArgumentException e) {
        throw unparseable(value, "UUID", e);
      }
    }
    if (value instanceof byte[] && ((byte[]) value).length == 16) {
      ByteBuffer bytes = ByteBuffer.wrap((byte[]) value);
      return new UUID(bytes.getLong(), bytes.getLong());
    }
    throw unparseable(value.getClass().getSimpleName(), "UUID", null);
  }

  private static ZoneId zone(Calendar calendar) {
    return calendar == null ? ZoneId.systemDefault() : calendar.getTimeZone().toZoneId();
  }

  private static LocalDate toDate(Object value, Calendar calendar) throws SQLException {
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof java.sql.Date) {
      java.sql.Date date = (java.sql.Date) value;
      return calendar == null
          ? date.toLocalDate()
          : Instant.ofEpochMilli(date.getTime()).atZone(zone(calendar)).toLocalDate();
    }
    if (value instanceof String) {
      try {
        return LocalDate.parse(((String) value).trim());
      } catch (DateTimeParseException e) {
        throw unparseable(value, "DATE", e);
      }
    }
    return toLocalDateTime(value, calendar, "DATE").toLocalDate();
  }

  private static LocalTime toTime(Object value, Calendar calendar) throws SQLException {
    if (value instanceof LocalTime) {
      return (LocalTime) value;
    }
    if (value instanceof Time) {
      return Instant.ofEpochMilli(((Time) value).getTime()).atZone(zone(calendar)).toLocalTime();
    }
    if (value instanceof String) {
      try {
        return LocalTime.parse(((String) value).trim());
      } catch (DateTimeParseException e) {
        throw unparseable(value, "TIME", e);
      }
    }
    return toLocalDateTime(value, calendar, "TIME").toLocalTime();
  }

  private static LocalDateTime toLocalDateTime(Object value, Calendar calendar, String target)
      throws SQLException {
    if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    }
    if (value instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) value;
      return calendar == null
          ? timestamp.toLocalDateTime()
          : timestamp.toInstant().atZone(zone(calendar)).toLocalDateTime();
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).atStartOfDay();
    }
    if (value instanceof java.sql.Date) {
      return toDate(value, calendar).atStartOfDay();
    }
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toLocalDateTime();
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toLocalDateTime();
    }
    if (value instanceof Instant) {
      return LocalDateTime.ofInstant((Instant) value, ZoneOffset.UTC);
    }
    if (value instanceof java.util.Date) {
      return Instant.ofEpochMilli(((java.util.Date) value).getTime())
          .atZone(zone(calendar))
          .toLocalDateTime();
    }
    if (value instanceof Calendar) {
      Calendar given = (Calendar) value;
      return given.toInstant().atZone(given.getTimeZone().toZoneId()).toLocalDateTime();
    }
    if (value instanceof String) {
      try {
        return LocalDateTime.parse(((String) value).trim().replace(' ', 'T'));
      } catch (DateTimeParseException e) {
        throw unparseable(value, target, e);
      }
    }
    throw unparseable(value.getClass().getSimpleName(), target, null);
  }

  /**
   * The instant to store. A wall-clock column stores the wall clock as if it were UTC, reading a
   * point in time in the calendar's zone (or the JVM's, as JDBC defines for {@link Timestamp}). A
   * zoned column stores the absolute instant of a value that is a point in time; values without a
   * zone never get here ({@link #sentField}), because only the server knows the session's zone.
   */
  private static Instant toInstant(Object value, Calendar calendar, boolean zoned)
      throws SQLException {
    if (!zoned) {
      return toLocalDateTime(value, calendar, "TIMESTAMP").toInstant(ZoneOffset.UTC);
    }
    if (value instanceof Instant) {
      return (Instant) value;
    } else if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toInstant();
    } else if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toInstant();
    } else if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    } else if (value instanceof Calendar) {
      return ((Calendar) value).toInstant();
    } else if (value instanceof java.sql.Date && calendar != null) {
      // The Calendar names the zone: that date as the Calendar's zone sees it, at its midnight.
      return toDate(value, calendar).atStartOfDay(zone(calendar)).toInstant();
    } else if (value instanceof java.util.Date
        && !(value instanceof java.sql.Date)
        && !(value instanceof Time)) {
      return Instant.ofEpochMilli(((java.util.Date) value).getTime());
    } else if (value instanceof String || value instanceof Character) {
      Instant instant = offsetText(value.toString());
      if (instant != null) {
        return instant;
      }
    }
    // A time of day has no date (the server refuses TIME as a TIMESTAMP WITH TIME ZONE too), and
    // numbers and the like are no timestamps.
    throw unparseable(
        value instanceof String ? value : value.getClass().getSimpleName(),
        "TIMESTAMP WITH TIME ZONE",
        null);
  }

  private static long epoch(Instant instant, TimeUnit unit) throws SQLException {
    try {
      switch (unit) {
        case SECOND:
          if (instant.getNano() != 0) {
            throw truncation("second");
          }
          return instant.getEpochSecond();
        case MILLISECOND:
          if (instant.getNano() % 1_000_000 != 0) {
            throw truncation("millisecond");
          }
          return instant.toEpochMilli();
        case MICROSECOND:
          if (instant.getNano() % 1_000 != 0) {
            throw truncation("microsecond");
          }
          return Math.addExact(
              Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000);
        default:
          return Math.addExact(
              Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
      }
    } catch (ArithmeticException e) {
      throw outOfRange(instant, "TIMESTAMP");
    }
  }

  private static long exactDivide(long nanos, long divisor, String unit) throws SQLException {
    if (nanos % divisor != 0) {
      throw truncation(unit);
    }
    return nanos / divisor;
  }

  private static SQLDataException truncation(String unit) {
    return new SQLDataException(
        "The value has a finer fraction than the parameter's " + unit + " precision", "22008");
  }

  private static ChargewireInterval toInterval(Object value) throws SQLException {
    if (value instanceof ChargewireInterval) {
      return (ChargewireInterval) value;
    }
    if (value instanceof Period) {
      Period period = (Period) value;
      return new ChargewireInterval(Math.toIntExact(period.toTotalMonths()), period.getDays(), 0);
    }
    if (value instanceof Duration) {
      Duration duration = (Duration) value;
      try {
        return new ChargewireInterval(0, 0, duration.toNanos());
      } catch (ArithmeticException e) {
        throw outOfRange(value, "INTERVAL");
      }
    }
    throw unparseable(value.getClass().getSimpleName(), "INTERVAL", null);
  }
}
