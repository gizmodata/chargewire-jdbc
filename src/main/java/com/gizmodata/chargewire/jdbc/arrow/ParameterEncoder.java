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

    /**
     * Creates a bound value.
     *
     * @param value the value, or {@code null}
     * @param sqlType the {@link Types} hint, or {@link #NO_TYPE}
     * @param calendar the zone for wall-clock values, or {@code null}
     */
    public Value(Object value, int sqlType, Calendar calendar) {
      this.value = value;
      this.sqlType = sqlType;
      this.calendar = calendar;
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
      fields.add(type != null ? declaredField : inferField(name, rows, c));
    }
    try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator)) {
      root.allocateNew();
      for (int c = 0; c < columns; c++) {
        FieldVector vector = root.getVector(c);
        for (int r = 0; r < rows.size(); r++) {
          Value value = rows.get(r)[c];
          try {
            set(vector, r, value.value, value.calendar);
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
        return toInstant(value, null, true);
      default:
        return value;
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

  // ---- Writing values ----

  static void set(FieldVector vector, int row, Object value, Calendar calendar)
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
      ((VarCharVector) vector).setSafe(row, toText(value).getBytes(StandardCharsets.UTF_8));
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
      setList((ListVector) vector, row, value, calendar);
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

  private static void setList(ListVector vector, int row, Object value, Calendar calendar)
      throws SQLException {
    List<?> elements = elements(value);
    if (elements == null) {
      throw new SQLDataException(
          "A list parameter needs an array, a java.sql.Array or a List", "22023");
    }
    FieldVector data = vector.getDataVector();
    int start = vector.startNewValue(row);
    for (int i = 0; i < elements.size(); i++) {
      set(data, start + i, elements.get(i), calendar);
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
   * The instant to store. A wall-clock column stores the wall clock as if it were UTC; a zoned
   * column stores the absolute instant, reading wall-clock values in the calendar's zone (or the
   * JVM's).
   */
  private static Instant toInstant(Object value, Calendar calendar, boolean zoned)
      throws SQLException {
    if (zoned) {
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
      } else if (value instanceof java.util.Date && !(value instanceof java.sql.Date)) {
        return Instant.ofEpochMilli(((java.util.Date) value).getTime());
      } else if (value instanceof String) {
        String text = ((String) value).trim().replace(' ', 'T');
        try {
          return OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeParseException e) {
          // A wall-clock text: read below in the calendar's zone.
        }
      }
      return toLocalDateTime(value, calendar, "TIMESTAMP WITH TIME ZONE")
          .atZone(zone(calendar))
          .toInstant();
    }
    return toLocalDateTime(value, calendar, "TIMESTAMP").toInstant(ZoneOffset.UTC);
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
