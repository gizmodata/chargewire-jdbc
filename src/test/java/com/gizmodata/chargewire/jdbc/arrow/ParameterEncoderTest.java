package com.gizmodata.chargewire.jdbc.arrow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireInterval;
import com.gizmodata.chargewire.jdbc.accessor.Accessor;
import com.gizmodata.chargewire.jdbc.accessor.Accessors;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
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
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.extension.OpaqueType;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Parameter encoding (ported from {@code AvaticaParameterBinder}'s and the parameter converters'
 * tests, plus the inference Chargewire needs because servers may not know a parameter's type).
 */
class ParameterEncoderTest {
  private final BufferAllocator allocator = new RootAllocator();

  @AfterEach
  void close() {
    allocator.close();
  }

  private static ParameterEncoder.Value value(Object value) {
    return new ParameterEncoder.Value(value, ParameterEncoder.NO_TYPE, null);
  }

  /** Encodes, decodes, and returns the schema and each row's values. */
  private Decoded encode(Schema declared, List<ParameterEncoder.Value[]> rows) throws Exception {
    byte[] bytes = ParameterEncoder.encode(allocator, declared, rows);
    try (ArrowIpc.HeapReadChannel channel =
            new ArrowIpc.HeapReadChannel(new ByteArrayInputStream(bytes));
        ArrowStreamReader reader = ArrowIpc.reader(channel, allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      List<Object[]> values = new ArrayList<>();
      while (reader.loadNextBatch()) {
        for (int r = 0; r < root.getRowCount(); r++) {
          Object[] row = new Object[root.getFieldVectors().size()];
          for (int c = 0; c < row.length; c++) {
            Accessor accessor = Accessors.create(root.getVector(c), null);
            row[c] = accessor.isNull(r) ? null : accessor.getObject(r);
          }
          values.add(row);
        }
      }
      return new Decoded(root.getSchema(), values);
    }
  }

  private Decoded one(Object... values) throws Exception {
    ParameterEncoder.Value[] row =
        Arrays.stream(values)
            .map(ParameterEncoderTest::value)
            .toArray(ParameterEncoder.Value[]::new);
    return encode(null, List.<ParameterEncoder.Value[]>of(row));
  }

  private static final class Decoded {
    final Schema schema;
    final List<Object[]> rows;

    Decoded(Schema schema, List<Object[]> rows) {
      this.schema = schema;
      this.rows = rows;
    }

    ArrowType type(int column) {
      return schema.getFields().get(column).getType();
    }
  }

  @Test
  void infersTypesFromJavaValues() throws Exception {
    Decoded decoded =
        one(
            true,
            (byte) 1,
            (short) 2,
            3,
            4L,
            1.5f,
            2.5,
            new BigDecimal("12.345"),
            "text",
            new byte[] {1},
            LocalDate.of(2026, 1, 2),
            LocalTime.of(1, 2, 3),
            LocalDateTime.of(2026, 1, 2, 3, 4, 5),
            Instant.parse("2026-01-02T03:04:05Z"),
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            Duration.ofSeconds(90));
    assertEquals(ArrowType.Bool.INSTANCE, decoded.type(0));
    assertEquals(new ArrowType.Int(8, true), decoded.type(1));
    assertEquals(new ArrowType.Int(16, true), decoded.type(2));
    assertEquals(new ArrowType.Int(32, true), decoded.type(3));
    assertEquals(new ArrowType.Int(64, true), decoded.type(4));
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), decoded.type(5));
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), decoded.type(6));
    assertEquals(new ArrowType.Decimal(5, 3, 128), decoded.type(7));
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(8));
    assertEquals(ArrowType.Binary.INSTANCE, decoded.type(9));
    assertEquals(new ArrowType.Date(DateUnit.DAY), decoded.type(10));
    assertEquals(new ArrowType.Time(TimeUnit.MICROSECOND, 64), decoded.type(11));
    assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), decoded.type(12));
    assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), decoded.type(13));
    assertEquals(
        ArrowType.Utf8.INSTANCE, decoded.type(14), "UUIDs travel as text the engine casts");
    assertEquals(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), decoded.type(15));
    Object[] row = decoded.rows.get(0);
    assertEquals(new BigDecimal("12.345"), row[7]);
    assertEquals("00000000-0000-0000-0000-000000000001", row[14]);
    assertEquals(new ChargewireInterval(0, 0, 90_000_000_000L), row[15]);
    assertEquals(Timestamp.valueOf("2026-01-02 03:04:05"), row[12]);
  }

  @Test
  void subMicrosecondValuesUseNanoseconds() throws Exception {
    Decoded decoded = one(LocalDateTime.of(2026, 1, 1, 0, 0, 0, 1), LocalTime.of(0, 0, 0, 1));
    assertEquals(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), decoded.type(0));
    assertEquals(new ArrowType.Time(TimeUnit.NANOSECOND, 64), decoded.type(1));
  }

  @Test
  void decimalsGetAPrecisionThatFits() throws Exception {
    Decoded decoded =
        one(
            new BigDecimal("0.05"),
            new BigDecimal("1E+3"),
            new BigInteger("123"),
            new BigDecimal("1" + "0".repeat(45)));
    assertEquals(new ArrowType.Decimal(2, 2, 128), decoded.type(0));
    assertEquals(new ArrowType.Decimal(4, 0, 128), decoded.type(1), "a negative scale becomes 0");
    assertEquals(new ArrowType.Decimal(3, 0, 128), decoded.type(2));
    assertEquals(new ArrowType.Decimal(46, 0, 256), decoded.type(3));
    assertEquals(new BigDecimal("1000"), decoded.rows.get(0)[1]);
  }

  @Test
  void batchRowsWidenToACommonType() throws Exception {
    List<ParameterEncoder.Value[]> rows =
        List.of(
            new ParameterEncoder.Value[] {
              value(1), value(1), value(new BigDecimal("1.5")), value(1), value(null)
            },
            new ParameterEncoder.Value[] {
              value(2L), value(2.5), value(new BigDecimal("123.25")), value("x"), value(null)
            });
    Decoded decoded = encode(null, rows);
    assertEquals(new ArrowType.Int(64, true), decoded.type(0));
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), decoded.type(1));
    assertEquals(new ArrowType.Decimal(5, 2, 128), decoded.type(2));
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(3), "unrelated types fall back to text");
    assertEquals(ArrowType.Null.INSTANCE, decoded.type(4));
    assertEquals(2L, decoded.rows.get(1)[0]);
    assertEquals("1", decoded.rows.get(0)[3]);
    assertEquals(new BigDecimal("1.50"), decoded.rows.get(0)[2]);
  }

  @Test
  void typedNullsUseTheirHint() throws Exception {
    ParameterEncoder.Value[] row = {
      new ParameterEncoder.Value(null, Types.INTEGER, null),
      new ParameterEncoder.Value(null, Types.VARCHAR, null),
      new ParameterEncoder.Value(null, Types.DECIMAL, null)
    };
    Decoded decoded = encode(null, List.<ParameterEncoder.Value[]>of(row));
    assertEquals(new ArrowType.Int(32, true), decoded.type(0));
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(1));
    assertEquals(ArrowType.Null.INSTANCE, decoded.type(2));
    assertArrayEquals(new Object[] {null, null, null}, decoded.rows.get(0));
  }

  @Test
  void listsInferTheirElementType() throws Exception {
    Decoded decoded = one(new Object[] {1, null, 3}, new int[] {4, 5}, List.of("a"));
    assertEquals(ArrowType.List.INSTANCE, decoded.type(0));
    assertEquals(
        new ArrowType.Int(32, true),
        decoded.schema.getFields().get(0).getChildren().get(0).getType());
    assertEquals(Arrays.asList(1, null, 3), decoded.rows.get(0)[0]);
    assertEquals(List.of(4, 5), decoded.rows.get(0)[1]);
    assertEquals(List.of("a"), decoded.rows.get(0)[2]);
  }

  private static Schema declared(ArrowType... types) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      fields.add(Field.nullable(Integer.toString(i + 1), types[i]));
    }
    return new Schema(fields);
  }

  @Test
  void declaredTypesWinAndValuesConvert() throws Exception {
    Schema schema =
        declared(
            new ArrowType.Int(32, true),
            ArrowType.Utf8.INSTANCE,
            new ArrowType.Decimal(10, 3, 128),
            new ArrowType.Date(DateUnit.DAY),
            ArrowType.Bool.INSTANCE,
            ArrowType.Null.INSTANCE,
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            new ArrowType.Int(8, false));
    ParameterEncoder.Value[] row = {
      value("12"),
      value(34),
      value(new BigDecimal("9.87654")),
      value("2026-10-17"),
      value("true"),
      value(7L),
      value(3),
      value(200)
    };
    Decoded decoded = encode(schema, List.<ParameterEncoder.Value[]>of(row));
    assertEquals(new ArrowType.Int(32, true), decoded.type(0));
    assertEquals(new ArrowType.Int(64, true), decoded.type(5), "a declared null type is inferred");
    Object[] values = decoded.rows.get(0);
    assertEquals(12, values[0]);
    assertEquals("34", values[1]);
    assertEquals(
        new BigDecimal("9.877"), values[2], "decimals round half up to the declared scale");
    assertEquals(Date.valueOf("2026-10-17"), values[3]);
    assertEquals(true, values[4]);
    assertEquals(7L, values[5]);
    assertEquals(3.0, values[6]);
    assertEquals((short) 200, values[7]);
  }

  @Test
  void uuidParametersForServersThatDeclareThem() throws Exception {
    UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    Schema schema =
        new Schema(
            List.of(
                new Field("1", FieldType.nullable(UuidType.INSTANCE), null),
                new Field("2", FieldType.nullable(UuidType.INSTANCE), null),
                new Field("3", FieldType.nullable(UuidType.INSTANCE), null)));
    ParameterEncoder.Value[] row = {value(uuid), value(uuid.toString()), value(null)};
    Decoded decoded = encode(schema, List.<ParameterEncoder.Value[]>of(row));
    assertEquals(uuid, decoded.rows.get(0)[0]);
    assertEquals(uuid, decoded.rows.get(0)[1]);
    assertEquals(null, decoded.rows.get(0)[2]);
    assertCode(
        "22018",
        new Schema(List.of(new Field("1", FieldType.nullable(UuidType.INSTANCE), null))),
        "nope");
  }

  @Test
  void conversionsThatWouldLoseInformationFail() {
    Schema integer = declared(new ArrowType.Int(32, true));
    assertCode("22003", integer, 1L << 40);
    assertCode("22003", integer, 1.5);
    assertCode("22003", integer, new BigDecimal("2.5"));
    assertCode("22018", integer, "twelve");
    assertCode("22003", declared(new ArrowType.Int(8, false)), -1);
    assertCode("22003", declared(new ArrowType.Decimal(4, 2, 128)), new BigDecimal("123.45"));
    assertCode("22018", declared(new ArrowType.Date(DateUnit.DAY)), "17/10/2026");
    assertCode("22018", declared(ArrowType.Bool.INSTANCE), "maybe");
    assertCode(
        "22008",
        declared(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
        LocalDateTime.of(2026, 1, 1, 0, 0, 0, 1));
    assertCode(
        "22008",
        declared(new ArrowType.Time(TimeUnit.SECOND, 32)),
        LocalTime.of(0, 0, 0, 5_000_000));
    assertCode("22001", declared(new ArrowType.FixedSizeBinary(4)), new byte[] {1, 2});
    assertCode("22018", declared(ArrowType.Utf8.INSTANCE), new byte[] {1});
  }

  private void assertCode(String sqlState, Schema declared, Object value) {
    SQLException error =
        assertThrows(
            SQLException.class,
            () ->
                encode(
                    declared,
                    List.<ParameterEncoder.Value[]>of(
                        new ParameterEncoder.Value[] {value(value)})));
    assertEquals(sqlState, error.getSQLState(), error.getMessage());
    assertTrue(error.getMessage().startsWith("Parameter 1"), error.getMessage());
  }

  @Test
  void batchErrorsNameTheRow() {
    SQLException error =
        assertThrows(
            SQLDataException.class,
            () ->
                encode(
                    declared(new ArrowType.Int(32, true)),
                    List.of(
                        new ParameterEncoder.Value[] {value(1)},
                        new ParameterEncoder.Value[] {value("x")})));
    assertTrue(error.getMessage().startsWith("Parameter 1 of batch row 2"), error.getMessage());
  }

  @Test
  void unsupportedJavaTypesAreRefused() {
    SQLException error = assertThrows(SQLException.class, () -> one(new Object()));
    assertEquals("07006", error.getSQLState());
  }

  @Test
  void timestampsHonourZonesAndCalendars() throws Exception {
    Calendar tokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    Timestamp instant = Timestamp.from(Instant.parse("2026-01-02T03:04:05Z"));
    Schema schema =
        declared(
            new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
            new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
            new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
            new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    ParameterEncoder.Value[] row = {
      new ParameterEncoder.Value(instant, Types.TIMESTAMP, tokyo),
      new ParameterEncoder.Value(instant, Types.TIMESTAMP, tokyo),
      value(OffsetDateTime.parse("2026-01-02T12:04:05+09:00")),
      value(Instant.parse("2026-01-02T03:04:05Z"))
    };
    Decoded decoded = encode(schema, List.<ParameterEncoder.Value[]>of(row));
    Object[] values = decoded.rows.get(0);
    assertEquals(Timestamp.valueOf("2026-01-02 12:04:05"), values[0], "the wall clock in Tokyo");
    assertEquals(instant, values[1], "an instant is an instant");
    assertEquals(instant, values[2]);
    assertEquals(
        Timestamp.valueOf("2026-01-02 03:04:05"), values[3], "an Instant's wall clock is UTC");
  }

  // ---- A parameter the server types TIMESTAMP WITH TIME ZONE ----

  private static final ArrowType ZONED =
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Kolkata");
  private static final ArrowType WALL_CLOCK = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);

  /**
   * Encodes one zoned column, one row per value, with the JVM in another zone, and reads it back in
   * forms that do not depend on the JVM's zone: a timestamp without a zone as {@link
   * LocalDateTime}, one with a zone as {@link Instant}, a date as {@link LocalDate}.
   */
  private Decoded zoned(ParameterEncoder.Value... values) throws Exception {
    return inAuckland(
        () -> {
          List<ParameterEncoder.Value[]> rows = new ArrayList<>();
          for (ParameterEncoder.Value value : values) {
            rows.add(new ParameterEncoder.Value[] {value});
          }
          Decoded decoded = encode(declared(ZONED), rows);
          boolean instants = ZONED.equals(decoded.type(0));
          for (Object[] row : decoded.rows) {
            if (row[0] instanceof Timestamp) {
              Timestamp timestamp = (Timestamp) row[0];
              row[0] = instants ? timestamp.toInstant() : timestamp.toLocalDateTime();
            } else if (row[0] instanceof Date) {
              row[0] = ((Date) row[0]).toLocalDate();
            }
          }
          return decoded;
        });
  }

  private static <T> T inAuckland(java.util.concurrent.Callable<T> work) throws Exception {
    TimeZone before = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Auckland"));
    try {
      return work.call();
    } finally {
      TimeZone.setDefault(before);
    }
  }

  private static Object column(Decoded decoded, int row) {
    return decoded.rows.get(row)[0];
  }

  @Test
  void aValueWithoutAZoneTravelsWithoutOneToAZonedParameter() throws Exception {
    LocalDateTime wallClock = LocalDateTime.of(2026, 1, 1, 0, 0, 0, 123_456_000);
    Decoded decoded = zoned(value(wallClock), value(null), value(wallClock.plusDays(1)));
    // Not converted with the JVM's zone, nor the declared one: the server reads it in the
    // session's zone, as it reads TIMESTAMP '2026-01-01 00:00:00.123456'.
    assertEquals(WALL_CLOCK, decoded.type(0));
    assertEquals(wallClock, column(decoded, 0));
    assertEquals(null, column(decoded, 1));
    assertEquals(wallClock.plusDays(1), column(decoded, 2));
  }

  @Test
  void datesWithoutAZoneTravelAsDates() throws Exception {
    Date sqlDate = inAuckland(() -> Date.valueOf("2026-03-09"));
    Decoded decoded =
        zoned(
            value(LocalDate.of(2026, 3, 8)), new ParameterEncoder.Value(sqlDate, Types.DATE, null));
    assertEquals(new ArrowType.Date(DateUnit.DAY), decoded.type(0));
    assertEquals(LocalDate.of(2026, 3, 8), column(decoded, 0));
    assertEquals(LocalDate.of(2026, 3, 9), column(decoded, 1));
  }

  /** One Arrow type per column: a date first must not cut the time off a later date-time. */
  @Test
  void datesAndDateTimesInOneBatchKeepTheTime() throws Exception {
    LocalDateTime wallClock = LocalDateTime.of(2026, 3, 8, 13, 14, 15);
    for (boolean dateFirst : new boolean[] {true, false}) {
      ParameterEncoder.Value date = value(LocalDate.of(2026, 3, 9));
      ParameterEncoder.Value dateTime = value(wallClock);
      Decoded decoded = dateFirst ? zoned(date, dateTime) : zoned(dateTime, date);
      assertEquals(WALL_CLOCK, decoded.type(0));
      assertEquals(LocalDateTime.of(2026, 3, 9, 0, 0), column(decoded, dateFirst ? 0 : 1));
      assertEquals(wallClock, column(decoded, dateFirst ? 1 : 0));
    }
  }

  @Test
  void pointsInTimeKeepTheDeclaredTypeAndTheirInstant() throws Exception {
    Instant instant = Instant.parse("2026-01-02T03:04:05.5Z");
    Calendar tokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    Decoded decoded =
        zoned(
            value(instant),
            value(instant.atOffset(java.time.ZoneOffset.ofHours(-7))),
            value(instant.atZone(java.time.ZoneId.of("Europe/Paris"))),
            new ParameterEncoder.Value(Timestamp.from(instant), Types.TIMESTAMP, null),
            new ParameterEncoder.Value(Timestamp.from(instant), Types.TIMESTAMP, tokyo),
            value(java.util.Date.from(instant)),
            value("2026-01-02 12:04:05.5+09:00"),
            value(null));
    assertEquals(ZONED, decoded.type(0));
    for (int row = 0; row < 7; row++) {
      assertEquals(instant, column(decoded, row), "row " + row);
    }
  }

  /** {@code setObject(x, Types.TIMESTAMP)} of a point in time is still that instant here. */
  @Test
  void aPointInTimeConvertedToATimestampIsStillThatInstant() throws Exception {
    Instant instant = Instant.parse("2026-01-02T03:04:05Z");
    Object[] sources = {
      instant,
      instant.atOffset(java.time.ZoneOffset.ofHours(9)),
      Timestamp.from(instant),
      java.util.Date.from(instant)
    };
    ParameterEncoder.Value[] values = new ParameterEncoder.Value[sources.length];
    for (int i = 0; i < sources.length; i++) {
      Object source = sources[i];
      Object wallClock = inAuckland(() -> ParameterEncoder.coerce(source, Types.TIMESTAMP, -1));
      assertTrue(wallClock instanceof LocalDateTime, "TIMESTAMP is a wall clock");
      values[i] = new ParameterEncoder.Value(wallClock, Types.TIMESTAMP, null, source);
    }
    Decoded decoded = zoned(values);
    assertEquals(ZONED, decoded.type(0));
    for (int row = 0; row < sources.length; row++) {
      assertEquals(instant, column(decoded, row), sources[row].getClass().getName());
    }
    // To a TIMESTAMP parameter it is the wall clock it was converted to, as before.
    List<ParameterEncoder.Value[]> rows = new ArrayList<>();
    rows.add(new ParameterEncoder.Value[] {values[0]});
    assertEquals(
        Timestamp.valueOf("2026-01-02 03:04:05"),
        encode(declared(WALL_CLOCK), rows).rows.get(0)[0],
        "an Instant's wall clock is UTC");
  }

  /** A column that mixes points in time with values without a zone, or has text, goes as text. */
  @Test
  void aMixedColumnTravelsAsTextTheServerReads() throws Exception {
    Decoded decoded =
        zoned(
            value(null),
            value(LocalDateTime.of(2026, 1, 1, 0, 0, 0, 500_000_000)),
            value(Instant.parse("2026-01-02T03:04:05.000001Z")),
            value(LocalDate.of(2026, 3, 8)),
            new ParameterEncoder.Value(
                Timestamp.from(Instant.parse("2026-01-02T03:04:05Z")), Types.TIMESTAMP, null),
            value("2026-01-15"),
            value("2026-01-02T12:04:05+09:00"),
            value(LocalDateTime.of(-43, 3, 15, 12, 0)),
            value(Instant.parse("+12345-06-07T08:09:10Z")));
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(0));
    assertEquals(null, column(decoded, 0));
    assertEquals("2026-01-01 00:00:00.5", column(decoded, 1));
    assertEquals("2026-01-02 03:04:05.000001+00:00", column(decoded, 2));
    assertEquals("2026-03-08", column(decoded, 3));
    assertEquals("2026-01-02 03:04:05+00:00", column(decoded, 4));
    assertEquals("2026-01-15", column(decoded, 5), "text without an offset, as it was given");
    assertEquals(
        "2026-01-02 03:04:05+00:00", column(decoded, 6), "text with an offset: its instant");
    assertEquals("-0043-03-15 12:00:00", column(decoded, 7));
    assertEquals("12345-06-07 08:09:10+00:00", column(decoded, 8));
  }

  @Test
  void textWithoutAnOffsetIsLeftToTheServer() throws Exception {
    Decoded decoded = zoned(value("2026-01-01 00:00:00"), value("2026-01-15"), value("tomorrow"));
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(0));
    assertEquals("2026-01-01 00:00:00", column(decoded, 0));
    assertEquals("tomorrow", column(decoded, 2));
  }

  @Test
  void aZonedParameterKeepsItsPrecisionAndRefusesWhatIsNoTimestamp() throws Exception {
    Object[] tooFine = {
      LocalDateTime.of(2026, 1, 1, 0, 0, 0, 1), Instant.parse("2026-01-01T00:00:00.000000001Z")
    };
    for (Object value : tooFine) {
      assertEquals(
          "22008", assertThrows(SQLDataException.class, () -> zoned(value(value))).getSQLState());
      // And in a column sent as text.
      assertEquals(
          "22008",
          assertThrows(SQLDataException.class, () -> zoned(value("2026-01-15"), value(value)))
              .getSQLState());
    }
    for (Object value : new Object[] {Time.valueOf("12:34:56"), LocalTime.of(12, 34), 5, true}) {
      SQLDataException error = assertThrows(SQLDataException.class, () -> zoned(value(value)));
      assertEquals("22018", error.getSQLState());
      assertEquals(
          "Parameter 1: Cannot convert '"
              + value.getClass().getSimpleName()
              + "' to TIMESTAMP WITH TIME ZONE",
          error.getMessage());
    }
  }

  @Test
  void listsOfZonedTimestampsFollowTheSameRule() throws Exception {
    Field zonedList =
        new Field(
            "1",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(Field.nullable("item", ZONED)));
    LocalDateTime wallClock = LocalDateTime.of(2026, 1, 1, 12, 0);
    Instant instant = Instant.parse("2026-01-02T03:04:05Z");
    Decoded wallClocks =
        encode(
            new Schema(List.of(zonedList)),
            List.<ParameterEncoder.Value[]>of(
                new ParameterEncoder.Value[] {value(List.of(wallClock))},
                new ParameterEncoder.Value[] {value(new Object[] {wallClock.plusDays(1), null})}));
    assertEquals(WALL_CLOCK, wallClocks.schema.getFields().get(0).getChildren().get(0).getType());
    assertEquals(List.of(Timestamp.valueOf(wallClock)), wallClocks.rows.get(0)[0]);
    Decoded instants =
        encode(
            new Schema(List.of(zonedList)),
            List.<ParameterEncoder.Value[]>of(
                new ParameterEncoder.Value[] {value(List.of(instant, Timestamp.from(instant)))}));
    assertEquals(ZONED, instants.schema.getFields().get(0).getChildren().get(0).getType());
    assertEquals(
        List.of(Timestamp.from(instant), Timestamp.from(instant)), instants.rows.get(0)[0]);
    Decoded mixed =
        encode(
            new Schema(List.of(zonedList)),
            List.<ParameterEncoder.Value[]>of(
                new ParameterEncoder.Value[] {value(List.of(wallClock))},
                new ParameterEncoder.Value[] {value(List.of(instant))}));
    assertEquals(
        ArrowType.Utf8.INSTANCE, mixed.schema.getFields().get(0).getChildren().get(0).getType());
    assertEquals(List.of("2026-01-01 12:00:00"), mixed.rows.get(0)[0]);
    assertEquals(List.of("2026-01-02 03:04:05+00:00"), mixed.rows.get(1)[0]);
  }

  /** {@code setObject(x, Types.TIMESTAMP_WITH_TIMEZONE)} gives no zone to a value without one. */
  @Test
  void coerceToTimestampWithTimeZoneKeepsValuesWithoutAZone() throws Exception {
    LocalDateTime wallClock = LocalDateTime.of(2026, 10, 17, 1, 2, 3);
    assertEquals(wallClock, ParameterEncoder.coerce(wallClock, Types.TIMESTAMP_WITH_TIMEZONE, -1));
    assertEquals(
        wallClock,
        ParameterEncoder.coerce("2026-10-17 01:02:03", Types.TIMESTAMP_WITH_TIMEZONE, -1));
    assertEquals(
        LocalDate.of(2026, 10, 17),
        ParameterEncoder.coerce(LocalDate.of(2026, 10, 17), Types.TIMESTAMP_WITH_TIMEZONE, -1));
    assertEquals(
        LocalDate.of(2026, 10, 17),
        inAuckland(
            () ->
                ParameterEncoder.coerce(
                    Date.valueOf("2026-10-17"), Types.TIMESTAMP_WITH_TIMEZONE, -1)));
    assertEquals(
        "2026-10-17", ParameterEncoder.coerce("2026-10-17", Types.TIMESTAMP_WITH_TIMEZONE, -1));
    Instant instant = Instant.parse("2026-10-17T00:00:00Z");
    for (Object pointInTime :
        new Object[] {
          instant, Timestamp.from(instant), instant.atOffset(java.time.ZoneOffset.ofHours(5))
        }) {
      assertEquals(
          instant, ParameterEncoder.coerce(pointInTime, Types.TIMESTAMP_WITH_TIMEZONE, -1));
    }
    assertEquals(
        "22018",
        assertThrows(
                SQLDataException.class,
                () -> ParameterEncoder.coerce(LocalTime.NOON, Types.TIMESTAMP_WITH_TIMEZONE, -1))
            .getSQLState());
  }

  // ---- Parameters DuckDB declares in encodings of its own ----

  private static Field opaque(ArrowType storage, String typeName) {
    return Field.nullable("1", new OpaqueType(storage, typeName, "DuckDB"));
  }

  private static Field named(ArrowType storage, String extensionName) {
    return new Field(
        "1",
        new FieldType(true, storage, null, java.util.Map.of("ARROW:extension:name", extensionName)),
        List.of());
  }

  private Decoded column(Field declared, Object... values) throws Exception {
    List<ParameterEncoder.Value[]> rows = new ArrayList<>();
    for (Object value : values) {
      rows.add(new ParameterEncoder.Value[] {value(value)});
    }
    return encode(new Schema(List.of(declared)), rows);
  }

  @Test
  void sixteenByteIntegersAndBignumsGoAsExactText() throws Exception {
    String max = "170141183460469231731687303715884105727";
    for (String type : List.of("hugeint", "uhugeint", "bignum")) {
      ArrowType storage =
          type.equals("bignum") ? ArrowType.Binary.INSTANCE : new ArrowType.FixedSizeBinary(16);
      Decoded decoded =
          column(
              opaque(storage, type),
              new BigInteger(max),
              new BigDecimal("-5"),
              7L,
              (short) 3,
              true,
              " 12345678901234567890 ",
              null,
              new BigDecimal("1E+3"));
      assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(0), type);
      assertEquals(max, decoded.rows.get(0)[0]);
      assertEquals("-5", decoded.rows.get(1)[0]);
      assertEquals("7", decoded.rows.get(2)[0]);
      assertEquals("3", decoded.rows.get(3)[0]);
      assertEquals("1", decoded.rows.get(4)[0]);
      assertEquals("12345678901234567890", decoded.rows.get(5)[0], "text is the server's to read");
      assertEquals(null, decoded.rows.get(6)[0]);
      assertEquals("1000", decoded.rows.get(7)[0]);
      SQLDataException fraction =
          assertThrows(
              SQLDataException.class, () -> column(opaque(storage, type), new BigDecimal("1.5")));
      assertEquals("22003", fraction.getSQLState());
    }
  }

  @Test
  void bitsGoAsText() throws Exception {
    Field bit = opaque(ArrowType.Binary.INSTANCE, "bit");
    Decoded decoded = column(bit, "10101", null, "1");
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(0));
    assertEquals("10101", decoded.rows.get(0)[0]);
    SQLDataException error = assertThrows(SQLDataException.class, () -> column(bit, 5));
    assertEquals("22018", error.getSQLState());
    assertEquals(
        "Parameter 1: Cannot convert 'Integer' to BIT (give the bits as text)", error.getMessage());
  }

  @Test
  void timesWithATimeZone() throws Exception {
    Field timeTz = opaque(new ArrowType.FixedSizeBinary(8), "time_tz");
    Decoded offsets =
        column(
            timeTz,
            OffsetTime.of(13, 14, 0, 0, ZoneOffset.ofHours(2)),
            OffsetTime.of(1, 2, 3, 500_000_000, ZoneOffset.UTC),
            null,
            LocalTime.of(4, 5, 6),
            "07:08:09-03",
            OffsetDateTime.parse("2026-01-02T10:11:12.000001+05:30"));
    assertEquals(ArrowType.Utf8.INSTANCE, offsets.type(0));
    assertEquals("13:14:00+02:00", offsets.rows.get(0)[0], "seconds are always written");
    assertEquals("01:02:03.5+00:00", offsets.rows.get(1)[0]);
    assertEquals(null, offsets.rows.get(2)[0]);
    assertEquals("04:05:06", offsets.rows.get(3)[0], "no offset: the server gives it one");
    assertEquals("07:08:09-03", offsets.rows.get(4)[0], "text as it was given");
    assertEquals("10:11:12.000001+05:30", offsets.rows.get(5)[0]);
    // Times of day alone travel as a time, and the server gives them an offset.
    Decoded plain = column(timeTz, LocalTime.of(4, 5, 6), Time.valueOf("07:08:09"), null);
    assertEquals(new ArrowType.Time(TimeUnit.MICROSECOND, 64), plain.type(0));
    assertEquals(Time.valueOf("04:05:06"), plain.rows.get(0)[0]);
    assertEquals(
        "22008",
        assertThrows(
                SQLDataException.class,
                () -> column(timeTz, OffsetTime.of(1, 2, 3, 1, ZoneOffset.UTC)))
            .getSQLState(),
        "finer than a microsecond");
    assertEquals(
        "22018", assertThrows(SQLDataException.class, () -> column(timeTz, 5)).getSQLState());
    // To a TIME parameter an OffsetTime goes as text too, so the server decides what it means.
    Field time = Field.nullable("1", new ArrowType.Time(TimeUnit.MICROSECOND, 64));
    Decoded mixed = column(time, LocalTime.of(4, 5, 6), OffsetTime.of(1, 2, 3, 0, ZoneOffset.UTC));
    assertEquals(ArrowType.Utf8.INSTANCE, mixed.type(0));
    assertEquals("04:05:06", mixed.rows.get(0)[0]);
    assertEquals("01:02:03+00:00", mixed.rows.get(1)[0]);
    assertEquals(time.getType(), column(time, LocalTime.of(4, 5, 6)).type(0), "as declared");
  }

  /** Servers refuse arrow.bool8 and arrow.json in a parameter stream: their storage is sent. */
  @Test
  void extensionTypesWithoutAClassGoAsWhatTheyStore() throws Exception {
    Decoded booleans =
        column(named(new ArrowType.Int(8, true), "arrow.bool8"), true, false, null, "true", 0);
    Field sent = booleans.schema.getFields().get(0);
    assertEquals(ArrowType.Bool.INSTANCE, sent.getType());
    assertEquals(null, SqlTypes.extensionName(sent));
    assertEquals(true, booleans.rows.get(0)[0]);
    assertEquals(false, booleans.rows.get(1)[0]);
    assertEquals(null, booleans.rows.get(2)[0]);
    assertEquals(true, booleans.rows.get(3)[0]);
    assertEquals(false, booleans.rows.get(4)[0]);
    Decoded json = column(named(ArrowType.Utf8.INSTANCE, "arrow.json"), "{\"a\": 1}");
    assertEquals(ArrowType.Utf8.INSTANCE, json.type(0));
    assertEquals(null, SqlTypes.extensionName(json.schema.getFields().get(0)));
    assertEquals("{\"a\": 1}", json.rows.get(0)[0]);
    // A list of bool8: its elements too.
    Field list =
        new Field(
            "1",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(
                new Field(
                    "item",
                    new FieldType(
                        true,
                        new ArrowType.Int(8, true),
                        null,
                        java.util.Map.of("ARROW:extension:name", "arrow.bool8")),
                    List.of())));
    Decoded lists = column(list, Arrays.asList(true, null, false));
    assertEquals(
        ArrowType.Bool.INSTANCE, lists.schema.getFields().get(0).getChildren().get(0).getType());
    assertEquals(Arrays.asList(true, null, false), lists.rows.get(0)[0]);
  }

  /** What only the server can encode goes in the value's own type, for the server to cast. */
  @Test
  void enumsVariantsAndFixedSizeListsGoInTheValuesOwnType() throws Exception {
    Field dictionary =
        new Field(
            "1",
            new FieldType(
                true,
                new ArrowType.Int(8, false),
                new org.apache.arrow.vector.types.pojo.DictionaryEncoding(1, false, null)),
            List.of());
    Decoded enums = column(dictionary, "ok", null);
    assertEquals(ArrowType.Utf8.INSTANCE, enums.type(0));
    assertEquals(null, enums.schema.getFields().get(0).getDictionary());
    assertEquals("ok", enums.rows.get(0)[0]);
    Field variant =
        new Field(
            "1",
            new FieldType(
                true,
                ArrowType.Struct.INSTANCE,
                null,
                java.util.Map.of("ARROW:extension:name", "arrow.parquet.variant")),
            List.of(
                Field.notNullable("metadata", ArrowType.Binary.INSTANCE),
                Field.nullable("value", ArrowType.Binary.INSTANCE)));
    assertEquals(new ArrowType.Int(32, true), column(variant, 42).type(0));
    assertEquals(ArrowType.Utf8.INSTANCE, column(variant, "x").type(0));
    assertEquals(ArrowType.List.INSTANCE, column(variant, List.of(1, 2)).type(0));
    Field fixed =
        new Field(
            "1",
            FieldType.nullable(new ArrowType.FixedSizeList(3)),
            List.of(Field.nullable("item", new ArrowType.Int(32, true))));
    Decoded arrays = column(fixed, List.of(4, 5, 6));
    assertEquals(ArrowType.List.INSTANCE, arrays.type(0));
    assertEquals(List.of(4, 5, 6), arrays.rows.get(0)[0]);
  }

  // ---- A parameter is the value it had when it was set ----

  @Test
  void whatTheApplicationCanChangeIsCopiedWhenItIsSet() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("2026-01-01 00:00:00.123456789");
    Date date = Date.valueOf("2026-01-01");
    Time time = Time.valueOf("01:00:00");
    java.util.Date instant = new java.util.Date(1_000);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    byte[] bytes = {1, 2};
    int[] ints = {1, 2};
    Object[] objects = {timestamp, new byte[] {3}};
    List<Object> list = new ArrayList<>(List.of(1, new ArrayList<>(List.of(date))));
    StringBuilder text = new StringBuilder("text");
    java.util.concurrent.atomic.AtomicLong number = new java.util.concurrent.atomic.AtomicLong(7);

    Object keptTimestamp = ParameterEncoder.snapshot(timestamp);
    Object keptDate = ParameterEncoder.snapshot(date);
    Object keptTime = ParameterEncoder.snapshot(time);
    Object keptInstant = ParameterEncoder.snapshot(instant);
    Calendar keptCalendar = (Calendar) ParameterEncoder.snapshot(calendar);
    byte[] keptBytes = (byte[]) ParameterEncoder.snapshot(bytes);
    int[] keptInts = (int[]) ParameterEncoder.snapshot(ints);
    Object[] keptObjects = (Object[]) ParameterEncoder.snapshot(objects);
    List<?> keptList = (List<?>) ParameterEncoder.snapshot(list);
    Object keptText = ParameterEncoder.snapshot(text);
    Object keptNumber = ParameterEncoder.snapshot(number);

    timestamp.setTime(0);
    date.setTime(0);
    time.setTime(0);
    instant.setTime(0);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    bytes[0] = 9;
    ints[0] = 9;
    ((byte[]) objects[1])[0] = 9;
    objects[0] = null;
    list.clear();
    text.append(" changed");
    number.set(9);

    // Each keeps its class, so it converts as it did before the copy, and its value.
    assertEquals(Timestamp.valueOf("2026-01-01 00:00:00.123456789"), keptTimestamp);
    assertEquals(Timestamp.class, keptTimestamp.getClass());
    assertEquals(Date.valueOf("2026-01-01"), keptDate);
    assertEquals(Date.class, keptDate.getClass());
    assertEquals(Time.valueOf("01:00:00"), keptTime);
    assertEquals(Time.class, keptTime.getClass());
    assertEquals(new java.util.Date(1_000), keptInstant);
    assertEquals("Asia/Tokyo", keptCalendar.getTimeZone().getID());
    assertArrayEquals(new byte[] {1, 2}, keptBytes);
    assertArrayEquals(new int[] {1, 2}, keptInts);
    assertEquals(Timestamp.valueOf("2026-01-01 00:00:00.123456789"), keptObjects[0]);
    assertArrayEquals(new byte[] {3}, (byte[]) keptObjects[1]);
    assertEquals(List.of(1, List.of(Date.valueOf("2026-01-01"))), keptList);
    assertEquals("text", keptText);
    assertEquals(7L, keptNumber);

    // What cannot change is not copied.
    for (Object same :
        List.of(
            "text",
            1,
            2L,
            new BigDecimal("1.5"),
            BigInteger.TEN,
            UUID.randomUUID(),
            Instant.EPOCH,
            LocalDate.EPOCH,
            LocalDateTime.MIN,
            LocalTime.NOON,
            OffsetDateTime.MAX,
            OffsetTime.MAX,
            Duration.ZERO,
            Period.ZERO,
            new ChargewireInterval(1, 2, 3))) {
      assertTrue(same == ParameterEncoder.snapshot(same), same.getClass().getName());
    }
    assertEquals(null, ParameterEncoder.snapshot(null));
  }

  // ---- A batch as runs of rows that go as they go alone ----

  private static final Field VARIANT =
      new Field(
          "1",
          new FieldType(
              true,
              ArrowType.Struct.INSTANCE,
              null,
              java.util.Map.of("ARROW:extension:name", "arrow.parquet.variant")),
          List.of(
              Field.notNullable("metadata", ArrowType.Binary.INSTANCE),
              Field.nullable("value", ArrowType.Binary.INSTANCE)));

  /** The sizes of the runs a one-column batch of these values is sent in. */
  private static List<Integer> runs(Field declared, Object... values) throws SQLException {
    List<ParameterEncoder.Value[]> rows = new ArrayList<>();
    for (Object value : values) {
      rows.add(
          new ParameterEncoder.Value[] {
            value instanceof ParameterEncoder.Value ? (ParameterEncoder.Value) value : value(value)
          });
    }
    List<Integer> sizes = new ArrayList<>();
    int row = 0;
    for (List<ParameterEncoder.Value[]> run :
        ParameterEncoder.runs(declared == null ? null : new Schema(List.of(declared)), rows)) {
      sizes.add(run.size());
      for (ParameterEncoder.Value[] each : run) {
        assertTrue(each == rows.get(row++), "the rows keep their order");
      }
    }
    assertEquals(rows.size(), row);
    return sizes;
  }

  @Test
  void aBatchForADeclaredTypeIsOneRunWhateverItsRowsHold() throws SQLException {
    assertEquals(
        List.of(5),
        runs(Field.nullable("1", new ArrowType.Decimal(18, 3, 128)), 1, 2L, "3.5", null, 4.5d));
    assertEquals(
        List.of(4), runs(Field.nullable("1", ArrowType.Utf8.INSTANCE), 1, "a", true, null));
    assertEquals(
        List.of(3), runs(opaque(new ArrowType.FixedSizeBinary(16), "hugeint"), 1, "2", 3L));
    assertEquals(List.of(1), runs(VARIANT, 1));
  }

  @Test
  void aVariantBatchIsSplitWhereTheTypeOfItsRowsChanges() throws Exception {
    // 2.3.0 before this: one column of text for all four, so every row was stored as VARCHAR.
    assertEquals(List.of(1, 1, 1, 1), runs(VARIANT, 1, "a", 2L, 1.5d));
    assertEquals(List.of(3, 2), runs(VARIANT, 1, 2, 3, "a", "b"));
    // A NULL has the type of its run, wherever it stands.
    assertEquals(List.of(4, 2), runs(VARIANT, null, 1, null, 2, "a", null));
    // An Integer and a Long are two types to a VARIANT (INT32, INT64), and so are two decimals
    // of different digits.
    assertEquals(List.of(1, 1), runs(VARIANT, 1, 2L));
    assertEquals(
        List.of(2, 1),
        runs(VARIANT, new BigDecimal("1.5"), new BigDecimal("2.5"), new BigDecimal("10.25")));
    assertEquals(List.of(2, 1), runs(VARIANT, List.of(1, 2), List.of(3), List.of("a")));
    // Each run is then one column of its rows' own type.
    assertEquals(new ArrowType.Int(32, true), column(VARIANT, 1, null, 3).type(0));
  }

  @Test
  void aBatchForAnUntypedParameterIsSplitTheSameAndANullIsARowOfItsOwnType() throws SQLException {
    assertEquals(List.of(2, 1, 1), runs(null, 1, 2, "a", new byte[] {1}));
    assertEquals(List.of(3), runs(null, "a", "b", "c"));
    // Alone, a NULL goes as a column of no type, or of its setNull type: so it does in a batch.
    assertEquals(List.of(1, 1, 1), runs(null, 1, null, 2));
    assertEquals(
        List.of(2, 1),
        runs(
            null,
            1,
            new ParameterEncoder.Value(null, Types.INTEGER, null),
            new ParameterEncoder.Value(null, Types.VARCHAR, null)));
  }

  @Test
  void aBatchForAZonedTimestampOrATimeIsSplitWhereTheFormOfItsRowsChanges() throws SQLException {
    Field zoned = Field.nullable("1", ZONED);
    Instant instant = Instant.parse("2026-01-15T12:00:00Z");
    LocalDateTime wallClock = LocalDateTime.of(2026, 1, 15, 12, 0);
    assertEquals(List.of(3), runs(zoned, instant, null, new Timestamp(0)));
    // Text with an offset is a point in time, as an Instant is; text without one is the server's
    // to read.
    assertEquals(
        List.of(2, 2, 2, 1),
        runs(
            zoned,
            instant,
            instant,
            wallClock,
            null,
            "2026-01-15 12:00:00+02",
            instant,
            "2026-01-15 12:00:00"));
    assertEquals(List.of(1, 1), runs(zoned, wallClock, LocalDate.of(2026, 1, 15)));
    Field time = Field.nullable("1", new ArrowType.Time(TimeUnit.MICROSECOND, 64));
    assertEquals(
        List.of(2, 1),
        runs(
            time,
            LocalTime.NOON,
            Time.valueOf("12:00:00"),
            OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC)));
    Field zonedList =
        new Field(
            "1",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(Field.nullable("item", ZONED)));
    assertEquals(List.of(1, 1), runs(zonedList, List.of(instant), List.of(wallClock)));
    Field numbers =
        new Field(
            "1",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(Field.nullable("item", new ArrowType.Int(32, true))));
    assertEquals(List.of(2), runs(numbers, List.of(1), List.of("2", 3L)));
  }

  @Test
  void aListOfUnrelatedTypesForAVariantIsRefusedNotSentAsText() throws Exception {
    SQLException error =
        assertThrows(SQLDataException.class, () -> column(VARIANT, List.of(1, "a")));
    assertEquals("07006", error.getSQLState());
    assertEquals(
        "A VARIANT keeps the type of each value, and values of the types INTEGER and VARCHAR"
            + " cannot be sent in one list: bind the list's JSON text to"
            + " CAST(CAST(? AS JSON) AS VARIANT)",
        error.getMessage());
    // Numbers of two widths have a type that holds both, and so have an untyped parameter's
    // values, which the server converts as it needs them.
    assertEquals(List.of(1L, 2L), column(VARIANT, List.of(1, 2L)).rows.get(0)[0]);
    assertEquals(List.of("1", "a"), one(List.of(1, "a")).rows.get(0)[0]);
  }

  @Test
  void anOffsetTimeWithoutADeclaredTypeIsTextWithItsOffset() throws Exception {
    Decoded decoded = one(OffsetTime.of(13, 14, 0, 0, ZoneOffset.ofHoursMinutes(-5, -30)));
    assertEquals(ArrowType.Utf8.INSTANCE, decoded.type(0));
    assertEquals("13:14:00-05:30", decoded.rows.get(0)[0]);
    assertEquals(
        "13:14:00-05:30",
        ParameterEncoder.jsonValue(
            OffsetTime.of(13, 14, 0, 0, ZoneOffset.ofHoursMinutes(-5, -30))));
    assertEquals(
        OffsetTime.of(10, 11, 12, 0, ZoneOffset.ofHours(2)),
        ParameterEncoder.coerce(
            OffsetDateTime.parse("2026-01-02T10:11:12+02:00"), Types.TIME_WITH_TIMEZONE, -1));
  }

  @Test
  void datesAndTimesFromLegacyTypes() throws Exception {
    Schema schema =
        declared(new ArrowType.Date(DateUnit.DAY), new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    ParameterEncoder.Value[] row = {
      value(Date.valueOf("2026-10-17")), value(Time.valueOf("13:14:15"))
    };
    Object[] values = encode(schema, List.<ParameterEncoder.Value[]>of(row)).rows.get(0);
    assertEquals(Date.valueOf("2026-10-17"), values[0]);
    assertEquals(Time.valueOf("13:14:15"), values[1]);
  }

  @Test
  void coerceFollowsTheTargetJdbcType() throws SQLException {
    assertEquals(new BigDecimal("1.001"), ParameterEncoder.coerce("1.0005", Types.DECIMAL, 3));
    assertEquals(new BigDecimal("1.0005"), ParameterEncoder.coerce("1.0005", Types.NUMERIC, -1));
    assertEquals(12, ParameterEncoder.coerce("12", Types.INTEGER, -1));
    assertEquals("12", ParameterEncoder.coerce(12, Types.VARCHAR, -1));
    assertEquals(true, ParameterEncoder.coerce(1, Types.BOOLEAN, -1));
    assertEquals(LocalDate.of(2026, 10, 17), ParameterEncoder.coerce("2026-10-17", Types.DATE, -1));
    assertEquals(
        Instant.parse("2026-10-17T00:00:00Z"),
        ParameterEncoder.coerce("2026-10-17T00:00:00Z", Types.TIMESTAMP_WITH_TIMEZONE, -1));
    assertEquals(
        "22003",
        assertThrows(SQLDataException.class, () -> ParameterEncoder.coerce(300, Types.TINYINT, -1))
            .getSQLState());
  }

  @Test
  void jsonValuesForServersWithoutPreparedStatements() throws SQLException {
    assertEquals(5, ParameterEncoder.jsonValue(5));
    assertEquals("x", ParameterEncoder.jsonValue('x'));
    assertEquals("2026-10-17", ParameterEncoder.jsonValue(LocalDate.of(2026, 10, 17)));
    assertEquals(
        "2026-10-17 01:02:03",
        ParameterEncoder.jsonValue(Timestamp.valueOf("2026-10-17 01:02:03")));
    assertEquals(
        "2026-10-17T01:02:03Z", ParameterEncoder.jsonValue(Instant.parse("2026-10-17T01:02:03Z")));
    assertEquals("P1D", ParameterEncoder.jsonValue(Period.ofDays(1)));
    assertEquals(
        "0A000",
        assertThrows(SQLException.class, () -> ParameterEncoder.jsonValue(new byte[] {1}))
            .getSQLState());
  }
}
