package com.gizmodata.warpsql.jdbc.arrow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpInterval;
import com.gizmodata.warpsql.jdbc.accessor.Accessor;
import com.gizmodata.warpsql.jdbc.accessor.Accessors;
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
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
 * tests, plus the inference Warp SQL needs because servers may not know a parameter's type).
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
    assertEquals(new WarpInterval(0, 0, 90_000_000_000L), row[15]);
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
