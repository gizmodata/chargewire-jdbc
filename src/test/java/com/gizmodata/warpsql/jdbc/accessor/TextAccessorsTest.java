package com.gizmodata.warpsql.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Strings, binaries and UUIDs (ported from the {@code VarChar}, {@code Binary} and {@code Uuid} accessor tests). */
class TextAccessorsTest extends AccessorFixture {
  private Accessor text(String... values) {
    VarCharVector vector = vector(ArrowType.Utf8.INSTANCE, values.length, v -> {
      for (int i = 0; i < values.length; i++) {
        if (values[i] != null) {
          v.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
        }
      }
    });
    return accessor(vector);
  }

  private static String read(Reader reader) throws IOException {
    StringBuilder text = new StringBuilder();
    for (int c; (c = reader.read()) >= 0; ) {
      text.append((char) c);
    }
    return text.toString();
  }

  @Test
  void textAndItsBytes() throws Exception {
    Accessor accessor = text("héllo ✓", null);
    assertEquals("héllo ✓", accessor.getObject(0));
    assertEquals("héllo ✓", accessor.getString(0));
    assertArrayEquals("héllo ✓".getBytes(StandardCharsets.UTF_8), accessor.getBytes(0));
    assertTrue(accessor.isNull(1));
    assertEquals("héllo ✓", read(accessor.getCharacterStream(0)));
    try (InputStream ascii = accessor.getAsciiStream(0)) {
      assertEquals("h?llo ?", new String(ascii.readAllBytes(), StandardCharsets.US_ASCII));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"Utf8", "LargeUtf8", "Utf8View"})
  void everyStringLayout(String layout) throws SQLException {
    byte[] bytes = "a longer value than twelve bytes".getBytes(StandardCharsets.UTF_8);
    Accessor accessor;
    switch (layout) {
      case "LargeUtf8":
        accessor = accessor(vector(ArrowType.LargeUtf8.INSTANCE, 1, v -> ((LargeVarCharVector) v).setSafe(0, bytes)));
        break;
      case "Utf8View":
        accessor = accessor(vector(ArrowType.Utf8View.INSTANCE, 1, v -> ((ViewVarCharVector) v).setSafe(0, bytes)));
        break;
      default:
        accessor = accessor(vector(ArrowType.Utf8.INSTANCE, 1, v -> ((VarCharVector) v).setSafe(0, bytes)));
    }
    assertEquals("a longer value than twelve bytes", accessor.getString(0));
  }

  @Test
  void numbersParseFromText() throws SQLException {
    Accessor accessor = text("127", "-128", " 42 ", "2147483648", "1.5", "1e3", "Infinity", "NaN");
    assertEquals((byte) 127, accessor.getByte(0));
    assertEquals((byte) -128, accessor.getByte(1));
    assertEquals(42, accessor.getInt(2), "surrounding spaces are ignored");
    assertEquals(2147483648L, accessor.getLong(3));
    assertEquals(new BigDecimal("1.5"), accessor.getBigDecimal(4));
    assertEquals(1.5, accessor.getDouble(4));
    assertEquals(1000.0, accessor.getDouble(5));
    assertEquals(Double.POSITIVE_INFINITY, accessor.getDouble(6));
    assertEquals(Float.POSITIVE_INFINITY, accessor.getFloat(6));
    assertTrue(Double.isNaN(accessor.getDouble(7)));
  }

  @Test
  void textThatIsNotANumberRaises22018() {
    Accessor accessor = text("abc", "1.5");
    for (Getter getter :
        new Getter[] {
          a -> a.getByte(0), a -> a.getShort(0), a -> a.getInt(0), a -> a.getLong(0),
          a -> a.getFloat(0), a -> a.getDouble(0), a -> a.getBigDecimal(0), a -> a.getInt(1)
        }) {
      assertEquals("22018", assertThrows(SQLDataException.class, () -> getter.get(accessor)).getSQLState());
    }
  }

  @Test
  void numbersOutOfRangeRaise22003() {
    Accessor accessor = text("128", "-129", "40000", "2147483648");
    assertEquals("22003", assertThrows(SQLDataException.class, () -> accessor.getByte(0)).getSQLState());
    assertEquals("22003", assertThrows(SQLDataException.class, () -> accessor.getByte(1)).getSQLState());
    assertEquals("22003", assertThrows(SQLDataException.class, () -> accessor.getShort(2)).getSQLState());
    assertEquals("22003", assertThrows(SQLDataException.class, () -> accessor.getInt(3)).getSQLState());
  }

  @Test
  void booleansParseFromText() throws SQLException {
    Accessor accessor = text("true", "FALSE", "1", "0", "", "yes");
    assertTrue(accessor.getBoolean(0));
    assertFalse(accessor.getBoolean(1));
    assertTrue(accessor.getBoolean(2));
    assertFalse(accessor.getBoolean(3));
    assertEquals("22018", assertThrows(SQLDataException.class, () -> accessor.getBoolean(4)).getSQLState());
    assertEquals("22018", assertThrows(SQLDataException.class, () -> accessor.getBoolean(5)).getSQLState());
  }

  @Test
  void datesAndTimesParseFromText() throws SQLException {
    Accessor accessor =
        text("2026-10-17", "23:59:58.5", "2026-10-17 01:02:03.000004", "2026-10-17T01:02:03+02:00", "soon");
    assertEquals(LocalDate.of(2026, 10, 17), accessor.getLocalDate(0));
    assertEquals(Date.valueOf("2026-10-17"), accessor.getDate(0, null));
    assertEquals(LocalTime.of(23, 59, 58, 500_000_000), accessor.getLocalTime(1));
    assertEquals(Time.valueOf("23:59:58").getTime() + 500, accessor.getTime(1, null).getTime());
    assertEquals(LocalDateTime.of(2026, 10, 17, 1, 2, 3, 4000), accessor.getLocalDateTime(2));
    assertEquals(Timestamp.valueOf("2026-10-17 01:02:03.000004"), accessor.getTimestamp(2, null));
    assertEquals(OffsetDateTime.parse("2026-10-17T01:02:03+02:00"), accessor.getOffsetDateTime(3));
    Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    assertEquals(
        java.time.Instant.parse("2026-10-17T01:02:03.000004Z"), accessor.getTimestamp(2, utc).toInstant(),
        "wall-clock text is read in the calendar's zone");
    assertEquals("22018", assertThrows(SQLDataException.class, () -> accessor.getLocalDate(4)).getSQLState());
    assertEquals("22018", assertThrows(SQLDataException.class, () -> accessor.getTimestamp(4, null)).getSQLState());
    assertEquals("22018", assertThrows(SQLDataException.class, () -> accessor.getTime(4, null)).getSQLState());
  }

  @Test
  void uuidsParseFromText() throws SQLException {
    UUID uuid = UUID.randomUUID();
    Accessor accessor = text(uuid.toString(), "not-a-uuid");
    assertEquals(uuid, accessor.getObject(0, UUID.class));
    assertEquals("22018", assertThrows(SQLDataException.class, () -> accessor.getObject(1, UUID.class)).getSQLState());
  }

  @Test
  void binaryValues() throws Exception {
    byte[] bytes = "binary ✓".getBytes(StandardCharsets.UTF_8);
    VarBinaryVector binary = vector(ArrowType.Binary.INSTANCE, 2, v -> v.setSafe(0, bytes));
    Accessor accessor = accessor(binary);
    assertArrayEquals(bytes, (byte[]) accessor.getObject(0));
    assertArrayEquals(bytes, accessor.getBytes(0));
    assertEquals("binary ✓", accessor.getString(0), "text is the bytes as UTF-8");
    try (InputStream stream = accessor.getBinaryStream(0)) {
      assertArrayEquals(bytes, stream.readAllBytes());
    }
    assertEquals("binary ✓", read(accessor.getCharacterStream(0)));
    assertTrue(accessor.isNull(1));
    assertEquals("07006", assertThrows(SQLException.class, () -> accessor.getInt(0)).getSQLState());
  }

  @Test
  void everyBinaryLayout() throws SQLException {
    byte[] bytes = {1, 2, 3, 4};
    LargeVarBinaryVector large = vector(ArrowType.LargeBinary.INSTANCE, 1, v -> v.setSafe(0, bytes));
    ViewVarBinaryVector view = vector(ArrowType.BinaryView.INSTANCE, 1, v -> v.setSafe(0, bytes));
    FixedSizeBinaryVector fixed = vector(new ArrowType.FixedSizeBinary(4), 1, v -> v.setSafe(0, bytes));
    assertArrayEquals(bytes, accessor(large).getBytes(0));
    assertArrayEquals(bytes, accessor(view).getBytes(0));
    assertArrayEquals(bytes, accessor(fixed).getBytes(0));
  }

  @Test
  void uuidExtension() throws SQLException {
    UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    UuidVector vector = vector(new Field("u", FieldType.nullable(UuidType.INSTANCE), null), 2, v -> v.setSafe(0, uuid));
    Accessor accessor = accessor(vector);
    assertEquals(uuid, accessor.getObject(0));
    assertEquals("123e4567-e89b-12d3-a456-426614174000", accessor.getString(0));
    assertEquals(16, accessor.getBytes(0).length);
    assertEquals(0x12, accessor.getBytes(0)[0], "big-endian");
    assertTrue(accessor.isNull(1));
    assertEquals(uuid, accessor.getObject(0, UUID.class));
  }

  /** A getter under test. */
  @FunctionalInterface
  private interface Getter {
    Object get(Accessor accessor) throws SQLException;
  }
}
