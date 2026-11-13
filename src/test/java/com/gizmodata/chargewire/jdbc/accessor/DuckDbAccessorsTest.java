package com.gizmodata.chargewire.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.arrow.SqlTypes;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.extension.OpaqueType;
import org.apache.arrow.vector.extension.OpaqueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * The types DuckDB sends in encodings of its own, from bytes a Hardcharger 2.12.0 server sent:
 * BOOLEAN as {@code arrow.bool8}, BIGNUM, BIT and TIME WITH TIME ZONE as {@code arrow.opaque}, and
 * VARIANT as {@code arrow.parquet.variant}.
 */
class DuckDbAccessorsTest extends AccessorFixture {
  private static final HexFormat HEX = HexFormat.of();

  private static Field extension(String name, ArrowType storage, String extensionName) {
    return new Field(
        name,
        new FieldType(
            true,
            storage,
            null,
            Map.of("ARROW:extension:name", extensionName, "ARROW:extension:metadata", "")),
        List.of());
  }

  private OpaqueVector opaque(ArrowType storage, String typeName, String... hexValues) {
    return vector(
        new OpaqueType(storage, typeName, "DuckDB"),
        hexValues.length + 1,
        v -> {
          for (int i = 0; i < hexValues.length; i++) {
            byte[] bytes = HEX.parseHex(hexValues[i]);
            if (v.getUnderlyingVector() instanceof VarBinaryVector) {
              ((VarBinaryVector) v.getUnderlyingVector()).setSafe(i, bytes);
            } else {
              ((FixedSizeBinaryVector) v.getUnderlyingVector()).setSafe(i, bytes);
            }
          }
        });
  }

  @Test
  void bool8IsABoolean() throws SQLException {
    Field field = extension("b", new ArrowType.Int(8, true), "arrow.bool8");
    TinyIntVector vector =
        vector(
            field,
            4,
            v -> {
              v.setSafe(0, 1);
              v.setSafe(1, 0);
              v.setSafe(2, -3);
            });
    Accessor accessor = accessor(vector);
    assertEquals(true, accessor.getObject(0));
    assertEquals(false, accessor.getObject(1));
    assertEquals(true, accessor.getObject(2), "any byte but zero is true");
    assertTrue(accessor.isNull(3));
    assertEquals("true", accessor.getString(0));
    assertEquals("false", accessor.getString(1));
    assertEquals(1, accessor.getInt(0));
    assertEquals(0L, accessor.getLong(1));
    assertEquals(Types.BOOLEAN, SqlTypes.jdbcType(field));
    assertEquals("BOOLEAN", SqlTypes.typeName(field));
    assertEquals(Boolean.class, ColumnClasses.of(field));
    assertEquals(1, SqlTypes.columnSize(field));
    assertFalse(SqlTypes.isSigned(field));
    assertFalse(SqlTypes.isNumeric(field));
    // A plain 8-bit integer, and bool8 over another storage, are not booleans.
    assertEquals(Types.TINYINT, SqlTypes.jdbcType(Field.nullable("t", new ArrowType.Int(8, true))));
    assertFalse(SqlTypes.isBool8(extension("x", new ArrowType.Int(16, true), "arrow.bool8")));
  }

  @Test
  void bool8InsideAListIsABoolean() throws SQLException {
    Field list =
        new Field(
            "l",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(extension("item", new ArrowType.Int(8, true), "arrow.bool8")));
    ListVector vector =
        vector(
            list,
            1,
            v -> {
              TinyIntVector items = (TinyIntVector) v.getDataVector();
              v.startNewValue(0);
              items.setSafe(0, 1);
              items.setSafe(1, 0);
              items.setNull(2);
              v.endValue(0, 3);
              items.setValueCount(3);
            });
    assertEquals(Arrays.asList(true, false, null), accessor(vector).getObject(0));
    assertEquals("[true, false, null]", accessor(vector).getString(0));
    assertEquals("BOOLEAN[]", SqlTypes.typeName(list));
  }

  @Test
  void bignumReadsExactly() throws SQLException {
    // 123456789012345678901234567890123456789012345 and its negative, 0, 1, -1 and 256.
    OpaqueVector vector =
        opaque(
            ArrowType.Binary.INSTANCE,
            "bignum",
            "800013058936e527ad679ec3aa92ef5b7c74f1e2df79",
            "7fffecfa76c91ad85298613c556d10a4838b0e1d2086",
            "80000100",
            "80000101",
            "7ffffefe",
            "8000020100");
    Accessor accessor = accessor(vector);
    BigInteger big = new BigInteger("123456789012345678901234567890123456789012345");
    assertEquals(new BigDecimal(big), accessor.getObject(0));
    assertEquals(new BigDecimal(big.negate()), accessor.getBigDecimal(1));
    assertEquals(big.negate(), accessor.getObject(1, BigInteger.class));
    assertEquals("-123456789012345678901234567890123456789012345", accessor.getString(1));
    assertEquals(BigDecimal.ZERO, accessor.getObject(2));
    assertEquals(1L, accessor.getLong(3));
    assertEquals(-1, accessor.getInt(4));
    assertEquals(256.0, accessor.getDouble(5));
    assertTrue(accessor.isNull(6));
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getLong(0)).getSQLState());
    assertArrayEquals(HEX.parseHex("80000101"), accessor.getBytes(3), "the encoding, as before");
    Field field = vector.getField();
    assertEquals(Types.NUMERIC, SqlTypes.jdbcType(field));
    assertEquals("BIGNUM", SqlTypes.typeName(field));
    assertEquals(BigDecimal.class, ColumnClasses.of(field));
    assertTrue(SqlTypes.isSigned(field));
    assertTrue(SqlTypes.isNumeric(field));
    assertEquals(0, SqlTypes.decimalDigits(field));
  }

  @Test
  void aBignumWhoseHeaderDoesNotMatchItsLengthIsRefused() {
    for (String malformed : List.of("800001", "8000020100ff", "80", "")) {
      OpaqueVector vector = opaque(ArrowType.Binary.INSTANCE, "bignum", malformed);
      SQLException error = assertThrows(SQLException.class, () -> accessor(vector).getObject(0));
      assertEquals("08P01", error.getSQLState(), malformed);
    }
  }

  @Test
  void bitsReadAsText() throws SQLException {
    // '10101', '1', '0', eight bits, nine bits.
    OpaqueVector vector =
        opaque(ArrowType.Binary.INSTANCE, "bit", "03f5", "07ff", "07fe", "00a5", "07ff80");
    Accessor accessor = accessor(vector);
    assertEquals("10101", accessor.getObject(0));
    assertEquals("1", accessor.getString(1));
    assertEquals("0", accessor.getString(2));
    assertEquals("10100101", accessor.getString(3));
    assertEquals("110000000", accessor.getString(4));
    assertTrue(accessor.getBoolean(1));
    assertFalse(accessor.getBoolean(2));
    assertEquals(
        "07006", assertThrows(SQLException.class, () -> accessor.getBoolean(0)).getSQLState());
    assertArrayEquals(HEX.parseHex("03f5"), accessor.getBytes(0), "the encoding, as before");
    Field field = vector.getField();
    assertEquals(Types.OTHER, SqlTypes.jdbcType(field));
    assertEquals("BIT", SqlTypes.typeName(field));
    assertEquals(String.class, ColumnClasses.of(field));
    OpaqueVector malformed = opaque(ArrowType.Binary.INSTANCE, "bit", "09ff");
    assertEquals(
        "08P01",
        assertThrows(SQLException.class, () -> accessor(malformed).getObject(0)).getSQLState());
  }

  @Test
  void timesWithATimeZoneKeepTheirOffset() throws SQLException {
    // 13:14:15.123456+02:00, 23:59:59.999999+15:59:59, 00:00:00-15:59:59, 12:00:00+00.
    OpaqueVector vector =
        opaque(
            new ArrowType.FixedSizeBinary(8),
            "time_tz",
            "dfc400007a77180b",
            "000000ff5fd71d14",
            "fec1010000000000",
            "ffe00000b0eb0e0a");
    Accessor accessor = accessor(vector);
    OffsetTime first = OffsetTime.of(13, 14, 15, 123_456_000, ZoneOffset.ofHours(2));
    assertEquals(first, accessor.getObject(0));
    assertEquals(first, accessor.getObject(0, OffsetTime.class));
    assertEquals(LocalTime.of(13, 14, 15, 123_456_000), accessor.getObject(0, LocalTime.class));
    assertEquals("13:14:15.123456+02:00", accessor.getString(0));
    assertEquals(
        OffsetTime.of(23, 59, 59, 999_999_000, ZoneOffset.ofHoursMinutesSeconds(15, 59, 59)),
        accessor.getObject(1));
    assertEquals(
        OffsetTime.of(0, 0, 0, 0, ZoneOffset.ofHoursMinutesSeconds(-15, -59, -59)),
        accessor.getObject(2));
    assertEquals("00:00:00-15:59:59", accessor.getString(2));
    assertEquals("12:00:00+00:00", accessor.getString(3));
    assertTrue(accessor.isNull(4));
    // getTime: that moment on 1970-01-01, whatever the JVM's zone or a Calendar.
    assertEquals(
        LocalDate.EPOCH.atTime(13, 14, 15, 123_000_000).toInstant(ZoneOffset.ofHours(2)),
        java.time.Instant.ofEpochMilli(accessor.getTime(0, null).getTime()));
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getTimestamp(0, null)).getSQLState());
    Field field = vector.getField();
    assertEquals(Types.TIME_WITH_TIMEZONE, SqlTypes.jdbcType(field));
    assertEquals("TIME WITH TIME ZONE", SqlTypes.typeName(field));
    assertEquals(OffsetTime.class, ColumnClasses.of(field));
    assertEquals(21, SqlTypes.columnSize(field));
    assertEquals(6, SqlTypes.decimalDigits(field));
  }

  private StructVector variant(String metadataBase64, String valueBase64) {
    Field field =
        new Field(
            "v",
            new FieldType(
                true,
                ArrowType.Struct.INSTANCE,
                null,
                Map.of(
                    "ARROW:extension:name",
                    "arrow.parquet.variant",
                    "ARROW:extension:metadata",
                    "")),
            List.of(
                Field.notNullable("metadata", ArrowType.Binary.INSTANCE),
                Field.nullable("value", ArrowType.Binary.INSTANCE)));
    return vector(
        field,
        2,
        v -> {
          ((VarBinaryVector) v.getChild("metadata"))
              .setSafe(0, Base64.getDecoder().decode(metadataBase64));
          ((VarBinaryVector) v.getChild("value"))
              .setSafe(0, Base64.getDecoder().decode(valueBase64));
          v.setIndexDefined(0);
          v.getChild("metadata").setValueCount(2);
          v.getChild("value").setValueCount(2);
        });
  }

  @Test
  void variantsReadAsJavaValues() throws SQLException {
    // {'a': 1, 'b': [true, false], 'c': 'x'}::VARIANT
    StructVector object = variant("AQMAAQIDYWJj", "AgMAAQIABQwOFAEAAAADAgABAgQIBXg=");
    Accessor accessor = accessor(object);
    assertEquals(Map.of("a", 1, "b", List.of(true, false), "c", "x"), accessor.getObject(0));
    assertEquals("{\"a\": 1, \"b\": [true, false], \"c\": \"x\"}", accessor.getString(0));
    assertTrue(accessor.isNull(1));
    Field field = object.getField();
    assertEquals(Types.OTHER, SqlTypes.jdbcType(field));
    assertEquals("VARIANT", SqlTypes.typeName(field));
    assertEquals(Object.class, ColumnClasses.of(field));
    // 42::VARIANT
    Accessor number = accessor(variant("AQAA", "FCoAAAA="));
    assertEquals(42, number.getObject(0));
    assertEquals("42", number.getString(0));
    assertEquals(42L, number.getLong(0));
    assertEquals(new BigDecimal("42"), number.getBigDecimal(0));
    assertEquals(42, number.getObject(0, Integer.class));
  }

  @Test
  void everyVariantPrimitiveDecodes() throws SQLException {
    // Each value as the server sent it (base64), with the SQL that made it.
    byte[] metadata = {1, 0, 0};
    Object[][] cases = {
      {"AA==", null}, // a variant null inside a value
      {"BA==", true}, // true::VARIANT
      {"CA==", false},
      {"DPs=", (byte) -5}, // (-5)::TINYINT::VARIANT
      {"EPv/", (short) -5},
      {"FCoAAAA=", 42},
      {"GP////////9/", Long.MAX_VALUE},
      {"HAAAAAAAAPg/", 1.5}, // 1.5::DOUBLE::VARIANT
      {"IAKWAAAA", new BigDecimal("1.50")}, // DECIMAL(5,2)
      {"JANA4gEAAAAAAA==", new BigDecimal("123.456")}, // DECIMAL(18,3)
      {"KAkVzVsHAAAAAAAAAAAAAAAA", new BigDecimal("0.123456789")}, // DECIMAL(38,9)
      {"LOdPAAA=", LocalDate.of(2026, 1, 2)},
      {"MEDzTPZeRwYA", OffsetDateTime.parse("2026-01-02T03:04:05Z")}, // TIMESTAMPTZ
      {"NEDzTPZeRwYA", LocalDateTime.parse("2026-01-02T03:04:05")}, // TIMESTAMP
      {"OAAAwD8=", 1.5f},
      {"PAIAAABhYg==", new byte[] {'a', 'b'}}, // 'ab'::BLOB::VARIANT
      {"RMB46N0AAAAA", LocalTime.of(1, 2, 3)},
      {"TBXNWwcAAAAA", LocalDateTime.parse("1970-01-01T00:00:00.123456789")}, // TIMESTAMP_NS
      {"UBI+RWfomxLTpFZCZhQXQAA=", UUID.fromString("123e4567-e89b-12d3-a456-426614174000")},
      {"BXg=", "x"}, // a short string
      {
        "QFAAAADDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nDqcOpw6nD"
            + "qcOpw6nDqcOpw6nDqcOpw6nDqQ==",
        "é".repeat(40)
      }, // a string too long for the short form
    };
    for (Object[] c : cases) {
      Object decoded = Variants.decode(metadata, Base64.getDecoder().decode((String) c[0]));
      if (c[1] instanceof byte[]) {
        assertArrayEquals((byte[]) c[1], (byte[]) decoded, (String) c[0]);
      } else {
        assertEquals(c[1], decoded, (String) c[0]);
      }
    }
    SQLException unknown =
        assertThrows(SQLException.class, () -> Variants.decode(metadata, HEX.parseHex("fc")));
    assertEquals("0A000", unknown.getSQLState());
    assertTrue(unknown.getMessage().contains("primitive type 63"), unknown.getMessage());
    assertEquals(
        "08P01",
        assertThrows(SQLException.class, () -> Variants.decode(metadata, HEX.parseHex("142a")))
            .getSQLState(),
        "a value cut short");
    assertEquals(
        "0A000",
        assertThrows(
                SQLException.class, () -> Variants.decode(new byte[] {2, 0, 0}, new byte[] {0}))
            .getSQLState(),
        "another version of the encoding");
  }
}
