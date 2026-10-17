package com.gizmodata.warpsql.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.sql.SQLException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

/**
 * Integers, floating point, decimals and booleans (ported from the {@code BaseIntVector}, {@code
 * Bit}, {@code Decimal}, {@code Float4} and {@code Float8} accessor tests). Unlike Avatica's
 * accessors, a value that does not fit the requested type raises {@code 22003} instead of wrapping
 * around.
 */
class NumberAccessorsTest extends AccessorFixture {
  private static final ArrowType DOUBLE =
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  @Test
  void signedIntegersOfEveryWidth() throws SQLException {
    TinyIntVector tiny = vector(new ArrowType.Int(8, true), 2, v -> v.setSafe(0, (byte) -128));
    SmallIntVector small =
        vector(new ArrowType.Int(16, true), 1, v -> v.setSafe(0, (short) 0xAABB));
    IntVector integer =
        vector(new ArrowType.Int(32, true), 1, v -> v.setSafe(0, Integer.MIN_VALUE));
    BigIntVector big =
        vector(new ArrowType.Int(64, true), 1, v -> v.setSafe(0, 0xAABBCCDDEEFFAABBL));
    assertEquals((byte) -128, accessor(tiny).getObject(0));
    assertTrue(accessor(tiny).isNull(1));
    assertEquals((short) 0xAABB, accessor(small).getObject(0));
    assertEquals(Integer.MIN_VALUE, accessor(integer).getObject(0));
    assertEquals(0xAABBCCDDEEFFAABBL, accessor(big).getObject(0));
    assertEquals("-128", accessor(tiny).getString(0));
    assertEquals(Long.toString(0xAABBCCDDEEFFAABBL), accessor(big).getString(0));
  }

  @Test
  void integersConvertToEveryNumericType() throws SQLException {
    IntVector integer =
        vector(
            new ArrowType.Int(32, true),
            2,
            v -> {
              v.setSafe(0, 100);
              v.setSafe(1, 0);
            });
    Accessor accessor = accessor(integer);
    assertEquals((byte) 100, accessor.getByte(0));
    assertEquals((short) 100, accessor.getShort(0));
    assertEquals(100, accessor.getInt(0));
    assertEquals(100L, accessor.getLong(0));
    assertEquals(100f, accessor.getFloat(0));
    assertEquals(100.0, accessor.getDouble(0));
    assertEquals(BigDecimal.valueOf(100), accessor.getBigDecimal(0));
    assertTrue(accessor.getBoolean(0));
    assertFalse(accessor.getBoolean(1));
    assertEquals(BigInteger.valueOf(100), accessor.getObject(0, BigInteger.class));
    assertEquals(100L, accessor.getObject(0, Long.class));
  }

  @Test
  void narrowingOutOfRangeRaises22003() {
    IntVector integer =
        vector(
            new ArrowType.Int(32, true),
            2,
            v -> {
              v.setSafe(0, 300);
              v.setSafe(1, -40000);
            });
    Accessor accessor = accessor(integer);
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getByte(0)).getSQLState());
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getShort(1)).getSQLState());
  }

  @Test
  void unsignedIntegersWidenSoValuesStayExact() throws SQLException {
    UInt1Vector u1 = vector(new ArrowType.Int(8, false), 1, v -> v.setSafe(0, 0xFF));
    UInt2Vector u2 = vector(new ArrowType.Int(16, false), 1, v -> v.setSafe(0, 0xFFFF));
    UInt4Vector u4 = vector(new ArrowType.Int(32, false), 1, v -> v.setSafe(0, 0x80000001));
    UInt8Vector u8 =
        vector(
            new ArrowType.Int(64, false),
            2,
            v -> {
              v.setSafe(0, -1L);
              v.setSafe(1, 7L);
            });
    assertEquals((short) 255, accessor(u1).getObject(0));
    assertEquals(65535, accessor(u2).getObject(0));
    assertEquals(0x80000001L, accessor(u4).getObject(0));
    assertEquals(new BigDecimal("18446744073709551615"), accessor(u8).getObject(0));
    assertEquals("18446744073709551615", accessor(u8).getString(0));
    assertEquals(1.8446744073709552E19, accessor(u8).getDouble(0));
    assertEquals(new BigDecimal("18446744073709551615"), accessor(u8).getBigDecimal(0));
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor(u8).getLong(0)).getSQLState());
    assertEquals(7L, accessor(u8).getLong(1));
    assertEquals(new BigDecimal(7), accessor(u8).getObject(1));
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor(u4).getInt(0)).getSQLState());
  }

  @Test
  void integersHaveNoBytesDatesOrTimes() {
    Accessor accessor =
        accessor(vector(new ArrowType.Int(32, true), 1, v -> ((IntVector) v).setSafe(0, 1)));
    assertEquals(
        "07006", assertThrows(SQLException.class, () -> accessor.getBytes(0)).getSQLState());
    assertEquals(
        "07006", assertThrows(SQLException.class, () -> accessor.getDate(0, null)).getSQLState());
    assertEquals(
        "07006",
        assertThrows(SQLException.class, () -> accessor.getTimestamp(0, null)).getSQLState());
  }

  @Test
  void doubles() throws SQLException {
    Float8Vector doubles =
        vector(
            DOUBLE,
            5,
            v -> {
              v.setSafe(0, -1.75);
              v.setSafe(1, Double.NaN);
              v.setSafe(2, Double.POSITIVE_INFINITY);
              v.setSafe(3, 1e300);
              v.setSafe(4, 0.1);
            });
    Accessor accessor = accessor(doubles);
    assertEquals(-1.75, accessor.getObject(0));
    assertEquals("-1.75", accessor.getString(0));
    assertEquals(-1L, accessor.getLong(0), "truncated toward zero");
    assertEquals(-1, accessor.getInt(0));
    assertEquals(new BigDecimal("-1.75"), accessor.getBigDecimal(0));
    assertEquals(
        new BigDecimal("0.1"),
        accessor.getBigDecimal(4),
        "the shortest decimal, not 0.1000000000000000055...");
    assertTrue(accessor.getBoolean(0));
    assertEquals("NaN", accessor.getString(1));
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getLong(1)).getSQLState());
    assertEquals(
        "22003",
        assertThrows(SQLDataException.class, () -> accessor.getBigDecimal(2)).getSQLState());
    assertEquals(Float.POSITIVE_INFINITY, accessor.getFloat(2), "infinity stays infinity");
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getFloat(3)).getSQLState());
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getLong(3)).getSQLState());
  }

  @Test
  void floatsAndHalfFloats() throws SQLException {
    Float4Vector floats =
        vector(
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1, v -> v.setSafe(0, 1.1f));
    assertEquals(1.1f, accessor(floats).getObject(0));
    assertEquals(1.1f, accessor(floats).getFloat(0));
    assertEquals("1.1", accessor(floats).getString(0));
    assertEquals(
        new BigDecimal("1.1"), accessor(floats).getBigDecimal(0), "from the float's own digits");
    Float2Vector halves =
        vector(
            new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
            1,
            v -> v.setSafeWithPossibleTruncate(0, 2.5f));
    assertEquals(2.5f, accessor(halves).getObject(0));
    assertEquals(2.5, accessor(halves).getDouble(0));
  }

  @Test
  void decimals() throws SQLException {
    DecimalVector decimals =
        vector(
            new ArrowType.Decimal(10, 3, 128),
            3,
            v -> {
              v.setSafe(0, new BigDecimal("-12345.678"));
              v.setSafe(1, new BigDecimal("0.000"));
              v.setSafe(2, new BigDecimal("9999999.999"));
            });
    Accessor accessor = accessor(decimals);
    assertEquals(new BigDecimal("-12345.678"), accessor.getObject(0));
    assertEquals("-12345.678", accessor.getString(0));
    assertEquals(-12345L, accessor.getLong(0));
    assertEquals(-12345, accessor.getInt(0));
    assertEquals((short) -12345, accessor.getShort(0));
    assertEquals(
        "22003", assertThrows(SQLDataException.class, () -> accessor.getByte(0)).getSQLState());
    assertEquals(-12345.678, accessor.getDouble(0));
    assertTrue(accessor.getBoolean(0));
    assertFalse(accessor.getBoolean(1));
    assertEquals("0.000", accessor.getString(1));
    assertEquals(9999999.999f, accessor.getFloat(2));
  }

  @Test
  void decimal256() throws SQLException {
    BigDecimal value = new BigDecimal("123456789012345678901234567890123456789012345.6789");
    Decimal256Vector decimals =
        vector(new ArrowType.Decimal(76, 4, 256), 1, v -> v.setSafe(0, value));
    assertEquals(value, accessor(decimals).getObject(0));
    assertEquals(value.toPlainString(), accessor(decimals).getString(0));
    assertEquals(
        "22003",
        assertThrows(SQLDataException.class, () -> accessor(decimals).getLong(0)).getSQLState());
  }

  @Test
  void booleans() throws SQLException {
    BitVector bits =
        vector(
            ArrowType.Bool.INSTANCE,
            3,
            v -> {
              v.setSafe(0, 1);
              v.setSafe(1, 0);
            });
    Accessor accessor = accessor(bits);
    assertEquals(true, accessor.getObject(0));
    assertEquals(false, accessor.getObject(1));
    assertTrue(accessor.isNull(2));
    assertEquals("true", accessor.getString(0));
    assertEquals(1, accessor.getInt(0));
    assertEquals(0L, accessor.getLong(1));
    assertEquals((byte) 1, accessor.getByte(0));
    assertEquals(1.0, accessor.getDouble(0));
    assertEquals(BigDecimal.ONE, accessor.getBigDecimal(0));
    assertEquals(BigDecimal.ZERO, accessor.getBigDecimal(1));
    assertEquals(Boolean.TRUE, accessor.getObject(0, Boolean.class));
  }
}
