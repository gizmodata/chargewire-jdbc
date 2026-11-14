package com.gizmodata.chargewire.jdbc.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/** Arrow to JDBC type mapping (ported from {@code SqlTypesTest}). */
class SqlTypesTest {
  @Test
  void jdbcTypes() {
    assertEquals(Types.TINYINT, SqlTypes.jdbcType(new ArrowType.Int(8, true)));
    assertEquals(Types.SMALLINT, SqlTypes.jdbcType(new ArrowType.Int(16, true)));
    assertEquals(Types.INTEGER, SqlTypes.jdbcType(new ArrowType.Int(32, true)));
    assertEquals(Types.BIGINT, SqlTypes.jdbcType(new ArrowType.Int(64, true)));
    assertEquals(Types.SMALLINT, SqlTypes.jdbcType(new ArrowType.Int(8, false)));
    assertEquals(Types.INTEGER, SqlTypes.jdbcType(new ArrowType.Int(16, false)));
    assertEquals(Types.BIGINT, SqlTypes.jdbcType(new ArrowType.Int(32, false)));
    assertEquals(Types.NUMERIC, SqlTypes.jdbcType(new ArrowType.Int(64, false)));
    assertEquals(
        Types.REAL, SqlTypes.jdbcType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    assertEquals(
        Types.REAL, SqlTypes.jdbcType(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
    assertEquals(
        Types.DOUBLE,
        SqlTypes.jdbcType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertEquals(Types.BOOLEAN, SqlTypes.jdbcType(ArrowType.Bool.INSTANCE));
    assertEquals(Types.VARCHAR, SqlTypes.jdbcType(ArrowType.Utf8.INSTANCE));
    assertEquals(Types.VARCHAR, SqlTypes.jdbcType(ArrowType.Utf8View.INSTANCE));
    assertEquals(Types.LONGVARCHAR, SqlTypes.jdbcType(ArrowType.LargeUtf8.INSTANCE));
    assertEquals(Types.VARBINARY, SqlTypes.jdbcType(ArrowType.Binary.INSTANCE));
    assertEquals(Types.VARBINARY, SqlTypes.jdbcType(ArrowType.BinaryView.INSTANCE));
    assertEquals(Types.LONGVARBINARY, SqlTypes.jdbcType(ArrowType.LargeBinary.INSTANCE));
    assertEquals(Types.BINARY, SqlTypes.jdbcType(new ArrowType.FixedSizeBinary(4)));
    assertEquals(Types.DECIMAL, SqlTypes.jdbcType(new ArrowType.Decimal(10, 2, 128)));
    assertEquals(Types.DATE, SqlTypes.jdbcType(new ArrowType.Date(DateUnit.DAY)));
    assertEquals(Types.TIME, SqlTypes.jdbcType(new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
    assertEquals(
        Types.TIMESTAMP, SqlTypes.jdbcType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    assertEquals(
        Types.TIMESTAMP_WITH_TIMEZONE,
        SqlTypes.jdbcType(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")));
    assertEquals(
        Types.OTHER, SqlTypes.jdbcType(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)));
    assertEquals(Types.OTHER, SqlTypes.jdbcType(new ArrowType.Duration(TimeUnit.SECOND)));
    assertEquals(Types.ARRAY, SqlTypes.jdbcType(ArrowType.List.INSTANCE));
    assertEquals(Types.ARRAY, SqlTypes.jdbcType(new ArrowType.FixedSizeList(2)));
    assertEquals(Types.ARRAY, SqlTypes.jdbcType(ArrowType.LargeList.INSTANCE));
    assertEquals(Types.STRUCT, SqlTypes.jdbcType(ArrowType.Struct.INSTANCE));
    assertEquals(Types.JAVA_OBJECT, SqlTypes.jdbcType(new ArrowType.Map(false)));
    assertEquals(
        Types.JAVA_OBJECT, SqlTypes.jdbcType(new ArrowType.Union(UnionMode.Sparse, new int[0])));
    assertEquals(Types.NULL, SqlTypes.jdbcType(ArrowType.Null.INSTANCE));
    assertEquals(Types.OTHER, SqlTypes.jdbcType(UuidType.INSTANCE));
  }

  @Test
  void typeNamesPreferTheServers() {
    Field named =
        new Field(
            "c",
            new FieldType(
                true, new ArrowType.Int(32, true), null, Map.of(SqlTypes.TYPE_NAME, "INT4")),
            null);
    assertEquals("INT4", SqlTypes.typeName(named));
    assertEquals("INTEGER", SqlTypes.typeName(Field.nullable("c", new ArrowType.Int(32, true))));
    assertEquals("UBIGINT", SqlTypes.typeName(Field.nullable("c", new ArrowType.Int(64, false))));
    assertEquals(
        "DECIMAL(10,2)", SqlTypes.typeName(Field.nullable("c", new ArrowType.Decimal(10, 2, 128))));
    assertEquals(
        "TIMESTAMP WITH TIME ZONE",
        SqlTypes.typeName(Field.nullable("c", new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"))));
    assertEquals(
        "VARCHAR[]",
        SqlTypes.typeName(
            new Field(
                "l",
                FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(Field.nullable("item", ArrowType.Utf8.INSTANCE)))));
    assertEquals(
        "UUID", SqlTypes.typeName(new Field("u", FieldType.nullable(UuidType.INSTANCE), null)));
  }

  @Test
  void columnSizesAndDigits() {
    assertEquals(3, SqlTypes.columnSize(new ArrowType.Int(8, true)));
    assertEquals(5, SqlTypes.columnSize(new ArrowType.Int(16, true)));
    assertEquals(10, SqlTypes.columnSize(new ArrowType.Int(32, true)));
    assertEquals(19, SqlTypes.columnSize(new ArrowType.Int(64, true)));
    assertEquals(20, SqlTypes.columnSize(new ArrowType.Int(64, false)));
    assertEquals(12, SqlTypes.columnSize(new ArrowType.Decimal(12, 3, 128)));
    assertEquals(3, SqlTypes.decimalDigits(new ArrowType.Decimal(12, 3, 128)));
    assertEquals(10, SqlTypes.columnSize(new ArrowType.Date(DateUnit.DAY)));
    assertEquals(8, SqlTypes.columnSize(new ArrowType.Time(TimeUnit.SECOND, 32)));
    assertEquals(15, SqlTypes.columnSize(new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
    assertEquals(29, SqlTypes.columnSize(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));
    assertEquals(32, SqlTypes.columnSize(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")));
    assertEquals(9, SqlTypes.decimalDigits(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));
    assertEquals(0, SqlTypes.decimalDigits(new ArrowType.Time(TimeUnit.SECOND, 32)));
    assertEquals(SqlTypes.UNBOUNDED, SqlTypes.columnSize(ArrowType.Utf8.INSTANCE));
    assertEquals(16, SqlTypes.columnSize(new ArrowType.FixedSizeBinary(16)));
    assertEquals(36, SqlTypes.columnSize(UuidType.INSTANCE));
    assertNull(SqlTypes.columnSize(ArrowType.Struct.INSTANCE));
    assertNull(SqlTypes.decimalDigits(ArrowType.Utf8.INSTANCE));
  }

  @Test
  void signedNumericAndText() {
    assertTrue(SqlTypes.isSigned(new ArrowType.Int(32, true)));
    assertFalse(SqlTypes.isSigned(new ArrowType.Int(32, false)));
    assertTrue(SqlTypes.isSigned(new ArrowType.Decimal(5, 0, 128)));
    assertFalse(SqlTypes.isSigned(ArrowType.Utf8.INSTANCE));
    assertTrue(SqlTypes.isNumeric(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertFalse(SqlTypes.isNumeric(ArrowType.Bool.INSTANCE));
    assertTrue(SqlTypes.isText(ArrowType.LargeUtf8.INSTANCE));
  }

  @Test
  void typedNullHints() {
    assertEquals(new ArrowType.Int(32, true), SqlTypes.arrowType(Types.INTEGER));
    assertEquals(ArrowType.Utf8.INSTANCE, SqlTypes.arrowType(Types.NVARCHAR));
    assertEquals(ArrowType.Binary.INSTANCE, SqlTypes.arrowType(Types.BLOB));
    assertEquals(
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
        SqlTypes.arrowType(Types.TIMESTAMP_WITH_TIMEZONE));
    assertNull(
        SqlTypes.arrowType(Types.DECIMAL),
        "a decimal needs its precision; let the server infer it");
    assertNull(SqlTypes.arrowType(Types.OTHER));
  }

  /** A table's column as Hardcharger 2.12.0's catalog gives it: its storage, and its name. */
  private static Field catalog(String typeName, ArrowType storage, Field... children) {
    return new Field(
        "c",
        new FieldType(
            true,
            storage,
            null,
            Map.of(SqlTypes.TYPE_NAME, typeName, "ARROW:FLIGHT:SQL:TABLE_NAME", "t")),
        List.of(children));
  }

  @Test
  void aCatalogColumnIsDescribedAsAResultCarriesIt() {
    ArrowType bytes16 = new ArrowType.FixedSizeBinary(16);
    ArrowType bytes8 = new ArrowType.FixedSizeBinary(8);
    ArrowType binary = ArrowType.Binary.INSTANCE;
    for (String name : List.of("HUGEINT", "UHUGEINT")) {
      Field field = SqlTypes.catalogField(catalog(name, bytes16), true);
      assertEquals(Types.NUMERIC, SqlTypes.jdbcType(field), name);
      assertEquals(name, SqlTypes.typeName(field));
      assertEquals(39, SqlTypes.columnSize(field), name);
      assertEquals(0, SqlTypes.decimalDigits(field), name);
      assertTrue(SqlTypes.isNumeric(field), name);
    }
    Field time = SqlTypes.catalogField(catalog("TIME WITH TIME ZONE", bytes8), true);
    assertEquals(Types.TIME_WITH_TIMEZONE, SqlTypes.jdbcType(time));
    assertEquals("TIME WITH TIME ZONE", SqlTypes.typeName(time));
    assertEquals(21, SqlTypes.columnSize(time));
    assertEquals(6, SqlTypes.decimalDigits(time));
    Field bignum = SqlTypes.catalogField(catalog("BIGNUM", binary), false);
    assertEquals(Types.NUMERIC, SqlTypes.jdbcType(bignum));
    assertEquals("BIGNUM", SqlTypes.typeName(bignum));
    Field bit = SqlTypes.catalogField(catalog("BIT", binary), true);
    assertEquals(Types.OTHER, SqlTypes.jdbcType(bit));
    assertEquals("BIT", SqlTypes.typeName(bit));
    Field variant =
        SqlTypes.catalogField(
            catalog(
                "VARIANT",
                ArrowType.Struct.INSTANCE,
                Field.notNullable("metadata", binary),
                Field.nullable("value", binary)),
            false);
    assertEquals(Types.OTHER, SqlTypes.jdbcType(variant));
    assertEquals("VARIANT", SqlTypes.typeName(variant));

    // Without the exact types a BIT arrives as its bytes, and is described as them.
    Field plainBit = catalog("BIT", binary);
    assertTrue(plainBit == SqlTypes.catalogField(plainBit, false));
    assertEquals(Types.VARBINARY, SqlTypes.jdbcType(plainBit));
    // The name alone decides nothing: a column of another storage is left as it is.
    for (Field other :
        List.of(
            catalog("HUGEINT", new ArrowType.Decimal(38, 0, 128)),
            catalog("TIME WITH TIME ZONE", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
            catalog("BLOB", binary),
            catalog("UUID", bytes16),
            catalog("VARIANT", ArrowType.Struct.INSTANCE, Field.nullable("x", binary)),
            Field.nullable("no_name", bytes16))) {
      assertTrue(other == SqlTypes.catalogField(other, true), other.toString());
    }
  }
}
