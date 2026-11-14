package com.gizmodata.chargewire.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Lists, structs, maps, unions, dictionaries and the null type (ported from the {@code List},
 * {@code Struct}, {@code Map}, {@code Union}, {@code DenseUnion} and {@code Null} accessor tests
 * and {@code ArrowFlightJdbcArrayTest}).
 */
class NestedAccessorsTest extends AccessorFixture {
  private static final ArrowType INT = new ArrowType.Int(32, true);

  private ListVector intLists() {
    return vector(
        field("l", ArrowType.List.INSTANCE, field("item", INT)),
        3,
        v -> {
          ListVector lists = v;
          UnionListWriter writer = lists.getWriter();
          writer.setPosition(0);
          writer.startList();
          writer.integer().writeInt(1);
          writer.writeNull();
          writer.integer().writeInt(3);
          writer.endList();
          writer.setPosition(2);
          writer.startList();
          writer.endList();
          writer.setValueCount(3);
        });
  }

  @Test
  void listsAreJavaLists() throws SQLException {
    Accessor accessor = accessor(intLists());
    assertEquals(Arrays.asList(1, null, 3), accessor.getObject(0));
    assertTrue(accessor.isNull(1));
    assertEquals(List.of(), accessor.getObject(2));
    assertEquals("[1, null, 3]", accessor.getString(0));
    assertEquals(Arrays.asList(1, null, 3), accessor.getObject(0, List.class));
  }

  @Test
  void listsAreJdbcArrays() throws SQLException {
    Array array = accessor(intLists()).getArray(0);
    assertEquals(Types.INTEGER, array.getBaseType());
    assertEquals("INTEGER", array.getBaseTypeName());
    assertArrayEquals(new Object[] {1, null, 3}, (Object[]) array.getArray());
    assertArrayEquals(new Object[] {null, 3}, (Object[]) array.getArray(2, 2));
    assertEquals(
        "2202E", assertThrows(SQLException.class, () -> array.getArray(3, 2)).getSQLState());
    assertEquals("[1, null, 3]", array.toString());
    assertThrows(SQLException.class, () -> array.getArray(Map.of("x", String.class)));
    assertThrows(SQLException.class, array::getResultSet);
    array.free();
    assertEquals("HY010", assertThrows(SQLException.class, array::getArray).getSQLState());
  }

  @Test
  void largeAndFixedSizeLists() throws SQLException {
    LargeListVector large =
        vector(
            field("l", ArrowType.LargeList.INSTANCE, field("item", INT)),
            1,
            v -> {
              LargeListVector lists = v;
              var writer = lists.getWriter();
              writer.setPosition(0);
              writer.startList();
              writer.integer().writeInt(7);
              writer.endList();
              writer.setValueCount(1);
            });
    assertEquals(List.of(7), accessor(large).getObject(0));
    FixedSizeListVector fixed =
        vector(
            field("f", new ArrowType.FixedSizeList(2), field("item", INT)),
            2,
            v -> {
              FixedSizeListVector lists = v;
              IntVector items = (IntVector) lists.getDataVector();
              for (int i = 0; i < 4; i++) {
                items.setSafe(i, i * 10);
              }
              items.setValueCount(4);
              lists.setNotNull(0);
              lists.setNotNull(1);
            });
    assertEquals(List.of(0, 10), accessor(fixed).getObject(0));
    assertEquals(List.of(20, 30), accessor(fixed).getObject(1));
  }

  @Test
  void structsAreOrderedMapsAndJdbcStructs() throws SQLException {
    StructVector structs =
        vector(
            field(
                "s",
                ArrowType.Struct.INSTANCE,
                field("n", INT),
                field("t", ArrowType.Utf8.INSTANCE)),
            2,
            v -> {
              StructVector struct = v;
              ((IntVector) struct.getChild("n")).setSafe(0, 5);
              ((VarCharVector) struct.getChild("t"))
                  .setSafe(0, "a\"b\n".getBytes(StandardCharsets.UTF_8));
              struct.setIndexDefined(0);
              struct.setNull(1);
            });
    Accessor accessor = accessor(structs);
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("n", 5);
    expected.put("t", "a\"b\n");
    assertEquals(expected, accessor.getObject(0));
    assertEquals(
        List.of("n", "t"),
        List.copyOf(((Map<?, ?>) accessor.getObject(0)).keySet()),
        "in field order");
    assertTrue(accessor.isNull(1));
    assertEquals("{\"n\": 5, \"t\": \"a\\\"b\\n\"}", accessor.getString(0), "JSON, escaped");
    Struct struct = accessor.getStruct(0);
    assertEquals("STRUCT", struct.getSQLTypeName());
    assertArrayEquals(new Object[] {5, "a\"b\n"}, struct.getAttributes());
  }

  @Test
  void mapsKeepTheirEntryOrder() throws SQLException {
    MapVector maps =
        vector(
            new Field(
                "m",
                FieldType.nullable(new ArrowType.Map(false)),
                List.of(
                    new Field(
                        "entries",
                        FieldType.notNullable(ArrowType.Struct.INSTANCE),
                        List.of(
                            Field.notNullable("key", ArrowType.Utf8.INSTANCE),
                            Field.nullable("value", INT))))),
            1,
            v -> {
              UnionMapWriter writer = v.getWriter();
              writer.setPosition(0);
              writer.startMap();
              for (String key : new String[] {"z", "a"}) {
                writer.startEntry();
                byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                try (var buffer = allocator.buffer(bytes.length)) {
                  buffer.setBytes(0, bytes);
                  writer.key().varChar().writeVarChar(0, bytes.length, buffer);
                }
                writer.value().integer().writeInt(key.equals("z") ? 26 : 1);
                writer.endEntry();
              }
              writer.endMap();
              writer.setValueCount(1);
            });
    Map<?, ?> map = (Map<?, ?>) accessor(maps).getObject(0);
    assertEquals(List.of("z", "a"), List.copyOf(map.keySet()));
    assertEquals(26, map.get("z"));
    assertEquals("{\"z\": 26, \"a\": 1}", accessor(maps).getString(0));
  }

  @Test
  void sparseUnionsReadTheRowsOwnMember() throws SQLException {
    UnionVector union =
        vector(
            new Field(
                "u",
                FieldType.nullable(
                    new ArrowType.Union(
                        UnionMode.Sparse,
                        new int[] {MinorType.INT.ordinal(), MinorType.VARCHAR.ordinal()})),
                List.of(
                    Field.nullable("int", INT),
                    Field.nullable("varchar", ArrowType.Utf8.INSTANCE))),
            3,
            v -> {
              UnionVector u = v;
              NullableIntHolder holder = new NullableIntHolder();
              holder.isSet = 1;
              holder.value = 42;
              u.setSafe(0, holder);
              u.setType(0, MinorType.INT);
              u.getVarCharVector().setSafe(1, "hi".getBytes(StandardCharsets.UTF_8));
              u.setType(1, MinorType.VARCHAR);
            });
    Accessor accessor = accessor(union);
    assertEquals(42, accessor.getObject(0));
    assertEquals(42L, accessor.getLong(0));
    assertEquals("hi", accessor.getObject(1));
    assertEquals("hi", accessor.getString(1));
    assertTrue(accessor.isNull(2));
  }

  /**
   * A union as a server sends it: type ids of its own (DuckDB's are the members' positions), two
   * members of one Arrow type, and members Arrow Java's {@code UnionVector} cannot look up by type
   * (it needs the precision, the zone or the extension it was not given, and has one slot for all
   * lists and one for all structs).
   */
  @Test
  void aSparseUnionReadsEachMemberByItsDeclaredTypeId() throws SQLException {
    Field declared =
        new Field(
            "u",
            FieldType.nullable(
                new ArrowType.Union(UnionMode.Sparse, new int[] {0, 1, 2, 3, 4, 5, 6, 7})),
            List.of(
                Field.nullable("a", INT),
                Field.nullable("b", INT),
                Field.nullable("d", new ArrowType.Decimal(10, 2, 128)),
                Field.nullable(
                    "t",
                    new ArrowType.Timestamp(
                        org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "Asia/Kolkata")),
                new Field(
                    "l",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    List.of(Field.nullable("item", INT))),
                new Field(
                    "s",
                    FieldType.nullable(ArrowType.Struct.INSTANCE),
                    List.of(Field.nullable("x", INT))),
                Field.nullable("id", org.apache.arrow.vector.extension.UuidType.INSTANCE),
                Field.nullable(
                    "tz",
                    new org.apache.arrow.vector.extension.OpaqueType(
                        new ArrowType.FixedSizeBinary(8), "time_tz", "DuckDB"))));
    java.util.UUID id = java.util.UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    UnionVector union =
        vector(
            declared,
            9,
            v -> {
              UnionVector u = v;
              List<org.apache.arrow.vector.FieldVector> members = u.getChildrenFromFields();
              for (int row = 0; row < 8; row++) {
                u.getTypeBuffer().setByte(row, row);
              }
              // A null of the union: DuckDB gives it the first member's id and a null there.
              u.getTypeBuffer().setByte(8, 0);
              ((IntVector) members.get(0)).setSafe(0, 7);
              ((IntVector) members.get(1)).setSafe(1, 8);
              ((org.apache.arrow.vector.DecimalVector) members.get(2))
                  .setSafe(2, new java.math.BigDecimal("12.34"));
              ((org.apache.arrow.vector.TimeStampMicroTZVector) members.get(3))
                  .setSafe(3, 1_768_478_400_000_000L);
              ListVector list = (ListVector) members.get(4);
              UnionListWriter writer = list.getWriter();
              writer.setPosition(4);
              writer.startList();
              writer.writeInt(1);
              writer.writeInt(2);
              writer.endList();
              StructVector struct = (StructVector) members.get(5);
              struct.setIndexDefined(5);
              ((IntVector) struct.getChild("x")).setSafe(5, 9);
              ((org.apache.arrow.vector.UuidVector) members.get(6)).setSafe(6, id);
              // 13:14:15.123456+02:00 as Hardcharger 2.12.0 sent it.
              ((org.apache.arrow.vector.FixedSizeBinaryVector)
                      ((org.apache.arrow.vector.extension.OpaqueVector) members.get(7))
                          .getUnderlyingVector())
                  .setSafe(7, java.util.HexFormat.of().parseHex("dfc400007a77180b"));
            });
    Accessor accessor = Accessors.create(union, declared, null);
    assertEquals(7, accessor.getObject(0));
    assertEquals(8, accessor.getObject(1));
    assertEquals(new java.math.BigDecimal("12.34"), accessor.getObject(2));
    assertEquals("12.34", accessor.getString(2));
    assertEquals(
        java.time.Instant.parse("2026-01-15T12:00:00Z"),
        ((java.sql.Timestamp) accessor.getObject(3)).toInstant());
    assertEquals("2026-01-15 17:30:00+05:30", accessor.getString(3));
    assertEquals(List.of(1, 2), accessor.getObject(4));
    assertEquals("[1, 2]", accessor.getString(4));
    assertEquals(Map.of("x", 9), accessor.getObject(5));
    assertEquals(id, accessor.getObject(6));
    assertEquals(id.toString(), accessor.getString(6));
    assertEquals(java.time.OffsetTime.parse("13:14:15.123456+02:00"), accessor.getObject(7));
    assertEquals("13:14:15.123456+02:00", accessor.getString(7));
    assertTrue(accessor.isNull(8));
    for (int row = 0; row < 8; row++) {
      assertTrue(!accessor.isNull(row), "row " + row);
    }
  }

  @Test
  void denseUnionsReadThroughTheirOffsets() throws SQLException {
    DenseUnionVector union =
        vector(
            new Field(
                "d",
                FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {0, 1})),
                List.of(Field.nullable("a", INT), Field.nullable("b", ArrowType.Utf8.INSTANCE))),
            2,
            v -> {
              DenseUnionVector d = v;
              IntVector a = (IntVector) d.getVectorByType((byte) 0);
              VarCharVector b = (VarCharVector) d.getVectorByType((byte) 1);
              d.setTypeId(0, (byte) 1);
              b.setSafe(0, "first".getBytes(StandardCharsets.UTF_8));
              b.setValueCount(1);
              d.getOffsetBuffer().setInt(0, 0);
              d.setTypeId(1, (byte) 0);
              a.setSafe(0, 9);
              a.setValueCount(1);
              d.getOffsetBuffer().setInt(4, 0);
            });
    Accessor accessor = accessor(union);
    assertEquals("first", accessor.getObject(0));
    assertEquals(9, accessor.getObject(1));
  }

  @Test
  void dictionaryEncodedColumnsReadTheirEntries() throws SQLException {
    VarCharVector values =
        vector(
            ArrowType.Utf8.INSTANCE,
            2,
            v -> {
              v.setSafe(0, "low".getBytes(StandardCharsets.UTF_8));
              v.setSafe(1, "high".getBytes(StandardCharsets.UTF_8));
            });
    DictionaryEncoding encoding = new DictionaryEncoding(7, false, new ArrowType.Int(8, true));
    TinyIntVector indices =
        vector(
            new Field("level", new FieldType(true, new ArrowType.Int(8, true), encoding), null),
            3,
            v -> {
              v.setSafe(0, (byte) 1);
              v.setSafe(1, (byte) 0);
            });
    DictionaryProvider.MapDictionaryProvider dictionaries =
        new DictionaryProvider.MapDictionaryProvider(new Dictionary(values, encoding));
    Accessor accessor = Accessors.create(indices, dictionaries);
    assertEquals("high", accessor.getObject(0));
    assertEquals("low", accessor.getString(1));
    assertArrayEquals("low".getBytes(StandardCharsets.UTF_8), accessor.getBytes(1));
    assertTrue(accessor.isNull(2));
    assertEquals("22018", assertThrows(SQLException.class, () -> accessor.getInt(0)).getSQLState());
  }

  @Test
  void theNullTypeIsAlwaysNull() throws SQLException {
    NullVector nulls = new NullVector("n", 3);
    Accessor accessor = accessor(nulls);
    assertTrue(accessor.isNull(0));
    assertNull(accessor.getObject(2));
    nulls.close();
  }
}
