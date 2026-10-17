package com.gizmodata.warpsql.jdbc.accessor;

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
 * {@code Struct}, {@code Map}, {@code Union}, {@code DenseUnion} and {@code Null} accessor tests and
 * {@code ArrowFlightJdbcArrayTest}).
 */
class NestedAccessorsTest extends AccessorFixture {
  private static final ArrowType INT = new ArrowType.Int(32, true);

  private ListVector intLists() {
    return vector(field("l", ArrowType.List.INSTANCE, field("item", INT)), 3, v -> {
      ListVector lists = (ListVector) v;
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
    assertEquals("2202E", assertThrows(SQLException.class, () -> array.getArray(3, 2)).getSQLState());
    assertEquals("[1, null, 3]", array.toString());
    assertThrows(SQLException.class, () -> array.getArray(Map.of("x", String.class)));
    assertThrows(SQLException.class, array::getResultSet);
    array.free();
    assertEquals("HY010", assertThrows(SQLException.class, array::getArray).getSQLState());
  }

  @Test
  void largeAndFixedSizeLists() throws SQLException {
    LargeListVector large = vector(field("l", ArrowType.LargeList.INSTANCE, field("item", INT)), 1, v -> {
      LargeListVector lists = (LargeListVector) v;
      var writer = lists.getWriter();
      writer.setPosition(0);
      writer.startList();
      writer.integer().writeInt(7);
      writer.endList();
      writer.setValueCount(1);
    });
    assertEquals(List.of(7), accessor(large).getObject(0));
    FixedSizeListVector fixed = vector(field("f", new ArrowType.FixedSizeList(2), field("item", INT)), 2, v -> {
      FixedSizeListVector lists = (FixedSizeListVector) v;
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
    StructVector structs = vector(
        field("s", ArrowType.Struct.INSTANCE, field("n", INT), field("t", ArrowType.Utf8.INSTANCE)), 2, v -> {
          StructVector struct = (StructVector) v;
          ((IntVector) struct.getChild("n")).setSafe(0, 5);
          ((VarCharVector) struct.getChild("t")).setSafe(0, "a\"b\n".getBytes(StandardCharsets.UTF_8));
          struct.setIndexDefined(0);
          struct.setNull(1);
        });
    Accessor accessor = accessor(structs);
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("n", 5);
    expected.put("t", "a\"b\n");
    assertEquals(expected, accessor.getObject(0));
    assertEquals(List.of("n", "t"), List.copyOf(((Map<?, ?>) accessor.getObject(0)).keySet()), "in field order");
    assertTrue(accessor.isNull(1));
    assertEquals("{\"n\": 5, \"t\": \"a\\\"b\\n\"}", accessor.getString(0), "JSON, escaped");
    Struct struct = accessor.getStruct(0);
    assertEquals("STRUCT", struct.getSQLTypeName());
    assertArrayEquals(new Object[] {5, "a\"b\n"}, struct.getAttributes());
  }

  @Test
  void mapsKeepTheirEntryOrder() throws SQLException {
    MapVector maps = vector(
        new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(
            new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                Field.notNullable("key", ArrowType.Utf8.INSTANCE), Field.nullable("value", INT))))),
        1,
        v -> {
          UnionMapWriter writer = ((MapVector) v).getWriter();
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
    UnionVector union = vector(new Field("u", FieldType.nullable(
        new ArrowType.Union(UnionMode.Sparse, new int[] {MinorType.INT.ordinal(), MinorType.VARCHAR.ordinal()})),
        List.of(Field.nullable("int", INT), Field.nullable("varchar", ArrowType.Utf8.INSTANCE))), 3, v -> {
          UnionVector u = (UnionVector) v;
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

  @Test
  void denseUnionsReadThroughTheirOffsets() throws SQLException {
    DenseUnionVector union = vector(new Field("d", FieldType.nullable(
        new ArrowType.Union(UnionMode.Dense, new int[] {0, 1})),
        List.of(Field.nullable("a", INT), Field.nullable("b", ArrowType.Utf8.INSTANCE))), 2, v -> {
          DenseUnionVector d = (DenseUnionVector) v;
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
    VarCharVector values = vector(ArrowType.Utf8.INSTANCE, 2, v -> {
      v.setSafe(0, "low".getBytes(StandardCharsets.UTF_8));
      v.setSafe(1, "high".getBytes(StandardCharsets.UTF_8));
    });
    DictionaryEncoding encoding = new DictionaryEncoding(7, false, new ArrowType.Int(8, true));
    TinyIntVector indices = vector(
        new Field("level", new FieldType(true, new ArrowType.Int(8, true), encoding), null), 3, v -> {
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
