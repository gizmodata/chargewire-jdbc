package com.gizmodata.chargewire.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Union and dictionary accessors answer every getter exactly as the value's own accessor would,
 * value or error (ported from {@code AbstractArrowFlightJdbcUnionVectorAccessorTest}).
 */
class DelegatingAccessorsTest extends AccessorFixture {
  private static final Calendar TOKYO = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));

  /** Every getter, as a name and a call. */
  private static final Map<String, Getter> GETTERS =
      Map.ofEntries(
          Map.entry("getObject", (a, r) -> a.getObject(r)),
          Map.entry("getString", (a, r) -> a.getString(r)),
          Map.entry("getBoolean", (a, r) -> a.getBoolean(r)),
          Map.entry("getByte", (a, r) -> a.getByte(r)),
          Map.entry("getShort", (a, r) -> a.getShort(r)),
          Map.entry("getInt", (a, r) -> a.getInt(r)),
          Map.entry("getLong", (a, r) -> a.getLong(r)),
          Map.entry("getFloat", (a, r) -> a.getFloat(r)),
          Map.entry("getDouble", (a, r) -> a.getDouble(r)),
          Map.entry("getBigDecimal", (a, r) -> a.getBigDecimal(r)),
          Map.entry("getBytes", (a, r) -> a.getBytes(r)),
          Map.entry("getLocalDate", (a, r) -> a.getLocalDate(r)),
          Map.entry("getLocalTime", (a, r) -> a.getLocalTime(r)),
          Map.entry("getLocalDateTime", (a, r) -> a.getLocalDateTime(r)),
          Map.entry("getOffsetDateTime", (a, r) -> a.getOffsetDateTime(r)),
          Map.entry("getDate", (a, r) -> a.getDate(r, TOKYO)),
          Map.entry("getTime", (a, r) -> a.getTime(r, TOKYO)),
          Map.entry("getTimestamp", (a, r) -> a.getTimestamp(r, TOKYO)),
          Map.entry("getArray", (a, r) -> a.getArray(r)),
          Map.entry("getStruct", (a, r) -> a.getStruct(r)),
          Map.entry("getObject(BigDecimal)", (a, r) -> a.getObject(r, BigDecimal.class)),
          Map.entry("getObject(LocalDate)", (a, r) -> a.getObject(r, LocalDate.class)));

  /** Calls one getter, turning an error into its SQLSTATE. */
  private static Object outcome(Getter getter, Accessor accessor, int row) {
    try {
      Object value = getter.get(accessor, row);
      return value instanceof byte[] ? new String((byte[]) value, StandardCharsets.UTF_8) : value;
    } catch (SQLException e) {
      return "error " + e.getSQLState();
    }
  }

  private static void assertSameAnswers(
      Accessor expected, int expectedRow, Accessor actual, int actualRow) {
    for (Map.Entry<String, Getter> getter : GETTERS.entrySet()) {
      Object want = outcome(getter.getValue(), expected, expectedRow);
      Object got = outcome(getter.getValue(), actual, actualRow);
      if (want instanceof Object[]) {
        assertArrayEquals((Object[]) want, (Object[]) got, getter.getKey());
      } else if (want instanceof java.sql.Array || want instanceof java.sql.Struct) {
        assertEquals(want.toString(), got.toString(), getter.getKey());
      } else {
        assertEquals(want, got, getter.getKey());
      }
    }
  }

  @Test
  void dictionaryEntriesAnswerLikeTheirValues() {
    VarCharVector words =
        vector(
            ArrowType.Utf8.INSTANCE,
            3,
            v -> {
              v.setSafe(0, "12".getBytes(StandardCharsets.UTF_8));
              v.setSafe(1, "2026-10-17".getBytes(StandardCharsets.UTF_8));
              v.setSafe(2, "true".getBytes(StandardCharsets.UTF_8));
            });
    DictionaryEncoding encoding = new DictionaryEncoding(1, false, new ArrowType.Int(32, true));
    IntVector indices =
        vector(
            new Field("w", new FieldType(true, new ArrowType.Int(32, true), encoding), null),
            3,
            v -> {
              v.setSafe(0, 2);
              v.setSafe(1, 0);
              v.setSafe(2, 1);
            });
    Accessor values = accessor(words);
    Accessor encoded =
        Accessors.create(
            indices, new DictionaryProvider.MapDictionaryProvider(new Dictionary(words, encoding)));
    assertSameAnswers(values, 2, encoded, 0);
    assertSameAnswers(values, 0, encoded, 1);
    assertSameAnswers(values, 1, encoded, 2);
  }

  @Test
  void unionMembersAnswerLikeThemselves() {
    UnionVector union =
        vector(
            new Field(
                "u",
                FieldType.nullable(
                    new ArrowType.Union(
                        UnionMode.Sparse,
                        new int[] {MinorType.INT.ordinal(), MinorType.DATEDAY.ordinal()})),
                List.of(
                    Field.nullable("int", new ArrowType.Int(32, true)),
                    Field.nullable("dateday", new ArrowType.Date(DateUnit.DAY)))),
            2,
            v -> {
              NullableIntHolder holder = new NullableIntHolder();
              holder.isSet = 1;
              holder.value = 42;
              v.setSafe(0, holder);
              v.setType(0, MinorType.INT);
              v.getDateDayVector().setSafe(1, 20_000);
              v.setType(1, MinorType.DATEDAY);
            });
    Accessor accessor = accessor(union);
    FieldVector ints = union.getIntVector();
    FieldVector dates = union.getDateDayVector();
    assertSameAnswers(accessor(ints), 0, accessor, 0);
    assertSameAnswers(accessor(dates), 1, accessor, 1);
    assertEquals(DateDayVector.class, dates.getClass());
  }

  /** A getter under test. */
  @FunctionalInterface
  private interface Getter {
    Object get(Accessor accessor, int row) throws SQLException;
  }
}
