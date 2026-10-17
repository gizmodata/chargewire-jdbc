package com.gizmodata.warpsql.jdbc.accessor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.gizmodata.warpsql.jdbc.arrow.Rows;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * {@link ColumnClasses} (behind {@code getColumnClassName}) names the class each accessor's
 * {@code getObject} really returns (ported from {@code ArrowFlightJdbcAccessorFactoryTest}).
 */
class ColumnClassesTest extends AccessorFixture {
  @Test
  void everyTypeReportsTheClassItsValuesHave() throws SQLException {
    List<Field> fields = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    add(fields, values, new ArrowType.Int(8, true), (byte) 1);
    add(fields, values, new ArrowType.Int(16, true), (short) 1);
    add(fields, values, new ArrowType.Int(32, true), 1);
    add(fields, values, new ArrowType.Int(64, true), 1L);
    add(fields, values, new ArrowType.Int(8, false), 1);
    add(fields, values, new ArrowType.Int(16, false), 1);
    add(fields, values, new ArrowType.Int(32, false), 1);
    add(fields, values, new ArrowType.Int(64, false), 1);
    add(fields, values, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1.5f);
    add(fields, values, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), 1.5);
    add(fields, values, new ArrowType.Decimal(5, 2, 128), new BigDecimal("1.25"));
    add(fields, values, new ArrowType.Decimal(50, 2, 256), new BigDecimal("1.25"));
    add(fields, values, ArrowType.Bool.INSTANCE, true);
    add(fields, values, ArrowType.Utf8.INSTANCE, "x");
    add(fields, values, ArrowType.LargeUtf8.INSTANCE, "x");
    add(fields, values, ArrowType.Binary.INSTANCE, new byte[] {1});
    add(fields, values, ArrowType.LargeBinary.INSTANCE, new byte[] {1});
    add(fields, values, new ArrowType.FixedSizeBinary(1), new byte[] {1});
    add(fields, values, new ArrowType.Date(DateUnit.DAY), LocalDate.EPOCH);
    add(fields, values, new ArrowType.Date(DateUnit.MILLISECOND), LocalDate.EPOCH);
    add(fields, values, new ArrowType.Time(TimeUnit.SECOND, 32), LocalTime.NOON);
    add(fields, values, new ArrowType.Time(TimeUnit.MILLISECOND, 32), LocalTime.NOON);
    add(fields, values, new ArrowType.Time(TimeUnit.MICROSECOND, 64), LocalTime.NOON);
    add(fields, values, new ArrowType.Time(TimeUnit.NANOSECOND, 64), LocalTime.NOON);
    add(fields, values, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), Timestamp.valueOf("2026-01-01 00:00:00"));
    add(fields, values, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), Timestamp.valueOf("2026-01-01 00:00:00"));
    add(fields, values, new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), Duration.ofSeconds(1));
    fields.add(new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
        List.of(Field.nullable("item", new ArrowType.Int(32, true)))));
    values.add(List.of(1));
    Schema schema = new Schema(fields);
    try (VectorSchemaRoot root = Rows.toRoot(allocator, schema, List.<Object[]>of(values.toArray()))) {
      for (FieldVector vector : root.getFieldVectors()) {
        Object value = Accessors.create(vector, null).getObject(0);
        Class<?> reported = ColumnClasses.of(vector.getField());
        assertEquals(reported, reported.isInstance(value) ? reported : value.getClass(),
            vector.getField().getType().toString());
      }
    }
  }

  private static void add(List<Field> fields, List<Object> values, ArrowType type, Object value) {
    fields.add(Field.nullable("c" + fields.size(), type));
    values.add(value);
  }

  @Test
  void specialCases() {
    assertEquals(UUID.class, ColumnClasses.of(new Field("u", FieldType.nullable(UuidType.INSTANCE), null)));
    assertEquals(Date.class, ColumnClasses.of(Field.nullable("d", new ArrowType.Date(DateUnit.DAY))));
    assertEquals(Time.class, ColumnClasses.of(Field.nullable("t", new ArrowType.Time(TimeUnit.SECOND, 32))));
    assertEquals(Object.class, ColumnClasses.of(Field.nullable("n", ArrowType.Null.INSTANCE)));
  }
}
