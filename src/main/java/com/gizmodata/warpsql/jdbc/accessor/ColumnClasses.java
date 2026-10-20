package com.gizmodata.warpsql.jdbc.accessor;

import com.gizmodata.warpsql.jdbc.WarpInterval;
import com.gizmodata.warpsql.jdbc.arrow.SqlTypes;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * The class {@link java.sql.ResultSet#getObject(int)} returns for a column, without reading a
 * batch; it matches what each {@link Accessor} returns.
 *
 * <p>Internal API.
 */
public final class ColumnClasses {
  private ColumnClasses() {}

  /**
   * Returns the class of a column's values.
   *
   * @param field the column, with its value type
   * @return the class
   */
  public static Class<?> of(Field field) {
    ArrowType type = field.getType();
    if (SqlTypes.isUuid(type)) {
      return UUID.class;
    }
    if (SqlTypes.duckDbInteger(type) != null) {
      return BigDecimal.class;
    }
    if (type instanceof ArrowType.ExtensionType) {
      type = ((ArrowType.ExtensionType) type).storageType();
    }
    switch (type.getTypeID()) {
      case Int:
        ArrowType.Int integer = (ArrowType.Int) type;
        int width = integer.getBitWidth();
        if (integer.getIsSigned()) {
          return width == 8
              ? Byte.class
              : width == 16 ? Short.class : width == 32 ? Integer.class : Long.class;
        }
        return width == 8
            ? Short.class
            : width == 16 ? Integer.class : width == 32 ? Long.class : BigDecimal.class;
      case FloatingPoint:
        return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE
            ? Double.class
            : Float.class;
      case Decimal:
        return BigDecimal.class;
      case Bool:
        return Boolean.class;
      case Utf8:
      case Utf8View:
      case LargeUtf8:
        return String.class;
      case Binary:
      case BinaryView:
      case LargeBinary:
      case FixedSizeBinary:
        return byte[].class;
      case Date:
        return Date.class;
      case Time:
        return Time.class;
      case Timestamp:
        return Timestamp.class;
      case Interval:
        IntervalUnit unit = ((ArrowType.Interval) type).getUnit();
        return unit == IntervalUnit.YEAR_MONTH
            ? Period.class
            : unit == IntervalUnit.DAY_TIME ? Duration.class : WarpInterval.class;
      case Duration:
        return Duration.class;
      case List:
      case LargeList:
      case FixedSizeList:
      case ListView:
        return List.class;
      case Struct:
      case Map:
        return Map.class;
      default:
        return Object.class;
    }
  }
}
