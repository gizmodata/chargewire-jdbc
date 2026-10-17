package com.gizmodata.warpsql.jdbc.arrow;

import java.sql.Types;
import java.util.Map;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Maps Arrow types to JDBC's: {@link Types} codes, type names, precision and scale.
 *
 * <p>Unsigned integers map to the next wider signed type (an unsigned 64-bit integer to {@code
 * NUMERIC(20, 0)}), so every value fits the reported type. The column metadata keys are Arrow
 * Flight SQL's, which Warp SQL adopts (spec §10).
 */
public final class SqlTypes {
  /** Field metadata key: the database's own name for the column's type. */
  public static final String TYPE_NAME = "ARROW:FLIGHT:SQL:TYPE_NAME";

  /** Field metadata key: the column's catalog. */
  public static final String CATALOG_NAME = "ARROW:FLIGHT:SQL:CATALOG_NAME";

  /** Field metadata key: the column's schema. */
  public static final String SCHEMA_NAME = "ARROW:FLIGHT:SQL:DB_SCHEMA_NAME";

  /** Field metadata key: the column's table. */
  public static final String TABLE_NAME = "ARROW:FLIGHT:SQL:TABLE_NAME";

  /** Field metadata key: precision, or a character column's length. */
  public static final String PRECISION = "ARROW:FLIGHT:SQL:PRECISION";

  /** Field metadata key: scale. */
  public static final String SCALE = "ARROW:FLIGHT:SQL:SCALE";

  /** Field metadata key: {@code 1} if the column is auto-incremented. */
  public static final String IS_AUTO_INCREMENT = "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT";

  /** Field metadata key: {@code 1} if comparisons are case-sensitive. */
  public static final String IS_CASE_SENSITIVE = "ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE";

  /** Field metadata key: {@code 1} if the column is read-only. */
  public static final String IS_READ_ONLY = "ARROW:FLIGHT:SQL:IS_READ_ONLY";

  /** Field metadata key: {@code 1} if the column can be used in a WHERE clause. */
  public static final String IS_SEARCHABLE = "ARROW:FLIGHT:SQL:IS_SEARCHABLE";

  /** Field metadata key: the column's comment. */
  public static final String REMARKS = "ARROW:FLIGHT:SQL:REMARKS";

  /** Size reported for strings and binaries without a known length. */
  public static final int UNBOUNDED = Integer.MAX_VALUE;

  private SqlTypes() {}

  /**
   * Returns the JDBC type of a column.
   *
   * @param field the column, with its value type (not its dictionary index type)
   * @return a {@link Types} code
   */
  public static int jdbcType(Field field) {
    return jdbcType(field.getType());
  }

  /**
   * Returns the JDBC type of an Arrow type.
   *
   * @param type the Arrow type
   * @return a {@link Types} code
   */
  public static int jdbcType(ArrowType type) {
    if (type instanceof ArrowType.ExtensionType) {
      return isUuid(type) ? Types.OTHER : jdbcType(((ArrowType.ExtensionType) type).storageType());
    }
    switch (type.getTypeID()) {
      case Null:
        return Types.NULL;
      case Int:
        return intType((ArrowType.Int) type);
      case FloatingPoint:
        return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE
            ? Types.DOUBLE
            : Types.REAL;
      case Bool:
        return Types.BOOLEAN;
      case Utf8:
      case Utf8View:
        return Types.VARCHAR;
      case LargeUtf8:
        return Types.LONGVARCHAR;
      case Binary:
      case BinaryView:
        return Types.VARBINARY;
      case LargeBinary:
        return Types.LONGVARBINARY;
      case FixedSizeBinary:
        return Types.BINARY;
      case Decimal:
        return Types.DECIMAL;
      case Date:
        return Types.DATE;
      case Time:
        return Types.TIME;
      case Timestamp:
        return ((ArrowType.Timestamp) type).getTimezone() == null
            ? Types.TIMESTAMP
            : Types.TIMESTAMP_WITH_TIMEZONE;
      case List:
      case LargeList:
      case FixedSizeList:
      case ListView:
      case LargeListView:
        return Types.ARRAY;
      case Struct:
        return Types.STRUCT;
      case Map:
      case Union:
        return Types.JAVA_OBJECT;
      default:
        return Types.OTHER;
    }
  }

  private static int intType(ArrowType.Int type) {
    int width = type.getBitWidth();
    if (type.getIsSigned()) {
      return width == 8
          ? Types.TINYINT
          : width == 16 ? Types.SMALLINT : width == 32 ? Types.INTEGER : Types.BIGINT;
    }
    return width == 8
        ? Types.SMALLINT
        : width == 16 ? Types.INTEGER : width == 32 ? Types.BIGINT : Types.NUMERIC;
  }

  /**
   * Returns whether a type is Arrow's canonical UUID extension.
   *
   * @param type the Arrow type
   * @return {@code true} for {@code arrow.uuid}
   */
  public static boolean isUuid(ArrowType type) {
    return type instanceof ArrowType.ExtensionType
        && "arrow.uuid".equals(((ArrowType.ExtensionType) type).extensionName());
  }

  /**
   * Returns the column's type name: the server's own when it sent one, else a generic SQL name.
   *
   * @param field the column, with its value type
   * @return the type name
   */
  public static String typeName(Field field) {
    Map<String, String> metadata = field.getMetadata();
    String name = metadata == null ? null : metadata.get(TYPE_NAME);
    return name != null ? name : typeName(field, field.getType());
  }

  private static String typeName(Field field, ArrowType type) {
    if (type instanceof ArrowType.ExtensionType) {
      return isUuid(type)
          ? "UUID"
          : typeName(field, ((ArrowType.ExtensionType) type).storageType());
    }
    switch (type.getTypeID()) {
      case Null:
        return "NULL";
      case Int:
        ArrowType.Int integer = (ArrowType.Int) type;
        String name =
            integer.getBitWidth() == 8
                ? "TINYINT"
                : integer.getBitWidth() == 16
                    ? "SMALLINT"
                    : integer.getBitWidth() == 32 ? "INTEGER" : "BIGINT";
        return integer.getIsSigned() ? name : "U" + name;
      case FloatingPoint:
        return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE
            ? "DOUBLE"
            : "REAL";
      case Bool:
        return "BOOLEAN";
      case Utf8:
      case Utf8View:
      case LargeUtf8:
        return "VARCHAR";
      case Binary:
      case BinaryView:
      case LargeBinary:
      case FixedSizeBinary:
        return "VARBINARY";
      case Decimal:
        ArrowType.Decimal decimal = (ArrowType.Decimal) type;
        return "DECIMAL(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
      case Date:
        return "DATE";
      case Time:
        return "TIME";
      case Timestamp:
        return ((ArrowType.Timestamp) type).getTimezone() == null
            ? "TIMESTAMP"
            : "TIMESTAMP WITH TIME ZONE";
      case Interval:
        return "INTERVAL";
      case Duration:
        return "DURATION";
      case List:
      case LargeList:
      case FixedSizeList:
      case ListView:
      case LargeListView:
        return field.getChildren().isEmpty()
            ? "ARRAY"
            : typeName(field.getChildren().get(0)) + "[]";
      case Struct:
        return "STRUCT";
      case Map:
        return "MAP";
      case Union:
        return "UNION";
      default:
        return type.getTypeID().name().toUpperCase(java.util.Locale.ROOT);
    }
  }

  /**
   * Returns the column size as JDBC defines it: digits for numbers, characters for dates and times,
   * length for strings and binaries.
   *
   * @param type the Arrow type
   * @return the size, or {@code null} where it does not apply
   */
  public static Integer columnSize(ArrowType type) {
    if (type instanceof ArrowType.ExtensionType) {
      return isUuid(type) ? 36 : columnSize(((ArrowType.ExtensionType) type).storageType());
    }
    switch (type.getTypeID()) {
      case Int:
        ArrowType.Int integer = (ArrowType.Int) type;
        switch (integer.getBitWidth()) {
          case 8:
            return 3;
          case 16:
            return 5;
          case 32:
            return 10;
          default:
            return integer.getIsSigned() ? 19 : 20;
        }
      case FloatingPoint:
        return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE
            ? 15
            : 7;
      case Bool:
        return 1;
      case Decimal:
        return ((ArrowType.Decimal) type).getPrecision();
      case Utf8:
      case Utf8View:
      case LargeUtf8:
      case Binary:
      case BinaryView:
      case LargeBinary:
        return UNBOUNDED;
      case FixedSizeBinary:
        return ((ArrowType.FixedSizeBinary) type).getByteWidth();
      case Date:
        return 10;
      case Time:
        return withFraction(8, ((ArrowType.Time) type).getUnit());
      case Timestamp:
        ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
        return withFraction(19, timestamp.getUnit()) + (timestamp.getTimezone() == null ? 0 : 6);
      default:
        return null;
    }
  }

  /**
   * Returns the number of fractional digits.
   *
   * @param type the Arrow type
   * @return the scale, or {@code null} where it does not apply
   */
  public static Integer decimalDigits(ArrowType type) {
    switch (type.getTypeID()) {
      case Int:
        return 0;
      case Decimal:
        return ((ArrowType.Decimal) type).getScale();
      case Time:
        return fractionDigits(((ArrowType.Time) type).getUnit());
      case Timestamp:
        return fractionDigits(((ArrowType.Timestamp) type).getUnit());
      default:
        return null;
    }
  }

  /**
   * Returns whether the type's numbers can be negative.
   *
   * @param type the Arrow type
   * @return {@code true} for signed numbers
   */
  public static boolean isSigned(ArrowType type) {
    switch (type.getTypeID()) {
      case Int:
        return ((ArrowType.Int) type).getIsSigned();
      case FloatingPoint:
      case Decimal:
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns whether the type is numeric.
   *
   * @param type the Arrow type
   * @return {@code true} for integers, floating point and decimals
   */
  public static boolean isNumeric(ArrowType type) {
    switch (type.getTypeID()) {
      case Int:
      case FloatingPoint:
      case Decimal:
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns whether the type is character data.
   *
   * @param type the Arrow type
   * @return {@code true} for strings
   */
  public static boolean isText(ArrowType type) {
    switch (type.getTypeID()) {
      case Utf8:
      case Utf8View:
      case LargeUtf8:
        return true;
      default:
        return false;
    }
  }

  private static int withFraction(int base, TimeUnit unit) {
    int digits = fractionDigits(unit);
    return digits == 0 ? base : base + 1 + digits;
  }

  private static int fractionDigits(TimeUnit unit) {
    switch (unit) {
      case MILLISECOND:
        return 3;
      case MICROSECOND:
        return 6;
      case NANOSECOND:
        return 9;
      default:
        return 0;
    }
  }

  /**
   * Returns the Arrow type for a JDBC type code, for typed {@code NULL} parameters.
   *
   * @param sqlType a {@link Types} code
   * @return the Arrow type, or {@code null} when there is no natural one
   */
  public static ArrowType arrowType(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case Types.TINYINT:
        return new ArrowType.Int(8, true);
      case Types.SMALLINT:
        return new ArrowType.Int(16, true);
      case Types.INTEGER:
        return new ArrowType.Int(32, true);
      case Types.BIGINT:
        return new ArrowType.Int(64, true);
      case Types.REAL:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case Types.FLOAT:
      case Types.DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
      case Types.NCLOB:
        return ArrowType.Utf8.INSTANCE;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return ArrowType.Binary.INSTANCE;
      case Types.DATE:
        return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
        return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case Types.TIMESTAMP:
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
      default:
        return null;
    }
  }
}
