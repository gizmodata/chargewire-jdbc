package com.gizmodata.warpsql.jdbc;

import com.gizmodata.warpsql.jdbc.accessor.ColumnClasses;
import com.gizmodata.warpsql.jdbc.arrow.SqlTypes;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Describes a result's columns from its Arrow schema, using the Flight SQL column metadata keys
 * (spec §10) when the server sends them.
 */
public final class WarpResultSetMetaData implements ResultSetMetaData {
  private final List<Field> columns;

  WarpResultSetMetaData(List<Field> columns) {
    this.columns = columns;
  }

  private Field column(int column) throws SQLException {
    if (column < 1 || column > columns.size()) {
      throw new SQLException(
          "Column " + column + " does not exist; there are " + columns.size(), "07009");
    }
    return columns.get(column - 1);
  }

  private String metadata(int column, String key) throws SQLException {
    Map<String, String> metadata = column(column).getMetadata();
    return metadata == null ? null : metadata.get(key);
  }

  private Boolean flag(int column, String key) throws SQLException {
    String value = metadata(column, key);
    return value == null ? null : "1".equals(value) || "true".equalsIgnoreCase(value);
  }

  private Integer number(int column, String key) throws SQLException {
    String value = metadata(column, key);
    try {
      return value == null ? null : Integer.valueOf(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  public int getColumnCount() {
    return columns.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return Boolean.TRUE.equals(flag(column, SqlTypes.IS_AUTO_INCREMENT));
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    Boolean sensitive = flag(column, SqlTypes.IS_CASE_SENSITIVE);
    return sensitive != null ? sensitive : SqlTypes.isText(column(column).getType());
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    Boolean searchable = flag(column, SqlTypes.IS_SEARCHABLE);
    return searchable == null || searchable;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    column(column);
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return column(column).isNullable() ? columnNullable : columnNoNulls;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return SqlTypes.isSigned(column(column).getType());
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    Integer size = SqlTypes.columnSize(column(column).getType());
    if (size == null) {
      return SqlTypes.UNBOUNDED;
    }
    ArrowType type = column(column).getType();
    if (SqlTypes.isNumeric(type)) {
      Integer scale = SqlTypes.decimalDigits(type);
      return size + (scale != null && scale > 0 ? 1 : 0) + (SqlTypes.isSigned(type) ? 1 : 0);
    }
    return size;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return column(column).getName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return column(column).getName();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    String schema = metadata(column, SqlTypes.SCHEMA_NAME);
    return schema == null ? "" : schema;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    Integer precision = number(column, SqlTypes.PRECISION);
    if (precision != null) {
      return precision;
    }
    Integer size = SqlTypes.columnSize(column(column).getType());
    return size == null ? 0 : size;
  }

  @Override
  public int getScale(int column) throws SQLException {
    Integer scale = number(column, SqlTypes.SCALE);
    if (scale != null) {
      return scale;
    }
    Integer digits = SqlTypes.decimalDigits(column(column).getType());
    return digits == null ? 0 : digits;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    String table = metadata(column, SqlTypes.TABLE_NAME);
    return table == null ? "" : table;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    String catalog = metadata(column, SqlTypes.CATALOG_NAME);
    return catalog == null ? "" : catalog;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return SqlTypes.jdbcType(column(column));
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return SqlTypes.typeName(column(column));
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return Boolean.TRUE.equals(flag(column, SqlTypes.IS_READ_ONLY));
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return !isReadOnly(column);
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    column(column);
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return ColumnClasses.of(column(column)).getName();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Not a wrapper for " + iface.getName(), "HY000");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
