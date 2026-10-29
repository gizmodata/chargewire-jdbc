package com.gizmodata.chargewire.jdbc;

import com.gizmodata.chargewire.jdbc.accessor.ColumnClasses;
import com.gizmodata.chargewire.jdbc.arrow.SqlTypes;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Describes a prepared statement's parameters from the server's parameter schema (spec §7.1). A
 * parameter whose type the server does not know (Arrow's null type) is {@link Types#OTHER}.
 */
public final class ChargewireParameterMetaData implements ParameterMetaData {
  private final Schema schema;
  private final int count;

  ChargewireParameterMetaData(Schema schema, int count) {
    this.schema = schema;
    this.count = count;
  }

  private Field field(int parameter) throws SQLException {
    if (parameter < 1 || parameter > count) {
      throw new SQLException(
          "Parameter " + parameter + " does not exist; the statement has " + count, "07009");
    }
    if (schema == null || parameter > schema.getFields().size()) {
      return null;
    }
    Field field = schema.getFields().get(parameter - 1);
    return field.getType() instanceof ArrowType.Null ? null : field;
  }

  @Override
  public int getParameterCount() {
    return count;
  }

  @Override
  public int isNullable(int parameter) throws SQLException {
    field(parameter);
    return parameterNullableUnknown;
  }

  @Override
  public boolean isSigned(int parameter) throws SQLException {
    Field field = field(parameter);
    return field != null && SqlTypes.isSigned(field.getType());
  }

  @Override
  public int getPrecision(int parameter) throws SQLException {
    Field field = field(parameter);
    Integer size = field == null ? null : SqlTypes.columnSize(field.getType());
    return size == null ? 0 : size;
  }

  @Override
  public int getScale(int parameter) throws SQLException {
    Field field = field(parameter);
    Integer digits = field == null ? null : SqlTypes.decimalDigits(field.getType());
    return digits == null ? 0 : digits;
  }

  @Override
  public int getParameterType(int parameter) throws SQLException {
    Field field = field(parameter);
    return field == null ? Types.OTHER : SqlTypes.jdbcType(field);
  }

  @Override
  public String getParameterTypeName(int parameter) throws SQLException {
    Field field = field(parameter);
    return field == null ? "UNKNOWN" : SqlTypes.typeName(field);
  }

  @Override
  public String getParameterClassName(int parameter) throws SQLException {
    Field field = field(parameter);
    return field == null ? Object.class.getName() : ColumnClasses.of(field).getName();
  }

  @Override
  public int getParameterMode(int parameter) throws SQLException {
    field(parameter);
    return parameterModeIn;
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
