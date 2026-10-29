package com.gizmodata.chargewire.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;

/** An array made by {@link java.sql.Connection#createArrayOf}, to bind to a list parameter. */
final class ParameterArray implements Array {
  private final String typeName;
  private Object[] elements;

  ParameterArray(String typeName, Object[] elements) {
    this.typeName = typeName;
    this.elements = elements == null ? new Object[0] : elements.clone();
  }

  private Object[] elements() throws SQLException {
    if (elements == null) {
      throw new SQLException("The array was freed", "HY010");
    }
    return elements;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    elements();
    return typeName;
  }

  @Override
  public int getBaseType() throws SQLException {
    elements();
    return Types.OTHER;
  }

  @Override
  public Object getArray() throws SQLException {
    return elements().clone();
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    return getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    Object[] all = elements();
    if (index < 1 || count < 0 || index - 1 + count > all.length) {
      throw new SQLException("Array slice is outside 1.." + all.length, "2202E");
    }
    return Arrays.copyOfRange(all, (int) index - 1, (int) index - 1 + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    return getArray(index, count);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new SQLFeatureNotSupportedException("Array.getResultSet is not supported", "0A000");
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    return getResultSet();
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    return getResultSet();
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    return getResultSet();
  }

  @Override
  public void free() {
    elements = null;
  }
}
