package com.gizmodata.chargewire.jdbc.accessor;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;

/** A list value as a {@link java.sql.Array}, already read from its batch. */
final class ChargewireArray implements Array {
  private final String baseTypeName;
  private final int baseType;
  private Object[] elements;

  ChargewireArray(String baseTypeName, int baseType, Object[] elements) {
    this.baseTypeName = baseTypeName;
    this.baseType = baseType;
    this.elements = elements;
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
    return baseTypeName;
  }

  @Override
  public int getBaseType() throws SQLException {
    elements();
    return baseType;
  }

  @Override
  public Object getArray() throws SQLException {
    return elements().clone();
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    requireNoTypeMap(map);
    return getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    Object[] all = elements();
    if (index < 1 || count < 0 || index - 1 + count > all.length) {
      throw new SQLException(
          "Array slice [" + index + ", " + count + "] is outside 1.." + all.length, "2202E");
    }
    return Arrays.copyOfRange(all, (int) index - 1, (int) index - 1 + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    requireNoTypeMap(map);
    return getArray(index, count);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Array.getResultSet is not supported; use getArray", "0A000");
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

  @Override
  public String toString() {
    return elements == null ? "[freed]" : NestedAccessors.render(Arrays.asList(elements));
  }

  static void requireNoTypeMap(Map<String, Class<?>> map) throws SQLException {
    if (map != null && !map.isEmpty()) {
      throw new SQLFeatureNotSupportedException("Custom type maps are not supported", "0A000");
    }
  }
}
