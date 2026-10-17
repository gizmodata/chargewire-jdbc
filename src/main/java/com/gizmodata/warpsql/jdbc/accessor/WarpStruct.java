package com.gizmodata.warpsql.jdbc.accessor;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;

/** A struct value as a {@link java.sql.Struct}, already read from its batch. */
final class WarpStruct implements Struct {
  private final String typeName;
  private final Object[] attributes;

  WarpStruct(String typeName, Object[] attributes) {
    this.typeName = typeName;
    this.attributes = attributes;
  }

  @Override
  public String getSQLTypeName() {
    return typeName;
  }

  @Override
  public Object[] getAttributes() {
    return attributes.clone();
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    WarpArray.requireNoTypeMap(map);
    return getAttributes();
  }

  @Override
  public String toString() {
    return NestedAccessors.render(Arrays.asList(attributes));
  }
}
