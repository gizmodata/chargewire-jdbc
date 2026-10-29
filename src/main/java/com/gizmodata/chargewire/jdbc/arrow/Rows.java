package com.gizmodata.chargewire.jdbc.arrow;

import java.sql.SQLException;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Builds an Arrow batch from Java rows: how driver-made result sets, such as JDBC's metadata
 * results, reuse the one Arrow-backed {@link java.sql.ResultSet}.
 *
 * <p>Internal API.
 */
public final class Rows {
  private Rows() {}

  /**
   * Builds a batch.
   *
   * @param allocator where the vectors are allocated; the caller closes the root
   * @param schema the columns
   * @param rows the values, one array per row, in column order
   * @return the batch
   * @throws SQLException if a value does not fit its column
   */
  public static VectorSchemaRoot toRoot(
      BufferAllocator allocator, Schema schema, List<Object[]> rows) throws SQLException {
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    try {
      root.allocateNew();
      for (int c = 0; c < schema.getFields().size(); c++) {
        FieldVector vector = root.getVector(c);
        for (int r = 0; r < rows.size(); r++) {
          ParameterEncoder.set(vector, r, rows.get(r)[c], null);
        }
        vector.setValueCount(rows.size());
      }
      root.setRowCount(rows.size());
      return root;
    } catch (SQLException | RuntimeException e) {
      root.close();
      throw e;
    }
  }
}
