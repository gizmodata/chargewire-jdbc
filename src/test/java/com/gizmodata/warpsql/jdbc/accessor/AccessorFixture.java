package com.gizmodata.warpsql.jdbc.accessor;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;

/**
 * Builds vectors for accessor tests and frees them after each test; a leaked buffer fails the test
 * when the allocator closes.
 */
abstract class AccessorFixture {
  final BufferAllocator allocator = new RootAllocator();
  private final List<FieldVector> vectors = new ArrayList<>();

  @AfterEach
  void freeVectors() {
    vectors.forEach(FieldVector::close);
    allocator.close();
  }

  /** A vector of the type, filled by {@code fill}, with its value count set. */
  <V extends FieldVector> V vector(ArrowType type, int count, Fill<V> fill) {
    return vector(Field.nullable("v", type), count, fill);
  }

  @SuppressWarnings("unchecked")
  <V extends FieldVector> V vector(Field field, int count, Fill<V> fill) {
    V vector = (V) field.createVector(allocator);
    vectors.add(vector);
    vector.allocateNew();
    try {
      fill.accept(vector);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    vector.setValueCount(count);
    return vector;
  }

  static Field field(String name, ArrowType type, Field... children) {
    return new Field(name, FieldType.nullable(type), List.of(children));
  }

  Accessor accessor(FieldVector vector) {
    return Accessors.create(vector, null);
  }

  /** Fills a vector. */
  @FunctionalInterface
  interface Fill<V> {
    void accept(V vector) throws Exception;
  }
}
