package com.gizmodata.chargewire.jdbc;

import com.gizmodata.chargewire.jdbc.client.ResultStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Where a {@link ChargewireResultSet}'s batches come from: a server result, or rows the driver
 * built.
 */
abstract class ResultBatches implements AutoCloseable {
  /**
   * Returns the columns as the user sees them: dictionary-encoded columns have their value type.
   *
   * @return the columns
   */
  abstract List<Field> columns();

  /**
   * Loads the next batch.
   *
   * @return {@code false} at the end
   * @throws SQLException if the result cannot be read
   */
  abstract boolean next() throws SQLException;

  /**
   * Returns the current batch.
   *
   * @return the batch
   * @throws SQLException if the result cannot be read
   */
  abstract VectorSchemaRoot root() throws SQLException;

  /**
   * Returns the dictionaries for dictionary-encoded columns.
   *
   * @return the dictionaries, or {@code null}
   */
  abstract DictionaryProvider dictionaries();

  /**
   * Returns whether every batch was read.
   *
   * @return {@code true} at the end
   */
  abstract boolean isFinished();

  /** Stops a read in progress on another thread. */
  abstract void abort();

  /**
   * Returns the object to hold while reading the current batch: the batches may be buffered from
   * another thread.
   *
   * @return the lock
   */
  abstract Object lock();

  /**
   * Returns a number that changes when the current batch moved to another root without a call to
   * {@link #next()}.
   *
   * @return the generation
   */
  abstract int generation();

  @Override
  public abstract void close();

  /** A server result, streamed batch by batch. */
  static final class Streamed extends ResultBatches {
    private final ResultStream reader;
    private final List<Field> columns;

    Streamed(ResultStream reader) {
      this.reader = reader;
      this.columns = userColumns(reader.schema(), reader.dictionaries());
    }

    @Override
    List<Field> columns() {
      return columns;
    }

    @Override
    boolean next() throws SQLException {
      return reader.next();
    }

    @Override
    VectorSchemaRoot root() throws SQLException {
      return reader.root();
    }

    @Override
    DictionaryProvider dictionaries() {
      return reader.dictionaries();
    }

    @Override
    boolean isFinished() {
      return reader.isFinished();
    }

    @Override
    void abort() {
      reader.abort();
    }

    @Override
    Object lock() {
      return reader;
    }

    @Override
    int generation() {
      return reader.generation();
    }

    @Override
    public void close() {
      reader.close();
    }

    private static List<Field> userColumns(Schema schema, DictionaryProvider dictionaries) {
      List<Field> columns = new ArrayList<>();
      for (Field field : schema.getFields()) {
        Dictionary dictionary =
            field.getDictionary() == null
                ? null
                : dictionaries.lookup(field.getDictionary().getId());
        if (dictionary == null) {
          columns.add(field);
        } else {
          Field values = dictionary.getVector().getField();
          columns.add(
              new Field(
                  field.getName(),
                  new org.apache.arrow.vector.types.pojo.FieldType(
                      field.isNullable(), values.getType(), null, field.getMetadata()),
                  values.getChildren()));
        }
      }
      return columns;
    }
  }

  /** One batch the driver built, such as a metadata result. */
  static final class InMemory extends ResultBatches {
    private final VectorSchemaRoot root;
    private boolean delivered;

    InMemory(VectorSchemaRoot root) {
      this.root = root;
    }

    @Override
    List<Field> columns() {
      return root.getSchema().getFields();
    }

    @Override
    boolean next() {
      if (delivered) {
        return false;
      }
      delivered = true;
      return true;
    }

    @Override
    VectorSchemaRoot root() {
      return root;
    }

    @Override
    DictionaryProvider dictionaries() {
      return null;
    }

    @Override
    boolean isFinished() {
      return delivered;
    }

    @Override
    void abort() {}

    @Override
    Object lock() {
      return this;
    }

    @Override
    int generation() {
      return 0;
    }

    @Override
    public void close() {
      root.close();
    }
  }
}
