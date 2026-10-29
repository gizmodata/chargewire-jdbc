package com.gizmodata.chargewire.jdbc.client;

import java.sql.SQLException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A statement's result as one stream of batches, read from one partition ({@link ResultReader}) or
 * from several at once ({@link PartitionedResult}).
 *
 * <p>Internal API.
 */
public interface ResultStream extends AutoCloseable {
  /**
   * Returns the statement the result belongs to.
   *
   * @return the statement id
   */
  String statementId();

  /**
   * Returns the result's schema, with dictionary-encoded columns as their index type.
   *
   * @return the schema of {@link #root()}
   */
  Schema schema();

  /**
   * Returns the current batch. The root object can change after {@link #next()} or {@link
   * #buffer(long)}; {@link #generation()} tells when.
   *
   * @return the current batch
   * @throws SQLException if the result cannot be read
   */
  VectorSchemaRoot root() throws SQLException;

  /**
   * Returns the dictionaries for the current batch's dictionary-encoded columns.
   *
   * @return the dictionaries
   */
  DictionaryProvider dictionaries();

  /**
   * Returns a number that changes whenever the object {@link #root()} returns for the current batch
   * changes without {@link #next()}.
   *
   * @return the generation
   */
  int generation();

  /**
   * Returns whether every batch was read.
   *
   * @return {@code true} once {@link #next()} returned {@code false}
   */
  boolean isFinished();

  /**
   * Loads the next batch into {@link #root()}.
   *
   * @return {@code false} at the end of the result
   * @throws SQLException if the result cannot be read, with the statement's error when it failed
   */
  boolean next() throws SQLException;

  /**
   * Reads the rest of the result into memory, keeping the current batch, so the server can go on
   * with the session's other work. A failure is raised later, by the {@link #next()} that reaches
   * it; a result larger than {@code limitBytes} is dropped past that point.
   *
   * @param limitBytes the most Arrow buffer memory to hold
   */
  void buffer(long limitBytes);

  /** Stops reading at once, from any thread: the reading thread's next read fails. */
  void abort();

  @Override
  void close();
}
