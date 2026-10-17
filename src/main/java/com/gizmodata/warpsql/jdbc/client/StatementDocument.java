package com.gizmodata.warpsql.jdbc.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.gizmodata.warpsql.jdbc.WarpProblem;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** A statement's status (spec §6.2), as the server reported it at one moment. */
public final class StatementDocument {
  /** A statement's lifecycle state. */
  public enum State {
    /** Waiting to start. */
    QUEUED,
    /** Running. */
    RUNNING,
    /** Finished with a result. */
    SUCCEEDED,
    /** Finished with an error. */
    FAILED,
    /** Stopped before finishing. */
    CANCELLED;

    /**
     * Returns whether the state can no longer change.
     *
     * @return {@code true} for succeeded, failed and cancelled
     */
    public boolean isTerminal() {
      return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
  }

  /** One piece of a result (spec §6.2). */
  public static final class Partition {
    private final int index;
    private final URI location;

    Partition(int index, URI location) {
      this.index = index;
      this.location = location;
    }

    /**
     * Returns the partition's number.
     *
     * @return the index
     */
    public int index() {
      return index;
    }

    /**
     * Returns the base URL of the server that holds the partition.
     *
     * @return the location, or {@code null} for this server
     */
    public URI location() {
      return location;
    }
  }

  private final String statementId;
  private final State state;
  private final WarpProblem error;
  private final String cancelReason;
  private final boolean rows;
  private final long rowsAffected;
  private final byte[] schema;
  private final List<Partition> partitions;
  private final boolean retained;
  private final String partitionOrder;

  private StatementDocument(
      String statementId,
      State state,
      WarpProblem error,
      String cancelReason,
      boolean rows,
      long rowsAffected,
      byte[] schema,
      List<Partition> partitions,
      boolean retained,
      String partitionOrder) {
    this.statementId = statementId;
    this.state = state;
    this.error = error;
    this.cancelReason = cancelReason;
    this.rows = rows;
    this.rowsAffected = rowsAffected;
    this.schema = schema;
    this.partitions = partitions;
    this.retained = retained;
    this.partitionOrder = partitionOrder;
  }

  /**
   * Reads a statement document.
   *
   * @param json the document
   * @return the parsed document
   * @throws IllegalArgumentException if the document is not a statement document
   */
  public static StatementDocument parse(JsonNode json) {
    String id = json.path("statement_id").asText(null);
    String stateText = json.path("state").asText(null);
    if (id == null || stateText == null) {
      throw new IllegalArgumentException("not a statement document: " + json);
    }
    State state;
    try {
      state = State.valueOf(stateText.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("unknown statement state '" + stateText + "'", e);
    }
    JsonNode errorJson = json.get("error");
    WarpProblem error =
        errorJson == null || errorJson.isNull() ? null : Problems.parse(errorJson, 0);
    String cancelReason = json.path("cancel_reason").asText(null);
    JsonNode result = json.get("result");
    boolean rows = false;
    long rowsAffected = -1;
    byte[] schema = null;
    List<Partition> partitions = Collections.emptyList();
    boolean retained = false;
    String partitionOrder = null;
    if (result != null && result.isObject()) {
      partitionOrder = result.path("partition_order").asText(null);
      rows = "rows".equals(result.path("kind").asText());
      JsonNode affected = result.get("rows_affected");
      rowsAffected = affected == null || affected.isNull() ? -1 : affected.asLong();
      String schemaText = result.path("schema").asText(null);
      schema = schemaText == null ? null : Base64.getDecoder().decode(schemaText);
      retained = result.path("retained").asBoolean(false);
      JsonNode parts = result.get("partitions");
      if (parts != null && parts.isArray()) {
        partitions = new ArrayList<>();
        for (JsonNode part : parts) {
          String location = part.path("location").asText(null);
          partitions.add(
              new Partition(
                  part.path("index").asInt(partitions.size()),
                  location == null || location.isEmpty() ? null : URI.create(location)));
        }
      }
    }
    return new StatementDocument(
        id,
        state,
        error,
        cancelReason,
        rows,
        rowsAffected,
        schema,
        partitions,
        retained,
        partitionOrder);
  }

  /**
   * Returns the statement's id.
   *
   * @return the id
   */
  public String statementId() {
    return statementId;
  }

  /**
   * Returns the state.
   *
   * @return the state
   */
  public State state() {
    return state;
  }

  /**
   * Returns the error of a failed or cancelled statement.
   *
   * @return the problem, or {@code null}
   */
  public WarpProblem error() {
    return error;
  }

  /**
   * Returns why a cancelled statement was cancelled.
   *
   * @return for example {@code client} or {@code timeout}, or {@code null}
   */
  public String cancelReason() {
    return cancelReason;
  }

  /**
   * Returns whether the succeeded statement returned rows.
   *
   * @return {@code true} for {@code kind: rows}
   */
  public boolean hasRows() {
    return rows;
  }

  /**
   * Returns how many rows a statement without a result changed.
   *
   * @return the count, or -1 when the server did not say (DDL, for example)
   */
  public long rowsAffected() {
    return rowsAffected;
  }

  /**
   * Returns the result's Arrow schema as an IPC stream.
   *
   * @return the schema bytes, or {@code null}
   */
  public byte[] schema() {
    return schema;
  }

  /**
   * Returns the result's partitions.
   *
   * @return the partitions, in index order as the server listed them
   */
  public List<Partition> partitions() {
    return partitions;
  }

  /**
   * Returns whether the server keeps the result so it can be re-read and resumed.
   *
   * @return {@code true} for a retained result
   */
  public boolean retained() {
    return retained;
  }

  /**
   * Returns how a result split into several partitions spreads its rows over them (spec §6.10).
   *
   * @return {@code interleaved} or {@code any}, or {@code null} for one partition, a split not
   *     decided yet, or partitions on other servers
   */
  public String partitionOrder() {
    return partitionOrder;
  }
}
