package com.gizmodata.warpsql.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gizmodata.warpsql.jdbc.arrow.ArrowIpc;
import com.gizmodata.warpsql.jdbc.arrow.ParameterEncoder;
import com.gizmodata.warpsql.jdbc.client.SqlErrors;
import com.gizmodata.warpsql.jdbc.client.WarpClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A statement prepared on the server (spec §7), or, on a server without the {@code
 * prepared_statements} capability, SQL text submitted with JSON parameters.
 *
 * <p>Parameters travel as an Arrow stream with the server's parameter types, so decimals,
 * timestamps and binaries keep their exact values. {@link #executeBatch()} sends the whole batch as
 * one request, one row per execution.
 */
public final class WarpPreparedStatement extends WarpStatement implements PreparedStatement {
  private final String sql;
  private final boolean serverPrepared;
  private String preparedId;
  private String preparedSessionId;
  private int preparedGeneration;
  private Schema parameterSchema;
  private Schema resultSchema;
  private ParameterEncoder.Value[] parameters;
  private final List<ParameterEncoder.Value[]> batchRows = new ArrayList<>();

  WarpPreparedStatement(WarpConnection connection, String sql) throws SQLException {
    super(connection);
    if (sql == null) {
      throw new SQLException("The SQL must not be null", "HY009");
    }
    this.sql = sql;
    this.serverPrepared = connection.client().hasCapability("prepared_statements");
    if (serverPrepared) {
      connection.withSession(
          id -> {
            prepare(id);
            return null;
          });
    } else {
      parameters = new ParameterEncoder.Value[Placeholders.count(sql)];
    }
  }

  private void prepare(String sessionId) throws SQLException {
    WarpClient client = connection.client();
    JsonNode prepared = client.prepare(sessionId, sql);
    preparedId = prepared.path("prepared_id").asText(null);
    if (preparedId == null) {
      throw SqlErrors.protocolError("The server's prepare response has no prepared_id");
    }
    preparedSessionId = sessionId;
    preparedGeneration = connection.sessionGeneration();
    parameterSchema = schema(prepared.path("parameter_schema"));
    resultSchema = schema(prepared.path("result_schema"));
    int count =
        parameterSchema == null ? Placeholders.count(sql) : parameterSchema.getFields().size();
    if (parameters == null || parameters.length != count) {
      parameters = new ParameterEncoder.Value[count];
    }
  }

  private static Schema schema(JsonNode encoded) throws SQLException {
    if (encoded == null
        || encoded.isNull()
        || encoded.isMissingNode()
        || encoded.asText().isEmpty()) {
      return null;
    }
    try {
      return ArrowIpc.readSchema(Base64.getDecoder().decode(encoded.asText()));
    } catch (IOException | IllegalArgumentException e) {
      throw SqlErrors.protocolError("The server sent an unreadable schema: " + e.getMessage(), e);
    }
  }

  /** Prepares again in a session that replaced the one the statement was prepared in. */
  private void ensurePrepared(String sessionId) throws SQLException {
    if (preparedGeneration != connection.sessionGeneration()
        || !sessionId.equals(preparedSessionId)) {
      prepare(sessionId);
    }
  }

  /** Executes once per row; a prepared statement the server lost is prepared again. */
  private boolean runRows(List<ParameterEncoder.Value[]> rows) throws SQLException {
    WarpClient client = connection.client();
    if (!serverPrepared) {
      ObjectNode body = client.newObject().put("sql", sql);
      ArrayNode values = body.putArray("parameters");
      for (ParameterEncoder.Value value : rows.get(0)) {
        values.addPOJO(ParameterEncoder.jsonValue(value.value()));
      }
      return run((id, options) -> client.submit(id, body, options));
    }
    return run(
        (id, options) -> {
          ensurePrepared(id);
          try {
            return executeOnce(client, id, rows, options);
          } catch (SQLException e) {
            if (!"prepared_not_found".equals(WarpProblem.codeOf(e))) {
              throw e;
            }
            prepare(id);
            return executeOnce(client, id, rows, options);
          }
        });
  }

  private WarpClient.Submission executeOnce(
      WarpClient client,
      String sessionId,
      List<ParameterEncoder.Value[]> rows,
      WarpClient.SubmitOptions options)
      throws SQLException {
    if (rows.get(0).length == 0) {
      byte[] empty = "{}".getBytes(StandardCharsets.UTF_8);
      return client.executePrepared(sessionId, preparedId, "application/json", empty, options);
    }
    byte[] body = ParameterEncoder.encode(connection.allocator(), parameterSchema, rows);
    return client.executePrepared(sessionId, preparedId, WarpClient.ARROW_STREAM, body, options);
  }

  private ParameterEncoder.Value[] boundRow() throws SQLException {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] == null) {
        throw new SQLException("Parameter " + (i + 1) + " is not set", "07001");
      }
    }
    return parameters.clone();
  }

  // ---- Execution ----

  @Override
  public boolean execute() throws SQLException {
    checkOpen();
    return runRows(List.<ParameterEncoder.Value[]>of(boundRow()));
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    if (!execute()) {
      throw new SQLException("The statement did not return a result set", "02000");
    }
    return getResultSet();
  }

  @Override
  public int executeUpdate() throws SQLException {
    return saturate(executeLargeUpdate());
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    if (execute()) {
      throw returnedRows();
    }
    return getLargeUpdateCount();
  }

  @Override
  public void addBatch() throws SQLException {
    checkOpen();
    batchRows.add(boundRow());
  }

  @Override
  public void clearBatch() throws SQLException {
    checkOpen();
    batchRows.clear();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    long[] counts = executeLargeBatch();
    int[] result = new int[counts.length];
    for (int i = 0; i < counts.length; i++) {
      result[i] = saturate(counts[i]);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The batch goes to the server as one request whose Arrow body has one row per entry. The
   * server reports one total, so each entry of a batch of several gets {@link #SUCCESS_NO_INFO}; a
   * batch of one keeps its count.
   */
  @Override
  public long[] executeLargeBatch() throws SQLException {
    checkOpen();
    List<ParameterEncoder.Value[]> rows = new ArrayList<>(batchRows);
    batchRows.clear();
    if (rows.isEmpty()) {
      return new long[0];
    }
    long[] counts = new long[rows.size()];
    try {
      if (serverPrepared && rows.get(0).length > 0) {
        if (runRows(rows)) {
          throw returnedRows();
        }
        long total = getLargeUpdateCount();
        Arrays.fill(counts, rows.size() == 1 ? total : SUCCESS_NO_INFO);
      } else {
        for (int i = 0; i < rows.size(); i++) {
          if (runRows(List.<ParameterEncoder.Value[]>of(rows.get(i)))) {
            throw returnedRows();
          }
          counts[i] = getLargeUpdateCount();
        }
      }
    } catch (SQLException e) {
      throw new BatchUpdateException(
          e.getMessage(), e.getSQLState(), e.getErrorCode(), new long[0], e);
    }
    return counts;
  }

  /**
   * {@inheritDoc}
   *
   * @return the result's columns, or {@code null} when the server does not know them before
   *     execution
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkOpen();
    return resultSchema == null ? null : new WarpResultSetMetaData(resultSchema.getFields());
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    checkOpen();
    return new WarpParameterMetaData(parameterSchema, parameters.length);
  }

  @Override
  void closeServerSide() {
    if (preparedId != null
        && !connection.isClosing()
        && preparedGeneration == connection.sessionGeneration()) {
      try {
        connection.client().closePrepared(preparedSessionId, preparedId);
      } catch (SQLException e) {
        // The server frees it with the session at the latest.
      }
    }
  }

  // ---- SQL-text methods do not apply ----

  private static SQLException hasSql() {
    return new SQLException(
        "A PreparedStatement runs its own SQL; use execute() without arguments", "HY000");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw hasSql();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw hasSql();
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw hasSql();
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    throw hasSql();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw hasSql();
  }

  // ---- Parameters ----

  private void set(int index, Object value, int sqlType, Calendar calendar) throws SQLException {
    checkOpen();
    if (index < 1 || index > parameters.length) {
      throw new SQLException(
          "Parameter " + index + " does not exist; the statement has " + parameters.length,
          "07009");
    }
    parameters[index - 1] = new ParameterEncoder.Value(value, sqlType, calendar);
  }

  private void set(int index, Object value) throws SQLException {
    set(index, value, ParameterEncoder.NO_TYPE, null);
  }

  @Override
  public void clearParameters() throws SQLException {
    checkOpen();
    Arrays.fill(parameters, null);
  }

  @Override
  public void setNull(int index, int sqlType) throws SQLException {
    set(index, null, sqlType, null);
  }

  @Override
  public void setNull(int index, int sqlType, String typeName) throws SQLException {
    setNull(index, sqlType);
  }

  @Override
  public void setBoolean(int index, boolean x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setByte(int index, byte x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setShort(int index, short x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setInt(int index, int x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setLong(int index, long x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setFloat(int index, float x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setDouble(int index, double x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setBigDecimal(int index, BigDecimal x) throws SQLException {
    set(index, x, Types.DECIMAL, null);
  }

  @Override
  public void setString(int index, String x) throws SQLException {
    set(index, x, Types.VARCHAR, null);
  }

  @Override
  public void setNString(int index, String value) throws SQLException {
    setString(index, value);
  }

  @Override
  public void setBytes(int index, byte[] x) throws SQLException {
    set(index, x, Types.VARBINARY, null);
  }

  @Override
  public void setDate(int index, Date x) throws SQLException {
    set(index, x, Types.DATE, null);
  }

  @Override
  public void setDate(int index, Date x, Calendar cal) throws SQLException {
    set(index, x, Types.DATE, cal);
  }

  @Override
  public void setTime(int index, Time x) throws SQLException {
    set(index, x, Types.TIME, null);
  }

  @Override
  public void setTime(int index, Time x, Calendar cal) throws SQLException {
    set(index, x, Types.TIME, cal);
  }

  @Override
  public void setTimestamp(int index, Timestamp x) throws SQLException {
    set(index, x, Types.TIMESTAMP, null);
  }

  @Override
  public void setTimestamp(int index, Timestamp x, Calendar cal) throws SQLException {
    set(index, x, Types.TIMESTAMP, cal);
  }

  @Override
  public void setObject(int index, Object x) throws SQLException {
    set(index, x);
  }

  @Override
  public void setObject(int index, Object x, int targetSqlType) throws SQLException {
    setObject(index, x, targetSqlType, -1);
  }

  @Override
  public void setObject(int index, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    Object value = x == null ? null : ParameterEncoder.coerce(x, targetSqlType, scaleOrLength);
    set(index, value, targetSqlType, null);
  }

  @Override
  public void setObject(int index, Object x, SQLType targetSqlType) throws SQLException {
    setObject(index, x, vendorType(targetSqlType), -1);
  }

  @Override
  public void setObject(int index, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    setObject(index, x, vendorType(targetSqlType), scaleOrLength);
  }

  private static int vendorType(SQLType type) throws SQLException {
    if (type == null || type.getVendorTypeNumber() == null) {
      throw new SQLFeatureNotSupportedException("Unsupported SQL type " + type, "0A000");
    }
    return type.getVendorTypeNumber();
  }

  @Override
  public void setArray(int index, Array x) throws SQLException {
    set(index, x == null ? null : x.getArray());
  }

  @Override
  public void setURL(int index, URL x) throws SQLException {
    set(index, x == null ? null : x.toString(), Types.VARCHAR, null);
  }

  @Override
  public void setBinaryStream(int index, InputStream x) throws SQLException {
    setBytes(index, x == null ? null : readBytes(x, -1));
  }

  @Override
  public void setBinaryStream(int index, InputStream x, int length) throws SQLException {
    setBytes(index, x == null ? null : readBytes(x, length));
  }

  @Override
  public void setBinaryStream(int index, InputStream x, long length) throws SQLException {
    setBytes(index, x == null ? null : readBytes(x, length));
  }

  @Override
  public void setAsciiStream(int index, InputStream x) throws SQLException {
    setString(index, x == null ? null : new String(readBytes(x, -1), StandardCharsets.US_ASCII));
  }

  @Override
  public void setAsciiStream(int index, InputStream x, int length) throws SQLException {
    setString(
        index, x == null ? null : new String(readBytes(x, length), StandardCharsets.US_ASCII));
  }

  @Override
  public void setAsciiStream(int index, InputStream x, long length) throws SQLException {
    setString(
        index, x == null ? null : new String(readBytes(x, length), StandardCharsets.US_ASCII));
  }

  /** {@inheritDoc} The stream is read as UTF-16, high byte first, as JDBC defines it. */
  @Override
  @Deprecated
  public void setUnicodeStream(int index, InputStream x, int length) throws SQLException {
    setString(
        index, x == null ? null : new String(readBytes(x, length), StandardCharsets.UTF_16BE));
  }

  @Override
  public void setCharacterStream(int index, Reader reader) throws SQLException {
    setString(index, reader == null ? null : readText(reader, -1));
  }

  @Override
  public void setCharacterStream(int index, Reader reader, int length) throws SQLException {
    setString(index, reader == null ? null : readText(reader, length));
  }

  @Override
  public void setCharacterStream(int index, Reader reader, long length) throws SQLException {
    setString(index, reader == null ? null : readText(reader, length));
  }

  @Override
  public void setNCharacterStream(int index, Reader value) throws SQLException {
    setCharacterStream(index, value);
  }

  @Override
  public void setNCharacterStream(int index, Reader value, long length) throws SQLException {
    setCharacterStream(index, value, length);
  }

  @Override
  public void setBlob(int index, Blob x) throws SQLException {
    setBytes(index, x == null ? null : x.getBytes(1, Math.toIntExact(x.length())));
  }

  @Override
  public void setBlob(int index, InputStream inputStream) throws SQLException {
    setBinaryStream(index, inputStream);
  }

  @Override
  public void setBlob(int index, InputStream inputStream, long length) throws SQLException {
    setBinaryStream(index, inputStream, length);
  }

  @Override
  public void setClob(int index, Clob x) throws SQLException {
    setString(index, x == null ? null : x.getSubString(1, Math.toIntExact(x.length())));
  }

  @Override
  public void setClob(int index, Reader reader) throws SQLException {
    setCharacterStream(index, reader);
  }

  @Override
  public void setClob(int index, Reader reader, long length) throws SQLException {
    setCharacterStream(index, reader, length);
  }

  @Override
  public void setNClob(int index, NClob value) throws SQLException {
    setClob(index, value);
  }

  @Override
  public void setNClob(int index, Reader reader) throws SQLException {
    setCharacterStream(index, reader);
  }

  @Override
  public void setNClob(int index, Reader reader, long length) throws SQLException {
    setCharacterStream(index, reader, length);
  }

  @Override
  public void setRef(int index, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("REF parameters are not supported", "0A000");
  }

  @Override
  public void setRowId(int index, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("ROWID parameters are not supported", "0A000");
  }

  @Override
  public void setSQLXML(int index, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("SQLXML parameters are not supported", "0A000");
  }

  private static byte[] readBytes(InputStream in, long length) throws SQLException {
    try {
      if (length < 0) {
        return in.readAllBytes();
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      long left = length;
      while (left > 0) {
        int read = in.read(chunk, 0, (int) Math.min(chunk.length, left));
        if (read < 0) {
          break;
        }
        out.write(chunk, 0, read);
        left -= read;
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new SQLException("Cannot read the parameter's stream: " + e.getMessage(), "HY000", e);
    }
  }

  private static String readText(Reader reader, long length) throws SQLException {
    try {
      StringWriter out = new StringWriter();
      char[] chunk = new char[8192];
      long left = length < 0 ? Long.MAX_VALUE : length;
      while (left > 0) {
        int read = reader.read(chunk, 0, (int) Math.min(chunk.length, left));
        if (read < 0) {
          break;
        }
        out.write(chunk, 0, read);
        left -= read;
      }
      return out.toString();
    } catch (IOException e) {
      throw new SQLException("Cannot read the parameter's reader: " + e.getMessage(), "HY000", e);
    }
  }
}
