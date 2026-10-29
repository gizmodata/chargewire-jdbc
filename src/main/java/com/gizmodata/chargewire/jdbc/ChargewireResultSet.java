package com.gizmodata.chargewire.jdbc;

import com.gizmodata.chargewire.jdbc.accessor.Accessor;
import com.gizmodata.chargewire.jdbc.accessor.Accessors;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A forward-only, read-only result that streams Arrow batches: only the current batch is in memory.
 */
public final class ChargewireResultSet implements ResultSet {
  private final ChargewireStatement statement;
  private final ResultBatches batches;
  private final List<Field> columns;
  private final Map<String, Integer> labels = new HashMap<>();
  private final long maxRows;
  private final Runnable onClose;
  private final Object lock;
  private Accessor[] accessors;
  private int boundGeneration;
  private int batchRows;
  private int rowInBatch = -1;
  private long rowNumber;
  private boolean afterLast;
  private boolean wasNull;
  private int fetchSize;
  private SQLWarning warnings;
  private volatile boolean closed;

  ChargewireResultSet(
      ChargewireStatement statement, ResultBatches batches, long maxRows, Runnable onClose) {
    this.statement = statement;
    this.batches = batches;
    this.columns = batches.columns();
    this.maxRows = maxRows;
    this.onClose = onClose;
    this.lock = batches.lock();
    for (int i = columns.size() - 1; i >= 0; i--) {
      labels.put(columns.get(i).getName().toLowerCase(Locale.ROOT), i + 1);
    }
  }

  List<Field> columns() {
    return columns;
  }

  boolean isFinished() {
    return batches.isFinished();
  }

  void abort() {
    batches.abort();
  }

  // ---- Cursor ----

  @Override
  public boolean next() throws SQLException {
    synchronized (lock) {
      return advance();
    }
  }

  private boolean advance() throws SQLException {
    checkOpen();
    if (afterLast) {
      return false;
    }
    if (maxRows > 0 && rowNumber >= maxRows) {
      return finish();
    }
    if (rowInBatch + 1 < batchRows) {
      rowInBatch++;
      rowNumber++;
      return true;
    }
    if (!loadNonEmptyBatch()) {
      return finish();
    }
    rowInBatch = 0;
    rowNumber++;
    return true;
  }

  /** Loads the first batch with rows, if any, before the first {@link #next()}. */
  void readAhead() throws SQLException {
    synchronized (lock) {
      loadNonEmptyBatch();
    }
  }

  private boolean loadNonEmptyBatch() throws SQLException {
    while (batches.next()) {
      VectorSchemaRoot root = batches.root();
      if (root.getRowCount() > 0) {
        bind(root);
        return true;
      }
    }
    batchRows = 0;
    accessors = null;
    return false;
  }

  private void bind(VectorSchemaRoot root) {
    bindAccessors(root);
    batchRows = root.getRowCount();
    rowInBatch = -1;
  }

  private void bindAccessors(VectorSchemaRoot root) {
    List<FieldVector> vectors = root.getFieldVectors();
    Accessor[] bound = new Accessor[vectors.size()];
    for (int i = 0; i < bound.length; i++) {
      bound[i] = Accessors.create(vectors.get(i), batches.dictionaries());
    }
    accessors = bound;
    boundGeneration = batches.generation();
  }

  private boolean finish() {
    afterLast = true;
    accessors = null;
    if (!batches.isFinished()) {
      // maxRows stopped the reading: let the server stop a query (a write still runs to its end)
      // rather than hold the session.
      batches.abort();
    }
    if (statement != null && statement.closesOnCompletion()) {
      close();
    }
    return false;
  }

  private Accessor accessor(int column) throws SQLException {
    checkOpen();
    if (accessors == null || rowInBatch < 0) {
      throw new SQLException(
          afterLast ? "The cursor is after the last row" : "Call next() before reading a row",
          "24000");
    }
    if (column < 1 || column > accessors.length) {
      throw new SQLException(
          "Column " + column + " does not exist; there are " + accessors.length, "07009");
    }
    if (boundGeneration != batches.generation()) {
      bindAccessors(batches.root());
    }
    return accessors[column - 1];
  }

  /** Returns the accessor for a non-null value, or {@code null} after recording a null. */
  private Accessor value(int column) throws SQLException {
    Accessor accessor = accessor(column);
    wasNull = accessor.isNull(rowInBatch);
    return wasNull ? null : accessor;
  }

  private void checkOpen() throws SQLException {
    if (closed) {
      throw new SQLException("The result set is closed", "24000");
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    accessors = null;
    batches.close();
    if (onClose != null) {
      onClose.run();
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public boolean wasNull() throws SQLException {
    checkOpen();
    return wasNull;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    checkOpen();
    Integer index = columnLabel == null ? null : labels.get(columnLabel.toLowerCase(Locale.ROOT));
    if (index == null) {
      throw new SQLException("No column named '" + columnLabel + "'", "42S22");
    }
    return index;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkOpen();
    return new ChargewireResultSetMetaData(columns);
  }

  @Override
  public Statement getStatement() throws SQLException {
    checkOpen();
    return statement;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    synchronized (lock) {
      checkOpen();
      if (rowNumber > 0 || afterLast) {
        return false;
      }
      return batchRows > 0 || loadNonEmptyBatch();
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkOpen();
    return afterLast && rowNumber > 0;
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkOpen();
    return rowNumber == 1 && !afterLast;
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "isLast is not supported on this forward-only result set", "0A000");
  }

  @Override
  public int getRow() throws SQLException {
    checkOpen();
    return afterLast || rowInBatch < 0 ? 0 : (int) Math.min(rowNumber, Integer.MAX_VALUE);
  }

  private static SQLException forwardOnly() {
    return new SQLException("The result set is forward-only", "24000");
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw forwardOnly();
  }

  @Override
  public void afterLast() throws SQLException {
    throw forwardOnly();
  }

  @Override
  public boolean first() throws SQLException {
    throw forwardOnly();
  }

  @Override
  public boolean last() throws SQLException {
    throw forwardOnly();
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw forwardOnly();
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw forwardOnly();
  }

  @Override
  public boolean previous() throws SQLException {
    throw forwardOnly();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkOpen();
    if (direction != FETCH_FORWARD) {
      throw forwardOnly();
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkOpen();
    return FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkOpen();
    if (rows < 0) {
      throw new SQLException("The fetch size must not be negative", "HY024");
    }
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkOpen();
    return fetchSize;
  }

  @Override
  public int getType() throws SQLException {
    checkOpen();
    return TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    checkOpen();
    return CONCUR_READ_ONLY;
  }

  @Override
  public int getHoldability() throws SQLException {
    checkOpen();
    return HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkOpen();
    return warnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkOpen();
    warnings = null;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Named cursors are not supported", "0A000");
  }

  // ---- Getters by index ----

  @Override
  public String getString(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getString(rowInBatch);
    }
  }

  @Override
  public boolean getBoolean(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor != null && accessor.getBoolean(rowInBatch);
    }
  }

  @Override
  public byte getByte(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? 0 : accessor.getByte(rowInBatch);
    }
  }

  @Override
  public short getShort(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? 0 : accessor.getShort(rowInBatch);
    }
  }

  @Override
  public int getInt(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? 0 : accessor.getInt(rowInBatch);
    }
  }

  @Override
  public long getLong(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? 0 : accessor.getLong(rowInBatch);
    }
  }

  @Override
  public float getFloat(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? 0 : accessor.getFloat(rowInBatch);
    }
  }

  @Override
  public double getDouble(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? 0 : accessor.getDouble(rowInBatch);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getBigDecimal(rowInBatch);
    }
  }

  /** {@inheritDoc} Rounds half up to the scale. */
  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int column, int scale) throws SQLException {
    BigDecimal value = getBigDecimal(column);
    return value == null ? null : value.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public byte[] getBytes(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getBytes(rowInBatch);
    }
  }

  @Override
  public Date getDate(int column) throws SQLException {
    return getDate(column, (Calendar) null);
  }

  @Override
  public Date getDate(int column, Calendar calendar) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getDate(rowInBatch, calendar);
    }
  }

  @Override
  public Time getTime(int column) throws SQLException {
    return getTime(column, (Calendar) null);
  }

  @Override
  public Time getTime(int column, Calendar calendar) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getTime(rowInBatch, calendar);
    }
  }

  @Override
  public Timestamp getTimestamp(int column) throws SQLException {
    return getTimestamp(column, (Calendar) null);
  }

  @Override
  public Timestamp getTimestamp(int column, Calendar calendar) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getTimestamp(rowInBatch, calendar);
    }
  }

  @Override
  public InputStream getAsciiStream(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getAsciiStream(rowInBatch);
    }
  }

  /** {@inheritDoc} The stream is UTF-16, high byte first, as JDBC defines it. */
  @Override
  @Deprecated
  public InputStream getUnicodeStream(int column) throws SQLException {
    String text = getString(column);
    return text == null ? null : new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_16BE));
  }

  @Override
  public InputStream getBinaryStream(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getBinaryStream(rowInBatch);
    }
  }

  @Override
  public Reader getCharacterStream(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getCharacterStream(rowInBatch);
    }
  }

  @Override
  public Object getObject(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getObject(rowInBatch);
    }
  }

  @Override
  public Object getObject(int column, Map<String, Class<?>> map) throws SQLException {
    if (map != null && !map.isEmpty()) {
      throw new SQLFeatureNotSupportedException("Custom type maps are not supported", "0A000");
    }
    return getObject(column);
  }

  @Override
  public <T> T getObject(int column, Class<T> type) throws SQLException {
    synchronized (lock) {
      if (type == null) {
        throw new SQLException("The type must not be null", "HY009");
      }
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getObject(rowInBatch, type);
    }
  }

  @Override
  public Array getArray(int column) throws SQLException {
    synchronized (lock) {
      Accessor accessor = value(column);
      return accessor == null ? null : accessor.getArray(rowInBatch);
    }
  }

  @Override
  public Blob getBlob(int column) throws SQLException {
    byte[] bytes = getBytes(column);
    return bytes == null ? null : new SerialBlob(bytes);
  }

  @Override
  public Clob getClob(int column) throws SQLException {
    String text = getString(column);
    return text == null ? null : new SerialClob(text.toCharArray());
  }

  @Override
  public NClob getNClob(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("NClob is not supported; use getString", "0A000");
  }

  @Override
  public String getNString(int column) throws SQLException {
    return getString(column);
  }

  @Override
  public Reader getNCharacterStream(int column) throws SQLException {
    return getCharacterStream(column);
  }

  @Override
  public URL getURL(int column) throws SQLException {
    String text = getString(column);
    try {
      return text == null ? null : new URL(text);
    } catch (MalformedURLException e) {
      throw new SQLException("'" + text + "' is not a URL", "22018", e);
    }
  }

  @Override
  public Ref getRef(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("REF values are not supported", "0A000");
  }

  @Override
  public RowId getRowId(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("ROWID values are not supported", "0A000");
  }

  @Override
  public SQLXML getSQLXML(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("SQLXML values are not supported", "0A000");
  }

  // ---- Getters by label ----

  @Override
  public String getString(String label) throws SQLException {
    return getString(findColumn(label));
  }

  @Override
  public boolean getBoolean(String label) throws SQLException {
    return getBoolean(findColumn(label));
  }

  @Override
  public byte getByte(String label) throws SQLException {
    return getByte(findColumn(label));
  }

  @Override
  public short getShort(String label) throws SQLException {
    return getShort(findColumn(label));
  }

  @Override
  public int getInt(String label) throws SQLException {
    return getInt(findColumn(label));
  }

  @Override
  public long getLong(String label) throws SQLException {
    return getLong(findColumn(label));
  }

  @Override
  public float getFloat(String label) throws SQLException {
    return getFloat(findColumn(label));
  }

  @Override
  public double getDouble(String label) throws SQLException {
    return getDouble(findColumn(label));
  }

  /** {@inheritDoc} Rounds half up to the scale. */
  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String label, int scale) throws SQLException {
    return getBigDecimal(findColumn(label), scale);
  }

  @Override
  public BigDecimal getBigDecimal(String label) throws SQLException {
    return getBigDecimal(findColumn(label));
  }

  @Override
  public byte[] getBytes(String label) throws SQLException {
    return getBytes(findColumn(label));
  }

  @Override
  public Date getDate(String label) throws SQLException {
    return getDate(findColumn(label));
  }

  @Override
  public Date getDate(String label, Calendar calendar) throws SQLException {
    return getDate(findColumn(label), calendar);
  }

  @Override
  public Time getTime(String label) throws SQLException {
    return getTime(findColumn(label));
  }

  @Override
  public Time getTime(String label, Calendar calendar) throws SQLException {
    return getTime(findColumn(label), calendar);
  }

  @Override
  public Timestamp getTimestamp(String label) throws SQLException {
    return getTimestamp(findColumn(label));
  }

  @Override
  public Timestamp getTimestamp(String label, Calendar calendar) throws SQLException {
    return getTimestamp(findColumn(label), calendar);
  }

  @Override
  public InputStream getAsciiStream(String label) throws SQLException {
    return getAsciiStream(findColumn(label));
  }

  /** {@inheritDoc} The stream is UTF-16, high byte first, as JDBC defines it. */
  @Override
  @Deprecated
  public InputStream getUnicodeStream(String label) throws SQLException {
    return getUnicodeStream(findColumn(label));
  }

  @Override
  public InputStream getBinaryStream(String label) throws SQLException {
    return getBinaryStream(findColumn(label));
  }

  @Override
  public Reader getCharacterStream(String label) throws SQLException {
    return getCharacterStream(findColumn(label));
  }

  @Override
  public Object getObject(String label) throws SQLException {
    return getObject(findColumn(label));
  }

  @Override
  public Object getObject(String label, Map<String, Class<?>> map) throws SQLException {
    return getObject(findColumn(label), map);
  }

  @Override
  public <T> T getObject(String label, Class<T> type) throws SQLException {
    return getObject(findColumn(label), type);
  }

  @Override
  public Array getArray(String label) throws SQLException {
    return getArray(findColumn(label));
  }

  @Override
  public Blob getBlob(String label) throws SQLException {
    return getBlob(findColumn(label));
  }

  @Override
  public Clob getClob(String label) throws SQLException {
    return getClob(findColumn(label));
  }

  @Override
  public NClob getNClob(String label) throws SQLException {
    return getNClob(findColumn(label));
  }

  @Override
  public String getNString(String label) throws SQLException {
    return getNString(findColumn(label));
  }

  @Override
  public Reader getNCharacterStream(String label) throws SQLException {
    return getNCharacterStream(findColumn(label));
  }

  @Override
  public URL getURL(String label) throws SQLException {
    return getURL(findColumn(label));
  }

  @Override
  public Ref getRef(String label) throws SQLException {
    return getRef(findColumn(label));
  }

  @Override
  public RowId getRowId(String label) throws SQLException {
    return getRowId(findColumn(label));
  }

  @Override
  public SQLXML getSQLXML(String label) throws SQLException {
    return getSQLXML(findColumn(label));
  }

  // ---- Updates: the result set is read-only ----

  private static SQLException readOnly() {
    return new SQLFeatureNotSupportedException("The result set is read-only", "0A000");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    checkOpen();
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    checkOpen();
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    checkOpen();
    return false;
  }

  @Override
  public void insertRow() throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateRow() throws SQLException {
    throw readOnly();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw readOnly();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw readOnly();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw readOnly();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw readOnly();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNull(int column) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBoolean(int column, boolean x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateByte(int column, byte x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateShort(int column, short x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateInt(int column, int x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateLong(int column, long x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateFloat(int column, float x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateDouble(int column, double x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBigDecimal(int column, BigDecimal x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateString(int column, String x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBytes(int column, byte[] x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateDate(int column, Date x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateTime(int column, Time x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateTimestamp(int column, Timestamp x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateAsciiStream(int column, InputStream x, int length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBinaryStream(int column, InputStream x, int length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateCharacterStream(int column, Reader x, int length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateObject(int column, Object x, int scaleOrLength) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateObject(int column, Object x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNull(String label) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBoolean(String label, boolean x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateByte(String label, byte x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateShort(String label, short x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateInt(String label, int x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateLong(String label, long x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateFloat(String label, float x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateDouble(String label, double x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBigDecimal(String label, BigDecimal x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateString(String label, String x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBytes(String label, byte[] x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateDate(String label, Date x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateTime(String label, Time x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateTimestamp(String label, Timestamp x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateAsciiStream(String label, InputStream x, int length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBinaryStream(String label, InputStream x, int length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateCharacterStream(String label, Reader reader, int length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateObject(String label, Object x, int scaleOrLength) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateObject(String label, Object x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateRef(int column, Ref x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateRef(String label, Ref x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBlob(int column, Blob x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBlob(String label, Blob x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateClob(int column, Clob x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateClob(String label, Clob x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateArray(int column, Array x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateArray(String label, Array x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateRowId(int column, RowId x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateRowId(String label, RowId x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNString(int column, String nString) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNString(String label, String nString) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNClob(int column, NClob nClob) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNClob(String label, NClob nClob) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateSQLXML(int column, SQLXML xmlObject) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateSQLXML(String label, SQLXML xmlObject) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNCharacterStream(int column, Reader x, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNCharacterStream(String label, Reader reader, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateAsciiStream(int column, InputStream x, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBinaryStream(int column, InputStream x, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateCharacterStream(int column, Reader x, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateAsciiStream(String label, InputStream x, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBinaryStream(String label, InputStream x, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateCharacterStream(String label, Reader reader, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBlob(int column, InputStream inputStream, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBlob(String label, InputStream inputStream, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateClob(int column, Reader reader, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateClob(String label, Reader reader, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNClob(int column, Reader reader, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNClob(String label, Reader reader, long length) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNCharacterStream(int column, Reader x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNCharacterStream(String label, Reader reader) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateAsciiStream(int column, InputStream x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBinaryStream(int column, InputStream x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateCharacterStream(int column, Reader x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateAsciiStream(String label, InputStream x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBinaryStream(String label, InputStream x) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateCharacterStream(String label, Reader reader) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBlob(int column, InputStream inputStream) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateBlob(String label, InputStream inputStream) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateClob(int column, Reader reader) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateClob(String label, Reader reader) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNClob(int column, Reader reader) throws SQLException {
    throw readOnly();
  }

  @Override
  public void updateNClob(String label, Reader reader) throws SQLException {
    throw readOnly();
  }

  // ---- Wrapper ----

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
