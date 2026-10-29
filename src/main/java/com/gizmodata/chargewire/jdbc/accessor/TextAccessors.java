package com.gizmodata.chargewire.jdbc.accessor;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.UUID;
import java.util.function.IntFunction;
import org.apache.arrow.vector.ValueVector;

/** Accessors for strings, binaries and UUIDs. */
final class TextAccessors {
  private TextAccessors() {}

  /** UTF-8 strings. Numbers, booleans, dates and times parse from their text. */
  static final class TextAccessor extends Accessor {
    private final IntFunction<byte[]> bytes;

    TextAccessor(ValueVector vector, IntFunction<byte[]> bytes) {
      super(vector);
      this.bytes = bytes;
    }

    @Override
    public String getObject(int row) {
      return getString(row);
    }

    @Override
    public String getString(int row) {
      return new String(bytes.apply(row), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBytes(int row) {
      return bytes.apply(row);
    }

    @Override
    public boolean getBoolean(int row) throws SQLException {
      String text = getString(row).trim();
      if ("true".equalsIgnoreCase(text) || "1".equals(text)) {
        return true;
      }
      if ("false".equalsIgnoreCase(text) || "0".equals(text)) {
        return false;
      }
      throw unparseable(text, "boolean", null);
    }

    @Override
    public long getLong(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException e) {
        throw unparseable(text, "long", e);
      }
    }

    @Override
    public double getDouble(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return Double.parseDouble(text);
      } catch (NumberFormatException e) {
        throw unparseable(text, "double", e);
      }
    }

    @Override
    public BigDecimal getBigDecimal(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return new BigDecimal(text);
      } catch (NumberFormatException e) {
        throw unparseable(text, "BigDecimal", e);
      }
    }

    @Override
    public LocalDate getLocalDate(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return LocalDate.parse(text);
      } catch (DateTimeParseException e) {
        throw unparseable(text, "date", e);
      }
    }

    @Override
    public LocalTime getLocalTime(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return LocalTime.parse(text);
      } catch (DateTimeParseException e) {
        throw unparseable(text, "time", e);
      }
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return LocalDateTime.parse(text.replace(' ', 'T'));
      } catch (DateTimeParseException e) {
        throw unparseable(text, "timestamp", e);
      }
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int row) throws SQLException {
      String text = getString(row).trim();
      try {
        return OffsetDateTime.parse(text.replace(' ', 'T'));
      } catch (DateTimeParseException e) {
        throw unparseable(text, "timestamp with time zone", e);
      }
    }
  }

  /** Binary values; their text form decodes them as UTF-8. */
  static final class BinaryAccessor extends Accessor {
    private final IntFunction<byte[]> bytes;

    BinaryAccessor(ValueVector vector, IntFunction<byte[]> bytes) {
      super(vector);
      this.bytes = bytes;
    }

    @Override
    public byte[] getObject(int row) {
      return bytes.apply(row);
    }

    @Override
    public byte[] getBytes(int row) {
      return bytes.apply(row);
    }

    @Override
    public String getString(int row) {
      return new String(bytes.apply(row), StandardCharsets.UTF_8);
    }
  }

  /** Arrow's canonical UUID extension: 16 bytes, big-endian. */
  static final class UuidAccessor extends Accessor {
    private final IntFunction<byte[]> bytes;

    UuidAccessor(ValueVector vector, IntFunction<byte[]> bytes) {
      super(vector);
      this.bytes = bytes;
    }

    @Override
    public UUID getObject(int row) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes.apply(row));
      return new UUID(buffer.getLong(), buffer.getLong());
    }

    @Override
    public String getString(int row) {
      return getObject(row).toString().toLowerCase(Locale.ROOT);
    }

    @Override
    public byte[] getBytes(int row) {
      return bytes.apply(row);
    }

    @Override
    protected String typeDescription() {
      return "UUID";
    }
  }
}
