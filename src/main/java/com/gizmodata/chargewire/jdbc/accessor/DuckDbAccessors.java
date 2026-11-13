package com.gizmodata.chargewire.jdbc.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.function.IntFunction;
import org.apache.arrow.vector.ValueVector;

/**
 * Accessors for the types DuckDB sends in encodings of its own, as {@code arrow.opaque} with vendor
 * {@code DuckDB}: BIGNUM (always), and BIT and TIME WITH TIME ZONE (with {@code
 * arrow_lossless_conversion}; without it they arrive as plain binary and as a time without its
 * offset, which nothing can tell from BLOB and TIME).
 */
final class DuckDbAccessors {
  private static final long MICROS_PER_DAY = 86_400_000_000L;

  /** The largest offset from UTC a TIME WITH TIME ZONE holds, in seconds: 15:59:59. */
  private static final int MAX_OFFSET_SECONDS = 16 * 60 * 60 - 1;

  private DuckDbAccessors() {}

  private static SQLException malformed(String type, byte[] value) {
    return new SQLException(
        "The server sent a " + type + " value of " + value.length + " bytes the driver can't read",
        "08P01");
  }

  /**
   * Decodes a BIGNUM: three header bytes (the top bit set for zero and above, then the number of
   * magnitude bytes), then the magnitude, most significant byte first; a negative number has every
   * bit of both inverted, so the encodings sort as the numbers do.
   *
   * @param value the encoded number
   * @return the number
   * @throws SQLException if the bytes are not a BIGNUM
   */
  static BigInteger bignum(byte[] value) throws SQLException {
    if (value.length < 4) {
      throw malformed("BIGNUM", value);
    }
    boolean negative = (value[0] & 0x80) == 0;
    int size = ((value[0] & 0x7F) << 16) | ((value[1] & 0xFF) << 8) | (value[2] & 0xFF);
    if (negative) {
      size = ~size & 0x7FFFFF;
    }
    if (size != value.length - 3) {
      throw malformed("BIGNUM", value);
    }
    byte[] magnitude = new byte[size];
    for (int i = 0; i < size; i++) {
      magnitude[i] = negative ? (byte) ~value[3 + i] : value[3 + i];
    }
    BigInteger number = new BigInteger(1, magnitude);
    return negative ? number.negate() : number;
  }

  /**
   * Decodes a BIT: one byte counting the padding bits at the front of the first data byte, then the
   * bits, most significant first.
   *
   * @param value the encoded bit string
   * @return the bits as {@code 0} and {@code 1} characters
   * @throws SQLException if the bytes are not a BIT
   */
  static String bits(byte[] value) throws SQLException {
    if (value.length < 1 || (value[0] & 0xFF) > 7 || (value.length == 1 && value[0] != 0)) {
      throw malformed("BIT", value);
    }
    int padding = value[0];
    int end = (value.length - 1) * 8;
    StringBuilder bits = new StringBuilder(Math.max(end - padding, 0));
    for (int i = padding; i < end; i++) {
      bits.append((value[1 + i / 8] >> (7 - i % 8) & 1) == 0 ? '0' : '1');
    }
    return bits.toString();
  }

  /**
   * BIGNUM, DuckDB's integer of any size, read as {@link BigDecimal} like HUGEINT and UBIGINT, so
   * every value is exact.
   */
  static final class BignumAccessor extends Accessor {
    private final IntFunction<byte[]> bytes;

    BignumAccessor(ValueVector vector, IntFunction<byte[]> bytes) {
      super(vector);
      this.bytes = bytes;
    }

    @Override
    public BigDecimal getObject(int row) throws SQLException {
      return new BigDecimal(bignum(bytes.apply(row)));
    }

    @Override
    public BigDecimal getBigDecimal(int row) throws SQLException {
      return getObject(row);
    }

    @Override
    public String getString(int row) throws SQLException {
      return bignum(bytes.apply(row)).toString();
    }

    @Override
    public boolean getBoolean(int row) throws SQLException {
      return bignum(bytes.apply(row)).signum() != 0;
    }

    @Override
    public long getLong(int row) throws SQLException {
      return toLong(getObject(row));
    }

    @Override
    public double getDouble(int row) throws SQLException {
      return getObject(row).doubleValue();
    }

    /** {@inheritDoc} The value as the server encoded it, as before the driver read BIGNUM. */
    @Override
    public byte[] getBytes(int row) {
      return bytes.apply(row);
    }

    @Override
    protected String typeDescription() {
      return "BIGNUM";
    }
  }

  /** BIT, a string of bits, read as its text: {@code 10101}. */
  static final class BitAccessor extends Accessor {
    private final IntFunction<byte[]> bytes;

    BitAccessor(ValueVector vector, IntFunction<byte[]> bytes) {
      super(vector);
      this.bytes = bytes;
    }

    @Override
    public String getObject(int row) throws SQLException {
      return bits(bytes.apply(row));
    }

    @Override
    public String getString(int row) throws SQLException {
      return getObject(row);
    }

    /** {@inheritDoc} A string of one bit reads as a boolean. */
    @Override
    public boolean getBoolean(int row) throws SQLException {
      String bits = getObject(row);
      if (bits.length() != 1) {
        throw cannotConvert("boolean");
      }
      return bits.charAt(0) == '1';
    }

    /** {@inheritDoc} The value as the server encoded it, as before the driver read BIT. */
    @Override
    public byte[] getBytes(int row) {
      return bytes.apply(row);
    }

    @Override
    protected String typeDescription() {
      return "BIT";
    }
  }

  /**
   * TIME WITH TIME ZONE: a time of day and its offset from UTC, in eight bytes (little-endian): the
   * microseconds of the day in the upper 40 bits, and 15:59:59 minus the offset in seconds in the
   * lower 24.
   */
  static final class TimeTzAccessor extends Accessor {
    private final IntFunction<byte[]> bytes;

    TimeTzAccessor(ValueVector vector, IntFunction<byte[]> bytes) {
      super(vector);
      this.bytes = bytes;
    }

    private long bits(int row) throws SQLException {
      byte[] value = bytes.apply(row);
      if (value.length != 8) {
        throw malformed("TIME WITH TIME ZONE", value);
      }
      long bits = 0;
      for (int i = 7; i >= 0; i--) {
        bits = (bits << 8) | (value[i] & 0xFF);
      }
      return bits;
    }

    private static long micros(long bits) {
      return bits >>> 24;
    }

    private static ZoneOffset offset(long bits) {
      return ZoneOffset.ofTotalSeconds(MAX_OFFSET_SECONDS - (int) (bits & 0xFFFFFF));
    }

    private static String offsetText(ZoneOffset offset) {
      return offset.getId().replace("Z", "+00:00");
    }

    @Override
    public OffsetTime getOffsetTime(int row) throws SQLException {
      long bits = bits(row);
      return OffsetTime.of(localTime(bits), offset(bits));
    }

    private LocalTime localTime(long bits) throws SQLException {
      long micros = micros(bits);
      if (micros >= MICROS_PER_DAY) {
        throw new SQLDataException(
            "The time "
                + TemporalAccessors.formatTime(micros * 1_000L)
                + offsetText(offset(bits))
                + " has no java.time form",
            "22008");
      }
      return LocalTime.ofNanoOfDay(micros * 1_000L);
    }

    @Override
    public Object getObject(int row) throws SQLException {
      return getOffsetTime(row);
    }

    /** {@inheritDoc} The time of day as written, without its offset. */
    @Override
    public LocalTime getLocalTime(int row) throws SQLException {
      return localTime(bits(row));
    }

    /**
     * {@inheritDoc} The value is a time of day at its own offset, so the {@link Time} is that
     * moment on 1970-01-01, and a {@link Calendar} does not shift it.
     */
    @Override
    public Time getTime(int row, Calendar calendar) throws SQLException {
      long bits = bits(row);
      return new Time(
          LocalDate.EPOCH
              .atStartOfDay()
              .plusNanos(micros(bits) * 1_000L)
              .toInstant(offset(bits))
              .toEpochMilli());
    }

    /** {@inheritDoc} {@code HH:mm:ss}, the fraction as far as needed, and the offset. */
    @Override
    public String getString(int row) throws SQLException {
      long bits = bits(row);
      return TemporalAccessors.formatTime(micros(bits) * 1_000L) + offsetText(offset(bits));
    }

    /** {@inheritDoc} The value as the server encoded it. */
    @Override
    public byte[] getBytes(int row) {
      return bytes.apply(row);
    }

    @Override
    protected String typeDescription() {
      return "TIME WITH TIME ZONE";
    }
  }
}
