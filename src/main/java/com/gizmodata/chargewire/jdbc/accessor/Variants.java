package com.gizmodata.chargewire.jdbc.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Decodes the Variant binary encoding (the Parquet and Arrow {@code arrow.parquet.variant} format:
 * a metadata dictionary of field names, and a self-describing value) into Java values: {@code
 * null}, {@link Boolean}, {@link Byte}, {@link Short}, {@link Integer}, {@link Long}, {@link
 * Float}, {@link Double}, {@link BigDecimal}, {@link String}, {@code byte[]}, {@link UUID}, {@link
 * LocalDate}, {@link LocalTime}, {@link LocalDateTime} (a timestamp without a zone), {@link
 * OffsetDateTime} at UTC (a timestamp with one), {@link List} and {@link Map} (an object's fields,
 * in the order the encoding keeps them: by name).
 */
final class Variants {
  private Variants() {}

  /**
   * Decodes one value.
   *
   * @param metadata the value's metadata: the dictionary of field names
   * @param value the encoded value
   * @return the Java value, {@code null} for a variant null
   * @throws SQLException if the bytes are not a Variant this driver reads
   */
  static Object decode(byte[] metadata, byte[] value) throws SQLException {
    try {
      return new Variants.Reader(metadata, value).read(0);
    } catch (IndexOutOfBoundsException | ArithmeticException e) {
      throw new SQLException("The server sent a VARIANT value the driver can't read", "08P01", e);
    }
  }

  private static final class Reader {
    private final byte[] metadata;
    private final byte[] value;
    private final int keyOffsetSize;
    private final int keyCount;

    Reader(byte[] metadata, byte[] value) throws SQLException {
      this.metadata = metadata;
      this.value = value;
      int header = metadata[0] & 0xFF;
      if ((header & 0x0F) != 1) {
        throw new SQLException(
            "The server sent a VARIANT of version " + (header & 0x0F) + "; the driver reads 1",
            "0A000");
      }
      this.keyOffsetSize = ((header >> 6) & 0x3) + 1;
      this.keyCount = Math.toIntExact(unsigned(metadata, 1, keyOffsetSize));
    }

    private static long unsigned(byte[] bytes, int at, int size) {
      long number = 0;
      for (int i = size - 1; i >= 0; i--) {
        number = (number << 8) | (bytes[at + i] & 0xFF);
      }
      return number;
    }

    private static long signed(byte[] bytes, int at, int size) {
      long number = bytes[at + size - 1];
      for (int i = size - 2; i >= 0; i--) {
        number = (number << 8) | (bytes[at + i] & 0xFF);
      }
      return number;
    }

    private String key(int id) {
      if (id < 0 || id >= keyCount) {
        throw new IndexOutOfBoundsException("field name " + id);
      }
      int offsets = 1 + keyOffsetSize;
      int strings = offsets + (keyCount + 1) * keyOffsetSize;
      int start = Math.toIntExact(unsigned(metadata, offsets + id * keyOffsetSize, keyOffsetSize));
      int end =
          Math.toIntExact(unsigned(metadata, offsets + (id + 1) * keyOffsetSize, keyOffsetSize));
      return new String(metadata, strings + start, end - start, StandardCharsets.UTF_8);
    }

    Object read(int at) throws SQLException {
      int first = value[at] & 0xFF;
      int header = first >> 2;
      switch (first & 0x3) {
        case 0:
          return primitive(header, at + 1);
        case 1:
          return new String(value, at + 1, header, StandardCharsets.UTF_8);
        case 2:
          return object(header, at + 1);
        default:
          return array(header, at + 1);
      }
    }

    private Object primitive(int type, int at) throws SQLException {
      switch (type) {
        case 0:
          return null;
        case 1:
          return Boolean.TRUE;
        case 2:
          return Boolean.FALSE;
        case 3:
          return value[at];
        case 4:
          return (short) signed(value, at, 2);
        case 5:
          return (int) signed(value, at, 4);
        case 6:
          return signed(value, at, 8);
        case 7:
          return Double.longBitsToDouble(signed(value, at, 8));
        case 8:
          return BigDecimal.valueOf(signed(value, at + 1, 4), value[at] & 0xFF);
        case 9:
          return BigDecimal.valueOf(signed(value, at + 1, 8), value[at] & 0xFF);
        case 10:
          byte[] big = new byte[16];
          for (int i = 0; i < 16; i++) {
            big[i] = value[at + 16 - i];
          }
          return new BigDecimal(new BigInteger(big), value[at] & 0xFF);
        case 11:
          return LocalDate.ofEpochDay(signed(value, at, 4));
        case 12:
          return instant(signed(value, at, 8), 1_000_000L).atOffset(ZoneOffset.UTC);
        case 13:
          return LocalDateTime.ofInstant(instant(signed(value, at, 8), 1_000_000L), ZoneOffset.UTC);
        case 14:
          return Float.intBitsToFloat((int) signed(value, at, 4));
        case 15:
          int length = Math.toIntExact(unsigned(value, at, 4));
          return Arrays.copyOfRange(value, at + 4, at + 4 + length);
        case 16:
          return new String(
              value, at + 4, Math.toIntExact(unsigned(value, at, 4)), StandardCharsets.UTF_8);
        case 17:
          return LocalTime.ofNanoOfDay(Math.multiplyExact(signed(value, at, 8), 1_000L));
        case 18:
          return instant(signed(value, at, 8), 1_000_000_000L).atOffset(ZoneOffset.UTC);
        case 19:
          return LocalDateTime.ofInstant(
              instant(signed(value, at, 8), 1_000_000_000L), ZoneOffset.UTC);
        case 20:
          return new UUID(bigEndian(at), bigEndian(at + 8));
        default:
          throw new SQLException(
              "The server sent a VARIANT value of primitive type "
                  + type
                  + ", which the driver does not know; cast it in SQL (to VARCHAR or JSON)",
              "0A000");
      }
    }

    private long bigEndian(int at) {
      long number = 0;
      for (int i = 0; i < 8; i++) {
        number = (number << 8) | (value[at + i] & 0xFF);
      }
      return number;
    }

    private static Instant instant(long sinceEpoch, long perSecond) {
      return Instant.ofEpochSecond(
          Math.floorDiv(sinceEpoch, perSecond),
          Math.floorMod(sinceEpoch, perSecond) * (1_000_000_000L / perSecond));
    }

    private Map<String, Object> object(int header, int at) throws SQLException {
      int offsetSize = (header & 0x3) + 1;
      int idSize = ((header >> 2) & 0x3) + 1;
      int countSize = (header >> 4 & 1) == 1 ? 4 : 1;
      int count = Math.toIntExact(unsigned(value, at, countSize));
      int ids = at + countSize;
      int offsets = ids + count * idSize;
      int data = offsets + (count + 1) * offsetSize;
      Map<String, Object> fields = new LinkedHashMap<>();
      for (int i = 0; i < count; i++) {
        int id = Math.toIntExact(unsigned(value, ids + i * idSize, idSize));
        int offset = Math.toIntExact(unsigned(value, offsets + i * offsetSize, offsetSize));
        fields.put(key(id), read(data + offset));
      }
      return fields;
    }

    private List<Object> array(int header, int at) throws SQLException {
      int offsetSize = (header & 0x3) + 1;
      int countSize = (header >> 2 & 1) == 1 ? 4 : 1;
      int count = Math.toIntExact(unsigned(value, at, countSize));
      int offsets = at + countSize;
      int data = offsets + (count + 1) * offsetSize;
      List<Object> elements = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        int offset = Math.toIntExact(unsigned(value, offsets + i * offsetSize, offsetSize));
        elements.add(read(data + offset));
      }
      return elements;
    }
  }
}
