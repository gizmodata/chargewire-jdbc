package com.gizmodata.chargewire.jdbc.accessor;

import com.gizmodata.chargewire.jdbc.arrow.SqlTypes;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;

/** Accessors for nested and encoded types, and the null type. */
final class NestedAccessors {
  private NestedAccessors() {}

  /**
   * Renders a nested value as JSON text: the {@link java.sql.ResultSet#getString(int)} form of
   * lists, structs and maps.
   */
  static String render(Object value) {
    StringBuilder out = new StringBuilder();
    render(value, out);
    return out.toString();
  }

  static void render(Object value, StringBuilder out) {
    if (value == null) {
      out.append("null");
    } else if (value instanceof Number || value instanceof Boolean) {
      out.append(value instanceof BigDecimal ? ((BigDecimal) value).toPlainString() : value);
    } else if (value instanceof Map) {
      out.append('{');
      boolean first = true;
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        if (!first) {
          out.append(", ");
        }
        first = false;
        quote(String.valueOf(entry.getKey()), out);
        out.append(": ");
        render(entry.getValue(), out);
      }
      out.append('}');
    } else if (value instanceof List) {
      out.append('[');
      boolean first = true;
      for (Object element : (List<?>) value) {
        if (!first) {
          out.append(", ");
        }
        first = false;
        render(element, out);
      }
      out.append(']');
    } else if (value instanceof byte[]) {
      quote(Base64.getEncoder().encodeToString((byte[]) value), out);
    } else {
      quote(value.toString(), out);
    }
  }

  static void quote(String text, StringBuilder out) {
    out.append('"');
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      switch (c) {
        case '"':
          out.append("\\\"");
          break;
        case '\\':
          out.append("\\\\");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        default:
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    out.append('"');
  }

  /** Lists of any kind; {@link #getObject} returns a {@link List} of the elements' objects. */
  static final class ListAccessor extends Accessor {
    private final Accessor elements;
    private final Field elementField;

    ListAccessor(
        ValueVector vector,
        FieldVector elementVector,
        Field declaredElement,
        DictionaryProvider dictionaries) {
      super(vector);
      this.elements = Accessors.create(elementVector, declaredElement, dictionaries);
      this.elementField = elementVector.getField();
    }

    private long start(int row) {
      return vector instanceof LargeListVector
          ? ((LargeListVector) vector).getElementStartIndex(row)
          : ((BaseListVector) vector).getElementStartIndex(row);
    }

    private long end(int row) {
      return vector instanceof LargeListVector
          ? ((LargeListVector) vector).getElementEndIndex(row)
          : ((BaseListVector) vector).getElementEndIndex(row);
    }

    @Override
    public List<Object> getObject(int row) throws SQLException {
      List<Object> list = new ArrayList<>();
      for (long i = start(row); i < end(row); i++) {
        int index = Math.toIntExact(i);
        list.add(elements.isNull(index) ? null : elements.getObject(index));
      }
      return list;
    }

    @Override
    public String getString(int row) throws SQLException {
      StringBuilder out = new StringBuilder();
      appendJson(row, out);
      return out.toString();
    }

    @Override
    void appendJson(int row, StringBuilder out) throws SQLException {
      if (isNull(row)) {
        out.append("null");
        return;
      }
      out.append('[');
      for (long i = start(row); i < end(row); i++) {
        if (i > start(row)) {
          out.append(", ");
        }
        elements.appendJson(Math.toIntExact(i), out);
      }
      out.append(']');
    }

    @Override
    public Array getArray(int row) throws SQLException {
      return new ChargewireArray(
          SqlTypes.typeName(elementField),
          SqlTypes.jdbcType(elementField),
          getObject(row).toArray());
    }
  }

  /** Structs; {@link #getObject} returns a {@link Map} from field name to value, in order. */
  static final class StructAccessor extends Accessor {
    private final List<String> names = new ArrayList<>();
    private final List<Accessor> children = new ArrayList<>();

    StructAccessor(StructVector vector, Field declared, DictionaryProvider dictionaries) {
      super(vector);
      List<FieldVector> fields = vector.getChildrenFromFields();
      for (int i = 0; i < fields.size(); i++) {
        names.add(fields.get(i).getName());
        children.add(Accessors.create(fields.get(i), Accessors.child(declared, i), dictionaries));
      }
    }

    @Override
    public Map<String, Object> getObject(int row) throws SQLException {
      Map<String, Object> map = new LinkedHashMap<>();
      for (int i = 0; i < children.size(); i++) {
        Accessor child = children.get(i);
        map.put(names.get(i), child.isNull(row) ? null : child.getObject(row));
      }
      return map;
    }

    @Override
    public String getString(int row) throws SQLException {
      StringBuilder out = new StringBuilder();
      appendJson(row, out);
      return out.toString();
    }

    @Override
    void appendJson(int row, StringBuilder out) throws SQLException {
      if (isNull(row)) {
        out.append("null");
        return;
      }
      out.append('{');
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) {
          out.append(", ");
        }
        quote(names.get(i), out);
        out.append(": ");
        children.get(i).appendJson(row, out);
      }
      out.append('}');
    }

    @Override
    public Struct getStruct(int row) throws SQLException {
      return new ChargewireStruct(
          SqlTypes.typeName(vector.getField()), getObject(row).values().toArray());
    }
  }

  /**
   * VARIANT, a value that says its own type ({@code arrow.parquet.variant}: a struct of the
   * encoding's metadata and value). {@link #getObject} returns the Java value ({@link Variants});
   * its text is the value itself for a scalar and JSON for an object or an array.
   */
  static final class VariantAccessor extends Accessor {
    private final Accessor metadata;
    private final Accessor value;

    VariantAccessor(StructVector vector, DictionaryProvider dictionaries) {
      super(vector);
      this.metadata = Accessors.create(vector.getChild("metadata"), dictionaries);
      this.value = Accessors.create(vector.getChild("value"), dictionaries);
    }

    @Override
    public Object getObject(int row) throws SQLException {
      if (value.isNull(row)) {
        return null;
      }
      return Variants.decode(metadata.getBytes(row), value.getBytes(row));
    }

    @Override
    public String getString(int row) throws SQLException {
      Object decoded = getObject(row);
      if (decoded instanceof Map || decoded instanceof List) {
        return render(decoded);
      }
      if (decoded instanceof BigDecimal) {
        return ((BigDecimal) decoded).toPlainString();
      }
      if (decoded instanceof LocalDateTime) {
        return TemporalAccessors.formatDateTime((LocalDateTime) decoded);
      }
      if (decoded instanceof byte[]) {
        return Base64.getEncoder().encodeToString((byte[]) decoded);
      }
      return String.valueOf(decoded);
    }

    private Number number(int row, String target) throws SQLException {
      Object decoded = getObject(row);
      if (decoded instanceof Boolean) {
        return (Boolean) decoded ? 1 : 0;
      }
      if (decoded instanceof Number) {
        return (Number) decoded;
      }
      throw cannotConvert(target);
    }

    @Override
    public boolean getBoolean(int row) throws SQLException {
      Object decoded = getObject(row);
      if (decoded instanceof Boolean) {
        return (Boolean) decoded;
      }
      return number(row, "boolean").doubleValue() != 0;
    }

    @Override
    public long getLong(int row) throws SQLException {
      Number number = number(row, "long");
      return number instanceof BigDecimal || number instanceof Double || number instanceof Float
          ? toLong(getBigDecimal(row))
          : number.longValue();
    }

    @Override
    public double getDouble(int row) throws SQLException {
      return number(row, "double").doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int row) throws SQLException {
      Number number = number(row, "BigDecimal");
      return number instanceof BigDecimal ? (BigDecimal) number : new BigDecimal(number.toString());
    }

    @Override
    protected String typeDescription() {
      return "VARIANT";
    }
  }

  /** Maps; {@link #getObject} returns a {@link Map} in entry order. */
  static final class MapAccessor extends Accessor {
    private final Accessor keys;
    private final Accessor values;

    MapAccessor(MapVector vector, Field declared, DictionaryProvider dictionaries) {
      super(vector);
      StructVector entries = (StructVector) vector.getDataVector();
      List<FieldVector> fields = entries.getChildrenFromFields();
      Field declaredEntries = Accessors.child(declared, 0);
      this.keys =
          Accessors.create(fields.get(0), Accessors.child(declaredEntries, 0), dictionaries);
      this.values =
          Accessors.create(fields.get(1), Accessors.child(declaredEntries, 1), dictionaries);
    }

    @Override
    public Map<Object, Object> getObject(int row) throws SQLException {
      MapVector map = (MapVector) vector;
      Map<Object, Object> result = new LinkedHashMap<>();
      for (int i = map.getElementStartIndex(row); i < map.getElementEndIndex(row); i++) {
        result.put(keys.getObject(i), values.isNull(i) ? null : values.getObject(i));
      }
      return result;
    }

    @Override
    public String getString(int row) throws SQLException {
      StringBuilder out = new StringBuilder();
      appendJson(row, out);
      return out.toString();
    }

    @Override
    void appendJson(int row, StringBuilder out) throws SQLException {
      if (isNull(row)) {
        out.append("null");
        return;
      }
      MapVector map = (MapVector) vector;
      // As getObject has them: a key that comes twice is shown once, where it first stood, with
      // its last value.
      Map<String, Integer> entries = new LinkedHashMap<>();
      for (int i = map.getElementStartIndex(row); i < map.getElementEndIndex(row); i++) {
        entries.put(keys.jsonKey(i), i);
      }
      out.append('{');
      boolean first = true;
      for (Map.Entry<String, Integer> entry : entries.entrySet()) {
        if (!first) {
          out.append(", ");
        }
        first = false;
        quote(entry.getKey(), out);
        out.append(": ");
        values.appendJson(entry.getValue(), out);
      }
      out.append('}');
    }
  }

  /**
   * Sparse and dense unions: every getter reads the row's own member.
   *
   * <p>A sparse union's member is found by its position among the union's children, through the
   * type ids the stream's schema declares. Arrow Java's {@code UnionVector.getVectorByType} looks a
   * member up by the kind of its type instead: it has one slot per kind, so two INTEGER members are
   * one to it and a LIST or STRUCT member is a vector of its own making, and it cannot make the
   * vector of a kind that needs more than its name (a decimal's precision, a timestamp's zone, an
   * extension type), where it fails with a {@code NullPointerException}.
   */
  static final class UnionAccessor extends Accessor {
    private final DictionaryProvider dictionaries;
    private final Field declared;
    private final Map<Integer, Accessor> members = new HashMap<>();

    UnionAccessor(ValueVector vector, Field declared, DictionaryProvider dictionaries) {
      super(vector);
      this.dictionaries = dictionaries;
      this.declared =
          declared != null && declared.getType() instanceof ArrowType.Union ? declared : null;
    }

    private Accessor member(int row) {
      int typeId =
          vector instanceof UnionVector
              ? ((UnionVector) vector).getTypeValue(row)
              : ((DenseUnionVector) vector).getTypeId(row);
      return members.computeIfAbsent(typeId, this::memberOf);
    }

    private Accessor memberOf(int typeId) {
      if (vector instanceof UnionVector) {
        List<FieldVector> children = ((UnionVector) vector).getChildrenFromFields();
        // Without the declaration, the ids are the ones a vector built in Java has: its own.
        int[] ids =
            ((ArrowType.Union) (declared != null ? declared : vector.getField()).getType())
                .getTypeIds();
        for (int i = 0; i < children.size(); i++) {
          if ((ids == null || ids.length <= i ? i : ids[i]) == typeId) {
            return Accessors.create(children.get(i), Accessors.child(declared, i), dictionaries);
          }
        }
        // A row without a member (Arrow Java's null type id) is null.
        return new NullAccessor(vector);
      }
      ValueVector member = ((DenseUnionVector) vector).getVectorByType((byte) typeId);
      return member == null
          ? new NullAccessor(vector)
          : Accessors.create((FieldVector) member, dictionaries);
    }

    private int index(int row) {
      return vector instanceof UnionVector ? row : ((DenseUnionVector) vector).getOffset(row);
    }

    @Override
    public boolean isNull(int row) {
      return vector.isNull(row) || member(row).isNull(index(row));
    }

    @Override
    public Object getObject(int row) throws SQLException {
      return member(row).getObject(index(row));
    }

    @Override
    void appendJson(int row, StringBuilder out) throws SQLException {
      if (isNull(row)) {
        out.append("null");
      } else {
        member(row).appendJson(index(row), out);
      }
    }

    @Override
    String jsonKey(int row) throws SQLException {
      return member(row).jsonKey(index(row));
    }

    @Override
    public String getString(int row) throws SQLException {
      return member(row).getString(index(row));
    }

    @Override
    public boolean getBoolean(int row) throws SQLException {
      return member(row).getBoolean(index(row));
    }

    @Override
    public long getLong(int row) throws SQLException {
      return member(row).getLong(index(row));
    }

    @Override
    public double getDouble(int row) throws SQLException {
      return member(row).getDouble(index(row));
    }

    @Override
    public BigDecimal getBigDecimal(int row) throws SQLException {
      return member(row).getBigDecimal(index(row));
    }

    @Override
    public byte[] getBytes(int row) throws SQLException {
      return member(row).getBytes(index(row));
    }

    @Override
    public LocalDate getLocalDate(int row) throws SQLException {
      return member(row).getLocalDate(index(row));
    }

    @Override
    public LocalTime getLocalTime(int row) throws SQLException {
      return member(row).getLocalTime(index(row));
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) throws SQLException {
      return member(row).getLocalDateTime(index(row));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int row) throws SQLException {
      return member(row).getOffsetDateTime(index(row));
    }

    @Override
    public Date getDate(int row, Calendar calendar) throws SQLException {
      return member(row).getDate(index(row), calendar);
    }

    @Override
    public Time getTime(int row, Calendar calendar) throws SQLException {
      return member(row).getTime(index(row), calendar);
    }

    @Override
    public Timestamp getTimestamp(int row, Calendar calendar) throws SQLException {
      return member(row).getTimestamp(index(row), calendar);
    }

    @Override
    public Array getArray(int row) throws SQLException {
      return member(row).getArray(index(row));
    }

    @Override
    public Struct getStruct(int row) throws SQLException {
      return member(row).getStruct(index(row));
    }
  }

  /** Dictionary-encoded columns: every getter reads the dictionary entry the row points at. */
  static final class DictionaryAccessor extends Accessor {
    private final Accessor values;

    DictionaryAccessor(BaseIntVector indices, Accessor values) {
      super(indices);
      this.values = values;
    }

    private int index(int row) {
      return Math.toIntExact(((BaseIntVector) vector).getValueAsLong(row));
    }

    @Override
    public boolean isNull(int row) {
      return vector.isNull(row) || values.isNull(index(row));
    }

    @Override
    public Object getObject(int row) throws SQLException {
      return values.getObject(index(row));
    }

    @Override
    void appendJson(int row, StringBuilder out) throws SQLException {
      if (isNull(row)) {
        out.append("null");
      } else {
        values.appendJson(index(row), out);
      }
    }

    @Override
    String jsonKey(int row) throws SQLException {
      return values.jsonKey(index(row));
    }

    @Override
    public String getString(int row) throws SQLException {
      return values.getString(index(row));
    }

    @Override
    public boolean getBoolean(int row) throws SQLException {
      return values.getBoolean(index(row));
    }

    @Override
    public long getLong(int row) throws SQLException {
      return values.getLong(index(row));
    }

    @Override
    public double getDouble(int row) throws SQLException {
      return values.getDouble(index(row));
    }

    @Override
    public BigDecimal getBigDecimal(int row) throws SQLException {
      return values.getBigDecimal(index(row));
    }

    @Override
    public byte[] getBytes(int row) throws SQLException {
      return values.getBytes(index(row));
    }

    @Override
    public LocalDate getLocalDate(int row) throws SQLException {
      return values.getLocalDate(index(row));
    }

    @Override
    public LocalTime getLocalTime(int row) throws SQLException {
      return values.getLocalTime(index(row));
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) throws SQLException {
      return values.getLocalDateTime(index(row));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int row) throws SQLException {
      return values.getOffsetDateTime(index(row));
    }

    @Override
    public Date getDate(int row, Calendar calendar) throws SQLException {
      return values.getDate(index(row), calendar);
    }

    @Override
    public Time getTime(int row, Calendar calendar) throws SQLException {
      return values.getTime(index(row), calendar);
    }

    @Override
    public Timestamp getTimestamp(int row, Calendar calendar) throws SQLException {
      return values.getTimestamp(index(row), calendar);
    }

    @Override
    public Array getArray(int row) throws SQLException {
      return values.getArray(index(row));
    }

    @Override
    public Struct getStruct(int row) throws SQLException {
      return values.getStruct(index(row));
    }
  }

  /** The null type: every row is null. */
  static final class NullAccessor extends Accessor {
    NullAccessor(ValueVector vector) {
      super(vector);
    }

    @Override
    public boolean isNull(int row) {
      return true;
    }

    @Override
    public Object getObject(int row) {
      return null;
    }
  }

  /** Any other type: Arrow's own Java object for the value, with text as {@link String}. */
  static final class GenericAccessor extends Accessor {
    GenericAccessor(ValueVector vector) {
      super(vector);
    }

    @Override
    public Object getObject(int row) {
      return plain(vector.getObject(row));
    }

    @Override
    public String getString(int row) {
      Object value = getObject(row);
      return value instanceof List || value instanceof Map ? render(value) : String.valueOf(value);
    }

    private static Object plain(Object value) {
      if (value instanceof Text) {
        return value.toString();
      }
      if (value instanceof JsonStringArrayList) {
        List<Object> list = new ArrayList<>();
        for (Object element : (List<?>) value) {
          list.add(plain(element));
        }
        return list;
      }
      if (value instanceof JsonStringHashMap) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          map.put(plain(entry.getKey()), plain(entry.getValue()));
        }
        return map;
      }
      return value;
    }
  }
}
