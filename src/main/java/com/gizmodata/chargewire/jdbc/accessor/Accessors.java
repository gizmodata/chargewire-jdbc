package com.gizmodata.chargewire.jdbc.accessor;

import com.gizmodata.chargewire.jdbc.arrow.SqlTypes;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

/**
 * Creates the {@link Accessor} for a vector.
 *
 * <p>Internal API.
 */
public final class Accessors {
  private Accessors() {}

  /**
   * Creates the accessor for a column of the current batch.
   *
   * @param vector the column
   * @param dictionaries the stream's dictionaries, for dictionary-encoded columns, or {@code null}
   * @return the accessor
   */
  public static Accessor create(FieldVector vector, DictionaryProvider dictionaries) {
    DictionaryEncoding encoding = vector.getField().getDictionary();
    if (encoding != null && dictionaries != null) {
      Dictionary dictionary = dictionaries.lookup(encoding.getId());
      if (dictionary != null) {
        return new NestedAccessors.DictionaryAccessor(
            (BaseIntVector) vector, create(dictionary.getVector(), dictionaries));
      }
    }
    if (vector instanceof NullVector) {
      return new NestedAccessors.NullAccessor(vector);
    }
    if (vector instanceof BaseIntVector && vector.getField().getType() instanceof ArrowType.Int) {
      ArrowType.Int type = (ArrowType.Int) vector.getField().getType();
      return new NumberAccessors.IntAccessor(vector, type.getBitWidth(), type.getIsSigned());
    }
    if (vector instanceof Float2Vector
        || vector instanceof Float4Vector
        || vector instanceof Float8Vector) {
      return new NumberAccessors.FloatAccessor(vector);
    }
    if (vector instanceof DecimalVector || vector instanceof Decimal256Vector) {
      return new NumberAccessors.DecimalAccessor(vector);
    }
    if (vector instanceof BitVector) {
      return new NumberAccessors.BoolAccessor((BitVector) vector);
    }
    if (vector instanceof VarCharVector) {
      return new TextAccessors.TextAccessor(vector, ((VarCharVector) vector)::get);
    }
    if (vector instanceof LargeVarCharVector) {
      return new TextAccessors.TextAccessor(vector, ((LargeVarCharVector) vector)::get);
    }
    if (vector instanceof ViewVarCharVector) {
      return new TextAccessors.TextAccessor(vector, ((ViewVarCharVector) vector)::get);
    }
    if (vector instanceof VarBinaryVector) {
      return new TextAccessors.BinaryAccessor(vector, ((VarBinaryVector) vector)::get);
    }
    if (vector instanceof LargeVarBinaryVector) {
      return new TextAccessors.BinaryAccessor(vector, ((LargeVarBinaryVector) vector)::get);
    }
    if (vector instanceof ViewVarBinaryVector) {
      return new TextAccessors.BinaryAccessor(vector, ((ViewVarBinaryVector) vector)::get);
    }
    if (vector instanceof FixedSizeBinaryVector) {
      return new TextAccessors.BinaryAccessor(vector, ((FixedSizeBinaryVector) vector)::get);
    }
    if (vector instanceof UuidVector) {
      FixedSizeBinaryVector storage = ((UuidVector) vector).getUnderlyingVector();
      return new TextAccessors.UuidAccessor(vector, storage::get);
    }
    if (vector instanceof DateDayVector || vector instanceof DateMilliVector) {
      return new TemporalAccessors.DateAccessor(vector);
    }
    if (vector instanceof TimeSecVector
        || vector instanceof TimeMilliVector
        || vector instanceof TimeMicroVector
        || vector instanceof TimeNanoVector) {
      return new TemporalAccessors.TimeAccessor(vector);
    }
    if (vector instanceof TimeStampVector) {
      return new TemporalAccessors.TimestampAccessor((TimeStampVector) vector);
    }
    if (vector instanceof IntervalYearVector
        || vector instanceof IntervalDayVector
        || vector instanceof IntervalMonthDayNanoVector) {
      return new TemporalAccessors.IntervalAccessor(vector);
    }
    if (vector instanceof DurationVector) {
      return new TemporalAccessors.DurationAccessor((DurationVector) vector);
    }
    if (vector instanceof MapVector) {
      return new NestedAccessors.MapAccessor((MapVector) vector, dictionaries);
    }
    if (vector instanceof ListVector) {
      return new NestedAccessors.ListAccessor(
          vector, ((ListVector) vector).getDataVector(), dictionaries);
    }
    if (vector instanceof LargeListVector) {
      return new NestedAccessors.ListAccessor(
          vector, ((LargeListVector) vector).getDataVector(), dictionaries);
    }
    if (vector instanceof FixedSizeListVector) {
      return new NestedAccessors.ListAccessor(
          vector, ((FixedSizeListVector) vector).getDataVector(), dictionaries);
    }
    if (vector instanceof ListViewVector) {
      return new NestedAccessors.ListAccessor(
          vector, ((ListViewVector) vector).getDataVector(), dictionaries);
    }
    if (vector instanceof StructVector) {
      return new NestedAccessors.StructAccessor((StructVector) vector, dictionaries);
    }
    if (vector instanceof UnionVector || vector instanceof DenseUnionVector) {
      return new NestedAccessors.UnionAccessor(vector, dictionaries);
    }
    String integer16 = SqlTypes.duckDbInteger(vector.getField().getType());
    if (integer16 != null && vector instanceof ExtensionTypeVector) {
      return new NumberAccessors.Int128Accessor(
          vector,
          (FixedSizeBinaryVector) ((ExtensionTypeVector<?>) vector).getUnderlyingVector(),
          integer16.equals("HUGEINT"));
    }
    if (vector instanceof ExtensionTypeVector) {
      ExtensionTypeVector<?> extension = (ExtensionTypeVector<?>) vector;
      return create(extension.getUnderlyingVector(), dictionaries);
    }
    return new NestedAccessors.GenericAccessor(vector);
  }
}
