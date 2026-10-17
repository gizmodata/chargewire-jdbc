package com.gizmodata.warpsql.jdbc.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;

/** Accessors for integers, floating point, decimals and booleans. */
final class NumberAccessors {
  private NumberAccessors() {}

  /**
   * Signed and unsigned integers of every width. {@link #getObject} returns {@link Byte}, {@link
   * Short}, {@link Integer} or {@link Long} for signed types, and the next wider type for unsigned
   * ones ({@link BigDecimal} for 64 bits), so every value is exact.
   */
  static final class IntAccessor extends Accessor {
    private final int width;
    private final boolean signed;

    IntAccessor(ValueVector vector, int width, boolean signed) {
      super(vector);
      this.width = width;
      this.signed = signed;
    }

    /** The value's bits; for unsigned 64-bit values, read with {@link Long#toUnsignedString}. */
    private long raw(int row) {
      if (vector instanceof TinyIntVector) {
        return ((TinyIntVector) vector).get(row);
      } else if (vector instanceof SmallIntVector) {
        return ((SmallIntVector) vector).get(row);
      } else if (vector instanceof IntVector) {
        return ((IntVector) vector).get(row);
      } else if (vector instanceof BigIntVector) {
        return ((BigIntVector) vector).get(row);
      } else if (vector instanceof UInt1Vector) {
        return ((UInt1Vector) vector).get(row) & 0xFFL;
      } else if (vector instanceof UInt2Vector) {
        return ((UInt2Vector) vector).get(row);
      } else if (vector instanceof UInt4Vector) {
        return ((UInt4Vector) vector).get(row) & 0xFFFFFFFFL;
      } else {
        return ((UInt8Vector) vector).get(row);
      }
    }

    private boolean isHugeUnsigned(long raw) {
      return !signed && width == 64 && raw < 0;
    }


    @Override
    public Object getObject(int row) {
      long value = raw(row);
      if (signed) {
        switch (width) {
          case 8:
            return (byte) value;
          case 16:
            return (short) value;
          case 32:
            return (int) value;
          default:
            return value;
        }
      }
      switch (width) {
        case 8:
          return (short) value;
        case 16:
          return (int) value;
        case 32:
          return value;
        default:
          return new BigDecimal(new BigInteger(Long.toUnsignedString(value)));
      }
    }

    @Override
    public String getString(int row) {
      long value = raw(row);
      return isHugeUnsigned(value) ? Long.toUnsignedString(value) : Long.toString(value);
    }

    @Override
    public boolean getBoolean(int row) {
      return raw(row) != 0;
    }

    @Override
    public long getLong(int row) throws SQLException {
      long value = raw(row);
      if (isHugeUnsigned(value)) {
        throw outOfRange(Long.toUnsignedString(value), "long");
      }
      return value;
    }

    @Override
    public double getDouble(int row) {
      long value = raw(row);
      return isHugeUnsigned(value)
          ? new BigInteger(Long.toUnsignedString(value)).doubleValue()
          : (double) value;
    }

    @Override
    public BigDecimal getBigDecimal(int row) {
      long value = raw(row);
      return isHugeUnsigned(value)
          ? new BigDecimal(new BigInteger(Long.toUnsignedString(value)))
          : BigDecimal.valueOf(value);
    }
  }

  /** Half, single and double precision floating point. */
  static final class FloatAccessor extends Accessor {
    FloatAccessor(ValueVector vector) {
      super(vector);
    }

    private boolean isDouble() {
      return vector instanceof Float8Vector;
    }


    private float floatValue(int row) {
      return vector instanceof Float4Vector
          ? ((Float4Vector) vector).get(row)
          : ((Float2Vector) vector).getValueAsFloat(row);
    }

    @Override
    public Object getObject(int row) {
      return isDouble() ? (Object) ((Float8Vector) vector).get(row) : (Object) floatValue(row);
    }

    @Override
    public double getDouble(int row) {
      return isDouble() ? ((Float8Vector) vector).get(row) : floatValue(row);
    }

    @Override
    public float getFloat(int row) throws SQLException {
      return isDouble() ? super.getFloat(row) : floatValue(row);
    }

    @Override
    public String getString(int row) {
      return isDouble()
          ? Double.toString(((Float8Vector) vector).get(row))
          : Float.toString(floatValue(row));
    }

    @Override
    public boolean getBoolean(int row) {
      return getDouble(row) != 0;
    }

    @Override
    public long getLong(int row) throws SQLException {
      double value = getDouble(row);
      if (Double.isNaN(value) || value < -0x1p63 || value >= 0x1p63) {
        throw outOfRange(value, "long");
      }
      return (long) value;
    }

    @Override
    public BigDecimal getBigDecimal(int row) throws SQLException {
      double value = getDouble(row);
      if (Double.isNaN(value) || Double.isInfinite(value)) {
        throw outOfRange(value, "BigDecimal");
      }
      return new BigDecimal(getString(row));
    }
  }

  /** 128- and 256-bit decimals. */
  static final class DecimalAccessor extends Accessor {
    DecimalAccessor(ValueVector vector) {
      super(vector);
    }


    @Override
    public BigDecimal getObject(int row) {
      return vector instanceof DecimalVector
          ? ((DecimalVector) vector).getObject(row)
          : ((Decimal256Vector) vector).getObject(row);
    }

    @Override
    public BigDecimal getBigDecimal(int row) {
      return getObject(row);
    }

    @Override
    public String getString(int row) {
      return getObject(row).toPlainString();
    }

    @Override
    public boolean getBoolean(int row) {
      return getObject(row).signum() != 0;
    }

    @Override
    public long getLong(int row) throws SQLException {
      return toLong(getObject(row));
    }

    @Override
    public double getDouble(int row) {
      return getObject(row).doubleValue();
    }
  }

  /** Booleans: also readable as 1 and 0. */
  static final class BoolAccessor extends Accessor {
    BoolAccessor(BitVector vector) {
      super(vector);
    }

    private boolean value(int row) {
      return ((BitVector) vector).get(row) != 0;
    }


    @Override
    public Object getObject(int row) {
      return value(row);
    }

    @Override
    public boolean getBoolean(int row) {
      return value(row);
    }

    @Override
    public long getLong(int row) {
      return value(row) ? 1 : 0;
    }

    @Override
    public double getDouble(int row) {
      return value(row) ? 1 : 0;
    }

    @Override
    public BigDecimal getBigDecimal(int row) {
      return value(row) ? BigDecimal.ONE : BigDecimal.ZERO;
    }
  }
}
