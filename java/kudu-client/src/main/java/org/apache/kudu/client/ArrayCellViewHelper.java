// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.client;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.serdes.BinaryArray;
import org.apache.kudu.serdes.DoubleArray;
import org.apache.kudu.serdes.FloatArray;
import org.apache.kudu.serdes.Int16Array;
import org.apache.kudu.serdes.Int32Array;
import org.apache.kudu.serdes.Int64Array;
import org.apache.kudu.serdes.Int8Array;
import org.apache.kudu.serdes.ScalarArray;
import org.apache.kudu.serdes.StringArray;
import org.apache.kudu.serdes.UInt8Array;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.TimestampUtil;


class ArrayCellViewHelper {

  private ArrayCellViewHelper() {
  }

  // ----------------------------------------------------------------------
  // Boxed numeric arrays with null (validity) handling
  // ----------------------------------------------------------------------

  // Decodes a BOOL array (physically encoded as a UInt8Array) from its FlatBuffer view.
  // Each element is stored as a single byte (0 or 1), not bit-packed.
  // Using valuesAsByteBuffer() is significantly faster than calling arr.values(i) per element,
  // but FlatBuffers may return a ByteBuffer whose position is not guaranteed to start at 0.
  // To ensure correctness, compute the base offset and perform absolute reads (base + i),
  // which avoids position drift and unnecessary buffer slicing while remaining allocation-free.
  static Boolean[] toBoolArray(ArrayCellView view) {
    UInt8Array arr = view.asUInt8FB();
    final int n = arr.valuesLength();
    ByteBuffer bb = arr.valuesAsByteBuffer();
    bb.order(ByteOrder.LITTLE_ENDIAN);
    int base = bb.position();
    Boolean[] out = new Boolean[n];
    for (int i = 0; i < n; i++) {
      if (!view.isValid(i)) {
        out[i] = null;
        continue;
      }
      out[i] = (bb.get(base + i) & 0xFF) != 0;
    }
    return out;
  }

  // Decodes an INT8 array (physically represented as an Int8Array) from its FlatBuffer view.
  // Each element is stored as a signed 8-bit value in little-endian order.
  // Similar to toBoolArray(), this implementation accesses the underlying ByteBuffer directly
  // to avoid the overhead of calling arr.values(i) for every element.
  // Because FlatBuffers may return a ByteBuffer whose position is not guaranteed to be 0,
  // the code computes the base offset and uses absolute reads (base + i) to prevent
  // position drift and unnecessary slicing while remaining allocation-free.
  // Null elements are filled with Java nulls according to the validity bitmap.
  static Byte[] toInt8Array(ArrayCellView view) {
    Int8Array arr = view.asInt8FB();
    ByteBuffer bb = arr.valuesAsByteBuffer();
    bb.order(ByteOrder.LITTLE_ENDIAN);
    final int n = arr.valuesLength();
    int base = bb.position();
    Byte[] out = new Byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? bb.get(base + i) : null;
    }
    return out;
  }

  static Short[] toInt16Array(ArrayCellView view) {
    Int16Array arr = view.asInt16FB();
    final int n = arr.valuesLength();
    short[] tmp = new short[n];
    ShortBuffer sb = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
    sb.get(tmp);
    Short[] out = new Short[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? tmp[i] : null;
    }
    return out;
  }

  static Integer[] toInt32Array(ArrayCellView view) {
    Int32Array arr = view.asInt32FB();
    final int n = arr.valuesLength();
    int[] tmp = new int[n];
    IntBuffer ib = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    ib.get(tmp);
    Integer[] out = new Integer[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? tmp[i] : null;
    }
    return out;
  }

  static Long[] toInt64Array(ArrayCellView view) {
    Int64Array arr = view.asInt64FB();
    final int n = arr.valuesLength();
    long[] tmp = new long[n];
    LongBuffer lb = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    lb.get(tmp);
    Long[] out = new Long[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? tmp[i] : null;
    }
    return out;
  }

  static Float[] toFloatArray(ArrayCellView view) {
    FloatArray arr = view.asFloatFB();
    final int n = arr.valuesLength();
    float[] tmp = new float[n];
    FloatBuffer fb = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    fb.get(tmp);
    Float[] out = new Float[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? tmp[i] : null;
    }
    return out;
  }

  static Double[] toDoubleArray(ArrayCellView view) {
    DoubleArray arr = view.asDoubleFB();
    final int n = arr.valuesLength();
    double[] tmp = new double[n];
    DoubleBuffer db = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
    db.get(tmp);
    Double[] out = new Double[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? tmp[i] : null;
    }
    return out;
  }

  // ----------------------------------------------------------------------
  // String-like types
  // ----------------------------------------------------------------------

  static String[] toStringArray(ArrayCellView view) {
    StringArray arr = view.asStringFB();
    final int n = arr.valuesLength();
    String[] out = new String[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? arr.values(i) : null;
    }
    return out;
  }

  static String[] toVarcharArray(ArrayCellView view) {
    // identical to String arrays, truncation handled on write side
    return toStringArray(view);
  }

  static byte[][] toBinaryArray(ArrayCellView view) {
    BinaryArray arr = view.asBinaryFB();
    final int n = arr.valuesLength();
    byte[][] out = new byte[n][];
    UInt8Array elem = new UInt8Array();
    for (int i = 0; i < n; i++) {
      if (!view.isValid(i)) {
        out[i] = null;
        continue;
      }
      ByteBuffer bb = Objects.requireNonNull(arr.values(elem, i)).valuesAsByteBuffer();
      byte[] b = new byte[bb.remaining()];
      bb.get(b);
      out[i] = b;
    }
    return out;
  }

  // ----------------------------------------------------------------------
  // Date and timestamp
  // ----------------------------------------------------------------------

  static Date[] toDateArray(ArrayCellView view) {
    Int32Array arr = view.asInt32FB();
    final int n = arr.valuesLength();
    int[] tmp = new int[n];
    IntBuffer ib = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    ib.get(tmp);
    Date[] out = new Date[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? DateUtil.epochDaysToSqlDate(tmp[i]) : null;
    }
    return out;
  }

  static Timestamp[] toTimestampArray(ArrayCellView view) {
    Int64Array arr = view.asInt64FB();
    LongBuffer lb = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    final int n = arr.valuesLength();
    long[] tmp = new long[n];
    lb.get(tmp);
    Timestamp[] out = new Timestamp[n];
    for (int i = 0; i < n; i++) {
      out[i] = view.isValid(i) ? TimestampUtil.microsToTimestamp(tmp[i]) : null;
    }
    return out;
  }

  // ----------------------------------------------------------------------
  // Decimal
  // ----------------------------------------------------------------------

  static BigDecimal[] toDecimalArray(ArrayCellView view, int precision, int scale) {
    BigDecimal[] out;
    if (precision <= 9) {
      Int32Array arr = view.asInt32FB();
      IntBuffer ib = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
      final int n = arr.valuesLength();
      int[] tmp = new int[n];
      ib.get(tmp);
      out = new BigDecimal[n];
      for (int i = 0; i < n; i++) {
        out[i] = view.isValid(i) ? BigDecimal.valueOf(tmp[i], scale) : null;
      }
    } else if (precision <= 18) {
      Int64Array arr = view.asInt64FB();
      LongBuffer lb = arr.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
      final int n = arr.valuesLength();
      long[] tmp = new long[n];
      lb.get(tmp);
      out = new BigDecimal[n];
      for (int i = 0; i < n; i++) {
        out[i] = view.isValid(i) ? BigDecimal.valueOf(tmp[i], scale) : null;
      }
    } else {
      throw new IllegalStateException("DECIMAL128 arrays not yet supported");
    }
    return out;
  }

  // ----------------------------------------------------------------------
  // Generic dispatcher
  // ----------------------------------------------------------------------

  static Object toJavaArray(ArrayCellView view,
                            ColumnSchema col) {

    int precision = 0;
    int scale = 0;
    if (col.getTypeAttributes() != null) {
      precision = col.getTypeAttributes().getPrecision();
      scale = col.getTypeAttributes().getScale();
    }
    Type logicalType = col.getNestedTypeDescriptor().getArrayDescriptor().getElemType();

    switch (view.typeTag()) {
      case ScalarArray.Int8Array:
        return toInt8Array(view);

      case ScalarArray.UInt8Array:
        // BOOL and UINT8 share the same physical encoding.
        if (logicalType == org.apache.kudu.Type.BOOL) {
          return toBoolArray(view);
        }
        return toInt8Array(view);

      case ScalarArray.Int16Array:
        return toInt16Array(view);

      case ScalarArray.Int32Array: {
        // DECIMAL32, DATE, or plain INT32
        if (precision > 0) {
          return toDecimalArray(view, precision, scale);
        }
        if (logicalType == org.apache.kudu.Type.DATE) {
          return toDateArray(view);
        }
        return toInt32Array(view);
      }

      case ScalarArray.Int64Array: {
        // DECIMAL64, UNIXTIME_MICROS, or plain INT64
        if (precision > 0) {
          return toDecimalArray(view, precision, scale);
        }
        if (logicalType == org.apache.kudu.Type.UNIXTIME_MICROS) {
          return toTimestampArray(view);
        }
        return toInt64Array(view);
      }

      case ScalarArray.FloatArray:
        return toFloatArray(view);

      case ScalarArray.DoubleArray:
        return toDoubleArray(view);

      case ScalarArray.StringArray:
        // STRING and VARCHAR share the same encoding
        return toStringArray(view);

      case ScalarArray.BinaryArray:
        return toBinaryArray(view);

      default:
        throw new IllegalStateException(String.format(
            "Unsupported array type tag %d (%s)",
            view.typeTag(), ScalarArray.name(view.typeTag())));
    }
  }

}