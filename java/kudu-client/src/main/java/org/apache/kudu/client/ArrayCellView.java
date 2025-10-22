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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntFunction;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.serdes.BinaryArray;
import org.apache.kudu.serdes.Content;
import org.apache.kudu.serdes.DoubleArray;
import org.apache.kudu.serdes.FloatArray;
import org.apache.kudu.serdes.Int16Array;
import org.apache.kudu.serdes.Int32Array;
import org.apache.kudu.serdes.Int64Array;
import org.apache.kudu.serdes.Int8Array;
import org.apache.kudu.serdes.ScalarArray;
import org.apache.kudu.serdes.StringArray;
import org.apache.kudu.serdes.UInt16Array;
import org.apache.kudu.serdes.UInt32Array;
import org.apache.kudu.serdes.UInt64Array;
import org.apache.kudu.serdes.UInt8Array;

/**
 * Lightweight view over a FlatBuffers Content blob representing a single array cell.
 */
class ArrayCellView {

  private final byte[] rawBytes;
  private final Content content;
  private final byte typeTag;
  private final BitSet validityBitSet;
  private final int elemNum;

  private static final String NULL_ELEMENT_MSG = "Element %d is NULL";

  /**
   * Construct an {@code ArrayCellView} from a serialized FlatBuffer blob.
   * The byte array is not copied; callers must ensure it remains valid.
   *
   * @param buf the FlatBuffer bytes for a {@code Content} object
   */

  ArrayCellView(byte[] buf) {
    this.rawBytes = buf;
    ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
    this.content = Content.getRootAsContent(bb);
    this.typeTag = content.dataType();
    this.elemNum = getElemNum();

    // Build validity/not-null BitSet.
    final int validityElemNum = content.validityLength(); // number of bytes in the array
    if (validityElemNum == 0) {
      // Setting the validity bitset explicitly to null: all array elements are valid.
      this.validityBitSet = null;
    } else {
      // One validity bit per element of the 'data' array.
      final int dataElemNum = length();
      final int expectedValidityElemNum = (dataElemNum + 7) / 8;
      if (validityElemNum != expectedValidityElemNum) {
        throw new IllegalArgumentException(
            String.format("invalid validity length %d: expected %d for %d elements in array)",
                validityElemNum, expectedValidityElemNum, dataElemNum));
      }
      this.validityBitSet = BitSet.valueOf(content.validityAsByteBuffer());
    }
  }

  /** Return the underlying FlatBuffer bytes exactly as passed in. */

  byte[] toBytes() {
    return rawBytes;
  }

  /** Number of logical elements in the array, including null/non-valid elements. */
  int length() {
    return elemNum;
  }

  /** Get the number of array elements in the underlying flatbuffers contents. */
  private int getElemNum() {
    switch (typeTag) {
      case ScalarArray.Int8Array: {
        Int8Array arr = new Int8Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.UInt8Array: {
        UInt8Array arr = new UInt8Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.Int16Array: {
        Int16Array arr = new Int16Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.UInt16Array: {
        UInt16Array arr = new UInt16Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.Int32Array: {
        Int32Array arr = new Int32Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.UInt32Array: {
        UInt32Array arr = new UInt32Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.Int64Array: {
        Int64Array arr = new Int64Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.UInt64Array: {
        UInt64Array arr = new UInt64Array();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.FloatArray: {
        FloatArray arr = new FloatArray();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.DoubleArray: {
        DoubleArray arr = new DoubleArray();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.StringArray: {
        StringArray arr = new StringArray();
        content.data(arr);
        return arr.valuesLength();
      }
      case ScalarArray.BinaryArray: {
        BinaryArray arr = new BinaryArray();
        content.data(arr);
        return arr.valuesLength();
      }
      default:
        throw new UnsupportedOperationException(
            "Unsupported array type tag: " + typeTag);
    }
  }

  /** Returns true iff element i is valid (non-null). */
  boolean isValid(int i) {
    final int n = length();
    if (i < 0 || i >= n) {
      throw new IndexOutOfBoundsException(
          String.format("Index %d out of bounds for array length %d", i, n));
    }
    if (validityBitSet == null) {
      // No validity vector present => all elements are valid
      return true;
    }
    return validityBitSet.get(i);
  }

  // ----------------------------------------------------------------------
  // Types single element accessors
  // ----------------------------------------------------------------------

  /** BOOL is encoded as UInt8 (0/1). */

  boolean getBoolean(int i) {
    ensureTag(ScalarArray.UInt8Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    UInt8Array arr = new UInt8Array();
    content.data(arr);
    return arr.values(i) != 0;
  }


  byte getInt8(int i) {
    ensureTag(ScalarArray.Int8Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int8Array arr = new Int8Array();
    content.data(arr);
    return arr.values(i);
  }


  short getInt16(int i) {
    ensureTag(ScalarArray.Int16Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int16Array arr = new Int16Array();
    content.data(arr);
    return arr.values(i);
  }

  /** INT32 (also used by DATE, DECIMAL32 on the wire). */

  int getInt32(int i) {
    ensureTag(ScalarArray.Int32Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int32Array arr = new Int32Array();
    content.data(arr);
    return arr.values(i);
  }

  /** INT64 (also used by DECIMAL64, UNIXTIME_MICROS on the wire). */

  long getInt64(int i) {
    ensureTag(ScalarArray.Int64Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int64Array arr = new Int64Array();
    content.data(arr);
    return arr.values(i);
  }


  float getFloat(int i) {
    ensureTag(ScalarArray.FloatArray);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    FloatArray arr = new FloatArray();
    content.data(arr);
    return arr.values(i);
  }


  double getDouble(int i) {
    ensureTag(ScalarArray.DoubleArray);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    DoubleArray arr = new DoubleArray();
    content.data(arr);
    return arr.values(i);
  }


  String getString(int i) {
    ensureTag(ScalarArray.StringArray);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    StringArray arr = new StringArray();
    content.data(arr);
    return arr.values(i);
  }


  byte[] getBinary(int i) {
    ensureTag(ScalarArray.BinaryArray);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    BinaryArray arr = new BinaryArray();
    content.data(arr);
    UInt8Array elem = arr.values(new UInt8Array(), i);
    if (elem == null) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    ByteBuffer bb = elem.valuesAsByteBuffer();
    byte[] out = new byte[bb.remaining()];
    bb.get(out);
    return out;
  }


  /**
   * Returns a human-readable string representation of the array contents,
   * preserving {@code NULL} markers for invalid elements.
   *
   * <p>This is intended for debugging and should not be parsed.</p>
   */
  @Override
  public String toString() {
    final int n = length();
    StringBuilder sb = new StringBuilder();
    sb.append('[');

    switch (typeTag) {
      case ScalarArray.Int8Array: {
        Int8Array arr = new Int8Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.UInt8Array: {
        UInt8Array arr = new UInt8Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.Int16Array: {
        Int16Array arr = new Int16Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.UInt16Array: {
        UInt16Array arr = new UInt16Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.Int32Array: {
        Int32Array arr = new Int32Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.UInt32Array: {
        UInt32Array arr = new UInt32Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.Int64Array: {
        Int64Array arr = new Int64Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.UInt64Array: {
        UInt64Array arr = new UInt64Array();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.FloatArray: {
        FloatArray arr = new FloatArray();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.DoubleArray: {
        DoubleArray arr = new DoubleArray();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.StringArray: {
        StringArray arr = new StringArray();
        content.data(arr);
        appendArrayValuesToString(sb, n, arr::values);
      } break;

      case ScalarArray.BinaryArray: {
        BinaryArray bin = new BinaryArray();
        content.data(bin);
        appendArrayValuesToString(sb, n, i -> {
          UInt8Array elem = bin.values(new UInt8Array(), i);
          if (elem == null) {
            throw new IllegalStateException(
                String.format("Corrupt BinaryArray: missing element at index %d", i));
          }
          int len = elem.valuesLength();
          byte[] out = new byte[len];
          for (int j = 0; j < len; j++) {
            out[j] = (byte) elem.values(j);
          }
          return Arrays.toString(out);
        });
      } break;
      default:
        sb.append("<unsupported array type tag ").append(typeTag).append('>');
        break;
    }

    sb.append(']');
    return sb.toString();
  }

  /**
   * Verifies that this array's type tag matches the expected
   * physical array type.
   *
   * @throws IllegalStateException if the tag does not match
   */
  private void ensureTag(byte expectedTag) {
    if (typeTag != expectedTag) {
      String expectedName = ScalarArray.name(expectedTag);
      String actualName   = ScalarArray.name(typeTag);
      throw new IllegalStateException(
          String.format("Type mismatch: expected %s (tag=%d) but found %s (tag=%d)",
              expectedName, expectedTag,
              actualName, typeTag));
    }
  }

  /**
   * Appends formatted element values to the provided {@link StringBuilder},
   * substituting {@code NULL} for invalid entries.
   */
  private void appendArrayValuesToString(
      StringBuilder sb, int n,
      IntFunction<Object> valueFn) {
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      if (!isValid(i)) {
        sb.append("NULL");
        continue;
      }
      sb.append(valueFn.apply(i));
    }
  }

  /**
   * Converts this array cell into a boxed Java array using the column schema
   * to resolve the element's logical type and column attributes.
   *
   * <p>This variant interprets the element type (for example,
   * {@code DATE}, {@code TIMESTAMP}, or {@code DECIMAL}) using the
   * provided {@link ColumnSchema}, and applies precision and scale
   * information when relevant.</p>
   *
   * <p>Internally delegates to
   * {@link ArrayCellViewHelper#toJavaArray(ArrayCellView, ColumnSchema)}.</p>
   *
   * @param columnSchema the schema describing this array column, including
   *                     logical type, precision, and scale
   * @return a boxed Java array corresponding to the logical element type
   */

  Object toJavaArray(ColumnSchema columnSchema) {
    return ArrayCellViewHelper.toJavaArray(this, columnSchema);
  }

  /**
   * Converts this array cell into a boxed Java array without schema context.
   *
   * <p>This variant performs a physical-to-Java mapping based only on
   * the array's storage type and does not apply any logical-type
   * conversions or precision/scale metadata.</p>
   *
   * <p>For {@code DECIMAL} arrays, prefer
   * {@link #toJavaArray(ColumnSchema)} to ensure correct precision and
   * scale are applied.</p>
   *
   * @return a boxed Java array corresponding to the underlying storage type
   */

  Object toJavaArray() {
    return ArrayCellViewHelper.toJavaArray(this, null);
  }


  byte typeTag() {
    return typeTag;
  }

  /**
   * Returns the underlying FlatBuffer view of this array's physical values
   * (for internal use or advanced diagnostics). Validates that the
   * {@code typeTag} matches the expected array type.
   */

  Int8Array asInt8FB() {
    ensureTag(ScalarArray.Int8Array);
    return (Int8Array) content.data(new Int8Array());
  }


  Int16Array asInt16FB() {
    ensureTag(ScalarArray.Int16Array);
    return (Int16Array) content.data(new Int16Array());
  }


  Int32Array asInt32FB() {
    ensureTag(ScalarArray.Int32Array);
    return (Int32Array) content.data(new Int32Array());
  }


  Int64Array asInt64FB() {
    ensureTag(ScalarArray.Int64Array);
    return (Int64Array) content.data(new Int64Array());
  }


  FloatArray asFloatFB() {
    ensureTag(ScalarArray.FloatArray);
    return (FloatArray) content.data(new FloatArray());
  }


  DoubleArray asDoubleFB() {
    ensureTag(ScalarArray.DoubleArray);
    return (DoubleArray) content.data(new DoubleArray());
  }


  UInt8Array asUInt8FB() {
    ensureTag(ScalarArray.UInt8Array);
    return (UInt8Array) content.data(new UInt8Array());
  }


  StringArray asStringFB() {
    ensureTag(ScalarArray.StringArray);
    return (StringArray) content.data(new StringArray());
  }


  BinaryArray asBinaryFB() {
    ensureTag(ScalarArray.BinaryArray);
    return (BinaryArray) content.data(new BinaryArray());
  }

}