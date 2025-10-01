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
import java.util.function.IntFunction;

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
public final class ArrayCellView {

  private final byte[] rawBytes;
  private final Content content;
  private final byte typeTag;

  private static final String NULL_ELEMENT_MSG = "Element %d is NULL";

  public ArrayCellView(byte[] buf) {
    this.rawBytes = buf;
    ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
    this.content = Content.getRootAsContent(bb);
    this.typeTag = content.dataType();
  }

  /** Return the underlying FlatBuffer bytes exactly as passed in. */
  public byte[] toBytes() {
    return rawBytes;
  }

  /** Number of logical elements (driven by the data vector). */
  public int length() {
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
  public boolean isValid(int i) {
    int n = length();
    if (i < 0 || i >= n) {
      throw new IndexOutOfBoundsException(
          String.format("Index %d out of bounds for array length %d", i, n));
    }

    int vlen = content.validityLength();
    if (vlen == 0) {
      // No validity vector present => all elements are valid
      return true;
    }

    if (i >= vlen) {
      throw new IllegalStateException(
          String.format("Validity vector shorter than array: i=%d, vlen=%d", i, vlen));
    }

    return content.validity(i);
  }

  // ----------------------------------------------------------------------
  // Types single element accessors
  // ----------------------------------------------------------------------

  /** BOOL is encoded as UInt8 (0/1). */
  public boolean getBoolean(int i) {
    ensureTag(ScalarArray.UInt8Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    UInt8Array arr = new UInt8Array();
    content.data(arr);
    return arr.values(i) != 0;
  }

  public byte getInt8(int i) {
    ensureTag(ScalarArray.Int8Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int8Array arr = new Int8Array();
    content.data(arr);
    return arr.values(i);
  }

  public short getInt16(int i) {
    ensureTag(ScalarArray.Int16Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int16Array arr = new Int16Array();
    content.data(arr);
    return arr.values(i);
  }

  /** INT32 (also used by DATE, DECIMAL32 on the wire). */
  public int getInt32(int i) {
    ensureTag(ScalarArray.Int32Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int32Array arr = new Int32Array();
    content.data(arr);
    return arr.values(i);
  }

  /** INT64 (also used by DECIMAL64, UNIXTIME_MICROS on the wire). */
  public long getInt64(int i) {
    ensureTag(ScalarArray.Int64Array);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    Int64Array arr = new Int64Array();
    content.data(arr);
    return arr.values(i);
  }

  public float getFloat(int i) {
    ensureTag(ScalarArray.FloatArray);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    FloatArray arr = new FloatArray();
    content.data(arr);
    return arr.values(i);
  }

  public double getDouble(int i) {
    ensureTag(ScalarArray.DoubleArray);
    if (!isValid(i)) {
      throw new IllegalStateException(String.format(NULL_ELEMENT_MSG, i));
    }
    DoubleArray arr = new DoubleArray();
    content.data(arr);
    return arr.values(i);
  }

  public String getString(int i) {
    ensureTag(ScalarArray.StringArray);
    if (!isValid(i)) {
      return null;
    }
    StringArray arr = new StringArray();
    content.data(arr);
    return arr.values(i);
  }

  public byte[] getBinary(int i) {
    ensureTag(ScalarArray.BinaryArray);
    if (!isValid(i)) {
      return null;
    }
    BinaryArray arr = new BinaryArray();
    content.data(arr);
    UInt8Array elem = arr.values(new UInt8Array(), i);
    if (elem == null) {
      throw new IllegalStateException(
          String.format("Corrupt BinaryArray: missing element at index %d", i));
    }
    ByteBuffer bb = elem.valuesAsByteBuffer();
    byte[] out = new byte[bb.remaining()];
    bb.get(out);
    return out;
  }


  // ----------------------------------------------------------------------
  // Bulk conversion for callers that want plain Java arrays
  // ----------------------------------------------------------------------

  public Object toJavaArray() {
    switch (typeTag) {
      case ScalarArray.Int32Array:   return Array1dSerdes.parseInt32(rawBytes).getValues();
      case ScalarArray.Int64Array:   return Array1dSerdes.parseInt64(rawBytes).getValues();
      case ScalarArray.Int16Array:   return Array1dSerdes.parseInt16(rawBytes).getValues();
      case ScalarArray.Int8Array:    return Array1dSerdes.parseInt8(rawBytes).getValues();
      case ScalarArray.UInt8Array:   return Array1dSerdes.parseUInt8(rawBytes).getValues();
      case ScalarArray.UInt16Array:  return Array1dSerdes.parseUInt16(rawBytes).getValues();
      case ScalarArray.UInt32Array:  return Array1dSerdes.parseUInt32(rawBytes).getValues();
      case ScalarArray.UInt64Array:  return Array1dSerdes.parseUInt64(rawBytes).getValues();
      case ScalarArray.FloatArray:   return Array1dSerdes.parseFloat(rawBytes).getValues();
      case ScalarArray.DoubleArray:  return Array1dSerdes.parseDouble(rawBytes).getValues();
      case ScalarArray.StringArray:  return Array1dSerdes.parseString(rawBytes).getValues();
      case ScalarArray.BinaryArray:  return Array1dSerdes.parseBinary(rawBytes).getValues();
      default:
        throw new UnsupportedOperationException("Unsupported array type tag: " + typeTag);
    }
  }

  // ----------------------------------------------------------------------
  // Pretty print with NULLs preserved
  // ----------------------------------------------------------------------

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

}