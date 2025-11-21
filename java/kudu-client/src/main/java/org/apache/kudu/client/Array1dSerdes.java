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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.FlatBufferBuilder;

// FlatBuffers generated classes
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

public class Array1dSerdes {

  // ---------------- Result container ----------------
  public static class ArrayResult {
    private final Object values;
    private final boolean[] validity;

    public ArrayResult(Object values, boolean[] validity) {
      this.values = values;
      this.validity = validity;
    }

    public Object getValues() {
      return values;
    }

    public boolean[] getValidity() {
      return validity;
    }
  }

  // ---------------- Helper interfaces ----------------
  @FunctionalInterface interface CreateVec<V> { int apply(FlatBufferBuilder b, V values); }

  @FunctionalInterface interface Start { void apply(FlatBufferBuilder b); }

  @FunctionalInterface interface AddValues { void apply(FlatBufferBuilder b, int vecOff); }

  @FunctionalInterface interface End { int apply(FlatBufferBuilder b); }

  @FunctionalInterface interface GetUnion<A> { A apply(Content c); }

  @FunctionalInterface interface ArrLen<A> { int apply(A arr); }

  @FunctionalInterface interface Copier<A> { void copy(A arr, int i, Object outArray); }

  @FunctionalInterface interface NewArray { Object apply(int m); }

  // ---------------- Common helpers ----------------
  private static int validityOffset(FlatBufferBuilder b, boolean[] validity) {
    if (validity == null || validity.length == 0) {
      return 0;
    }
    // If all elements are valid, the validity field isn't populated,
    // similar to null and empty validity vectors.
    boolean allValid = true;
    for (boolean elem : validity) {
      if (!elem) {
        allValid = false;
        break;
      }
    }
    if (allValid) {
      return 0;
    }

    // NOTE: java.util.BitSet is unusable here since it automatically
    //   truncates all trailing zero bits after the last set bit
    final int validityBitNum = validity.length;
    final int validityByteNum = (validityBitNum + 7) / 8;
    byte[] validityBytes = new byte[validityByteNum];
    Arrays.fill(validityBytes, (byte)0);
    for (int idx = 0; idx < validityBitNum; ++idx) {
      if (validity[idx]) {
        validityBytes[idx >> 3] |= (byte)(1 << (idx & 7));
      }
    }
    return Content.createValidityVector(b, validityBytes);
  }

  private static int finishContent(FlatBufferBuilder b, int discriminator,
                                   int arrOffset, int validityOffset) {
    Content.startContent(b);
    Content.addDataType(b, (byte) discriminator);
    Content.addData(b, arrOffset);
    if (validityOffset != 0) {
      Content.addValidity(b, validityOffset);
    }
    return Content.endContent(b);
  }

  private static Content readContent(byte[] buf) {
    return Content.getRootAsContent(ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN));
  }

  private static boolean[] buildValidity(Content c, int m) {
    final int n = c.validityLength();
    boolean[] validity = new boolean[m];

    if (n == 0) {
      // No validity buffer present. By convention, all elements are non-null.
      Arrays.fill(validity, true);
      return validity;
    }

    final int expected_validity_len = (m + 7) / 8;
    if (n != expected_validity_len) {
      throw new IllegalArgumentException(
          String.format("invalid validity length %d: expected %d for %d elements in array",
                        n, expected_validity_len, m));
    }
    final ByteVector vv = c.validityVector();
    Preconditions.checkState(vv.length() == n,
        "unexpected length of validityVector: %s (expected %s)", vv.length(), n);
    for (int idx = 0; idx < m; ++idx) {
      validity[idx] = ((vv.get(idx >> 3) & (1 << (idx & 7))) != 0);
    }
    return validity;
  }

  private static boolean hasNulls(Object[] values) {
    if (values == null) {
      return false;
    }
    for (Object v : values) {
      if (v == null) {
        return true;
      }
    }
    return false;
  }

  // ---------------- Int8 ----------------
  public static byte[] serializeInt8(byte[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.Int8Array,
      Int8Array::createValuesVector,
      Int8Array::startInt8Array,
      Int8Array::addValues,
      Int8Array::endInt8Array
    );
  }

  public static ArrayResult parseInt8(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.Int8Array,
      c -> (Int8Array) c.data(new Int8Array()),
      Int8Array::valuesLength,
      byte[]::new,
      (a, i, out) -> ((byte[]) out)[i] = a.values(i)
    );
  }

  // ---------------- UInt8 ----------------
  public static byte[] serializeUInt8(byte[] values, boolean[] validity) {
    byte[] narrowed = null;
    if (values != null) {
      narrowed = new byte[values.length];
      for (int i = 0; i < values.length; i++) {
        narrowed[i] = (byte)(values[i] & 0xFF);
      }
    }
    return serializePrimitive(
      narrowed, validity, ScalarArray.UInt8Array,
      UInt8Array::createValuesVector,
      UInt8Array::startUInt8Array,
      UInt8Array::addValues,
      UInt8Array::endUInt8Array
    );
  }

  public static ArrayResult parseUInt8(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.UInt8Array,
      c -> (UInt8Array) c.data(new UInt8Array()),
      UInt8Array::valuesLength,
      int[]::new,
      (a, i, out) -> ((int[]) out)[i] = a.values(i) & 0xFF
    );
  }

  // ---------------- Int16 ----------------
  public static byte[] serializeInt16(short[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.Int16Array,
      Int16Array::createValuesVector,
      Int16Array::startInt16Array,
      Int16Array::addValues,
      Int16Array::endInt16Array
    );
  }

  public static ArrayResult parseInt16(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.Int16Array,
      c -> (Int16Array) c.data(new Int16Array()),
      Int16Array::valuesLength,
      short[]::new,
      (a, i, out) -> ((short[]) out)[i] = a.values(i)
    );
  }

  // ---------------- UInt16 ----------------
  public static byte[] serializeUInt16(short[] values, boolean[] validity) {
    int[] widened = null;
    if (values != null) {
      widened = new int[values.length];
      for (int i = 0; i < values.length; i++) {
        widened[i] = values[i] & 0xFFFF;
      }
    }
    return serializePrimitive(
      widened, validity, ScalarArray.UInt16Array,
      UInt16Array::createValuesVector,
      UInt16Array::startUInt16Array,
      UInt16Array::addValues,
      UInt16Array::endUInt16Array
    );
  }

  public static ArrayResult parseUInt16(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.UInt16Array,
      c -> (UInt16Array) c.data(new UInt16Array()),
      UInt16Array::valuesLength,
      int[]::new,
      (a, i, out) -> ((int[]) out)[i] = a.values(i) & 0xFFFF
    );
  }

  // ---------------- Int32 ----------------
  public static byte[] serializeInt32(int[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.Int32Array,
      Int32Array::createValuesVector,
      Int32Array::startInt32Array,
      Int32Array::addValues,
      Int32Array::endInt32Array
    );
  }

  public static ArrayResult parseInt32(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.Int32Array,
      c -> (Int32Array) c.data(new Int32Array()),
      Int32Array::valuesLength,
      int[]::new,
      (a, i, out) -> ((int[]) out)[i] = a.values(i)
    );
  }

  // ---------------- UInt32 ----------------
  public static byte[] serializeUInt32(int[] values, boolean[] validity) {
    long[] widened = null;
    if (values != null) {
      widened = new long[values.length];
      for (int i = 0; i < values.length; i++) {
        widened[i] = values[i] & 0xFFFFFFFFL;
      }
    }
    return serializePrimitive(
      widened, validity, ScalarArray.UInt32Array,
      UInt32Array::createValuesVector,
      UInt32Array::startUInt32Array,
      UInt32Array::addValues,
      UInt32Array::endUInt32Array
    );
  }

  public static ArrayResult parseUInt32(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.UInt32Array,
      c -> (UInt32Array) c.data(new UInt32Array()),
      UInt32Array::valuesLength,
      long[]::new,
      (a, i, out) -> ((long[]) out)[i] = a.values(i) & 0xFFFFFFFFL
    );
  }

  // ---------------- Int64 ----------------
  public static byte[] serializeInt64(long[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.Int64Array,
      Int64Array::createValuesVector,
      Int64Array::startInt64Array,
      Int64Array::addValues,
      Int64Array::endInt64Array
    );
  }

  public static ArrayResult parseInt64(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.Int64Array,
      c -> (Int64Array) c.data(new Int64Array()),
      Int64Array::valuesLength,
      long[]::new,
      (a, i, out) -> ((long[]) out)[i] = a.values(i)
    );
  }

  // ---------------- UInt64 ----------------
  public static byte[] serializeUInt64(long[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.UInt64Array,
      UInt64Array::createValuesVector,
      UInt64Array::startUInt64Array,
      UInt64Array::addValues,
      UInt64Array::endUInt64Array
    );
  }

  public static ArrayResult parseUInt64(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.UInt64Array,
      c -> (UInt64Array) c.data(new UInt64Array()),
      UInt64Array::valuesLength,
      long[]::new,
      (a, i, out) -> ((long[]) out)[i] = a.values(i)
    );
  }

  // ---------------- Float ----------------
  public static byte[] serializeFloat(float[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.FloatArray,
      FloatArray::createValuesVector,
      FloatArray::startFloatArray,
      FloatArray::addValues,
      FloatArray::endFloatArray
    );
  }

  public static ArrayResult parseFloat(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.FloatArray,
      c -> (FloatArray) c.data(new FloatArray()),
      FloatArray::valuesLength,
      float[]::new,
      (a, i, out) -> ((float[]) out)[i] = a.values(i)
    );
  }

  // ---------------- Double ----------------
  public static byte[] serializeDouble(double[] values, boolean[] validity) {
    return serializePrimitive(
      values, validity, ScalarArray.DoubleArray,
      DoubleArray::createValuesVector,
      DoubleArray::startDoubleArray,
      DoubleArray::addValues,
      DoubleArray::endDoubleArray
    );
  }

  public static ArrayResult parseDouble(byte[] buf) {
    return parsePrimitive(
      buf, ScalarArray.DoubleArray,
      c -> (DoubleArray) c.data(new DoubleArray()),
      DoubleArray::valuesLength,
      double[]::new,
      (a, i, out) -> ((double[]) out)[i] = a.values(i)
    );
  }

  // ---------------- String ----------------
  public static byte[] serializeString(String[] values, boolean[] validity) {
    // If validity is omitted, data must not contain nulls. If not,
    // the serializer can't differentiate null from an empty value.
    if (validity == null || validity.length == 0) {
      if (hasNulls(values)) {
        throw new IllegalArgumentException(
            "Empty validity vector not allowed when values contain nulls");
      }
    }
    FlatBufferBuilder b = new FlatBufferBuilder();
    int[] offs = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      offs[i] = b.createString(values[i] == null ? "" : values[i]);
    }
    int vec = StringArray.createValuesVector(b, offs);
    int valOff = validityOffset(b, validity);

    StringArray.startStringArray(b);
    StringArray.addValues(b, vec);
    int arrOff = StringArray.endStringArray(b);

    int root = finishContent(b, ScalarArray.StringArray, arrOff, valOff);
    b.finish(root);
    return b.sizedByteArray();
  }

  public static ArrayResult parseString(byte[] buf) {
    Objects.requireNonNull(buf, "buf must not be null");
    Content c = readContent(buf);
    if (c.dataType() != ScalarArray.StringArray) {
      throw new IllegalArgumentException("Unexpected type");
    }
    StringArray arr = new StringArray();
    c.data(arr);
    int m = arr.valuesLength();
    boolean[] validity = buildValidity(c, m);
    String[] out = new String[m];
    for (int i = 0; i < m; i++) {
      String s = arr.values(i);
      out[i] = (validity.length > i && !validity[i]) ? null : s;
    }
    return new ArrayResult(out, validity);
  }

  // ---------------- Binary ----------------
  public static byte[] serializeBinary(byte[][] values, boolean[] validity) {
    // If validity is omitted, data must not contain nulls. If not,
    // the serializer can't differentiate null from an empty value.
    if (validity == null || validity.length == 0) {
      if (hasNulls(values)) {
        throw new IllegalArgumentException(
            "Empty validity vector not allowed when values contain nulls");
      }
    }
    FlatBufferBuilder b = new FlatBufferBuilder();
    int[] elemOffs = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      byte[] v = (values[i] == null) ? new byte[0] : values[i];
      int vec = UInt8Array.createValuesVector(b, v);
      elemOffs[i] = UInt8Array.createUInt8Array(b, vec);
    }
    int valsVec = BinaryArray.createValuesVector(b, elemOffs);
    int arrOff = BinaryArray.createBinaryArray(b, valsVec);
    int valOff = validityOffset(b, validity);
    int root = finishContent(b, ScalarArray.BinaryArray, arrOff, valOff);
    b.finish(root);
    return b.sizedByteArray();
  }

  public static ArrayResult parseBinary(byte[] buf) {
    Objects.requireNonNull(buf, "buf must not be null");
    Content c = readContent(buf);
    if (c.dataType() != ScalarArray.BinaryArray) {
      throw new IllegalArgumentException("Unexpected type");
    }
    BinaryArray arr = new BinaryArray();
    c.data(arr);
    int m = arr.valuesLength();
    boolean[] validity = buildValidity(c, m);
    byte[][] out = new byte[m][];
    for (int i = 0; i < m; i++) {
      ByteBuffer bb = arr.values(i).valuesAsByteBuffer();
      byte[] v = new byte[bb.remaining()];
      bb.get(v);
      out[i] = v;
    }
    return new ArrayResult(out, validity);
  }

  // ---------------- Generic serialize/parse ----------------
  private static <V> byte[] serializePrimitive(
      V values, boolean[] validity, int discriminator,
      CreateVec<V> createVec, Start start, AddValues addValues, End end) {

    if (values != null && validity != null) {
      int valueLen = Array.getLength(values);
      if (validity.length != 0 && validity.length != valueLen) {
        throw new IllegalArgumentException(
            String.format("Validity length %d does not match values length %d",
                validity.length, valueLen));
      }
      if (validity.length == 0) {
        for (int i = 0; i < valueLen; i++) {
          Object elem = Array.get(values, i);
          if (elem == null) {
            throw new IllegalArgumentException(
                String.format("Empty validity vector provided, but values[%d] is null", i));
          }
        }
      }
    }

    FlatBufferBuilder b = new FlatBufferBuilder();
    int vecOff = (values == null) ? 0 : createVec.apply(b, values);
    int valOff = validityOffset(b, validity);

    start.apply(b);
    if (vecOff != 0) {
      addValues.apply(b, vecOff);
    }
    int arrOff = end.apply(b);

    int root = finishContent(b, discriminator, arrOff, valOff);
    b.finish(root);
    return b.sizedByteArray();
  }

  private static <A> ArrayResult parsePrimitive(
      byte[] buf, int discriminator,
      GetUnion<A> getUnion, ArrLen<A> lenFn,
      NewArray newArray, Copier<A> copy) {

    Objects.requireNonNull(buf, "buf must not be null");
    Content c = readContent(buf);
    if (c.dataType() != discriminator) {
      throw new IllegalArgumentException("Unexpected union type: " + c.dataType());
    }
    A arr = getUnion.apply(c);
    int m = lenFn.apply(arr);
    boolean[] validity = buildValidity(c, m);

    Object values = newArray.apply(m);
    for (int i = 0; i < m; i++) {
      copy.copy(arr, i, values);
    }

    return new ArrayResult(values, validity);
  }
}
