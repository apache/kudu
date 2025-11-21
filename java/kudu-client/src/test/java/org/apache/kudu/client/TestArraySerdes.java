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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import org.junit.Test;

public class TestArraySerdes {
  // ----------------------------
  // INT8
  // ----------------------------
  @Test
  public void testInt8RoundTrip() {
    byte[] vals = {1, -2, 127};
    boolean[] validity = {true, false, true};

    byte[] buf = Array1dSerdes.serializeInt8(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt8(buf);

    assertArrayEquals(vals, (byte[]) res.getValues());
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // INT32
  // ----------------------------
  @Test
  public void testInt32RoundTrip() {
    int[] vals = {1, -2, 42};
    boolean[] validity = {true, false, true};

    byte[] buf = Array1dSerdes.serializeInt32(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt32(buf);

    assertArrayEquals(vals, (int[]) res.getValues());
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // INT64
  // ----------------------------
  @Test
  public void testInt64RoundTrip() {
    long[] vals = {1L, -2L, Long.MAX_VALUE};
    boolean[] validity = {true, true, false};

    byte[] buf = Array1dSerdes.serializeInt64(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt64(buf);

    assertArrayEquals(vals, (long[]) res.getValues());
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // INT16
  // ----------------------------
  @Test
  public void testInt16RoundTrip() {
    short[] vals = {1, -2, 32767};
    boolean[] validity = {true, true, true};

    byte[] buf = Array1dSerdes.serializeInt16(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt16(buf);

    assertArrayEquals(vals, (short[]) res.getValues());
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // UINT8
  // ----------------------------
  @Test
  public void testUInt8RoundTrip() {
    byte[] vals = {(byte) 0, (byte) 255};
    boolean[] validity = {true, true};

    byte[] buf = Array1dSerdes.serializeUInt8(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseUInt8(buf);

    int[] parsed = (int[]) res.getValues();
    assertEquals(0, parsed[0]);
    assertEquals(255, parsed[1]);
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // UINT16
  // ----------------------------
  @Test
  public void testUInt16RoundTrip() {
    short[] vals = {0, (short) 0xFFFF};
    boolean[] validity = {true, true};

    byte[] buf = Array1dSerdes.serializeUInt16(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseUInt16(buf);

    int[] parsed = (int[]) res.getValues();
    assertEquals(0, parsed[0]);
    assertEquals(65535, parsed[1]);
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // UINT32
  // ----------------------------
  @Test
  public void testUInt32RoundTrip() {
    int[] vals = {0, -1};
    boolean[] validity = {true, true};

    byte[] buf = Array1dSerdes.serializeUInt32(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseUInt32(buf);

    long[] parsed = (long[]) res.getValues();
    assertEquals(0L, parsed[0]);
    assertEquals(4294967295L, parsed[1]);
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // UINT64
  // ----------------------------
  @Test
  public void testUInt64RoundTrip() {
    long[] vals = {0L, -1L};
    boolean[] validity = {true, true};

    byte[] buf = Array1dSerdes.serializeUInt64(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseUInt64(buf);

    long[] parsed = (long[]) res.getValues();
    assertEquals(0L, parsed[0]);
    assertEquals(-1L, parsed[1]);
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // FLOAT
  // ----------------------------
  @Test
  public void testFloatRoundTrip() {
    float[] vals = {1.0f, -2.5f, Float.NaN};
    boolean[] validity = {true, false, true};

    byte[] buf = Array1dSerdes.serializeFloat(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseFloat(buf);

    assertArrayEquals(vals, (float[]) res.getValues(), 0.0001f);
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // DOUBLE
  // ----------------------------
  @Test
  public void testDoubleRoundTrip() {
    double[] vals = {1.0, -2.5, Double.MAX_VALUE};
    boolean[] validity = {true, true, false};

    byte[] buf = Array1dSerdes.serializeDouble(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseDouble(buf);

    assertArrayEquals(vals, (double[]) res.getValues(), 0.0001);
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // STRING
  // ----------------------------
  @Test
  public void testStringRoundTrip() {
    String[] vals = {"foo", "bar", null};
    boolean[] validity = {true, true, false};

    byte[] buf = Array1dSerdes.serializeString(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseString(buf);

    assertArrayEquals(vals, (String[]) res.getValues());
    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // BINARY
  // ----------------------------
  @Test
  public void testBinaryRoundTrip() {
    byte[][] vals = {
        new byte[] {1, 2, 3},
        new byte[] {10, 20},
        null
    };
    boolean[] validity = {true, true, false};

    byte[] buf = Array1dSerdes.serializeBinary(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseBinary(buf);

    byte[][] decoded = (byte[][]) res.getValues();

    assertEquals(vals.length, decoded.length);
    for (int i = 0; i < vals.length; i++) {
      if (validity[i]) {
        assertArrayEquals(vals[i], decoded[i]);
      } else {
        assertEquals(0, decoded[i].length);
      }
    }

    assertArrayEquals(validity, res.getValidity());
  }

  // ----------------------------
  // INT32 edge cases
  // ----------------------------
  @Test
  public void testInt32RoundTripEmptyArray() {
    int[] vals = {};
    boolean[] validity = {};

    byte[] buf = Array1dSerdes.serializeInt32(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt32(buf);

    assertEquals(0, ((int[]) res.getValues()).length);
    assertEquals(0, res.getValidity().length);
  }

  // Null and empty arrays both deserialize to int[0] because FlatBuffers treats “absent”
  // and “empty” vectors the same. The actual null vs. empty distinction is tracked
  // separately via the cell’s non-null bitmap.
  @Test
  public void testInt32RoundTripNullArray() {
    int[] vals = null;
    boolean[] validity = null;

    byte[] buf = Array1dSerdes.serializeInt32(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt32(buf);

    // Expect the result to represent an empty array.
    assertNotNull(res.getValues());
    assertEquals(0, ((int[]) res.getValues()).length);

    // Validity should also be non-null and length 0
    assertNotNull(res.getValidity());
    assertEquals(0, res.getValidity().length);
  }

  // ----------------------------
  // Empty validity vector tests
  // ----------------------------
  @Test
  public void testEmptyValidityInt32AllValid() {
    int[] vals = {10, 20, 30};
    boolean[] validity = new boolean[0];

    byte[] buf = Array1dSerdes.serializeInt32(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseInt32(buf);

    assertArrayEquals(vals, (int[]) res.getValues());
    assertArrayEquals(new boolean[]{true, true, true}, res.getValidity());
  }

  @Test
  public void testEmptyValidityStringAllValid() {
    String[] vals = {"a", "b", "c"};
    boolean[] validity = new boolean[0];

    byte[] buf = Array1dSerdes.serializeString(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseString(buf);

    assertArrayEquals(vals, (String[]) res.getValues());
    assertArrayEquals(new boolean[]{true, true, true}, res.getValidity());
  }

  @Test
  public void testEmptyValidityBinaryAllValid() {
    byte[][] vals = {
        new byte[]{1, 2},
        new byte[]{3, 4}
    };
    boolean[] validity = new boolean[0];

    byte[] buf = Array1dSerdes.serializeBinary(vals, validity);
    Array1dSerdes.ArrayResult res = Array1dSerdes.parseBinary(buf);

    byte[][] decoded = (byte[][]) res.getValues();
    assertEquals(vals.length, decoded.length);
    for (int i = 0; i < vals.length; i++) {
      assertArrayEquals(vals[i], decoded[i]);
    }
    assertArrayEquals(new boolean[]{true, true}, res.getValidity());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyValidityStringWithNullsRejected() {
    String[] vals = {"a", null, "b"};
    boolean[] validity = new boolean[0];
    Array1dSerdes.serializeString(vals, validity);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyValidityBinaryWithNullsRejected() {
    byte[][] vals = {
        new byte[]{1, 2},
        null,
        new byte[]{3}
    };
    boolean[] validity = new boolean[0];
    Array1dSerdes.serializeBinary(vals, validity);
  }

  // All true validity vector test
  @Test
  public void testAllTrueValiditySkipped_Int32() {
    byte[] buf1 = Array1dSerdes.serializeInt32(new int[]{1, 2, 3}, null);
    byte[] buf2 = Array1dSerdes.serializeInt32(new int[]{1, 2, 3},
        new boolean[]{true, true, true});
    byte[] buf3 = Array1dSerdes.serializeInt32(new int[]{1, 2, 3}, new boolean[0]);

    assertArrayEquals(buf1, buf2);
    assertArrayEquals(buf1, buf3);
  }

  @Test
  public void testAllTrueValiditySkipped_String() {
    String[] vals = {"a", "b", "c"};
    byte[] buf1 = Array1dSerdes.serializeString(vals, null);
    byte[] buf2 = Array1dSerdes.serializeString(vals,
        new boolean[]{true, true, true});
    byte[] buf3 = Array1dSerdes.serializeString(vals, new boolean[0]);

    assertArrayEquals(buf1, buf2);
    assertArrayEquals(buf1, buf3);
  }

  @Test
  public void testAllTrueValiditySkipped_Binary() {
    byte[][] vals = {
        new byte[]{1, 2},
        new byte[]{3, 4}
    };
    byte[] buf1 = Array1dSerdes.serializeBinary(vals, null);
    byte[] buf2 = Array1dSerdes.serializeBinary(vals,
        new boolean[]{true, true});
    byte[] buf3 = Array1dSerdes.serializeBinary(vals, new boolean[0]);

    assertArrayEquals(buf1, buf2);
    assertArrayEquals(buf1, buf3);
  }


}
