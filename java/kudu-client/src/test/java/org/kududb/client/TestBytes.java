// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.math.BigInteger;

public class TestBytes {

  @Test
  public void test() {
    byte[] bytes = new byte[8];

    // Boolean
    Bytes.setUnsignedByte(bytes, (short) 1);
    assert(Bytes.getBoolean(bytes));
    Bytes.setUnsignedByte(bytes, (short) 0);
    assert(!Bytes.getBoolean(bytes));

    // BYTES
    short smallUbyte = 120;
    Bytes.setUnsignedByte(bytes, smallUbyte);
    assertEquals(smallUbyte, Bytes.getUnsignedByte(bytes));
    short largeUbyte = 250;
    Bytes.setUnsignedByte(bytes, largeUbyte);
    assertEquals(largeUbyte, Bytes.getUnsignedByte(bytes));

    // SHORTS
    short nshort = -300;
    Bytes.setShort(bytes, nshort);
    assertEquals(nshort, Bytes.getShort(bytes));
    short pshort = 300;
    Bytes.setShort(bytes, pshort);
    assertEquals(pshort, Bytes.getShort(bytes));
    int smallUshort = 300;
    Bytes.setUnsignedShort(bytes, smallUshort);
    assertEquals(smallUshort, Bytes.getUnsignedShort(bytes));
    int largeUshort = 60000;
    Bytes.setUnsignedShort(bytes, largeUshort);
    assertEquals(largeUshort, Bytes.getUnsignedShort(bytes));

    // INTS
    int nint = -60000;
    Bytes.setInt(bytes, nint);
    assertEquals(nint, Bytes.getInt(bytes));
    int pint = 60000;
    Bytes.setInt(bytes, pint);
    assertEquals(pint, Bytes.getInt(bytes));
    long smallUint = 60000;
    Bytes.setUnsignedInt(bytes, smallUint);
    assertEquals(smallUint, Bytes.getUnsignedInt(bytes));
    long largeUint = 4000000000L;
    Bytes.setUnsignedInt(bytes, largeUint);
    assertEquals(largeUint, Bytes.getUnsignedInt(bytes));

    // LONGS
    long nlong = -4000000000L;
    Bytes.setLong(bytes, nlong);
    assertEquals(nlong, Bytes.getLong(bytes));
    long plong = 4000000000L;
    Bytes.setLong(bytes, plong);
    assertEquals(plong, Bytes.getLong(bytes));
    BigInteger smallUlong = new BigInteger("4000000000");
    Bytes.setUnsignedLong(bytes, smallUlong);
    assertEquals(smallUlong, Bytes.getUnsignedLong(bytes));
    BigInteger largeUlong = new BigInteger("10000000000000000000");
    Bytes.setUnsignedLong(bytes, largeUlong);
    assertEquals(largeUlong, Bytes.getUnsignedLong(bytes));

    // FLOAT
    float aFloat = 123.456f;
    Bytes.setFloat(bytes, aFloat);
    assertEquals(aFloat, Bytes.getFloat(bytes), 0.001);

    // DOUBLE
    double aDouble = 123.456;
    Bytes.setDouble(bytes, aDouble);
    assertEquals(aDouble, Bytes.getDouble(bytes), 0.001);
  }
}
