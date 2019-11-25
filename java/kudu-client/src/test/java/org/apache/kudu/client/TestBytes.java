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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;
import org.apache.kudu.util.DecimalUtil;

public class TestBytes {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void test() {
    byte[] bytes = new byte[16];

    // Boolean
    Bytes.setUnsignedByte(bytes, (short) 1);
    assertTrue(Bytes.getBoolean(bytes));
    Bytes.setUnsignedByte(bytes, (short) 0);
    assertFalse(Bytes.getBoolean(bytes));

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
    float floatVal = 123.456f;
    Bytes.setFloat(bytes, floatVal);
    assertEquals(floatVal, Bytes.getFloat(bytes), 0.001);

    // DOUBLE
    double doubleVal = 123.456;
    Bytes.setDouble(bytes, doubleVal);
    assertEquals(doubleVal, Bytes.getDouble(bytes), 0.001);

    // DECIMAL (32 bits)
    BigDecimal smallDecimal = new BigDecimal(BigInteger.valueOf(123456789), 0,
        new MathContext(DecimalUtil.MAX_DECIMAL32_PRECISION, RoundingMode.UNNECESSARY));
    Bytes.setBigDecimal(bytes, smallDecimal, DecimalUtil.MAX_DECIMAL32_PRECISION);
    assertEquals(smallDecimal,
        Bytes.getDecimal(bytes, 0, DecimalUtil.MAX_DECIMAL32_PRECISION, 0));
    BigDecimal negSmallDecimal = new BigDecimal(BigInteger.valueOf(-123456789), 0,
        new MathContext(DecimalUtil.MAX_DECIMAL32_PRECISION, RoundingMode.UNNECESSARY));
    Bytes.setBigDecimal(bytes, negSmallDecimal, DecimalUtil.MAX_DECIMAL32_PRECISION);
    assertEquals(negSmallDecimal,
        Bytes.getDecimal(bytes, 0, DecimalUtil.MAX_DECIMAL32_PRECISION, 0));

    // DECIMAL (64 bits)
    BigDecimal mediumDecimal = new BigDecimal(BigInteger.valueOf(123456789L), 0,
        new MathContext(DecimalUtil.MAX_DECIMAL64_PRECISION, RoundingMode.UNNECESSARY));
    Bytes.setBigDecimal(bytes, mediumDecimal, DecimalUtil.MAX_DECIMAL64_PRECISION);
    assertEquals(mediumDecimal,
        Bytes.getDecimal(bytes, DecimalUtil.MAX_DECIMAL64_PRECISION, 0));
    BigDecimal negMediumDecimal = new BigDecimal(BigInteger.valueOf(-123456789L), 0,
        new MathContext(DecimalUtil.MAX_DECIMAL64_PRECISION, RoundingMode.UNNECESSARY));
    Bytes.setBigDecimal(bytes, negMediumDecimal, DecimalUtil.MAX_DECIMAL64_PRECISION);
    assertEquals(negMediumDecimal,
        Bytes.getDecimal(bytes, DecimalUtil.MAX_DECIMAL64_PRECISION, 0));

    // DECIMAL (128 bits)
    BigDecimal largeDecimal =
        new BigDecimal(new java.math.BigInteger("1234567891011121314151617181920212223"), 0,
        new MathContext(DecimalUtil.MAX_DECIMAL128_PRECISION, RoundingMode.UNNECESSARY));
    Bytes.setBigDecimal(bytes, largeDecimal, DecimalUtil.MAX_DECIMAL128_PRECISION);
    assertEquals(largeDecimal,
        Bytes.getDecimal(bytes, DecimalUtil.MAX_DECIMAL128_PRECISION, 0));
    BigDecimal negLargeDecimal =
        new BigDecimal(new java.math.BigInteger("-1234567891011121314151617181920212223"), 0,
            new MathContext(DecimalUtil.MAX_DECIMAL128_PRECISION, RoundingMode.UNNECESSARY));
    Bytes.setBigDecimal(bytes, negLargeDecimal, DecimalUtil.MAX_DECIMAL128_PRECISION);
    assertEquals(negLargeDecimal,
        Bytes.getDecimal(bytes, DecimalUtil.MAX_DECIMAL128_PRECISION, 0));
  }

  @Test
  public void testHex() {
    byte[] bytes = new byte[] { (byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67,
                                (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF };
    Assert.assertEquals("0x0123456789ABCDEF", Bytes.hex(bytes));
  }
}
