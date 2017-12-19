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

package org.apache.kudu.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

import com.google.common.base.Strings;

import org.apache.kudu.ColumnTypeAttributes;

import static org.apache.kudu.Common.DataType;

public class DecimalUtil {
  public static final int MAX_DECIMAL32_PRECISION = 9;
  public static final int MAX_UNSCALED_DECIMAL32 = 999999999;
  public static final int MIN_UNSCALED_DECIMAL32 = -MAX_UNSCALED_DECIMAL32;
  public static final int DECIMAL32_SIZE = 32 / Byte.SIZE;

  public static final int MAX_DECIMAL64_PRECISION = 18;
  public static final long MAX_UNSCALED_DECIMAL64 = 999999999999999999L;
  public static final long MIN_UNSCALED_DECIMAL64 = -MAX_UNSCALED_DECIMAL64;
  public static final int DECIMAL64_SIZE = 64 / Byte.SIZE;

  public static final int MAX_DECIMAL128_PRECISION = 38;
  public static final BigInteger MAX_UNSCALED_DECIMAL128 =
      new BigInteger(Strings.repeat("9", MAX_DECIMAL128_PRECISION));
  public static final BigInteger MIN_UNSCALED_DECIMAL128 = MAX_UNSCALED_DECIMAL128.negate();
  public static final int DECIMAL128_SIZE = 128 / Byte.SIZE;

  public static final int MAX_DECIMAL_PRECISION = MAX_DECIMAL128_PRECISION;

  /**
   * Given a precision, returns the size of the Decimal in Bytes.
   * @return the size in Bytes.
   */
  public static int precisionToSize(int precision) {
    if (precision <= MAX_DECIMAL32_PRECISION) {
      return DECIMAL32_SIZE;
    } else if (precision <= MAX_DECIMAL64_PRECISION) {
      return DECIMAL64_SIZE;
    } else if (precision <= MAX_DECIMAL128_PRECISION) {
      return DECIMAL128_SIZE;
    } else {
      throw new IllegalArgumentException("Unsupported decimal type precision: " + precision);
    }
  }

  /**
   * Given a precision, returns the smallest unscaled data type.
   * @return the smallest valid DataType.
   */
  public static DataType precisionToDataType(int precision) {
    if (precision <= MAX_DECIMAL32_PRECISION) {
      return DataType.DECIMAL32;
    } else if (precision <= MAX_DECIMAL64_PRECISION) {
      return DataType.DECIMAL64;
    } else if (precision <= MAX_DECIMAL128_PRECISION) {
      return DataType.DECIMAL128;
    } else {
      throw new IllegalArgumentException("Unsupported decimal type precision: " + precision);
    }
  }

  /**
   * Returns the maximum value of a Decimal give a precision and scale.
   * @param precision the precision of the decimal.
   * @param scale the scale of the decimal.
   * @return the maximum decimal value.
   */
  public static BigDecimal maxValue(int precision, int scale) {
    String maxPrecision = Strings.repeat("9", precision);
    return new BigDecimal(new BigInteger(maxPrecision), scale);
  }

  /**
   * Returns the minimum value of a Decimal give a precision and scale.
   * @param precision the precision of the decimal.
   * @param scale the scale of the decimal.
   * @return the minimum decimal value.
   */
  public static BigDecimal minValue(int precision, int scale) {
    return maxValue(precision, scale).negate();
  }

  /**
   * Returns the smallest value of a Decimal give a precision and scale.
   * This value can be useful for incrementing a Decimal.
   * @param scale the scale of the decimal.
   * @return the smallest decimal value.
   */
  public static BigDecimal smallestValue(int scale) {
    return new BigDecimal(BigInteger.ONE, scale);
  }

  /**
   * Attempts to coerce a big decimal to a target precision and scale and
   * returns the result. Throws an {@link IllegalArgumentException} if the value
   * can't be coerced without rounding or exceeding the targetPrecision.
   *
   * @param val the BigDecimal value to coerce.
   * @param targetPrecision the target precision of the coerced value.
   * @param targetScale the target scale of the coerced value.
   * @return the coerced BigDecimal value.
   */
  public static BigDecimal coerce(BigDecimal val, int targetPrecision, int targetScale) {
    if (val.scale() != targetScale) {
      try {
        val = val.setScale(targetScale, BigDecimal.ROUND_UNNECESSARY);
      } catch (ArithmeticException ex) {
        throw new IllegalArgumentException("Value scale " + val.scale() +
            " can't be coerced to target scale " +  targetScale + ". ");
      }
    }
    if (val.precision() > targetPrecision) {
      throw new IllegalArgumentException("Value precision " + val.precision() +
          " (after scale coercion) can't be coerced to target precision " +  targetPrecision + ". ");
    }
    return val;
  }

  /**
   * Convenience method to create column type attributes for decimal columns.
   * @param precision the precision.
   * @param scale the scale.
   * @return the column type attributes.
   */
  public static ColumnTypeAttributes typeAttributes(int precision, int scale) {
    return new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
        .precision(precision)
        .scale(scale)
        .build();
  }

}
