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

import com.google.common.base.Preconditions;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import javax.xml.bind.DatatypeConverter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

/**
 * A utility class to generate random data and rows.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DataGenerator {

  private final Random random;
  private final int stringLength;
  private final int binaryLength;
  private final float nullRate;
  private final float defaultRate;

  private DataGenerator(final Random random,
                        final int stringLength,
                        final int binaryLength,
                        final float nullRate,
                        final float defaultRate) {
    this.random = random;
    this.stringLength = stringLength;
    this.binaryLength = binaryLength;
    this.nullRate = nullRate;
    this.defaultRate = defaultRate;
  }

  /**
   * Randomizes the fields in a given PartialRow.
   * @param row the PartialRow to randomize.
   */
  public void randomizeRow(PartialRow row) {
    Schema schema = row.getSchema();
    List<ColumnSchema> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      ColumnSchema col = columns.get(i);
      Type type = col.getType();
      if (col.isNullable() && random.nextFloat() <= nullRate) {
        // Sometimes set nullable columns to null.
        row.setNull(i);
      } else if(col.getDefaultValue() != null && !col.isKey() && random.nextFloat() <= defaultRate) {
        // Sometimes use the column default value.
      } else {
        switch (type) {
          // TODO(ghenke): Support range bound configuration.
          case BOOL:
            row.addBoolean(i, random.nextBoolean()); break;
          case INT8:
            row.addByte(i, (byte) random.nextInt()); break;
          case INT16:
            row.addShort(i, (short) random.nextInt()); break;
          case INT32:
            row.addInt(i, random.nextInt()); break;
          case INT64:
          case UNIXTIME_MICROS:
            row.addLong(i, random.nextLong()); break;
          case FLOAT:
            row.addFloat(i, random.nextFloat()); break;
          case DOUBLE:
            row.addDouble(i, random.nextDouble()); break;
          case DECIMAL:
            row.addDecimal(i, randomDecimal(col.getTypeAttributes(), random)); break;
          case STRING:
            row.addString(i, randomString(stringLength, random)); break;
          case BINARY:
            row.addBinary(i, randomBinary(binaryLength, random)); break;
          default:
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
      }
    }
  }

  /**
   * Utility method to return a random decimal value.
   */
  public static BigDecimal randomDecimal(ColumnTypeAttributes attributes, Random random) {
    int numBits = BigInteger.TEN.pow(attributes.getPrecision())
        .subtract(BigInteger.ONE).bitCount();
    BigInteger randomUnscaled = new BigInteger(numBits, random);
    return new BigDecimal(randomUnscaled, attributes.getScale());
  }

  /**
   * Utility method to return a random string value.
   */
  public static String randomString(int length, Random random) {
    byte bytes[] = new byte[length];
    random.nextBytes(bytes);
    return DatatypeConverter.printBase64Binary(bytes);
  }

  /**
   * Utility method to return a random binary value.
   */
  public static byte[] randomBinary(int length, Random random) {
    byte bytes[] = new byte[length];
    random.nextBytes(bytes);
    return bytes;
  }

  /**
   *  A builder to configure and construct a DataGenerator instance.
   */
  public static class DataGeneratorBuilder {

    private Random random = new Random(System.currentTimeMillis());
    private int stringLength = 128;
    private int binaryLength = 128;
    private float nullRate = 0.1f;
    private float defaultRate = 0.1f;

    public DataGeneratorBuilder() {}

    /**
     * Define a custom Random instance to use for any random generation.
     * @return this instance
     */
    public DataGeneratorBuilder random(Random random) {
      this.random = random;
      return this;
    }

    /**
     * Define the length of the data when randomly generating column values for string columns.
     * @return this instance
     */
    public DataGeneratorBuilder stringLength(int stringLength) {
      this.stringLength = stringLength;
      return this;
    }

    /**
     * Define the length of the data when randomly generating column values for binary columns.
     * @return this instance
     */
    public DataGeneratorBuilder binaryLength(int binaryLength) {
      this.binaryLength = binaryLength;
      return this;
    }

    /**
     * Define the rate at which null values should be used when randomly generating
     * column values.
     * @return this instance
     */
    public DataGeneratorBuilder nullRate(float nullRate) {
      Preconditions.checkArgument(nullRate >= 0f && nullRate <= 1f,
          "nullRate must be between 0 and 1");
      this.nullRate = nullRate;
      return this;
    }

    /**
     * Define the rate at which default values should be used when randomly generating
     * column values.
     * @return this instance
     */
    public DataGeneratorBuilder defaultRate(float defaultRate) {
      Preconditions.checkArgument(defaultRate >= 0f && defaultRate <= 1f,
          "defaultRate must be between 0 and 1");
      this.defaultRate = defaultRate;
      return this;
    }

    public DataGenerator build() {
      return new DataGenerator(
          random,
          stringLength,
          binaryLength,
          nullRate,
          defaultRate
      );
    }
  }
}



