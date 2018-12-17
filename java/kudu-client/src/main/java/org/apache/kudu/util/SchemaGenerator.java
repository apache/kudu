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
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.PartialRow;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.kudu.util.DataGenerator.randomBinary;
import static org.apache.kudu.util.DataGenerator.randomDecimal;
import static org.apache.kudu.util.DataGenerator.randomString;

/**
 * A utility class to generate random schemas and schema components.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SchemaGenerator {

  // TODO(ghenke): Make string and binary length configurable.
  private static final int DEFAULT_BINARY_LENGTH = 128;
  private static final int MIN_HASH_BUCKETS = 2;

  private final Random random;
  private final int columnCount;
  private final int keyColumnCount;
  private final List<Type> types;
  private final List<Type> keyTypes;
  private final List<Encoding> encodings;
  private final List<CompressionAlgorithm> compressions;
  private final List<Integer> blockSizes;
  private final Float defaultRate;
  private final int minPrecision;
  private final int maxPrecision;

  private SchemaGenerator(final Random random,
                          final int columnCount,
                          final int keyColumnCount,
                          final List<Type> types,
                          final List<Type> keyTypes,
                          final List<Encoding> encodings,
                          final List<CompressionAlgorithm> compressions,
                          final List<Integer> blockSizes,
                          final Float defaultRate,
                          final int minPrecision,
                          final int maxPrecision) {
    this.random = random;
    this.columnCount = columnCount;
    this.keyColumnCount = keyColumnCount;
    this.types = types;
    this.keyTypes = keyTypes;
    this.encodings = encodings;
    this.compressions = compressions;
    this.blockSizes = blockSizes;
    this.defaultRate = defaultRate;
    this.minPrecision = minPrecision;
    this.maxPrecision = maxPrecision;
  }

  /**
   * Generates a random Schema.
   * @return a random Schema.
   */
  public Schema randomSchema() {
    List<ColumnSchema> columns = new ArrayList<>();
    for (int i = 0; i < columnCount; i++) {
      boolean key = i < keyColumnCount;
      Type colType = randomType(key);
      String colName = colType.getName() + i;
      ColumnSchema column = randomColumnSchema(colName, colType, key);
      columns.add(column);
    }
    return new Schema(columns);
  }

  /**
   * Generates a random ColumnSchema.
   * @return a random ColumnSchema.
   */
  public ColumnSchema randomColumnSchema(String name, Type type, boolean key) {
    final ColumnSchemaBuilder builder = new ColumnSchemaBuilder(name, type)
        .key(key)
        // TODO(ghenke): Make nullable columns configurable.
        .nullable(random.nextBoolean() && !key)
        .compressionAlgorithm(randomCompression())
        .desiredBlockSize(randomBlockSize())
        .encoding(randomEncoding(type));

    ColumnTypeAttributes typeAttributes = null;
    if (type == Type.DECIMAL) {
      int precision = random.nextInt((maxPrecision - minPrecision) + 1) + minPrecision;
      // TODO(ghenke): Make scale configurable.
      int scale = random.nextInt(precision);
      typeAttributes = DecimalUtil.typeAttributes(precision, scale);
      builder.typeAttributes(typeAttributes);
    }

    // Sometimes set a column default value.
    if (random.nextFloat() <= defaultRate) {
      switch (type) {
        case BOOL:
          builder.defaultValue(random.nextBoolean());
          break;
        case INT8:
          builder.defaultValue((byte)random.nextInt());
          break;
        case INT16:
          builder.defaultValue((short)random.nextInt());
          break;
        case INT32:
          builder.defaultValue(random.nextInt());
          break;
        case INT64:
        case UNIXTIME_MICROS:
          builder.defaultValue(random.nextLong());
          break;
        case FLOAT:
          builder.defaultValue(random.nextFloat());
          break;
        case DOUBLE:
          builder.defaultValue(random.nextDouble());
          break;
        case DECIMAL:
          builder.defaultValue(randomDecimal(typeAttributes, random));
          break;
        case STRING:
          builder.defaultValue(randomString(DEFAULT_BINARY_LENGTH, random));
          break;
        case BINARY:
          builder.defaultValue(randomBinary(DEFAULT_BINARY_LENGTH, random));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type " + type);
      }
    }
    return builder.build();
  }

  public int randomBlockSize() {
    return blockSizes.get(random.nextInt(blockSizes.size()));
  }

  public CompressionAlgorithm randomCompression() {
    return compressions.get(random.nextInt(compressions.size()));
  }

  public Type randomType(boolean key) {
    if (key) {
      return keyTypes.get(random.nextInt(keyTypes.size()));
    } else {
      return types.get(random.nextInt(types.size()));
    }
  }

  public Encoding randomEncoding(Type type) {
    final List<Encoding> validEncodings = new ArrayList<>(encodings);
    // Remove the unsupported encodings for the type.
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case UNIXTIME_MICROS:
        validEncodings.retainAll(Arrays.asList(
            Encoding.AUTO_ENCODING,
            Encoding.PLAIN_ENCODING,
            Encoding.BIT_SHUFFLE,
            Encoding.RLE));
        break;
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        validEncodings.retainAll(Arrays.asList(
            Encoding.AUTO_ENCODING,
            Encoding.PLAIN_ENCODING,
            Encoding.BIT_SHUFFLE));
        break;
      case STRING:
      case BINARY:
        validEncodings.retainAll(Arrays.asList(
            Encoding.AUTO_ENCODING,
            Encoding.PLAIN_ENCODING,
            Encoding.PREFIX_ENCODING,
            Encoding.DICT_ENCODING));
        break;
      case BOOL:
        validEncodings.retainAll(Arrays.asList(
            Encoding.AUTO_ENCODING,
            Encoding.PLAIN_ENCODING,
            Encoding.RLE));
        break;
      default: throw new IllegalArgumentException("Unsupported type " + type);
    }

    if (validEncodings.size() == 0) {
      throw new IllegalArgumentException("There are no valid encodings for type " + type);
    }

    return validEncodings.get(random.nextInt(validEncodings.size()));
  }

  public CreateTableOptions randomCreateTableOptions(Schema schema) {
    CreateTableOptions options = new CreateTableOptions();
    final List<ColumnSchema> keyColumns = schema.getPrimaryKeyColumns();

    // Add hash partitioning (Max out at 3 levels to avoid being excessive).
    int hashPartitionLevels = random.nextInt(Math.min(keyColumns.size(), 3)) + 1;
    for (int i = 0; i < hashPartitionLevels; i++) {
      final ColumnSchema hashColumn = keyColumns.get(i);
      // TODO(ghenke): Make buckets configurable.
      final int hashBuckets = random.nextInt(8) + MIN_HASH_BUCKETS;
      final int hashSeed = random.nextInt();
      options.addHashPartitions(Arrays.asList(hashColumn.getName()), hashBuckets, hashSeed);
    }

    boolean hasRangePartition = random.nextBoolean();
    ColumnSchema int64Key = null;
    for (ColumnSchema col : keyColumns) {
      if (col.getType() == Type.INT64) {
        int64Key = col;
        break;
      }
    }
    // TODO(ghenke): Configurable range partition rate and more supported types.
    if (hasRangePartition && int64Key != null) {
      options.setRangePartitionColumns(Arrays.asList(int64Key.getName()));
      int splits = random.nextInt(8); // TODO(ghenke): Configurable splits.
      List<Long> used = new ArrayList<>();
      int i = 0;
      while (i < splits) {
        PartialRow split = schema.newPartialRow();
        long value = random.nextLong();
        if (!used.contains(value)) {
          used.add(value);
          split.addLong(int64Key.getName(), random.nextLong());
          i++;
        }
      }
    }
    return options;
  }

  /**
   * A builder to configure and construct a SchemaGeneratorBuilder instance.
   */
  public static class SchemaGeneratorBuilder {

    private Random random = new Random(System.currentTimeMillis());

    private int columnCount = 10;
    private int keyColumnCount = 1;
    private List<Type> types = Arrays.asList(Type.values());
    private List<Encoding> encodings = new ArrayList<>();
    private List<CompressionAlgorithm> compressions = new ArrayList<>();
    // Default, min, middle, max.
    private List<Integer> blockSizes = Arrays.asList(0, 4096, 524288, 1048576);
    private float defaultRate = 0.25f;
    private int minPrecision = DecimalUtil.MIN_DECIMAL_PRECISION;
    private int maxPrecision = DecimalUtil.MAX_DECIMAL_PRECISION;

    public SchemaGeneratorBuilder() {
      // Add all encoding options and remove any invalid ones.
      encodings.addAll(Arrays.asList(Encoding.values()));
      encodings.remove(Encoding.UNKNOWN);
      // Add all compression options and remove any invalid ones.
      compressions.addAll(Arrays.asList(CompressionAlgorithm.values()));
      compressions.remove(CompressionAlgorithm.UNKNOWN);
    }

    /**
     * Define a custom Random instance to use for any random generation.
     * @return this instance
     */
    public SchemaGeneratorBuilder random(Random random) {
      this.random = random;
      return this;
    }

    /**
     * Define the column count of a random schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder columnCount(int columnCount) {
      Preconditions.checkArgument(columnCount > 0,
          "columnCount must be greater than 0");
      this.columnCount = columnCount;
      return this;
    }

    /**
     * Define the key column count of a random schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder keyColumnCount(int keyColumnCount) {
      Preconditions.checkArgument(columnCount > 0,
          "keyColumnCount must be greater than 0");
      this.keyColumnCount = keyColumnCount;
      return this;
    }

    /**
     * Define the types that can be used when randomly generating a column schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder types(Type... types) {
      this.types = Arrays.asList(types);
      return this;
    }

    /**
     * Define the types that can *not* be used when randomly generating a column schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder excludeTypes(Type... types) {
      List<Type> includedTypes = new ArrayList<>();
      // Add all possible types.
      includedTypes.addAll(Arrays.asList(Type.values()));
      // Remove the excluded types.
      for (Type type : types) {
        includedTypes.remove(type);
      }
      this.types = includedTypes;
      return this;
    }

    /**
     * Define the encoding options that can be used when randomly generating
     * a column schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder encodings(Encoding... encodings) {
      this.encodings = Arrays.asList(encodings);
      return this;
    }

    /**
     * Define the compression options that can be used when randomly generating
     * a column schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder compressions(CompressionAlgorithm... compressions) {
      this.compressions = Arrays.asList(compressions);
      return this;
    }

    /**
     * Define the rate at which default values should be used when randomly generating
     * a column schema.
     * @return this instance
     */
    public SchemaGeneratorBuilder defaultRate(float defaultRate) {
      Preconditions.checkArgument(defaultRate >= 0f && defaultRate <= 1f,
          "defaultRate must be between 0 and 1");
      this.defaultRate = defaultRate;
      return this;
    }

    /**
     * Define the precision value to use when when randomly generating
     * a column schema with a Decimal type.
     * @return this instance
     */
    public SchemaGeneratorBuilder precision(int precision) {
      return precisionRange(precision, precision);
    }

    /**
     * Define the range of precision values to use when when randomly generating
     * a column schema with a Decimal type.
     * @return this instance
     */
    public SchemaGeneratorBuilder precisionRange(int minPrecision, int maxPrecision) {
      Preconditions.checkArgument(minPrecision >= DecimalUtil.MIN_DECIMAL_PRECISION,
          "minPrecision must be greater than or equal to " +
              DecimalUtil.MIN_DECIMAL_PRECISION);
      Preconditions.checkArgument(maxPrecision <= DecimalUtil.MAX_DECIMAL_PRECISION,
          "maxPrecision must be less than or equal to " +
              DecimalUtil.MAX_DECIMAL_PRECISION);
      Preconditions.checkArgument(minPrecision <= maxPrecision,
          "minPrecision must be less than or equal to " + maxPrecision);
      this.minPrecision = minPrecision;
      this.maxPrecision = maxPrecision;
      return this;
    }

    public SchemaGenerator build() {
      Preconditions.checkArgument(keyColumnCount <= columnCount,
          "keyColumnCount must be less than or equal to the columnCount");

      // Filter the types that are compatible for key columns.
      List<Type> keyTypes = new ArrayList<>(types);
      keyTypes.removeAll(Arrays.asList(Type.BOOL, Type.FLOAT, Type.DOUBLE));
      Preconditions.checkArgument(!keyTypes.isEmpty(),
          "At least one type must be supported for key columns");

      return new SchemaGenerator(
          random,
          columnCount,
          keyColumnCount,
          types,
          keyTypes,
          encodings,
          compressions,
          blockSizes,
          defaultRate,
          minPrecision,
          maxPrecision
      );
    }
  }


}



