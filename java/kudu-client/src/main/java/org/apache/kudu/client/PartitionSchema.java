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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedBytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Schema;

/**
 * A partition schema describes how the rows of a table are distributed among
 * tablets.
 *
 * Primarily, a table's partition schema is responsible for translating the
 * primary key column values of a row into a partition key that can be used to
 * find the tablet containing the key.
 *
 * In case of table-wide hash partitioning, the partition schema is made up of
 * zero or more hash bucket components, followed by a single range component.
 * In case of custom hash bucketing per range, the partition schema contains
 * information on hash bucket components per range.
 *
 * Each hash bucket component includes one or more columns from the primary key
 * column set, with the restriction that an individual primary key column may
 * only be included in a single hash component.
 *
 * This class is new, and not considered stable or suitable for public use.
 */
@InterfaceAudience.LimitedPrivate("Impala")
@InterfaceStability.Unstable
public class PartitionSchema {

  private static final class BoundsComparator
      implements Comparator<EncodedRangeBoundsWithHashSchema>, Serializable {
    private static final long serialVersionUID = 36028797018963969L;
    private static final Comparator<byte[]> comparator =
        UnsignedBytes.lexicographicalComparator();

    @Override
    public int compare(EncodedRangeBoundsWithHashSchema lhs,
                       EncodedRangeBoundsWithHashSchema rhs) {
      return comparator.compare(lhs.lower, rhs.lower);
    }
  }

  private static final Comparator<EncodedRangeBoundsWithHashSchema> COMPARATOR =
      new BoundsComparator();

  private final RangeSchema rangeSchema;
  private final List<HashBucketSchema> hashBucketSchemas;
  private final List<RangeWithHashSchema> rangesWithHashSchemas;
  private final List<EncodedRangeBoundsWithHashSchema> encodedRangesWithHashSchemas;
  private TreeSet<EncodedRangeBoundsWithHashSchema> hashSchemasPerRange;
  private final boolean isSimple;

  static class EncodedRangeBoundsWithHashSchema {
    final byte[] lower;
    final byte[] upper;
    final List<HashBucketSchema> hashSchemas;

    public EncodedRangeBoundsWithHashSchema(
        byte[] lower,
        byte[] upper,
        List<HashBucketSchema> hashSchemas) {
      Preconditions.checkNotNull(lower);
      Preconditions.checkNotNull(upper);
      Preconditions.checkState(upper.length == 0 || Bytes.memcmp(lower, upper) < 0);
      this.lower = lower;
      this.upper = upper;
      this.hashSchemas = hashSchemas;
    }
  }

  /**
   * Creates a new partition schema from the range and hash bucket schemas.
   *
   * @param rangeSchema the range schema
   * @param hashBucketSchemas the table-wide hash schema
   * @param schema the table schema
   */
  public PartitionSchema(RangeSchema rangeSchema,
                         List<HashBucketSchema> hashBucketSchemas,
                         List<RangeWithHashSchema> rangesWithHashSchemas,
                         Schema schema) {
    this.rangeSchema = rangeSchema;
    this.hashBucketSchemas = hashBucketSchemas;
    this.rangesWithHashSchemas = rangesWithHashSchemas;
    this.hashSchemasPerRange = new TreeSet<>(COMPARATOR);
    this.encodedRangesWithHashSchemas = new ArrayList<>(rangesWithHashSchemas.size());

    for (RangeWithHashSchema rhs : this.rangesWithHashSchemas) {
      final boolean isLowerBoundEmpty =
          rhs.lowerBound == null || rhs.lowerBound.getColumnsBitSet().isEmpty();
      byte[] lower = isLowerBoundEmpty ? new byte[0]
          : KeyEncoder.encodeRangePartitionKey(rhs.lowerBound, this.rangeSchema);
      final boolean isUpperBoundEmpty =
          rhs.upperBound == null || rhs.upperBound.getColumnsBitSet().isEmpty();
      byte[] upper = isUpperBoundEmpty ? new byte[0]
          : KeyEncoder.encodeRangePartitionKey(rhs.upperBound, this.rangeSchema);
      if (!hashSchemasPerRange.add(
          new EncodedRangeBoundsWithHashSchema(lower, upper, rhs.hashSchemas))) {
        throw new IllegalArgumentException(
            rhs.lowerBound.toString() + ": duplicate lower range boundary");
      }
    }

    // Populate the convenience collection storing the information on ranges
    // with encoded bounds sorted in ascending order by lower bounds.
    encodedRangesWithHashSchemas.addAll(this.hashSchemasPerRange);

    boolean isSimple =
        rangesWithHashSchemas.isEmpty() &&
        hashBucketSchemas.isEmpty() &&
        rangeSchema.columns.size() == schema.getPrimaryKeyColumnCount();
    if (isSimple) {
      int i = 0;
      for (Integer id : rangeSchema.columns) {
        if (schema.getColumnIndex(id) != i++) {
          isSimple = false;
          break;
        }
      }
    }
    this.isSimple = isSimple;
  }

  /**
   * Creates a new partition schema from the range and hash bucket schemas.
   *
   * @param rangeSchema the range schema
   * @param hashBucketSchemas the table-wide hash schema
   * @param schema the table schema
   */
  public PartitionSchema(RangeSchema rangeSchema,
                         List<HashBucketSchema> hashBucketSchemas,
                         Schema schema) {
    this(rangeSchema, hashBucketSchemas, ImmutableList.of(), schema);
  }

  /**
   * Returns the encoded partition key of the row.
   * @return a byte array containing the encoded partition key of the row
   */
  public byte[] encodePartitionKey(PartialRow row) {
    return KeyEncoder.encodePartitionKey(row, this);
  }

  public RangeSchema getRangeSchema() {
    return rangeSchema;
  }

  public List<HashBucketSchema> getHashBucketSchemas() {
    return hashBucketSchemas;
  }

  public List<RangeWithHashSchema> getRangesWithHashSchemas() {
    return rangesWithHashSchemas;
  }

  List<EncodedRangeBoundsWithHashSchema> getEncodedRangesWithHashSchemas() {
    return encodedRangesWithHashSchemas;
  }

  /**
   * Returns true if the partition schema if the partition schema does not include any hash
   * components, and the range columns match the table's primary key columns.
   *
   * @return whether the partition schema is the default simple range partitioning.
   */
  boolean isSimpleRangePartitioning() {
    return isSimple;
  }

  /**
   * @return whether the partition schema has ranges with custom hash schemas.
   */
  boolean hasCustomHashSchemas() {
    return !rangesWithHashSchemas.isEmpty();
  }

  /**
   * Find hash schema for the given encoded range key. Depending on the
   * partition schema and the key, it might be either table-wide or a custom
   * hash schema for a particular range.
   *
   * @return hash bucket schema for the encoded range key
   */
  List<HashBucketSchema> getHashSchemaForRange(byte[] rangeKey) {
    if (!hasCustomHashSchemas()) {
      // By definition, the table-wide hash schema provides the hash bucketing
      // structure in the absence of per-range custom hash schemas.
      return hashBucketSchemas;
    }

    final EncodedRangeBoundsWithHashSchema entry = hashSchemasPerRange.floor(
        new EncodedRangeBoundsWithHashSchema(rangeKey, new byte[0], ImmutableList.of()));
    if (entry == null) {
      return hashBucketSchemas;
    }
    // Check if 'rangeKey' is in the range (null upper boundary means unbounded
    // range partition).
    final byte[] upper = entry.upper;
    if (upper == null || Bytes.memcmp(rangeKey, upper) < 0) {
      return entry.hashSchemas;
    }
    return hashBucketSchemas;
  }

  public static class RangeSchema {
    private final List<Integer> columns;

    public RangeSchema(List<Integer> columns) {
      this.columns = columns;
    }

    /**
     * Gets the column IDs of the columns in the range partition.
     * @return the column IDs of the columns in the range partition
     * @deprecated Use {@link #getColumnIds} instead.
     */
    @Deprecated
    public List<Integer> getColumns() {
      return columns;
    }

    /**
     * Gets the column IDs of the columns in the range partition.
     * @return the column IDs of the columns in the range partition
     */
    public List<Integer> getColumnIds() {
      return columns;
    }
  }

  public static class HashBucketSchema {
    private final List<Integer> columnIds;
    private int numBuckets;
    private int seed;

    public HashBucketSchema(List<Integer> columnIds, int numBuckets, int seed) {
      this.columnIds = columnIds;
      this.numBuckets = numBuckets;
      this.seed = seed;
    }

    /**
     * Gets the column IDs of the columns in the hash partition.
     * @return the column IDs of the columns in the hash partition
     */
    public List<Integer> getColumnIds() {
      return columnIds;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    public int getSeed() {
      return seed;
    }
  }

  /**
   * This utility class is used to represent information on a custom hash schema
   * for a particular range.
   */
  public static class RangeWithHashSchema {
    public PartialRow lowerBound;
    public PartialRow upperBound;
    public List<HashBucketSchema> hashSchemas;

    public RangeWithHashSchema(
        PartialRow lowerBound,
        PartialRow upperBound,
        List<HashBucketSchema> hashSchemas) {
      Preconditions.checkNotNull(lowerBound);
      Preconditions.checkNotNull(upperBound);
      Preconditions.checkArgument(
          lowerBound.getSchema().equals(upperBound.getSchema()));
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.hashSchemas = hashSchemas;
    }
  }
}
