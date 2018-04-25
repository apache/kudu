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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Common;
import org.apache.kudu.master.Master;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CreateTableOptions {

  private final List<PartialRow> splitRows = Lists.newArrayList();
  private final List<RangePartition> rangePartitions = Lists.newArrayList();
  private Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();
  private boolean wait = true;

  /**
   * Add a set of hash partitions to the table.
   *
   * Each column must be a part of the table's primary key, and an individual
   * column may only appear in a single hash component.
   *
   * For each set of hash partitions added to the table, the total number of
   * table partitions is multiplied by the number of buckets. For example, if a
   * table is created with 3 split rows, and two hash partitions with 4 and 5
   * buckets respectively, the total number of table partitions will be 80
   * (4 range partitions * 4 hash buckets * 5 hash buckets).
   *
   * @param columns the columns to hash
   * @param buckets the number of buckets to hash into
   * @return this instance
   */
  public CreateTableOptions addHashPartitions(List<String> columns, int buckets) {
    addHashPartitions(columns, buckets, 0);
    return this;
  }

  /**
   * Add a set of hash partitions to the table.
   *
   * This constructor takes a seed value, which can be used to randomize the
   * mapping of rows to hash buckets. Setting the seed may provide some
   * amount of protection against denial of service attacks when the hashed
   * columns contain user provided values.
   *
   * @param columns the columns to hash
   * @param buckets the number of buckets to hash into
   * @param seed a hash seed
   * @return this instance
   */
  public CreateTableOptions addHashPartitions(List<String> columns, int buckets, int seed) {
    Common.PartitionSchemaPB.HashBucketSchemaPB.Builder hashBucket =
        pb.getPartitionSchemaBuilder().addHashBucketSchemasBuilder();
    for (String column : columns) {
      hashBucket.addColumnsBuilder().setName(column);
    }
    hashBucket.setNumBuckets(buckets);
    hashBucket.setSeed(seed);
    return this;
  }

  /**
   * Set the columns on which the table will be range-partitioned.
   *
   * Every column must be a part of the table's primary key. If not set,
   * the table is range partitioned by the primary key columns with a single
   * unbounded partition. If called with an empty vector, the table will be
   * created without range partitioning.
   *
   * Tables must be created with either range, hash, or range and hash
   * partitioning. To force the use of a single tablet (not recommended),
   * call this method with an empty list and set no split rows and no hash
   * partitions.
   *
   * @param columns the range partitioned columns
   * @return this instance
   */
  public CreateTableOptions setRangePartitionColumns(List<String> columns) {
    Common.PartitionSchemaPB.RangeSchemaPB.Builder rangePartition =
        pb.getPartitionSchemaBuilder().getRangeSchemaBuilder();
    for (String column : columns) {
      rangePartition.addColumnsBuilder().setName(column);
    }
    return this;
  }

  /**
   * Add a range partition partition to the table with an inclusive lower bound
   * and an exclusive upper bound.
   *
   * If either row is empty, then that end of the range will be unbounded. If a
   * range column is missing a value, the logical minimum value for that column
   * type will be used as the default.
   *
   * Multiple range bounds may be added, but they must not overlap. All split
   * rows must fall in one of the range bounds. The lower bound must be less
   * than the upper bound.
   *
   * If not provided, the table's range will be unbounded.
   *
   * @param lower the inclusive lower bound
   * @param upper the exclusive upper bound
   * @return this instance
   */
  public CreateTableOptions addRangePartition(PartialRow lower,
                                              PartialRow upper) {
    return addRangePartition(lower, upper,
                             RangePartitionBound.INCLUSIVE_BOUND,
                             RangePartitionBound.EXCLUSIVE_BOUND);
  }

  /**
   * Add a range partition partition to the table with a lower bound and upper
   * bound.
   *
   * If either row is empty, then that end of the range will be unbounded. If a
   * range column is missing a value, the logical minimum value for that column
   * type will be used as the default.
   *
   * Multiple range bounds may be added, but they must not overlap. All split
   * rows must fall in one of the range bounds. The lower bound must be less
   * than the upper bound.
   *
   * If not provided, the table's range will be unbounded.
   *
   * @param lower the lower bound
   * @param upper the upper bound
   * @param lowerBoundType the type of the lower bound, either inclusive or exclusive
   * @param upperBoundType the type of the upper bound, either inclusive or exclusive
   * @return this instance
   */
  public CreateTableOptions addRangePartition(PartialRow lower,
                                              PartialRow upper,
                                              RangePartitionBound lowerBoundType,
                                              RangePartitionBound upperBoundType) {
    rangePartitions.add(new RangePartition(lower, upper, lowerBoundType, upperBoundType));
    return this;
  }

  /**
   * Add a range partition split. The split row must fall in a range partition,
   * and causes the range partition to split into two contiguous range partitions.
   * The row may be reused or modified safely after this call without changing
   * the split point.
   *
   * @param row a key row for the split point
   * @return this instance
   */
  public CreateTableOptions addSplitRow(PartialRow row) {
    splitRows.add(new PartialRow(row));
    return this;
  }

  /**
   * Sets the number of replicas that each tablet will have. If not specified, it uses the
   * server-side default which is usually 3 unless changed by an administrator.
   *
   * @param numReplicas the number of replicas to use
   * @return this instance
   */
  public CreateTableOptions setNumReplicas(int numReplicas) {
    pb.setNumReplicas(numReplicas);
    return this;
  }

  /**
   * Whether to wait for the table to be fully created before this create
   * operation is considered to be finished.
   * <p>
   * If false, the create will finish quickly, but subsequent row operations
   * may take longer as they may need to wait for portions of the table to be
   * fully created.
   * <p>
   * If true, the create will take longer, but the speed of subsequent row
   * operations will not be impacted.
   * <p>
   * If not provided, defaults to true.
   * <p>
   * @param wait whether to wait for the table to be fully created
   * @return this instance
   */
  public CreateTableOptions setWait(boolean wait) {
    this.wait = wait;
    return this;
  }

  Master.CreateTableRequestPB.Builder getBuilder() {
    if (!splitRows.isEmpty() || !rangePartitions.isEmpty()) {
      pb.setSplitRowsRangeBounds(new Operation.OperationsEncoder()
                                              .encodeRangePartitions(rangePartitions, splitRows));
    }
    return pb;
  }

  List<Integer> getRequiredFeatureFlags() {
    if (rangePartitions.isEmpty()) {
      return ImmutableList.of();
    } else {
      return ImmutableList.of(Master.MasterFeatures.RANGE_PARTITION_BOUNDS_VALUE);
    }
  }

  boolean shouldWait() {
    return wait;
  }

  static final class RangePartition {
    private final PartialRow lowerBound;
    private final PartialRow upperBound;
    private final RangePartitionBound lowerBoundType;
    private final RangePartitionBound upperBoundType;

    public RangePartition(PartialRow lowerBound,
                          PartialRow upperBound,
                          RangePartitionBound lowerBoundType,
                          RangePartitionBound upperBoundType) {
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.lowerBoundType = lowerBoundType;
      this.upperBoundType = upperBoundType;
    }

    public PartialRow getLowerBound() {
      return lowerBound;
    }

    public PartialRow getUpperBound() {
      return upperBound;
    }

    public RangePartitionBound getLowerBoundType() {
      return lowerBoundType;
    }

    public RangePartitionBound getUpperBoundType() {
      return upperBoundType;
    }
  }
}
