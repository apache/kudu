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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.master.Master;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CreateTableOptions {

  private final List<PartialRow> splitRows = Lists.newArrayList();
  private final List<RangePartition> rangePartitions = Lists.newArrayList();
  private final List<RangePartitionWithCustomHashSchema> customRangePartitions =
      Lists.newArrayList(); // range partitions with custom hash schemas
  private Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();
  private boolean wait = true;
  private boolean isPbGenerationDone = false;

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
        pb.getPartitionSchemaBuilder().addHashSchemaBuilder();
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
   * Add a range partition to the table with an inclusive lower bound and an
   * exclusive upper bound.
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
   * Add range partition with custom hash schema.
   *
   * @param rangePartition range partition with custom hash schema
   * @return this CreateTableOptions object modified accordingly
   */
  public CreateTableOptions addRangePartition(RangePartitionWithCustomHashSchema rangePartition) {
    if (!splitRows.isEmpty()) {
      throw new IllegalArgumentException(
          "no range partitions with custom hash schema are allowed when using " +
              "split rows to define range partitioning for a table");
    }
    customRangePartitions.add(rangePartition);
    pb.getPartitionSchemaBuilder().addCustomHashSchemaRanges(rangePartition.toPB());
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
    if (!customRangePartitions.isEmpty()) {
      throw new IllegalArgumentException(
          "no split rows are allowed to define range partitioning for a table " +
              "when range partitions with custom hash schema are present");
    }
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
   * Sets the dimension label for all tablets created at table creation time.
   *
   * By default, the master will try to place newly created tablet replicas on tablet
   * servers with a small number of tablet replicas. If the dimension label is provided,
   * newly created replicas will be evenly distributed in the cluster based on the dimension
   * label. In other words, the master will try to place newly created tablet replicas on
   * tablet servers with a small number of tablet replicas belonging to this dimension label.
   *
   * @param dimensionLabel the dimension label for the tablet to be created.
   * @return this instance
   */
  public CreateTableOptions setDimensionLabel(String dimensionLabel) {
    Preconditions.checkArgument(dimensionLabel != null,
        "dimension label must not be null");
    pb.setDimensionLabel(dimensionLabel);
    return this;
  }

  /**
   * Sets the table's extra configuration properties.
   *
   * If the value of the kv pair is empty, the property will be ignored.
   *
   * @param extraConfig the table's extra configuration properties
   * @return this instance
   */
  public CreateTableOptions setExtraConfigs(Map<String, String> extraConfig) {
    pb.putAllExtraConfigs(extraConfig);
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

  /**
   * Set the table owner as the provided username.
   * Overrides the default of the currently logged-in username or Kerberos principal.
   *
   * This is an unstable method because it is not yet clear whether this should
   * be supported directly in the long run, rather than requiring the table creator
   * to re-assign ownership explicitly.
   *
   * @param owner the username to set as the table owner.
   * @return this instance
   */
  public CreateTableOptions setOwner(String owner) {
    pb.setOwner(owner);
    return this;
  }

  /**
   * Set the table comment.
   *
   * @param comment the table comment
   * @return this instance
   */
  public CreateTableOptions setComment(String comment) {
    pb.setComment(comment);
    return this;
  }

  Master.CreateTableRequestPB.Builder getBuilder() {
    if (isPbGenerationDone) {
      return pb;
    }

    if (!splitRows.isEmpty() && !customRangePartitions.isEmpty()) {
      throw new IllegalArgumentException(
          "no split rows are allowed to define range partitioning for a table " +
              "when range partitions with custom hash schema are present");
    }
    if (customRangePartitions.isEmpty()) {
      if (!splitRows.isEmpty() || !rangePartitions.isEmpty()) {
        pb.setSplitRowsRangeBounds(new Operation.OperationsEncoder()
            .encodeRangePartitions(rangePartitions, splitRows));
      }
    } else {
      // With the presence of a range with custom hash schema when the
      // table-wide hash schema is used for a particular range, add proper
      // element into PartitionSchemaPB::custom_hash_schema_ranges to satisfy
      // the convention used by the backend. Do so for all the ranges with
      // table-wide hash schemas.
      for (RangePartition p : rangePartitions) {
        org.apache.kudu.Common.PartitionSchemaPB.RangeWithHashSchemaPB.Builder b =
            pb.getPartitionSchemaBuilder().addCustomHashSchemaRangesBuilder();
        // Set the hash schema for the range.
        for (org.apache.kudu.Common.PartitionSchemaPB.HashBucketSchemaPB hashSchema :
            pb.getPartitionSchemaBuilder().getHashSchemaList()) {
          b.addHashSchema(hashSchema);
        }
        b.setRangeBounds(
            new Operation.OperationsEncoder().encodeLowerAndUpperBounds(
                p.lowerBound, p.upperBound, p.lowerBoundType, p.upperBoundType));
      }
    }
    isPbGenerationDone = true;
    return pb;
  }

  List<Integer> getRequiredFeatureFlags(Schema schema) {
    List<Integer> requiredFeatureFlags = new ArrayList<>();
    if (schema.hasAutoIncrementingColumn()) {
      requiredFeatureFlags.add(
              Integer.valueOf(Master.MasterFeatures.AUTO_INCREMENTING_COLUMN_VALUE));
    }
    if (schema.hasImmutableColumns()) {
      requiredFeatureFlags.add(
              Integer.valueOf(Master.MasterFeatures.IMMUTABLE_COLUMN_ATTRIBUTE_VALUE));
    }
    if (!rangePartitions.isEmpty() || !customRangePartitions.isEmpty()) {
      requiredFeatureFlags.add(Integer.valueOf(Master.MasterFeatures.RANGE_PARTITION_BOUNDS_VALUE));
    }
    if (!customRangePartitions.isEmpty()) {
      requiredFeatureFlags.add(
              Integer.valueOf(Master.MasterFeatures.RANGE_SPECIFIC_HASH_SCHEMA_VALUE));
    }

    return requiredFeatureFlags;
  }

  boolean shouldWait() {
    return wait;
  }
}
