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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Common;

/**
 * This class represents a range partition with custom hash bucketing schema.
 *
 * See also RangePartition.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RangePartitionWithCustomHashSchema extends RangePartition {
  // Using the corresponding PB type to represent this range with its custom
  // hash schema.
  private Common.PartitionSchemaPB.RangeWithHashSchemaPB.Builder pb =
      Common.PartitionSchemaPB.RangeWithHashSchemaPB.newBuilder();

  /**
   * @param lowerBound upper bound of the range partition
   * @param upperBound lower bound of the range partition
   * @param lowerBoundType lower bound type: inclusive/exclusive
   * @param upperBoundType upper bound type: inclusive/exclusive
   * @return new RangePartitionWithCustomHashSchema object
   */
  public RangePartitionWithCustomHashSchema(
      PartialRow lowerBound,
      PartialRow upperBound,
      RangePartitionBound lowerBoundType,
      RangePartitionBound upperBoundType) {
    super(lowerBound, upperBound, lowerBoundType, upperBoundType);
    pb.setRangeBounds(
        new Operation.OperationsEncoder().encodeLowerAndUpperBounds(
            lowerBound, upperBound, lowerBoundType, upperBoundType));
  }

  /**
   * Add a level of hash sub-partitioning for this range partition.
   *
   * The hash schema for the range partition is defined by the whole set of
   * its hash sub-partitioning levels. A range partition can have zero or
   * multiple levels of hash sub-partitioning: this method can be called
   * many times on the same object to define a multi-dimensional hash
   * bucketing structure for the range.
   *
   * @param columns name of table's columns to use for hash bucketing
   * @param numBuckets number of buckets used by the hash function
   * @param seed the seed for the hash function
   * @return this RangePartition object modified accordingly
   */
  public RangePartition addHashPartitions(
      List<String> columns, int numBuckets, int seed) {
    Common.PartitionSchemaPB.HashBucketSchemaPB.Builder b =
        pb.addHashSchemaBuilder();
    for (String column : columns) {
      b.addColumnsBuilder().setName(column);
    }
    b.setNumBuckets(numBuckets);
    b.setSeed(seed);
    return this;
  }

  public Common.PartitionSchemaPB.RangeWithHashSchemaPB toPB() {
    return pb.build();
  }
}
