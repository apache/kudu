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

#pragma once

#include <string>
#include <tuple>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/common/partition.h"

namespace kudu {

class Partition;
class PartitionSchema;
class ScanSpec;
class Schema;

// Provides partition key ranges to a scanner in order to prune tablets which
// are not necessary for the scan. The scanner retrieves the partition key of
// the next tablet to scan through the NextPartitionKey method, and notifies the
// partition pruner that a tablet has been scanned by calling
// RemovePartitionKeyRange with the tablet's upper bound partition key.
//
// Partition keys are in the same encoded format as used by the Partition class.
class PartitionPruner {
 public:

  PartitionPruner() = default;

  // Initializes the partition pruner for a new scan. The scan spec should
  // already be optimized by the ScanSpec::Optimize method.
  void Init(const Schema& schema,
            const PartitionSchema& partition_schema,
            const ScanSpec& scan_spec);

  // Returns whether there are more partition key ranges to scan.
  bool HasMorePartitionKeyRanges() const;

  // Returns the inclusive lower bound partition key of the next tablet to scan.
  const std::string& NextPartitionKey() const;

  // Removes all partition key ranges through the provided exclusive upper bound.
  void RemovePartitionKeyRange(const std::string& upper_bound);

  // Returns true if the provided partition should be pruned.
  bool ShouldPrune(const Partition& partition) const;

  // Returns a text description of this partition pruner suitable for debug
  // printing.
  std::string ToString(const Schema& schema, const PartitionSchema& partition_schema) const;

 private:
  // Search all combination of in-list and equality predicates.
  // Return hash values bitset of these combination.
  std::vector<bool> PruneHashComponent(
      const PartitionSchema& partition_schema,
      const PartitionSchema::HashBucketSchema& hash_bucket_schema,
      const Schema& schema,
      const ScanSpec& scan_spec);

  // The reverse sorted set of partition key ranges. Each range has an inclusive
  // lower and exclusive upper bound.
  std::vector<std::tuple<std::string, std::string>> partition_key_ranges_;

  DISALLOW_COPY_AND_ASSIGN(PartitionPruner);
};

} // namespace kudu
