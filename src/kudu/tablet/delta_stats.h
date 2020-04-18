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
#ifndef KUDU_TABLET_DELTA_STATS_H
#define KUDU_TABLET_DELTA_STATS_H

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>

#include "kudu/common/schema.h" // IWYU pragma: keep
#include "kudu/common/timestamp.h"
#include "kudu/gutil/map-util.h"
#include "kudu/util/status.h"

namespace kudu {

class RowChangeList;

namespace tablet {

class DeltaStatsPB;

// A wrapper class for describing data statistics.
class DeltaStats {
 public:
  DeltaStats();

  // Increment update count for column 'col_id' by 'update_count'.
  void IncrUpdateCount(ColumnId col_id, int64_t update_count);

  // Increment the per-store delete count by 'delete_count'.
  void IncrDeleteCount(int64_t delete_count);

  // Increment the per-store reinsert count by 'reinsert_count'.
  void IncrReinsertCount(int64_t reinsert_count);

  // Increment delete and update counts based on changes contained in
  // 'update'.
  Status UpdateStats(const Timestamp& timestamp,
                     const RowChangeList& update);

  // Return the number of deletes in the current delta store.
  int64_t delete_count() const { return delete_count_; }

  // Return the number of reinserts in the current delta store.
  int64_t reinsert_count() const { return reinsert_count_; }

  // Returns number of updates for a given column.
  int64_t update_count_for_col_id(const ColumnId& col_id) const {
    return FindWithDefault(update_counts_by_col_id_, col_id, 0);
  }

  // Returns the maximum op timestamp of any mutation in a delta file.
  Timestamp max_timestamp() const {
    return max_timestamp_;
  }

  // Returns the minimum op timestamp of any mutation in a delta file.
  Timestamp min_timestamp() const {
    return min_timestamp_;
  }

  // Set the maximum op timestamp of any mutation in a delta file.
  void set_max_timestamp(const Timestamp& timestamp) {
    max_timestamp_ = timestamp;
  }

  // Set the minimum op timestamp in of any mutation in a delta file.
  void set_min_timestamp(const Timestamp& timestamp) {
    min_timestamp_ = timestamp;
  }

  // Returns the number of updates across all columns.
  int64_t UpdateCount() const;

  std::string ToString() const;

  // Convert this object to the protobuf which is stored in the DeltaFile footer.
  void ToPB(DeltaStatsPB* pb) const;

  // Load this object from the protobuf which is stored in the DeltaFile footer.
  Status InitFromPB(const DeltaStatsPB& pb);

  // For each column which has at least one update, add that column's ID to the
  // set 'col_ids'.
  void AddColumnIdsWithUpdates(std::set<ColumnId>* col_ids) const;

 private:
  std::unordered_map<ColumnId, int64_t> update_counts_by_col_id_;
  uint64_t delete_count_;
  uint64_t reinsert_count_;
  Timestamp max_timestamp_;
  Timestamp min_timestamp_;
};


} // namespace tablet
} // namespace kudu

#endif
