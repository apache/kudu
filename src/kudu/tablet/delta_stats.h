// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#ifndef KUDU_TABLET_DELTA_STATS_H
#define KUDU_TABLET_DELTA_STATS_H

#include <glog/logging.h>
#include <boost/function.hpp>

#include <set>
#include <stdint.h>
#include <string>
#include <tr1/unordered_map>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/map-util.h"
#include "kudu/common/row_changelist.h"
#include "kudu/tablet/mvcc.h"

namespace kudu {

namespace tablet {

class DeltaStatsPB;

// A wrapper class for describing data statistics.
class DeltaStats {
 public:
  DeltaStats();

  // Increment update count for column 'col_id' by 'update_count'.
  void IncrUpdateCount(int col_id, int64_t update_count);

  // Increment the per-store delete count by 'delete_count'.
  void IncrDeleteCount(int64_t delete_count);

  // Increment delete and update counts based on changes contained in
  // 'update'.
  Status UpdateStats(const Timestamp& timestamp,
                     const RowChangeList& update);

  // Return the number of deletes in the current delta store.
  int64_t delete_count() const { return delete_count_; }

  // Returns number of updates for a given column.
  int64_t update_count_for_col_id(int col_id) const {
    return FindWithDefault(update_counts_by_col_id_, col_id, 0);
  }

  // Returns the maximum transaction id of any mutation in a delta file.
  Timestamp max_timestamp() const {
    return max_timestamp_;
  }

  // Returns the minimum transaction id of any mutation in a delta file.
  Timestamp min_timestamp() const {
    return min_timestamp_;
  }

  // Set the maximum transaction id of any mutation in a delta file.
  void set_max_timestamp(const Timestamp& timestamp) {
    max_timestamp_ = timestamp;
  }

  // Set the minimum transaction id in of any mutation in a delta file.
  void set_min_timestamp(const Timestamp& timestamp) {
    min_timestamp_ = timestamp;
  }

  std::string ToString() const;

  // Convert this object to the protobuf which is stored in the DeltaFile footer.
  void ToPB(DeltaStatsPB* pb) const;

  // Load this object from the protobuf which is stored in the DeltaFile footer.
  Status InitFromPB(const DeltaStatsPB& pb);

  // For each column which has at least one update, add that column's ID to the
  // set 'col_ids'.
  void AddColumnIdsWithUpdates(std::set<int>* col_ids) const;

 private:
  std::tr1::unordered_map<int, int64_t> update_counts_by_col_id_;
  uint64_t delete_count_;
  Timestamp max_timestamp_;
  Timestamp min_timestamp_;
};


} // namespace tablet
} // namespace kudu

#endif
