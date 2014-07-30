// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTA_STATS_H
#define KUDU_TABLET_DELTA_STATS_H

#include <glog/logging.h>
#include <boost/function.hpp>

#include <stdint.h>
#include <vector>
#include <string>


#include "kudu/gutil/atomicops.h"
#include "kudu/common/schema.h"
#include "kudu/common/row_changelist.h"
#include "kudu/tablet/mvcc.h"

namespace kudu {

namespace tablet {

// A wrapper class for describing data statistics.
class DeltaStats {
 public:
  // Constructs a stats object with an initial count of columns. This
  // can be resized Resize() method below.
  explicit DeltaStats(size_t ncols);

  // Resizes the the vector containing the update counts to 'ncols',
  // setting the additional columns to 0.
  void Resize(size_t ncols);

  // Increment update count for column at 'col_idx' (relative to the
  // current schema) by 'update_count'.
  void IncrUpdateCount(size_t col_idx, int64_t update_count);

  // Increment the per-store delete count by 'delete_count'.
  void IncrDeleteCount(int64_t delete_count);

  // Increment delete and update counts based on changes contained in
  // 'update'.
  Status UpdateStats(const Timestamp& timestamp,
                     const Schema& schema,
                     const RowChangeList& update);

  // Return the number of deletes in the current delta store.
  int64_t delete_count() const { return delete_count_; }

  // Returns number of updates for column at 'col_idx' relative to the
  // current schema.
  int64_t update_count(size_t col_idx) const {
    CHECK_LT(col_idx, num_columns());
    return update_counts_[col_idx];
  }

  size_t num_columns() const {
    return update_counts_.size();
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

 private:
  std::vector<uint64_t> update_counts_;
  uint64_t delete_count_;
  Timestamp max_timestamp_;
  Timestamp min_timestamp_;
};


} // namespace tablet
} // namespace kudu

#endif
