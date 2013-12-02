// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTA_STATS_H
#define KUDU_TABLET_DELTA_STATS_H

#include <glog/logging.h>
#include <boost/function.hpp>

#include <stdint.h>
#include <vector>
#include <string>


#include "gutil/atomicops.h"
#include "common/schema.h"
#include "common/row_changelist.h"

namespace kudu {

namespace tablet {

// A wrapper class for describing data statistics. Methods that mutate
// the objects' internal state are templatized to include both the
// atomic and non-atomic versions in one.
class DeltaStats {
 public:
  // Constructs a stats object with an initial count of columns. This
  // can be resized Resize() method below.
  explicit DeltaStats(size_t ncols);

  // Resizes the the vector containing the update counts to 'ncols',
  // setting the additional columns to 0. This method is not thread
  // safe and should be called with an external write lock, e.g., as
  // in DeltaMemStore::AlterSchema().
  void Resize(size_t ncols);

  // Increment update count for column at 'col_idx' (relative to the
  // current schema) by 'update_count'. This method is threadsafe if
  // the template ATOMIC parameter is set to true.
  template <bool ATOMIC>
  void IncrUpdateCount(size_t col_idx, int64_t update_count);

  // Increment the per-store delete count by 'delete_count'. This
  // method is threadsafe if the template ATOMIC parameter is set to
  // true.
  template <bool ATOMIC>
  void IncrDeleteCount(int64_t delete_count);

  // Increment delete and update counts based on changes contained in
  // 'update'.
  template <bool ATOMIC>
  Status UpdateStats(const Schema& schema, const RowChangeList& update);

  // Return the number of deletes in the current delta store. This
  // method is not guaranteed to be accurate in a multi-threaded
  // context as it does not use a memory barrier.
  int64_t delete_count() const { return delete_count_; }

  // Returns number of updates for column at 'col_idx' relative to the
  // current schema. This method is not guaranteed to be accurate in a
  // multi-threaded context as it does not use a memory barrier.
  int64_t update_count(size_t col_idx) const {
    CHECK_LT(col_idx, num_columns());
    return update_counts_[col_idx];
  }

  size_t num_columns() const {
    return update_counts_.size();
  }

 private:
  std::vector<base::subtle::Atomic64> update_counts_;
  base::subtle::Atomic64 delete_count_;
};


} // namespace tablet
} // namespace kudu

#endif
