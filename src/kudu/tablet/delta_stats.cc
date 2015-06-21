// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#include "kudu/tablet/delta_stats.h"

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"

namespace kudu {

using std::vector;

namespace tablet {

DeltaStats::DeltaStats(size_t ncols)
    : delete_count_(0),
      max_timestamp_(Timestamp::kMin),
      min_timestamp_(Timestamp::kMax) {
  Resize(ncols);
}

void DeltaStats::Resize(size_t ncols) {
  update_counts_.resize(ncols, 0);
}

void DeltaStats::IncrUpdateCount(size_t col_idx, int64_t update_count) {
  DCHECK_LT(col_idx, update_counts_.size());
  update_counts_[col_idx] += update_count;
}

void DeltaStats::IncrDeleteCount(int64_t delete_count) {
  delete_count_ += delete_count;
}

Status DeltaStats::UpdateStats(const Timestamp& timestamp,
                               const Schema& schema,
                               const RowChangeList& update) {
  DCHECK_LE(schema.num_columns(), update_counts_.size());

  // Decode the update, incrementing the update count for each of the
  // columns we find present.
  RowChangeListDecoder update_decoder(update);
  RETURN_NOT_OK(update_decoder.Init());
  if (PREDICT_FALSE(update_decoder.is_delete())) {
    IncrDeleteCount(1);
  } else if (PREDICT_TRUE(update_decoder.is_update())) {
    vector<int> col_ids;
    RETURN_NOT_OK(update_decoder.GetIncludedColumnIds(&col_ids));
    BOOST_FOREACH(int col_id, col_ids) {
      int col_idx = schema.find_column_by_id(col_id);
      if (col_idx != -1) {
        // TODO: should we keep stats for columns not in the schema?
        // probably should use a hashmap instead of a vector
        IncrUpdateCount(col_idx, 1);
      }
    }
  } // Don't handle re-inserts

  if (min_timestamp_.CompareTo(timestamp) > 0) {
    min_timestamp_ = timestamp;
  }
  if (max_timestamp_.CompareTo(timestamp) < 0) {
    max_timestamp_ = timestamp;
  }

  return Status::OK();
}

string DeltaStats::ToString() const {
  return strings::Substitute("ts range=[$0, $1]",
                             min_timestamp_.ToString(),
                             max_timestamp_.ToString());
}

} // namespace tablet
} // namespace kudu
