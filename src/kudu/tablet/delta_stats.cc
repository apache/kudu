// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#include "kudu/tablet/delta_stats.h"

#include <utility>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/bitmap.h"

namespace kudu {

using std::vector;

namespace tablet {

DeltaStats::DeltaStats()
    : delete_count_(0),
      max_timestamp_(Timestamp::kMin),
      min_timestamp_(Timestamp::kMax) {
}

void DeltaStats::IncrUpdateCount(int col_id, int64_t update_count) {
  DCHECK_GE(col_id, 0);
  update_counts_by_col_id_[col_id] += update_count;
}

void DeltaStats::IncrDeleteCount(int64_t delete_count) {
  delete_count_ += delete_count;
}

Status DeltaStats::UpdateStats(const Timestamp& timestamp,
                               const RowChangeList& update) {
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
      IncrUpdateCount(col_id, 1);
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
  string ret = strings::Substitute(
      "ts range=[$0, $1]",
      min_timestamp_.ToString(),
      max_timestamp_.ToString());
  ret.append(", update_counts_by_col_id=[");
  ret.append(JoinKeysAndValuesIterator(update_counts_by_col_id_.begin(),
                                       update_counts_by_col_id_.end(),
                                       ":", ","));
  ret.append(")");
  return ret;
}


void DeltaStats::ToPB(DeltaStatsPB* pb) const {
  pb->Clear();
  pb->set_delete_count(delete_count_);
  typedef std::pair<int, int64_t> entry;
  BOOST_FOREACH(const entry& e, update_counts_by_col_id_) {
    DeltaStatsPB::ColumnStats* stats = pb->add_column_stats();
    stats->set_col_id(e.first);
    stats->set_update_count(e.second);
  }

  pb->set_max_timestamp(max_timestamp_.ToUint64());
  pb->set_min_timestamp(min_timestamp_.ToUint64());
}

Status DeltaStats::InitFromPB(const DeltaStatsPB& pb) {
  delete_count_ = pb.delete_count();
  update_counts_by_col_id_.clear();
  BOOST_FOREACH(const DeltaStatsPB::ColumnStats stats, pb.column_stats()) {
    IncrUpdateCount(stats.col_id(), stats.update_count());
  }
  RETURN_NOT_OK(max_timestamp_.FromUint64(pb.max_timestamp()));
  RETURN_NOT_OK(min_timestamp_.FromUint64(pb.min_timestamp()));
  return Status::OK();
}

void DeltaStats::AddColumnIdsWithUpdates(std::set<int>* col_ids) const {
  typedef std::pair<int, int64_t> entry;
  BOOST_FOREACH(const entry& e, update_counts_by_col_id_) {
    if (e.second > 0) {
      col_ids->insert(e.first);
    }
  }
}


} // namespace tablet
} // namespace kudu
