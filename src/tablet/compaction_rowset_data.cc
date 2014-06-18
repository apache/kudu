// Copyright (c) 2014, Cloudera, inc.

#include "tablet/compaction_rowset_data.h"

#include <algorithm>
#include <tr1/memory>
#include <string>

#include <glog/logging.h>
#include <inttypes.h>

#include "gutil/endian.h"
#include "gutil/strings/util.h"
#include "tablet/rowset.h"
#include "tablet/rowset_tree.h"
#include "util/slice.h"

using std::tr1::shared_ptr;
using std::vector;

namespace kudu {
namespace tablet {
namespace compaction_policy {

// DataSizeCDF class -----------------------------------------------------------

const double DataSizeCDF::kEpsilon = 0.0001;

DataSizeCDF::DataSizeCDF(const RowSetTree* tree)
  : tree_(tree) {
  total_size_ = ComputeTotalSize(tree->all_rowsets());
}

double DataSizeCDF::Evaluate(const Slice& key) const {
  if (total_size_ == 0) {
    return 1;
  }

  double contained_size = 0;

  // Find all rowsets which intersect the interval [-inf, key]
  vector<RowSet *> intersecting;
  tree_->FindRowSetsIntersectingInterval(Slice(), key, &intersecting);

  // For each of those, fractionally include it based on what percentage
  // of that rowset is contained within the interval. This assumes that
  // each rowset is itself uniformly distributed, which is an approximation,
  // but without collecting further metadata this is as good as we can do
  // without actually doing IO.
  BOOST_FOREACH(const RowSet *rs, intersecting) {
    Slice rs_min, rs_max;
    if (!rs->GetBounds(&rs_min, &rs_max).ok()) {
      VLOG(2) << "Ignoring " << rs->ToString() << " in CDF calculation";
      continue;
    }

    double fraction_contained = StringFractionInRange(rs_min, rs_max, key);
    contained_size += fraction_contained * rs->EstimateOnDiskSize();
  }
  DCHECK_LE(contained_size, total_size_);
  DCHECK_GE(contained_size, 0);
  double ret = contained_size / total_size_;
  CHECK_GE(ret, 0);
  return ret;
}

double DataSizeCDF::ComputeTotalSize(const RowSetVector& rowsets) {
  double ret = 0;
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets) {
    ret += rs->EstimateOnDiskSize();
  }
  return ret;
}

double DataSizeCDF::StringFractionInRange(const Slice &min,
                                          const Slice &max,
                                          const Slice &point) {
  DCHECK_LE(min.compare(max), 0);
  if (point.compare(min) < 0) return 0;
  if (point.compare(max) >= 0) return 1;

  // Determine how much of a common prefix the strings share.
  int min_len = std::min(min.size(), max.size());
  int common_prefix = 0;
  while (common_prefix < min_len &&
         min[common_prefix] == max[common_prefix]) {
    common_prefix++;
  }

  DCHECK_EQ(memcmp(&min[0], &point[0], common_prefix), 0) << "point should share common prefix";

  // Convert the remaining portion of each string to an integer.
  uint64_t min_int = 0;
  memcpy(&min_int, &min[common_prefix],
         std::min(min.size() - common_prefix, sizeof(min_int)));
  min_int = BigEndian::ToHost64(min_int);

  uint64_t max_int = 0;
  memcpy(&max_int, &max[common_prefix],
         std::min(max.size() - common_prefix, sizeof(max_int)));
  max_int = BigEndian::ToHost64(max_int);

  uint64_t point_int = 0;
  memcpy(&point_int, &point[common_prefix],
         std::min(point.size() - common_prefix, sizeof(point_int)));
  point_int = BigEndian::ToHost64(point_int);

  // Compute how far between min and max the query point falls.
  return static_cast<double>(point_int - min_int) / (max_int - min_int);
}

// CompactionCandidate class ---------------------------------------------------

void CompactionCandidate::CollectCandidates(const RowSetTree& tree,
                                            vector<CompactionCandidate>* candidates) {
  DataSizeCDF cdf(&tree);

  // Create CompactionCandidate objects for each RowSet, and calculate min/max
  // bounds for the whole tablet.
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, tree.all_rowsets()) {
    if (rs->IsAvailableForCompaction()) {
      candidates->push_back(CompactionCandidate(cdf, rs));
    }
  }
}

CompactionCandidate::CompactionCandidate(const DataSizeCDF& cdf,
                                         const shared_ptr<RowSet>& rs)
  : rowset_(rs.get()) {
  size_mb_ = rs->EstimateOnDiskSize() / 1024 / 1024;

  Slice min, max;
  if (rs->GetBounds(&min, &max).ok()) {
    cdf_min_key_ = cdf.Evaluate(min);
    cdf_max_key_ = cdf.Evaluate(max);
    DCHECK_LE(cdf_min_key_, cdf_max_key_);
  } else {
    // The rowset doesn't know its own bounds. This counts as width
    // 1 because every access must check it.
    cdf_min_key_ = 0;
    cdf_max_key_ = 1;
  }
}

string CompactionCandidate::ToString() const {
  string ret;
  ret.append(rowset_->ToString());
  StringAppendF(&ret, "(% 3dM) [%.04f, %.04f]", size_mb_,
                cdf_min_key_, cdf_max_key_);
  Slice min, max;
  if (rowset_->GetBounds(&min, &max).ok()) {
    ret.append(" [").append(min.ToDebugString());
    ret.append(",").append(max.ToDebugString());
    ret.append("]");
  }
  return ret;
}

bool CompactionCandidate::Intersects(const CompactionCandidate &other) const {
  if (other.cdf_min_key() + DataSizeCDF::kEpsilon > cdf_max_key()) return false;
  if (other.cdf_max_key() - DataSizeCDF::kEpsilon < cdf_min_key()) return false;
  return true;
}

} // namespace compaction_policy
} // namespace tablet
} // namespace kudu
