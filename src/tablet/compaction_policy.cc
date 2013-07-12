// Copyright (c) 2013, Cloudera, inc.

#include "tablet/compaction_policy.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <tr1/memory>

#include <algorithm>
#include <vector>

#include "gutil/endian.h"
#include "gutil/map-util.h"
#include "tablet/compaction.h"
#include "tablet/rowset.h"
#include "tablet/rowset_tree.h"
#include "util/slice.h"
#include "util/status.h"

using std::tr1::shared_ptr;

namespace kudu { namespace tablet {

// Return true if the given RowSet's compact_flush_lock is currently
// unlocked.
static bool IsAvailableForCompaction(RowSet *rs) {
  // Try to obtain the lock. If we don't succeed, it means the rowset
  // was already locked for compaction by some other compactor thread,
  // or it is a RowSet type which can't be used as a compaction input.
  //
  // We can be sure that our check here will remain true until after
  // the compaction selection has finished because only one thread
  // makes compaction selection at a time on a given Tablet due to
  // Tablet::compact_select_lock_.
  boost::mutex::scoped_try_lock try_lock(*rs->compact_flush_lock());
  return try_lock.owns_lock();
}

static bool CompareBySize(const shared_ptr<RowSet> &a,
                          const shared_ptr<RowSet> &b) {
  return a->EstimateOnDiskSize() < b->EstimateOnDiskSize();
}

Status SizeRatioCompactionPolicy::PickRowSets(const RowSetTree &tree,
                                              std::tr1::unordered_set<RowSet*> *picked) {
  vector<shared_ptr<RowSet> > tmp_rowsets;
  tmp_rowsets.assign(tree.all_rowsets().begin(),
                     tree.all_rowsets().end());

  // Sort the rowsets by their on-disk size
  std::sort(tmp_rowsets.begin(), tmp_rowsets.end(), CompareBySize);
  uint64_t accumulated_size = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, tmp_rowsets) {
    if (!IsAvailableForCompaction(rs.get())) continue;

    uint64_t this_size = rs->EstimateOnDiskSize();
    if (picked->size() < 2 || this_size < accumulated_size * 2) {
      InsertOrDie(picked, rs.get());
      accumulated_size += this_size;
    } else {
      break;
    }
  }

  return Status::OK();
}

////////////////////////////////////////////////////////////
// BudgetedCompactionPolicy
////////////////////////////////////////////////////////////

double BudgetedCompactionPolicy::StringFractionInRange(const Slice &min,
                                                       const Slice &max,
                                                       const Slice &point) {
  DCHECK_GE(point.compare(min), 0) << "point " << point.ToDebugString() << " < " << min.ToDebugString();
  DCHECK_LE(point.compare(max), 0) << "point " << point.ToDebugString() << " > " << max.ToDebugString();
  DCHECK_LT(min.compare(max), 0);

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

} // namespace tablet
} // namespace kudu
