// Copyright (c) 2013, Cloudera, inc.

#include "tablet/compaction_policy.h"

#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <tr1/memory>

#include <algorithm>
#include <vector>

#include "gutil/endian.h"
#include "tablet/compaction.h"
#include "tablet/rowset.h"
#include "tablet/rowset_tree.h"
#include "util/slice.h"
#include "util/status.h"

using std::tr1::shared_ptr;

namespace kudu { namespace tablet {

static bool CompareBySize(const shared_ptr<RowSet> &a,
                          const shared_ptr<RowSet> &b) {
  return a->EstimateOnDiskSize() < b->EstimateOnDiskSize();
}

Status SizeRatioCompactionPolicy::PickRowSets(const RowSetTree &tree,
                                              RowSetsInCompaction *picked) {
  vector<shared_ptr<RowSet> > tmp_rowsets;
  tmp_rowsets.assign(tree.all_rowsets().begin(),
                     tree.all_rowsets().end());

  // Sort the rowsets by their on-disk size
  std::sort(tmp_rowsets.begin(), tmp_rowsets.end(), CompareBySize);
  uint64_t accumulated_size = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, tmp_rowsets) {
    uint64_t this_size = rs->EstimateOnDiskSize();
    if (picked->num_rowsets() < 2 || this_size < accumulated_size * 2) {
      // Grab the compact_flush_lock: this prevents any other concurrent
      // compaction from selecting this same rowset, and also ensures that
      // we don't select a rowset which is currently in the middle of being
      // flushed.
      shared_ptr<boost::mutex::scoped_try_lock> lock(
        new boost::mutex::scoped_try_lock(*rs->compact_flush_lock()));
      if (!lock->owns_lock()) {
        LOG(INFO) << "Unable to select " << rs->ToString() << " for compaction: it is busy";
        continue;
      }

      // Push the lock on our scoped list, so we unlock when done.
      picked->AddRowSet(rs, lock);
      accumulated_size += this_size;
    } else {
      break;
    }
  }

  return Status::OK();
}

} // namespace tablet
} // namespace kudu
