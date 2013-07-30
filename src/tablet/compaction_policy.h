// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_COMPACTION_POLICY_H
#define KUDU_TABLET_COMPACTION_POLICY_H

#include "gutil/macros.h"
#include "util/slice.h"
#include "util/status.h"

#include <tr1/unordered_set>

namespace kudu {
namespace tablet {

class RowSet;
class RowSetTree;

// Forward declarations for internals (defined in compaction_policy-internal.h)
namespace compaction_policy {
class CompactionCandidate;
class DataSizeCDF;
}

// A Compaction Policy is responsible for picking which files in a tablet
// should be compacted together.
class CompactionPolicy {
 public:
  CompactionPolicy() {}
  virtual ~CompactionPolicy() {}

  // Select a set of RowSets to compact out of 'tree'.
  //
  // Callers are responsible for externally synchronizing selection within a
  // given Tablet. This will only select rowsets whose compact_flush_lock
  // is unlocked, but will not itself take the lock. Hence no other threads
  // should lock or unlock the rowsets' compact_flush_lock while this method
  // is running.
  virtual Status PickRowSets(const RowSetTree &tree, std::tr1::unordered_set<RowSet*>* picked) = 0;

  // Return the size at which flush/compact should "roll" to new files. Some
  // compaction policies may prefer to deal with small constant-size files
  // whereas others may prefer large ones.
  virtual uint64_t target_rowset_size() const {
    return 1024 * 1024 * 1024; // no rolling
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CompactionPolicy);
};

// The size-ratio based compaction policy sorts the RowSets by their on-disk
// size and then proceeds through them, adding more to the compaction so long
// as the additional RowSet doesn't increase the total compaction size by
// more than a given ratio.
//
// This policy is more-or-less based on HBase.
class SizeRatioCompactionPolicy : public CompactionPolicy {
 public:
  virtual Status PickRowSets(const RowSetTree &tree, std::tr1::unordered_set<RowSet*>* picked);
};

// Compaction policy which, given a size budget for a compaction, and a workload,
// tries to pick a set of RowSets which fit into that budget and minimize the
// future cost of operations on the tablet.
//
// See src/tablet/compaction-policy.txt for details.
class BudgetedCompactionPolicy : public CompactionPolicy {
 public:
  // TODO: not yet implemented
  virtual Status PickRowSets(const RowSetTree &tree, std::tr1::unordered_set<RowSet*>* picked);
};

} // namespace tablet
} // namespace kudu
#endif
