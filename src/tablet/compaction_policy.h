// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_COMPACTION_POLICY_H
#define KUDU_TABLET_COMPACTION_POLICY_H

#include <gtest/gtest.h>
#include "gutil/macros.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

class RowSetTree;
class RowSetsInCompaction;

// A Compaction Policy is responsible for picking which files in a tablet
// should be compacted together.
class CompactionPolicy {
 public:
  CompactionPolicy() {}
  virtual ~CompactionPolicy() {}

  virtual Status PickRowSets(const RowSetTree &tree, RowSetsInCompaction *picked) = 0;

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
  virtual Status PickRowSets(const RowSetTree &tree, RowSetsInCompaction *picked);
};

// Compaction policy which, given a size budget for a compaction, and a workload,
// tries to pick a set of RowSets which fit into that budget and minimize the
// future cost of operations on the tablet.
//
// See src/tablet/compaction-policy.txt for details.
class BudgetedCompactionPolicy : public CompactionPolicy {
 public:
  // TODO: not yet implemented
  virtual Status PickRowSets(const RowSetTree &tree, RowSetsInCompaction *picked);

 private:
  FRIEND_TEST(TestBudgetedCompactionPolicy, TestStringFractionInRange);

  // Return the fraction indicating where "point" falls lexicographically between the
  // key range of [min, max].
  // For example, between "0000" and "9999", "5000" is right in the middle of the range,
  // hence this would return 0.5f. On the other hand, "1000" is 10% of the way through,
  // so would return 0.1f.
  static double StringFractionInRange(const Slice &min, const Slice &max, const Slice &point);
};

} // namespace tablet
} // namespace kudu
#endif
