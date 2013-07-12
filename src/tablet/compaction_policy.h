// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_COMPACTION_POLICY_H
#define KUDU_TABLET_COMPACTION_POLICY_H

#include <gtest/gtest.h>
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

} // namespace tablet
} // namespace kudu
#endif
