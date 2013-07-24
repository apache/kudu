// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_ROWSET_MANAGER_H
#define KUDU_TABLET_ROWSET_MANAGER_H

#include <tr1/memory>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "util/status.h"
#include "tablet/rowset.h"

namespace kudu {

template<class Traits>
class IntervalTree;

namespace tablet {

struct RowSetIntervalTraits;
struct RowSetWithBounds;

// Class which encapsulates the set of rowsets which are active for a given
// Tablet. This provides efficient lookup by key for RowSets which may overlap
// that key range.
class RowSetTree {
 public:
  RowSetTree();
  Status Reset(const RowSetVector &rowsets);
  ~RowSetTree();

  // Return all RowSets whose range may contain the given encoded key.
  //
  // The returned pointers are guaranteed to be valid at least until this
  // RowSetTree object is Reset().
  void FindRowSetsWithKeyInRange(const Slice &encoded_key,
                                 std::vector<RowSet *> *rowsets) const;

  void FindRowSetsIntersectingInterval(const Slice &lower_bound,
                                       const Slice &upper_bound,
                                       std::vector<RowSet *> *rowsets) const;

  const RowSetVector &all_rowsets() const { return all_rowsets_; }

 private:
  // Interval tree of the rowsets. Used to efficiently find rowsets which might contain
  // a probe row.
  gscoped_ptr<IntervalTree<RowSetIntervalTraits> > tree_;

  // Container for all of the entries in tree_. IntervalTree does
  // not itself manage memory, so this provides a simple way to enumerate
  // all the entry structs and free them in the destructor.
  std::vector<RowSetWithBounds *> entries_;

  // All of the rowsets which were put in this RowSetTree.
  RowSetVector all_rowsets_;

  // Rowsets for which the bounds are unknown -- e.g because they
  // are mutable (MemRowSets).
  //
  // These have to be consulted for every access, so are not
  // stored in the interval tree.
  RowSetVector unbounded_rowsets_;

  bool initted_;
};

} // namespace tablet
} // namespace kudu
#endif
