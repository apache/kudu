// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAMEMSTORE_H
#define KUDU_TABLET_DELTAMEMSTORE_H

#include <boost/noncopyable.hpp>
#include <gtest/gtest.h>

#include "common/columnblock.h"
#include "common/row_changelist.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "tablet/concurrent_btree.h"
#include "tablet/layer-interfaces.h"
#include "util/memory/arena.h"

namespace kudu {
namespace tablet {


class DeltaFileWriter;
class DMSIterator;

// In-memory storage for data which has been recently updated.
// This essentially tracks a 'diff' per row, which contains the
// modified columns.

class DeltaMemStore : public DeltaTrackerInterface, 
                      public std::tr1::enable_shared_from_this<DeltaMemStore>,
                      boost::noncopyable {
public:
  explicit DeltaMemStore(const Schema &schema);

  // Update the given row in the database.
  // Copies the data, as well as any referenced
  // values into this DMS's local arena.
  void Update(rowid_t row_idx, const RowChangeList &update);

  size_t Count() const {
    return tree_.count();
  }

  Status FlushToFile(DeltaFileWriter *dfw) const;

  // Create an iterator for applying deltas from this DMS.
  //
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  DeltaIteratorInterface *NewDeltaIterator(const Schema &projection);

  const Schema &schema() const {
    return schema_;
  }

private:
  friend class DMSIterator;


  static rowid_t DecodeKey(const Slice &key);

  const Schema schema_;

  typedef btree::CBTree<btree::BTreeTraits> DMSTree;
  typedef btree::CBTreeIterator<btree::BTreeTraits> DMSTreeIter;

  // Concurrent B-Tree storing <key index> -> RowChangeList
  DMSTree tree_;

  ThreadSafeArena arena_;
};


// Iterator over the deltas currently in the delta memstore.
// This iterator is a wrapper around the underlying tree iterator
// which snapshots sets of deltas on a per-block basis, and allows
// the caller to then apply the deltas column-by-column. This supports
// column-by-column predicate evaluation, and lazily loading columns
// only after predicates have passed.
//
// See DeltaTrackerInterface for more details on usage and the implemented
// functions.
class DMSIterator : boost::noncopyable, public DeltaIteratorInterface {
public:
  Status Init();

  Status SeekToOrdinal(rowid_t row_idx);

  Status PrepareBatch(size_t nrows);

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst);

private:
  FRIEND_TEST(TestDMSIterator, TestIteratorDoesUpdates);
  friend class DeltaMemStore;

  // Initialize the iterator.
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  DMSIterator(const shared_ptr<DeltaMemStore> &dms, const Schema &projection);


  enum {
    kPreparedBufInitialCapacity = 512
  };

  const shared_ptr<DeltaMemStore> dms_;
  const Schema projection_;

  gscoped_ptr<DeltaMemStore::DMSTreeIter> iter_;


  // The index at which the last Prepare call was made
  rowid_t prepared_idx_;

  // The number of rows for which the last Prepare call was made
  uint32_t prepared_count_;

  // The last block that PrepareToApply(...) was called on.
  bool prepared_;

  faststring prepared_buf_;

  // Projection from the schema of the deltamemstore to the projection
  // of the row blocks which will be passed to PrepareToApply, etc.
  vector<size_t> projection_indexes_;

};



} // namespace tablet
} // namespace kudu

#endif
