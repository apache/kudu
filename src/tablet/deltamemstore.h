// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAMEMSTORE_H
#define KUDU_TABLET_DELTAMEMSTORE_H

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "common/columnblock.h"
#include "common/row_changelist.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "tablet/concurrent_btree.h"
#include "tablet/delta_key.h"
#include "tablet/delta_tracker.h"
#include "tablet/mvcc.h"
#include "util/memory/arena.h"

namespace kudu {
namespace tablet {

class DeltaFileWriter;
class DMSIterator;
class Mutation;

// In-memory storage for data which has been recently updated.
// This essentially tracks a 'diff' per row, which contains the
// modified columns.

class DeltaMemStore : public DeltaStore,
                      public std::tr1::enable_shared_from_this<DeltaMemStore> {
 public:
  explicit DeltaMemStore(int64_t id, const Schema &schema);

  // Update the given row in the database.
  // Copies the data, as well as any referenced values into this DMS's local
  // arena.
  Status Update(txid_t txid, rowid_t row_idx,
                const RowChangeList &update);

  size_t Count() const {
    return tree_.count();
  }

  // Dump a debug version of the tree to the logs. This is not thread-safe, so
  // is only really useful in unit tests.
  void DebugPrint() const;

  Status FlushToFile(DeltaFileWriter *dfw) const;

  // Create an iterator for applying deltas from this DMS.
  //
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  //
  // 'snapshot' determines which updates will be applied -- updates which come
  // from transactions which were not yet committed at the time the snapshot was
  // created will be ignored.
  DeltaIterator *NewDeltaIterator(const Schema &projection,
                                  const MvccSnapshot &snapshot) const;

  virtual Status CheckRowDeleted(rowid_t row_idx, bool *deleted) const;

  Status AlterSchema(const Schema& schema);

  const Schema &schema() const {
    return schema_;
  }

  const int64_t id() const { return id_; }

  typedef btree::CBTree<btree::BTreeTraits> DMSTree;
  typedef btree::CBTreeIterator<btree::BTreeTraits> DMSTreeIter;

  size_t memory_footprint() const {
    return arena_.memory_footprint() + tree_.estimate_memory_usage();
  }

 private:
  friend class DMSIterator;
  friend class DeltaCompactionInput;

  DISALLOW_COPY_AND_ASSIGN(DeltaMemStore);

  const DMSTree &tree() const {
    return tree_;
  }

  const int64_t id_;
  Schema schema_;

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
// See DeltaStore for more details on usage and the implemented
// functions.
class DMSIterator : public DeltaIterator {
 public:
  Status Init();

  Status SeekToOrdinal(rowid_t row_idx);

  Status PrepareBatch(size_t nrows);

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst);

  Status ApplyDeletes(SelectionVector *sel_vec);

  Status CollectMutations(vector<Mutation *> *dst, Arena *arena);

  string ToString() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(DMSIterator);
  FRIEND_TEST(TestDeltaMemStore, TestIteratorDoesUpdates);
  FRIEND_TEST(TestDeltaMemStore, TestCollectMutations);
  friend class DeltaMemStore;

  // Initialize the iterator.
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  DMSIterator(const shared_ptr<const DeltaMemStore> &dms, const Schema &projection,
              const MvccSnapshot &snapshot);


  // Decode a mutation as stored in the DMS.
  Status DecodeMutation(Slice *src, DeltaKey *key, RowChangeList *changelist) const;

  // Format a Corruption status
  Status CorruptionStatus(const string &message, rowid_t row,
                          const RowChangeList *changelist) const;

  enum {
    kPreparedBufInitialCapacity = 512
  };

  const shared_ptr<const DeltaMemStore> dms_;

  // MVCC state which allows us to ignore uncommitted transactions.
  const MvccSnapshot mvcc_snapshot_;

  gscoped_ptr<DeltaMemStore::DMSTreeIter> iter_;

  bool initted_;

  // The index at which the last Prepare call was made
  rowid_t prepared_idx_;

  // The number of rows for which the last Prepare call was made
  uint32_t prepared_count_;

  // The last block that PrepareToApply(...) was called on.
  bool prepared_;

  // True if SeekToOrdinal() been called at least once.
  bool seeked_;

  faststring prepared_buf_;

  // Projection from the schema of the deltamemstore to the projection
  // of the row blocks which will be passed to PrepareToApply, etc.
  DeltaProjector projector_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;
};



} // namespace tablet
} // namespace kudu

#endif
