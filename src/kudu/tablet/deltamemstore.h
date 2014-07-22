// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAMEMSTORE_H
#define KUDU_TABLET_DELTAMEMSTORE_H

#include <gtest/gtest_prod.h>
#include <string>
#include <vector>
#include <boost/thread/mutex.hpp>

#include "kudu/common/columnblock.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/tablet/concurrent_btree.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/memory/arena.h"

namespace kudu {

class MemTracker;

namespace tablet {

class DeltaFileWriter;
class DMSIterator;
class Mutation;

struct DMSTreeTraits : public btree::BTreeTraits {
  typedef ThreadSafeMemoryTrackingArena ArenaType;
};

// In-memory storage for data which has been recently updated.
// This essentially tracks a 'diff' per row, which contains the
// modified columns.

class DeltaMemStore : public DeltaStore,
                      public std::tr1::enable_shared_from_this<DeltaMemStore> {
 public:
  DeltaMemStore(int64_t id, const Schema &schema, log::OpIdAnchorRegistry* opid_anchor_registry,
                MemTracker* parent_tracker = NULL);

  // Update the given row in the database.
  // Copies the data, as well as any referenced values into this DMS's local
  // arena.
  Status Update(Timestamp timestamp, rowid_t row_idx,
                const RowChangeList &update,
                const consensus::OpId& op_id);

  size_t Count() const {
    return tree_.count();
  }

  // Dump a debug version of the tree to the logs. This is not thread-safe, so
  // is only really useful in unit tests.
  void DebugPrint() const;

  Status FlushToFile(DeltaFileWriter *dfw);

  // Create an iterator for applying deltas from this DMS.
  //
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  //
  // 'snapshot' is the MVCC state which determines which transactions
  // should be considered committed (and thus applied by the iterator).
  //
  // Returns Status::OK and sets 'iterator' to the new DeltaIterator, or
  // returns Status::NotFound if the mutations within this delta store
  // cannot include 'snap'.
  virtual Status NewDeltaIterator(const Schema *projection,
                                  const MvccSnapshot &snap,
                                  DeltaIterator** iterator) const OVERRIDE;

  virtual Status CheckRowDeleted(rowid_t row_idx, bool *deleted) const OVERRIDE;

  Status AlterSchema(const Schema& schema);

  virtual const Schema &schema() const OVERRIDE {
    return schema_;
  }

  const int64_t id() const { return id_; }

  virtual const DeltaStats& delta_stats() const OVERRIDE { return delta_stats_; }

  typedef btree::CBTree<DMSTreeTraits> DMSTree;
  typedef btree::CBTreeIterator<DMSTreeTraits> DMSTreeIter;

  size_t memory_footprint() const {
    return arena_->memory_footprint();
  }

  virtual std::string ToString() const OVERRIDE {
    return "DMS";
  }

 private:
  friend class DMSIterator;

  const DMSTree& tree() const {
    return tree_;
  }

  const int64_t id_;
  Schema schema_;

  std::tr1::shared_ptr<MemTracker> mem_tracker_;
  std::tr1::shared_ptr<MemoryTrackingBufferAllocator> allocator_;

  std::tr1::shared_ptr<ThreadSafeMemoryTrackingArena> arena_;

  // Concurrent B-Tree storing <key index> -> RowChangeList
  DMSTree tree_;

  DeltaStats delta_stats_;

  log::OpIdMinAnchorer anchorer_;

  DISALLOW_COPY_AND_ASSIGN(DeltaMemStore);
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
  Status Init() OVERRIDE;

  Status SeekToOrdinal(rowid_t row_idx) OVERRIDE;

  Status PrepareBatch(size_t nrows) OVERRIDE;

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) OVERRIDE;

  Status ApplyDeletes(SelectionVector *sel_vec) OVERRIDE;

  Status CollectMutations(vector<Mutation *> *dst, Arena *arena) OVERRIDE;

  Status FilterColumnsAndAppend(const metadata::ColumnIndexes& col_indexes,
                                vector<DeltaKeyAndUpdate>* out,
                                Arena* arena) OVERRIDE;

  string ToString() const OVERRIDE;

  virtual bool HasNext() OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(DMSIterator);
  FRIEND_TEST(TestDeltaMemStore, TestIteratorDoesUpdates);
  FRIEND_TEST(TestDeltaMemStore, TestCollectMutations);
  friend class DeltaMemStore;

  // Initialize the iterator.
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  // The pointer must also remain valid for the lifetime of the iterator.
  DMSIterator(const shared_ptr<const DeltaMemStore> &dms,
              const Schema *projection,
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

  // The index at which the last PrepareBatch() call was made
  rowid_t prepared_idx_;

  // The number of rows for which the last PrepareBatch() call was made
  uint32_t prepared_count_;

  // Whether there are prepared blocks built through PrepareBatch().
  bool prepared_;

  // True if SeekToOrdinal() been called at least once.
  bool seeked_;

  faststring prepared_buf_;

  // Projection from the schema of the deltamemstore to the projection
  // of the row blocks which will be passed to PrepareBatch(), etc.
  DeltaProjector projector_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;
};



} // namespace tablet
} // namespace kudu

#endif
