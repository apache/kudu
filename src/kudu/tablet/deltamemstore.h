// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/common/rowid.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/concurrent_btree.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"

namespace kudu {

class ColumnBlock;
class MemTracker;
class MemoryTrackingBufferAllocator;
class RowChangeList;
class ScanSpec;
class SelectionVector;
struct ColumnId;

namespace consensus {
class OpId;
} // namespace consensus

namespace fs {
struct IOContext;
} // namespace fs

namespace tablet {

class DeltaFileWriter;
class Mutation;
struct RowIteratorOptions;

struct DMSTreeTraits : public btree::BTreeTraits {
  typedef ThreadSafeMemoryTrackingArena ArenaType;
};

// In-memory storage for data which has been recently updated.
// This essentially tracks a 'diff' per row, which contains the
// modified columns.

class DeltaMemStore : public DeltaStore,
                      public std::enable_shared_from_this<DeltaMemStore> {
 public:
  static Status Create(int64_t id, int64_t rs_id,
                       log::LogAnchorRegistry* log_anchor_registry,
                       std::shared_ptr<MemTracker> parent_tracker,
                       std::shared_ptr<DeltaMemStore>* dms);

  virtual Status Init(const fs::IOContext* io_context) OVERRIDE;

  virtual bool Initted() const OVERRIDE {
    return true;
  }

  // Update the given row in the database.
  // Copies the data, as well as any referenced values into this DMS's local
  // arena.
  Status Update(Timestamp timestamp, rowid_t row_idx,
                const RowChangeList &update,
                const consensus::OpId& op_id);

  size_t Count() const {
    return tree_.count();
  }

  bool Empty() const {
    return tree_.empty();
  }

  // Dump a debug version of the tree to the logs. This is not thread-safe, so
  // is only really useful in unit tests.
  void DebugPrint() const;

  // Flush the DMS to the given file writer.
  // Returns statistics in *stats.
  Status FlushToFile(DeltaFileWriter *dfw,
                     std::unique_ptr<DeltaStats>* stats);

  // Create an iterator for applying deltas from this DMS.
  //
  // The projection passed in 'opts' must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  //
  // The snapshot in 'opts' is the MVCC state which determines which transactions
  // should be considered committed (and thus applied by the iterator).
  //
  // Returns Status::OK and sets 'iterator' to the new DeltaIterator, or
  // returns Status::NotFound if the mutations within this delta store
  // cannot include the snapshot.
  virtual Status NewDeltaIterator(const RowIteratorOptions& opts,
                                  std::unique_ptr<DeltaIterator>* iterator) const OVERRIDE;

  virtual Status CheckRowDeleted(rowid_t row_idx, const fs::IOContext* io_context,
                                 bool* deleted) const OVERRIDE;

  virtual uint64_t EstimateSize() const OVERRIDE {
    return arena_->memory_footprint();
  }

  const int64_t id() const { return id_; }

  typedef btree::CBTree<DMSTreeTraits> DMSTree;
  typedef btree::CBTreeIterator<DMSTreeTraits> DMSTreeIter;

  virtual std::string ToString() const OVERRIDE {
    return "DMS";
  }

  // Get the minimum log index for this DMS, -1 if it wasn't set.
  int64_t MinLogIndex() const {
    return anchorer_.minimum_log_index();
  }

  // The returned stats will always be empty, and the number of columns unset.
  virtual const DeltaStats& delta_stats() const OVERRIDE {
    return delta_stats_;
  }

  // Returns the number of deleted rows in this DMS.
  int64_t deleted_row_count() const;

  // Returns the highest timestamp of any updates applied to this DMS. Returns
  // 'none' if no updates have been applied.
  boost::optional<Timestamp> highest_timestamp() const {
    std::lock_guard<simple_spinlock> l(ts_lock_);
    return highest_timestamp_ == Timestamp::kMin ?
        boost::none : boost::make_optional(highest_timestamp_);
  }

 private:
  friend class DMSIterator;

  DeltaMemStore(int64_t id,
                int64_t rs_id,
                log::LogAnchorRegistry* log_anchor_registry,
                std::shared_ptr<MemTracker> parent_tracker);

  const DMSTree& tree() const {
    return tree_;
  }

  const int64_t id_;    // DeltaMemStore ID.
  const int64_t rs_id_; // Rowset ID.

  mutable simple_spinlock ts_lock_;
  Timestamp highest_timestamp_;

  std::shared_ptr<MemoryTrackingBufferAllocator> allocator_;

  std::shared_ptr<ThreadSafeMemoryTrackingArena> arena_;

  // Concurrent B-Tree storing <key index> -> RowChangeList
  DMSTree tree_;

  log::MinLogIndexAnchorer anchorer_;

  const DeltaStats delta_stats_;

  // It's possible for multiple mutations to apply to the same row
  // in the same timestamp (e.g. if a batch contains multiple updates for that
  // row). In that case, we need to append a sequence number to the delta key
  // in the underlying tree, so that the later operations will sort after
  // the earlier ones. This atomic integer serves to provide such a sequence
  // number, and is only used in the case that such a collision occurs.
  AtomicInt<Atomic32> disambiguator_sequence_number_;

  // Number of deleted rows in this DMS.
  AtomicInt<int64_t> deleted_row_count_;

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
  Status Init(ScanSpec* spec) override;

  Status SeekToOrdinal(rowid_t row_idx) override;

  Status PrepareBatch(size_t nrows, int prepare_flags) override;

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                      const SelectionVector& filter) override;

  Status ApplyDeletes(SelectionVector* sel_vec) override;

  Status SelectDeltas(SelectedDeltas* deltas) override;

  Status CollectMutations(std::vector<Mutation*>* dst, Arena* arena) override;

  Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                         std::vector<DeltaKeyAndUpdate>* out,
                                         Arena* arena) override;

  std::string ToString() const override;

  bool HasNext() override;

  bool MayHaveDeltas() const override;

 private:
  DISALLOW_COPY_AND_ASSIGN(DMSIterator);
  friend class DeltaMemStore;

  // Initialize the iterator.
  // The projection passed here must be the same as the schema of any
  // RowBlocks which are passed in, or else bad things will happen.
  // The pointers in 'opts' must also remain valid for the lifetime of the iterator.
  DMSIterator(const std::shared_ptr<const DeltaMemStore> &dms,
              RowIteratorOptions opts);

  const std::shared_ptr<const DeltaMemStore> dms_;

  DeltaPreparer<DMSPreparerTraits> preparer_;

  std::unique_ptr<DeltaMemStore::DMSTreeIter> iter_;

  bool initted_;

  // True if SeekToOrdinal() been called at least once.
  bool seeked_;
};

} // namespace tablet
} // namespace kudu
