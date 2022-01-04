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

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/row.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/concurrent_btree.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/atomic.h"
#include "kudu/util/faststring.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class MemTracker;
class MemoryTrackingBufferAllocator;
class RowBlock;
class RowBlockRow;
class RowChangeList;
class ScanSpec;

namespace fs {
struct IOContext;
}  // namespace fs

namespace tablet {
class MvccSnapshot;
}  // namespace tablet

namespace consensus {
class OpId;
}  // namespace consensus

namespace tablet {
//
// Implementation notes:
// --------------------------
// The MemRowSet is a concurrent b-tree which stores newly inserted data which
// has not yet been flushed to on-disk rowsets. In order to provide snapshot
// consistency, data is never updated in-place in the memrowset after insertion.
// Rather, a chain of mutations hangs off each row, acting as a per-row "redo log".
//
// Each row is stored in exactly one CBTree entry. Its key is the encoded form
// of the row's primary key, such that the entries sort correctly using the default
// lexicographic comparator. The value for each row is an instance of MRSRow.
//
// NOTE: all allocations done by the MemRowSet are done inside its associated
// thread-safe arena, and then freed in bulk when the MemRowSet is destructed.

class CompactionInput;
class MemRowSet;
class Mutation;
class OperationResultPB;
class TxnMetadata;

// The value stored in the CBTree for a single row.
class MRSRow {
 public:
  typedef ContiguousRowCell<MRSRow> Cell;

  MRSRow(const MemRowSet *memrowset, const Slice &s) {
    DCHECK_GE(s.size(), sizeof(Header));
    row_slice_ = s;
    header_ = reinterpret_cast<Header *>(row_slice_.mutable_data());
    row_slice_.remove_prefix(sizeof(Header));
    memrowset_ = memrowset;
  }

  const Schema* schema() const;

  Timestamp insertion_timestamp() const { return header_->insertion_timestamp; }

  Mutation* redo_head() { return header_->redo_head; }

  // Load 'redo_head' with an 'Acquire' memory barrier.
  Mutation* acquire_redo_head() {
    return reinterpret_cast<Mutation*>(
        base::subtle::Acquire_Load(reinterpret_cast<AtomicWord*>(&header_->redo_head)));
  }
  const Mutation* redo_head() const { return header_->redo_head; }

  const Slice &row_slice() const { return row_slice_; }

  const uint8_t* row_data() const { return row_slice_.data(); }

  bool is_null(size_t col_idx) const {
    return ContiguousRowHelper::is_null(*schema(), row_slice_.data(), col_idx);
  }

  void set_null(size_t col_idx, bool is_null) const {
    ContiguousRowHelper::SetCellIsNull(*schema(),
      const_cast<uint8_t*>(row_slice_.data()), col_idx, is_null);
  }

  const uint8_t *cell_ptr(size_t col_idx) const {
    return ContiguousRowHelper::cell_ptr(*schema(), row_slice_.data(), col_idx);
  }

  uint8_t *mutable_cell_ptr(size_t col_idx) const {
    return const_cast<uint8_t*>(cell_ptr(col_idx));
  }

  const uint8_t *nullable_cell_ptr(size_t col_idx) const {
    return ContiguousRowHelper::nullable_cell_ptr(*schema(), row_slice_.data(), col_idx);
  }

  Cell cell(size_t col_idx) const {
    return Cell(this, col_idx);
  }

  // Return true if this row is a "ghost" -- i.e its most recent mutation is
  // a deletion.
  bool IsGhost() const;

 private:
  friend class MemRowSet;

  template <class ArenaType>
  Status CopyRow(const ConstContiguousRow& row, ArenaType *arena) {
    // the representation of the MRSRow and ConstContiguousRow is the same.
    // so, instead of using CopyRow we can just do a memcpy.
    memcpy(row_slice_.mutable_data(), row.row_data(), row_slice_.size());
    // Copy any referred-to memory to arena.
    return kudu::RelocateIndirectDataToArena(this, arena);
  }

  struct Header {
      // Timestamp for the op which inserted this row. If a scanner with an
      // older snapshot sees this row, it will be ignored.
      Timestamp insertion_timestamp;

      // Pointers to the first and last mutations that have been applied to this row.
      // Together they comprise a singly-linked list of all of the row's mutations,
      // with the head and tail used for efficient iteration and insertion respectively.
      Mutation* redo_head;
      Mutation* redo_tail;
  };

  Header *header_;

  // Actual row data.
  Slice row_slice_;

  const MemRowSet *memrowset_;
};

struct MSBTreeTraits : public btree::BTreeTraits {
  typedef ThreadSafeMemoryTrackingArena ArenaType;
};

// Define an MRSRow instance using on-stack storage.
// This defines an array on the stack which is sized correctly for an MRSRow::Header
// plus a single row of the given schema, then constructs an MRSRow object which
// points into that stack storage.
#define DEFINE_MRSROW_ON_STACK(memrowset, varname, slice_name) \
  size_t varname##_size = sizeof(MRSRow::Header) + \
                           ContiguousRowHelper::row_size(*(memrowset)->schema_nonvirtual().get()); \
  uint8_t varname##_storage[varname##_size]; \
  Slice slice_name(varname##_storage, varname##_size); \
  ContiguousRowHelper::InitNullsBitmap(*(memrowset)->schema_nonvirtual().get(), slice_name); \
  MRSRow varname(memrowset, slice_name);


// In-memory storage for data currently being written to the tablet.
// This is a holding area for inserts, currently held in row form
// (i.e not columnar)
//
// The data is kept sorted.
class MemRowSet : public RowSet,
                  public std::enable_shared_from_this<MemRowSet>,
                  public enable_make_shared<MemRowSet> {
 public:
  class Iterator;

  static Status Create(int64_t id,
                       const SchemaPtr& schema,
                       log::LogAnchorRegistry* log_anchor_registry,
                       std::shared_ptr<MemTracker> parent_tracker,
                       std::shared_ptr<MemRowSet>* mrs);

  // Create() variant for a MRS that get inserted to by a single transaction of
  // the given transaction ID and metadata.
  static Status Create(int64_t id,
                       const SchemaPtr& schema,
                       int64_t txn_id,
                       scoped_refptr<TxnMetadata> txn_metadata,
                       log::LogAnchorRegistry* log_anchor_registry,
                       std::shared_ptr<MemTracker> parent_tracker,
                       std::shared_ptr<MemRowSet>* mrs);
  ~MemRowSet();

  // Insert a new row into the memrowset.
  //
  // The provided 'row' must have the same memrowset's Schema.
  // (TODO: Different schema are not yet supported)
  //
  // After insert, the row and any referred-to memory (eg for strings)
  // have been copied into this MemRowSet's internal storage, and thus
  // the provided memory buffer may safely be re-used or freed.
  //
  // Returns Status::OK unless allocation fails.
  Status Insert(Timestamp timestamp,
                const ConstContiguousRow& row,
                const consensus::OpId& op_id);

  // Update or delete an existing row in the memrowset.
  //
  // Returns Status::NotFound if the row doesn't exist.
  virtual Status MutateRow(Timestamp timestamp,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &delta,
                           const consensus::OpId& op_id,
                           const fs::IOContext* io_context,
                           ProbeStats* stats,
                           OperationResultPB *result) override;

  // Return the number of entries in the memrowset.
  // NOTE: this requires iterating all data, and is thus
  // not very fast.
  uint64_t entry_count() const {
    return tree_.count();
  }

  // Conform entry_count to RowSet
  Status CountRows(const fs::IOContext* /*io_context*/, rowid_t *count) const override {
    *count = entry_count();
    return Status::OK();
  }

  virtual Status CountLiveRows(uint64_t* count) const override {
    *count = live_row_count_.Load();
    return Status::OK();
  }

  virtual Status GetBounds(std::string *min_encoded_key,
                           std::string *max_encoded_key) const override;

  uint64_t OnDiskSize() const override {
    return 0;
  }

  uint64_t OnDiskBaseDataSize() const override {
    return 0;
  }

  uint64_t OnDiskBaseDataSizeWithRedos() const override {
    return 0;
  }

  uint64_t OnDiskBaseDataColumnSize(const ColumnId& col_id) const override {
    return 0;
  }

  std::mutex *compact_flush_lock() override {
    return &compact_flush_lock_;
  }

  bool has_been_compacted() const override {
    return has_been_compacted_.load();
  }

  void set_has_been_compacted() override {
    has_been_compacted_.store(true);
  }

  // MemRowSets are never available for compaction, currently.
  virtual bool IsAvailableForCompaction() override {
    return false;
  }

  // Return true if there are no entries in the memrowset.
  bool empty() const {
    return tree_.empty();
  }

  // TODO(todd): unit test me
  Status CheckRowPresent(const RowSetKeyProbe &probe, const fs::IOContext* io_context,
                         bool *present, ProbeStats* stats) const override;

  // Return the memory footprint of this memrowset.
  // Note that this may be larger than the sum of the data
  // inserted into the memrowset, due to arena and data structure
  // overhead.
  size_t memory_footprint() const {
    return arena_->memory_footprint();
  }

  // Return an iterator over the items in this memrowset.
  //
  // NOTE: for this function to work, there must be a shared_ptr
  // referring to this MemRowSet. Otherwise, this will throw
  // a C++ exception and all bets are off.
  //
  // TODO(todd): clarify the consistency of this iterator in the method doc
  Iterator *NewIterator() const;
  Iterator *NewIterator(const RowIteratorOptions& opts) const;

  // Alias to conform to DiskRowSet interface
  virtual Status NewRowIterator(const RowIteratorOptions& opts,
                                std::unique_ptr<RowwiseIterator>* out) const override;

  // Create compaction input.
  virtual Status NewCompactionInput(const SchemaPtr& projection,
                                    const MvccSnapshot& snap,
                                    const fs::IOContext* io_context,
                                    std::unique_ptr<CompactionInput>* out) const override;

  // Return the Schema for the rows in this memrowset.
   const SchemaPtr schema() const {
    return schema_;
  }

  // Same as schema(), but non-virtual method
  const SchemaPtr schema_nonvirtual() const {
    return schema_;
  }

  int64_t mrs_id() const {
    return id_;
  }

  // Transaction ID that inserted the rows of this MRS. 'none' if the rows in
  // this MRS were not inserted as a part of a transaction.
  const boost::optional<int64_t>& txn_id() const {
    return txn_id_;
  }

  // Transaction metadata of the transaction that inserted the rows of this
  // MRS. 'nullptr' if the rows in this MRS were not inserted as a part of a
  // transaction.
  const scoped_refptr<TxnMetadata>& txn_metadata() const {
    return txn_metadata_;
  }

  std::shared_ptr<RowSetMetadata> metadata() override {
    return std::shared_ptr<RowSetMetadata>(
        reinterpret_cast<RowSetMetadata *>(NULL));
  }

  // Dump the contents of the memrowset to the given vector.
  // If 'lines' is NULL, dumps to LOG(INFO).
  //
  // This dumps every row, so should only be used in tests, etc.
  virtual Status DebugDump(std::vector<std::string> *lines) override;

  std::string ToString() const override {
    return strings::Substitute("memrowset$0",
        txn_id_ ? strings::Substitute("(txn_id=$0)", *txn_id_) : "");
  }

  // Mark the memrowset as frozen. See CBTree::Freeze()
  void Freeze() {
    tree_.Freeze();
  }

  uint64_t debug_insert_count() const {
    return debug_insert_count_;
  }
  uint64_t debug_update_count() const {
    return debug_update_count_;
  }

  size_t DeltaMemStoreSize() const override { return 0; }

  bool DeltaMemStoreInfo(size_t* /*size_bytes*/, MonoTime* /*creation_time*/) const override {
    return false;
  }

  bool DeltaMemStoreEmpty() const override { return true; }

  int64_t MinUnflushedLogIndex() const override {
    return anchorer_.minimum_log_index();
  }

  double DeltaStoresCompactionPerfImprovementScore(
      DeltaCompactionType /*type*/) const override {
    return 0;
  }

  Status EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp /*ancient_history_mark*/,
                                                     int64_t* bytes) override {
    DCHECK(bytes);
    *bytes = 0;
    return Status::OK();
  }

  Status IsDeletedAndFullyAncient(Timestamp /*ancient_history_mark*/,
                                  bool* deleted_and_ancient) override {
    DCHECK(deleted_and_ancient);
    *deleted_and_ancient = false;
    return Status::OK();
  }

  Status InitUndoDeltas(Timestamp /*ancient_history_mark*/,
                        MonoTime /*deadline*/,
                        const fs::IOContext* /*io_context*/,
                        int64_t* delta_blocks_initialized,
                        int64_t* bytes_in_ancient_undos) override {
    if (delta_blocks_initialized) *delta_blocks_initialized = 0;
    if (bytes_in_ancient_undos) *bytes_in_ancient_undos = 0;
    return Status::OK();
  }

  Status DeleteAncientUndoDeltas(Timestamp /*ancient_history_mark*/,
                                 const fs::IOContext* /*io_context*/,
                                 int64_t* blocks_deleted, int64_t* bytes_deleted) override {
    if (blocks_deleted) *blocks_deleted = 0;
    if (bytes_deleted) *bytes_deleted = 0;
    return Status::OK();
  }

  Status FlushDeltas(const fs::IOContext* /*io_context*/) override { return Status::OK(); }

  Status MinorCompactDeltaStores(
      const fs::IOContext* /*io_context*/) override { return Status::OK(); }

 protected:
  MemRowSet(int64_t id,
            const SchemaPtr schema,
            boost::optional<int64_t> txn_id,
            scoped_refptr<TxnMetadata> txn_metadata,
            log::LogAnchorRegistry* log_anchor_registry,
            std::shared_ptr<MemTracker> parent_tracker);

 private:
  friend class Iterator;

  // Perform a "Reinsert" -- handle an insertion into a row which was previously
  // inserted and deleted, but still has an entry in the MemRowSet.
  Status Reinsert(Timestamp timestamp,
                  const ConstContiguousRow& row,
                  MRSRow *ms_row);

  typedef btree::CBTree<MSBTreeTraits> MSBTree;

  int64_t id_;
  SchemaPtr schema_;

  // The transaction ID that inserted into this MemRowSet, and its corresponding metadata.
  boost::optional<int64_t> txn_id_;
  scoped_refptr<TxnMetadata> txn_metadata_;

  std::shared_ptr<MemoryTrackingBufferAllocator> allocator_;
  std::shared_ptr<ThreadSafeMemoryTrackingArena> arena_;

  typedef btree::CBTreeIterator<MSBTreeTraits> MSBTIter;

  MSBTree tree_;

  // Approximate counts of mutations. This variable is updated non-atomically,
  // so it cannot be relied upon to be in any way accurate. It's only used
  // as a sanity check during flush.
  volatile uint64_t debug_insert_count_;
  volatile uint64_t debug_update_count_;

  std::mutex compact_flush_lock_;

  log::MinLogIndexAnchorer anchorer_;

  // Flag indicating whether the rowset has been removed from a rowset tree,
  // and thus should not be scheduled for further compactions.
  std::atomic<bool> has_been_compacted_;

  // Number of live rows in this MRS.
  AtomicInt<uint64_t> live_row_count_;

  DISALLOW_COPY_AND_ASSIGN(MemRowSet);
};

// An iterator through in-memory data stored in a MemRowSet.
// This holds a reference to the MemRowSet, and so the memrowset
// must not be freed while this iterator is outstanding.
//
// This iterator is not a full snapshot, but individual rows
// are consistent, and it is safe to iterate during concurrent
// mutation. The consistency guarantee is that it will return
// at least all rows that were present at the time of construction,
// and potentially more. Each row will be at least as current as
// the time of construction, and potentially more current.
class MemRowSet::Iterator : public RowwiseIterator {
 public:
  class MRSRowProjector;

  virtual ~Iterator();

  virtual Status Init(ScanSpec *spec) override;

  Status SeekAtOrAfter(const Slice &key, bool *exact);

  virtual Status NextBlock(RowBlock *dst) override;

  bool has_upper_bound() const {
    return exclusive_upper_bound_.is_initialized();
  }

  bool out_of_bounds(const Slice &key) const {
    DCHECK(has_upper_bound()) << "No upper bound set!";
    return key >= *exclusive_upper_bound_;
  }

  size_t remaining_in_leaf() const {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    return iter_->remaining_in_leaf();
  }

  virtual bool HasNext() const override {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    return state_ != kFinished && iter_->IsValid();
  }

  // NOTE: This method will return a MRSRow with the MemRowSet schema.
  //       The row is NOT projected using the schema specified to the iterator.
  MRSRow GetCurrentRow() const {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    Slice mrsrow_data = iter_->GetCurrentValue();
    return MRSRow(memrowset_.get(), mrsrow_data);
  }

  // Copy the current MRSRow to the 'dst_row' provided using the iterator
  // projection schema. Used in compactions.
  Status GetCurrentRow(RowBlockRow* dst_row,
                       Arena* row_arena,
                       Mutation** redo_head,
                       Arena* mutation_arena,
                       Timestamp* insertion_timestamp);

  bool Next() {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    return iter_->Next();
  }

  std::string ToString() const override {
    return "memrowset iterator";
  }

  const SchemaPtr schema() const override {
    return opts_.projection;
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const override {
    // Currently we do not expose any non-disk related statistics in
    // IteratorStats.  However, callers of GetIteratorStats expected
    // an IteratorStats object for every column; vector::resize() is
    // used as it will also fill the 'stats' with new instances of
    // IteratorStats.
    stats->resize(schema()->num_columns());
  }

 private:
  friend class MemRowSet;

  enum ScanState {
    // Enumerated constants to indicate the iterator state:
    kUninitialized = 0,
    kScanning = 1,  // We may continue fetching and returning values.
    kFinished = 2   // We either know we can never reach the lower bound, or
                    // we've exceeded the upper bound.
  };

  DISALLOW_COPY_AND_ASSIGN(Iterator);

  Iterator(const std::shared_ptr<const MemRowSet> &mrs,
           MemRowSet::MSBTIter *iter, RowIteratorOptions opts);

  // Retrieves a block of dst->nrows() rows from the MemRowSet.
  //
  // Writes the number of rows retrieved to 'fetched'.
  Status FetchRows(RowBlock* dst, size_t* fetched);

  // Walks the mutations in 'mutation_head', applying relevant ones to 'dst_row'
  // (performing any allocations out of 'dst_arena'). 'insert_excluded' is true
  // if the row's original insertion took place outside the iterator's time range.
  //
  // On success, 'apply_status' summarizes the application process.
  enum ApplyStatus {
    // No mutations were applied to the row, either because none existed or
    // because none were relevant.
    NONE_APPLIED,

    // At least one mutation was applied to the row.
    APPLIED_AND_PRESENT,

    // At least one mutation was applied to the row, and the row's final state
    // was deleted (i.e. the last mutation was a DELETE).
    APPLIED_AND_DELETED,

    // Some mutations were applied, but the sequence of applied mutations was
    // such that clients should never see this row in their output (i.e. the row
    // was inserted and deleted in the same timestamp).
    APPLIED_AND_UNOBSERVABLE,
  };
  Status ApplyMutationsToProjectedRow(const Mutation* mutation_head,
                                      RowBlockRow* dst_row,
                                      Arena* dst_arena,
                                      bool insert_excluded,
                                      ApplyStatus* apply_status);

  const std::shared_ptr<const MemRowSet> memrowset_;
  std::unique_ptr<MemRowSet::MSBTIter> iter_;

  const RowIteratorOptions opts_;

  // Mapping from projected column index back to memrowset column index.
  // Relies on the MRSRowProjector interface to abstract from the two
  // different implementations of the RowProjector, which may change
  // at runtime (using vs. not using code generation).
  const std::unique_ptr<MRSRowProjector> projector_;
  DeltaProjector delta_projector_;

  // The index of the first IS_DELETED virtual column in the projection schema,
  // or kColumnNotFound if one doesn't exist.
  const int projection_vc_is_deleted_idx_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;

  // Temporary local buffer used for seeking to hold the encoded
  // seek target.
  faststring tmp_buf;

  // State of the scanner: indicates whether we should keep scanning/fetching,
  // whether we've scanned the last batch, or whether we've reached the upper bounds
  // or will never reach the lower bounds (no more rows can be returned)
  ScanState state_;

  // Pushed down encoded upper bound key, if any
  boost::optional<const Slice &> exclusive_upper_bound_;
};

inline const Schema* MRSRow::schema() const {
  return memrowset_->schema_nonvirtual().get();
}

} // namespace tablet
} // namespace kudu
