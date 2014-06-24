// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_MEMROWSET_H
#define KUDU_TABLET_MEMROWSET_H

#include <boost/optional.hpp>
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/scan_spec.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "consensus/opid_anchor_registry.h"
#include "tablet/concurrent_btree.h"
#include "tablet/mutation.h"
#include "tablet/rowset.h"
#include "tablet/tablet.pb.h"
#include "util/memory/arena.h"
#include "util/memory/memory.h"
#include "util/status.h"

namespace kudu {
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

class MemRowSet;

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

  const Schema& schema() const;

  Timestamp insertion_timestamp() const { return header_->insertion_timestamp; }

  Mutation* redo_head() { return header_->redo_head; }
  const Mutation* redo_head() const { return header_->redo_head; }

  const Slice &row_slice() const { return row_slice_; }

  bool is_null(size_t col_idx) const {
    return ContiguousRowHelper::is_null(schema(), row_slice_.data(), col_idx);
  }

  void set_null(size_t col_idx, bool is_null) const {
    ContiguousRowHelper::SetCellIsNull(schema(),
      const_cast<uint8_t*>(row_slice_.data()), col_idx, is_null);
  }

  const uint8_t *cell_ptr(size_t col_idx) const {
    return ContiguousRowHelper::cell_ptr(schema(), row_slice_.data(), col_idx);
  }

  uint8_t *mutable_cell_ptr(size_t col_idx) const {
    return const_cast<uint8_t*>(cell_ptr(col_idx));
  }

  const uint8_t *nullable_cell_ptr(size_t col_idx) const {
    return ContiguousRowHelper::nullable_cell_ptr(schema(), row_slice_.data(), col_idx);
  }

  Cell cell(size_t col_idx) const {
    return Cell(this, col_idx);
  }

  // Return true if this row is a "ghost" -- i.e its most recent mutation is
  // a deletion.
  //
  // NOTE: this call is O(n) in the number of mutations, since it has to walk
  // the linked list all the way to the end, checking if each mutation is a
  // DELETE or REINSERT. We expect the list is usually short (low-update use
  // cases) but if this becomes a bottleneck, we could cache the 'ghost' status
  // as a bit inside the row header.
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
    // Timestamp for the transaction which inserted this row. If a scanner with an
    // older snapshot sees this row, it will be ignored.
    Timestamp insertion_timestamp;

    // Pointer to the first mutation which has been applied to this row. Each
    // mutation is an instance of the Mutation class, making up a singly-linked
    // list for any mutations applied to the row.
    Mutation* redo_head;
  };

  Header *header_;

  // Actual row data.
  Slice row_slice_;

  const MemRowSet *memrowset_;
};

// Define an MRSRow instance using on-stack storage.
// This defines an array on the stack which is sized correctly for an MRSRow::Header
// plus a single row of the given schema, then constructs an MRSRow object which
// points into that stack storage.
#define DEFINE_MRSROW_ON_STACK(memrowset, varname, slice_name) \
  uint8_t varname##_size = sizeof(MRSRow::Header) + \
                           ContiguousRowHelper::row_size((memrowset)->schema_nonvirtual()); \
  uint8_t varname##_storage[varname##_size]; \
  Slice slice_name(varname##_storage, varname##_size); \
  ContiguousRowHelper::InitNullsBitmap((memrowset)->schema_nonvirtual(), slice_name); \
  MRSRow varname(memrowset, slice_name);


// In-memory storage for data currently being written to the tablet.
// This is a holding area for inserts, currently held in row form
// (i.e not columnar)
//
// The data is kept sorted.
class MemRowSet : public RowSet,
                  public std::tr1::enable_shared_from_this<MemRowSet> {
 public:
  class Iterator;

  MemRowSet(int64_t id,
            const Schema &schema,
            log::OpIdAnchorRegistry* opid_anchor_registry);

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
                           ProbeStats* stats,
                           OperationResultPB *result) OVERRIDE;

  // Return the number of entries in the memrowset.
  // NOTE: this requires iterating all data, and is thus
  // not very fast.
  uint64_t entry_count() const {
    return tree_.count();
  }

  // Conform entry_count to RowSet
  Status CountRows(rowid_t *count) const OVERRIDE {
    *count = entry_count();
    return Status::OK();
  }

  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const OVERRIDE;

  uint64_t EstimateOnDiskSize() const OVERRIDE {
    return 0;
  }

  boost::mutex *compact_flush_lock() OVERRIDE {
    return &compact_flush_lock_;
  }

  // MemRowSets are never available for compaction, currently.
  virtual bool IsAvailableForCompaction() OVERRIDE {
    return false;
  }

  // Return true if there are no entries in the memrowset.
  bool empty() const {
    return tree_.empty();
  }

  // TODO: unit test me
  Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                         ProbeStats* stats) const OVERRIDE;

  // Return the memory footprint of this memrowset.
  // Note that this may be larger than the sum of the data
  // inserted into the memrowset, due to arena and data structure
  // overhead.
  size_t memory_footprint() const {
    // TODO: merge the two into the same arena?
    return arena_.memory_footprint() + tree_.estimate_memory_usage();
  }

  // Return an iterator over the items in this memrowset.
  //
  // NOTE: for this function to work, there must be a shared_ptr
  // referring to this MemRowSet. Otherwise, this will throw
  // a C++ exception and all bets are off.
  //
  // TODO: clarify the consistency of this iterator in the method doc
  Iterator *NewIterator() const;
  Iterator *NewIterator(const Schema *projection,
                        const MvccSnapshot &snap) const;

  // Alias to conform to DiskRowSet interface
  virtual RowwiseIterator *NewRowIterator(const Schema *projection,
                                          const MvccSnapshot &snap) const OVERRIDE;

  // Create compaction input.
  CompactionInput *NewCompactionInput(const Schema* projection,
                                      const MvccSnapshot &snap) const OVERRIDE;

  // Return the Schema for the rows in this memrowset.
  virtual const Schema &schema() const OVERRIDE {
    return schema_;
  }

  // Same as schema(), but non-virtual method
  const Schema& schema_nonvirtual() const {
    return schema_;
  }

  int64_t mrs_id() const {
    return id_;
  }

  std::tr1::shared_ptr<metadata::RowSetMetadata> metadata() OVERRIDE {
    return std::tr1::shared_ptr<metadata::RowSetMetadata>(
        reinterpret_cast<metadata::RowSetMetadata *>(NULL));
  }

  Status AlterSchema(const Schema& schema) OVERRIDE;

  // Dump the contents of the memrowset to the given vector.
  // If 'lines' is NULL, dumps to LOG(INFO).
  //
  // This dumps every row, so should only be used in tests, etc.
  virtual Status DebugDump(vector<string> *lines = NULL) OVERRIDE;

  string ToString() const OVERRIDE {
    return string("memrowset");
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

  size_t DeltaMemStoreSize() const OVERRIDE { return 0; }

  size_t CountDeltaStores() const OVERRIDE { return 0; }

  Status FlushDeltas() OVERRIDE { return Status::OK(); }

  Status MinorCompactDeltaStores() OVERRIDE { return Status::OK(); }

 private:
  friend class Iterator;

  // Temporary hack to slow down mutators when the memrowset is over 1GB.
  void SlowMutators();

  // Perform a "Reinsert" -- handle an insertion into a row which was previously
  // inserted and deleted, but still has an entry in the MemRowSet.
  Status Reinsert(Timestamp timestamp,
                  const ConstContiguousRow& row_data,
                  MRSRow *row);

  typedef btree::CBTree<btree::BTreeTraits> MSBTree;

  int64_t id_;

  const Schema schema_;
  ThreadSafeArena arena_;

  typedef btree::CBTreeIterator<btree::BTreeTraits> MSBTIter;

  MSBTree tree_;

  // Approximate counts of mutations. This variable is updated non-atomically,
  // so it cannot be relied upon to be in any way accurate. It's only used
  // as a sanity check during flush.
  volatile uint64_t debug_insert_count_;
  volatile uint64_t debug_update_count_;

  boost::mutex compact_flush_lock_;

  Atomic32 has_logged_throttling_;

  log::OpIdMinAnchorer anchorer_;

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
  virtual ~Iterator() {}

  virtual Status Init(ScanSpec *spec) OVERRIDE;

  Status SeekAtOrAfter(const Slice &key, bool *exact);

  virtual Status PrepareBatch(size_t *nrows) OVERRIDE;

  bool has_upper_bound() const {
    return upper_bound_.is_initialized();
  }

  bool out_of_bounds(const Slice &key) const {
    DCHECK(has_upper_bound()) << "No upper bound set!";

    return key.compare(*upper_bound_) > 0;
  }

  size_t remaining_in_leaf() const {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    return iter_->remaining_in_leaf();
  }

  virtual Status MaterializeBlock(RowBlock *dst) OVERRIDE;

  virtual Status FinishBatch() OVERRIDE;

  virtual bool HasNext() const OVERRIDE {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    return state_ != kFinished && iter_->IsValid();
  }

  // NOTE: This method will return a MRSRow with the MemRowSet schema.
  //       The row is NOT projected using the schema specified to the iterator.
  const MRSRow GetCurrentRow() const {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    Slice dummy, mrsrow_data;
    iter_->GetCurrentEntry(&dummy, &mrsrow_data);
    return MRSRow(memrowset_.get(), mrsrow_data);
  }

  // Copy the current MRSRow to the 'dst_row' provided using the iterator projection schema.
  template <class RowType, class RowArenaType, class MutationArenaType>
  Status GetCurrentRow(RowType* dst_row,
                       RowArenaType* row_arena,
                       const Mutation** redo_head,
                       MutationArenaType* mutation_arena,
                       Timestamp* insertion_timestamp) {

    DCHECK(redo_head != NULL);

    // Get the row from the MemRowSet. It may have a different schema from the iterator projection.
    const MRSRow src_row = GetCurrentRow();

    *insertion_timestamp = src_row.insertion_timestamp();

    // Project the RowChangeList if required
    *redo_head = src_row.redo_head();
    if (!delta_projector_.is_identity()) {
      DCHECK(mutation_arena != NULL);

      Mutation *prev_redo = NULL;
      *redo_head = NULL;
      for (const Mutation *mut = src_row.redo_head(); mut != NULL; mut = mut->next()) {
        RETURN_NOT_OK(RowChangeListDecoder::ProjectUpdate(delta_projector_,
                                                          mut->changelist(),
                                                          &delta_buf_));

        // The projection resulted in an empty mutation (e.g. update of a removed column)
        if (delta_buf_.size() == 0) continue;

        Mutation *mutation = Mutation::CreateInArena(mutation_arena,
                                                     mut->timestamp(),
                                                     RowChangeList(delta_buf_));
        if (prev_redo != NULL) {
          prev_redo->set_next(mutation);
        } else {
          *redo_head = mutation;
        }
        prev_redo = mutation;
      }
    }

    // Project the Row
    return projector_.ProjectRowForRead(src_row, dst_row, row_arena);
  }

  bool Next() {
    DCHECK_NE(state_, kUninitialized) << "not initted";
    return iter_->Next();
  }

  string ToString() const OVERRIDE {
    return "memrowset iterator";
  }

  const Schema &schema() const OVERRIDE {
    return projector_.projection();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    // Currently we do not expose any non-disk related statistics in
    // IteratorStats.  However, callers of GetIteratorStats expected
    // an IteratorStats object for every column; vector::resize() is
    // used as it will also fill the 'stats' with new instances of
    // IteratorStats.
    stats->resize(schema().num_columns());
  }

 private:
  friend class MemRowSet;

  enum ScanState {
    // Enumerated constants to indicate the iterator state:
    kUninitialized = 0,
    kScanning = 1,  // We may continue fetching and returning values.
    kLastBatch = 2, // Current batch contains the upper bound, we
                    // may return all rows up to and including the upper bound,
                    // but we may not prepare any further batches.
    kFinished = 3   // We either know we can never reach the lower bound, or
                    // we've exceeded the upper bound.
  };

  DISALLOW_COPY_AND_ASSIGN(Iterator);

  Iterator(const std::tr1::shared_ptr<const MemRowSet> &mrs,
           MemRowSet::MSBTIter *iter,
           const Schema *projection,
           const MvccSnapshot &mvcc_snap)
    : memrowset_(mrs),
      iter_(iter),
      mvcc_snap_(mvcc_snap),
      projector_(&mrs->schema_nonvirtual(), projection),
      delta_projector_(&mrs->schema_nonvirtual(), projection),
      prepared_count_(0),
      prepared_idx_in_leaf_(0),
      state_(kUninitialized) {
    // TODO: various code assumes that a newly constructed iterator
    // is pointed at the beginning of the dataset. This causes a redundant
    // seek. Could make this lazy instead, or change the semantics so that
    // a seek is required (probably the latter)
    iter_->SeekToStart();
  }

  Status ApplyMutationsToProjectedRow(const Mutation *mutation_head,
                                      RowBlockRow *dst_row,
                                      Arena *dst_arena) {
    // Fast short-circuit the likely case of a row which was inserted and never
    // updated.
    if (PREDICT_TRUE(mutation_head == NULL)) {
      return Status::OK();
    }

    bool is_deleted = false;

    for (const Mutation *mut = mutation_head;
         mut != NULL;
         mut = mut->next_) {
      if (!mvcc_snap_.IsCommitted(mut->timestamp_)) {
        // Transaction which wasn't committed yet in the reader's snapshot.
        continue;
      }

      // Apply the mutation.

      // Check if it's a deletion.
      // TODO: can we reuse the 'decoder' object by adding a Reset or something?
      RowChangeListDecoder decoder(memrowset_->schema_nonvirtual(), mut->changelist());
      RETURN_NOT_OK(decoder.Init());
      if (decoder.is_delete()) {
        decoder.TwiddleDeleteStatus(&is_deleted);
      } else if (decoder.is_reinsert()) {
        decoder.TwiddleDeleteStatus(&is_deleted);

        ConstContiguousRow reinserted(memrowset_->schema_nonvirtual(),
                                      decoder.reinserted_row_slice());
        RETURN_NOT_OK(projector_.ProjectRowForRead(reinserted, dst_row, dst_arena));
      } else {
        DCHECK(decoder.is_update());

        // TODO: this is slow, since it makes multiple passes through the rowchangelist.
        // Instead, we should keep the backwards mapping of columns.
        BOOST_FOREACH(const RowProjector::ProjectionIdxMapping& mapping,
                      projector_.base_cols_mapping()) {
          RowChangeListDecoder decoder(memrowset_->schema_nonvirtual(), mut->changelist());
          RETURN_NOT_OK(decoder.Init());
          ColumnBlock dst_col = dst_row->column_block(mapping.first);
          RETURN_NOT_OK(decoder.ApplyToOneColumn(dst_row->row_index(), &dst_col,
                                                 mapping.second, dst_arena));
        }

        // TODO: Handle Delta Apply on projector_.adapter_cols_mapping()
        DCHECK_EQ(projector_.adapter_cols_mapping().size(), 0) << "alter type is not supported";
      }
    }

    // If the most recent mutation seen for the row was a DELETE, then set the selection
    // vector bit to 0, so it doesn't show up in the results.
    if (is_deleted) {
      dst_row->SetRowUnselected();
    }

    return Status::OK();
  }


  const std::tr1::shared_ptr<const MemRowSet> memrowset_;
  gscoped_ptr<MemRowSet::MSBTIter> iter_;

  // The MVCC snapshot which determines which rows and mutations are visible to
  // this iterator.
  const MvccSnapshot mvcc_snap_;

  // Mapping from projected column index back to memrowset column index.
  RowProjector projector_;
  DeltaProjector delta_projector_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;

  size_t prepared_count_;
  size_t prepared_idx_in_leaf_;

  // Temporary local buffer used for seeking to hold the encoded
  // seek target.
  faststring tmp_buf;

  // State of the scanner: indicates whether we should keep scanning/fetching,
  // whether we've scanned the last batch, or whether we've reached the upper bounds
  // or will never reach the lower bounds (no more rows can be returned)
  ScanState state_;

  // Pushed down encoded upper bound key, if any
  boost::optional<const Slice &> upper_bound_;
};

inline const Schema& MRSRow::schema() const {
  return memrowset_->schema_nonvirtual();
}

} // namespace tablet
} // namespace kudu

#endif
