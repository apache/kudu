// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_MEMROWSET_H
#define KUDU_TABLET_MEMROWSET_H

#include <boost/noncopyable.hpp>
#include <tr1/memory>

#include "tablet/concurrent_btree.h"
#include "tablet/rowset-interfaces.h"
#include "tablet/mutation.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "util/memory/arena.h"
#include "util/memory/memory.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

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
// lexicographic comparator. The value for each row is an instance of MSRow.
//
// NOTE: all allocations done by the MemRowSet are done inside its associated
// thread-safe arena, and then freed in bulk when the MemRowSet is destructed.


// The value stored in the CBTree for a single row.
class MSRow {
 public:
  explicit MSRow(const Slice &s) {
    DCHECK_GE(s.size(), sizeof(Header));
    row_slice_ = s;
    header_ = reinterpret_cast<Header *>(row_slice_.mutable_data());
    row_slice_.remove_prefix(sizeof(Header));
  }

  txid_t insertion_txid() const { return header_->insertion_txid; }

  Mutation *mutation_head() { return header_->mutation_head; }
  const Mutation *mutation_head() const { return header_->mutation_head; }

  const Slice &row_slice() const { return row_slice_; }

 private:
  friend class MemRowSet;

  struct Header {
    // txid_t for the transaction which inserted this row. If a scanner with an
    // older snapshot sees this row, it will be ignored.
    txid_t insertion_txid;

    // Pointer to the first mutation which has been applied to this row. Each
    // mutation is an instance of the Mutation class, making up a singly-linked
    // list for any mutations applied to the row.
    Mutation *mutation_head;
  };

  Header *header_;

  // Actual row data.
  Slice row_slice_;
};

// Define an MSRow instance using on-stack storage.
// This defines an array on the stack which is sized correctly for an MSRow::Header
// plus a single row of the given schema, then constructs an MSRow object which
// points into that stack storage.
#define DEFINE_MSROW_ON_STACK(schema, varname, slice_name) \
  uint8_t varname##_size = sizeof(MSRow::Header) + schema.byte_size(); \
  uint8_t varname##_storage[varname##_size]; \
  Slice slice_name(varname##_storage, varname##_size); \
  MSRow varname(slice_name);



// In-memory storage for data currently being written to the tablet.
// This is a holding area for inserts, currently held in row form
// (i.e not columnar)
//
// The data is kept sorted.
class MemRowSet : boost::noncopyable,
                 public RowSet,
                 public std::tr1::enable_shared_from_this<MemRowSet> {
 public:
  class Iterator;

  explicit MemRowSet(const Schema &schema);


  // Insert a new row into the memrowset.
  //
  // The provided 'data' slice should have length equivalent to this
  // memrowset's Schema.byte_size().
  //
  // After insert, the row and any referred-to memory (eg for strings)
  // have been copied into this MemRowSet's internal storage, and thus
  // the provided memory buffer may safely be re-used or freed.
  //
  // Returns Status::OK unless allocation fails.
  Status Insert(txid_t txid, const Slice &data);


  // Update an existing row in the memrowset.
  //
  // Returns Status::NotFound if the row doesn't exist.
  Status UpdateRow(txid_t txid,
                   const void *key,
                   const RowChangeList &update);

  // Return the number of entries in the memrowset.
  // NOTE: this requires iterating all data, and is thus
  // not very fast.
  uint64_t entry_count() const {
    return tree_.count();
  }

  // Conform entry_count to RowSet
  Status CountRows(rowid_t *count) const {
    *count = entry_count();
    return Status::OK();
  }

  uint64_t EstimateOnDiskSize() const {
    return 0;
  }

  boost::mutex *compact_flush_lock() {
    return &compact_flush_lock_;
  }

  // Return true if there are no entries in the memrowset.
  bool empty() const {
    return tree_.empty();
  }

  // TODO: unit test me
  Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present) const;

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
  Iterator *NewIterator(const Schema &projection,
                        const MvccSnapshot &snap) const;

  // Alias to conform to DiskRowSet interface
  RowwiseIterator *NewRowIterator(const Schema &projection,
                                  const MvccSnapshot &snap) const;

  // Create compaction input.
  CompactionInput *NewCompactionInput(const MvccSnapshot &snap) const;

  // Return the Schema for the rows in this memrowset.
  const Schema &schema() const {
    return schema_;
  }

  // Dump the contents of the memrowset to the INFO log.
  // This dumps every row, so should only be used in tests, etc
  void DebugDump();

  string ToString() const {
    return string("memrowset");
  }

  Status Delete() {
    // After a flush, the flush/compact code will call Delete(). This
    // has no effect since there is nothing on-disk to remove.
    return Status::OK();
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

 private:
  friend class Iterator;

  // Temporary hack to slow down mutators when the memrowset is over 1GB.
  void SlowMutators();

  typedef btree::CBTree<btree::BTreeTraits> MSBTree;

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
class MemRowSet::Iterator : public RowwiseIterator, boost::noncopyable {
 public:
  virtual ~Iterator() {}

  virtual Status Init(ScanSpec *spec) {
    RETURN_NOT_OK(projection_.GetProjectionFrom(
                    memrowset_->schema(), &projection_mapping_));

    return Status::OK();
  }

  Status SeekAtOrAfter(const Slice &key, bool *exact) {
    CHECK(!projection_mapping_.empty()) << "not initted";

    if (key.size() > 0) {
      tmp_buf.clear();
      memrowset_->schema().EncodeComparableKey(key, &tmp_buf);
    } else {
      // Seeking to empty key shouldn't try to run any encoding.
      tmp_buf.resize(0);
    }

    if (iter_->SeekAtOrAfter(Slice(tmp_buf), exact) ||
        key.size() == 0) {
      return Status::OK();
    } else {
      return Status::NotFound("no match in memrowset");
    }
  }

  virtual Status PrepareBatch(size_t *nrows) {
    DCHECK(!projection_mapping_.empty()) << "not initted";
    if (PREDICT_FALSE(!iter_->IsValid())) {
      *nrows = 0;
      return Status::NotFound("end of iter");
    }

    size_t rem_in_leaf = iter_->remaining_in_leaf();
    if (PREDICT_TRUE(rem_in_leaf < *nrows)) {
      *nrows = rem_in_leaf;
    }
    prepared_count_ = *nrows;
    prepared_idx_in_leaf_ = iter_->index_in_leaf();
    return Status::OK();
  }

  size_t remaining_in_leaf() const {
    return iter_->remaining_in_leaf();
  }

  virtual Status MaterializeBlock(RowBlock *dst) {
    // TODO: add dcheck that dst->schema() matches our schema
    // also above TODO applies to a lot of other CopyNextRows cases
    DCHECK(!projection_mapping_.empty()) << "not initted";

    DCHECK_EQ(dst->nrows(), prepared_count_);
    Slice k, v;
    size_t fetched = 0;
    for (size_t i = prepared_idx_in_leaf_; fetched < prepared_count_; i++) {
      // Copy the row into the destination, including projection
      // and relocating slices.
      // TODO: can we share some code here with CopyRowToArena() from row.h
      // or otherwise put this elsewhere?
      iter_->GetEntryInLeaf(i, &k, &v);
      MSRow row(v);
      if (mvcc_snap_.IsCommitted(row.insertion_txid())) {
        v = row.row_slice();

        uint8_t *dst_row = dst->row_ptr(fetched);
        for (size_t proj_col_idx = 0; proj_col_idx < projection_mapping_.size(); proj_col_idx++) {
          size_t src_col_idx = projection_mapping_[proj_col_idx];
          void *dst_cell = dst_row + projection_.column_offset(proj_col_idx);
          const void *src_cell = v.data() + memrowset_->schema().column_offset(src_col_idx);
          RETURN_NOT_OK(projection_.column(proj_col_idx).CopyCell(dst_cell, src_cell, dst->arena()));
        }

        // Roll-forward MVCC for committed updates.
        RETURN_NOT_OK(ApplyMutationsToProjectedRow(
                        row.header_->mutation_head, dst_row, dst->arena()));
      } else {
        // This row was not yet committed in the current MVCC snapshot, so zero the selection
        // bit -- this causes it to not show up in any result set.
        BitmapClear(dst->selection_vector()->mutable_bitmap(), fetched);

        // In debug mode, fill the row data for easy debugging
        #ifndef NDEBUG
        OverwriteWithPattern(reinterpret_cast<char *>(dst->row_ptr(fetched)),
                             dst->schema().byte_size(),
                             "MVCCMVCCMVCCMVCCMVCCMVCC"
                             "MVCCMVCCMVCCMVCCMVCCMVCC"
                             "MVCCMVCCMVCCMVCCMVCCMVCC");
        #endif
      }

      // advance to next row
      fetched++;
    }

    return Status::OK();
  }

  virtual Status FinishBatch() {
    for (int i = 0; i < prepared_count_; i++) {
      iter_->Next();
    }
    prepared_count_ = 0;
    return Status::OK();
  }

  virtual bool HasNext() const {
    return iter_->IsValid();
  }

  const MSRow GetCurrentRow() const {
    Slice dummy, msrow_data;
    iter_->GetCurrentEntry(&dummy, &msrow_data);
    return MSRow(msrow_data);
  }

  bool Next() {
    return iter_->Next();
  }

  string ToString() const {
    return "memrowset iterator";
  }

  const Schema &schema() const {
    return projection_;
  }

 private:
  friend class MemRowSet;

  Iterator(const shared_ptr<const MemRowSet> &mrs,
           MemRowSet::MSBTIter *iter,
           const Schema &projection,
           const MvccSnapshot &mvcc_snap) :
    memrowset_(mrs),
    iter_(iter),
    projection_(projection),
    mvcc_snap_(mvcc_snap),
    prepared_count_(0),
    prepared_idx_in_leaf_(0)
  {
    // TODO: various code assumes that a newly constructed iterator
    // is pointed at the beginning of the dataset. This causes a redundant
    // seek. Could make this lazy instead, or change the semantics so that
    // a seek is required (probably the latter)
    iter_->SeekToStart();
  }

  Status ApplyMutationsToProjectedRow(Mutation *mutation_head,
                                      uint8_t *dst_row,
                                      Arena *dst_arena) {
    // Fast short-circuit the likely case of a row which was inserted and never
    // updated.
    if (PREDICT_TRUE(mutation_head == NULL)) {
      return Status::OK();
    }

    for (Mutation *mut = mutation_head;
         mut != NULL;
         mut = mut->next_) {
      if (!mvcc_snap_.IsCommitted(mut->txid_)) {
        // Transaction which wasn't committed yet in the reader's snapshot.
        continue;
      }

      // Apply the mutation.

      // TODO: this is slow, since it makes multiple passes through the rowchangelist.
      // Instead, we should keep the backwards mapping of columns.
      for (int proj_col_idx = 0; proj_col_idx < projection_mapping_.size(); proj_col_idx++) {
        RowChangeListDecoder decoder(memrowset_->schema(), mut->changelist());
        int memrowset_col_idx = projection_mapping_[proj_col_idx];
        uint8_t *dst_cell = dst_row + projection_.column_offset(proj_col_idx);
        RETURN_NOT_OK(decoder.ApplyToOneColumn(memrowset_col_idx, dst_cell, dst_arena));
      }
    }

    return Status::OK();
  }


  const shared_ptr<const MemRowSet> memrowset_;
  gscoped_ptr<MemRowSet::MSBTIter> iter_;

  // The schema for the output of this iterator.
  // This may be a reordered subset of the schema of the memrowset.
  const Schema projection_;

  // The MVCC snapshot which determines which rows and mutations are visible to
  // this iterator.
  const MvccSnapshot mvcc_snap_;

  // Mapping from projected column index back to memrowset column index.
  vector<size_t> projection_mapping_;

  size_t prepared_count_;
  size_t prepared_idx_in_leaf_;

  // Temporary local buffer used for seeking to hold the encoded
  // seek target.
  faststring tmp_buf;
};

} // namespace tablet
} // namespace kudu

#endif
