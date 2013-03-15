// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_MEMSTORE_H
#define KUDU_TABLET_MEMSTORE_H

#include <boost/noncopyable.hpp>
#include <tr1/memory>

#include "tablet/concurrent_btree.h"
#include "tablet/layer-interfaces.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "util/memory/arena.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

// In-memory storage for data currently being written to the tablet.
// This is a holding area for inserts, currently held in row form
// (i.e not columnar)
//
// The data is kept sorted.
//
// TODO: evaluate whether it makes sense to support non-sorted
// or lazily sorted storage.
class MemStore : boost::noncopyable,
                 public LayerInterface,
                 public std::tr1::enable_shared_from_this<MemStore> {
public:
  class Iterator;

  explicit MemStore(const Schema &schema);


  // Insert a new row into the memstore.
  //
  // The provided 'data' slice should have length equivalent to this
  // memstore's Schema.byte_size().
  //
  // After insert, the row and any referred-to memory (eg for strings)
  // have been copied into this MemStore's internal storage, and thus
  // the provided memory buffer may safely be re-used or freed.
  //
  // Returns Status::OK unless allocation fails.
  Status Insert(const Slice &data);


  // Update an existing row in the memstore.
  //
  // Returns Status::NotFound if the row doesn't exist.
  Status UpdateRow(const void *key,
                   const RowChangeList &update);

  // Return the number of entries in the memstore.
  // NOTE: this requires iterating all data, and is thus
  // not very fast.
  size_t entry_count() const {
    return tree_.count();
  }

  // Conform entry_count to LayerInterface
  Status CountRows(size_t *count) const {
    *count = entry_count();
    return Status::OK();
  }

  uint64_t EstimateOnDiskSize() const {
    return 0;
  }

  boost::mutex *compact_flush_lock() {
    return &compact_flush_lock_;
  }

  // Return true if there are no entries in the memstore.
  bool empty() const {
    return tree_.empty();
  }

  // TODO: unit test me
  Status CheckRowPresent(const LayerKeyProbe &probe, bool *present) const;

  // Return the memory footprint of this memstore.
  // Note that this may be larger than the sum of the data
  // inserted into the memstore, due to arena and data structure
  // overhead.
  size_t memory_footprint() const {
    // TODO: merge the two into the same arena?
    return arena_.memory_footprint() + tree_.estimate_memory_usage();
  }

  // Return an iterator over the items in this memstore.
  //
  // NOTE: for this function to work, there must be a shared_ptr
  // referring to this MemStore. Otherwise, this will throw
  // a C++ exception and all bets are off.
  //
  // TODO: clarify the consistency of this iterator in the method doc
  Iterator *NewIterator() const;
  Iterator *NewIterator(const Schema &projection) const;

  // Alias to conform to Layer interface
  RowIteratorInterface *NewRowIterator(const Schema &projection) const;

  // Return the Schema for the rows in this memstore.
  const Schema &schema() const {
    return schema_;
  }

  // Dump the contents of the memstore to the INFO log.
  // This dumps every row, so should only be used in tests, etc
  void DebugDump();

  string ToString() const {
    return string("memstore");
  }

  Status Delete() {
    CHECK(0) << "Cannot Delete a memstore!";
    return Status::NotSupported("Delete of MemStore not supported");
  }

  // Mark the memstore as frozen. See CBTree::Freeze()
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

  // Temporary hack to slow down mutators when the memstore is over 1GB.
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

// An iterator through in-memory data stored in a MemStore.
// This holds a reference to the MemStore, and so the memstore
// must not be freed while this iterator is outstanding.
//
// This iterator is not a full snapshot, but individual rows
// are consistent, and it is safe to iterate during concurrent
// mutation. The consistency guarantee is that it will return
// at least all rows that were present at the time of construction,
// and potentially more. Each row will be at least as current as
// the time of construction, and potentially more current.
class MemStore::Iterator : public RowIteratorInterface, boost::noncopyable {
public:
  virtual Status Init() {
    RETURN_NOT_OK(projection_.GetProjectionFrom(
                    memstore_->schema(), &projection_mapping_));

    return Status::OK();
  }

  Status SeekAtOrAfter(const Slice &key, bool *exact) {
    CHECK(!projection_mapping_.empty()) << "not initted";

    if (key.size() > 0) {
      tmp_buf.clear();
      memstore_->schema().EncodeComparableKey(key, &tmp_buf);
    } else {
      // Seeking to empty key shouldn't try to run any encoding.
      tmp_buf.resize(0);
    }

    if (iter_->SeekAtOrAfter(Slice(tmp_buf), exact) ||
        key.size() == 0) {
      return Status::OK();
    } else {
      return Status::NotFound("no match in memstore");
    }
  }

  virtual Status CopyNextRows(size_t *nrows, RowBlock *dst) {
    // TODO: add dcheck that dst->schema() matches our schema
    // also above TODO applies to a lot of other CopyNextRows cases
    DCHECK(!projection_mapping_.empty()) << "not initted";
    DCHECK_GT(*nrows, 0);
    if (!HasNext()) {
      return Status::NotFound("no more rows");
    }

    size_t fetched = 0;
    for (size_t i = 0; i < *nrows && HasNext(); i++) {
      // Copy the row into the destination, including projection
      // and relocating slices.
      // TODO: can we share some code here with CopyRowToArena() from row.h
      // or otherwise put this elsewhere?
      Slice s = GetCurrentRow();
      for (size_t proj_col_idx = 0; proj_col_idx < projection_mapping_.size(); proj_col_idx++) {
        size_t src_col_idx = projection_mapping_[proj_col_idx];
        void *dst_cell = dst->row_ptr(fetched) + projection_.column_offset(proj_col_idx);
        const void *src_cell = s.data() + memstore_->schema().column_offset(src_col_idx);
        RETURN_NOT_OK(projection_.column(proj_col_idx).CopyCell(dst_cell, src_cell, dst->arena()));
      }

      // advance to next row
      fetched++;
      Next();
    }

    *nrows = fetched;

    return Status::OK();
  }


  virtual bool HasNext() const {
    return iter_->IsValid();
  }

  const Slice GetCurrentRow() const {
    Slice dummy, ret;
    iter_->GetCurrentEntry(&dummy, &ret);
    return ret;
  }

  bool Next() {
    return iter_->Next();
  }

  string ToString() const {
    return "memstore iterator";
  }

  const Schema &schema() const {
    return projection_;
  }

private:
  friend class MemStore;

  Iterator(const shared_ptr<const MemStore> &ms,
           MemStore::MSBTIter *iter,
           const Schema &projection) :
    memstore_(ms),
    iter_(iter),
    projection_(projection)
  {
    // TODO: various code assumes that a newly constructed iterator
    // is pointed at the beginning of the dataset. This causes a redundant
    // seek. Could make this lazy instead, or change the semantics so that
    // a seek is required (probably the latter)
    iter_->SeekToStart();
  }


  const shared_ptr<const MemStore> memstore_;
  gscoped_ptr<MemStore::MSBTIter> iter_;

  const Schema projection_;
  vector<size_t> projection_mapping_;

  // Temporary local buffer used for seeking to hold the encoded
  // seek target.
  faststring tmp_buf;
};

} // namespace tablet
} // namespace kudu

#endif
