// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_MEMSTORE_H
#define KUDU_TABLET_MEMSTORE_H

#include <boost/noncopyable.hpp>
#include <tr1/memory>

#include "tablet/concurrent_btree.h"
#include "tablet/rowdelta.h"
#include "tablet/layer-interfaces.h"
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
  // have been copied into this MemStore's internal Arena, and thus
  // the provided memory buffer may safely be re-used or freed.
  //
  // Returns Status::OK unless allocation fails.
  Status Insert(const Slice &data);


  // Update an existing row in the memstore.
  //
  // Returns Status::NotFound if the row doesn't exist.
  Status UpdateRow(const void *key,
                   const RowDelta &update);

  // Return true if the given key is contained in the memstore.
  // TODO: unit test me
  bool ContainsRow(const void *key) const;

  // Return the number of entries in the memstore.
  // NOTE: this requires iterating all data, and is thus
  // not very fast.
  size_t entry_count() const {
    return tree_.count();
  }

  // Return true if there are no entries in the memstore.
  bool empty() const {
    return tree_.empty();
  }

  // Return the memory footprint of this memstore.
  // Note that this may be larger than the sum of the data
  // inserted into the memstore, due to arena and data structure
  // overhead.
  size_t memory_footprint() const {
    // TODO: need to make cbtree use arena so this is accurate
    return arena_.memory_footprint();
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

  // Return the Schema for the rows in this memstore.
  const Schema &schema() const {
    return schema_;
  }

  // Dump the contents of the memstore to the INFO log.
  // This dumps every row, so should only be used in tests, etc
  void DebugDump();

  uint64_t debug_insert_count() const {
    return debug_insert_count_;
  }

private:
  friend class Iterator;

  // Copy the given row into the local Arena.
  // This copies both the row data (primitives) as well as any
  // indirect data (eg string types).
  //
  // Returns Status::OK() and sets *dst to point at the copied
  // row unless allocation fails.
  Status CopyRowToArena(const Slice &row, Slice *dst);

  typedef btree::CBTree<btree::BTreeTraits> MSBTree;

  const Schema schema_;
  Arena arena_;

  typedef btree::CBTreeIterator<btree::BTreeTraits> MSBTIter;

  MSBTree tree_;

  // Approximate count of insertions. This variable is updated non-atomically,
  // so it cannot be relied upon to be in any way accurate. It's only used
  // as a sanity check during flush.
  volatile uint64_t debug_insert_count_;
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

  virtual Status SeekAtOrAfter(const Slice &key, bool *exact) {
    CHECK(!projection_mapping_.empty()) << "not initted";

    tmp_buf.clear();
    memstore_->schema().EncodeComparableKey(key, &tmp_buf);

    if (iter_->SeekAtOrAfter(Slice(tmp_buf), exact)) {
      return Status::OK();
    } else {
      return Status::NotFound("no match in memstore");
    }
  }

  virtual Status CopyNextRows(size_t *nrows,
                              uint8_t *dst,
                              Arena *dst_arena) {
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
        void *dst_cell = dst + projection_.column_offset(proj_col_idx);
        const void *src_cell = s.data() + memstore_->schema().column_offset(src_col_idx);
        RETURN_NOT_OK(projection_.column(proj_col_idx).CopyCell(dst_cell, src_cell, dst_arena));
      }

      // advance to next row
      fetched++;
      dst += projection_.byte_size();
      Next();
    }

    *nrows = fetched;

    return Status::OK();
  }


  virtual bool HasNext() const {
    return iter_->IsValid();
  }

  void SeekToStart() {
    iter_->SeekToStart();
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
    SeekToStart();
  }


  const shared_ptr<const MemStore> memstore_;
  scoped_ptr<MemStore::MSBTIter> iter_;

  const Schema projection_;
  vector<size_t> projection_mapping_;

  // Temporary local buffer used for seeking to hold the encoded
  // seek target.
  faststring tmp_buf;
};

} // namespace tablet
} // namespace kudu

#endif
