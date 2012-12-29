// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_MEMSTORE_H
#define KUDU_TABLET_MEMSTORE_H

#include <boost/noncopyable.hpp>

#include "tablet/concurrent_btree.h"
#include "tablet/rowdelta.h"
#include "common/schema.h"
#include "util/memory/arena.h"
#include "util/status.h"

namespace kudu {
namespace tablet {


// In-memory storage for data currently being written to the tablet.
// This is a holding area for inserts, currently held in row form
// (i.e not columnar)
//
// The data is kept sorted.
//
// TODO: evaluate whether it makes sense to support non-sorted
// or lazily sorted storage.
class MemStore : boost::noncopyable {
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

  // Return the number of entries in the memstore.
  // NOTE: this requires iterating all data, and is thus
  // not very fast.
  size_t entry_count() const {
    return tree_.count();
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
  // TODO: clarify the consistency of this iterator in the method doc
  Iterator *NewIterator() const;

  // Return the Schema for the rows in this memstore.
  const Schema &schema() const {
    return schema_;
  }

  // Dump the contents of the memstore to the INFO log.
  // This dumps every row, so should only be used in tests, etc
  void DebugDump();

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
};

// An iterator through in-memory data stored in a MemStore.
// This holds a reference to the MemStore, and so the memstore
// must not be freed.
//
// Additionally, currently, this iterator cannot survive mutations
// to the underlying store.
//
// TODO: swap out the STL set with the leveldb skiplist, so we can
// do snapshot iteration while updating
class MemStore::Iterator : boost::noncopyable {
public:

  Status SeekAtOrAfter(const Slice &key, bool *exact) {
    tmp_buf.clear();
    memstore_->schema().EncodeComparableKey(key, &tmp_buf);

    if (iter_->SeekAtOrAfter(Slice(tmp_buf), exact)) {
      return Status::OK();
    } else {
      return Status::NotFound("no match in memstore");
    }
  }

  const Slice GetCurrentRow() const {
    Slice dummy, ret;
    iter_->GetCurrentEntry(&dummy, &ret);
    return ret;
  }

  bool IsValid() const {
    return iter_->IsValid();
  }

  bool Next() {
    return iter_->Next();
  }

  void SeekToStart() {
    iter_->SeekToStart();
  }

private:
  friend class MemStore;

  Iterator(const MemStore *ms,
           MemStore::MSBTIter *iter) :
    memstore_(ms),
    iter_(iter)
  {
    // TODO: various code assumes that a newly constructed iterator
    // is pointed at the beginning of the dataset. This causes a redundant
    // seek. Could make this lazy instead, or change the semantics so that
    // a seek is required (probably the latter)
    SeekToStart();
  }

  const MemStore *memstore_;
  scoped_ptr<MemStore::MSBTIter> iter_;

  // Temporary local buffer used for seeking to hold the encoded
  // seek target.
  faststring tmp_buf;
};

} // namespace tablet
} // namespace kudu

#endif
