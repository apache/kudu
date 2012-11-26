// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_MEMSTORE_H
#define KUDU_TABLET_MEMSTORE_H

#include <boost/noncopyable.hpp>
#include <set>

#include "tablet/rowdelta.h"
#include "common/schema.h"
#include "util/memory/arena.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

using std::set;


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

  size_t entry_count() {
    return entries_.size();
  }

  // Return the memory footprint of this memstore.
  // Note that this may be larger than the sum of the data
  // inserted into the memstore, due to arena and data structure
  // overhead.
  size_t memory_footprint() const {
    return arena_.memory_footprint();
  }

  // Return an iterator over the items in this memstore.
  // NOTE: currently this iterator is not safe with regard to
  // concurrent modifications.
  Iterator *NewIterator() const;

  // Return the Schema for the rows in this memstore.
  const Schema &schema() const {
    return schema_;
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

  // One of the entries in the MemStore.
  // Currently just a simple wrapper around a pointer.
  struct Entry {
    explicit Entry(const void *data_) :
      data(data_)
    {}

    const void *data;
  };

  struct EntryComparator {
    EntryComparator(const MemStore &store);
    int operator() (const Entry &lhs,
                    const Entry &rhs) const;

    const MemStore &store_;
  };

  typedef set<Entry, EntryComparator, ArenaAllocator<Entry> > MSSet;


  MSSet::const_iterator IteratorAtOrAfter(const Slice &key,
                                          bool *exact) const {
    CHECK_GE(key.size(), schema().key_byte_size());

    pair<MSSet::const_iterator, MSSet::const_iterator> range =
      entries_.equal_range(Entry(key.data()));
    *exact = (range.first != range.second);
    return range.first;
  }


  const Schema schema_;
  Arena arena_;

  // Note: important that entries_ be declared below arena_
  // since entries_ uses the arena's allocator. This ensures
  // proper destructor order.
  MSSet entries_;
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
    iter_ = memstore_->IteratorAtOrAfter(key, exact);
    return Status::OK();
  }

  const Slice GetCurrentRow() const {
    return Slice(reinterpret_cast<const char *>((*iter_).data),
                 memstore_->schema().byte_size());
  }

  bool IsValid() const {
    return iter_ != memstore_->entries_.end();
  }

  bool Next() {
    ++iter_;
    return IsValid();
  }

private:
  friend class MemStore;

  Iterator(const MemStore *ms) :
    memstore_(ms),
    iter_(ms->entries_.begin())
  {
  }

  const MemStore *memstore_;
  MemStore::MSSet::const_iterator iter_;

};

} // namespace tablet
} // namespace kudu

#endif
