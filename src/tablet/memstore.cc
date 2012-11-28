// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include "common/common.pb.h"
#include "common/row.h"
#include "tablet/memstore.h"

namespace kudu { namespace tablet {

using std::pair;

static const int kInitialArenaSize = 1*1024*1024;
static const int kMaxArenaBufferSize = 4*1024*1024;

MemStore::EntryLessThan::EntryLessThan(const MemStore &store) :
  store_(store)
{}

bool MemStore::EntryLessThan::operator()(
  const Entry &lhs,
  const Entry &rhs) const {

  return store_.schema_.Compare(lhs.data, rhs.data) < 0;
}

MemStore::MemStore(const Schema &schema) :
  schema_(schema),
  arena_(kInitialArenaSize, kMaxArenaBufferSize),
  entries_(EntryLessThan(*this), ArenaAllocator<Entry>(&arena_))
{}

Status MemStore::CopyRowToArena(const Slice &row,
                                Slice *copied) {
  return kudu::CopyRowToArena(row, schema_, &arena_, copied);
}

Status MemStore::Insert(const Slice &data) {
  CHECK_EQ(data.size(), schema_.byte_size());


  pair<MSSet::iterator, MSSet::iterator> range =
    entries_.equal_range(Entry(data.data()));

  if (range.first != range.second) {
    // Entry was found in the set already
    return Status::AlreadyPresent("entry already present in memstore");
  }

  // Otherwise, insert it.
  // Copy the row and any referred-to memory to arena
  Slice copied;
  RETURN_NOT_OK(CopyRowToArena(data, &copied));
  Entry new_entry(copied.data());

  // For a non-present row, the equal_range function returns a range
  // which consists of the element following the searched-for element.
  // So, iterate it backwards one to serve as the "insertion hint" --
  // this makes the insertion O(1) at this point.
  MSSet::iterator insertion_hint = range.first;
  if (insertion_hint != entries_.begin()) {
    --insertion_hint;
  }

  entries_.insert(insertion_hint, new_entry);
  return Status::OK();
}

Status MemStore::UpdateRow(const void *key,
                           const RowDelta &delta) {
  MSSet::iterator it = entries_.find(Entry(key));
  if (it == entries_.end()) {
    return Status::NotFound("not in memstore");
  }

  Entry existing = *it;

  // Ugly: remove constness of the set member.
  // We know that the data here is owned by the memstore, so it's safe
  // to mutate, but unfortunately we can't make the Entry non-const, since
  // then we couldn't use const keys for _lookups_.
  void *data = const_cast<void *>(existing.data);

  delta.ApplyRowUpdate(schema_, data, &arena_);
  return Status::OK();
}


MemStore::Iterator *MemStore::NewIterator() const {
  return new MemStore::Iterator(this);
}

} // namespace tablet
} // namespace kudu
