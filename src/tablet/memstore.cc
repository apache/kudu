// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include "cfile/cfile.pb.h"
#include "tablet/memstore.h"
#include "tablet/row.h"

namespace kudu { namespace tablet {

using std::pair;

static const int kInitialArenaSize = 1*1024*1024;
static const int kMaxArenaBufferSize = 4*1024*1024;

MemStore::EntryComparator::EntryComparator(const MemStore &store) :
  store_(store)
{}

int MemStore::EntryComparator::operator()(
  const Entry &lhs,
  const Entry &rhs) const {

  return store_.schema_.Compare(lhs.data, rhs.data);
}

MemStore::MemStore(const Schema &schema) :
  schema_(schema),
  arena_(kInitialArenaSize, kMaxArenaBufferSize),
  entries_(EntryComparator(*this),
           ArenaAllocator<Entry>(&arena_))
{}

Status MemStore::CopyRowToArena(const Slice &row,
                                Slice *copied) {
  return kudu::tablet::CopyRowToArena(row, schema_, &arena_, copied);
}

Status MemStore::Insert(const Slice &data) {
  CHECK_EQ(data.size(), schema_.byte_size());

  // Copy the row and any referred-to memory to arena
  Slice copied;
  RETURN_NOT_OK(CopyRowToArena(data, &copied));

  Entry new_entry(copied.data());
  pair<MSSet::iterator, bool> ret = entries_.insert(new_entry);
  if (!ret.second) {
    // Already exists with same key; should replace entry.
    MSSet::iterator it = ret.first;
    entries_.erase(it++);
    // Use the "hinted" insertion:
    // http://gcc.gnu.org/onlinedocs/libstdc++/manual/bk01pt07ch17.html
    entries_.insert(it, new_entry);
  }
  return Status::OK();
}


MemStore::Iterator *MemStore::NewIterator() const {
  return new MemStore::Iterator(this);
}

} // namespace tablet
} // namespace kudu
