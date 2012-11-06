// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include "tablet/memstore.h"
#include "cfile/cfile.pb.h"

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
  // Copy row to arena
  if (!arena_.RelocateSlice(row, copied)) {
    return Status::IOError("unable to copy row into memstore arena");
  }

  // For any Slice columns, copy the sliced data into the arena
  // and update the pointers
  char *ptr = copied->mutable_data();
  for (int i = 0; i < schema_.num_columns(); i++) {
    if (schema_.column(i).type_info().type() == cfile::STRING) {
      Slice *slice = reinterpret_cast<Slice *>(ptr);
      Slice copied_slice;
      if (!arena_.RelocateSlice(*slice, &copied_slice)) {
        return Status::IOError("Unable to relocate slice");
      }

      *slice = copied_slice;
    }
    ptr += schema_.column(i).type_info().size();
  }
  DCHECK_EQ(ptr, copied->data() + schema_.byte_size());
  return Status::OK();
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
