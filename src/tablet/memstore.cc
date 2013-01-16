// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include "common/common.pb.h"
#include "common/row.h"
#include "tablet/memstore.h"

namespace kudu { namespace tablet {

using std::pair;

static const int kInitialArenaSize = 1*1024*1024;
static const int kMaxArenaBufferSize = 4*1024*1024;

MemStore::MemStore(const Schema &schema) :
  schema_(schema),
  arena_(kInitialArenaSize, kMaxArenaBufferSize),
  debug_mutate_count_(0)
{}

void MemStore::DebugDump() {
  scoped_ptr<Iterator> iter(NewIterator());
  while (iter->HasNext()) {
    Slice k, v;
    LOG(INFO) << "row " << iter->GetCurrentRow().data();
    iter->Next();
  }
}

Status MemStore::CopyRowToArena(const Slice &row,
                                Slice *copied) {
  return kudu::CopyRowToArena(row, schema_, &arena_, copied);
}

Status MemStore::Insert(const Slice &data) {
  CHECK_EQ(data.size(), schema_.byte_size());

  faststring key_buf;
  schema_.EncodeComparableKey(data, &key_buf);
  Slice key(key_buf);

  btree::PreparedMutation<btree::BTreeTraits> mutation(key);
  mutation.Prepare(&tree_);

  // TODO: for now, the key ends up stored doubly --
  // once encoded in the btree key, and again in the value
  // (unencoded).
  // That's not very memory-efficient!


  if (mutation.exists()) {
    return Status::AlreadyPresent("entry already present in memstore");
  }

  // Copy the row and any referred-to memory to arena.
  Slice copied;
  // TODO: don't need to copy the row key and val, just the indirect
  // data -- since the key/val parts already get copied by CBTree.
  RETURN_NOT_OK(CopyRowToArena(data, &copied));

  CHECK(mutation.Insert(copied))
    << "Expected to be able to insert, since the prepared mutation "
    << "succeeded!";

  debug_mutate_count_++;
  return Status::OK();
}

Status MemStore::UpdateRow(const void *key,
                           const RowDelta &delta) {
  Slice unencoded_key_slice(reinterpret_cast<const char *>(key),
                            schema_.key_byte_size());

  faststring key_buf;
  schema_.EncodeComparableKey(unencoded_key_slice, &key_buf);
  Slice encoded_key_slice(key_buf);

  btree::PreparedMutation<btree::BTreeTraits> mutation(encoded_key_slice);
  mutation.Prepare(&tree_);

  if (!mutation.exists()) {
    return Status::NotFound("not in memstore");
  }

  // Update the row in-place.
  Slice existing = mutation.current_mutable_value();
  delta.ApplyRowUpdate(schema_, existing.mutable_data(), &arena_);

  debug_mutate_count_++;
  return Status::OK();
}


MemStore::Iterator *MemStore::NewIterator(const Schema &projection) const {
  return new MemStore::Iterator(this, tree_.NewIterator(), projection);
}

MemStore::Iterator *MemStore::NewIterator() const {
  return NewIterator(schema());
}


} // namespace tablet
} // namespace kudu
