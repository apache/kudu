// Copyright (c) 2012, Cloudera, inc.

#include <boost/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/common.pb.h"
#include "common/row.h"
#include "tablet/memstore.h"

DEFINE_int32(memstore_throttle_mb, 0,
             "number of MB of RAM beyond which memstore inserts will be throttled");


namespace kudu { namespace tablet {

using std::pair;

static const int kInitialArenaSize = 1*1024*1024;
static const int kMaxArenaBufferSize = 4*1024*1024;

MemStore::MemStore(const Schema &schema) :
  schema_(schema),
  arena_(kInitialArenaSize, kMaxArenaBufferSize),
  debug_insert_count_(0),
  debug_update_count_(0)
{}

void MemStore::DebugDump() {
  scoped_ptr<Iterator> iter(NewIterator());
  while (iter->HasNext()) {
    Slice k, v;
    LOG(INFO) << "row " << schema_.DebugRow(iter->GetCurrentRow().data());
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

  debug_insert_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemStore::UpdateRow(const void *key,
                           const RowDelta &delta) {
  Slice unencoded_key_slice(reinterpret_cast<const uint8_t *>(key),
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

  debug_update_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemStore::CheckRowPresent(const void *key, bool *present) const {
  Slice unencoded_key_slice(reinterpret_cast<const uint8_t *>(key),
                            schema_.key_byte_size());

  faststring key_buf;
  schema_.EncodeComparableKey(unencoded_key_slice, &key_buf);
  Slice encoded_key_slice(key_buf);

  *present = tree_.ContainsKey(encoded_key_slice);
  return Status::OK();
}

void MemStore::SlowMutators() {
  if (FLAGS_memstore_throttle_mb == 0) return;

  ssize_t over_mem = memory_footprint() - FLAGS_memstore_throttle_mb * 1024 * 1024;
  if (over_mem > 0) {
    size_t us_to_sleep = over_mem / 1024 / 512;
    boost::this_thread::sleep(boost::posix_time::microseconds(us_to_sleep));
  }
}

MemStore::Iterator *MemStore::NewIterator(const Schema &projection) const {
  return new MemStore::Iterator(shared_from_this(), tree_.NewIterator(), projection);
}

MemStore::Iterator *MemStore::NewIterator() const {
  return NewIterator(schema());
}

RowIteratorInterface *MemStore::NewRowIterator(const Schema &projection) const {
  return NewIterator(projection);
}


} // namespace tablet
} // namespace kudu
