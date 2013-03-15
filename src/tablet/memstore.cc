// Copyright (c) 2012, Cloudera, inc.

#include <boost/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/common.pb.h"
#include "common/row.h"
#include "gutil/atomicops.h"
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
  debug_update_count_(0),
  has_logged_throttling_(0)
{}

void MemStore::DebugDump() {
  gscoped_ptr<Iterator> iter(NewIterator());
  while (iter->HasNext()) {
    Slice k, v;
    LOG(INFO) << "row " << schema_.DebugRow(iter->GetCurrentRow().data());
    iter->Next();
  }
}


Status MemStore::Insert(const Slice &data) {
  CHECK_EQ(data.size(), schema_.byte_size());

  faststring enc_key_buf;
  schema_.EncodeComparableKey(data, &enc_key_buf);
  Slice enc_key(enc_key_buf);

  // Copy the non-encoded key onto the stack since we need
  // to mutate it when we relocate its Slices into our arena.
  uint8_t row_copy[schema_.byte_size()];
  memcpy(row_copy, data.data(), data.size());

  btree::PreparedMutation<btree::BTreeTraits> mutation(enc_key);
  mutation.Prepare(&tree_);

  // TODO: for now, the key ends up stored doubly --
  // once encoded in the btree key, and again in the value
  // (unencoded).
  // That's not very memory-efficient!

  if (mutation.exists()) {
    return Status::AlreadyPresent("entry already present in memstore");
  }

  // Copy any referred-to memory to arena.
  RETURN_NOT_OK(kudu::CopyRowIndirectDataToArena(row_copy, schema_, &arena_));

  CHECK(mutation.Insert(Slice(row_copy, data.size())))
    << "Expected to be able to insert, since the prepared mutation "
    << "succeeded!";

  debug_insert_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemStore::UpdateRow(const void *key,
                           const RowChangeList &delta) {
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
  RowChangeListDecoder decoder(schema_, delta.slice());
  decoder.ApplyRowUpdate(&existing, &arena_);

  debug_update_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemStore::CheckRowPresent(const LayerKeyProbe &probe, bool *present) const {
  *present = tree_.ContainsKey(probe.encoded_key());
  return Status::OK();
}

void MemStore::SlowMutators() {
  if (FLAGS_memstore_throttle_mb == 0) return;

  ssize_t over_mem = memory_footprint() - FLAGS_memstore_throttle_mb * 1024 * 1024;
  if (over_mem > 0) {
    if (!has_logged_throttling_ &&
        base::subtle::NoBarrier_AtomicExchange(&has_logged_throttling_, 1) == 0) {
      LOG(WARNING) << "Throttling memstore insert rate";
    }

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
