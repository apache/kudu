// Copyright (c) 2012, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/common.pb.h"
#include "common/generic_iterators.h"
#include "common/row.h"
#include "gutil/atomicops.h"
#include "tablet/memstore.h"

DEFINE_int32(memstore_throttle_mb, 0,
             "number of MB of RAM beyond which memstore inserts will be throttled");


namespace kudu { namespace tablet {

using std::pair;

static const int kInitialArenaSize = 1*1024*1024;
static const int kMaxArenaBufferSize = 4*1024*1024;

void MSRow::AppendMutation(Mutation *mut)
{
  mut->AppendToList(&header_->mutation_head);
}

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
    MSRow row = iter->GetCurrentRow();
    LOG(INFO) << "@" << row.insertion_txid() << ": row "
              << schema_.DebugRow(row.row_slice().data())
              << " mutations=" << Mutation::StringifyMutationList(schema_, row.header_->mutation_head);
    iter->Next();
  }
}


Status MemStore::Insert(txid_t txid, const Slice &data) {
  CHECK_EQ(data.size(), schema_.byte_size());

  faststring enc_key_buf;
  schema_.EncodeComparableKey(data, &enc_key_buf);
  Slice enc_key(enc_key_buf);

  // Copy the non-encoded key onto the stack since we need
  // to mutate it when we relocate its Slices into our arena.
  DEFINE_MSROW_ON_STACK(schema(), msrow, msrow_slice);
  msrow.header_->insertion_txid = txid;
  msrow.header_->mutation_head = NULL;
  uint8_t *rowdata_ptr = msrow.row_slice_.mutable_data();
  memcpy(rowdata_ptr, data.data(), data.size());

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
  RETURN_NOT_OK(kudu::CopyRowIndirectDataToArena(rowdata_ptr, schema_, &arena_));

  CHECK(mutation.Insert(msrow_slice))
    << "Expected to be able to insert, since the prepared mutation "
    << "succeeded!";

  debug_insert_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemStore::UpdateRow(txid_t txid,
                           const void *key,
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

  Mutation *mut = Mutation::CreateInArena(&arena_, txid, delta);

  // Append to the linked list of mutations for this row.
  MSRow row(mutation.current_mutable_value());
  row.AppendMutation(mut);

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

MemStore::Iterator *MemStore::NewIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const {
  // TODO: take into account 'snap'
  return new MemStore::Iterator(shared_from_this(), tree_.NewIterator(),
                                projection, snap);
}

MemStore::Iterator *MemStore::NewIterator() const {
  // TODO: can we kill this function? should be only used by tests?
  return NewIterator(schema(), MvccSnapshot::CreateSnapshotIncludingAllTransactions());
}

RowwiseIterator *MemStore::NewRowIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const{
  return NewIterator(projection, snap);
}

} // namespace tablet
} // namespace kudu
