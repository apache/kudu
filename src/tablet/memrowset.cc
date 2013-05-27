// Copyright (c) 2012, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/common.pb.h"
#include "common/generic_iterators.h"
#include "common/row.h"
#include "gutil/atomicops.h"
#include "tablet/memrowset.h"
#include "tablet/compaction.h"

DEFINE_int32(memrowset_throttle_mb, 0,
             "number of MB of RAM beyond which memrowset inserts will be throttled");


namespace kudu { namespace tablet {

using std::pair;

static const int kInitialArenaSize = 1*1024*1024;
static const int kMaxArenaBufferSize = 4*1024*1024;

MemRowSet::MemRowSet(const Schema &schema) :
  schema_(schema),
  arena_(kInitialArenaSize, kMaxArenaBufferSize),
  debug_insert_count_(0),
  debug_update_count_(0),
  has_logged_throttling_(0)
{}

Status MemRowSet::DebugDump(vector<string> *lines) {
  gscoped_ptr<Iterator> iter(NewIterator());
  while (iter->HasNext()) {
    MRSRow row = iter->GetCurrentRow();
    LOG(INFO) << "@" << row.insertion_txid() << ": row "
              << schema_.DebugRow(row)
              << " mutations=" << Mutation::StringifyMutationList(schema_, row.header_->mutation_head);
    iter->Next();
  }

  return Status::OK();
}


Status MemRowSet::Insert(txid_t txid, const Slice &data) {
  CHECK_EQ(data.size(), schema_.byte_size());

  ConstContiguousRow row_slice(schema_, data.data());

  faststring enc_key_buf;
  schema_.EncodeComparableKey(row_slice, &enc_key_buf);
  Slice enc_key(enc_key_buf);

  // Copy the non-encoded key onto the stack since we need
  // to mutate it when we relocate its Slices into our arena.
  DEFINE_MRSROW_ON_STACK(this, mrsrow, mrsrow_slice);
  mrsrow.header_->insertion_txid = txid;
  mrsrow.header_->mutation_head = NULL;
  uint8_t *rowdata_ptr = mrsrow.row_slice_.mutable_data();
  memcpy(rowdata_ptr, data.data(), data.size());

  btree::PreparedMutation<btree::BTreeTraits> mutation(enc_key);
  mutation.Prepare(&tree_);

  // TODO: for now, the key ends up stored doubly --
  // once encoded in the btree key, and again in the value
  // (unencoded).
  // That's not very memory-efficient!

  if (mutation.exists()) {
    return Status::AlreadyPresent("entry already present in memrowset");
  }

  // Copy any referred-to memory to arena.
  RETURN_NOT_OK(kudu::CopyRowIndirectDataToArena(&mrsrow, &arena_));

  CHECK(mutation.Insert(mrsrow_slice))
    << "Expected to be able to insert, since the prepared mutation "
    << "succeeded!";

  debug_insert_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemRowSet::UpdateRow(txid_t txid,
                           const void *key,
                           const RowChangeList &delta) {
  ConstContiguousRow row_slice(schema_, key);

  faststring key_buf;
  schema_.EncodeComparableKey(row_slice, &key_buf);
  Slice encoded_key_slice(key_buf);

  {
    btree::PreparedMutation<btree::BTreeTraits> mutation(encoded_key_slice);
    mutation.Prepare(&tree_);

    if (!mutation.exists()) {
      return Status::NotFound("not in memrowset");
    }

    Mutation *mut = Mutation::CreateInArena(&arena_, txid, delta);

    // Append to the linked list of mutations for this row.
    MRSRow row(this, mutation.current_mutable_value());

    // Ensure that all of the creation of the mutation is published before
    // publishing the pointer itself.
    // We don't need to do a CAS or anything since the CBTree code has already
    // locked the relevant leaf node from concurrent writes.
    base::subtle::MemoryBarrier();
    mut->AppendToList(&row.header_->mutation_head);
  }

  // Throttle the writer if we're low on memory, but do this outside of the lock
  // so we don't slow down readers.
  debug_update_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemRowSet::CheckRowPresent(const RowSetKeyProbe &probe, bool *present) const {
  *present = tree_.ContainsKey(probe.encoded_key());
  return Status::OK();
}

void MemRowSet::SlowMutators() {
  if (FLAGS_memrowset_throttle_mb == 0) return;

  ssize_t over_mem = memory_footprint() - FLAGS_memrowset_throttle_mb * 1024 * 1024;
  if (over_mem > 0) {
    if (!has_logged_throttling_ &&
        base::subtle::NoBarrier_AtomicExchange(&has_logged_throttling_, 1) == 0) {
      LOG(WARNING) << "Throttling memrowset insert rate";
    }

    size_t us_to_sleep = over_mem / 1024 / 512;
    boost::this_thread::sleep(boost::posix_time::microseconds(us_to_sleep));
  }
}

MemRowSet::Iterator *MemRowSet::NewIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const {
  return new MemRowSet::Iterator(shared_from_this(), tree_.NewIterator(),
                                projection, snap);
}

MemRowSet::Iterator *MemRowSet::NewIterator() const {
  // TODO: can we kill this function? should be only used by tests?
  return NewIterator(schema(), MvccSnapshot::CreateSnapshotIncludingAllTransactions());
}

RowwiseIterator *MemRowSet::NewRowIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const{
  return NewIterator(projection, snap);
}

CompactionInput *MemRowSet::NewCompactionInput(const MvccSnapshot &snap) const  {
  return CompactionInput::Create(*this, snap);
}

} // namespace tablet
} // namespace kudu
