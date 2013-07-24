// Copyright (c) 2012, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <string>
#include <vector>

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

bool MRSRow::IsGhost() const {
  bool is_ghost = false;
  for (const Mutation *mut = header_->mutation_head;
       mut != NULL;
       mut = mut->next()) {
    RowChangeListDecoder decoder(schema(), mut->changelist());
    Status s = decoder.Init();
    if (!PREDICT_TRUE(s.ok())) {
      LOG(FATAL) << "Failed to decode: " << mut->changelist().ToString(schema())
                  << " (" << s.ToString() << ")";
    }
    if (decoder.is_delete()) {
      DCHECK(!is_ghost);
      is_ghost = true;
    } else if (decoder.is_reinsert()) {
      DCHECK(is_ghost);
      is_ghost = false;
    }
  }
  return is_ghost;
}


MemRowSet::MemRowSet(const Schema &schema)
  : schema_(schema),
    arena_(kInitialArenaSize, kMaxArenaBufferSize),
    debug_insert_count_(0),
    debug_update_count_(0),
    has_logged_throttling_(0) {
}

Status MemRowSet::DebugDump(vector<string> *lines) {
  gscoped_ptr<Iterator> iter(NewIterator());
  while (iter->HasNext()) {
    MRSRow row = iter->GetCurrentRow();
    LOG_STRING(INFO, lines)
      << "@" << row.insertion_txid() << ": row "
      << schema_.DebugRow(row)
      << " mutations=" << Mutation::StringifyMutationList(schema_, row.header_->mutation_head)
      << std::endl;
    iter->Next();
  }

  return Status::OK();
}


Status MemRowSet::Insert(txid_t txid, const ConstContiguousRow& row) {
  // TODO: Handle different schema
  DCHECK(schema_.Equals(row.schema()));

  faststring enc_key_buf;
  schema_.EncodeComparableKey(row, &enc_key_buf);
  Slice enc_key(enc_key_buf);

  btree::PreparedMutation<btree::BTreeTraits> mutation(enc_key);
  mutation.Prepare(&tree_);

  // TODO: for now, the key ends up stored doubly --
  // once encoded in the btree key, and again in the value
  // (unencoded).
  // That's not very memory-efficient!

  if (mutation.exists()) {
    // It's OK for it to exist if it's just a "ghost" row -- i.e the
    // row is deleted.
    MRSRow ms_row(this, mutation.current_mutable_value());
    if (!ms_row.IsGhost()) {
      return Status::AlreadyPresent("entry already present in memrowset");
    }

    // Insert a "reinsert" mutation.
    return Reinsert(txid, row, &ms_row);
  }

  // Copy the non-encoded key onto the stack since we need
  // to mutate it when we relocate its Slices into our arena.
  DEFINE_MRSROW_ON_STACK(this, mrsrow, mrsrow_slice);
  mrsrow.header_->insertion_txid = txid;
  mrsrow.header_->mutation_head = NULL;
  mrsrow.CopyCellsFrom(row);

  // Copy any referred-to memory to arena.
  RETURN_NOT_OK(kudu::CopyRowIndirectDataToArena(&mrsrow, &arena_));

  CHECK(mutation.Insert(mrsrow_slice))
    << "Expected to be able to insert, since the prepared mutation "
    << "succeeded!";

  debug_insert_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemRowSet::Reinsert(txid_t txid, const ConstContiguousRow& row, MRSRow *ms_row) {

  // TODO(perf): This path makes some unnecessary copies that could be reduced,
  // but let's assume that REINSERT is really rare and code for clarity over speed
  // here.

  // Make a copy of the row, and relocate any of its indirected data into
  // our Arena.
  uint8_t row_copy_buf[row.row_size()];
  memcpy(row_copy_buf, row.row_data(), row.row_size());
  ContiguousRow row_copy(schema_, row_copy_buf);
  RETURN_NOT_OK(CopyRowIndirectDataToArena(&row_copy, &arena_));

  // Encode the REINSERT mutation from the relocated row copy.
  faststring buf;
  RowChangeListEncoder encoder(schema_, &buf);
  encoder.SetToReinsert(Slice(row_copy_buf, row.row_size()));

  // Move the REINSERT mutation itself into our Arena.
  Mutation *mut = Mutation::CreateInArena(&arena_, txid, encoder.as_changelist());

  // Ensure that all of the creation of the mutation is published before
  // publishing the pointer itself.
  // We don't need to do a CAS or anything since the CBTree code has already
  // locked the relevant leaf node from concurrent writes.
  base::subtle::MemoryBarrier();

  // Append the mutation into the row's mutation list.
  mut->AppendToList(&ms_row->header_->mutation_head);
  return Status::OK();
}

Status MemRowSet::MutateRow(txid_t txid,
                            const RowSetKeyProbe &probe,
                            const RowChangeList &delta) {
  {
    btree::PreparedMutation<btree::BTreeTraits> mutation(probe.encoded_key());
    mutation.Prepare(&tree_);

    if (!mutation.exists()) {
      return Status::NotFound("not in memrowset");
    }

    MRSRow row(this, mutation.current_mutable_value());

    // If the row exists, it may still be a "ghost" row -- i.e a row
    // that's been deleted. If that's the case, we should not treat it as
    // NotFound.
    if (row.IsGhost()) {
      return Status::NotFound("not in memrowset (ghost)");
    }


    // Append to the linked list of mutations for this row.
    Mutation *mut = Mutation::CreateInArena(&arena_, txid, delta);

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
  // Use a PreparedMutation here even though we don't plan to mutate. Even though
  // this takes a lock rather than an optimistic copy, it should be a very short
  // critical section, and this call is only made on updates, which are rare.

  btree::PreparedMutation<btree::BTreeTraits> mutation(probe.encoded_key());
  mutation.Prepare(const_cast<MSBTree *>(&tree_));

  if (!mutation.exists()) {
    *present = false;
    return Status::OK();
  }

  // TODO(perf): using current_mutable_value() will actually change the data's
  // version number, even though we're not going to do any mutation. This would
  // make concurrent readers retry, even though they don't have to (we aren't
  // actually mutating anything here!)
  MRSRow row(this, mutation.current_mutable_value());

  // If the row exists, it may still be a "ghost" row -- i.e a row
  // that's been deleted. If that's the case, we should not treat it as
  // NotFound.
  *present = !row.IsGhost();
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
                                           const MvccSnapshot &snap) const {
  return NewIterator(projection, snap);
}

CompactionInput *MemRowSet::NewCompactionInput(const MvccSnapshot &snap) const  {
  return CompactionInput::Create(*this, snap);
}

Status MemRowSet::GetBounds(Slice *min_encoded_key,
                            Slice *max_encoded_key) const {
  return Status::NotSupported("");
}

} // namespace tablet
} // namespace kudu
