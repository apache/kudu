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


MemRowSet::MemRowSet(int64_t id,
                     const Schema &schema)
  : id_(id),
    schema_(schema),
    arena_(kInitialArenaSize, kMaxArenaBufferSize),
    debug_insert_count_(0),
    debug_update_count_(0),
    has_logged_throttling_(0) {
  CHECK(schema.has_column_ids());
}

Status MemRowSet::AlterSchema(const Schema& schema) {
  // The MemRowSet is flushed and re-created with the new Schema.
  // See Tablet::AlterSchema()
  return Status::NotSupported("AlterSchema not supported by MemRowSet");
}

Status MemRowSet::DebugDump(vector<string> *lines) {
  gscoped_ptr<Iterator> iter(NewIterator());
  RETURN_NOT_OK(iter->Init(NULL));
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


Status MemRowSet::Insert(txid_t txid,
                         const ConstContiguousRow& row) {
  CHECK(row.schema().has_column_ids());
  DCHECK_SCHEMA_EQ(schema_, row.schema());

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
  RETURN_NOT_OK(mrsrow.CopyRow(row, &arena_));

  CHECK(mutation.Insert(mrsrow_slice))
    << "Expected to be able to insert, since the prepared mutation "
    << "succeeded!";

  debug_insert_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemRowSet::Reinsert(txid_t txid, const ConstContiguousRow& row, MRSRow *ms_row) {
  DCHECK_SCHEMA_EQ(schema_, row.schema());

  // TODO(perf): This path makes some unnecessary copies that could be reduced,
  // but let's assume that REINSERT is really rare and code for clarity over speed
  // here.

  // Make a copy of the row, and relocate any of its indirected data into
  // our Arena.
  DEFINE_MRSROW_ON_STACK(this, row_copy, row_copy_slice);
  RETURN_NOT_OK(row_copy.CopyRow(row, &arena_));

  // Encode the REINSERT mutation from the relocated row copy.
  faststring buf;
  RowChangeListEncoder encoder(schema_, &buf);
  encoder.SetToReinsert(row_copy.row_slice());

  // Move the REINSERT mutation itself into our Arena.
  Mutation *mut = Mutation::CreateInArena(&arena_, txid, encoder.as_changelist());

  // Append the mutation into the row's mutation list.
  // This function has "release" semantics which ensures that the memory writes
  // for the mutation are fully published before any concurrent reader sees
  // the appended mutation.
  mut->AppendToListAtomic(&ms_row->header_->mutation_head);
  return Status::OK();
}

Status MemRowSet::MutateRow(txid_t txid,
                            const RowSetKeyProbe &probe,
                            const RowChangeList &delta,
                            ProbeStats* stats,
                            MutationResultPB *result) {
  {
    btree::PreparedMutation<btree::BTreeTraits> mutation(probe.encoded_key_slice());
    mutation.Prepare(&tree_);

    if (!mutation.exists()) {
      return Status::NotFound("not in memrowset");
    }

    MRSRow row(this, mutation.current_mutable_value());

    // If the row exists, it may still be a "ghost" row -- i.e a row
    // that's been deleted. If that's the case, we should treat it as
    // NotFound.
    if (row.IsGhost()) {
      return Status::NotFound("not in memrowset (ghost)");
    }

    // Append to the linked list of mutations for this row.
    Mutation *mut = Mutation::CreateInArena(&arena_, txid, delta);

    // This function has "release" semantics which ensures that the memory writes
    // for the mutation are fully published before any concurrent reader sees
    // the appended mutation.
    mut->AppendToListAtomic(&row.header_->mutation_head);

    MutationTargetPB* target = result->add_mutations();
    target->set_mrs_id(id_);
  }

  stats->mrs_consulted++;

  // Throttle the writer if we're low on memory, but do this outside of the lock
  // so we don't slow down readers.
  debug_update_count_++;
  SlowMutators();
  return Status::OK();
}

Status MemRowSet::CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                                  ProbeStats* stats) const {
  // Use a PreparedMutation here even though we don't plan to mutate. Even though
  // this takes a lock rather than an optimistic copy, it should be a very short
  // critical section, and this call is only made on updates, which are rare.

  stats->mrs_consulted++;

  btree::PreparedMutation<btree::BTreeTraits> mutation(probe.encoded_key_slice());
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
  // that's been deleted. If that's the case, we should treat it as
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

MemRowSet::Iterator *MemRowSet::NewIterator(const Schema *projection,
                                            const MvccSnapshot &snap) const {
  return new MemRowSet::Iterator(shared_from_this(), tree_.NewIterator(),
                                projection, snap);
}

MemRowSet::Iterator *MemRowSet::NewIterator() const {
  // TODO: can we kill this function? should be only used by tests?
  return NewIterator(&schema(), MvccSnapshot::CreateSnapshotIncludingAllTransactions());
}

RowwiseIterator *MemRowSet::NewRowIterator(const Schema *projection,
                                           const MvccSnapshot &snap) const {
  return NewIterator(projection, snap);
}

CompactionInput *MemRowSet::NewCompactionInput(const Schema* projection,
                                               const MvccSnapshot &snap) const  {
  return CompactionInput::Create(*this, projection, snap);
}

Status MemRowSet::GetBounds(Slice *min_encoded_key,
                            Slice *max_encoded_key) const {
  return Status::NotSupported("");
}

Status MemRowSet::Iterator::Init(ScanSpec *spec) {
  DCHECK_EQ(state_, kUninitialized);

  RETURN_NOT_OK(projector_.Init());
  RETURN_NOT_OK(delta_projector_.Init());

  if (spec != NULL && spec->has_encoded_ranges()) {
    boost::optional<const Slice &> max_lower_bound;
    BOOST_FOREACH(const EncodedKeyRange *range, spec->encoded_ranges()) {
      if (range->has_lower_bound()) {
        bool exact;
        const Slice &lower_bound = range->lower_bound().encoded_key();
        if (!max_lower_bound.is_initialized() ||
              lower_bound.compare(*max_lower_bound) > 0) {
          if (!iter_->SeekAtOrAfter(lower_bound, &exact)) {
            // Lower bound is after the end of the key range, no rows will
            // pass the predicate so we can stop the scan right away.
            state_ = kFinished;
            return Status::OK();
          }
          max_lower_bound.reset(lower_bound);
        }
      }
      if (range->has_upper_bound()) {
        const Slice &upper_bound = range->upper_bound().encoded_key();
        if (!has_upper_bound() || upper_bound.compare(*upper_bound_) < 0) {
          upper_bound_.reset(upper_bound);
        }
      }
      if (VLOG_IS_ON(1)) {
        Schema key_schema = memrowset_->schema().CreateKeyProjection();
        VLOG_IF(1, max_lower_bound.is_initialized())
            << "Pushed MemRowSet lower bound value "
            << range->lower_bound().Stringify(key_schema);
        VLOG_IF(1, has_upper_bound())
            << "Pushed MemRowSet upper bound value "
            << range->upper_bound().Stringify(key_schema);
      }
    }
    if (max_lower_bound.is_initialized()) {
      bool exact;
      iter_->SeekAtOrAfter(*max_lower_bound, &exact);
    }
  }
  state_ = kScanning;
  return Status::OK();
}

Status MemRowSet::Iterator::SeekAtOrAfter(const Slice &key, bool *exact) {
  DCHECK_NE(state_, kUninitialized) << "not initted";

  if (key.size() > 0) {
    ConstContiguousRow row_slice(memrowset_->schema(), key);
    memrowset_->schema().EncodeComparableKey(row_slice, &tmp_buf);
  } else {
    // Seeking to empty key shouldn't try to run any encoding.
    tmp_buf.resize(0);
  }

  if (iter_->SeekAtOrAfter(Slice(tmp_buf), exact) ||
      key.size() == 0) {
    return Status::OK();
  } else {
    return Status::NotFound("no match in memrowset");
  }
}

Status MemRowSet::Iterator::PrepareBatch(size_t *nrows) {
  DCHECK_NE(state_, kUninitialized) << "not initted";
  if (PREDICT_FALSE(!iter_->IsValid())) {
    *nrows = 0;
    return Status::NotFound("end of iter");
  }

  if (PREDICT_FALSE(state_ != kScanning)) {
    // The scan is finished as we've gone past the upper bound
    *nrows = 0;
    return Status::OK();
  }

  size_t rem_in_leaf = iter_->remaining_in_leaf();
  if (PREDICT_TRUE(rem_in_leaf < *nrows)) {
    *nrows = rem_in_leaf;
  }
  prepared_count_ = *nrows;
  prepared_idx_in_leaf_ = iter_->index_in_leaf();
  if (has_upper_bound() && prepared_count_ > 0) {
    size_t end_idx = prepared_idx_in_leaf_ + prepared_count_ - 1;
    if (out_of_bounds(iter_->GetKeyInLeaf(end_idx))) {
      // At the end of this batch we will have exceeded the upper bound
      // so we will not be able to Prepare any more batches.
      state_ = kLastBatch;
    }
  }
  return Status::OK();
}

Status MemRowSet::Iterator::MaterializeBlock(RowBlock *dst) {
  // TODO: add dcheck that dst->schema() matches our schema
  // also above TODO applies to a lot of other CopyNextRows cases
  DCHECK_NE(state_, kUninitialized) << "not initted";

  dst->selection_vector()->SetAllTrue();

  DCHECK_EQ(dst->nrows(), prepared_count_);
  Slice k, v;
  size_t fetched = 0;
  for (size_t i = prepared_idx_in_leaf_; fetched < prepared_count_; i++) {
    RowBlockRow dst_row = dst->row(fetched);

    // Copy the row into the destination, including projection
    // and relocating slices.
    // TODO: can we share some code here with CopyRowToArena() from row.h
    // or otherwise put this elsewhere?
    iter_->GetEntryInLeaf(i, &k, &v);

    MRSRow row(memrowset_.get(), v);
    if (mvcc_snap_.IsCommitted(row.insertion_txid()) && state_ != kFinished) {
      if (state_ == kLastBatch && has_upper_bound() && out_of_bounds(k)) {
        state_ = kFinished;
        BitmapClear(dst->selection_vector()->mutable_bitmap(), fetched);
      } else {
        RETURN_NOT_OK(projector_.ProjectRowForRead(row, &dst_row, dst->arena()));

        // Roll-forward MVCC for committed updates.
        RETURN_NOT_OK(ApplyMutationsToProjectedRow(
            row.header_->mutation_head, &dst_row, dst->arena()));
      }
    } else {
      // This row was not yet committed in the current MVCC snapshot or we have
      // reached upper bounds of the scan, so zero the selection bit -- this
      // causes it to not show up in any result set.
      BitmapClear(dst->selection_vector()->mutable_bitmap(), fetched);

      // In debug mode, fill the row data for easy debugging
      #ifndef NDEBUG
      if (state_ != kFinished) {
        dst_row.OverwriteWithPattern("MVCCMVCCMVCCMVCCMVCCMVCC"
                                     "MVCCMVCCMVCCMVCCMVCCMVCC"
                                     "MVCCMVCCMVCCMVCCMVCCMVCC");
      }
      #endif
    }

    // advance to next row
    fetched++;
  }
  return Status::OK();
}

Status MemRowSet::Iterator::FinishBatch() {
  for (int i = 0; i < prepared_count_; i++) {
    iter_->Next();
  }
  prepared_count_ = 0;
  if (state_ == kLastBatch) {
    state_ = kFinished;
  }
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
