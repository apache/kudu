// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/memrowset.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/none_t.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/codegen/compilation_manager.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/memory.h"

DEFINE_bool(mrs_use_codegen, true, "whether the memrowset should use code "
            "generation for iteration");
TAG_FLAG(mrs_use_codegen, hidden);

using kudu::consensus::OpId;
using kudu::fs::IOContext;
using kudu::log::LogAnchorRegistry;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

static const int kInitialArenaSize = 16;

bool MRSRow::IsGhost() const {
  const Mutation *mut_tail = header_->redo_tail;
  if (mut_tail == nullptr) {
    return false;
  }
  RowChangeListDecoder decoder(mut_tail->changelist());
  Status s = decoder.Init();
  if (!PREDICT_TRUE(s.ok())) {
    LOG(FATAL) << Substitute("Failed to decode: $0 ($1)",
                             mut_tail->changelist().ToString(*schema()),
                             s.ToString());
  }
  return decoder.is_delete();
}

namespace {

shared_ptr<MemTracker> CreateMemTrackerForMemRowSet(
    int64_t id, shared_ptr<MemTracker> parent_tracker) {
  string mem_tracker_id = Substitute("MemRowSet-$0", id);
  return MemTracker::CreateTracker(-1, mem_tracker_id, std::move(parent_tracker));
}

} // anonymous namespace

Status MemRowSet::Create(int64_t id,
                         const SchemaPtr& schema,
                         LogAnchorRegistry* log_anchor_registry,
                         shared_ptr<MemTracker> parent_tracker,
                         shared_ptr<MemRowSet>* mrs) {
  auto local_mrs(MemRowSet::make_shared(
      id, schema, /*txn_id*/boost::none, /*txn_metadata*/nullptr, log_anchor_registry,
      std::move(parent_tracker)));
  *mrs = std::move(local_mrs);
  return Status::OK();
}

Status MemRowSet::Create(int64_t id,
                         const SchemaPtr& schema,
                         int64_t txn_id,
                         scoped_refptr<TxnMetadata> txn_metadata,
                         LogAnchorRegistry* log_anchor_registry,
                         shared_ptr<MemTracker> parent_tracker,
                         shared_ptr<MemRowSet>* mrs) {
  auto local_mrs(MemRowSet::make_shared(
      id, schema, boost::make_optional(txn_id), std::move(txn_metadata), log_anchor_registry,
      std::move(parent_tracker)));
  *mrs = std::move(local_mrs);
  return Status::OK();
}


MemRowSet::MemRowSet(int64_t id,
                     SchemaPtr schema,
                     boost::optional<int64_t> txn_id,
                     scoped_refptr<TxnMetadata> txn_metadata,
                     LogAnchorRegistry* log_anchor_registry,
                     shared_ptr<MemTracker> parent_tracker)
  : id_(id),
    schema_(schema),
    txn_id_(txn_id),
    txn_metadata_(std::move(txn_metadata)),
    allocator_(new MemoryTrackingBufferAllocator(
        HeapBufferAllocator::Get(),
        CreateMemTrackerForMemRowSet(id, std::move(parent_tracker)))),
    arena_(new ThreadSafeMemoryTrackingArena(kInitialArenaSize, allocator_)),
    tree_(arena_),
    debug_insert_count_(0),
    debug_update_count_(0),
    anchorer_(log_anchor_registry, Substitute("MemRowSet-$0$1", id_, txn_id_ ?
                                              Substitute("(txn_id=$0)", *txn_id) : "")),
    has_been_compacted_(false),
    live_row_count_(0) {
  CHECK(schema->has_column_ids());
  ANNOTATE_BENIGN_RACE(&debug_insert_count_, "insert count isnt accurate");
  ANNOTATE_BENIGN_RACE(&debug_update_count_, "update count isnt accurate");
}

MemRowSet::~MemRowSet() {
}

Status MemRowSet::DebugDump(vector<string> *lines) {
  unique_ptr<Iterator> iter(NewIterator());
  RETURN_NOT_OK(iter->Init(nullptr));
  while (iter->HasNext()) {
    MRSRow row = iter->GetCurrentRow();
    LOG_STRING(INFO, lines)
      << "@" << row.insertion_timestamp()
      << string(txn_id_ ?
                Substitute("(txn_id=$0$1)", *txn_id_,
                           DCHECK_NOTNULL(txn_metadata_)->commit_timestamp() ?
                               Substitute("@$0", txn_metadata_->commit_timestamp()->value()) : "") :
                "")
      << ": row " << schema_->DebugRow(row)
      << " mutations=" << Mutation::StringifyMutationList(*schema_.get(), row.header_->redo_head)
      << std::endl;
    iter->Next();
  }

  return Status::OK();
}


Status MemRowSet::Insert(Timestamp timestamp,
                         const ConstContiguousRow& row,
                         const OpId& op_id) {
  CHECK(row.schema()->has_column_ids());
  DCHECK_SCHEMA_EQ(*schema_.get(), *row.schema());

  {
    faststring enc_key_buf;
    schema_->EncodeComparableKey(row, &enc_key_buf);
    Slice enc_key(enc_key_buf);

    btree::PreparedMutation<MSBTreeTraits> mutation(enc_key);
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
        return Status::AlreadyPresent("key already present");
      }

      // Insert a "reinsert" mutation.
      return Reinsert(timestamp, row, &ms_row);
    }

    // Copy the non-encoded key onto the stack since we need
    // to mutate it when we relocate its Slices into our arena.
    DEFINE_MRSROW_ON_STACK(this, mrsrow, mrsrow_slice);
    mrsrow.header_->insertion_timestamp = timestamp;
    mrsrow.header_->redo_head = nullptr;
    mrsrow.header_->redo_tail = nullptr;
    RETURN_NOT_OK(mrsrow.CopyRow(row, arena_.get()));

    CHECK(mutation.Insert(mrsrow_slice))
        << "Expected to be able to insert, since the prepared mutation "
        << "succeeded!";
  }

  anchorer_.AnchorIfMinimum(op_id.index());

  debug_insert_count_++;
  live_row_count_.Increment();
  return Status::OK();
}

Status MemRowSet::Reinsert(Timestamp timestamp, const ConstContiguousRow& row, MRSRow *ms_row) {
  DCHECK_SCHEMA_EQ(*schema_.get(), *row.schema());

  // Encode the REINSERT mutation
  faststring buf;
  RowChangeListEncoder encoder(&buf);
  encoder.SetToReinsert(row);

  // Move the REINSERT mutation itself into our Arena.
  Mutation *mut = Mutation::CreateInArena(arena_.get(), timestamp, encoder.as_changelist());

  // Append the mutation into the row's mutation list.
  // This function has "release" semantics which ensures that the memory writes
  // for the mutation are fully published before any concurrent reader sees
  // the appended mutation.
  mut->AppendToListAtomic(&ms_row->header_->redo_head, &ms_row->header_->redo_tail);

  live_row_count_.Increment();
  return Status::OK();
}

Status MemRowSet::MutateRow(Timestamp timestamp,
                            const RowSetKeyProbe &probe,
                            const RowChangeList &delta,
                            const consensus::OpId& op_id,
                            const IOContext* /*io_context*/,
                            ProbeStats* stats,
                            OperationResultPB *result) {
  {
    btree::PreparedMutation<MSBTreeTraits> mutation(probe.encoded_key_slice());
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
    Mutation *mut = Mutation::CreateInArena(arena_.get(), timestamp, delta);

    // This function has "release" semantics which ensures that the memory writes
    // for the mutation are fully published before any concurrent reader sees
    // the appended mutation.
    mut->AppendToListAtomic(&row.header_->redo_head, &row.header_->redo_tail);

    MemStoreTargetPB* target = result->add_mutated_stores();
    target->set_mrs_id(id_);
    if (txn_id_) {
      target->set_rs_txn_id(*txn_id_);
    }
  }

  stats->mrs_consulted++;

  anchorer_.AnchorIfMinimum(op_id.index());
  debug_update_count_++;
  if (delta.is_delete()) {
    live_row_count_.IncrementBy(-1);
  }
  return Status::OK();
}

Status MemRowSet::CheckRowPresent(const RowSetKeyProbe &probe, const IOContext* /*io_context*/,
                                  bool* present, ProbeStats* stats) const {
  // Use a PreparedMutation here even though we don't plan to mutate. Even though
  // this takes a lock rather than an optimistic copy, it should be a very short
  // critical section, and this call is only made on updates, which are rare.

  stats->mrs_consulted++;

  btree::PreparedMutation<MSBTreeTraits> mutation(probe.encoded_key_slice());
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

MemRowSet::Iterator *MemRowSet::NewIterator(const RowIteratorOptions& opts) const {
  return new MemRowSet::Iterator(shared_from_this(), tree_.NewIterator(), opts);
}

MemRowSet::Iterator *MemRowSet::NewIterator() const {
  // TODO(todd): can we kill this function? should be only used by tests?
  RowIteratorOptions opts;
  opts.projection = schema();
  return NewIterator(opts);
}

Status MemRowSet::NewRowIterator(const RowIteratorOptions& opts,
                                 unique_ptr<RowwiseIterator>* out) const {
  out->reset(NewIterator(opts));
  return Status::OK();
}

Status MemRowSet::NewCompactionInput(const SchemaPtr& projection,
                                     const MvccSnapshot& snap,
                                     const IOContext* /*io_context*/,
                                     unique_ptr<CompactionInput>* out) const  {
  out->reset(CompactionInput::Create(*this, projection, snap));
  return Status::OK();
}

Status MemRowSet::GetBounds(string *min_encoded_key,
                            string *max_encoded_key) const {
  return Status::NotSupported("");
}

// Virtual interface allows two possible row projector implementations
class MemRowSet::Iterator::MRSRowProjector {
 public:
  typedef RowProjector::ProjectionIdxMapping ProjectionIdxMapping;
  virtual ~MRSRowProjector() {}
  virtual Status ProjectRowForRead(const MRSRow& src_row,
                                   RowBlockRow* dst_row,
                                   Arena* arena) = 0;
  virtual Status ProjectRowForRead(const ConstContiguousRow& src_row,
                                   RowBlockRow* dst_row,
                                   Arena* arena) = 0;
  virtual const vector<ProjectionIdxMapping>& base_cols_mapping() const = 0;
  virtual Status Init() = 0;
};

namespace {

typedef MemRowSet::Iterator::MRSRowProjector MRSRowProjector;

template<class ActualProjector>
class MRSRowProjectorImpl : public MRSRowProjector {
 public:
  explicit MRSRowProjectorImpl(unique_ptr<ActualProjector> actual)
    : actual_(std::move(actual)) {}

  Status Init() override { return actual_->Init(); }

  Status ProjectRowForRead(const MRSRow& src_row, RowBlockRow* dst_row,
                           Arena* arena) override {
    return actual_->ProjectRowForRead(src_row, dst_row, arena);
  }
  Status ProjectRowForRead(const ConstContiguousRow& src_row,
                           RowBlockRow* dst_row,
                           Arena* arena) override {
    return actual_->ProjectRowForRead(src_row, dst_row, arena);
  }

  const vector<ProjectionIdxMapping>& base_cols_mapping() const override {
    return actual_->base_cols_mapping();
  }

 private:
  unique_ptr<ActualProjector> actual_;
};

// If codegen is enabled, then generates a codegen::RowProjector;
// otherwise makes a regular one.
unique_ptr<MRSRowProjector> GenerateAppropriateProjector(
  const Schema* base, const Schema* projection) {
  // Attempt code-generated implementation
  if (FLAGS_mrs_use_codegen) {
    unique_ptr<codegen::RowProjector> actual;
    if (codegen::CompilationManager::GetSingleton()->RequestRowProjector(
          base, projection, &actual)) {
      return unique_ptr<MRSRowProjector>(
        new MRSRowProjectorImpl<codegen::RowProjector>(std::move(actual)));
    }
  }

  // Proceed with default implementation
  unique_ptr<RowProjector> actual(new RowProjector(base, projection));
  return unique_ptr<MRSRowProjector>(
    new MRSRowProjectorImpl<RowProjector>(std::move(actual)));
}

} // anonymous namespace

MemRowSet::Iterator::Iterator(const std::shared_ptr<const MemRowSet>& mrs,
                              MemRowSet::MSBTIter* iter,
                              RowIteratorOptions opts)
    : memrowset_(mrs),
      iter_(iter),
      opts_(std::move(opts)),
      projector_(
          GenerateAppropriateProjector(mrs->schema_nonvirtual().get(), opts_.projection.get())),
      delta_projector_(mrs->schema_nonvirtual().get(), opts_.projection.get()),
      projection_vc_is_deleted_idx_(opts_.projection->first_is_deleted_virtual_column_idx()),
      state_(kUninitialized) {
  // TODO(todd): various code assumes that a newly constructed iterator
  // is pointed at the beginning of the dataset. This causes a redundant
  // seek. Could make this lazy instead, or change the semantics so that
  // a seek is required (probably the latter)
  iter_->SeekToStart();
}

MemRowSet::Iterator::~Iterator() {}

Status MemRowSet::Iterator::Init(ScanSpec *spec) {
  DCHECK_EQ(state_, kUninitialized);

  RETURN_NOT_OK(projector_->Init());
  RETURN_NOT_OK(delta_projector_.Init());

  if (spec && spec->lower_bound_key()) {
    bool exact;
    const Slice &lower_bound = spec->lower_bound_key()->encoded_key();
    if (!iter_->SeekAtOrAfter(lower_bound, &exact)) {
      // Lower bound is after the end of the key range, no rows will
      // pass the predicate so we can stop the scan right away.
      state_ = kFinished;
      return Status::OK();
    }
  }

  if (spec && spec->exclusive_upper_bound_key()) {
    const Slice &upper_bound = spec->exclusive_upper_bound_key()->encoded_key();
    exclusive_upper_bound_.reset(upper_bound);
  }

  state_ = kScanning;
  return Status::OK();
}

Status MemRowSet::Iterator::SeekAtOrAfter(const Slice &key, bool *exact) {
  DCHECK_NE(state_, kUninitialized) << "not initted";

  if (key.size() > 0) {
    ConstContiguousRow row_slice(memrowset_->schema().get(), key);
    memrowset_->schema()->EncodeComparableKey(row_slice, &tmp_buf);
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

Status MemRowSet::Iterator::NextBlock(RowBlock *dst) {
  // TODO: add dcheck that dst->schema() matches our schema
  // also above TODO applies to a lot of other CopyNextRows cases

  DCHECK_NE(state_, kUninitialized) << "not initted";
  if (PREDICT_FALSE(!iter_->IsValid())) {
    dst->Resize(0);
    return Status::NotFound("end of iter");
  }
  if (PREDICT_FALSE(state_ != kScanning)) {
    dst->Resize(0);
    return Status::OK();
  }
  if (PREDICT_FALSE(dst->row_capacity() == 0)) {
    return Status::OK();
  }

  // Reset rowblock arena to eventually reach appropriate buffer size.
  // Always allocating the full capacity is only a problem for the last block.
  dst->Resize(dst->row_capacity());
  if (dst->arena()) {
    dst->arena()->Reset();
  }

  // Fill
  dst->selection_vector()->SetAllTrue();
  size_t fetched;
  RETURN_NOT_OK(FetchRows(dst, &fetched));
  DCHECK_LE(0, fetched);
  DCHECK_LE(fetched, dst->nrows());

  // Clear unreached bits by resizing
  dst->Resize(fetched);

  return Status::OK();
}

Status MemRowSet::Iterator::FetchRows(RowBlock* dst, size_t* fetched) {
  *fetched = 0;
  do {
    RowBlockRow dst_row = dst->row(*fetched);

    // Copy the row into the destination, including projection
    // and relocating slices.
    // TODO(todd): can we share some code here with CopyRowToArena() from row.h
    // or otherwise put this elsewhere?
    Slice v = iter_->GetCurrentValue();
    MRSRow row(memrowset_.get(), v);

    // Short-circuit if we've exceeded the iteration's upper bound.
    if (has_upper_bound() && out_of_bounds(iter_->GetCurrentKey())) {
      state_ = kFinished;
      break;
    }

    // The snapshots in 'opts_' represent a time range that this iterator must
    // respect. There are two possible cases:
    //
    // 1. 'snap_to_exclude' is unset but 'snap_to_include' is set. The time
    //    range is [INF, snap_to_include).
    // 2. Both 'snap_to exclude' and 'snap_to_include' are set. The time range
    //    is [snap_to_exclude, snap_to_include).
    //
    // If a non-transactional  row's insertion timestamp is applied in
    // 'snap_to_exclude' or a transactional row's commit timestamp is committed
    // in 'snap_to_exclude', the insertion was outside this iterator's time
    // range (i.e. the insert was "excluded").  However, subsequent mutations
    // may be inside the time range, so we must still project the row and walk
    // its mutation list.
    const auto& txn_meta = memrowset_->txn_metadata();
    bool insert_excluded = opts_.snap_to_exclude &&
        // TODO(awong): if we find this to be too slow, we should be able to
        // compute IsCommitted() once per iterator at construction time.
        (txn_meta ? opts_.snap_to_exclude->IsCommitted(*txn_meta.get()) :
                    opts_.snap_to_exclude->IsApplied(row.insertion_timestamp()));
    bool unset_in_sel_vector;
    ApplyStatus apply_status;
    if (insert_excluded ||
        (txn_meta ? opts_.snap_to_include.IsCommitted(*txn_meta.get()) :
                    opts_.snap_to_include.IsApplied(row.insertion_timestamp()))) {
      RETURN_NOT_OK(projector_->ProjectRowForRead(row, &dst_row, dst->arena()));

      // Roll-forward MVCC for committed updates.
      Mutation* redo_head = reinterpret_cast<Mutation*>(
          base::subtle::Acquire_Load(reinterpret_cast<AtomicWord*>(&row.header_->redo_head)));
      RETURN_NOT_OK(ApplyMutationsToProjectedRow(
          redo_head, &dst_row, dst->arena(), insert_excluded, &apply_status));
      unset_in_sel_vector = (apply_status == APPLIED_AND_DELETED && !opts_.include_deleted_rows) ||
                            (apply_status == NONE_APPLIED && insert_excluded) ||
                            (apply_status == APPLIED_AND_UNOBSERVABLE);
    } else {
      // The insertion is too new; the entire row should be omitted.
      unset_in_sel_vector = true;
    }

    if (unset_in_sel_vector) {
      dst->selection_vector()->SetRowUnselected(*fetched);

      // In debug mode, fill the row data for easy debugging
      #ifndef NDEBUG
      if (state_ != kFinished) {
        dst_row.OverwriteWithPattern("MVCCMVCCMVCCMVCCMVCCMVCC"
                                     "MVCCMVCCMVCCMVCCMVCCMVCC"
                                     "MVCCMVCCMVCCMVCCMVCCMVCC");
      }
      #endif
    } else if (projection_vc_is_deleted_idx_ != Schema::kColumnNotFound) {
      UnalignedStore(dst_row.mutable_cell_ptr(projection_vc_is_deleted_idx_),
                     apply_status == APPLIED_AND_DELETED);
    }

    ++*fetched;
  } while (iter_->Next() && *fetched < dst->nrows());

  return Status::OK();
}

Status MemRowSet::Iterator::ApplyMutationsToProjectedRow(
    const Mutation* mutation_head, RowBlockRow* dst_row, Arena* dst_arena,
    bool insert_excluded, ApplyStatus* apply_status) {
  ApplyStatus local_apply_status = NONE_APPLIED;

  // Fast short-circuit the likely case of a row which was inserted and never
  // updated.
  if (PREDICT_TRUE(mutation_head == nullptr)) {
    *apply_status = local_apply_status;
    return Status::OK();
  }

  // In order to find unobservable rows, we need to track row liveness at the
  // start and end of the time range. If a row was dead at both ends, its
  // lifespan must have been a subset of the time range and it should be
  // excluded from the results.
  //
  // Finding 'is_deleted_end' is relatively straight-forward: we use each
  // relevant mutation to drive a liveness state machine, and after we're done
  // applying, 'is_deleted_end' is just the final value of that state machine.
  //
  // Finding 'is_deleted_start' is trickier. If the insertion was inside the
  // time range, we know the value is true because the row was dead prior to the
  // insertion and the insertion happened after the start of the time range.
  // However, if the insertion was excluded from the time range, the value is
  // going to be whatever the value of the liveness state machine was at the
  // start of the time range.
  bool is_deleted_start = !insert_excluded;
  bool is_deleted_end = false;

  for (const Mutation *mut = mutation_head;
       mut != nullptr;
       mut = mut->acquire_next()) {
    if (!opts_.snap_to_include.IsApplied(mut->timestamp_)) {
      // This mutation is too new and should be omitted.
      //
      // All subsequent mutations are also too new because their timestamps are
      // guaranteed to be equal to or greater than this mutation's timestamp.
      break;
    }

    // If the mutation is too old, we still need to apply it (so that the column
    // values are correct if we see a relevant mutation later), but it doesn't
    // count towards the overall "application status".
    if (!opts_.snap_to_exclude ||
        !opts_.snap_to_exclude->IsApplied(mut->timestamp_)) {

      // This is the first mutation within the time range, so we may use it to
      // initialize 'is_deleted_start'.
      if (local_apply_status == NONE_APPLIED && insert_excluded) {
        is_deleted_start = is_deleted_end;
      }
      local_apply_status = APPLIED_AND_PRESENT;
    }

    // Apply the mutation.

    // Check if it's a deletion.
    RowChangeListDecoder decoder(mut->changelist());
    RETURN_NOT_OK(decoder.Init());
    if (decoder.is_delete()) {
      decoder.TwiddleDeleteStatus(&is_deleted_end);
    } else {
      DCHECK(decoder.is_update() || decoder.is_reinsert());
      if (decoder.is_reinsert()) {
        decoder.TwiddleDeleteStatus(&is_deleted_end);
      }

      // TODO(todd): this is slow, since it makes multiple passes through the rowchangelist.
      // Instead, we should keep the backwards mapping of columns.
      for (const RowProjector::ProjectionIdxMapping& mapping : projector_->base_cols_mapping()) {
        RowChangeListDecoder decoder(mut->changelist());
        RETURN_NOT_OK(decoder.Init());
        ColumnBlock dst_col = dst_row->column_block(mapping.first);
        RETURN_NOT_OK(decoder.ApplyToOneColumn(dst_row->row_index(), &dst_col,
                                               *memrowset_->schema_nonvirtual().get(),
                                               mapping.second, dst_arena));
      }
    }
  }

  if (opts_.snap_to_exclude && is_deleted_start && is_deleted_end) {
    // The row's lifespan was a subset of the time range. It can't be observed,
    // so it should definitely not show up in the results.
    //
    // Note: we condition on 'snap_to_exclude' because although insert_excluded
    // is false on some closed time range scans, it's also false in all open
    // time range scans, and we don't want this heuristic to fire in the latter case.
    local_apply_status = APPLIED_AND_UNOBSERVABLE;
  } else if (is_deleted_end && local_apply_status == APPLIED_AND_PRESENT) {
    // The row was selected and deleted. It may be omitted from the results,
    // depending on whether the results should include deleted rows or not.
    local_apply_status = APPLIED_AND_DELETED;
  }

  *apply_status = local_apply_status;
  return Status::OK();
}

// Copy the current MRSRow to the 'dst_row' provided using the iterator projection schema.
Status MemRowSet::Iterator::GetCurrentRow(RowBlockRow* dst_row,
                                          Arena* row_arena,
                                          Mutation** redo_head,
                                          Arena* mutation_arena,
                                          Timestamp* insertion_timestamp) {
  DCHECK(boost::none == opts_.snap_to_exclude);
  DCHECK(redo_head != nullptr);

  // Get the row from the MemRowSet. It may have a different schema from the iterator projection.
  MRSRow src_row = GetCurrentRow();
  const auto& mrs_txn_id = memrowset_->txn_id();
  if (mrs_txn_id) {
    // NOTE: we currently only support flushing committed MRSs.
    const auto& txn_meta = memrowset_->txn_metadata();
    CHECK(opts_.snap_to_include.IsCommitted(*txn_meta.get()));
    CHECK(boost::none != txn_meta->commit_timestamp());
    *insertion_timestamp = *txn_meta->commit_timestamp();
  } else {
    *insertion_timestamp = src_row.insertion_timestamp();
  }

  // Project the RowChangeList if required
  *redo_head = src_row.acquire_redo_head();
  if (!delta_projector_.is_identity()) {
    DCHECK(mutation_arena != nullptr);

    Mutation *prev_redo = nullptr;
    *redo_head = nullptr;
    for (const Mutation *mut = src_row.redo_head();
         mut != nullptr;
         mut = mut->acquire_next()) {

      delta_buf_.clear();
      RowChangeListEncoder enc(&delta_buf_);
      RETURN_NOT_OK(RowChangeListDecoder::ProjectChangeList(delta_projector_,
                                                            mut->changelist(),
                                                            &enc));

      // The projection resulted in an empty mutation (e.g. update of a removed column)
      if (enc.is_empty()) continue;

      Mutation *mutation = Mutation::CreateInArena(mutation_arena,
                                                   mut->timestamp(),
                                                   RowChangeList(delta_buf_));
      if (prev_redo != nullptr) {
        prev_redo->set_next(mutation);
      } else {
        *redo_head = mutation;
      }
      prev_redo = mutation;
    }
  }

  // Project the Row
  return projector_->ProjectRowForRead(src_row, dst_row, row_arena);
}

} // namespace tablet
} // namespace kudu
