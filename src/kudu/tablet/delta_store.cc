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

#include "kudu/tablet/delta_store.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <ostream>

#include <glog/logging.h>

#include "kudu/common/columnblock.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_relevancy.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"

namespace kudu {
namespace tablet {

using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

string DeltaKeyAndUpdate::Stringify(DeltaType type, const Schema& schema, bool pad_key) const {
  return StrCat(Substitute("($0 delta key=$2, change_list=$1)",
                           DeltaType_Name(type),
                           RowChangeList(cell).ToString(schema),
                           (pad_key ? StringPrintf("%06u@tx%06u", key.row_idx(),
                                                   atoi(key.timestamp().ToString().c_str()))
                                    : Substitute("$0@tx$1", key.row_idx(),
                                                 key.timestamp().ToString()))));
}

SelectedDeltas::SelectedDeltas(size_t nrows) {
  Reset(nrows);
}

void SelectedDeltas::MergeFrom(const SelectedDeltas& other) {
  DCHECK_EQ(rows_.size(), other.rows_.size());

  for (rowid_t idx = 0; idx < rows_.size(); idx++) {
    const auto& src = other.rows_[idx];
    if (!src) {
      continue;
    }
    if (src->same_delta) {
      ProcessDelta(idx, src->oldest);
    } else {
      ProcessDelta(idx, src->oldest);
      ProcessDelta(idx, src->newest);
    }
  }
}

void SelectedDeltas::ToSelectionVector(SelectionVector* sel_vec) const {
  DCHECK_EQ(rows_.size(), sel_vec->nrows());

  for (rowid_t idx = 0; idx < rows_.size(); idx++) {
    const auto& row = rows_[idx];

    if (!row) {
      // There were no relevant deltas for this row.
      sel_vec->SetRowUnselected(idx);
      continue;
    }

    if (row->same_delta) {
      // There was exactly one relevant delta; the row must be selected.
      sel_vec->SetRowSelected(idx);
      continue;
    }

    // There was more than one relevant delta.
    //
    // Before we mark the row as selected, we must first determine whether the
    // row was dead at the beginning and end of the time range: such rows should
    // be deselected. We've captured the oldest and newest deltas; the table
    // below indicates whether, for a given type of delta, the row is live or
    // dead at that endpoint.
    //
    // delta type    | oldest | newest
    // --------------+--------+-------
    // REDO DELETE   | L      | D
    // REDO REINSERT | D      | L
    // UNDO DELETE   | D      | L
    // UNDO REINSERT | L      | D
    const auto& oldest = row->oldest;
    const auto& newest = row->newest;
    if (((oldest.dtype == REDO && oldest.ctype == RowChangeList::kReinsert) ||
         (oldest.dtype == UNDO && oldest.ctype == RowChangeList::kDelete)) &&
        ((newest.dtype == REDO && newest.ctype == RowChangeList::kDelete) ||
         (newest.dtype == UNDO && newest.ctype == RowChangeList::kReinsert))) {
      sel_vec->SetRowUnselected(idx);
    } else {
      sel_vec->SetRowSelected(idx);
    }
  }
}

void SelectedDeltas::ProcessDelta(rowid_t row_idx, Delta new_delta) {
  DCHECK_LT(row_idx, rows_.size());
  auto& existing = rows_[row_idx];

  if (!existing) {
    existing = DeltaPair();
    existing->same_delta = true;
    existing->oldest = new_delta;
    existing->newest = new_delta;
    return;
  }

  existing->oldest = std::min(existing->oldest, new_delta, DeltaLessThanFunctor());
  existing->newest = std::max(existing->newest, new_delta, DeltaLessThanFunctor());
  existing->same_delta = false;
}

string SelectedDeltas::ToString() const {
  rowid_t idx = 0;
  return JoinMapped(rows_, [&idx](const boost::optional<DeltaPair>& dp) {
      if (!dp) {
        return Substitute("$0: UNSELECTED", idx++);
      }
      return Substitute("$0: @tx$1 $2 dis=$3 ($4) @tx$5 $6 dis=$7 ($8)$9", idx++,
                        dp->oldest.ts.ToString(),
                        DeltaType_Name(dp->oldest.dtype),
                        dp->oldest.disambiguator,
                        RowChangeList::ChangeType_Name(dp->oldest.ctype),
                        dp->newest.ts.ToString(),
                        DeltaType_Name(dp->newest.dtype),
                        dp->newest.disambiguator,
                        RowChangeList::ChangeType_Name(dp->newest.ctype),
                        dp->same_delta ? " (same delta)" : "");
    }, "\n");
}

void SelectedDeltas::Reset(size_t nrows) {
  rows_.clear();
  rows_.resize(nrows);
}

template<class Traits>
DeltaPreparer<Traits>::DeltaPreparer(RowIteratorOptions opts)
    : opts_(std::move(opts)),
      cur_prepared_idx_(0),
      prev_prepared_idx_(0),
      prepared_flags_(DeltaIterator::PREPARE_NONE),
      deletion_state_(UNKNOWN),
      deltas_selected_(0) {
}

template<class Traits>
void DeltaPreparer<Traits>::Seek(rowid_t row_idx) {
  cur_prepared_idx_ = row_idx;
  prev_prepared_idx_ = row_idx;
  prepared_flags_ = DeltaIterator::PREPARE_NONE;
}

template<class Traits>
void DeltaPreparer<Traits>::Start(size_t nrows, int prepare_flags) {
  DCHECK_NE(prepare_flags, DeltaIterator::PREPARE_NONE);

  if (prepare_flags & DeltaIterator::PREPARE_FOR_SELECT) {
    DCHECK(opts_.snap_to_exclude);
    selected_.Reset(nrows);
  }
  prepared_flags_ = prepare_flags;
  if (updates_by_col_.empty()) {
    updates_by_col_.resize(opts_.projection->num_columns());
  }
  for (UpdatesForColumn& ufc : updates_by_col_) {
    ufc.clear();
  }
  deleted_.clear();
  reinserted_.clear();
  prepared_deltas_.clear();
  deletion_state_ = UNKNOWN;

  if (VLOG_IS_ON(3)) {
    string snap_to_exclude = opts_.snap_to_exclude ?
                             opts_.snap_to_exclude->ToString() : "INF";
    VLOG(3) << "Starting batch for [" << snap_to_exclude << ","
            << opts_.snap_to_include.ToString() << ")";
  }
}

template<class Traits>
void DeltaPreparer<Traits>::Finish(size_t nrows) {
  MaybeProcessPreviousRowChange(boost::none);
  prev_prepared_idx_ = cur_prepared_idx_;
  cur_prepared_idx_ += nrows;

  if (VLOG_IS_ON(3)) {
    string snap_to_exclude = opts_.snap_to_exclude ?
                             opts_.snap_to_exclude->ToString() : "INF";
    VLOG(3) << "Finishing batch for [" << snap_to_exclude << ","
            << opts_.snap_to_include.ToString() << ")";
  }
}

template<class Traits>
Status DeltaPreparer<Traits>::AddDelta(const DeltaKey& key, Slice val, bool* finished_row) {
  MaybeProcessPreviousRowChange(key.row_idx());

  VLOG(4) << "Considering delta " << key.ToString() << ": "
          << RowChangeList(val).ToString(*opts_.projection);

  // Different preparations may use different criteria for delta relevancy. Each
  // criteria offers a short-circuit when processing of the current row is known
  // to be finished, but that short-circuit can only be used if we're not also
  // handling a preparation with a different criteria.

  RowChangeListDecoder decoder((RowChangeList(val)));

  if (prepared_flags_ & DeltaIterator::PREPARE_FOR_SELECT) {
    bool finished_row_for_select;
    if (IsDeltaRelevantForSelect<Traits::kType>(*opts_.snap_to_exclude,
                                                opts_.snap_to_include,
                                                key.timestamp(),
                                                &finished_row_for_select)) {
      RETURN_NOT_OK(InitDecoderIfNecessary(&decoder));

      // The logical ordering of UNDOs is the opposite of their counting order.
      int64_t disambiguator = Traits::kType == REDO ?
                              deltas_selected_ : -deltas_selected_;

      // We use the address of the DeltaPreparer itself as a "delta store" ID.
      // That's safe because it is globally unique and remains so for the
      // duration of the scan, which outlives this delta.
      SelectedDeltas::Delta new_delta = { key.timestamp(),
                                          Traits::kType,
                                          disambiguator,
                                          reinterpret_cast<int64_t>(this),
                                          decoder.get_type() };

      selected_.ProcessDelta(key.row_idx() - cur_prepared_idx_, new_delta);
      deltas_selected_++;
      VLOG(4) << "Selected deltas after AddDelta:\n" << selected_.ToString();
    }

    if (finished_row_for_select &&
        !(prepared_flags_ & ~DeltaIterator::PREPARE_FOR_SELECT)) {
      *finished_row = true;
    }
  }

  // Apply and collect use the same relevancy criteria.
  bool relevant_for_apply_or_collect = false;
  bool finished_row_for_apply_or_collect = false;
  if (prepared_flags_ & (DeltaIterator::PREPARE_FOR_APPLY |
                         DeltaIterator::PREPARE_FOR_COLLECT)) {
    relevant_for_apply_or_collect = IsDeltaRelevantForApply<Traits::kType>(
        opts_.snap_to_include, key.timestamp(), &finished_row_for_apply_or_collect);
  }

  if (prepared_flags_ & DeltaIterator::PREPARE_FOR_APPLY &&
      relevant_for_apply_or_collect) {
    RETURN_NOT_OK(InitDecoderIfNecessary(&decoder));
    UpdateDeletionState(decoder.get_type());
    if (!decoder.is_delete()) {
      while (decoder.HasNext()) {
        RowChangeListDecoder::DecodedUpdate dec;
        RETURN_NOT_OK(decoder.DecodeNext(&dec));
        int col_idx;
        const void* col_val;
        RETURN_NOT_OK(dec.Validate(*opts_.projection, &col_idx, &col_val));
        if (col_idx == -1) {
          // This column isn't being projected.
          continue;
        }
        int col_size = opts_.projection->column(col_idx).type_info()->size();

        // If we already have an earlier update for the same column, we can
        // just overwrite that one.
        if (updates_by_col_[col_idx].empty() ||
            updates_by_col_[col_idx].back().row_id != key.row_idx()) {
          updates_by_col_[col_idx].emplace_back();
        }

        ColumnUpdate& cu = updates_by_col_[col_idx].back();
        cu.row_id = key.row_idx();
        if (col_val == nullptr) {
          cu.new_val_ptr = nullptr;
        } else {
          memcpy(cu.new_val_buf, col_val, col_size);
          // NOTE: we're constructing a pointer here to an element inside the deque.
          // This is safe because deques never invalidate pointers to their elements.
          cu.new_val_ptr = cu.new_val_buf;
        }
      }
    }
  }

  if (prepared_flags_ & DeltaIterator::PREPARE_FOR_COLLECT &&
      relevant_for_apply_or_collect) {
    PreparedDelta d;
    d.key = key;
    d.val = val;
    prepared_deltas_.emplace_back(d);
  }

  if (finished_row_for_apply_or_collect &&
      !(prepared_flags_ & ~(DeltaIterator::PREPARE_FOR_APPLY |
                            DeltaIterator::PREPARE_FOR_COLLECT))) {
    *finished_row = true;
  }

  last_added_idx_ = key.row_idx();
  return Status::OK();
}

template<class Traits>
Status DeltaPreparer<Traits>::ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                                           const SelectionVector& filter) {
  DCHECK(prepared_flags_ & DeltaIterator::PREPARE_FOR_APPLY);
  DCHECK_LE(cur_prepared_idx_ - prev_prepared_idx_, dst->nrows());

  // Special handling for the IS_DELETED virtual column: convert 'deleted_' and
  // 'reinserted_' into true and false cell values.
  if (col_to_apply == opts_.projection->first_is_deleted_virtual_column_idx()) {
    // See ApplyDeletes() to understand why we adjust the virtual column's value
    // for both deleted and reinserted rows.
    for (const auto& row_id : deleted_) {
      uint32_t idx_in_block = row_id - prev_prepared_idx_;
      if (filter.IsRowSelected(idx_in_block)) {
        ColumnBlock::Cell cell = dst->cell(idx_in_block);
        UnalignedStore(cell.mutable_ptr(), true);
      }
    }

    for (const auto& row_id : reinserted_) {
      uint32_t idx_in_block = row_id - prev_prepared_idx_;
      if (filter.IsRowSelected(idx_in_block)) {
        ColumnBlock::Cell cell = dst->cell(idx_in_block);
        UnalignedStore(cell.mutable_ptr(), false);
      }
    }

    return Status::OK();
  }

  const ColumnSchema* col_schema = &opts_.projection->column(col_to_apply);
  for (const ColumnUpdate& cu : updates_by_col_[col_to_apply]) {
    int32_t idx_in_block = cu.row_id - prev_prepared_idx_;
    DCHECK_GE(idx_in_block, 0);
    if (!filter.IsRowSelected(idx_in_block)) {
      continue;
    }
    SimpleConstCell src(col_schema, cu.new_val_ptr);
    ColumnBlock::Cell dst_cell = dst->cell(idx_in_block);
    RETURN_NOT_OK(CopyCell(src, &dst_cell, dst->arena()));
  }

  return Status::OK();
}

template<class Traits>
Status DeltaPreparer<Traits>::ApplyDeletes(SelectionVector* sel_vec) {
  DCHECK(prepared_flags_ & DeltaIterator::PREPARE_FOR_APPLY);
  DCHECK_LE(cur_prepared_idx_ - prev_prepared_idx_, sel_vec->nrows());

  // To understand why we must adjust sel_vec for both deleted_ and reinserted_,
  // consider that DeltaIterators are often used en masse (i.e. via
  // DeltaIteratorMerger). In such cases, it's possible for one DeltaPreparer to
  // delete a row and for the next to reinsert it. Given that ApplyDeletes is
  // called on each DeltaPreparer in order, we must "twiddle" sel_vec in either
  // direction in order for the row's bit to hold the correct state at the end.
  for (const auto& row_id : deleted_) {
    uint32_t idx_in_block = row_id - prev_prepared_idx_;
    sel_vec->SetRowUnselected(idx_in_block);
  }

  for (const auto& row_id : reinserted_) {
    uint32_t idx_in_block = row_id - prev_prepared_idx_;
    sel_vec->SetRowSelected(idx_in_block);
  }

  return Status::OK();
}

template<class Traits>
Status DeltaPreparer<Traits>::SelectDeltas(SelectedDeltas* deltas) {
  DCHECK(prepared_flags_ & DeltaIterator::PREPARE_FOR_SELECT);
  DCHECK_LE(cur_prepared_idx_ - prev_prepared_idx_, deltas->rows_.size());
  VLOG(4) << "Selected deltas before SelectDeltas:\n" << selected_.ToString();
  VLOG(4) << "Pre-merge deltas:\n" << deltas->ToString();
  deltas->MergeFrom(selected_);
  VLOG(4) << "Post-merge deltas:\n" << deltas->ToString();
  return Status::OK();
}

template<class Traits>
Status DeltaPreparer<Traits>::CollectMutations(vector<Mutation*>* dst, Arena* arena) {
  DCHECK(prepared_flags_ & DeltaIterator::PREPARE_FOR_COLLECT);
  DCHECK_LE(cur_prepared_idx_ - prev_prepared_idx_, dst->size());
  for (const PreparedDelta& src : prepared_deltas_) {
    DeltaKey key = src.key;
    RowChangeList changelist(src.val);
    uint32_t rel_idx = key.row_idx() - prev_prepared_idx_;

    Mutation *mutation = Mutation::CreateInArena(arena, key.timestamp(), changelist);
    mutation->PrependToList(&dst->at(rel_idx));
  }
  return Status::OK();
}

template<class Traits>
Status DeltaPreparer<Traits>::FilterColumnIdsAndCollectDeltas(
    const vector<ColumnId>& col_ids,
    vector<DeltaKeyAndUpdate>* out,
    Arena* arena) {
  if (!Traits::kAllowFilterColumnIdsAndCollectDeltas) {
    LOG(DFATAL) << "Attempted to call FilterColumnIdsAndCollectDeltas on DMS"
                << GetStackTrace();
    return Status::InvalidArgument(
        "FilterColumnIdsAndCollectDeltas is not supported");
  }

  // May only be used on a fully inclusive snapshot.
  DCHECK(opts_.snap_to_include.Equals(Traits::kType == REDO ?
                                      MvccSnapshot::CreateSnapshotIncludingAllTransactions() :
                                      MvccSnapshot::CreateSnapshotIncludingNoTransactions()));

  faststring buf;
  RowChangeListEncoder encoder(&buf);
  for (const auto& src : prepared_deltas_) {
    encoder.Reset();
    RETURN_NOT_OK(
        RowChangeListDecoder::RemoveColumnIdsFromChangeList(RowChangeList(src.val),
                                                            col_ids,
                                                            &encoder));
    if (encoder.is_initialized()) {
      RowChangeList rcl = encoder.as_changelist();
      DeltaKeyAndUpdate upd;
      upd.key = src.key;
      CHECK(arena->RelocateSlice(rcl.slice(), &upd.cell));
      out->emplace_back(upd);
    }
  }

  return Status::OK();
}

template<class Traits>
bool DeltaPreparer<Traits>::MayHaveDeltas() const {
  DCHECK(prepared_flags_ & DeltaIterator::PREPARE_FOR_APPLY);
  if (!deleted_.empty()) {
    return true;
  }
  if (!reinserted_.empty()) {
    return true;
  }
  for (const auto& col : updates_by_col_) {
    if (!col.empty()) {
      return true;
    }
  }
  return false;
}

template<class Traits>
Status DeltaPreparer<Traits>::InitDecoderIfNecessary(RowChangeListDecoder* decoder) {
  if (decoder->IsInitialized()) {
    return Status::OK();
  }
  if (Traits::kInitializeDecodersWithSafetyChecks) {
    RETURN_NOT_OK(decoder->Init());
  } else {
    decoder->InitNoSafetyChecks();
  }
  if (!Traits::kAllowReinserts && decoder->is_reinsert()) {
    LOG(DFATAL) << "Attempted to reinsert but not supported" << GetStackTrace();
    return Status::InvalidArgument("Reinserts are not supported");
  }
  return Status::OK();
}

template<class Traits>
void DeltaPreparer<Traits>::MaybeProcessPreviousRowChange(boost::optional<rowid_t> cur_row_idx) {
  if (prepared_flags_ & DeltaIterator::PREPARE_FOR_APPLY &&
      last_added_idx_ &&
      (!cur_row_idx || cur_row_idx != *last_added_idx_)) {
    switch (deletion_state_) {
      case DELETED:
        deleted_.emplace_back(*last_added_idx_);
        deletion_state_ = UNKNOWN;
        break;
      case REINSERTED:
        reinserted_.emplace_back(*last_added_idx_);
        deletion_state_ = UNKNOWN;
        break;
      default:
        break;
    }
  }
}

template<class Traits>
void DeltaPreparer<Traits>::UpdateDeletionState(RowChangeList::ChangeType op) {
  // We can't use RowChangeListDecoder.TwiddleDeleteStatus because:
  // 1. Our deletion status includes an additional UNKNOWN state.
  // 2. The logical chain of DELETEs and REINSERTs for a given row may extend
  //    across DeltaPreparer instances. For example, the same row may be deleted
  //    in one delta file and reinserted in the next. But, because
  //    DeltaPreparers cannot exchange this information in the context of batch
  //    preparation, we have to allow any state transition from UNKNOWN.
  //
  // DELETE+REINSERT pairs are reset back to UNKNOWN: these rows were both
  // deleted and reinserted in the same batch, so their states haven't actually changed.
  if (op == RowChangeList::kDelete) {
    DCHECK_NE(deletion_state_, DELETED);
    if (deletion_state_ == UNKNOWN) {
      deletion_state_ = DELETED;
    } else {
      DCHECK_EQ(deletion_state_, REINSERTED);
      deletion_state_ = UNKNOWN;
    }
  } else {
    DCHECK(op == RowChangeList::kUpdate || op == RowChangeList::kReinsert);
    if (op == RowChangeList::kReinsert) {
      DCHECK_NE(deletion_state_, REINSERTED);
      if (deletion_state_ == UNKNOWN) {
        deletion_state_ = REINSERTED;
      } else {
        DCHECK_EQ(deletion_state_, DELETED);
        deletion_state_ = UNKNOWN;
      }
    }
  }
}

// Explicit specialization for callers outside this compilation unit.
template class DeltaPreparer<DMSPreparerTraits>;
template class DeltaPreparer<DeltaFilePreparerTraits<REDO>>;
template class DeltaPreparer<DeltaFilePreparerTraits<UNDO>>;

Status DebugDumpDeltaIterator(DeltaType type,
                              DeltaIterator* iter,
                              const Schema& schema,
                              size_t nrows,
                              vector<std::string>* out) {
  ScanSpec spec;
  spec.set_cache_blocks(false);
  RETURN_NOT_OK(iter->Init(&spec));
  RETURN_NOT_OK(iter->SeekToOrdinal(0));

  const size_t kRowsPerBlock = 100;

  Arena arena(32 * 1024);
  for (size_t i = 0; iter->HasNext(); ) {
    size_t n;
    if (nrows > 0) {
      if (i >= nrows) {
        break;
      }
      n = std::min(kRowsPerBlock, nrows - i);
    } else {
      n = kRowsPerBlock;
    }

    arena.Reset();

    RETURN_NOT_OK(iter->PrepareBatch(n, DeltaIterator::PREPARE_FOR_COLLECT));
    vector<DeltaKeyAndUpdate> cells;
    RETURN_NOT_OK(iter->FilterColumnIdsAndCollectDeltas(
                      vector<ColumnId>(),
                      &cells,
                      &arena));
    for (const DeltaKeyAndUpdate& cell : cells) {
      LOG_STRING(INFO, out) << cell.Stringify(type, schema, true /*pad_key*/ );
    }

    i += n;
  }
  return Status::OK();
}

template<DeltaType Type>
Status WriteDeltaIteratorToFile(DeltaIterator* iter,
                                size_t nrows,
                                DeltaFileWriter* out) {
  ScanSpec spec;
  spec.set_cache_blocks(false);
  RETURN_NOT_OK(iter->Init(&spec));
  RETURN_NOT_OK(iter->SeekToOrdinal(0));

  const size_t kRowsPerBlock = 100;
  DeltaStats stats;
  Arena arena(32 * 1024);
  for (size_t i = 0; iter->HasNext(); ) {
    size_t n;
    if (nrows > 0) {
      if (i >= nrows) {
        break;
      }
      n = std::min(kRowsPerBlock, nrows - i);
    } else {
      n = kRowsPerBlock;
    }

    arena.Reset();

    RETURN_NOT_OK(iter->PrepareBatch(n, DeltaIterator::PREPARE_FOR_COLLECT));
    vector<DeltaKeyAndUpdate> cells;
    RETURN_NOT_OK(iter->FilterColumnIdsAndCollectDeltas(vector<ColumnId>(),
                                                        &cells,
                                                        &arena));
    for (const DeltaKeyAndUpdate& cell : cells) {
      RowChangeList rcl(cell.cell);
      RETURN_NOT_OK(out->AppendDelta<Type>(cell.key, rcl));
      RETURN_NOT_OK(stats.UpdateStats(cell.key.timestamp(), rcl));
    }

    i += n;
  }
  out->WriteDeltaStats(stats);
  return Status::OK();
}

template
Status WriteDeltaIteratorToFile<REDO>(DeltaIterator* iter,
                                      size_t nrows,
                                      DeltaFileWriter* out);

template
Status WriteDeltaIteratorToFile<UNDO>(DeltaIterator* iter,
                                      size_t nrows,
                                      DeltaFileWriter* out);

} // namespace tablet
} // namespace kudu
