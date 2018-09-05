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
#include <memory>
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
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/debug-util.h"
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

DeltaPreparer::DeltaPreparer(RowIteratorOptions opts)
    : opts_(std::move(opts)),
      cur_prepared_idx_(0),
      prev_prepared_idx_(0),
      prepared_for_(NOT_PREPARED) {
}

void DeltaPreparer::Seek(rowid_t row_idx) {
  prev_prepared_idx_ = row_idx;
  cur_prepared_idx_ = row_idx;
  prepared_for_ = NOT_PREPARED;
}

void DeltaPreparer::Start(DeltaIterator::PrepareFlag flag) {
  if (updates_by_col_.empty()) {
    updates_by_col_.resize(opts_.projection->num_columns());
  }
  for (UpdatesForColumn& ufc : updates_by_col_) {
    ufc.clear();
  }
  deleted_.clear();
  prepared_deltas_.clear();
  switch (flag) {
    case DeltaIterator::PREPARE_FOR_APPLY:
      prepared_for_ = PREPARED_FOR_APPLY;
      break;
    case DeltaIterator::PREPARE_FOR_COLLECT:
      prepared_for_ = PREPARED_FOR_COLLECT;
      break;
    default:
      LOG(FATAL) << "Unknown prepare flag: " << flag;
  }
}

void DeltaPreparer::Finish(size_t nrows) {
  prev_prepared_idx_ = cur_prepared_idx_;
  cur_prepared_idx_ += nrows;
}

Status DeltaPreparer::AddDelta(const DeltaKey& key, Slice val) {
  if (!opts_.snap_to_include.IsCommitted(key.timestamp())) {
    return Status::OK();
  }

  if (prepared_for_ == PREPARED_FOR_APPLY) {
    RowChangeListDecoder decoder((RowChangeList(val)));
    decoder.InitNoSafetyChecks();
    DCHECK(!decoder.is_reinsert()) << "Reinserts are not supported in the DeltaMemStore.";
    if (decoder.is_delete()) {
      deleted_.emplace_back(key.row_idx());
    } else {
      DCHECK(decoder.is_update());
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
  } else {
    DCHECK_EQ(prepared_for_, PREPARED_FOR_COLLECT);
    PreparedDelta d;
    d.key = key;
    d.val = val;
    prepared_deltas_.emplace_back(d);
  }

  return Status::OK();
}

Status DeltaPreparer::ApplyUpdates(size_t col_to_apply, ColumnBlock* dst) {
  DCHECK_EQ(prepared_for_, PREPARED_FOR_APPLY);
  DCHECK_EQ(cur_prepared_idx_ - prev_prepared_idx_, dst->nrows());

  const ColumnSchema* col_schema = &opts_.projection->column(col_to_apply);
  for (const ColumnUpdate& cu : updates_by_col_[col_to_apply]) {
    int32_t idx_in_block = cu.row_id - prev_prepared_idx_;
    DCHECK_GE(idx_in_block, 0);
    SimpleConstCell src(col_schema, cu.new_val_ptr);
    ColumnBlock::Cell dst_cell = dst->cell(idx_in_block);
    RETURN_NOT_OK(CopyCell(src, &dst_cell, dst->arena()));
  }

  return Status::OK();
}

Status DeltaPreparer::ApplyDeletes(SelectionVector* sel_vec) {
  DCHECK_EQ(prepared_for_, PREPARED_FOR_APPLY);
  DCHECK_EQ(cur_prepared_idx_ - prev_prepared_idx_, sel_vec->nrows());

  for (const auto& row_id : deleted_) {
    uint32_t idx_in_block = row_id - prev_prepared_idx_;
    sel_vec->SetRowUnselected(idx_in_block);
  }

  return Status::OK();
}

Status DeltaPreparer::CollectMutations(vector<Mutation*>* dst, Arena* arena) {
  DCHECK_EQ(prepared_for_, PREPARED_FOR_COLLECT);
  for (const PreparedDelta& src : prepared_deltas_) {
    DeltaKey key = src.key;
    RowChangeList changelist(src.val);
    uint32_t rel_idx = key.row_idx() - prev_prepared_idx_;

    Mutation *mutation = Mutation::CreateInArena(arena, key.timestamp(), changelist);
    mutation->PrependToList(&dst->at(rel_idx));
  }
  return Status::OK();
}

Status DeltaPreparer::FilterColumnIdsAndCollectDeltas(const vector<ColumnId>& /*col_ids*/,
                                                      vector<DeltaKeyAndUpdate>* /*out*/,
                                                      Arena* /*arena*/) {
  LOG(DFATAL) << "Attempt to call FilterColumnIdsAndCollectDeltas on DMS" << GetStackTrace();
  return Status::InvalidArgument("FilterColumsAndAppend() is not supported by DMSIterator");
}

bool DeltaPreparer::MayHaveDeltas() const {
  DCHECK_EQ(prepared_for_, PREPARED_FOR_APPLY);
  if (!deleted_.empty()) {
    return true;
  }
  for (auto& col : updates_by_col_) {
    if (!col.empty()) {
      return true;
    }
  }
  return false;
}

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
