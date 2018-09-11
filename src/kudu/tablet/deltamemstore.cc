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

#include "kudu/tablet/deltamemstore.h"

#include <ostream>
#include <utility>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/row_changelist.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memcmpable_varint.h"
#include "kudu/util/memory/memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

using fs::IOContext;
using log::LogAnchorRegistry;
using std::string;
using std::shared_ptr;
using std::vector;
using strings::Substitute;

////////////////////////////////////////////////////////////
// DeltaMemStore implementation
////////////////////////////////////////////////////////////

static const int kInitialArenaSize = 16;

Status DeltaMemStore::Create(int64_t id,
                             int64_t rs_id,
                             LogAnchorRegistry* log_anchor_registry,
                             shared_ptr<MemTracker> parent_tracker,
                             shared_ptr<DeltaMemStore>* dms) {
  shared_ptr<DeltaMemStore> local_dms(new DeltaMemStore(id, rs_id,
                                                        log_anchor_registry,
                                                        std::move(parent_tracker)));

  dms->swap(local_dms);
  return Status::OK();
}

DeltaMemStore::DeltaMemStore(int64_t id,
                             int64_t rs_id,
                             LogAnchorRegistry* log_anchor_registry,
                             shared_ptr<MemTracker> parent_tracker)
  : id_(id),
    rs_id_(rs_id),
    allocator_(new MemoryTrackingBufferAllocator(
        HeapBufferAllocator::Get(), std::move(parent_tracker))),
    arena_(new ThreadSafeMemoryTrackingArena(kInitialArenaSize, allocator_)),
    tree_(arena_),
    anchorer_(log_anchor_registry,
              Substitute("Rowset-$0/DeltaMemStore-$1", rs_id_, id_)),
    disambiguator_sequence_number_(0) {
}

Status DeltaMemStore::Init(const IOContext* /*io_context*/) {
  return Status::OK();
}

Status DeltaMemStore::Update(Timestamp timestamp,
                             rowid_t row_idx,
                             const RowChangeList &update,
                             const consensus::OpId& op_id) {
  DeltaKey key(row_idx, timestamp);

  faststring buf;

  key.EncodeTo(&buf);

  Slice key_slice(buf);
  btree::PreparedMutation<DMSTreeTraits> mutation(key_slice);
  mutation.Prepare(&tree_);
  if (PREDICT_FALSE(mutation.exists())) {
    // We already have a delta for this row at the same timestamp.
    // Try again with a disambiguating sequence number appended to the key.
    int seq = disambiguator_sequence_number_.Increment();
    PutMemcmpableVarint64(&buf, seq);
    key_slice = Slice(buf);
    mutation.Reset(key_slice);
    mutation.Prepare(&tree_);
    CHECK(!mutation.exists())
      << "Appended a sequence number but still hit a duplicate "
      << "for rowid " << row_idx << " at timestamp " << timestamp;
  }
  if (PREDICT_FALSE(!mutation.Insert(update.slice()))) {
    return Status::IOError("Unable to insert into tree");
  }

  anchorer_.AnchorIfMinimum(op_id.index());

  return Status::OK();
}

Status DeltaMemStore::FlushToFile(DeltaFileWriter *dfw,
                                  gscoped_ptr<DeltaStats>* stats_ret) {
  gscoped_ptr<DeltaStats> stats(new DeltaStats());

  gscoped_ptr<DMSTreeIter> iter(tree_.NewIterator());
  iter->SeekToStart();
  while (iter->IsValid()) {
    Slice key_slice, val;
    iter->GetCurrentEntry(&key_slice, &val);
    DeltaKey key;
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    RowChangeList rcl(val);
    RETURN_NOT_OK_PREPEND(dfw->AppendDelta<REDO>(key, rcl), "Failed to append delta");
    stats->UpdateStats(key.timestamp(), rcl);
    iter->Next();
  }
  dfw->WriteDeltaStats(*stats);

  stats_ret->swap(stats);
  return Status::OK();
}

Status DeltaMemStore::NewDeltaIterator(const RowIteratorOptions& opts,
                                       DeltaIterator** iterator) const {
  *iterator = new DMSIterator(shared_from_this(), opts);
  return Status::OK();
}

Status DeltaMemStore::CheckRowDeleted(rowid_t row_idx,
                                      const IOContext* /*io_context*/,
                                      bool *deleted) const {
  *deleted = false;

  DeltaKey key(row_idx, Timestamp(0));
  faststring buf;
  key.EncodeTo(&buf);
  Slice key_slice(buf);

  bool exact;

  // TODO(unknown): can we avoid the allocation here?
  gscoped_ptr<DMSTreeIter> iter(tree_.NewIterator());
  if (!iter->SeekAtOrAfter(key_slice, &exact)) {
    return Status::OK();
  }

  while (iter->IsValid()) {
    // Iterate forward until reaching an entry with a larger row idx.
    Slice key_slice, v;
    iter->GetCurrentEntry(&key_slice, &v);
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    DCHECK_GE(key.row_idx(), row_idx);
    if (key.row_idx() != row_idx) break;

    RowChangeList val(v);
    // Mutation is for the target row, check deletion status.
    RowChangeListDecoder decoder((RowChangeList(v)));
    decoder.InitNoSafetyChecks();
    decoder.TwiddleDeleteStatus(deleted);

    iter->Next();
  }

  return Status::OK();
}

void DeltaMemStore::DebugPrint() const {
  tree_.DebugPrint();
}

////////////////////////////////////////////////////////////
// DMSIterator
////////////////////////////////////////////////////////////

DMSIterator::DMSIterator(const shared_ptr<const DeltaMemStore>& dms,
                         RowIteratorOptions opts)
    : dms_(dms),
      preparer_(std::move(opts)),
      iter_(dms->tree_.NewIterator()),
      seeked_(false) {}

Status DMSIterator::Init(ScanSpec* /*spec*/) {
  initted_ = true;
  return Status::OK();
}

Status DMSIterator::SeekToOrdinal(rowid_t row_idx) {
  faststring buf;
  DeltaKey key(row_idx, Timestamp(0));
  key.EncodeTo(&buf);

  bool exact; /* unused */
  iter_->SeekAtOrAfter(Slice(buf), &exact);
  preparer_.Seek(row_idx);
  seeked_ = true;
  return Status::OK();
}

Status DMSIterator::PrepareBatch(size_t nrows, int prepare_flags) {
  // This current implementation copies the whole batch worth of deltas
  // into a buffer local to this iterator, after filtering out deltas which
  // aren't yet committed in the current MVCC snapshot. The theory behind
  // this approach is the following:

  // Each batch needs to be processed once per column, meaning that unless
  // we make a local copy, we'd have to reset the CBTree iterator back to the
  // start of the batch and re-iterate for each column. CBTree iterators make
  // local copies as they progress in order to shield from concurrent mutation,
  // so with N columns, we'd end up making N copies of the data. Making a local
  // copy here is instead a single copy of the data, so is likely faster.
  CHECK(seeked_);
  DCHECK(initted_) << "must init";
  rowid_t start_row = preparer_.cur_prepared_idx();
  rowid_t stop_row = start_row + nrows - 1;

  preparer_.Start(prepare_flags);
  bool finished_row = false;
  while (iter_->IsValid()) {
    Slice key_slice, val;
    iter_->GetCurrentEntry(&key_slice, &val);
    DeltaKey key;
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    rowid_t cur_row = key.row_idx();
    DCHECK_GE(cur_row, start_row);

    // If this delta is for the same row as before, skip it if the previous
    // AddDelta() call told us that we're done with this row.
    if (preparer_.last_added_idx() &&
        preparer_.last_added_idx() == cur_row &&
        finished_row) {
      iter_->Next();
      continue;
    }
    finished_row = false;

    if (cur_row > stop_row) {
      // Delta is for a row which comes after the block we're processing.
      break;
    }

    // Note: if AddDelta() sets 'finished_row' to true, we could skip the
    // remaining deltas for this row by seeking the tree iterator. This trades
    // off the cost of a seek against the cost of decoding some irrelevant delta
    // keys. Experimentation with a microbenchmark revealed that only when ~50
    // deltas were skipped was the seek cheaper than the decoding.
    //
    // Given that updates are expected to be uncommon and that most scans are
    // _not_ historical, the current implementation eschews seeking in favor of
    // skipping irrelevant deltas one by one.
    RETURN_NOT_OK(preparer_.AddDelta(key, val, &finished_row));
    iter_->Next();
  }
  preparer_.Finish(nrows);
  return Status::OK();
}

Status DMSIterator::ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                                 const SelectionVector& filter) {
  return preparer_.ApplyUpdates(col_to_apply, dst, filter);
}

Status DMSIterator::ApplyDeletes(SelectionVector* sel_vec) {
  return preparer_.ApplyDeletes(sel_vec);
}


Status DMSIterator::CollectMutations(vector<Mutation*>*dst, Arena* arena) {
  return preparer_.CollectMutations(dst, arena);
}

Status DMSIterator::FilterColumnIdsAndCollectDeltas(const vector<ColumnId>& col_ids,
                                                    vector<DeltaKeyAndUpdate>* out,
                                                    Arena* arena) {
  return preparer_.FilterColumnIdsAndCollectDeltas(col_ids, out, arena);
}

bool DMSIterator::HasNext() {
  return iter_->IsValid();
}

bool DMSIterator::MayHaveDeltas() const {
  return preparer_.MayHaveDeltas();
}

string DMSIterator::ToString() const {
  return "DMSIterator";
}

} // namespace tablet
} // namespace kudu
