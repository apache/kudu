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

#include "kudu/tablet/delta_tracker.h"

#include <algorithm>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <utility>

#include <boost/range/adaptor/reversed.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/cfile/cfile_util.h"
#include "kudu/common/iterator.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_applier.h"
#include "kudu/tablet/delta_iterator_merger.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/deltamemstore.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class RowChangeList;

namespace tablet {

using cfile::ReaderOptions;
using fs::CreateBlockOptions;
using fs::ReadableBlock;
using fs::WritableBlock;
using log::LogAnchorRegistry;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

Status DeltaTracker::Open(const shared_ptr<RowSetMetadata>& rowset_metadata,
                          rowid_t num_rows,
                          LogAnchorRegistry* log_anchor_registry,
                          const TabletMemTrackers& mem_trackers,
                          gscoped_ptr<DeltaTracker>* delta_tracker) {
  gscoped_ptr<DeltaTracker> local_dt(
      new DeltaTracker(rowset_metadata, num_rows, log_anchor_registry,
                       mem_trackers));
  RETURN_NOT_OK(local_dt->DoOpen());

  delta_tracker->swap(local_dt);
  return Status::OK();
}

DeltaTracker::DeltaTracker(shared_ptr<RowSetMetadata> rowset_metadata,
                           rowid_t num_rows,
                           LogAnchorRegistry* log_anchor_registry,
                           TabletMemTrackers mem_trackers)
    : rowset_metadata_(std::move(rowset_metadata)),
      num_rows_(num_rows),
      open_(false),
      read_only_(false),
      log_anchor_registry_(log_anchor_registry),
      mem_trackers_(std::move(mem_trackers)),
      dms_empty_(true) {}

Status DeltaTracker::OpenDeltaReaders(const vector<BlockId>& blocks,
                                      vector<shared_ptr<DeltaStore> >* stores,
                                      DeltaType type) {
  FsManager* fs = rowset_metadata_->fs_manager();
  for (const BlockId& block_id : blocks) {
    unique_ptr<ReadableBlock> block;
    Status s = fs->OpenBlock(block_id, &block);
    if (!s.ok()) {
      LOG_WITH_PREFIX(ERROR) << "Failed to open " << DeltaType_Name(type)
                             << " delta file " << block_id.ToString() << ": "
                             << s.ToString();
      return s;
    }

    shared_ptr<DeltaFileReader> dfr;
    ReaderOptions options;
    options.parent_mem_tracker = mem_trackers_.tablet_tracker;
    s = DeltaFileReader::OpenNoInit(std::move(block),
                                    type,
                                    std::move(options),
                                    &dfr);
    if (!s.ok()) {
      LOG_WITH_PREFIX(ERROR) << "Failed to open " << DeltaType_Name(type)
                             << " delta file reader " << block_id.ToString() << ": "
                             << s.ToString();
      return s;
    }

    VLOG_WITH_PREFIX(1) << "Successfully opened " << DeltaType_Name(type)
                        << " delta file " << block_id.ToString();
    stores->push_back(dfr);
  }
  return Status::OK();
}


// Open any previously flushed DeltaFiles in this rowset
Status DeltaTracker::DoOpen() {
  CHECK(redo_delta_stores_.empty()) << "should call before opening any readers";
  CHECK(undo_delta_stores_.empty()) << "should call before opening any readers";
  CHECK(!open_);

  RETURN_NOT_OK(OpenDeltaReaders(rowset_metadata_->redo_delta_blocks(),
                                 &redo_delta_stores_,
                                 REDO));
  RETURN_NOT_OK(OpenDeltaReaders(rowset_metadata_->undo_delta_blocks(),
                                 &undo_delta_stores_,
                                 UNDO));

  // the id of the first DeltaMemStore is the max id of the current ones +1
  RETURN_NOT_OK(DeltaMemStore::Create(rowset_metadata_->last_durable_redo_dms_id() + 1,
                                      rowset_metadata_->id(),
                                      log_anchor_registry_,
                                      mem_trackers_.dms_tracker,
                                      &dms_));
  RETURN_NOT_OK(dms_->Init());

  open_ = true;
  return Status::OK();
}

Status DeltaTracker::MakeDeltaIteratorMergerUnlocked(size_t start_idx, size_t end_idx,
                                                     const Schema* projection,
                                                     vector<shared_ptr<DeltaStore> > *target_stores,
                                                     vector<BlockId> *target_blocks,
                                                     std::unique_ptr<DeltaIterator> *out) {
  CHECK(open_);
  CHECK_LE(start_idx, end_idx);
  CHECK_LT(end_idx, redo_delta_stores_.size());
  vector<shared_ptr<DeltaStore> > inputs;
  for (size_t idx = start_idx; idx <= end_idx; ++idx) {
    shared_ptr<DeltaStore> &delta_store = redo_delta_stores_[idx];

    // In DEBUG mode, the following asserts that the object is of the right type
    // (using RTTI)
    ignore_result(down_cast<DeltaFileReader*>(delta_store.get()));
    shared_ptr<DeltaFileReader> dfr = std::static_pointer_cast<DeltaFileReader>(delta_store);

    LOG_WITH_PREFIX(INFO) << "Preparing to minor compact delta file: " << dfr->ToString();

    inputs.push_back(delta_store);
    target_stores->push_back(delta_store);
    target_blocks->push_back(dfr->block_id());
  }
  RETURN_NOT_OK(DeltaIteratorMerger::Create(
      inputs, projection,
      MvccSnapshot::CreateSnapshotIncludingAllTransactions(), out));
  return Status::OK();
}

namespace {

string JoinDeltaStoreStrings(const SharedDeltaStoreVector& stores) {
  vector<string> strings;
  for (const shared_ptr<DeltaStore>& store : stores) {
    strings.push_back(store->ToString());
  }
  return ::JoinStrings(strings, ",");
}

} // anonymous namespace

Status DeltaTracker::ValidateDeltaOrder(const std::shared_ptr<DeltaStore>& first,
                                        const std::shared_ptr<DeltaStore>& second,
                                        DeltaType type) {
  shared_ptr<DeltaStore> first_copy = first;
  shared_ptr<DeltaStore> second_copy = second;

  // Make clones so we don't leave the original ones initted. That can affect
  // tests. We know it's a DeltaFileReader if it's not Initted().
  if (!first_copy->Initted()) {
    shared_ptr<DeltaFileReader> first_clone;
    RETURN_NOT_OK(down_cast<DeltaFileReader*>(first.get())->CloneForDebugging(
        rowset_metadata_->fs_manager(), mem_trackers_.tablet_tracker, &first_clone));
    RETURN_NOT_OK(first_clone->Init());
    first_copy = first_clone;
  }
  if (!second_copy->Initted()) {
    shared_ptr<DeltaFileReader> second_clone;
    RETURN_NOT_OK(down_cast<DeltaFileReader*>(second.get())->CloneForDebugging(
        rowset_metadata_->fs_manager(), mem_trackers_.tablet_tracker, &second_clone));
    RETURN_NOT_OK(second_clone->Init());
    second_copy = second_clone;
  }

  switch (type) {
    case REDO:
      DCHECK_LE(first_copy->delta_stats().min_timestamp(),
                second_copy->delta_stats().min_timestamp())
          << "Found out-of-order deltas: [{" << first_copy->ToString() << "}, {"
          << second_copy->ToString() << "}]: type = " << type;
      break;
    case UNDO:
      DCHECK_GE(first_copy->delta_stats().min_timestamp(),
                second_copy->delta_stats().min_timestamp())
          << "Found out-of-order deltas: [{" << first_copy->ToString() << "}, {"
          << second_copy->ToString() << "}]: type = " << type;
      break;
  }
  return Status::OK();
}

Status DeltaTracker::ValidateDeltasOrdered(const SharedDeltaStoreVector& list, DeltaType type) {
  for (size_t i = 1; i < list.size(); i++) {
    RETURN_NOT_OK(ValidateDeltaOrder(list[i - 1], list[i], type));
  }
  return Status::OK();
}

void DeltaTracker::AtomicUpdateStores(const SharedDeltaStoreVector& stores_to_replace,
                                      const SharedDeltaStoreVector& new_stores,
                                      DeltaType type) {
  std::lock_guard<rw_spinlock> lock(component_lock_);
  SharedDeltaStoreVector* stores_to_update =
      type == REDO ? &redo_delta_stores_ : &undo_delta_stores_;
  SharedDeltaStoreVector::iterator start_it;

  // We only support two scenarios in this function:
  //
  // 1. Prepending deltas to the specified list.
  //    In the case of prepending REDO deltas, that means we should only be
  //    prepending older-timestamped data because REDOs are stored in ascending
  //    timestamp order, and in the case of UNDO deltas, that means we should
  //    only be prepending newer-timestamped data because UNDOs are stored in
  //    descending timestamp order.
  //
  // 2. Replacing a range of deltas with a replacement range.
  //    In the case of major REDO delta compaction, we are simply compacting a
  //    range of REDOs into a smaller set of REDOs.
  //
  // We validate these assumptions (in DEBUG mode only) below.

  if (stores_to_replace.empty()) {
    // With nothing to remove, we always prepend to the front of the list.
    start_it = stores_to_update->begin();
  } else {
    // With something to remove, we do a range-replace.
    start_it = std::find(stores_to_update->begin(), stores_to_update->end(), stores_to_replace[0]);
    auto end_it = start_it;
    for (const shared_ptr<DeltaStore>& ds : stores_to_replace) {
      CHECK(end_it != stores_to_update->end() && *end_it == ds) <<
          Substitute("Cannot find $0 deltastore sequence <$1> in <$2>",
                     DeltaType_Name(type),
                     JoinDeltaStoreStrings(stores_to_replace),
                     JoinDeltaStoreStrings(*stores_to_update));
      ++end_it;
    }
    // Remove the old stores.
    stores_to_update->erase(start_it, end_it);
  }

#ifndef NDEBUG
  // Perform validation checks to ensure callers do not violate our contract.
  if (!new_stores.empty()) {
    // Make sure the new stores are already ordered. Non-OK Statuses here don't
    // indicate ordering errors, but rather, physical errors (e.g. disk
    // failure). These don't indicate incorrectness and are thusly ignored.
    WARN_NOT_OK(ValidateDeltasOrdered(new_stores, type), "Could not validate delta order");
    if (start_it != stores_to_update->end()) {
      // Sanity check that the last store we are adding would logically appear
      // before the first store that would follow it.
      WARN_NOT_OK(ValidateDeltaOrder(*new_stores.rbegin(), *start_it, type),
                  "Could not validate delta order");
    }
  }
#endif // NDEBUG

  // Insert the new stores.
  stores_to_update->insert(start_it, new_stores.begin(), new_stores.end());

  VLOG_WITH_PREFIX(1) << "New " << DeltaType_Name(type) << " stores: "
                      << JoinDeltaStoreStrings(*stores_to_update);
}

Status DeltaTracker::Compact() {
  return CompactStores(0, -1);
}

Status DeltaTracker::CommitDeltaStoreMetadataUpdate(const RowSetMetadataUpdate& update,
                                                    const SharedDeltaStoreVector& to_remove,
                                                    const vector<BlockId>& new_delta_blocks,
                                                    DeltaType type,
                                                    MetadataFlushType flush_type) {
  compact_flush_lock_.AssertAcquired();

  // This method is only used for compactions and GC, not data modifications.
  // Therefore, flushing is not required for safety.
  // We enforce that with this DCHECK.
  DCHECK(!to_remove.empty());

  SharedDeltaStoreVector new_stores;
  RETURN_NOT_OK_PREPEND(OpenDeltaReaders(new_delta_blocks, &new_stores, type),
                        "Unable to open delta blocks");

  vector<BlockId> removed_blocks;
  rowset_metadata_->CommitUpdate(update, &removed_blocks);
  rowset_metadata_->AddOrphanedBlocks(removed_blocks);
  // Once we successfully commit to the rowset metadata, let's ensure we update
  // the delta stores to maintain consistency between the two.
  AtomicUpdateStores(to_remove, new_stores, type);

  if (flush_type == FLUSH_METADATA) {
    // Flushing the metadata is considered best-effort in this function.
    // No consistency problems will be visible if we don't successfully
    // Flush(), so no need to CHECK_OK here, because this function is specified
    // only to be used for compactions or ancient history data GC, which do not
    // add or subtract any user-visible ops. Compactions only swap the location
    // of ops on disk, and ancient history data GC has no user-visible effects.
    RETURN_NOT_OK(rowset_metadata_->Flush());
  }
  return Status::OK();
}

Status DeltaTracker::CheckWritableUnlocked() const {
  compact_flush_lock_.AssertAcquired();
  if (PREDICT_FALSE(read_only_)) {
    return Status::IllegalState("delta tracker has been marked read-only");
  }
  return Status::OK();
}

Status DeltaTracker::CompactStores(int start_idx, int end_idx) {
  // Prevent concurrent compactions or a compaction concurrent with a flush
  //
  // TODO(perf): this could be more fine grained
  std::lock_guard<Mutex> l(compact_flush_lock_);
  RETURN_NOT_OK(CheckWritableUnlocked());

  // At the time of writing, minor delta compaction only compacts REDO delta
  // files, so we need at least 2 REDO delta stores to proceed.
  if (CountRedoDeltaStores() <= 1) {
    return Status::OK();
  }

  if (end_idx == -1) {
    end_idx = redo_delta_stores_.size() - 1;
  }

  CHECK_LE(start_idx, end_idx);
  CHECK_LT(end_idx, redo_delta_stores_.size());
  CHECK(open_);

  // Open a writer for the new destination delta block
  FsManager* fs = rowset_metadata_->fs_manager();
  unique_ptr<WritableBlock> block;
  CreateBlockOptions opts({ rowset_metadata_->tablet_metadata()->tablet_id() });
  RETURN_NOT_OK_PREPEND(fs->CreateNewBlock(opts, &block),
                        "Could not allocate delta block");
  BlockId new_block_id(block->id());

  // Merge and compact the stores.
  vector<shared_ptr<DeltaStore> > compacted_stores;
  vector<BlockId> compacted_blocks;
  RETURN_NOT_OK(DoCompactStores(start_idx, end_idx, std::move(block),
                                &compacted_stores, &compacted_blocks));

  vector<BlockId> new_blocks = { new_block_id };
  RowSetMetadataUpdate update;
  update.ReplaceRedoDeltaBlocks(compacted_blocks, new_blocks);

  LOG_WITH_PREFIX(INFO) << "Flushing compaction of redo delta blocks { " << compacted_blocks
                        << " } into block " << new_block_id;
  RETURN_NOT_OK_PREPEND(CommitDeltaStoreMetadataUpdate(update, compacted_stores, new_blocks, REDO,
                                                       FLUSH_METADATA),
                        "DeltaTracker: CompactStores: Unable to commit delta update");
  return Status::OK();
}

Status DeltaTracker::EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp ancient_history_mark,
                                                                 int64_t* bytes) {
  DCHECK_NE(Timestamp::kInvalidTimestamp, ancient_history_mark);
  DCHECK(bytes);
  SharedDeltaStoreVector undos_newest_first;
  CollectStores(&undos_newest_first, UNDOS_ONLY);

  int64_t tmp_bytes = 0;
  for (const auto& undo : boost::adaptors::reverse(undos_newest_first)) {
    // Short-circuit once we hit an initialized delta block with 'max_timestamp' > AHM.
    if (undo->Initted() &&
        undo->delta_stats().max_timestamp() >= ancient_history_mark) {
      break;
    }
    tmp_bytes += undo->EstimateSize(); // Can be called before Init().
  }

  *bytes = tmp_bytes;
  return Status::OK();
}

Status DeltaTracker::InitUndoDeltas(Timestamp ancient_history_mark,
                                    MonoTime deadline,
                                    int64_t* delta_blocks_initialized,
                                    int64_t* bytes_in_ancient_undos) {
  SharedDeltaStoreVector undos_newest_first;
  CollectStores(&undos_newest_first, UNDOS_ONLY);
  int64_t tmp_blocks_initialized = 0;
  int64_t tmp_bytes_in_ancient_undos = 0;

  // Traverse oldest-first, initializing delta stores as we go.
  for (auto& undo : boost::adaptors::reverse(undos_newest_first)) {
    if (deadline.Initialized() && MonoTime::Now() >= deadline) break;

    if (!undo->Initted()) {
      RETURN_NOT_OK(undo->Init());
      tmp_blocks_initialized++;
    }

    // Stop initializing delta files once we start hitting newer deltas that
    // are not GC'able.
    if (ancient_history_mark != Timestamp::kInvalidTimestamp &&
        undo->delta_stats().max_timestamp() >= ancient_history_mark) break;

    // We only want to count the bytes in the ancient undos so this needs to
    // come after the short-circuit above.
    tmp_bytes_in_ancient_undos += undo->EstimateSize();
  }

  if (delta_blocks_initialized) *delta_blocks_initialized = tmp_blocks_initialized;
  if (bytes_in_ancient_undos) *bytes_in_ancient_undos = tmp_bytes_in_ancient_undos;
  return Status::OK();
}

Status DeltaTracker::DeleteAncientUndoDeltas(Timestamp ancient_history_mark,
                                             int64_t* blocks_deleted, int64_t* bytes_deleted) {
  DCHECK_NE(Timestamp::kInvalidTimestamp, ancient_history_mark);

  // Performing data GC is similar is many ways to a compaction. We are
  // updating both the rowset metadata and the delta stores in this method, so
  // we need to be the only thread doing a flush or a compaction on this RowSet
  // while we do our work.
  std::lock_guard<Mutex> l(compact_flush_lock_);
  RETURN_NOT_OK(CheckWritableUnlocked());

  // Get the list of undo deltas.
  SharedDeltaStoreVector undos_newest_first;
  CollectStores(&undos_newest_first, UNDOS_ONLY);

  if (undos_newest_first.empty()) {
    *blocks_deleted = 0;
    *bytes_deleted = 0;
    return Status::OK();
  }

  SharedDeltaStoreVector undos_to_remove;
  vector<BlockId> block_ids_to_remove;

  int64_t tmp_blocks_deleted = 0;
  int64_t tmp_bytes_deleted = 0;

  // Traverse oldest-first.
  for (auto& undo : boost::adaptors::reverse(undos_newest_first)) {
    if (!undo->Initted()) break; // Never initialize the deltas in this code path (it's slow).
    if (undo->delta_stats().max_timestamp() >= ancient_history_mark) break;
    tmp_blocks_deleted++;
    tmp_bytes_deleted += undo->EstimateSize();
    // This is always a safe downcast because UNDO deltas are always on disk.
    block_ids_to_remove.push_back(down_cast<DeltaFileReader*>(undo.get())->block_id());
    undos_to_remove.push_back(std::move(undo));
  }
  undos_newest_first.clear(); // We did a std::move() on some elements from this vector above.

  // Only flush the rowset metadata if we are going to modify it.
  if (!undos_to_remove.empty()) {
    // We iterated in reverse order and CommitDeltaStoreMetadataUpdate() requires storage order.
    std::reverse(undos_to_remove.begin(), undos_to_remove.end());
    RowSetMetadataUpdate update;
    update.RemoveUndoDeltaBlocks(block_ids_to_remove);
    // We do not flush the tablet metadata - that is the caller's responsibility.
    RETURN_NOT_OK(CommitDeltaStoreMetadataUpdate(update, undos_to_remove, {}, UNDO,
                                                 NO_FLUSH_METADATA));
  }

  if (blocks_deleted) *blocks_deleted = tmp_blocks_deleted;
  if (bytes_deleted) *bytes_deleted = tmp_bytes_deleted;
  return Status::OK();
}

Status DeltaTracker::DoCompactStores(size_t start_idx, size_t end_idx,
         unique_ptr<WritableBlock> block,
         vector<shared_ptr<DeltaStore> > *compacted_stores,
         vector<BlockId> *compacted_blocks) {
  unique_ptr<DeltaIterator> inputs_merge;

  // Currently, DeltaFile iterators ignore the passed-in projection in
  // FilterColumnIdsAndCollectDeltas(). So, we just pass an empty schema here.
  // If this changes in the future, we'll have to pass in the current tablet
  // schema here.
  Schema empty_schema;
  RETURN_NOT_OK(MakeDeltaIteratorMergerUnlocked(start_idx, end_idx, &empty_schema, compacted_stores,
                                                compacted_blocks, &inputs_merge));
  LOG_WITH_PREFIX(INFO) << "Compacting " << (end_idx - start_idx + 1) << " delta files.";
  DeltaFileWriter dfw(std::move(block));
  RETURN_NOT_OK(dfw.Start());
  RETURN_NOT_OK(WriteDeltaIteratorToFile<REDO>(inputs_merge.get(),
                                               ITERATE_OVER_ALL_ROWS,
                                               &dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG_WITH_PREFIX(INFO) << "Succesfully compacted the specified delta files.";
  return Status::OK();
}

void DeltaTracker::CollectStores(vector<shared_ptr<DeltaStore>>* deltas,
                                 WhichStores which) const {
  std::lock_guard<rw_spinlock> lock(component_lock_);
  if (which != REDOS_ONLY) {
    deltas->assign(undo_delta_stores_.begin(), undo_delta_stores_.end());
  }
  if (which != UNDOS_ONLY) {
    deltas->insert(deltas->end(), redo_delta_stores_.begin(), redo_delta_stores_.end());
    deltas->push_back(dms_);
  }
}

Status DeltaTracker::NewDeltaIterator(const Schema* schema,
                                      const MvccSnapshot& snap,
                                      WhichStores which,
                                      unique_ptr<DeltaIterator>* out) const {
  std::vector<shared_ptr<DeltaStore> > stores;
  CollectStores(&stores, which);
  return DeltaIteratorMerger::Create(stores, schema, snap, out);
}

Status DeltaTracker::NewDeltaFileIterator(
    const Schema* schema,
    const MvccSnapshot& snap,
    DeltaType type,
    vector<shared_ptr<DeltaStore> >* included_stores,
    unique_ptr<DeltaIterator>* out) const {
  {
    std::lock_guard<rw_spinlock> lock(component_lock_);
    // TODO perf: is this really needed? Will check
    // DeltaIteratorMerger::Create()
    if (type == UNDO) {
      *included_stores = undo_delta_stores_;
    } else if (type == REDO) {
      *included_stores = redo_delta_stores_;
    } else {
      LOG_WITH_PREFIX(FATAL);
    }
  }

  // Verify that we're only merging files and not DeltaMemStores.
  // TODO: we need to somehow ensure this doesn't fail - we have to somehow coordinate
  // minor delta compaction against delta flush. Add a test case here to trigger this
  // condition.
  for (const shared_ptr<DeltaStore>& store : *included_stores) {
    ignore_result(down_cast<DeltaFileReader*>(store.get()));
  }

  return DeltaIteratorMerger::Create(*included_stores, schema, snap, out);
}

Status DeltaTracker::WrapIterator(const shared_ptr<CFileSet::Iterator> &base,
                                  const MvccSnapshot &mvcc_snap,
                                  gscoped_ptr<ColumnwiseIterator>* out) const {
  unique_ptr<DeltaIterator> iter;
  RETURN_NOT_OK(NewDeltaIterator(&base->schema(), mvcc_snap, &iter));

  out->reset(new DeltaApplier(base, std::move(iter)));
  return Status::OK();
}

Status DeltaTracker::Update(Timestamp timestamp,
                            rowid_t row_idx,
                            const RowChangeList &update,
                            const consensus::OpId& op_id,
                            OperationResultPB* result) {
  // TODO: can probably lock this more fine-grained.
  shared_lock<rw_spinlock> lock(component_lock_);
  DCHECK_LT(row_idx, num_rows_);

  Status s = dms_->Update(timestamp, row_idx, update, op_id);
  if (s.ok()) {
    dms_empty_.Store(false);

    MemStoreTargetPB* target = result->add_mutated_stores();
    target->set_rs_id(rowset_metadata_->id());
    target->set_dms_id(dms_->id());
  }
  return s;
}

Status DeltaTracker::CheckRowDeleted(rowid_t row_idx, bool *deleted,
                                     ProbeStats* stats) const {
  shared_lock<rw_spinlock> lock(component_lock_);

  DCHECK_LT(row_idx, num_rows_);

  *deleted = false;
  // Check if the row has a deletion in DeltaMemStore.
  RETURN_NOT_OK(dms_->CheckRowDeleted(row_idx, deleted));
  if (*deleted) {
    return Status::OK();
  }

  // Then check backwards through the list of trackers.
  for (auto ds = redo_delta_stores_.crbegin(); ds != redo_delta_stores_.crend(); ds++) {
    stats->deltas_consulted++;
    RETURN_NOT_OK((*ds)->CheckRowDeleted(row_idx, deleted));
    if (*deleted) {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status DeltaTracker::FlushDMS(DeltaMemStore* dms,
                              shared_ptr<DeltaFileReader>* dfr,
                              MetadataFlushType flush_type) {
  // Open file for write.
  FsManager* fs = rowset_metadata_->fs_manager();
  unique_ptr<WritableBlock> writable_block;
  CreateBlockOptions opts({ rowset_metadata_->tablet_metadata()->tablet_id() });
  RETURN_NOT_OK_PREPEND(fs->CreateNewBlock(opts, &writable_block),
                        "Unable to allocate new delta data writable_block");
  BlockId block_id(writable_block->id());

  DeltaFileWriter dfw(std::move(writable_block));
  RETURN_NOT_OK_PREPEND(dfw.Start(),
                        Substitute("Unable to start writing to delta block $0",
                                   block_id.ToString()));

  gscoped_ptr<DeltaStats> stats;
  RETURN_NOT_OK(dms->FlushToFile(&dfw, &stats));
  RETURN_NOT_OK(dfw.Finish());
  LOG_WITH_PREFIX(INFO) << "Flushed delta block: " << block_id.ToString()
                        << " ts range: [" << stats->min_timestamp()
                        << ", " << stats->max_timestamp() << "]";

  // Now re-open for read
  unique_ptr<ReadableBlock> readable_block;
  RETURN_NOT_OK(fs->OpenBlock(block_id, &readable_block));
  ReaderOptions options;
  options.parent_mem_tracker = mem_trackers_.tablet_tracker;
  RETURN_NOT_OK(DeltaFileReader::OpenNoInit(std::move(readable_block),
                                            REDO,
                                            std::move(options),
                                            dfr));
  LOG_WITH_PREFIX(INFO) << "Reopened delta block for read: " << block_id.ToString();

  RETURN_NOT_OK(rowset_metadata_->CommitRedoDeltaDataBlock(dms->id(), block_id));
  if (flush_type == FLUSH_METADATA) {
    RETURN_NOT_OK_PREPEND(rowset_metadata_->Flush(),
                          Substitute("Unable to commit Delta block metadata for: $0",
                                     block_id.ToString()));
  }
  return Status::OK();
}

Status DeltaTracker::Flush(MetadataFlushType flush_type) {
  std::lock_guard<Mutex> l(compact_flush_lock_);
  RETURN_NOT_OK(CheckWritableUnlocked());

  // First, swap out the old DeltaMemStore a new one,
  // and add it to the list of delta stores to be reflected
  // in reads.
  shared_ptr<DeltaMemStore> old_dms;
  size_t count;
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers.
    std::lock_guard<rw_spinlock> lock(component_lock_);

    count = dms_->Count();

    // Swap the DeltaMemStore to use the new schema
    old_dms = dms_;
    RETURN_NOT_OK(DeltaMemStore::Create(old_dms->id() + 1,
                                        rowset_metadata_->id(),
                                        log_anchor_registry_,
                                        mem_trackers_.dms_tracker,
                                        &dms_));
    RETURN_NOT_OK(dms_->Init());
    dms_empty_.Store(true);

    if (count == 0) {
      // No need to flush if there are no deltas.
      // Ensure that the DeltaMemStore is using the latest schema.
      return Status::OK();
    }

    redo_delta_stores_.push_back(old_dms);
  }

  LOG_WITH_PREFIX(INFO) << "Flushing " << count << " deltas from DMS " << old_dms->id() << "...";

  // Now, actually flush the contents of the old DMS.
  //
  // TODO(todd): need another lock to prevent concurrent flushers
  // at some point.
  shared_ptr<DeltaFileReader> dfr;
  Status s = FlushDMS(old_dms.get(), &dfr, flush_type);
  if (PREDICT_FALSE(!s.ok())) {
    // A failure here leaves a DeltaMemStore permanently in the store list.
    // This isn't allowed, and rolling back the store is difficult, so we leave
    // the delta tracker in an safe, read-only state.
    CHECK(s.IsDiskFailure()) << LogPrefix() << s.ToString();
    read_only_ = true;
    return s;
  }

  // Now, re-take the lock and swap in the DeltaFileReader in place of
  // of the DeltaMemStore
  {
    std::lock_guard<rw_spinlock> lock(component_lock_);
    size_t idx = redo_delta_stores_.size() - 1;

    CHECK_EQ(redo_delta_stores_[idx], old_dms)
        << "Another thread modified the delta store list during flush";
    redo_delta_stores_[idx] = dfr;
  }

  return Status::OK();
}

size_t DeltaTracker::DeltaMemStoreSize() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return dms_->EstimateSize();
}

int64_t DeltaTracker::MinUnflushedLogIndex() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return dms_->MinLogIndex();
}

size_t DeltaTracker::CountUndoDeltaStores() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return undo_delta_stores_.size();
}

size_t DeltaTracker::CountRedoDeltaStores() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return redo_delta_stores_.size();
}

uint64_t DeltaTracker::UndoDeltaOnDiskSize() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  uint64_t size = 0;
  for (const shared_ptr<DeltaStore>& ds : undo_delta_stores_) {
    size += ds->EstimateSize();
  }
  return size;
}

uint64_t DeltaTracker::RedoDeltaOnDiskSize() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  uint64_t size = 0;
  for (const shared_ptr<DeltaStore>& ds : redo_delta_stores_) {
    size += ds->EstimateSize();
  }
  return size;
}

void DeltaTracker::GetColumnIdsWithUpdates(std::vector<ColumnId>* col_ids) const {
  shared_lock<rw_spinlock> lock(component_lock_);

  set<ColumnId> column_ids_with_updates;
  for (const shared_ptr<DeltaStore>& ds : redo_delta_stores_) {
    // We won't force open files just to read their stats.
    if (!ds->Initted()) {
      continue;
    }

    ds->delta_stats().AddColumnIdsWithUpdates(&column_ids_with_updates);
  }
  col_ids->assign(column_ids_with_updates.begin(), column_ids_with_updates.end());
}

Status DeltaTracker::InitAllDeltaStoresForTests(WhichStores stores) {
  shared_lock<rw_spinlock> lock(component_lock_);
  if (stores == UNDOS_AND_REDOS || stores == UNDOS_ONLY) {
    for (const shared_ptr<DeltaStore>& ds : undo_delta_stores_) {
      RETURN_NOT_OK(ds->Init());
    }
  }
  if (stores == UNDOS_AND_REDOS || stores == REDOS_ONLY) {
    for (const shared_ptr<DeltaStore>& ds : redo_delta_stores_) {
      RETURN_NOT_OK(ds->Init());
    }
  }
  return Status::OK();
}

string DeltaTracker::LogPrefix() const {
  return Substitute("T $0 P $1: ",
                    rowset_metadata_->tablet_metadata()->tablet_id(),
                    rowset_metadata_->fs_manager()->uuid());
}

} // namespace tablet
} // namespace kudu
