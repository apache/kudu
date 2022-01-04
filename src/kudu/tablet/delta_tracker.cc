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
#include <type_traits>
#include <utility>

#include <boost/optional/optional.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <glog/logging.h>

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
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

namespace kudu {

class RowChangeList;

namespace tablet {

using cfile::ReaderOptions;
using fs::CreateBlockOptions;
using fs::IOContext;
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
                          LogAnchorRegistry* log_anchor_registry,
                          const TabletMemTrackers& mem_trackers,
                          const IOContext* io_context,
                          unique_ptr<DeltaTracker>* delta_tracker) {
  unique_ptr<DeltaTracker> local_dt(
      new DeltaTracker(rowset_metadata, log_anchor_registry,
                       mem_trackers));
  RETURN_NOT_OK(local_dt->DoOpen(io_context));

  *delta_tracker = std::move(local_dt);
  return Status::OK();
}

DeltaTracker::DeltaTracker(shared_ptr<RowSetMetadata> rowset_metadata,
                           LogAnchorRegistry* log_anchor_registry,
                           TabletMemTrackers mem_trackers)
    : rowset_metadata_(std::move(rowset_metadata)),
      open_(false),
      read_only_(false),
      log_anchor_registry_(log_anchor_registry),
      mem_trackers_(std::move(mem_trackers)),
      next_dms_id_(rowset_metadata_->last_durable_redo_dms_id() + 1),
      dms_exists_(false),
      deleted_row_count_(0) {}

Status DeltaTracker::OpenDeltaReaders(vector<DeltaBlockIdAndStats> blocks,
                                      const IOContext* io_context,
                                      vector<shared_ptr<DeltaStore> >* stores,
                                      DeltaType type) {
  FsManager* fs = rowset_metadata_->fs_manager();
  for (auto& block_and_stats : blocks) {
    const auto& block_id = block_and_stats.first;
    unique_ptr<DeltaStats> stats = std::move(block_and_stats.second);
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
    options.io_context = io_context;
    s = DeltaFileReader::OpenNoInit(std::move(block),
                                    type,
                                    std::move(options),
                                    std::move(stats),
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
Status DeltaTracker::DoOpen(const IOContext* io_context) {
  CHECK(redo_delta_stores_.empty()) << "should call before opening any readers";
  CHECK(undo_delta_stores_.empty()) << "should call before opening any readers";
  CHECK(!open_);

  vector<DeltaBlockIdAndStats> redos;
  for (auto block_id : rowset_metadata_->redo_delta_blocks()) {
    redos.emplace_back(std::make_pair(block_id, nullptr));
  }
  RETURN_NOT_OK(OpenDeltaReaders(std::move(redos),
                                 io_context,
                                 &redo_delta_stores_,
                                 REDO));
  vector<DeltaBlockIdAndStats> undos;
  for (auto block_id : rowset_metadata_->undo_delta_blocks()) {
    undos.emplace_back(std::make_pair(block_id, nullptr));
  }
  RETURN_NOT_OK(OpenDeltaReaders(std::move(undos),
                                 io_context,
                                 &undo_delta_stores_,
                                 UNDO));

  open_ = true;
  return Status::OK();
}

Status DeltaTracker::CreateAndInitDMSUnlocked(const fs::IOContext* io_context) {
  DCHECK(component_lock_.is_write_locked());
  shared_ptr<DeltaMemStore> dms;
  RETURN_NOT_OK(DeltaMemStore::Create(next_dms_id_++,
                                      rowset_metadata_->id(),
                                      log_anchor_registry_,
                                      mem_trackers_.dms_tracker,
                                      &dms));
  RETURN_NOT_OK(dms->Init(io_context));

  dms_ = std::move(dms);
  dms_exists_.Store(true);
  return Status::OK();
}

Status DeltaTracker::MakeDeltaIteratorMergerUnlocked(const IOContext* io_context,
                                                     size_t start_idx, size_t end_idx,
                                                     SchemaPtr projection,
                                                     SharedDeltaStoreVector* target_stores,
                                                     vector<BlockId> *target_blocks,
                                                     std::unique_ptr<DeltaIterator>* out) {
  CHECK(open_);
  CHECK_LE(start_idx, end_idx);
  CHECK_LT(end_idx, redo_delta_stores_.size());
  SharedDeltaStoreVector inputs;
  int64_t delete_count = 0;
  int64_t reinsert_count = 0;
  int64_t update_count = 0;
  for (size_t idx = start_idx; idx <= end_idx; ++idx) {
    shared_ptr<DeltaStore> &delta_store = redo_delta_stores_[idx];

    // In DEBUG mode, the following asserts that the object is of the right type
    // (using RTTI)
    ignore_result(down_cast<DeltaFileReader*>(delta_store.get()));
    shared_ptr<DeltaFileReader> dfr = std::static_pointer_cast<DeltaFileReader>(delta_store);

    if (dfr->has_delta_stats()) {
      delete_count += dfr->delta_stats().delete_count();
      reinsert_count += dfr->delta_stats().reinsert_count();
      update_count += dfr->delta_stats().UpdateCount();
    }
    VLOG_WITH_PREFIX(1) << "Preparing to minor compact delta file: "
                        << dfr->ToString();

    inputs.push_back(delta_store);
    target_stores->push_back(delta_store);
    target_blocks->push_back(dfr->block_id());
  }
  TRACE_COUNTER_INCREMENT("delete_count", delete_count);
  TRACE_COUNTER_INCREMENT("reinsert_count", reinsert_count);
  TRACE_COUNTER_INCREMENT("update_count", update_count);
  RowIteratorOptions opts;
  opts.projection = projection;
  opts.io_context = io_context;
  RETURN_NOT_OK(DeltaIteratorMerger::Create(inputs, opts, out));
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

#ifndef NDEBUG
Status DeltaTracker::ValidateDeltaOrder(const std::shared_ptr<DeltaStore>& first,
                                        const std::shared_ptr<DeltaStore>& second,
                                        const IOContext* io_context,
                                        DeltaType type) {
  shared_ptr<DeltaStore> first_copy = first;
  shared_ptr<DeltaStore> second_copy = second;

  // Make clones so we don't leave the original ones initted. That can affect
  // tests. We know it's a DeltaFileReader if it's not Initted().
  if (!first_copy->Initted()) {
    shared_ptr<DeltaFileReader> first_clone;
    RETURN_NOT_OK(down_cast<DeltaFileReader*>(first.get())->CloneForDebugging(
        rowset_metadata_->fs_manager(), mem_trackers_.tablet_tracker, &first_clone));
    RETURN_NOT_OK(first_clone->Init(io_context));
    first_copy = first_clone;
  }
  if (!second_copy->Initted()) {
    shared_ptr<DeltaFileReader> second_clone;
    RETURN_NOT_OK(down_cast<DeltaFileReader*>(second.get())->CloneForDebugging(
        rowset_metadata_->fs_manager(), mem_trackers_.tablet_tracker, &second_clone));
    RETURN_NOT_OK(second_clone->Init(io_context));
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

Status DeltaTracker::ValidateDeltasOrdered(const SharedDeltaStoreVector& list,
                                           const IOContext* io_context,
                                           DeltaType type) {
  for (size_t i = 1; i < list.size(); i++) {
    RETURN_NOT_OK(ValidateDeltaOrder(list[i - 1], list[i], io_context, type));
  }
  return Status::OK();
}
#endif // NDEBUG

void DeltaTracker::AtomicUpdateStores(const SharedDeltaStoreVector& stores_to_replace,
                                      const SharedDeltaStoreVector& new_stores,
                                      const IOContext* io_context,
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
    WARN_NOT_OK(ValidateDeltasOrdered(new_stores, io_context, type),
                "Could not validate delta order");
    if (start_it != stores_to_update->end()) {
      // Sanity check that the last store we are adding would logically appear
      // before the first store that would follow it.
      WARN_NOT_OK(ValidateDeltaOrder(*new_stores.rbegin(), *start_it, io_context, type),
                  "Could not validate delta order");
    }
  }
#endif // NDEBUG

  // Insert the new stores.
  stores_to_update->insert(start_it, new_stores.begin(), new_stores.end());

  VLOG_WITH_PREFIX(1) << "New " << DeltaType_Name(type) << " stores: "
                      << JoinDeltaStoreStrings(*stores_to_update);
}

Status DeltaTracker::Compact(const IOContext* io_context) {
  return CompactStores(io_context, 0, -1);
}

Status DeltaTracker::CommitDeltaStoreMetadataUpdate(const RowSetMetadataUpdate& update,
                                                    const SharedDeltaStoreVector& to_remove,
                                                    vector<DeltaBlockIdAndStats> new_delta_blocks,
                                                    const IOContext* io_context,
                                                    DeltaType type,
                                                    MetadataFlushType flush_type) {
  compact_flush_lock_.AssertAcquired();

  // This method is only used for compactions and GC, not data modifications.
  // Therefore, flushing is not required for safety.
  // We enforce that with this DCHECK.
  DCHECK(!to_remove.empty());

  SharedDeltaStoreVector new_stores;
  RETURN_NOT_OK_PREPEND(OpenDeltaReaders(std::move(new_delta_blocks), io_context,
                                         &new_stores, type),
                        "Unable to open delta blocks");

  BlockIdContainer removed_blocks;
  rowset_metadata_->CommitUpdate(update, &removed_blocks);
  rowset_metadata_->AddOrphanedBlocks(removed_blocks);
  // Once we successfully commit to the rowset metadata, let's ensure we update
  // the delta stores to maintain consistency between the two.
  AtomicUpdateStores(to_remove, new_stores, io_context, type);

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

Status DeltaTracker::CompactStores(const IOContext* io_context, int start_idx, int end_idx) {
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
  unique_ptr<DeltaStats> stats;
  RETURN_NOT_OK(DoCompactStores(io_context, start_idx, end_idx, std::move(block), &stats,
                                &compacted_stores, &compacted_blocks));

  vector<BlockId> new_block = { new_block_id };
  RowSetMetadataUpdate update;
  update.ReplaceRedoDeltaBlocks(compacted_blocks, new_block);

  const auto num_blocks_compacted = compacted_blocks.size();
  TRACE_COUNTER_INCREMENT("delta_blocks_compacted", num_blocks_compacted);
  VLOG_WITH_PREFIX(1) << Substitute("Flushing compaction of $0 redo delta "
                                    "blocks { $1 } into block $2",
                                    num_blocks_compacted,
                                    BlockId::JoinStrings(compacted_blocks),
                                    new_block_id.ToString());
  vector<DeltaBlockIdAndStats> new_block_and_stats;
  new_block_and_stats.emplace_back(std::make_pair(new_block_id, std::move(stats)));
  RETURN_NOT_OK_PREPEND(CommitDeltaStoreMetadataUpdate(update, compacted_stores,
                                                       std::move(new_block_and_stats),
                                                       io_context, REDO, FLUSH_METADATA),
                        "DeltaTracker: CompactStores: Unable to commit delta update");
  return Status::OK();
}

bool DeltaTracker::EstimateAllRedosAreAncient(Timestamp ancient_history_mark) {
  shared_ptr<DeltaStore> newest_redo;
  std::lock_guard<rw_spinlock> lock(component_lock_);
  const boost::optional<Timestamp> dms_highest_timestamp =
      dms_ ? dms_->highest_timestamp() : boost::none;
  if (dms_highest_timestamp) {
    return *dms_highest_timestamp < ancient_history_mark;
  }

  // If we don't have a DMS or our DMS hasn't been written to at all, look at
  // the newest redo store.
  if (!redo_delta_stores_.empty()) {
    newest_redo = redo_delta_stores_.back();
  }
  return newest_redo && newest_redo->has_delta_stats() &&
      newest_redo->delta_stats().max_timestamp() < ancient_history_mark;
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
    if (undo->has_delta_stats() &&
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
                                    const IOContext* io_context,
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
      RETURN_NOT_OK(undo->Init(io_context));
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
                                             const IOContext* io_context,
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
    if (!undo->has_delta_stats()) break;
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
    RETURN_NOT_OK(CommitDeltaStoreMetadataUpdate(update, undos_to_remove, {}, io_context, UNDO,
                                                 NO_FLUSH_METADATA));
  }

  if (blocks_deleted) *blocks_deleted = tmp_blocks_deleted;
  if (bytes_deleted) *bytes_deleted = tmp_bytes_deleted;
  return Status::OK();
}

Status DeltaTracker::DoCompactStores(const IOContext* io_context,
                                     size_t start_idx, size_t end_idx,
                                     unique_ptr<WritableBlock> block,
                                     unique_ptr<DeltaStats>* output_stats,
                                     SharedDeltaStoreVector* compacted_stores,
                                     vector<BlockId> *compacted_blocks) {
  unique_ptr<DeltaIterator> inputs_merge;

  // Currently, DeltaFile iterators ignore the passed-in projection in
  // FilterColumnIdsAndCollectDeltas(). So, we just pass an empty schema here.
  // If this changes in the future, we'll have to pass in the current tablet
  // schema here.
  SchemaPtr empty_schema(new Schema);
  RETURN_NOT_OK(MakeDeltaIteratorMergerUnlocked(io_context, start_idx, end_idx,
                                                empty_schema, compacted_stores,
                                                compacted_blocks, &inputs_merge));
  DeltaFileWriter dfw(std::move(block));
  RETURN_NOT_OK(dfw.Start());
  RETURN_NOT_OK(WriteDeltaIteratorToFile<REDO>(inputs_merge.get(),
                                               ITERATE_OVER_ALL_ROWS,
                                               &dfw));
  RETURN_NOT_OK(dfw.Finish());
  *output_stats = dfw.release_delta_stats();
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
    if (dms_exists_.Load() && !dms_->Empty()) {
      deltas->push_back(dms_);
    }
  }
}

Status DeltaTracker::NewDeltaIterator(const RowIteratorOptions& opts,
                                      WhichStores which,
                                      unique_ptr<DeltaIterator>* out) const {
  std::vector<shared_ptr<DeltaStore>> stores;
  CollectStores(&stores, which);
  return DeltaIteratorMerger::Create(stores, opts, out);
}

Status DeltaTracker::NewDeltaFileIterator(
    const RowIteratorOptions& opts,
    DeltaType type,
    vector<shared_ptr<DeltaStore>>* included_stores,
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

  return DeltaIteratorMerger::Create(*included_stores, opts, out);
}

Status DeltaTracker::WrapIterator(const shared_ptr<CFileSet::Iterator> &base,
                                  const RowIteratorOptions& opts,
                                  unique_ptr<ColumnwiseIterator>* out) const {
  unique_ptr<DeltaIterator> iter;
  RETURN_NOT_OK(NewDeltaIterator(opts, &iter));

  out->reset(new DeltaApplier(opts, base, std::move(iter)));
  return Status::OK();
}

Status DeltaTracker::Update(Timestamp timestamp,
                            rowid_t row_idx,
                            const RowChangeList &update,
                            const consensus::OpId& op_id,
                            OperationResultPB* result) {
  Status s;
  while (true) {
    if (!dms_exists_.Load()) {
      std::lock_guard<rw_spinlock> lock(component_lock_);
      // Should check dms_exists_ here in case multiple threads are blocked.
      if (!dms_exists_.Load()) {
        RETURN_NOT_OK(CreateAndInitDMSUnlocked(nullptr));
      }
    }

    // TODO(todd): can probably lock this more fine-grained.
    shared_lock<rw_spinlock> lock(component_lock_);

    // Should check dms_exists_ here again since there is a gap
    // between the two critical sections defined by component_lock_.
    if (!dms_exists_.Load()) continue;

    s = dms_->Update(timestamp, row_idx, update, op_id);
    if (s.ok()) {
      MemStoreTargetPB* target = result->add_mutated_stores();
      target->set_rs_id(rowset_metadata_->id());
      target->set_dms_id(dms_->id());
    }
    break;
  }

  return s;
}

Status DeltaTracker::CheckRowDeleted(rowid_t row_idx, const IOContext* io_context,
                                     bool *deleted, ProbeStats* stats) const {
  shared_lock<rw_spinlock> lock(component_lock_);

  *deleted = false;
  // Check if the row has a deletion in DeltaMemStore.
  if (dms_exists_.Load()) {
    RETURN_NOT_OK(dms_->CheckRowDeleted(row_idx, io_context, deleted));
    if (*deleted) {
      return Status::OK();
    }
  }

  // Then check backwards through the list of trackers.
  for (auto ds = redo_delta_stores_.crbegin(); ds != redo_delta_stores_.crend(); ds++) {
    stats->deltas_consulted++;
    RETURN_NOT_OK((*ds)->CheckRowDeleted(row_idx, io_context, deleted));
    if (*deleted) {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status DeltaTracker::FlushDMS(DeltaMemStore* dms,
                              const IOContext* io_context,
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

  RETURN_NOT_OK(dms->FlushToFile(&dfw));
  RETURN_NOT_OK(dfw.Finish());
  unique_ptr<DeltaStats> stats = dfw.release_delta_stats();
  const auto bytes_written = dfw.written_size();
  TRACE_COUNTER_INCREMENT("bytes_written", bytes_written);
  TRACE_COUNTER_INCREMENT("delete_count", stats->delete_count());
  TRACE_COUNTER_INCREMENT("reinsert_count", stats->reinsert_count());
  TRACE_COUNTER_INCREMENT("update_count", stats->UpdateCount());
  VLOG_WITH_PREFIX(1) << Substitute("Flushed delta block $0 ($1 bytes on disk) "
                                    "stats: $2",
                                    block_id.ToString(),
                                    bytes_written,
                                    stats->ToString());

  // Now re-open for read
  unique_ptr<ReadableBlock> readable_block;
  RETURN_NOT_OK(fs->OpenBlock(block_id, &readable_block));
  ReaderOptions options;
  options.parent_mem_tracker = mem_trackers_.tablet_tracker;
  options.io_context = io_context;
  RETURN_NOT_OK(DeltaFileReader::OpenNoInit(std::move(readable_block),
                                            REDO,
                                            std::move(options),
                                            std::move(stats),
                                            dfr));
  VLOG_WITH_PREFIX(1) << "Opened new delta block " << block_id.ToString() << " for read";

  {
    // Merge the deleted row count of the old DMS to the RowSetMetadata
    // and reset deleted_row_count_ should be atomic, so we lock the
    // component_lock_ in exclusive mode.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    RETURN_NOT_OK(rowset_metadata_->CommitRedoDeltaDataBlock(dms->id(),
                                                             deleted_row_count_,
                                                             block_id));
    deleted_row_count_ = 0;
  }
  if (flush_type == FLUSH_METADATA) {
    RETURN_NOT_OK_PREPEND(rowset_metadata_->Flush(),
                          Substitute("Unable to commit Delta block metadata for: $0",
                                     block_id.ToString()));
  }
  return Status::OK();
}

Status DeltaTracker::Flush(const IOContext* io_context, MetadataFlushType flush_type) {
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

    count = dms_exists_.Load() ? dms_->Count() : 0;

    // Swap the DeltaMemStore and dms_ is null now.
    old_dms = std::move(dms_);
    dms_exists_.Store(false);

    if (count == 0) {
      // No need to flush if there are no deltas.
      // Ensure that the DeltaMemStore is using the latest schema.
      return Status::OK();
    }

    deleted_row_count_ = old_dms->deleted_row_count();
    redo_delta_stores_.push_back(old_dms);
  }

  VLOG_WITH_PREFIX(1) << Substitute("Flushing $0 deltas ($1 bytes in memory) "
                                    "from DMS $2",
                                    count, old_dms->EstimateSize(), old_dms->id());

  // Now, actually flush the contents of the old DMS.
  //
  // TODO(todd): need another lock to prevent concurrent flushers
  // at some point.
  shared_ptr<DeltaFileReader> dfr;
  Status s = FlushDMS(old_dms.get(), io_context, &dfr, flush_type);
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

bool DeltaTracker::GetDeltaMemStoreInfo(size_t* size_bytes, MonoTime* creation_time) const {
  // Check dms_exists_ first to avoid unnecessary contention on
  // component_lock_. We need to check again after taking the lock in case we
  // raced with a DMS flush.
  if (dms_exists_.Load()) {
    shared_lock<rw_spinlock> lock(component_lock_);
    if (dms_exists_.Load()) {
      *size_bytes = dms_->EstimateSize();
      *creation_time = dms_->creation_time();
      return true;
    }
  }
  return false;
}

size_t DeltaTracker::DeltaMemStoreSize() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return dms_exists_.Load() ? dms_->EstimateSize() : 0;
}

int64_t DeltaTracker::MinUnflushedLogIndex() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return dms_exists_.Load() ? dms_->MinLogIndex() : 0;
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
    if (!ds->has_delta_stats()) {
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
      RETURN_NOT_OK(ds->Init(nullptr));
    }
  }
  if (stores == UNDOS_AND_REDOS || stores == REDOS_ONLY) {
    for (const shared_ptr<DeltaStore>& ds : redo_delta_stores_) {
      RETURN_NOT_OK(ds->Init(nullptr));
    }
  }
  return Status::OK();
}

int64_t DeltaTracker::CountDeletedRows() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  DCHECK_GE(deleted_row_count_, 0);
  return deleted_row_count_ + (dms_exists_.Load() ? dms_->deleted_row_count() : 0);
}

string DeltaTracker::LogPrefix() const {
  return Substitute("T $0 P $1: ",
                    rowset_metadata_->tablet_metadata()->tablet_id(),
                    rowset_metadata_->fs_manager()->uuid());
}

} // namespace tablet
} // namespace kudu
