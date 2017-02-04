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

#include <mutex>
#include <set>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_applier.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/delta_iterator_merger.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

using cfile::ReaderOptions;
using fs::ReadableBlock;
using fs::WritableBlock;
using log::LogAnchorRegistry;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
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
                           const TabletMemTrackers& mem_trackers)
    : rowset_metadata_(std::move(rowset_metadata)),
      num_rows_(num_rows),
      open_(false),
      log_anchor_registry_(log_anchor_registry),
      mem_trackers_(mem_trackers),
      dms_empty_(true) {}

Status DeltaTracker::OpenDeltaReaders(const vector<BlockId>& blocks,
                                      vector<shared_ptr<DeltaStore> >* stores,
                                      DeltaType type) {
  FsManager* fs = rowset_metadata_->fs_manager();
  for (const BlockId& block_id : blocks) {
    gscoped_ptr<ReadableBlock> block;
    Status s = fs->OpenBlock(block_id, &block);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open " << DeltaType_Name(type)
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
      LOG(ERROR) << "Failed to open " << DeltaType_Name(type)
                 << " delta file reader " << block_id.ToString() << ": "
                 << s.ToString();
      return s;
    }

    VLOG(1) << "Successfully opened " << DeltaType_Name(type)
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

    LOG(INFO) << "Preparing to minor compact delta file: " << dfr->ToString();

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

// Validate that 'first' may precede 'second' in an ordered list of deltas,
// given a delta type of 'type'.
void ValidateDeltaOrder(const std::shared_ptr<DeltaStore>& first,
                        const std::shared_ptr<DeltaStore>& second,
                        DeltaType type) {
  DCHECK_OK(first->Init());
  DCHECK_OK(second->Init());
  switch (type) {
    case REDO:
      DCHECK_LE(first->delta_stats().min_timestamp(), second->delta_stats().min_timestamp())
          << "Found out-of-order deltas: [{" << first->ToString() << "}, {"
          << second->ToString() << "}]: type = " << type;
      break;
    case UNDO:
      DCHECK_GE(first->delta_stats().min_timestamp(), second->delta_stats().min_timestamp())
          << "Found out-of-order deltas: [{" << first->ToString() << "}, {"
          << second->ToString() << "}]: type = " << type;
      break;
  }
}

// Validate the relative ordering of the deltas in the specified list.
void ValidateDeltasOrdered(const SharedDeltaStoreVector& list, DeltaType type) {
  for (size_t i = 0; i < list.size() - 1; i++) {
    ValidateDeltaOrder(list[i], list[i + 1], type);
  }
}

} // anonymous namespace

Status DeltaTracker::AtomicUpdateStores(const SharedDeltaStoreVector& stores_to_replace,
                                        const vector<BlockId>& new_delta_blocks,
                                        DeltaType type) {
  SharedDeltaStoreVector new_stores;
  RETURN_NOT_OK_PREPEND(OpenDeltaReaders(new_delta_blocks, &new_stores, type),
                        "Unable to open delta blocks");

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
    start_it =
        std::find(stores_to_update->begin(), stores_to_update->end(), stores_to_replace[0]);

    auto end_it = start_it;
    for (const shared_ptr<DeltaStore>& ds : stores_to_replace) {
      if (end_it == stores_to_update->end() || *end_it != ds) {
        return Status::InvalidArgument(
            strings::Substitute("Cannot find deltastore sequence <$0> in <$1>",
                                JoinDeltaStoreStrings(stores_to_replace),
                                JoinDeltaStoreStrings(*stores_to_update)));
      }
      ++end_it;
    }
    // Remove the old stores.
    stores_to_update->erase(start_it, end_it);
  }

#ifndef NDEBUG
  // Perform validation checks to ensure callers do not violate our contract.
  if (!new_stores.empty()) {
    // Make sure the new stores are already ordered.
    ValidateDeltasOrdered(new_stores, type);
    if (start_it != stores_to_update->end()) {
      // Sanity check that the last store we are adding would logically appear
      // before the first store that would follow it.
      ValidateDeltaOrder(*new_stores.rbegin(), *start_it, type);
    }
  }
#endif // NDEBUG

  // Insert the new stores.
  stores_to_update->insert(start_it, new_stores.begin(), new_stores.end());

  VLOG(1) << "New " << DeltaType_Name(type) << " stores: "
          << JoinDeltaStoreStrings(*stores_to_update);
  return Status::OK();
}

Status DeltaTracker::Compact() {
  return CompactStores(0, -1);
}

Status DeltaTracker::CompactStores(int start_idx, int end_idx) {
  // Prevent concurrent compactions or a compaction concurrent with a flush
  //
  // TODO(perf): this could be more fine grained
  std::lock_guard<Mutex> l(compact_flush_lock_);

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
  gscoped_ptr<WritableBlock> block;
  RETURN_NOT_OK_PREPEND(fs->CreateNewBlock(&block),
                        "Could not allocate delta block");
  BlockId new_block_id(block->id());

  // Merge and compact the stores and write and output to "data_writer"
  vector<shared_ptr<DeltaStore> > compacted_stores;
  vector<BlockId> compacted_blocks;
  RETURN_NOT_OK(DoCompactStores(start_idx, end_idx, std::move(block),
                &compacted_stores, &compacted_blocks));

  // Update delta_stores_, removing the compacted delta files and inserted the new
  RETURN_NOT_OK(AtomicUpdateStores(compacted_stores, { new_block_id }, REDO));
  LOG(INFO) << "Opened delta block for read: " << new_block_id.ToString();

  // Update the metadata accordingly
  RowSetMetadataUpdate update;
  update.ReplaceRedoDeltaBlocks(compacted_blocks, { new_block_id });
  // TODO: need to have some error handling here -- if we somehow can't persist the
  // metadata, do we end up losing data on recovery?
  CHECK_OK(rowset_metadata_->CommitUpdate(update));

  Status s = rowset_metadata_->Flush();
  if (!s.ok()) {
    // TODO: again need to figure out some way of making this safe. Should we be
    // writing the metadata _ahead_ of the actual store swap? Probably.
    LOG(FATAL) << "Unable to commit delta data block metadata for "
               << new_block_id.ToString() << ": " << s.ToString();
    return s;
  }

  return Status::OK();
}

Status DeltaTracker::DoCompactStores(size_t start_idx, size_t end_idx,
         gscoped_ptr<WritableBlock> block,
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
  LOG(INFO) << "Compacting " << (end_idx - start_idx + 1) << " delta files.";
  DeltaFileWriter dfw(std::move(block));
  RETURN_NOT_OK(dfw.Start());
  RETURN_NOT_OK(WriteDeltaIteratorToFile<REDO>(inputs_merge.get(),
                                               ITERATE_OVER_ALL_ROWS,
                                               &dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Succesfully compacted the specified delta files.";
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
      LOG(FATAL);
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
  gscoped_ptr<WritableBlock> writable_block;
  RETURN_NOT_OK_PREPEND(fs->CreateNewBlock(&writable_block),
                        "Unable to allocate new delta data writable_block");
  BlockId block_id(writable_block->id());

  DeltaFileWriter dfw(std::move(writable_block));
  RETURN_NOT_OK_PREPEND(dfw.Start(),
                        Substitute("Unable to start writing to delta block $0",
                                   block_id.ToString()));

  gscoped_ptr<DeltaStats> stats;
  RETURN_NOT_OK(dms->FlushToFile(&dfw, &stats));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Flushed delta block: " << block_id.ToString()
            << " ts range: [" << stats->min_timestamp() << ", " << stats->max_timestamp() << "]";

  // Now re-open for read
  gscoped_ptr<ReadableBlock> readable_block;
  RETURN_NOT_OK(fs->OpenBlock(block_id, &readable_block));
  ReaderOptions options;
  options.parent_mem_tracker = mem_trackers_.tablet_tracker;
  RETURN_NOT_OK(DeltaFileReader::OpenNoInit(std::move(readable_block),
                                            REDO,
                                            std::move(options),
                                            dfr));
  LOG(INFO) << "Reopened delta block for read: " << block_id.ToString();

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

  LOG(INFO) << "Flushing " << count << " deltas from DMS " << old_dms->id() << "...";

  // Now, actually flush the contents of the old DMS.
  // TODO: need another lock to prevent concurrent flushers
  // at some point.
  shared_ptr<DeltaFileReader> dfr;
  Status s = FlushDMS(old_dms.get(), &dfr, flush_type);
  CHECK(s.ok())
    << "Failed to flush DMS: " << s.ToString()
    << "\nTODO: need to figure out what to do with error handling "
    << "if this fails -- we end up with a DeltaMemStore permanently "
    << "in the store list. For now, abort.";


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

  // TODO: wherever we write stuff, we should write to a tmp path
  // and rename to final path!
}

size_t DeltaTracker::DeltaMemStoreSize() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return dms_->EstimateSize();
}

int64_t DeltaTracker::MinUnflushedLogIndex() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return dms_->MinLogIndex();
}

size_t DeltaTracker::CountRedoDeltaStores() const {
  shared_lock<rw_spinlock> lock(component_lock_);
  return redo_delta_stores_.size();
}

uint64_t DeltaTracker::EstimateOnDiskSize() const {
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

} // namespace tablet
} // namespace kudu
