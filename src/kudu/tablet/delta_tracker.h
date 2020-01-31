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
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/common/rowid.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class ColumnwiseIterator;
class MonoTime;
class RowChangeList;
class Schema;
class Timestamp;
struct ColumnId;

namespace consensus {
class OpId;
}

namespace fs {
class WritableBlock;
struct IOContext;
}

namespace log {
class LogAnchorRegistry;
}

namespace tablet {

class DeltaFileReader;
class DeltaMemStore;
class OperationResultPB;
class RowSetMetadata;
class RowSetMetadataUpdate;
struct ProbeStats;
struct RowIteratorOptions;

// The DeltaTracker is the part of a DiskRowSet which is responsible for
// tracking modifications against the base data. It consists of a set of
// DeltaStores which each contain a set of mutations against the base data.
// These DeltaStores may be on disk (DeltaFileReader) or in-memory (DeltaMemStore).
//
// This class is also responsible for flushing the in-memory deltas to disk.
class DeltaTracker {
 public:
  enum MetadataFlushType {
    FLUSH_METADATA,
    NO_FLUSH_METADATA
  };

  static Status Open(const std::shared_ptr<RowSetMetadata>& rowset_metadata,
                     log::LogAnchorRegistry* log_anchor_registry,
                     const TabletMemTrackers& mem_trackers,
                     const fs::IOContext* io_context,
                     std::unique_ptr<DeltaTracker>* delta_tracker);

  Status WrapIterator(const std::shared_ptr<CFileSet::Iterator> &base,
                      const RowIteratorOptions& opts,
                      std::unique_ptr<ColumnwiseIterator>* out) const;

  // Enum used for NewDeltaIterator() and CollectStores() below.
  // Determines whether all types of stores should be considered,
  // or just UNDO or REDO stores.
  enum WhichStores {
    UNDOS_AND_REDOS,
    UNDOS_ONLY,
    REDOS_ONLY
  };

  // Create a new DeltaIterator which merges the delta stores tracked
  // by this DeltaTracker. Depending on the value of 'which' (see above),
  // this iterator may include UNDOs, REDOs, or both.
  //
  // Pointers in 'opts' must remain valid for the lifetime of the returned iterator.
  Status NewDeltaIterator(const RowIteratorOptions& opts,
                          WhichStores which,
                          std::unique_ptr<DeltaIterator>* out) const;

  Status NewDeltaIterator(const RowIteratorOptions& opts,
                          std::unique_ptr<DeltaIterator>* out) const {
    return NewDeltaIterator(opts, UNDOS_AND_REDOS, out);
  }


  // Like NewDeltaIterator() but only includes file based stores, does not include
  // the DMS.
  // Returns the delta stores being merged in *included_stores.
  Status NewDeltaFileIterator(
      const RowIteratorOptions& opts,
      DeltaType type,
      std::vector<std::shared_ptr<DeltaStore>>* included_stores,
      std::unique_ptr<DeltaIterator>* out) const;

  // Flushes the current DeltaMemStore and replaces it with a new one.
  // Caller selects whether to also have the RowSetMetadata (and consequently
  // the TabletMetadata) flushed.
  //
  // NOTE: 'flush_type' should almost always be set to 'FLUSH_METADATA', or else
  // delta stores might become unrecoverable.
  Status Flush(const fs::IOContext* io_context, MetadataFlushType flush_type);

  // Update the given row in the database.
  // Copies the data, as well as any referenced values into a local arena.
  // "result" tracks the status of the update as well as which data
  // structure(s) it ended up at.
  Status Update(Timestamp timestamp,
                rowid_t row_idx,
                const RowChangeList &update,
                const consensus::OpId& op_id,
                OperationResultPB* result);

  // Check if the given row has been deleted -- i.e if the most recent
  // delta for this row is a deletion.
  //
  // Sets *deleted to true if so; otherwise sets it to false.
  Status CheckRowDeleted(rowid_t row_idx, const fs::IOContext* io_context,
                         bool *deleted, ProbeStats* stats) const;

  // Compacts all REDO delta files.
  Status Compact(const fs::IOContext* io_context);

  // Updates the in-memory list of delta stores and then persists the updated
  // metadata. This should only be used for compactions or ancient history
  // data GC, not when adding mutations, since it makes the updated stores
  // visible before attempting to flush the metadata to disk.
  //
  // The 'compact_flush_lock_' should be acquired before calling this method.
  Status CommitDeltaStoreMetadataUpdate(const RowSetMetadataUpdate& update,
                                        const SharedDeltaStoreVector& to_remove,
                                        const std::vector<BlockId>& new_delta_blocks,
                                        const fs::IOContext* io_context,
                                        DeltaType type,
                                        MetadataFlushType flush_type);

  // Performs minor compaction on all REDO delta files between index
  // "start_idx" and "end_idx" (inclusive) and writes this to a
  // new REDO delta block. If "end_idx" is set to -1, then delta files at
  // all indexes starting with "start_idx" will be compacted.
  Status CompactStores(const fs::IOContext* io_context, int start_idx, int end_idx);

  // See RowSet::EstimateBytesInPotentiallyAncientUndoDeltas().
  Status EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp ancient_history_mark,
                                                     int64_t* bytes);

  // Returns whether all redo (DMS and newest redo delta file) are ancient
  // (i.e. that the redo with the highest timestamp is older than the AHM).
  // This is an estimate, since if the newest redo file has not yet been
  // initted, this will return a false negative.
  bool EstimateAllRedosAreAncient(Timestamp ancient_history_mark);

  // See RowSet::InitUndoDeltas().
  Status InitUndoDeltas(Timestamp ancient_history_mark,
                        MonoTime deadline,
                        const fs::IOContext* io_context,
                        int64_t* delta_blocks_initialized,
                        int64_t* bytes_in_ancient_undos);

  // See RowSet::DeleteAncientUndoDeltas().
  Status DeleteAncientUndoDeltas(Timestamp ancient_history_mark, const fs::IOContext* io_context,
                                 int64_t* blocks_deleted, int64_t* bytes_deleted);

  // Opens the input 'blocks' of type 'type' and returns the opened delta file
  // readers in 'stores'.
  Status OpenDeltaReaders(const std::vector<BlockId>& blocks,
                          const fs::IOContext* io_context,
                          std::vector<std::shared_ptr<DeltaStore>>* stores,
                          DeltaType type);

#ifndef NDEBUG
  // Validates that 'first' may precede 'second' in an ordered list of deltas,
  // given a delta type of 'type'. This should only be run in DEBUG mode.
  //
  // Crashes if there is an ordering violation, and returns an error if the
  // validation could not be performed.
  Status ValidateDeltaOrder(const std::shared_ptr<DeltaStore>& first,
                            const std::shared_ptr<DeltaStore>& second,
                            const fs::IOContext* io_context,
                            DeltaType type);

  // Validates the relative ordering of the deltas in the specified list. This
  // should only be run in DEBUG mode.
  //
  // Crashes if there is an ordering violation, and returns an error if the
  // validation could not be performed.
  Status ValidateDeltasOrdered(const SharedDeltaStoreVector& list,
                               const fs::IOContext* io_context,
                               DeltaType type);
#endif // NDEBUG

  // Replaces the subsequence of stores that matches 'stores_to_replace' with
  // delta file readers corresponding to 'new_delta_blocks', which may be empty.
  // If 'stores_to_replace' is empty then the stores represented by
  // 'new_delta_blocks' are prepended to the relevant delta stores list.
  //
  // In DEBUG mode, this may do IO to validate the delta ordering.
  void AtomicUpdateStores(const SharedDeltaStoreVector& stores_to_replace,
                          const SharedDeltaStoreVector& new_stores,
                          const fs::IOContext* io_context,
                          DeltaType type);

  // Get the delta MemStore's size in bytes, including pre-allocation.
  size_t DeltaMemStoreSize() const;

  // Returns true if the DMS doesn't exist. This doesn't rely on the size.
  bool DeltaMemStoreEmpty() const {
    return !dms_exists_.Load();
  }

  // Get the minimum log index for this tracker's DMS, -1 if it wasn't set.
  int64_t MinUnflushedLogIndex() const;

  // Return the number of undo delta stores.
  size_t CountUndoDeltaStores() const;

  // Return the number of redo delta stores, not including the DeltaMemStore.
  size_t CountRedoDeltaStores() const;

  // Return the size on-disk of UNDO deltas, in bytes.
  uint64_t UndoDeltaOnDiskSize() const;

  // Return the size on-disk of REDO deltas, in bytes.
  uint64_t RedoDeltaOnDiskSize() const;

  // Retrieves the list of column indexes that currently have updates.
  void GetColumnIdsWithUpdates(std::vector<ColumnId>* col_ids) const;

  Mutex* compact_flush_lock() {
    return &compact_flush_lock_;
  }

  // Returns an error if the delta tracker has been marked read-only.
  // Else, returns OK.
  //
  // 'compact_flush_lock_' must be held when this is called.
  Status CheckWritableUnlocked() const;

  // Init() all of the specified delta stores. For tests only.
  Status InitAllDeltaStoresForTests(WhichStores stores);

  // Count the number of deleted rows in the current DMS as well as
  // in a flushing DMS (if one exists)
  int64_t CountDeletedRows() const;

 private:
  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestRowSet, TestMakeDeltaIteratorMergerUnlocked);
  FRIEND_TEST(TestRowSet, TestCompactStores);
  FRIEND_TEST(TestMajorDeltaCompaction, TestCompact);

  DeltaTracker(std::shared_ptr<RowSetMetadata> rowset_metadata,
               log::LogAnchorRegistry* log_anchor_registry,
               TabletMemTrackers mem_trackers);

  Status DoOpen(const fs::IOContext* io_context);

  Status FlushDMS(DeltaMemStore* dms,
                  const fs::IOContext* io_context,
                  std::shared_ptr<DeltaFileReader>* dfr,
                  MetadataFlushType flush_type);

  // This collects undo and/or redo stores into '*stores'.
  void CollectStores(std::vector<std::shared_ptr<DeltaStore>>* stores,
                     WhichStores which) const;

  // Performs the actual compaction. Results of compaction are written to "block",
  // while delta stores that underwent compaction are appended to "compacted_stores", while
  // their corresponding block ids are appended to "compacted_blocks".
  //
  // NOTE: the caller of this method should acquire or already hold an
  // exclusive lock on 'compact_flush_lock_' before calling this
  // method in order to protect 'redo_delta_stores_'.
  Status DoCompactStores(const fs::IOContext* io_context,
                         size_t start_idx, size_t end_idx,
                         std::unique_ptr<fs::WritableBlock> block,
                         std::vector<std::shared_ptr<DeltaStore>>* compacted_stores,
                         std::vector<BlockId>* compacted_blocks);

  // Creates a merge delta iterator and captures the delta stores and
  // delta blocks under compaction into 'target_stores' and
  // 'target_blocks', respectively.  The merge iterator is stored in
  // 'out'; 'out' is valid until this instance of DeltaTracker
  // is destroyed.
  //
  // NOTE: the caller of this method must first acquire or already
  // hold a lock on 'compact_flush_lock_'in order to guard against a
  // race on 'redo_delta_stores_'.
  Status MakeDeltaIteratorMergerUnlocked(const fs::IOContext* io_context,
                                         size_t start_idx, size_t end_idx, const Schema* projection,
                                         std::vector<std::shared_ptr<DeltaStore>>* target_stores,
                                         std::vector<BlockId>* target_blocks,
                                         std::unique_ptr<DeltaIterator>* out);

  std::string LogPrefix() const;

  Status CreateAndInitDMSUnlocked(const fs::IOContext* io_context);

  std::shared_ptr<RowSetMetadata> rowset_metadata_;

  bool open_;

  // Certain errors (e.g. failed delta tracker flushes) may leave the delta
  // store lists in a state such that it would be unsafe to run further
  // maintenance ops.
  //
  // Must be checked immediately after locking compact_flush_lock_.
  bool read_only_;

  log::LogAnchorRegistry* log_anchor_registry_;

  TabletMemTrackers mem_trackers_;

  int64_t next_dms_id_;

  // The current DeltaMemStore into which updates should be written.
  std::shared_ptr<DeltaMemStore> dms_;
  // The set of tracked REDO delta stores, in increasing timestamp order.
  SharedDeltaStoreVector redo_delta_stores_;
  // The set of tracked UNDO delta stores, in decreasing timestamp order.
  SharedDeltaStoreVector undo_delta_stores_;

  // The maintenance scheduler calls DeltaMemStoreEmpty() a lot.
  // We use an atomic variable to indicate whether DMS exists or not and
  // to avoid having to take component_lock_ in order to satisfy this call.
  AtomicBool dms_exists_;

  // read-write lock protecting dms_ and {redo,undo}_delta_stores_.
  // - Readers take this lock in shared mode.
  // - Mutators take this lock in exclusive mode if they need to create
  //   a new DMS, and shared mode otherwise.
  // - Flushers take this lock in exclusive mode before they modify the
  //   structure of the rowset.
  //
  // TODO(perf): convert this to a reader-biased lock to avoid any cacheline
  // contention between threads.
  mutable rw_spinlock component_lock_;

  // Exclusive lock that ensures that only one flush or compaction can run
  // at a time. Protects redo_delta_stores_ and undo_delta_stores_.
  // NOTE: this lock cannot be acquired while component_lock is held:
  // otherwise, Flush and Compaction threads (that both first acquire this lock
  // and then component_lock) will deadlock.
  //
  // TODO(perf): this needs to be more fine grained
  mutable Mutex compact_flush_lock_;

  // Number of deleted rows for a DMS that is currently being flushed.
  // When the flush completes, this is merged into the RowSetMetadata
  // and reset.
  int64_t deleted_row_count_;

  DISALLOW_COPY_AND_ASSIGN(DeltaTracker);
};


} // namespace tablet
} // namespace kudu
