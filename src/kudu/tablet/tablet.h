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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/iterator.h"
#include "kudu/common/schema.h"
#include "kudu/fs/io_context.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/tablet/lock_manager.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/locks.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/rw_semaphore.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/status.h"

namespace kudu {

class AlterTableTest;
class ConstContiguousRow;
class EncodedKey;
class KeyRange;
class MemTracker;
class RowBlock;
class ScanSpec;
class Throttler;
class Timestamp;
struct IterWithBounds;
struct IteratorStats;

namespace consensus {
class OpId;
}  // namespace consensus

namespace clock {
class Clock;
} // namespace clock

namespace log {
class LogAnchorRegistry;
} // namespace log

namespace tablet {

class AlterSchemaOpState;
class CompactionPolicy;
class HistoryGcOpts;
class MemRowSet;
class ParticipantOpState;
class RowSetTree;
class RowSetsInCompaction;
class TxnMetadata;
class WriteOpState;
struct RowOp;
struct TabletComponents;
struct TabletMetrics;
struct TxnRowSets;

class Tablet {
 public:
  typedef std::map<int64_t, int64_t> ReplaySizeMap;
  friend class CompactRowSetsOp;
  friend class FlushMRSOp;

  class CompactionFaultHooks;
  class FlushCompactCommonHooks;
  class FlushFaultHooks;
  class Iterator;

  // Create a new tablet.
  //
  // If 'metric_registry' is non-NULL, then this tablet will create a 'tablet' entity
  // within the provided registry. Otherwise, no metrics are collected.
  Tablet(scoped_refptr<TabletMetadata> metadata,
         clock::Clock* clock,
         std::shared_ptr<MemTracker> parent_mem_tracker,
         MetricRegistry* metric_registry,
         scoped_refptr<log::LogAnchorRegistry> log_anchor_registry);

  ~Tablet();

  // Open the tablet, initializing transactions for 'in_flight_txn_ids', and
  // MRSs for 'txn_ids_with_mrs'. The created MRSs will be uncommitted -- it is
  // up to the caller to determine whether they should be committed after
  // finishing bootstrapping.
  //
  // Upon completion, the tablet enters the kBootstrapping state.
  Status Open(const std::unordered_set<int64_t>& in_flight_txn_ids = std::unordered_set<int64_t>{},
              const std::unordered_set<int64_t>& txn_ids_with_mrs = std::unordered_set<int64_t>{});

  // Mark that the tablet has finished bootstrapping.
  // This transitions from kBootstrapping to kOpen state.
  // Returns an error if tablet has been stopped.
  Status MarkFinishedBootstrapping();

  // Shuts down the tablet, unregistering various components that may attempt
  // to point back to it, and changing the lifecycle state to 'kShutdown'.
  void Shutdown();

  // Stops the tablet from making any progress. Currently-Applying operations
  // are terminated early on a best-effort basis, new ops will return
  // with Status::Aborted(), tablet maintenance ops will no longer be
  // scheduled, and the lifecycle state is set to 'kStopped'.
  //
  // Currently, the tablet will only be Stopped as the tablet is shutting down.
  // In the future, it can be Stopped when it hits a non-recoverable error
  // (e.g. a disk error) to immediately prevent further writes.
  void Stop();

  // Returns whether the tablet has been stopped, i.e. is in either the
  // 'kStopped' or 'kShutdown' state.
  bool HasBeenStopped() const;

  // Decode the Write (insert/mutate) operations from within a user's
  // request.
  Status DecodeWriteOperations(const Schema* client_schema,
                               WriteOpState* op_state);

  // Acquire locks for each of the operations in the given write op.
  // This also sets the row op's RowSetKeyProbe.
  Status AcquireRowLocks(WriteOpState* op_state);

  // Acquire locks for the given write op. If 'must_acquire' is true,
  // then wait until the lock is acquired. Otherwise, return
  // 'TXN_LOCKED_ABORT' or 'TXN_LOCKED_RETRY_OP' error if lock
  // cannot be acquired.
  Status AcquirePartitionLock(WriteOpState* op_state,
                              LockManager::LockWaitMode wait_mode);

  // Acquire a shared lock on the given transaction, to ensure the
  // transaction's state doesn't change while the given write is in flight.
  Status AcquireTxnLock(int64_t txn_id, WriteOpState* op_state);

  // Starts an MVCC op which must have a pre-assigned timestamp.
  //
  // TODO(todd): rename this to something like "FinishPrepare" or "StartApply", since
  // it's not the first thing in an op!
  void StartOp(WriteOpState* op_state);
  void StartOp(ParticipantOpState* op_state);

  // Like the above but actually assigns the timestamp. Only used for tests that
  // don't boot a tablet server.
  void AssignTimestampAndStartOpForTests(WriteOpState* op_state);

  // Signal that the given op is about to Apply.
  void StartApplying(WriteOpState* op_state);
  void StartApplying(ParticipantOpState* op_state);

  // Apply all of the row operations associated with this op.
  Status ApplyRowOperations(WriteOpState* op_state) WARN_UNUSED_RESULT;

  // Apply a single row operation, which must already be prepared.
  // The result is set back into row_op->result.
  Status ApplyRowOperation(const fs::IOContext* io_context,
                           WriteOpState* op_state,
                           RowOp* row_op,
                           ProbeStats* stats) WARN_UNUSED_RESULT;

  // Begins the transaction, recording its presence in the tablet metadata.
  // Upon calling this, 'op_id' will be anchored until the metadata is flushed,
  // using 'txn' as the anchor owner.
  void BeginTransaction(Txn* txn, const consensus::OpId& op_id);

  // Indicates that the transaction has started to commit, recording the
  // timestamp used by the MVCC op to demarcate the end of the transaction in
  // the tablet metadata. Upon calling this, 'op_id' will be anchored until
  // the metadata is flushed, using 'txn' as the anchor owner.
  void BeginCommit(Txn* txn, Timestamp mvcc_op_ts, const consensus::OpId& op_id);

  // Commits the transaction, recording its commit timestamp in the tablet metadata.
  // Upon calling this, 'op_id' will be anchored until the metadata is flushed,
  // using 'txn' as the anchor owner.
  void CommitTransaction(Txn* txn, Timestamp commit_ts, const consensus::OpId& op_id);

  // Merges the uncommitted transaction rowsets associated with the given
  // 'txn_id' with the committed rowsets.
  void CommitTxnRowSets(int64_t txn_id);

  // Aborts the transaction, recording the abort in the tablet metadata.
  // Upon calling this, 'op_id' will be anchored until the metadata is flushed,
  // using 'txn' as the anchor owner.
  void AbortTransaction(Txn* txn, const consensus::OpId& op_id);

  // Creates new rowsets for the given transaction.
  void CreateTxnRowSets(int64_t txn_id, scoped_refptr<TxnMetadata> txn_meta);

  // Create a new row iterator which yields the rows as of the current MVCC
  // state of this tablet.
  // The returned iterator is not initialized.
  Status NewRowIterator(const SchemaPtr& projection,
                        std::unique_ptr<RowwiseIterator>* iter) const;

  // Like above, but returns an ordered iterator.
  Status NewOrderedRowIterator(const SchemaPtr& projection,
                               std::unique_ptr<RowwiseIterator>* iter) const;

  // Create a new row iterator using specific iterator options.
  //
  // 'opts' contains the options desired from the iterator.
  //
  // Note: the Schema pointed to by the 'projection' field of the 'opts' struct
  // will be copied, so that pointer only needs to remain valid during the call
  // to NewRowIterator() and not after that.
  // Similarly, the 'io_context' field of the 'opts' struct will be ignored and
  // overwritten in the copy of the 'opts' struct used by the returned iterator
  // because the iterator constructs and holds the relevant instance of that
  // object as a member variable.
  Status NewRowIterator(RowIteratorOptions opts,
                        std::unique_ptr<RowwiseIterator>* iter) const;

  // Flush the current MemRowSet for this tablet to disk. This swaps
  // in a new (initially empty) MemRowSet in its place.
  //
  // This doesn't flush any DeltaMemStores for any existing RowSets.
  // To do that, call FlushBiggestDMS() for example.
  Status Flush();

  // Prepares the op context for the alter schema operation.
  // An error will be returned if the specified schema is invalid (e.g.
  // key mismatch, or missing IDs)
  Status CreatePreparedAlterSchema(AlterSchemaOpState *op_state,
                                   const Schema* schema);

  // Apply the Schema of the specified op.
  // This operation will trigger a flush on the current MemRowSet.
  Status AlterSchema(AlterSchemaOpState* op_state);

  // Rewind the schema to an earlier version than is written in the on-disk
  // metadata. This is done during bootstrap to roll the schema back to the
  // point in time where the logs-to-be-replayed begin, so we can then decode
  // the operations in the log with the correct schema.
  //
  // REQUIRES: state_ == kBootstrapping
  Status RewindSchemaForBootstrap(const Schema& new_schema,
                                  int64_t schema_version);

  // Prints current RowSet layout, taking a snapshot of the current RowSet interval
  // tree. Also prints the log of the compaction algorithm as evaluated
  // on the current layout.
  void PrintRSLayout(std::ostream* o);

  // Flags to change the behavior of compaction.
  enum CompactFlag {
    COMPACT_NO_FLAGS = 0,

    // Force the compaction to include all rowsets, regardless of the
    // configured compaction policy. This is currently only used in
    // tests.
    FORCE_COMPACT_ALL = 1 << 0
  };
  typedef int CompactFlags;

  Status Compact(CompactFlags flags);

  // Update the statistics for performing a compaction.
  void UpdateCompactionStats(MaintenanceOpStats* stats);

  // Returns the exact current size of the MRS, in bytes. A value greater than 0 doesn't imply
  // that the MRS has data, only that it has allocated that amount of memory.
  // This method takes a read lock on component_lock_ and is thread-safe.
  size_t MemRowSetSize() const;

  // Returns true if the MRS is empty, else false. Doesn't rely on size and
  // actually verifies that the MRS has no elements.
  // This method takes a read lock on component_lock_ and is thread-safe.
  bool MemRowSetEmpty() const;

  // Returns the size in bytes of WALs that would need to be replayed to restore
  // the current MRS.
  size_t MemRowSetLogReplaySize(const ReplaySizeMap& replay_size_map) const;

  // Returns the total on-disk size of this tablet, in bytes.
  // Includes the tablet superblock.
  size_t OnDiskSize() const;

  // Returns the on-disk size of this tablet's data, in bytes.
  // Excludes all metadata (both tablet metadata and the metadata of this tablet's rowsets).
  size_t OnDiskDataSize() const;

  // Get the total size of all the DMS
  size_t DeltaMemStoresSize() const;

  // Same as MemRowSetEmpty(), but for the DMS.
  bool DeltaMemRowSetEmpty() const;

  // Flushes the DMS with the highest retention.
  Status FlushBestDMS(const ReplaySizeMap &replay_size_map) const;

  // Flush only the biggest DMS
  Status FlushBiggestDMS();

  // Flush all delta memstores. Only used for tests.
  Status FlushAllDMSForTests();

  // Run a major compaction on all delta stores. Initializes any un-initialized
  // redo delta stores. Only used for tests.
  Status MajorCompactAllDeltaStoresForTests();

  // Finds the RowSet which has the most separate delta files and
  // issues a delta compaction.
  Status CompactWorstDeltas(RowSet::DeltaCompactionType type);

  // Get the highest performance improvement that would come from compacting the delta stores
  // of one of the rowsets. If the returned performance improvement is 0, or if 'rs' is NULL,
  // then 'rs' isn't set. Callers who already own compact_select_lock_
  // can call GetPerfImprovementForBestDeltaCompactUnlocked().
  double GetPerfImprovementForBestDeltaCompact(RowSet::DeltaCompactionType type,
                                               std::shared_ptr<RowSet>* rs) const;

  // Same as GetPerfImprovementForBestDeltaCompact(), but doesn't take a lock on
  // compact_select_lock_.
  double GetPerfImprovementForBestDeltaCompactUnlocked(RowSet::DeltaCompactionType type,
                                                       std::shared_ptr<RowSet>* rs) const;

  // Estimate the number of bytes in ancient undo delta stores. This may be an
  // overestimate.
  Status EstimateBytesInPotentiallyAncientUndoDeltas(int64_t* bytes);

  // Initialize undo delta blocks for up to 'time_budget' amount of time.
  // If 'time_budget' is not Initialized() then there is no time limit.
  // If this method returns OK, the number of bytes found in ancient undo files
  // is returned in the out-param 'bytes_in_ancient_undos'.
  Status InitAncientUndoDeltas(MonoDelta time_budget, int64_t* bytes_in_ancient_undos);

  // Find and delete all undo delta blocks that have a maximum op timestamp
  // prior to the current ancient history mark. If this method returns OK, the
  // number of blocks and bytes deleted are returned in the out-parameters.
  //
  // Returns an error if the metadata update fails. Upon failure, no in-memory
  // state is change.
  Status DeleteAncientUndoDeltas(int64_t* blocks_deleted = nullptr,
                                 int64_t* bytes_deleted = nullptr);

  // Returns the number of bytes potentially used by rowsets that have no live
  // rows and are entirely ancient.
  //
  // These checks may not touch on-disk block data if we can determine from the
  // live row count that the rowsets aren't fully deleted, or from the DMS that
  // the latest update is not considered ancient. If there is no DMS, looks at
  // the newest redo but doesn't initialize it. As such, since we may miss out
  // on counting rowsets we haven't initialized yet, this may be an
  // underestimate.
  Status GetBytesInAncientDeletedRowsets(int64_t* bytes_in_ancient_deleted_rowsets);

  // Finds and GCs all fully deleted rowsets that have a maximum op timestamp
  // prior to the current ancient history mark.
  //
  // Returns an error if the metadata update fails. Upon failure, no in-memory
  // state is change.
  Status DeleteAncientDeletedRowsets();

  // Counts the number of deltas in the tablet. Only used for tests.
  int64_t CountUndoDeltasForTests() const;
  int64_t CountRedoDeltasForTests() const;

  // Return the current number of rowsets in the tablet.
  size_t num_rowsets() const;

  // Attempt to count the total number of rows in the tablet.
  // This is not super-efficient since it must iterate over the
  // memrowset in the current implementation.
  Status CountRows(uint64_t *count) const;

  // Count the number of live rows in this tablet.
  Status CountLiveRows(uint64_t* count) const;

  // Verbosely dump this entire tablet to the logs. This is only
  // really useful when debugging unit tests failures where the tablet
  // has a very small number of rows.
  Status DebugDump(std::vector<std::string> *lines = NULL);

  const SchemaPtr schema() const {
    return metadata_->schema();
  }

  // Returns a reference to the key projection of the tablet schema.
  // The schema keys are immutable.
  const Schema& key_schema() const { return key_schema_; }

  // Return the MVCC manager for this tablet.
  MvccManager* mvcc_manager() { return &mvcc_; }

  // Return the Lock Manager for this tablet.
  LockManager* lock_manager() { return &lock_manager_; }

  // Return the transaction participant for this tablet.
  TxnParticipant* txn_participant() { return &txn_participant_; }

  const TabletMetadata *metadata() const { return metadata_.get(); }
  TabletMetadata *metadata() { return metadata_.get(); }
  scoped_refptr<TabletMetadata> shared_metadata() const { return metadata_; }

  void SetCompactionHooksForTests(const std::shared_ptr<CompactionFaultHooks> &hooks);
  void SetFlushHooksForTests(const std::shared_ptr<FlushFaultHooks> &hooks);
  void SetFlushCompactCommonHooksForTests(
      const std::shared_ptr<FlushCompactCommonHooks> &hooks);

  // Returns the current MemRowSet id, for tests.
  // This method takes a read lock on component_lock_ and is thread-safe.
  int32_t CurrentMrsIdForTests() const;

  // Runs a major delta major compaction on columns with specified IDs.
  // NOTE: RowSet must presently be a DiskRowSet. (Perhaps the API should be
  // a shared_ptr API for now?)
  //
  // Only used in tests.
  Status DoMajorDeltaCompaction(const std::vector<ColumnId>& col_ids,
                                const std::shared_ptr<RowSet>& input_rs,
                                const fs::IOContext* io_context = nullptr);

  // Calculates the ancient history mark and returns true iff tablet history GC
  // is enabled, which requires the use of a HybridClock.
  // Otherwise, returns false.
  bool GetTabletAncientHistoryMark(Timestamp* ancient_history_mark) const WARN_UNUSED_RESULT;

  // Calculates history GC options based on properties of the Clock implementation.
  HistoryGcOpts GetHistoryGcOpts() const;

  // Method used by tests to retrieve all rowsets of this table. This
  // will be removed once code for selecting the appropriate RowSet is
  // finished and delta files is finished is part of Tablet class.
  void GetRowSetsForTests(std::vector<std::shared_ptr<RowSet> >* out);

  // Register the maintenance ops associated with this tablet
  void RegisterMaintenanceOps(MaintenanceManager* maint_mgr);

  // Unregister the maintenance ops associated with this tablet. This will wait
  // for all ops to finish before returning.
  //
  // This method is not thread safe, but is currently only called during
  // TabletReplica::Shutdown(), which is single-threaded by design.
  void UnregisterMaintenanceOps();

  // Cancel the maintenance ops associated with this tablet. This will prevent
  // further scheduling of the ops and will not wait for any ops to finish.
  //
  // This method is thread-safe.
  void CancelMaintenanceOps();

  const std::string& tablet_id() const { return metadata_->tablet_id(); }

  // Return the metrics for this tablet.
  // May be NULL in unit tests, etc.
  TabletMetrics* metrics() { return metrics_.get(); }

  // Return handle to the metric entity of this tablet.
  const scoped_refptr<MetricEntity>& GetMetricEntity() const {
    return metric_entity_;
  }

  // Returns a reference to this tablet's memory tracker.
  const std::shared_ptr<MemTracker>& mem_tracker() const {
    return mem_trackers_.tablet_tracker;
  }

  // Throttle a RPC with 'bytes' request size.
  // Return true if this RPC is allowed.
  bool ShouldThrottleAllow(int64_t bytes);

  clock::Clock* clock() const { return clock_; }

  std::string LogPrefix() const;

  // Return the default bloom filter sizing parameters, configured by server flags.
  static BloomFilterSizing DefaultBloomSizing();

  // Split [start_key, stop_key) into primary key ranges by chunk size.
  //
  // If column_ids specified, then the size estimate used for 'target_chunk_size'
  // should only include these columns. This can be used if a query will
  // only scan a certain subset of the columns.
  void SplitKeyRange(const EncodedKey* start_key,
                     const EncodedKey* stop_key,
                     const std::vector<ColumnId>& column_ids,
                     uint64 target_chunk_size,
                     std::vector<KeyRange>* ranges);

  // Update the last read operation timestamp.
  // NOTE: It's a const function, because we have to call it in Iterator, where Tablet is a const
  // variable there.
  void UpdateLastReadTime() const;

  // Collect and update recent workload statistics for the tablet.
  // Return the current workload score of the tablet.
  //
  // This method is not thread safe and should only be called from a single thread at once.
  double CollectAndUpdateWorkloadStats(MaintenanceOp::PerfImprovementOpType type);

  // Returns the best DMS to flush, based on its memory size and retained
  // bytes. Also returns the earliest creation time of a DMS seen. Note that
  // 'mem_size' and 'replay_size' correspond to the same DMS but
  // 'earliest_dms_time' may not.
  std::shared_ptr<RowSet> FindBestDMSToFlush(const ReplaySizeMap& replay_size_map,
                                             int64_t* mem_size = nullptr,
                                             int64_t* replay_size = nullptr,
                                             MonoTime* earliest_dms_time = nullptr) const;

 private:
  friend class kudu::AlterTableTest;
  friend class Iterator;
  friend class TabletReplicaTest;
  friend class TabletReplicaTestBase;
  FRIEND_TEST(TestTablet, TestGetReplaySizeForIndex);
  FRIEND_TEST(TestTabletStringKey, TestSplitKeyRange);
  FRIEND_TEST(TestTabletStringKey, TestSplitKeyRangeWithZeroRowSets);
  FRIEND_TEST(TestTabletStringKey, TestSplitKeyRangeWithOneRowSet);
  FRIEND_TEST(TestTabletStringKey, TestSplitKeyRangeWithNonOverlappingRowSets);
  FRIEND_TEST(TestTabletStringKey, TestSplitKeyRangeWithMinimumValueRowSet);
  FRIEND_TEST(TxnParticipantTest, TestFlushMultipleMRSs);

  // Lifecycle states that a Tablet can be in. Legal state transitions for a
  // Tablet object:
  //
  //   kInitialized -> kBootstrapping -> kOpen -> kStopped -> kShutdown
  //         |               |             |        ^^^
  //         |               |             +--------+||
  //         |               +-----------------------+|
  //         +----------------------------------------+
  enum State {
    kInitialized,
    kBootstrapping,
    kOpen,
    kStopped,
    kShutdown
  };

  // Sets the lifecycle state of the tablet. See the definition of State for
  // the valid transitions.
  //
  // Must be called while 'state_lock_' is held.
  void set_state_unlocked(State s) {
    DCHECK(state_lock_.is_locked());
    switch (s) {
      case kBootstrapping:
        DCHECK_EQ(kInitialized, state_);
        break;
      case kOpen:
        DCHECK_EQ(kBootstrapping, state_);
        break;
      case kStopped:
        DCHECK(state_ == kInitialized ||
               state_ == kBootstrapping ||
               state_ == kOpen);
        break;
      case kShutdown:
        DCHECK(state_ == kStopped ||
               state_ == kShutdown);
        break;
      default:
        LOG(DFATAL) << "Illegal state transition!";
    }
    state_ = s;
  }

  // Returns an error if the tablet is in the 'kStopped' or 'kShutdown' state.
  // Sets *cur_state to the current state, which may be useful for later assertions.
  Status CheckHasNotBeenStopped(State* cur_state = nullptr) const;

  Status FlushUnlocked();

  // Validate the contents of 'op' and return a bad Status if it is invalid.
  static Status ValidateOp(const RowOp& op);

  // Validate 'op' as in 'ValidateOp()' above. If it is invalid, marks the op as failed
  // and returns false. If valid, marks the op as valid and returns true.
  static bool ValidateOpOrMarkFailed(RowOp* op);

  // Validate the given insert/upsert operation.
  static Status ValidateInsertOrUpsertUnlocked(const RowOp& op);

  // Validate the given update/delete operation.
  static Status ValidateMutateUnlocked(const RowOp& op);

  // Perform an INSERT, INSERT_IGNORE, or UPSERT operation, assuming that the op is
  // already in a prepared state. This state ensures that:
  // - the row lock is acquired
  // - the tablet components have been acquired
  // - the operation has been decoded
  Status InsertOrUpsertUnlocked(const fs::IOContext* io_context,
                                WriteOpState *op_state,
                                RowOp* op,
                                ProbeStats* stats);

  // Same as above, but for UPDATE, UPDATE_IGNORE, DELETE, or DELETE_IGNORE operations.
  Status MutateRowUnlocked(const fs::IOContext* io_context,
                           WriteOpState *op_state,
                           RowOp* op,
                           ProbeStats* stats);

  // In the case of an UPSERT against a duplicate row, converts the UPSERT
  // into an internal UPDATE operation and performs it.
  Status ApplyUpsertAsUpdate(const fs::IOContext* io_context,
                             WriteOpState *op_state,
                             RowOp* upsert,
                             RowSet* rowset,
                             ProbeStats* stats);

  // Return the list of RowSets that need to be consulted when processing the
  // given insertion or mutation.
  static std::vector<RowSet*> FindRowSetsToCheck(const RowOp* op,
                                                 const TabletComponents* comps);

  // For each of the operations in 'op_state', check for the presence of their
  // row keys in the RowSets in the current RowSetTree (as determined by the op's
  // captured TabletComponents).
  Status BulkCheckPresence(const fs::IOContext* io_context,
                           WriteOpState* op_state) WARN_UNUSED_RESULT;

  // Capture a set of iterators which, together, reflect all of the data in the tablet.
  //
  // These iterators are not true snapshot iterators, but they are safe against
  // concurrent modification. They will include all data that was present at the time
  // of creation, and potentially newer data.
  //
  // The returned iterators are not Init()ed.
  // The pointer fields of 'opts' must remain valid and unchanged for the
  // lifetime of the returned iterators.
  Status CaptureConsistentIterators(const RowIteratorOptions& opts,
                                    const ScanSpec* spec,
                                    std::vector<IterWithBounds>* iters) const;

  Status PickRowSetsToCompact(RowSetsInCompaction *picked,
                              CompactFlags flags) const;

  // Performs a merge compaction or a flush.
  Status DoMergeCompactionOrFlush(const RowSetsInCompaction &input,
                                  int64_t mrs_being_flushed,
                                  const std::vector<TxnInfoBeingFlushed>& txns_being_flushed);

  // Handle the case in which a compaction or flush yielded no output rows.
  // In this case, we just need to remove the rowsets in 'rowsets' from the
  // metadata and flush it.
  Status HandleEmptyCompactionOrFlush(const RowSetVector& rowsets,
                                      int mrs_being_flushed,
                                      const std::vector<TxnInfoBeingFlushed>& txns_being_flushed);

  // Updates the average rowset height metric. Acquires the tablet's
  // compact_select_lock_.
  void UpdateAverageRowsetHeight();

  Status FlushMetadata(const RowSetVector& to_remove,
                       const RowSetMetadataVector& to_add,
                       int64_t mrs_being_flushed,
                       const std::vector<TxnInfoBeingFlushed>& txns_being_flushed);

  static void ModifyRowSetTree(const RowSetTree& old_tree,
                               const RowSetVector& rowsets_to_remove,
                               const RowSetVector& rowsets_to_add,
                               RowSetTree* new_tree);

  // Swap out a set of rowsets, atomically replacing them with the new rowset
  // under the lock.
  void AtomicSwapRowSets(const RowSetVector &to_remove,
                         const RowSetVector &to_add);

  // Same as the above, but without taking the lock. This should only be used
  // in cases where the lock is already held.
  void AtomicSwapRowSetsUnlocked(const RowSetVector &to_remove,
                                 const RowSetVector &to_add);

  void GetComponents(scoped_refptr<TabletComponents>* comps) const {
    shared_lock<rw_spinlock> l(component_lock_);
    *comps = CHECK_NOTNULL(components_.get());
  }

  void GetComponentsOrNull(scoped_refptr<TabletComponents>* comps) const {
    shared_lock<rw_spinlock> l(component_lock_);
    *comps = components_;
  }

  // Create a new MemRowSet, replacing the current committed one(s).
  // 'old_mrss' will be populated to the current committed MemRowSet(s) set
  // before the replacement. If any MemRowSet is not empty it will be added to
  // the 'compaction' input and the MemRowSets' compaction locks will be taken
  // to prevent the inclusion in any concurrent compactions.
  Status ReplaceMemRowSetsUnlocked(RowSetsInCompaction* compaction,
                                   std::vector<std::shared_ptr<MemRowSet>>* old_mrss);

  // Convert the specified read client schema (without IDs) to a server schema (with IDs)
  // This method is used by NewRowIterator().
  Status GetMappedReadProjection(const Schema& projection,
                                 Schema *mapped_projection) const;

  Status CheckRowInTablet(const ConstContiguousRow& row) const;

  // Helper method to find how many bytes need to be replayed to restore in-memory
  // state from this index.
  static int64_t GetReplaySizeForIndex(int64_t min_log_index,
                                       const ReplaySizeMap& size_map);

  // The elapsed time, in seconds, since the last read operation on this tablet, or since this
  // Tablet object was created on current tserver if it hasn't been read since then.
  uint64_t LastReadElapsedSeconds() const;

  // Same as LastReadElapsedSeconds(), but for write operation.
  uint64_t LastWriteElapsedSeconds() const;

  // Test-only lock that synchronizes access to AssignTimestampAndStartOpForTests().
  // Tests that use LocalTabletWriter take this lock to synchronize timestamp assignment,
  // op start, and safe time adjustment.
  // NOTE: Should not be taken on non-test paths.
  mutable simple_spinlock test_start_op_lock_;

  // Lock protecting schema_ and key_schema_.
  //
  // Writers take this lock in shared mode before decoding and projecting
  // their requests. They hold the lock until after APPLY.
  //
  // Readers take this lock in shared mode only long enough to copy the
  // current schema into the iterator, after which all projection is taken
  // care of based on that copy.
  //
  // On an AlterSchema, this is taken in exclusive mode during Prepare() and
  // released after the schema change has been applied.
  mutable rw_semaphore schema_lock_;

  const Schema key_schema_;

  scoped_refptr<TabletMetadata> metadata_;

  // Lock protecting access to the 'components_' member (i.e the rowsets in the tablet)
  //
  // Shared mode:
  // - Writers take this in shared mode at the same time as they obtain an MVCC timestamp
  //   and capture a reference to components_. This ensures that we can use the MVCC timestamp
  //   to determine which writers are writing to which components during compaction.
  // - Readers take this in shared mode while capturing their iterators. This ensures that
  //   they see a consistent view when racing against flush/compact.
  //
  // Exclusive mode:
  // - Flushes/compactions take this lock in order to lock out concurrent updates when
  //   swapping in a new memrowset.
  //
  // NOTE: callers should avoid taking this lock for a long time, even in shared mode.
  // This is because the lock has some concept of fairness -- if, while a long reader
  // is active, a writer comes along, then all future short readers will be blocked.
  mutable rw_spinlock component_lock_;

  // The components of the tablet whose base data has been committed. These
  // should always be read or swapped under the component_lock.
  scoped_refptr<TabletComponents> components_;

  // Uncommitted transaction state.
  std::unordered_map<int64_t, scoped_refptr<TxnRowSets>> uncommitted_rowsets_by_txn_id_;

  scoped_refptr<log::LogAnchorRegistry> log_anchor_registry_;
  TabletMemTrackers mem_trackers_;

  scoped_refptr<MetricEntity> metric_entity_;
  std::unique_ptr<TabletMetrics> metrics_;

  std::unique_ptr<Throttler> throttler_;

  int64_t next_mrs_id_;

  // A pointer to the server's clock.
  clock::Clock* clock_;

  MvccManager mvcc_;

  LockManager lock_manager_;

  // Maintains the set of in-flight transactions, and any WAL anchors
  // associated with them.
  // NOTE: the participant may retain MVCC ops, so define it after the
  // MvccManager, to ensure those ops get destructed before the MvccManager.
  // The same goes for locks and the LockManager.
  TxnParticipant txn_participant_;

  std::unique_ptr<CompactionPolicy> compaction_policy_;

  // Lock protecting the selection of rowsets for compaction.
  // Only one thread may run the compaction selection algorithm at a time
  // so that they don't both try to select the same rowset.
  mutable std::mutex compact_select_lock_;

  // We take this lock when flushing the tablet's rowsets in Tablet::Flush.  We
  // don't want to have two flushes in progress at once, in case the one which
  // started earlier completes after the one started later.
  mutable Semaphore rowsets_flush_sem_;

  // Lock protecting access to mutate 'state_' and all access to 'maintenance_ops_'.
  // If taken with any other locks, this must be taken last, i.e. no locks can
  // be acquired while holding this lock.
  mutable simple_spinlock state_lock_;

  // Protected by state_lock_ for transitions, but may be read without holding a lock.
  std::atomic<State> state_;

  // Fake lock used to ensure calls to RegisterMaintenanceOps and
  // UnregisterMaintenanceOps don't overlap. This serves to ensure that only
  // one thread is updating the maintenance op list at a time.
  DFAKE_MUTEX(maintenance_registration_fake_lock_);

  // Fault hooks. In production code, these will always be NULL.
  std::shared_ptr<CompactionFaultHooks> compaction_hooks_;
  std::shared_ptr<FlushFaultHooks> flush_hooks_;
  std::shared_ptr<FlushCompactCommonHooks> common_hooks_;

  std::vector<MaintenanceOp*> maintenance_ops_;

  // Lock protecting access to 'last_write_time_' and 'last_read_time_'.
  mutable rw_spinlock last_rw_time_lock_;
  MonoTime last_write_time_;
  mutable MonoTime last_read_time_;

  // NOTE: it's important that this is the first member to be destructed. This
  // ensures we do not attempt to collect metrics while calling the destructor.
  FunctionGaugeDetacher metric_detacher_;

  MonoTime last_update_workload_stats_time_;
  int64_t last_scans_started_;
  int64_t last_rows_mutated_;
  double last_read_score_;
  double last_write_score_;

  DISALLOW_COPY_AND_ASSIGN(Tablet);
};


// Hooks used in test code to inject faults or other code into interesting
// parts of the compaction code.
class Tablet::CompactionFaultHooks {
 public:
  virtual Status PostSelectIterators() { return Status::OK(); }
  virtual ~CompactionFaultHooks() {}
};

class Tablet::FlushCompactCommonHooks {
 public:
  virtual Status PostTakeMvccSnapshot() { return Status::OK(); }
  virtual Status PostWriteSnapshot() { return Status::OK(); }
  virtual Status PostSwapInDuplicatingRowSet() { return Status::OK(); }
  virtual Status PostReupdateMissedDeltas() { return Status::OK(); }
  virtual Status PostSwapNewRowSet() { return Status::OK(); }
  virtual ~FlushCompactCommonHooks() {}
};

// Hooks used in test code to inject faults or other code into interesting
// parts of the Flush() code.
class Tablet::FlushFaultHooks {
 public:
  virtual Status PostSwapNewMemRowSet() { return Status::OK(); }
  virtual ~FlushFaultHooks() {}
};

class Tablet::Iterator : public RowwiseIterator {
 public:
  virtual ~Iterator();

  virtual Status Init(ScanSpec *spec) OVERRIDE;

  virtual bool HasNext() const OVERRIDE;

  virtual Status NextBlock(RowBlock *dst) OVERRIDE;

  std::string ToString() const OVERRIDE;

  const SchemaPtr schema() const OVERRIDE;

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE;

 private:
  friend class Tablet;

  DISALLOW_COPY_AND_ASSIGN(Iterator);

  // Instantiate iterator with given projection and options.
  //
  // Note: the Schema pointed to by the 'projection' field of the 'opts' struct
  // will be copied into projection_, so that pointer only needs to remain
  // valid during the call to the constructor and not after that.
  // Similarly, the 'io_context' field of the 'opts' struct will be ignored and
  // overwritten in the copy of the 'opts' struct used by this class because
  // this class constructs and holds the relevant instance of that object as a
  // member variable.
  Iterator(const Tablet* tablet,
           RowIteratorOptions opts);

  const Tablet* tablet_;
  fs::IOContext io_context_;
  SchemaPtr projection_;
  RowIteratorOptions opts_;
  std::unique_ptr<RowwiseIterator> iter_;
};

// Structure which represents the components of the tablet's storage.
// This structure is immutable -- an op can grab it and be sure that it won't
// change.
struct TabletComponents : public RefCountedThreadSafe<TabletComponents> {
  TabletComponents(std::shared_ptr<MemRowSet> mrs,
                   std::vector<std::shared_ptr<MemRowSet>> txn_mrss,
                   std::shared_ptr<RowSetTree> rs_tree);
  // The "main" MemRowSet that catches inserts to the tablet that are not a
  // part of any transaction.
  const std::shared_ptr<MemRowSet> memrowset;

  // MemRowSets whose insertion heads were inserted as a part of a transaction.
  const std::vector<std::shared_ptr<MemRowSet>> txn_memrowsets;

  // The persisted RowSets that comprise the rows of a tablet.
  const std::shared_ptr<RowSetTree> rowsets;
};

// Encapsulates data inserted as a part of a transaction that has not yet been
// committed.
// TODO(awong): when we support flushing transactional MRSs before committing,
// track uncommitted disk rowsets here.
struct TxnRowSets : public RefCountedThreadSafe<TxnRowSets> {
  explicit TxnRowSets(std::shared_ptr<MemRowSet> mrs)
      : memrowset(std::move(mrs)) {}
  const std::shared_ptr<MemRowSet> memrowset;
};

} // namespace tablet
} // namespace kudu

