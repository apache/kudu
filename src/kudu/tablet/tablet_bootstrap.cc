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

#include "kudu/tablet/tablet_bootstrap.h"

#include <cstdint>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/io_context.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/lock_manager.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/ops/alter_schema_op.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/participant_op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h" // IWYU pragma: keep
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"

DECLARE_bool(prevent_kudu_2233_corruption);
DECLARE_int32(group_commit_queue_size_bytes);

DEFINE_double(fault_crash_during_log_replay, 0.0,
              "Fraction of the time when the tablet will crash immediately "
              "after processing a log entry during log replay. "
              "(For testing only!)");
TAG_FLAG(fault_crash_during_log_replay, unsafe);

DECLARE_int32(max_clock_sync_error_usec);

using kudu::clock::Clock;
using kudu::consensus::ALTER_SCHEMA_OP;
using kudu::consensus::CHANGE_CONFIG_OP;
using kudu::consensus::CommitMsg;
using kudu::consensus::ConsensusBootstrapInfo;
using kudu::consensus::MinimumOpId;
using kudu::consensus::NO_OP;
using kudu::consensus::OpId;
using kudu::consensus::OpIdToString;
using kudu::consensus::OperationType;
using kudu::consensus::OperationType_Name;
using kudu::consensus::PARTICIPANT_OP;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::WRITE_OP;
using kudu::fs::IOContext;
using kudu::log::Log;
using kudu::log::LogAnchorRegistry;
using kudu::log::LogEntryPB;
using kudu::log::LogIndex;
using kudu::log::LogOptions;
using kudu::log::LogReader;
using kudu::log::ReadableLogSegment;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::ResultTracker;
using kudu::tserver::AlterSchemaRequestPB;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

struct ReplayState;

// Information from the tablet metadata which indicates which data was
// flushed prior to this restart and which memory stores are still active.
//
// We take a snapshot of this information at the beginning of the bootstrap
// process so that we can allow compactions and flushes to run during bootstrap
// without confusing our tracking of flushed stores.
//
// NOTE: automatic flushes and compactions are not currently scheduled during
// bootstrap. However, flushes may still be triggered due to operations like
// alter-table.
class FlushedStoresSnapshot {
 public:
  FlushedStoresSnapshot() {}
  Status InitFrom(const TabletMetadata& tablet_meta);

  // Return true if the given memory store is still active (i.e. edits that were
  // originally written to this memory store should be replayed during the bootstrap
  // process).
  //
  // NOTE: a store may be inactive for either of two reasons. Either:
  // (a) the store was flushed to disk, OR
  // (b) the store was in the process of being written by a flush or compaction
  //     but the process crashed before the associated tablet metadata update
  //     was committed.
  bool IsMemStoreActive(const MemStoreTargetPB& target) const;

 private:
  int64_t last_durable_mrs_id_ = 0;
  unordered_map<int64_t, int64_t> flushed_dms_by_drs_id_;

  DISALLOW_COPY_AND_ASSIGN(FlushedStoresSnapshot);
};

// Bootstraps an existing tablet by opening the metadata from disk, and rebuilding soft
// state by playing log segments. A bootstrapped tablet can then be added to an existing
// consensus configuration as a LEARNER, which will bring its state up to date with the
// rest of the consensus configuration, or it can start serving the data itself, after it
// has been appointed LEADER of that particular consensus configuration.
//
// The high-level steps to replay the WAL are as follows:
//  for each segment in the WAL:
//    for each entry in the segment:
//      - If the entry is a replicate message, keep track of it and write it to
//        the new WAL, since we may find a corresponding commit message later.
//      - If the entry is a commit message, determine whether or not we have a
//        corresponding replicate message in the WAL, and if so, replay the op,
//        skipping operations whose mem-stores have been persisted to disk. If
//        no replicate message is found, we can skip replaying the op since its
//        mutation has been persisted to disk.
//  for each commit message with no corresponding replicate message:
//    - Validate that the mutated stores are non-active.
//  for each replicate message with no corresponding commit message:
//    - Return the replicate message as an "orphaned replicate".
//    - When the RaftConsensus instance starts up, orphaned replicates are
//      initialized as follower ops.
//
// NOTE: this does not handle pulling data from other replicas in the cluster. That
// is handled by the 'TabletCopy' classes, which copy blocks and metadata locally
// before invoking this local bootstrap functionality to start the tablet.
//
// When rebuilding an existing WAL, we first move the WAL segments into a
// separate "recovery" directory to serve as the original copy. New WAL entries
// with new commit messages are added to a new WAL in the original WAL
// directory. Entire segments may be skipped (e.g. if the mem-stores were
// flushed prior to shutting down), so the resulting WAL may be shorter than
// the original. Once the bootstrap is complete, the recovery directory is
// deleted, leaving only the new WAL. Since recovery directories contain the
// original WALs, following a crash during bootstrapping, subsequent bootstraps
// should attempt to replay segments out of the recovery directory.
//
// TODO(dralves): Because the tablet that is being rebuilt is never
// flushed/compacted, consensus is only set on the tablet after bootstrap, when
// we get to flushes/compactions though we need to set it before replay or we
// won't be able to re-rebuild.
class TabletBootstrap {
 public:
  TabletBootstrap(scoped_refptr<TabletMetadata> tablet_meta,
                  RaftConfigPB committed_raft_config,
                  Clock* clock,
                  shared_ptr<MemTracker> mem_tracker,
                  scoped_refptr<ResultTracker> result_tracker,
                  MetricRegistry* metric_registry,
                  FileCache* file_cache,
                  scoped_refptr<TabletReplica> tablet_replica,
                  scoped_refptr<LogAnchorRegistry> log_anchor_registry);

  // Plays the log segments, rebuilding the portion of the Tablet's soft
  // state that is present in the log (additional soft state may be present
  // in other replicas).
  // A successful call will yield the rebuilt tablet and the rebuilt log.
  Status Bootstrap(shared_ptr<Tablet>* rebuilt_tablet,
                   scoped_refptr<Log>* rebuilt_log,
                   ConsensusBootstrapInfo* consensus_info);

 private:

  // The method that does the actual work of tablet bootstrap. Bootstrap() is
  // actually a wrapper method that is responsible for pinning and unpinning
  // the tablet metadata flush.
  Status RunBootstrap(shared_ptr<Tablet>* rebuilt_tablet,
                      scoped_refptr<Log>* rebuilt_log,
                      ConsensusBootstrapInfo* consensus_info);

  // Opens the tablet.
  // Sets '*has_blocks' to true if there was any data on disk for this tablet.
  Status OpenTablet(bool* has_blocks);

  // Checks if a previous log recovery directory exists. If so, it deletes any
  // files in the log dir and sets 'needs_recovery' to true, meaning that the
  // previous recovery attempt should be retried from the recovery dir.
  //
  // Otherwise, if there is a log directory with log files in it, renames that
  // log dir to the log recovery dir and creates a new, empty log dir so that
  // log replay can proceed. 'needs_recovery' is also returned as true in this
  // case.
  //
  // If no log segments are found, 'needs_recovery' is set to false.
  Status PrepareRecoveryDir(bool* needs_recovery);

  // Opens the latest log segments for the Tablet that will allow to rebuild
  // the tablet's soft state. If there are existing log segments in the tablet's
  // log directly they are moved to a "log-recovery" directory which is deleted
  // when the replay process is completed (as they have been duplicated in the
  // current log directory).
  //
  // If a "log-recovery" directory is already present, we will continue to replay
  // from the "log-recovery" directory. Tablet metadata is updated once replay
  // has finished from the "log-recovery" directory.
  Status OpenLogReaderInRecoveryDir();

  // Opens a new log in the tablet's log directory.
  // The directory is expected to be clean.
  Status OpenNewLog();

  // Finishes bootstrap, setting 'rebuilt_log' and 'rebuilt_tablet'.
  Status FinishBootstrap(const string& message,
                         scoped_refptr<log::Log>* rebuilt_log,
                         shared_ptr<Tablet>* rebuilt_tablet);

  // Plays the log segments into the tablet being built.
  // The process of playing the segments generates a new log that can be continued
  // later on when then tablet is rebuilt and starts accepting writes from clients.
  Status PlaySegments(const IOContext* io_context, ConsensusBootstrapInfo* consensus_info);

  // Append the given commit message to the log.
  // Does not support writing a TxResult.
  Status AppendCommitMsg(const CommitMsg& commit_msg);

  Status PlayWriteRequest(const IOContext* io_context, ReplicateMsg* replicate_msg,
                          const CommitMsg& commit_msg);

  Status PlayAlterSchemaRequest(const IOContext* io_context, ReplicateMsg* replicate_msg,
                                const CommitMsg& commit_msg);

  Status PlayChangeConfigRequest(const IOContext* io_context, ReplicateMsg* replicate_msg,
                                 const CommitMsg& commit_msg);

  Status PlayTxnParticipantOpRequest(const IOContext* io_context, ReplicateMsg* replicate_msg,
                                     const CommitMsg& commit_msg);

  Status PlayNoOpRequest(const IOContext* io_context, ReplicateMsg* replicate_msg,
                         const CommitMsg& commit_msg);

  // Plays operations, skipping those that have already been flushed or have previously failed.
  // See ApplyRowOperations() for more details on how the decision of whether an operation
  // is applied or skipped is made.
  Status PlayRowOperations(const IOContext* io_context,
                           WriteOpState* op_state,
                           const TxResultPB& orig_result,
                           TxResultPB* new_result);

  // Determine which of the operations from 'orig_result' must be skipped.
  // At the same time this builds the WriteResponsePB that we'll store on the ResultTracker.
  // 'new_result' store the results of the operations that were skipped, 'response' stores
  // any error that might have previously happened so that we can send them back to clients,
  // if needed.
  // Finally 'all_skipped' indicates whether all of the original operations were skipped.
  Status DetermineSkippedOpsAndBuildResponse(const TxResultPB& orig_result,
                                             TxResultPB* new_result,
                                             WriteResponsePB* response,
                                             bool* all_skipped);

  // Pass through all of the decoded operations in op_state. For each op:
  // - if it was previously failed, mark as failed
  // - if it previously succeeded but was flushed, skip it.
  // - otherwise, re-apply to the tablet being bootstrapped.
  Status ApplyOperations(const IOContext* io_context,
                         WriteOpState* op_state,
                         const TxResultPB& orig_result,
                         TxResultPB* new_result);

  enum OpAction {
    // The operation was never applied or was applied to an unflushed memory store and thus
    // needs to be applied again.
    NEEDS_REPLAY,
    // The operation was already applied to a memory store that was flushed.
    SKIP_PREVIOUSLY_FLUSHED,
    // The operation was never applied due to an error.
    SKIP_PREVIOUSLY_FAILED
  };

  // Filter a row operation, setting 'action' to indicate what needs to be done
  // to the operation, i.e. whether it must applied or skipped.
  Status FilterOperation(const OperationResultPB& op_result,
                         OpAction* action);

  enum ActiveStores {
    // The OperationResultPBs in the commit message do not reference any stores.
    // This can happen in the case that the operations did not result in any mutations
    // (e.g. because they were updates for not-found row keys).
    NO_MUTATED_STORES,

    // At least one operation resulted in a mutation to a store, but none of the
    // mutated stores are still active. Therefore the operation does not need to
    // be replayed.
    NO_STORES_ACTIVE,

    // At least one operation resulted in a mutation to a store, and at least
    // one of those mutated stores is still active. This implies that the operation
    // needs to be replayed.
    SOME_STORES_ACTIVE
  };

  // For the given commit message, analyze which memory stores were mutated
  // by the operation, returning one of the enum values above.
  ActiveStores AnalyzeActiveStores(const CommitMsg& commit);

  void DumpReplayStateToLog(const ReplayState& state);

  Status HandleEntry(const IOContext* io_context,
                     ReplayState* state,
                     unique_ptr<LogEntryPB> entry,
                     string* entry_debug_info);

  // Handlers for each type of message seen in the log during replay.
  Status HandleReplicateMessage(ReplayState* state,
                                unique_ptr<LogEntryPB> entry,
                                string* entry_debug_info);
  Status HandleCommitMessage(const IOContext* io_context, ReplayState* state,
                             unique_ptr<LogEntryPB> entry,
                             string* entry_debug_info);

  Status ApplyCommitMessage(const IOContext* io_context, ReplayState* state, LogEntryPB* entry);
  Status HandleEntryPair(const IOContext* io_context, LogEntryPB* replicate_entry,
                         LogEntryPB* commit_entry);

  // Checks that an orphaned commit message is actually irrelevant, i.e that none
  // of the data stores it refers to are live.
  Status CheckOrphanedCommitDoesntNeedReplay(const CommitMsg& commit);

  // Decodes a Timestamp from the provided string and updates the clock
  // with it.
  Status UpdateClock(uint64_t timestamp);

  // Return a log prefix string in the standard "T xxx P yyy" format.
  const string& LogPrefix() const {
    return log_prefix_;
  }

  // Log a status message and set the TabletReplica's status as well.
  void SetStatusMessage(string status);

  const scoped_refptr<TabletMetadata> tablet_meta_;
  const string log_prefix_;
  const RaftConfigPB committed_raft_config_;
  Clock* clock_;
  shared_ptr<MemTracker> mem_tracker_;
  scoped_refptr<rpc::ResultTracker> result_tracker_;
  MetricRegistry* metric_registry_;
  FileCache* file_cache_;
  scoped_refptr<TabletReplica> tablet_replica_;
  unique_ptr<tablet::Tablet> tablet_;
  const scoped_refptr<log::LogAnchorRegistry> log_anchor_registry_;
  scoped_refptr<log::Log> log_;
  shared_ptr<log::LogReader> log_reader_;

  // Statistics on the replay of entries in the log.
  struct Stats {
    Stats()
      : ops_read(0),
        ops_overwritten(0),
        ops_ignored(0),
        ops_committed(0),
        inserts_seen(0),
        inserts_ignored(0),
        mutations_seen(0),
        mutations_ignored(0),
        orphaned_commits(0) {
    }

    string ToString() const {
      return Substitute("ops{read=$0 overwritten=$1 applied=$2 ignored=$3} "
                        "inserts{seen=$4 ignored=$5} "
                        "mutations{seen=$6 ignored=$7} "
                        "orphaned_commits=$8",
                        ops_read, ops_overwritten, ops_committed, ops_ignored,
                        inserts_seen, inserts_ignored,
                        mutations_seen, mutations_ignored,
                        orphaned_commits);
    }

    // Number of REPLICATE messages read from the log
    int ops_read;
    // Number of REPLICATE messages which were overwritten by later entries.
    int ops_overwritten;
    // Number of REPLICATE messages which were able to be completely ignored
    // because the COMMIT message indicated that all of the contained operations
    // were already flushed.
    int ops_ignored;
    // Number of REPLICATE messages for which a matching COMMIT was found.
    int ops_committed;

    // Number inserts/mutations seen and ignored. Note inserts_ignored and mutations_ignored
    // do not refer to the INSERT_IGNORE, UPDATE_IGNORE, and DELETE_IGNORE operations.
    // They refer to inserts and mutations ignored during log replay.
    int inserts_seen, inserts_ignored;
    int mutations_seen, mutations_ignored;

    // Number of COMMIT messages for which a corresponding REPLICATE was not found.
    int orphaned_commits;
  };
  Stats stats_;

  // Snapshot of which stores were flushed prior to restart.
  FlushedStoresSnapshot flushed_stores_;

  // Transactions that were persisted as being in-flight (neither finalized or
  // aborted) and completed prior to restart.
  unordered_set<int64_t> in_flight_txn_ids_;
  unordered_set<int64_t> terminal_txn_ids_;

  // Transactions (committed or not) that have active MemRowSets.
  unordered_set<int64_t> mrs_txn_ids_;

  DISALLOW_COPY_AND_ASSIGN(TabletBootstrap);
};

void TabletBootstrap::SetStatusMessage(string status) {
  LOG_WITH_PREFIX(INFO) << status;
  if (tablet_replica_) {
    tablet_replica_->SetStatusMessage(std::move(status));
  }
}

Status BootstrapTablet(scoped_refptr<TabletMetadata> tablet_meta,
                       RaftConfigPB committed_raft_config,
                       Clock* clock,
                       shared_ptr<MemTracker> mem_tracker,
                       scoped_refptr<ResultTracker> result_tracker,
                       MetricRegistry* metric_registry,
                       FileCache* file_cache,
                       scoped_refptr<TabletReplica> tablet_replica,
                       scoped_refptr<log::LogAnchorRegistry> log_anchor_registry,
                       shared_ptr<tablet::Tablet>* rebuilt_tablet,
                       scoped_refptr<log::Log>* rebuilt_log,
                       ConsensusBootstrapInfo* consensus_info) {
  TRACE_EVENT1("tablet", "BootstrapTablet",
               "tablet_id", tablet_meta->tablet_id());
  TabletBootstrap bootstrap(std::move(tablet_meta),
                            std::move(committed_raft_config),
                            clock,
                            std::move(mem_tracker),
                            std::move(result_tracker),
                            metric_registry,
                            file_cache,
                            std::move(tablet_replica),
                            std::move(log_anchor_registry));
  RETURN_NOT_OK(bootstrap.Bootstrap(rebuilt_tablet, rebuilt_log, consensus_info));
  // This is necessary since OpenNewLog() initially disables sync.
  RETURN_NOT_OK((*rebuilt_log)->ReEnableSyncIfRequired());
  return Status::OK();
}

static string DebugInfo(const string& tablet_id,
                        int segment_seqno,
                        int entry_idx,
                        const string& segment_path,
                        const string& entry_debug_info) {
  // Truncate the debug string to a reasonable length for logging.
  // Otherwise, glog will truncate for us and we may miss important
  // information which came after this long string.
  string debug_str = entry_debug_info;
  if (debug_str.size() > 500) {
    debug_str.resize(500);
    debug_str.append("...");
  }
  if (!debug_str.empty()) {
    debug_str = Substitute(" Entry: $0", debug_str);
  }
  return Substitute("Debug Info: Error playing entry $0 of segment $1 of tablet $2. "
                    "Segment path: $3.$4", entry_idx, segment_seqno, tablet_id,
                    segment_path, debug_str);
}

TabletBootstrap::TabletBootstrap(
    scoped_refptr<TabletMetadata> tablet_meta,
    RaftConfigPB committed_raft_config,
    Clock* clock,
    shared_ptr<MemTracker> mem_tracker,
    scoped_refptr<ResultTracker> result_tracker,
    MetricRegistry* metric_registry,
    FileCache* file_cache,
    scoped_refptr<TabletReplica> tablet_replica,
    scoped_refptr<LogAnchorRegistry> log_anchor_registry)
    : tablet_meta_(std::move(tablet_meta)),
      log_prefix_(Substitute("T $0 P $1: ",
                             tablet_meta_->tablet_id(),
                             tablet_meta_->fs_manager()->uuid())),
      committed_raft_config_(std::move(committed_raft_config)),
      clock_(clock),
      mem_tracker_(std::move(mem_tracker)),
      result_tracker_(std::move(result_tracker)),
      metric_registry_(metric_registry),
      file_cache_(file_cache),
      tablet_replica_(std::move(tablet_replica)),
      log_anchor_registry_(std::move(log_anchor_registry)) {}

Status TabletBootstrap::Bootstrap(shared_ptr<Tablet>* rebuilt_tablet,
                                  scoped_refptr<Log>* rebuilt_log,
                                  ConsensusBootstrapInfo* consensus_info) {
  // We pin (prevent) metadata flush at the beginning of the bootstrap process
  // and always unpin it at the end.
  tablet_meta_->PinFlush();

  // Now run the actual bootstrap process.
  Status bootstrap_status = RunBootstrap(rebuilt_tablet, rebuilt_log, consensus_info);

  // Add a callback to TabletMetadata that makes sure that each time we flush the metadata
  // we also wait for in-flights to finish and for their wal entry to be fsynced.
  // This might be a bit conservative in some situations but it will prevent us from
  // ever flushing the metadata referring to tablet data blocks containing data whose
  // commit entries are not durable, a pre-requisite for recovery.
  CHECK((*rebuilt_tablet && *rebuilt_log) || !bootstrap_status.ok())
      << "Tablet and Log not initialized";
  if (bootstrap_status.ok()) {
    auto cb = make_scoped_refptr(new FlushInflightsToLogCallback(
        rebuilt_tablet->get(), *rebuilt_log));
    tablet_meta_->SetPreFlushCallback(
        [cb]() { return cb->WaitForInflightsAndFlushLog(); });
  }

  // This will cause any pending TabletMetadata flush to be executed.
  Status unpin_status = tablet_meta_->UnPinFlush();

  constexpr char kFailedUnpinMsg[] = "Failed to flush after unpinning";
  if (PREDICT_FALSE(!bootstrap_status.ok() && !unpin_status.ok())) {
    LOG_WITH_PREFIX(WARNING) << kFailedUnpinMsg << ": " << unpin_status.ToString();
    return bootstrap_status;
  }
  RETURN_NOT_OK(bootstrap_status);
  RETURN_NOT_OK_PREPEND(unpin_status, Substitute("$0$1", LogPrefix(), kFailedUnpinMsg));
  return Status::OK();
}

Status TabletBootstrap::RunBootstrap(shared_ptr<Tablet>* rebuilt_tablet,
                                     scoped_refptr<Log>* rebuilt_log,
                                     ConsensusBootstrapInfo* consensus_info) {
  string tablet_id = tablet_meta_->tablet_id();

  // Make sure we don't try to locally bootstrap a tablet that was in the middle
  // of a tablet copy. It's likely that not all files were copied over
  // successfully.
  TabletDataState tablet_data_state = tablet_meta_->tablet_data_state();
  if (tablet_data_state != TABLET_DATA_READY) {
    return Status::Corruption("Unable to locally bootstrap tablet " + tablet_id + ": " +
                              "TabletMetadata bootstrap state is " +
                              TabletDataState_Name(tablet_data_state));
  }

  SetStatusMessage("Bootstrap starting.");

  if (VLOG_IS_ON(1)) {
    TabletSuperBlockPB super_block;
    RETURN_NOT_OK(tablet_meta_->ToSuperBlock(&super_block));
    VLOG_WITH_PREFIX(1) << "Tablet Metadata: " << SecureDebugString(super_block);
  }

  // Ensure the tablet's data dirs are present and healthy before it is opened.
  DataDirGroupPB data_dir_group;
  RETURN_NOT_OK_PREPEND(
      tablet_meta_->fs_manager()->dd_manager()->GetDataDirGroupPB(tablet_id, &data_dir_group),
      "error retrieving tablet data dir group (one or more data dirs may have been removed)");
  if (tablet_meta_->fs_manager()->dd_manager()->IsTabletInFailedDir(tablet_id)) {
    return Status::IOError("some tablet data is in a failed directory");
  }

  RETURN_NOT_OK(flushed_stores_.InitFrom(*tablet_meta_.get()));
  tablet_meta_->GetTxnIds(&in_flight_txn_ids_, &terminal_txn_ids_, &mrs_txn_ids_);

  bool has_blocks;
  RETURN_NOT_OK(OpenTablet(&has_blocks));

  bool needs_recovery;
  RETURN_NOT_OK(PrepareRecoveryDir(&needs_recovery));
  if (needs_recovery) {
    RETURN_NOT_OK(OpenLogReaderInRecoveryDir());
  }

  // This is a new tablet, nothing left to do.
  if (!has_blocks && !needs_recovery) {
    LOG_WITH_PREFIX(INFO) << "Neither blocks nor log segments found. Creating new log.";
    RETURN_NOT_OK_PREPEND(OpenNewLog(), "Failed to open new log");
    RETURN_NOT_OK(FinishBootstrap("No bootstrap required, opened a new log",
                                  rebuilt_log, rebuilt_tablet));
    consensus_info->last_id = MinimumOpId();
    consensus_info->last_committed_id = MinimumOpId();
    return Status::OK();
  }

  // If there were blocks, there must be segments to replay. This is required
  // by Raft, since we always need to know the term and index of the last
  // logged op in order to vote, know how to respond to AppendEntries(), etc.
  if (has_blocks && !needs_recovery) {
    return Status::IllegalState(Substitute("Tablet $0: Found rowsets but no log "
                                           "segments could be found.",
                                           tablet_id));
  }

  IOContext io_context({ tablet_meta_->table_id() });
  RETURN_NOT_OK_PREPEND(PlaySegments(&io_context, consensus_info), "Failed log replay. Reason");

  RETURN_NOT_OK(Log::RemoveRecoveryDirIfExists(tablet_->metadata()->fs_manager(),
                                               tablet_->metadata()->tablet_id()));
  RETURN_NOT_OK(FinishBootstrap("Bootstrap complete.", rebuilt_log, rebuilt_tablet));

  return Status::OK();
}

Status TabletBootstrap::FinishBootstrap(const string& message,
                                      scoped_refptr<log::Log>* rebuilt_log,
                                      shared_ptr<Tablet>* rebuilt_tablet) {
  RETURN_NOT_OK(tablet_->MarkFinishedBootstrapping());
  SetStatusMessage(message);
  rebuilt_tablet->reset(tablet_.release());
  rebuilt_log->swap(log_);
  return Status::OK();
}

Status TabletBootstrap::OpenTablet(bool* has_blocks) {
  unique_ptr<Tablet> tablet(new Tablet(tablet_meta_,
                                       clock_,
                                       mem_tracker_,
                                       metric_registry_,
                                       log_anchor_registry_));
  // doing nothing for now except opening a tablet locally.
  {
    SCOPED_LOG_SLOW_EXECUTION_PREFIX(INFO, 100, LogPrefix(), "opening tablet");
    RETURN_NOT_OK(tablet->Open(in_flight_txn_ids_, mrs_txn_ids_));
  }
  *has_blocks = tablet->num_rowsets() != 0;
  tablet_ = std::move(tablet);
  return Status::OK();
}

Status TabletBootstrap::PrepareRecoveryDir(bool* needs_recovery) {
  *needs_recovery = false;

  FsManager* fs_manager = tablet_->metadata()->fs_manager();
  string tablet_id = tablet_->metadata()->tablet_id();
  string log_dir = fs_manager->GetTabletWalDir(tablet_id);

  // If the recovery directory exists, then we crashed mid-recovery.
  // Throw away any logs from the previous recovery attempt and restart the log
  // replay process from the beginning using the same recovery dir as last time.
  string recovery_path = fs_manager->GetTabletWalRecoveryDir(tablet_id);
  if (fs_manager->Exists(recovery_path)) {
    LOG_WITH_PREFIX(INFO) << "Previous recovery directory found at " << recovery_path << ": "
                          << "Replaying log files from this location instead of " << log_dir;

    // Since we have a recovery directory, clear out the log_dir by recursively
    // deleting it and creating a new one so that we don't end up with remnants
    // of old WAL segments or indexes after replay.
    if (fs_manager->Exists(log_dir)) {
      LOG_WITH_PREFIX(INFO) << "Deleting old log files from previous recovery attempt in "
                            << log_dir;
      RETURN_NOT_OK_PREPEND(fs_manager->GetEnv()->DeleteRecursively(log_dir),
                            "Could not recursively delete old log dir " + log_dir);
    }

    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(fs_manager->GetEnv(), log_dir),
                          "Failed to create log directory " + log_dir);

    *needs_recovery = true;
    return Status::OK();
  }

  // If we made it here, there was no pre-existing recovery dir.
  // Now we look for log files in log_dir, and if we find any then we rename
  // the whole log_dir to a recovery dir and return needs_recovery = true.
  RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(fs_manager->GetEnv(), log_dir),
                        "Failed to create log dir");

  vector<string> children;
  RETURN_NOT_OK_PREPEND(fs_manager->ListDir(log_dir, &children),
                        "Couldn't list log segments.");
  for (const string& child : children) {
    if (!log::IsLogFileName(child)) {
      continue;
    }

    string source_path = JoinPathSegments(log_dir, child);
    string dest_path = JoinPathSegments(recovery_path, child);
    VLOG_WITH_PREFIX(1) << "Will attempt to recover log segment " << source_path
                        << " to " << dest_path;
    *needs_recovery = true;
  }

  if (*needs_recovery) {
    // Atomically rename the log directory to the recovery directory
    // and then re-create the log directory.
    VLOG_WITH_PREFIX(1) << "Moving log directory " << log_dir << " to recovery directory "
                        << recovery_path << " in preparation for log replay";
    RETURN_NOT_OK_PREPEND(fs_manager->GetEnv()->RenameFile(log_dir, recovery_path),
                          Substitute("Could not move log directory $0 to recovery dir $1",
                                     log_dir, recovery_path));
    RETURN_NOT_OK_PREPEND(fs_manager->GetEnv()->CreateDir(log_dir),
                          "Failed to recreate log directory " + log_dir);
  }
  return Status::OK();
}

Status TabletBootstrap::OpenLogReaderInRecoveryDir() {
  const string& tablet_id = tablet_->tablet_id();
  FsManager* fs_manager = tablet_meta_->fs_manager();
  VLOG_WITH_PREFIX(1) << "Opening log reader in log recovery dir "
                      << fs_manager->GetTabletWalRecoveryDir(tablet_id);
  // Open the reader.
  // Since we're recovering, we don't want to have any log index -- since it
  // isn't fsynced() during writing, its contents are useless to us.
  scoped_refptr<LogIndex> log_index(nullptr);
  const string recovery_dir = fs_manager->GetTabletWalRecoveryDir(tablet_id);
  RETURN_NOT_OK_PREPEND(LogReader::Open(fs_manager->GetEnv(), recovery_dir, log_index, tablet_id,
                                        tablet_->GetMetricEntity().get(),
                                        file_cache_,
                                        &log_reader_),
                        "Could not open LogReader. Reason");
  return Status::OK();
}

Status TabletBootstrap::OpenNewLog() {
  RETURN_NOT_OK(Log::Open(LogOptions(),
                          tablet_->metadata()->fs_manager(),
                          file_cache_,
                          tablet_->tablet_id(),
                          *tablet_->schema(),
                          tablet_->metadata()->schema_version(),
                          tablet_->GetMetricEntity(),
                          &log_));
  // Disable sync temporarily in order to speed up appends during the
  // bootstrap process.
  log_->DisableSync();
  return Status::OK();
}

typedef map<int64_t, unique_ptr<LogEntryPB>> OpIndexToEntryMap;

// State kept during replay.
struct ReplayState {
  ReplayState()
      : prev_op_id(MinimumOpId()),
        committed_op_id(MinimumOpId()) {
  }

  // Return true if 'b' is allowed to immediately follow 'a' in the log.
  static bool IsValidSequence(const OpId& a, const OpId& b) {
    if (PREDICT_FALSE(a.term() == 0 && a.index() == 0)) {
      // Not initialized - can start with any opid.
      return true;
    }

    // Within the same term, we should never skip entries.
    // We can, however go backwards (see KUDU-783 for an example)
    if (b.term() == a.term() &&
        b.index() > a.index() + 1) {
      return false;
    }

    return true;
  }

  // Return a Corruption status if 'msg' seems to be out-of-sequence in the log.
  Status CheckSequentialReplicateId(const ReplicateMsg& msg) {
    DCHECK(msg.has_id());
    if (PREDICT_FALSE(!IsValidSequence(prev_op_id, msg.id()))) {
      string op_desc = Substitute("$0 REPLICATE (Type: $1)",
                                  OpIdToString(msg.id()),
                                  OperationType_Name(msg.op_type()));
      return Status::Corruption(
        Substitute("Unexpected opid following opid $0. Operation: $1",
                   OpIdToString(prev_op_id),
                   op_desc));
    }

    prev_op_id = msg.id();
    return Status::OK();
  }

  void UpdateCommittedOpId(const OpId& id) {
    if (id.index() > committed_op_id.index()) {
      committed_op_id = id;
    }
  }

  void AddEntriesToStrings(const OpIndexToEntryMap& entries, vector<string>* strings) const {
    for (const auto& map_entry : entries) {
      const LogEntryPB* entry = DCHECK_NOTNULL(map_entry.second.get());
      strings->push_back(Substitute("   $0", SecureShortDebugString(*entry)));
    }
  }

  void DumpReplayStateToStrings(vector<string>* strings) const {
    strings->push_back(Substitute("ReplayState: Previous OpId: $0, Committed OpId: $1, "
        "Pending Replicates: $2, Pending Commits: $3", OpIdToString(prev_op_id),
        OpIdToString(committed_op_id), pending_replicates.size(), pending_commits.size()));
    if (!pending_replicates.empty()) {
      strings->push_back("Dumping REPLICATES: ");
      AddEntriesToStrings(pending_replicates, strings);
    }
    if (!pending_commits.empty()) {
      strings->push_back("Dumping COMMITS: ");
      AddEntriesToStrings(pending_commits, strings);
    }
  }

  // The last replicate message's ID.
  OpId prev_op_id;

  // The last operation known to be committed.
  // All other operations with lower IDs are also committed.
  OpId committed_op_id;

  // REPLICATE log entries whose corresponding COMMIT record has
  // not yet been seen. Keyed by index.
  OpIndexToEntryMap pending_replicates;

  // COMMIT log entries which couldn't be applied immediately.
  OpIndexToEntryMap pending_commits;
};

// Handle the given log entry.
Status TabletBootstrap::HandleEntry(const IOContext* io_context,
                                    ReplayState* state,
                                    unique_ptr<LogEntryPB> entry,
                                    string* entry_debug_info) {
  DCHECK(entry);
  VLOG_WITH_PREFIX(1) << "Handling entry: " << SecureShortDebugString(*entry);

  const auto entry_type = entry->type();
  switch (entry_type) {
    case log::REPLICATE:
      RETURN_NOT_OK(HandleReplicateMessage(state, std::move(entry), entry_debug_info));
      break;
    case log::COMMIT:
      // check the unpaired ops for the matching replicate msg, abort if not found
      RETURN_NOT_OK(HandleCommitMessage(io_context, state,
                                        std::move(entry), entry_debug_info));
      break;
    default:
      return Status::Corruption(Substitute("unexpected log entry type: $0", entry_type));
  }
  MAYBE_FAULT(FLAGS_fault_crash_during_log_replay);
  return Status::OK();
}

// Repair overflow issue reported in KUDU-1933.
void CheckAndRepairOpIdOverflow(OpId* opid) {
  if (PREDICT_FALSE(opid->term() < consensus::kMinimumTerm)) {
    int64_t overflow = opid->term() - INT32_MIN + 1LL;
    CHECK_GE(overflow, 1) << OpIdToString(*opid);
    opid->set_term(static_cast<int64_t>(INT32_MAX) + overflow);
  }
  if (PREDICT_FALSE(opid->index() < consensus::kMinimumOpIdIndex &&
                    opid->index() != consensus::kInvalidOpIdIndex)) {
    int64_t overflow = opid->index() - INT32_MIN + 1LL;
    CHECK_GE(overflow, 1) << OpIdToString(*opid);
    // Sanity check. Even with the bug in KUDU-1933, the number of bytes
    // allowed in a single group commit is a generous upper bound on how far a
    // log index may have overflowed before causing a crash.
    CHECK_LT(overflow, FLAGS_group_commit_queue_size_bytes) << OpIdToString(*opid);
    opid->set_index(static_cast<int64_t>(INT32_MAX) + overflow);
  }
}

Status TabletBootstrap::HandleReplicateMessage(ReplayState* state,
                                               unique_ptr<LogEntryPB> entry,
                                               string* entry_debug_info) {
  auto info_collector = MakeScopedCleanup([&]() {
    if (entry) {
      *entry_debug_info = SecureShortDebugString(*entry);
    }
  });
  DCHECK(entry->has_replicate())
      << "not a replicate message: " << SecureDebugString(*entry);
  stats_.ops_read++;

  // Fix overflow if necessary (see KUDU-1933).
  CheckAndRepairOpIdOverflow(entry->mutable_replicate()->mutable_id());

  const ReplicateMsg& replicate = entry->replicate();
  RETURN_NOT_OK(state->CheckSequentialReplicateId(replicate));
  DCHECK(replicate.has_timestamp());
  // TODO(yingchun): Should we try to update clock by batch?
  CHECK_OK(UpdateClock(replicate.timestamp()));

  // Append the replicate message to the log as is
  RETURN_NOT_OK(log_->Append(entry.get()));

  const int64_t index = replicate.id().index();
  const auto existing_entry_iter = state->pending_replicates.find(index);
  if (PREDICT_FALSE(existing_entry_iter != state->pending_replicates.end())) {
    // If there was a entry with the same index we're overwriting then we need
    // to delete that entry and all entries with higher indexes.
    const auto& existing_entry = existing_entry_iter->second;

    auto iter = state->pending_replicates.lower_bound(index);
    DCHECK(iter->second->replicate().id() == existing_entry->replicate().id());

    const auto& last_entry = state->pending_replicates.rbegin()->second;
    VLOG_WITH_PREFIX(1) << "Overwriting operations starting at: "
                        << existing_entry->replicate().id()
                        << " up to: " << last_entry->replicate().id()
                        << " with operation: " << replicate.id();

    while (iter != state->pending_replicates.end()) {
      iter = state->pending_replicates.erase(iter);
      stats_.ops_overwritten++;
    }
  }
  EmplaceOrDie(&state->pending_replicates, index, std::move(entry));
  info_collector.cancel();

  return Status::OK();
}

// On returning OK, takes ownership of the pointer from the 'entry_ptr' wrapper.
Status TabletBootstrap::HandleCommitMessage(const IOContext* io_context, ReplayState* state,
                                            unique_ptr<LogEntryPB> entry,
                                            string* entry_debug_info) {
  auto info_collector = MakeScopedCleanup([&]() {
    if (entry) {
      *entry_debug_info = SecureShortDebugString(*entry);
    }
  });
  DCHECK(entry->has_commit())
      << "not a commit message: " << SecureDebugString(*entry);

  // Fix overflow if necessary (see KUDU-1933).
  CheckAndRepairOpIdOverflow(entry->mutable_commit()->mutable_commited_op_id());

  // Match up the COMMIT record with the original entry that it's applied to.
  const OpId& committed_op_id = entry->commit().commited_op_id();
  state->UpdateCommittedOpId(committed_op_id);

  // If there are no pending replicates, or if this commit's index is lower than the
  // the first pending replicate on record this is likely an orphaned commit.
  if (PREDICT_FALSE(state->pending_replicates.empty() ||
      (*state->pending_replicates.begin()).first > committed_op_id.index())) {
    VLOG_WITH_PREFIX(2) << "Found orphaned commit for " << committed_op_id;
    RETURN_NOT_OK(CheckOrphanedCommitDoesntNeedReplay(entry->commit()));
    stats_.orphaned_commits++;
    info_collector.cancel();
    return Status::OK();
  }

  // If this commit does not correspond to the first replicate message in the pending
  // replicates set we keep it to apply later...
  if ((*state->pending_replicates.begin()).first != committed_op_id.index()) {
    if (!ContainsKey(state->pending_replicates, committed_op_id.index())) {
      return Status::Corruption(Substitute("Could not find replicate for commit: $0",
                                           SecureShortDebugString(*entry)));
    }
    VLOG_WITH_PREFIX(2) << "Adding pending commit for " << committed_op_id;
    EmplaceOrDie(&state->pending_commits, committed_op_id.index(), std::move(entry));
    info_collector.cancel();
    return Status::OK();
  }

  // ... if it does, we apply it and all the commits that immediately follow in the sequence.
  OpId last_applied = committed_op_id;
  RETURN_NOT_OK(ApplyCommitMessage(io_context, state, entry.get()));

  auto iter = state->pending_commits.begin();
  while (iter != state->pending_commits.end()) {
    if (iter->first == last_applied.index() + 1) {
      auto& buffered_commit_entry(iter->second);
      last_applied = buffered_commit_entry->commit().commited_op_id();
      RETURN_NOT_OK(ApplyCommitMessage(io_context, state, buffered_commit_entry.get()));
      iter = state->pending_commits.erase(iter);
      continue;
    }
    break;
  }

  info_collector.cancel();
  return Status::OK();
}

TabletBootstrap::ActiveStores TabletBootstrap::AnalyzeActiveStores(const CommitMsg& commit) {
  bool has_mutated_stores = false;

  for (const OperationResultPB& op_result : commit.result().ops()) {
    for (const MemStoreTargetPB& mutated_store : op_result.mutated_stores()) {
      has_mutated_stores = true;
      if (flushed_stores_.IsMemStoreActive(mutated_store)) {
        return SOME_STORES_ACTIVE;
      }
    }
  }

  return has_mutated_stores ? NO_STORES_ACTIVE : NO_MUTATED_STORES;
}

Status TabletBootstrap::CheckOrphanedCommitDoesntNeedReplay(const CommitMsg& commit) {
  if (AnalyzeActiveStores(commit) == SOME_STORES_ACTIVE) {
    TabletSuperBlockPB super;
    WARN_NOT_OK(tablet_meta_->ToSuperBlock(&super),
                Substitute("$0$1", LogPrefix(), "Couldn't build TabletSuperBlockPB"));
    return Status::Corruption(Substitute("CommitMsg was orphaned but it referred to "
        "stores which need replay. Commit: $0. TabletMetadata: $1",
        SecureShortDebugString(commit),
        SecureShortDebugString(super)));
  }

  return Status::OK();
}

Status TabletBootstrap::ApplyCommitMessage(const IOContext* io_context,
                                           ReplayState* state, LogEntryPB* entry) {
  const OpId& committed_op_id = entry->commit().commited_op_id();
  VLOG_WITH_PREFIX(2) << "Applying commit for " << committed_op_id;

  // They should also have an associated replicate index (it may have been in a
  // deleted log segment though).
  unique_ptr<LogEntryPB> pending_replicate_entry(EraseKeyReturnValuePtr(
      &state->pending_replicates, committed_op_id.index()));
  if (PREDICT_TRUE(pending_replicate_entry)) {
    // We found a replicate with the same index, make sure it also has the same term.
    const auto& replicate = pending_replicate_entry->replicate();
    if (PREDICT_FALSE(committed_op_id != replicate.id())) {
      string error_msg = Substitute("Committed operation's OpId: $0 didn't match the"
          "commit message's committed OpId: $1. Pending operation: $2, Commit message: $3",
          SecureShortDebugString(replicate.id()),
          SecureShortDebugString(committed_op_id),
          SecureShortDebugString(replicate),
          SecureShortDebugString(entry->commit()));
      LOG_WITH_PREFIX(DFATAL) << error_msg;
      return Status::Corruption(error_msg);
    }
    RETURN_NOT_OK(HandleEntryPair(io_context, pending_replicate_entry.get(), entry));
    stats_.ops_committed++;
  } else {
    stats_.orphaned_commits++;
    RETURN_NOT_OK(CheckOrphanedCommitDoesntNeedReplay(entry->commit()));
  }

  return Status::OK();
}

// Never deletes 'replicate_entry' or 'commit_entry'.
Status TabletBootstrap::HandleEntryPair(const IOContext* io_context, LogEntryPB* replicate_entry,
                                        LogEntryPB* commit_entry) {
  const char* error_fmt = "Failed to play $0 request. ReplicateMsg: { $1 }, CommitMsg: { $2 }";

#define RETURN_NOT_OK_REPLAY(ReplayMethodName, io_context, replicate, commit)       \
  RETURN_NOT_OK_PREPEND(ReplayMethodName(io_context, replicate, commit),            \
                        Substitute(error_fmt, OperationType_Name(op_type),          \
                                   SecureShortDebugString(*(replicate)),            \
                                   SecureShortDebugString(commit)))

  ReplicateMsg* replicate = replicate_entry->mutable_replicate();
  const CommitMsg& commit = commit_entry->commit();
  OperationType op_type = commit.op_type();

  switch (op_type) {
    case WRITE_OP:
      RETURN_NOT_OK_REPLAY(PlayWriteRequest, io_context, replicate, commit);
      break;

    case ALTER_SCHEMA_OP:
      RETURN_NOT_OK_REPLAY(PlayAlterSchemaRequest, io_context, replicate, commit);
      break;

    case CHANGE_CONFIG_OP:
      RETURN_NOT_OK_REPLAY(PlayChangeConfigRequest, io_context, replicate, commit);
      break;

    case PARTICIPANT_OP:
      RETURN_NOT_OK_REPLAY(PlayTxnParticipantOpRequest, io_context, replicate, commit);
      break;

    case NO_OP:
      RETURN_NOT_OK_REPLAY(PlayNoOpRequest, io_context, replicate, commit);
      break;

    default:
      return Status::IllegalState(Substitute("Unsupported commit entry type: $0",
                                             commit.op_type()));
  }

#undef RETURN_NOT_OK_REPLAY

  // TODO(yingchun): We should try to avoid update MVCC's safe time for every entry?
  // We should only advance MVCC's safe time based on a specific set of
  // operations: those whose timestamps are guaranteed to be monotonically
  // increasing with respect to their entries in the write-ahead log.
  bool timestamp_assigned_in_opid_order = true;
  switch (op_type) {
    case CHANGE_CONFIG_OP:
      timestamp_assigned_in_opid_order = false;
      break;
    case NO_OP: {
      const auto& req = replicate->noop_request();
      if (req.has_timestamp_in_opid_order()) {
        timestamp_assigned_in_opid_order = req.timestamp_in_opid_order();
      }
      break;
    }
    default:
      break;
  }
  if (!timestamp_assigned_in_opid_order ||
      PREDICT_FALSE(!FLAGS_prevent_kudu_2233_corruption)) {
    return Status::OK();
  }

  // Handle advancement of our new timestamp lower bound watermark.
  //
  // If this message is a Raft election no-op, or is an op that has an external
  // consistency mode other than COMMIT_WAIT, we know that no future op will
  // have a timestamp that is lower than it, so we can just advance the safe
  // timestamp to the message's timestamp.
  //
  // If the hybrid clock is disabled, all ops will fall into this category.
  Timestamp new_lower_bound;
  if (replicate->op_type() != consensus::WRITE_OP ||
      replicate->write_request().external_consistency_mode() != COMMIT_WAIT) {
    new_lower_bound = Timestamp(replicate->timestamp());
  // ... else we set the new timestamp lower bound to be the op's
  // timestamp minus the maximum clock error. This opens the door for problems
  // if the flags changed across reboots, but this is unlikely and the problem
  // would manifest itself immediately and clearly (mvcc would complain the
  // operation is already committed, with a CHECK failure).
  } else {
    DCHECK(clock_->SupportsExternalConsistencyMode(COMMIT_WAIT)) << "The provided clock does not"
        "support COMMIT_WAIT external consistency mode.";
    new_lower_bound = clock::HybridClock::AddPhysicalTimeToTimestamp(
        Timestamp(replicate->timestamp()),
        MonoDelta::FromMicroseconds(-FLAGS_max_clock_sync_error_usec));
  }
  tablet_->mvcc_manager()->AdjustNewOpLowerBound(new_lower_bound);

  return Status::OK();
}

void TabletBootstrap::DumpReplayStateToLog(const ReplayState& state) {
  // Dump the replay state, this will log the pending replicates as well as the pending commits,
  // which might be useful for debugging.
  vector<string> state_dump;
  state.DumpReplayStateToStrings(&state_dump);
  for (const string& string : state_dump) {
    LOG_WITH_PREFIX(INFO) << string;
  }
}

Status TabletBootstrap::PlaySegments(const IOContext* io_context,
                                     ConsensusBootstrapInfo* consensus_info) {
  ReplayState state;
  log::SegmentSequence segments;
  log_reader_->GetSegmentsSnapshot(&segments);

  // The first thing to do is to rewind the tablet's schema back to the schema
  // as of the point in time where the logs begin. We must replay the writes
  // in the logs with the correct point-in-time schema.
  if (!segments.empty()) {
    const scoped_refptr<ReadableLogSegment>& segment = segments[0];
    // Set the point-in-time schema for the tablet based on the log header.
    Schema pit_schema;
    RETURN_NOT_OK_PREPEND(SchemaFromPB(segment->header().schema(), &pit_schema),
                          "Couldn't decode log segment schema");
    RETURN_NOT_OK_PREPEND(tablet_->RewindSchemaForBootstrap(
                              pit_schema, segment->header().schema_version()),
                          "couldn't set point-in-time schema");
  }

  // We defer opening the log until here, so that we properly reproduce the
  // point-in-time schema from the log we're reading into the log we're
  // writing.
  RETURN_NOT_OK_PREPEND(OpenNewLog(), "Failed to open new log");

  auto last_status_update = MonoTime::Now();
  const auto kStatusUpdateInterval = MonoDelta::FromSeconds(5);
  int segment_count = 0;

  for (const scoped_refptr<ReadableLogSegment>& segment : segments) {
    log::LogEntryReader reader(segment.get());

    int entry_count = 0;
    while (true) {
      {
        unique_ptr<LogEntryPB> entry;
        Status s = reader.ReadNextEntry(&entry);
        if (PREDICT_FALSE(!s.ok())) {
          if (s.IsEndOfFile()) {
            break;
          }
          return Status::Corruption(
              Substitute("Error reading Log Segment of tablet $0: $1 "
                         "(Read up to entry $2 of segment $3, in path $4)",
                         tablet_->tablet_id(),
                         s.ToString(),
                         entry_count,
                         segment->header().sequence_number(),
                         segment->path()));
        }
        entry_count++;

        string entry_debug_info;
        s = HandleEntry(io_context, &state, std::move(entry), &entry_debug_info);
        if (PREDICT_FALSE(!s.ok())) {
          DumpReplayStateToLog(state);
          RETURN_NOT_OK_PREPEND(s, DebugInfo(tablet_->tablet_id(),
                                             segment->header().sequence_number(),
                                             entry_count, segment->path(),
                                             entry_debug_info));
        }
      }

      const auto now = MonoTime::Now();
      if (now - last_status_update > kStatusUpdateInterval) {
        SetStatusMessage(Substitute("Bootstrap replaying log segment $0/$1 "
                                    "($2/$3 this segment, stats: $4)",
                                    segment_count + 1, log_reader_->num_segments(),
                                    HumanReadableNumBytes::ToString(reader.offset()),
                                    HumanReadableNumBytes::ToString(reader.read_up_to_offset()),
                                    stats_.ToString()));
        last_status_update = now;
      }
    }

    SetStatusMessage(Substitute("Bootstrap replayed $0/$1 log segments. "
                                "Stats: $2. Pending: $3 replicates",
                                segment_count + 1, log_reader_->num_segments(),
                                stats_.ToString(),
                                state.pending_replicates.size()));
    segment_count++;
  }

  // If we have non-applied commits they all must belong to pending operations and
  // they should only pertain to stores which are still active.
  for (const auto& entry : state.pending_commits) {
    if (!ContainsKey(state.pending_replicates, entry.first)) {
      DumpReplayStateToLog(state);
      return Status::Corruption("Had orphaned commits at the end of replay.");
    }

    if (entry.second->commit().op_type() == WRITE_OP &&
        AnalyzeActiveStores(entry.second->commit()) == NO_STORES_ACTIVE) {
      DumpReplayStateToLog(state);
      TabletSuperBlockPB super;
      WARN_NOT_OK(tablet_meta_->ToSuperBlock(&super), "Couldn't build TabletSuperBlockPB.");
      return Status::Corruption(Substitute("CommitMsg was pending but it did not refer "
          "to any active memory stores. Commit: $0. TabletMetadata: $1",
          SecureShortDebugString(entry.second->commit()), SecureShortDebugString(super)));
    }
  }

  // Note that we don't pass the information contained in the pending commits along with
  // ConsensusBootstrapInfo. We know that this is safe as they must refer to active
  // stores (we make doubly sure above).
  //
  // Example/Explanation:
  // Say we have two different operations that touch the same row, one insert and one
  // mutate. Since we use Early Lock Release the commit for the second (mutate) operation
  // might end up in the log before the insert's commit. This wouldn't matter since
  // we replay in order, but a corner case here is that we might crash before we
  // write the commit for the insert, meaning it might not be present at all.
  //
  // One possible log for this situation would be:
  // - Replicate 10.10 (insert)
  // - Replicate 10.11 (mutate)
  // - Commit    10.11 (mutate)
  // ~CRASH while Commit 10.10 is in-flight~
  //
  // We can't replay 10.10 during bootstrap because we haven't seen its commit, but
  // since we can't replay out-of-order we won't replay 10.11 either, in fact we'll
  // pass them both as "pending" to consensus to be applied again.
  //
  // The reason why it is safe to simply disregard 10.11's commit is that we know that
  // it must refer only to active stores. We know this because one important flush/compact
  // pre-condition is:
  // - No flush will become visible on reboot (meaning we won't durably update the tablet
  //   metadata), unless the snapshot under which the flush/compact was performed has no
  //   in-flight ops and all the messages that are in-flight to the log are durable.
  //
  // In our example this means that if we had flushed/compacted after 10.10 was applied
  // (meaning losing the commit message would lead to corruption as we might re-apply it)
  // then the commit for 10.10 would be durable. Since it isn't then no flush/compaction
  // occurred after 10.10 was applied and thus we can disregard the commit message for
  // 10.11 and simply apply both 10.10 and 10.11 as if we hadn't applied them before.
  //
  // This generalizes to:
  // - If a committed replicate message with index Y is missing a commit message,
  //   no later committed replicate message (with index > Y) is visible across reboots
  //   in the tablet data.

  if (VLOG_IS_ON(1)) {
    DumpReplayStateToLog(state);
  }

  // Set up the ConsensusBootstrapInfo structure for the caller.
  for (auto& entry : state.pending_replicates) {
    consensus_info->orphaned_replicates.push_back(entry.second->release_replicate());
  }
  consensus_info->last_id = state.prev_op_id;
  consensus_info->last_committed_id = state.committed_op_id;

  return Status::OK();
}

Status TabletBootstrap::AppendCommitMsg(const CommitMsg& commit_msg) {
  LogEntryPB commit_entry;
  commit_entry.set_type(log::COMMIT);
  CommitMsg* commit = commit_entry.mutable_commit();
  commit->CopyFrom(commit_msg);
  return log_->Append(&commit_entry);
}

Status TabletBootstrap::DetermineSkippedOpsAndBuildResponse(const TxResultPB& orig_result,
                                                            TxResultPB* new_result,
                                                            WriteResponsePB* response,
                                                            bool* all_skipped) {
  int num_ops = orig_result.ops_size();
  new_result->mutable_ops()->Reserve(num_ops);
  *all_skipped = true;

  for (int i = 0; i < num_ops; i++) {
    const auto& orig_op_result = orig_result.ops(i);
    OpAction action;
    RETURN_NOT_OK(FilterOperation(orig_op_result, &action));
    *all_skipped &= action != NEEDS_REPLAY;

    if (action != NEEDS_REPLAY) {
      new_result->mutable_ops(i)->set_skip_on_replay(true);
    }

    if (action == SKIP_PREVIOUSLY_FAILED) {
      if (response) {
        WriteResponsePB::PerRowErrorPB* error = response->add_per_row_errors();
        error->set_row_index(i);
        error->mutable_error()->CopyFrom(orig_op_result.failed_status());
      }
      // If the op is already flushed we won't be applying it.
      DCHECK(orig_op_result.has_failed_status());
      new_result->mutable_ops(i)->mutable_failed_status()->CopyFrom(orig_op_result.failed_status());
    }
  }

  if (*all_skipped) {
    stats_.ops_ignored++;
  }
  return Status::OK();
}

Status TabletBootstrap::PlayWriteRequest(const IOContext* io_context,
                                         ReplicateMsg* replicate_msg,
                                         const CommitMsg& commit_msg) {

  // Set up the new op.
  // Even if we're going to ignore the op, it's important to do this so that
  // MVCC advances.
  DCHECK(replicate_msg->has_timestamp());
  WriteRequestPB* write = replicate_msg->mutable_write_request();

  WriteOpState op_state(nullptr, write, nullptr);
  op_state.mutable_op_id()->CopyFrom(replicate_msg->id());
  op_state.set_timestamp(Timestamp(replicate_msg->timestamp()));

  // Prepare the commit entry for the rewritten log.
  LogEntryPB& commit_entry = *google::protobuf::Arena::CreateMessage<LogEntryPB>(
      op_state.pb_arena());
  commit_entry.set_type(log::COMMIT);
  CommitMsg* new_commit = commit_entry.mutable_commit();
  new_commit->CopyFrom(commit_msg);

  tablet_->StartOp(&op_state);
  tablet_->StartApplying(&op_state);

  unique_ptr<WriteResponsePB> response;

  bool tracking_results = result_tracker_.get() != nullptr && replicate_msg->has_request_id();

  // If the results are being tracked and this write has a request id, register
  // it with the result tracker.
  ResultTracker::RpcState state = ResultTracker::RpcState::NEW;
  if (tracking_results) {
    VLOG(1) << result_tracker_.get() << " Boostrapping request for tablet: "
        << write->tablet_id() << ". State: " << 0 << " id: "
        << SecureDebugString(replicate_msg->request_id());
    // We only replay committed requests so the result of tracking this request can be:
    // NEW:
    //   This is a previously untracked request, or we changed the driver -> store the result
    // COMPLETED or STALE:
    //   We've bootstrapped this tablet twice, and previously stored the result -> do
    //   nothing.
    state = result_tracker_->TrackRpcOrChangeDriver(replicate_msg->request_id());
    CHECK(state == ResultTracker::RpcState::NEW ||
          state == ResultTracker::RpcState::COMPLETED ||
          state == ResultTracker::RpcState::STALE)
        << "Wrong state: " << state;
    response.reset(new WriteResponsePB());
    response->set_timestamp(replicate_msg->timestamp());
  }

  // Determine which of the operations are already flushed to persistent
  // storage and don't need to be re-applied. We can do this even before
  // we decode any row operations, so we can short-circuit that decoding
  // in the case that the entire op has been already flushed.
  TxResultPB* new_result = new_commit->mutable_result();
  bool all_flushed;
  RETURN_NOT_OK(DetermineSkippedOpsAndBuildResponse(commit_msg.result(),
                                                    new_result,
                                                    response.get(),
                                                    &all_flushed));

  if (tracking_results && state == ResultTracker::NEW) {
    result_tracker_->RecordCompletionAndRespond(replicate_msg->request_id(), response.get());
  }

  Status play_status;
  if (!all_flushed && write->has_row_operations()) {
    // Rather than RETURN_NOT_OK() here, we need to just save the status and do the
    // RETURN_NOT_OK() down below the FinishApplying() call below. Even though it seems wrong
    // to commit the op when in fact it failed to apply, we would throw a CHECK
    // failure if we attempted to 'Abort()' after entering the applying stage. Allowing it to
    // Commit isn't problematic because we don't expose the results anyway, and the bad
    // Status returned below will cause us to fail the entire tablet bootstrap anyway.
    play_status = PlayRowOperations(io_context, &op_state, commit_msg.result(), new_result);

    if (play_status.ok()) {
      // Replace the original commit message's result with the new one from the replayed operation.
      op_state.ReleaseTxResultPB(new_commit->mutable_result());
    }
  }

  op_state.FinishApplyingOrAbort(Op::APPLIED);

  // If we failed to apply the operations, fail bootstrap before we write anything incorrect
  // to the recovery log.
  RETURN_NOT_OK(play_status);

  RETURN_NOT_OK(log_->Append(&commit_entry));

  return Status::OK();
}

Status TabletBootstrap::PlayAlterSchemaRequest(const IOContext* /*io_context*/,
                                               ReplicateMsg* replicate_msg,
                                               const CommitMsg& commit_msg) {
  // There are three potential outcomes to expect with this replay:
  // 1. There is no 'result' in the commit message. The alter succeeds, and the
  //    log updates its schema.
  // 2. There is no 'result' in the commit message. The alter fails, and the
  //    log doesn't update its schema. This can happen if trying to replay an
  //    invalid alter schema request from before we started putting the results
  //    in the commit message. Note that we'll leave the commit message as is;
  //    it's harmless since replaying the operation should be a no-op anyway.
  // 3. The commit message contains a 'result', which should only happen if the
  //    alter resulted in a failure. Exit out without attempting the alter.
  if (commit_msg.has_result()) {
    // If we put a result in the commit message, it should be an error and we
    // don't need to replay it. In case, in the future, we decide to put
    // positive results in the commit messages, just filter ops that have
    // failed statuses instead of D/CHECKing.
    DCHECK_EQ(1, commit_msg.result().ops_size());
    const OperationResultPB& op = commit_msg.result().ops(0);
    if (op.has_failed_status()) {
      Status error = StatusFromPB(op.failed_status());
      VLOG(1) << "Played a failed alter request: " << error.ToString();
      return AppendCommitMsg(commit_msg);
    }
  }
  AlterSchemaRequestPB* alter_schema = replicate_msg->mutable_alter_schema_request();

  // Decode schema
  SchemaPtr schema_ptr = std::make_shared<Schema>();
  RETURN_NOT_OK(SchemaFromPB(alter_schema->schema(), schema_ptr.get()));

  AlterSchemaOpState op_state(nullptr, alter_schema, nullptr);
  RETURN_NOT_OK(tablet_->CreatePreparedAlterSchema(&op_state, schema_ptr));

  // Apply the alter schema to the tablet
  RETURN_NOT_OK_PREPEND(tablet_->AlterSchema(&op_state), "Failed to AlterSchema:");

  if (!op_state.error()) {
    // If the alter completed successfully, update the log segment header. Note
    // that our new log isn't hooked up to the tablet yet.
    log_->SetSchemaForNextLogSegment(*schema_ptr, op_state.schema_version());
  }

  return AppendCommitMsg(commit_msg);
}

Status TabletBootstrap::PlayChangeConfigRequest(const IOContext* /*io_context*/,
                                                ReplicateMsg* replicate_msg,
                                                const CommitMsg& commit_msg) {
  // Invariant: The committed config change request is always locally persisted
  // in the consensus metadata before the commit message is written to the WAL.
  if (PREDICT_FALSE(replicate_msg->id().index() > committed_raft_config_.opid_index() &&
                    replicate_msg->change_config_record().tablet_id() == tablet_->tablet_id())) {
    string msg = Substitute("Committed config change op in WAL has opid index ($0) greater than "
                            "config persisted in the consensus metadata ($1). "
                            "Replicate message: {$2}. "
                            "Committed raft config in consensus metadata: {$3}",
                            replicate_msg->id().index(),
                            committed_raft_config_.opid_index(),
                            SecureShortDebugString(*replicate_msg),
                            SecureShortDebugString(committed_raft_config_));
    LOG_WITH_PREFIX(DFATAL) << msg;
    return Status::Corruption(msg);
  }
  return AppendCommitMsg(commit_msg);
}

Status TabletBootstrap::PlayTxnParticipantOpRequest(const IOContext* /*io_context*/,
                                                    ReplicateMsg* replicate_msg,
                                                    const CommitMsg& commit_msg) {
  // When replaying participant ops:
  // - If we've persisted completed transactions (i.e. aborted or committed),
  //   we can skip replaying all participant ops for that transaction.
  // - If we otherwise have persisted that a transaction exists, it was
  //   persisted on-disk as being in-flight before restarting, we should have
  //   opened a transaction before starting to replay, and we should replay all
  //   participant ops for that transaction.
  // - If we have no record of a transaction persisted, it was not persisted
  //   on-disk as being in-flight, and we must replay all participant ops.
  ParticipantOpState op_state(tablet_replica_.get(),
                              tablet_->txn_participant(),
                              &replicate_msg->participant_request());
  const auto& op_type = op_state.request()->op().type();
  const auto& txn_id = op_state.txn_id();
  if (ContainsKey(terminal_txn_ids_, op_state.txn_id())) {
    // Even if we're skipping over this participant op because its metadata
    // state has been persisted, we still need to commit its rowsets if active,
    // since we may have had to rebuild a committed MRS.
    if (op_type == ParticipantOpPB::FINALIZE_COMMIT &&
        ContainsKey(mrs_txn_ids_, txn_id)) {
      tablet_->CommitTxnRowSets(txn_id);
    }
    return AppendCommitMsg(commit_msg);
  }
  bool persisted_in_flight = ContainsKey(in_flight_txn_ids_, op_state.txn_id());
  if ((persisted_in_flight && op_type != ParticipantOpPB::BEGIN_TXN) ||
      !persisted_in_flight) {
    // If we're about to create a new MRS, add this transaction ID to the set
    // that have active MRSs.
    if (op_type == ParticipantOpPB::BEGIN_TXN) {
      EmplaceOrDie(&mrs_txn_ids_, txn_id);
    }
    op_state.mutable_op_id()->CopyFrom(replicate_msg->id());
    op_state.set_timestamp(Timestamp(replicate_msg->timestamp()));
    op_state.AcquireTxnAndLock();
    SCOPED_CLEANUP({
      op_state.ReleaseTxn();
    });
    // Start an MVCC op to track the commit if we're beginning to commit.
    tablet_->StartOp(&op_state);

    // Start applying the commit's MVCC op if we're finalizing the commit.
    // PerformOp() will take care of finishing applying the op.
    tablet_->StartApplying(&op_state);

    // NOTE: don't bother validating the current state of the op. Presumably that
    // happened the first time this op was written.
    RETURN_NOT_OK(op_state.PerformOp(replicate_msg->id(), tablet_.get()));
  }
  return AppendCommitMsg(commit_msg);
}

Status TabletBootstrap::PlayNoOpRequest(const IOContext* /*io_context*/,
                                        ReplicateMsg* /*replicate_msg*/,
                                        const CommitMsg& commit_msg) {
  return AppendCommitMsg(commit_msg);
}

Status TabletBootstrap::PlayRowOperations(const IOContext* io_context,
                                          WriteOpState* op_state,
                                          const TxResultPB& orig_result,
                                          TxResultPB* new_result) {
  Schema inserts_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(op_state->request()->schema(), &inserts_schema),
                        "Couldn't decode client schema");

  RETURN_NOT_OK_PREPEND(tablet_->DecodeWriteOperations(&inserts_schema, op_state),
                        Substitute("Could not decode row operations: $0",
                                   SecureDebugString(op_state->request()->row_operations())));

  // If the write is a part of a transaction that's currently open (i.e., it
  // has an in-flight Txn associated with it), lock the Txn now and make sure
  // it's open.
  // NOTE: we shouldn't take this lock if we've persisted that the transaction
  // has completed, since there is no corresponding in-flight Txn.
  if (op_state->txn_id() && !ContainsKey(terminal_txn_ids_, *op_state->txn_id())) {
    RETURN_NOT_OK_PREPEND(tablet_->AcquireTxnLock(*op_state->txn_id(), op_state),
                          "Failed to acquire txn lock");;
  }

  // Acquire partition/row locks, Apply, etc!
  RETURN_NOT_OK_PREPEND(tablet_->AcquirePartitionLock(op_state, LockManager::WAIT_FOR_LOCK),
                        "Failed to acquire partition lock");
  RETURN_NOT_OK_PREPEND(tablet_->AcquireRowLocks(op_state),
                        "Failed to acquire row locks");

  RETURN_NOT_OK(ApplyOperations(io_context, op_state, orig_result, new_result));

  return Status::OK();
}

Status TabletBootstrap::ApplyOperations(const IOContext* io_context,
                                        WriteOpState* op_state,
                                        const TxResultPB& orig_result,
                                        TxResultPB* new_result) {
  DCHECK_EQ(op_state->row_ops().size(), orig_result.ops_size());
  DCHECK_EQ(op_state->row_ops().size(), new_result->ops_size());
  int32_t op_idx = 0;

  for (RowOp* op : op_state->row_ops()) {
    int32_t curr_op_idx = op_idx++;
    // Increment the seen/ignored stats.
    switch (op->decoded_op.type) {
      case RowOperationsPB::INSERT:
      case RowOperationsPB::INSERT_IGNORE:
      case RowOperationsPB::UPSERT:
      case RowOperationsPB::UPSERT_IGNORE: {
        // TODO(unknown): should we have a separate counter for upserts?
        stats_.inserts_seen++;
        if (op->has_result()) {
          stats_.inserts_ignored++;
        }
        break;
      }
      case RowOperationsPB::UPDATE:
      case RowOperationsPB::UPDATE_IGNORE:
      case RowOperationsPB::DELETE:
      case RowOperationsPB::DELETE_IGNORE: {
        stats_.mutations_seen++;
        if (op->has_result()) {
          stats_.mutations_ignored++;
        }
        break;
      }
      default:
        LOG_WITH_PREFIX(FATAL) << "Bad op type: " << op->decoded_op.type;
        break;
    }

    const OperationResultPB& new_op_result = new_result->ops(curr_op_idx);
    // If the op is already flushed or had previously failed, no need to replay it.
    // TODO(dralves) this back and forth is weird. We're first setting the flushed/failed
    // status on the rewritten message's commit entry. Then we pass it here to
    // set the status on the op, then we set it back on the commit entry with
    // ReleaseTxResultPB(). This could be simplified if we build the RowOps on
    // demand and just created DecodedRowOperation/RowOp for the replayed stuff.
    if (new_op_result.skip_on_replay()) {
      op->SetSkippedResult(new_op_result);
      continue;
    }

    op->set_original_result_from_log(&orig_result.ops(curr_op_idx));

    // Actually apply it.
    ProbeStats stats; // we don't use this, but tablet internals require non-NULL.
    RETURN_NOT_OK(tablet_->ApplyRowOperation(io_context, op_state, op, &stats));
    DCHECK(op->has_result());

    // We expect that the above Apply() will always succeed, because we're
    // applying an operation that we know succeeded before the server
    // restarted. If it doesn't succeed, something is wrong and we are
    // diverging from our prior state, so bail.
    if (op->result->has_failed_status()) {
      return Status::Corruption("Operation which previously succeeded failed "
                                "during log replay",
                                Substitute("Op: $0\nFailure: $1",
                                           op->ToString(*tablet_->schema()),
                                           SecureShortDebugString(op->result->failed_status())));
    }
  }
  return Status::OK();
}

Status TabletBootstrap::FilterOperation(const OperationResultPB& op_result,
                                        OpAction* action) {

  // If the operation failed or was skipped, originally, no need to re-apply it.
  if (op_result.has_failed_status()) {
    *action = SKIP_PREVIOUSLY_FAILED;
    return Status::OK();
  }

  if (op_result.skip_on_replay()) {
    *action = SKIP_PREVIOUSLY_FLUSHED;
    return Status::OK();
  }

  int num_mutated_stores = op_result.mutated_stores_size();
  if (PREDICT_FALSE(num_mutated_stores > 2)) {
    return Status::Corruption(Substitute("All operations must have at most two mutated_stores: $0",
                                         SecureShortDebugString(op_result)));
  }
  // NOTE: it's possible that num_mutated_stores = 0 in the case of an
  // UPSERT which only specified the primary key. In that case, if the
  // row already existed, it gets dropped without converting into an UPDATE.

  // The mutation may have been duplicated, so we'll check whether any of the
  // output targets was active.
  int num_active_stores = 0;
  for (const MemStoreTargetPB& mutated_store : op_result.mutated_stores()) {
    if (mutated_store.has_rs_txn_id()) {
      // TODO(awong): once we begin flushing before commit, we'll need to have
      // this account for a last_flushed_mrs_id per transaction.
      if (ContainsKey(mrs_txn_ids_, mutated_store.rs_txn_id())) {
        num_active_stores++;
      }
      continue;
    }
    if (flushed_stores_.IsMemStoreActive(mutated_store)) {
      num_active_stores++;
    }
  }

  if (num_active_stores == 0) {
    // The mutation was fully flushed.
    *action = SKIP_PREVIOUSLY_FLUSHED;
    return Status::OK();
  }

  if (PREDICT_FALSE(num_active_stores == 2)) {
    // It's not possible for a duplicated mutation to refer to two stores which are still
    // active. Either the mutation arrived before the metadata was flushed, in which case
    // the 'first' store is live, or it arrived just after it was flushed, in which case
    // the 'second' store was live. But at no time should the metadata refer to both the
    // 'input' and 'output' stores of a compaction.
    return Status::Corruption("Mutation was duplicated to two stores that are considered live",
                              SecureShortDebugString(op_result));
  }

  *action = NEEDS_REPLAY;
  return Status::OK();
}

Status TabletBootstrap::UpdateClock(uint64_t timestamp) {
  return clock_->Update(Timestamp(timestamp));
}

Status FlushedStoresSnapshot::InitFrom(const TabletMetadata& tablet_meta) {
  CHECK(flushed_dms_by_drs_id_.empty()) << "already initted";
  last_durable_mrs_id_ = tablet_meta.last_durable_mrs_id();
  for (const shared_ptr<RowSetMetadata>& rsmd : tablet_meta.rowsets()) {
    if (!InsertIfNotPresent(&flushed_dms_by_drs_id_, rsmd->id(),
                            rsmd->last_durable_redo_dms_id())) {
      return Status::Corruption(Substitute(
          "Duplicate DRS ID $0 in tablet metadata. "
          "Found DRS $0 with last durable redo DMS ID $1 while trying to "
          "initialize DRS $0 with last durable redo DMS ID $2",
          rsmd->id(),
          flushed_dms_by_drs_id_[rsmd->id()],
          rsmd->last_durable_redo_dms_id()));
    }
  }
  return Status::OK();
}

bool FlushedStoresSnapshot::IsMemStoreActive(const MemStoreTargetPB& target) const {
  if (target.has_mrs_id()) {
    DCHECK(!target.has_rs_id());
    DCHECK(!target.has_dms_id());

    // The original mutation went to the MRS. If this MRS has not yet been made
    // durable, it needs to be replayed.
    return target.mrs_id() > last_durable_mrs_id_;
  } else {

    // The original mutation went to a DRS's delta store.
    DCHECK(target.has_rs_id());

    int64_t last_durable_dms_id;
    if (!FindCopy(flushed_dms_by_drs_id_, target.rs_id(), &last_durable_dms_id)) {
      // If we have no data about this DRS, then there are two cases:
      //
      // 1) The DRS has already been flushed, but then later got removed because
      //    it got compacted away or culled because it was empty. In the former
      //    case, the deltas should have been reflected in the new compaction
      //    output, and in the latter, all the rows in the rowset have been
      //    deleted and there's nothing to replay.
      //
      // 2) The DRS was in the process of being written, but haven't yet
      //    flushed the TabletMetadata update that includes it. We only write
      //    to an in-progress DRS like this when we are in the 'duplicating'
      //    phase of a compaction. In that case, the other duplicated 'target'
      //    should still be present in the metadata, and we can base our
      //    decision based on that one.
      return false;
    }

    // If the original rowset that we applied the edit to exists, check whether
    // the edit was in a flushed DMS or a live one.
    return target.dms_id() > last_durable_dms_id;
  }
}

} // namespace tablet
} // namespace kudu
