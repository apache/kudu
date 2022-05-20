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

#include "kudu/transactions/txn_status_manager.h"

#include <algorithm>
#include <ctime>
#include <functional>
#include <iterator>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/tablet/ops/op_tracker.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"

DEFINE_uint32(txn_keepalive_interval_ms, 30 * 1000, // 30 sec
              "Maximum interval (in milliseconds) between subsequent "
              "keep-alive heartbeats to let the transaction status manager "
              "know that a transaction is not abandoned. If the transaction "
              "status manager does not receive a keepalive message for a time "
              "interval longer than the specified, the transaction is deemed "
              "abandoned and automatically aborted. See the description of the "
              "--txn_staleness_tracker_interval_ms flag for more information "
              "on abandoned transactions tracking.");
TAG_FLAG(txn_keepalive_interval_ms, experimental);
TAG_FLAG(txn_keepalive_interval_ms, runtime);

DEFINE_int32(txn_status_manager_inject_latency_load_from_tablet_ms, 0,
             "Injects a random latency between 0 and this many milliseconds "
             "when loading data from the txn status tablet replica backing "
             "the instance of TxnStatusManager. This is a test-only flag, "
             "do not use in production.");
TAG_FLAG(txn_status_manager_inject_latency_load_from_tablet_ms, hidden);
TAG_FLAG(txn_status_manager_inject_latency_load_from_tablet_ms, unsafe);

DEFINE_int32(txn_status_manager_inject_latency_finalize_commit_ms, 0,
             "Injects a random latency between 0 and this many milliseconds "
             "before finalizing commits.");
TAG_FLAG(txn_status_manager_inject_latency_finalize_commit_ms, hidden);
TAG_FLAG(txn_status_manager_inject_latency_finalize_commit_ms, unsafe);

DEFINE_uint32(txn_staleness_tracker_interval_ms, 10 * 1000,  // 10 sec
              "Period (in milliseconds) of the task that tracks and aborts "
              "stale/abandoned transactions. If this flag is set to 0, "
              "TxnStatusManager doesn't automatically abort stale/abandoned "
              "transactions even if no keepalive messages are received for "
              "longer than defined by the --txn_keepalive_interval_ms flag.");
TAG_FLAG(txn_staleness_tracker_interval_ms, experimental);
TAG_FLAG(txn_staleness_tracker_interval_ms, runtime);

DEFINE_int32(txn_status_tablet_failover_catchup_timeout_ms, 30 * 1000, // 30 sec
             "Amount of time to give a newly-elected leader tserver of transaction "
             "status tablet to load the metadata containing all operations replicated "
             "by the previous leader and become active.");
TAG_FLAG(txn_status_tablet_failover_catchup_timeout_ms, advanced);
TAG_FLAG(txn_status_tablet_failover_catchup_timeout_ms, experimental);

DEFINE_bool(txn_status_tablet_failover_inject_timeout_error, false,
            "If true, inject timeout error when waiting the replica to catch up with "
            "all replicated operations in previous term.");
TAG_FLAG(txn_status_tablet_failover_inject_timeout_error, unsafe);

DEFINE_bool(txn_status_tablet_inject_load_failure_error, false,
            "If true, inject error when loading data from the transaction status "
            "tablet replica");
TAG_FLAG(txn_status_tablet_inject_load_failure_error, unsafe);

DEFINE_bool(txn_status_tablet_inject_uninitialized_leader_status_error, false,
            "If true, inject uninitialized leader status error");
TAG_FLAG(txn_status_tablet_inject_uninitialized_leader_status_error, unsafe);

DEFINE_uint32(txn_background_rpc_timeout_ms, 5000,
              "Period (in milliseconds) with which transaction-related background requests "
              "are made");
TAG_FLAG(txn_background_rpc_timeout_ms, experimental);
TAG_FLAG(txn_background_rpc_timeout_ms, runtime);

DEFINE_uint32(txn_client_initialization_timeout_ms, 10000,
              "Amount of time Kudu will try to initialize a client with "
              "which to perform transaction commit tasks.");
TAG_FLAG(txn_client_initialization_timeout_ms, runtime);

DEFINE_bool(txn_schedule_background_tasks, true,
            "Whether or not instances of the TxnStatusManager should schedule "
            "background tasks to operate on transactions (e.g. commit, abort)");
TAG_FLAG(txn_schedule_background_tasks, unsafe);

using kudu::consensus::ConsensusStatePB;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RaftPeerPB;

using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcContext;
using kudu::tablet::TabletReplica;
using kudu::tablet::ParticipantIdsByTxnId;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::TabletServerErrorPB;
using std::nullopt;
using std::optional;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace transactions {

namespace {

// The following special values are used to determine whether the data from
// the underlying transaction status tablet has already been loaded
// (an alternative would be introducing a dedicated member field into the
//  TxnStatusManager):
//   * kIdStatusDataNotLoaded: value assigned at construction time
//   * kIdStatusDataReady: value after loading data from the transaction status
//     tablet (unless the tablet contains a record with higher txn_id)
constexpr int64_t kIdStatusDataNotLoaded = -2;
constexpr int64_t kIdStatusDataReady = -1;

// Value to represent uninitialized 'leader_ready_term_' assigned at the
// transaction status manager construction time.
constexpr int64_t kUninitializedLeaderTerm = -1;

Status ReportIllegalTxnState(const string& errmsg,
                             TabletServerErrorPB* ts_error) {
  DCHECK(ts_error);
  auto s = Status::IllegalState(errmsg);
  TabletServerErrorPB error;
  StatusToPB(s, error.mutable_status());
  error.set_code(TabletServerErrorPB::TXN_ILLEGAL_STATE);
  *ts_error = std::move(error);
  return s;
}

} // anonymous namespace

bool CommitTasks::IsShuttingDownCleanupIfLastOp() {
  if (stop_task_ || txn_status_manager_->shutting_down()) {
    if (--ops_in_flight_ == 0) {
      txn_status_manager_->RemoveCommitTask(txn_id_.value(), this);
    }
    return true;
  }
  return false;
}

bool CommitTasks::IsShuttingDownCleanup() const {
  DCHECK_EQ(0, ops_in_flight_);
  if (stop_task_ || txn_status_manager_->shutting_down()) {
    txn_status_manager_->RemoveCommitTask(txn_id_.value(), this);
    return true;
  }
  return false;
}

bool CommitTasks::ScheduleAbortIfNecessary() {
  switch (abort_txn_) {
    case ABORT_IN_PROGRESS:
      LOG(INFO) << Substitute("Scheduling write for ABORT_IN_PROGRESS for txn $0",
                              txn_id_.ToString());
      ScheduleBeginAbortTxnWrite();
      return true;
    case ABORTED:
      LOG(INFO) << Substitute("Scheduling ABORT_TXNs on participants for txn $0",
                              txn_id_.ToString());
      AbortTxnAsync();
      return true;
    default:
      break;
  }
  return false;
}

void CommitTasks::BeginCommitAsyncTask(int participant_idx) {
  DCHECK_LT(participant_idx, participant_ids_.size());
  // Status callback called with the result from the participant op. This is
  // used to collect the participants' highest timestamps, with which we can
  // schedule the finalize commit task.
  //
  // The Status is the result returned from ParticipantRpc::AnalyzeResponse.
  scoped_refptr<CommitTasks> scoped_this(this);
  auto participated_cb = [this, scoped_this = std::move(scoped_this),
                          participant_idx] (const Status& s) {
    if (IsShuttingDownCleanupIfLastOp()) {
      return;
    }
    if (PREDICT_FALSE(s.IsTimedOut())) {
      // Retry timeout errors. Other transient errors should be retried by the
      // client until timeout.
      BeginCommitAsyncTask(participant_idx);
      return;
    }
    if (PREDICT_FALSE(!s.ok())) {
      // We might see errors if the participant was deleted, or because the
      // participant didn't successfully start the transaction. In any case,
      // abort the transaction.
      LOG(INFO) << Substitute("Participant $0 of txn $1 returned error for BEGIN_COMMIT op, "
                              "aborting: $2", participant_ids_[participant_idx],
                              txn_id_.ToString(), s.ToString());
      SetNeedsBeginAbort();
    }

    // If this was the last participant op for this task, we have some cleanup
    // to do.
    if (--ops_in_flight_ == 0) {
      if (IsShuttingDownCleanup()) {
        return;
      }
      if (ScheduleAbortIfNecessary()) {
        return;
      }
      Timestamp max_timestamp(Timestamp::kInitialTimestamp);
      for (const auto& ts : begin_commit_timestamps_) {
        max_timestamp = std::max(ts, max_timestamp);
      }
      DCHECK_NE(Timestamp::kInitialTimestamp, max_timestamp);
      set_commit_timestamp(max_timestamp);
      ScheduleFinalizeCommitWrite();
    }
  };
  ParticipantOpPB op_pb;
  op_pb.set_txn_id(txn_id_.value());
  op_pb.set_type(ParticipantOpPB::BEGIN_COMMIT);
  txn_client_->ParticipateInTransactionAsync(
      participant_ids_[participant_idx],
      std::move(op_pb),
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_txn_background_rpc_timeout_ms),
      std::move(participated_cb),
      &begin_commit_timestamps_[participant_idx]);
}

void CommitTasks::FinalizeCommitAsyncTask(int participant_idx) {
  DCHECK_EQ(TxnStatePB::UNKNOWN, abort_txn_);
  DCHECK_LT(participant_idx, participant_ids_.size());
  // Status callback called with the result from the participant op.
  scoped_refptr<CommitTasks> scoped_this(this);
  auto participated_cb = [this, scoped_this = std::move(scoped_this),
                          participant_idx] (const Status& s) {
    if (IsShuttingDownCleanupIfLastOp()) {
      return;
    }
    if (PREDICT_FALSE(s.IsTimedOut())) {
      LOG(WARNING) << Substitute("Retrying FINALIZE_COMMIT op for txn $0: $1",
                                 txn_id_.ToString(), s.ToString());
      FinalizeCommitAsyncTask(participant_idx);
      return;
    }
    if (PREDICT_FALSE(!s.ok())) {
      // Presumably the error is not transient (e.g. not found) so retrying
      // won't help. But we've already begun sending out FINALIZE_TXN ops, so
      // we must complete the transaction.
      LOG(WARNING) << Substitute("Participant $0 FINALIZE_COMMIT op returned $1",
                                 participant_ids_[participant_idx], s.ToString());
    }
    // If this was the last participant op for this task, write the finalized
    // commit timestamp to the tablet.
    if (--ops_in_flight_ == 0) {
      if (IsShuttingDownCleanup()) {
        return;
      }
      ScheduleCompleteCommitWrite();
    }
  };
  ParticipantOpPB op_pb;
  op_pb.set_txn_id(txn_id_.value());
  op_pb.set_type(ParticipantOpPB::FINALIZE_COMMIT);
  op_pb.set_finalized_commit_timestamp(commit_timestamp_.value());
  txn_client_->ParticipateInTransactionAsync(
      participant_ids_[participant_idx],
      std::move(op_pb),
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_txn_background_rpc_timeout_ms),
      std::move(participated_cb));
}

void CommitTasks::AbortTxnAsyncTask(int participant_idx) {
  // Status callback called with the result from the participant op.
  auto participated_cb = [this, participant_idx] (const Status& s) {
    if (IsShuttingDownCleanupIfLastOp()) {
      return;
    }
    if (PREDICT_FALSE(s.IsTimedOut())) {
      // Retry timeout errors. Other transient errors should be retried by the
      // client until timeout.
      AbortTxnAsyncTask(participant_idx);
      return;
    }
    if (PREDICT_FALSE(s.IsNotFound())) {
      // If the participant has been deleted, treat it as though it's already
      // been aborted. The participant's data can't be read anyway.
      LOG(INFO) << Substitute("Participant $0 was not found for ABORT_TXN, proceeding "
                              "as if op succeeded: $1",
                              participant_ids_[participant_idx], s.ToString());
    } else if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << Substitute("Participant $0 ABORT_TXN op returned $1",
                                 participant_ids_[participant_idx], s.ToString());
      stop_task_ = true;
    }
    // If this was the last participant op for this task, write the abort
    // record to the tablet.
    if (--ops_in_flight_ == 0) {
      if (IsShuttingDownCleanup()) {
        return;
      }
      ScheduleFinalizeAbortTxnWrite();
    }
  };
  ParticipantOpPB op_pb;
  op_pb.set_txn_id(txn_id_.value());
  op_pb.set_type(ParticipantOpPB::ABORT_TXN);
  txn_client_->ParticipateInTransactionAsync(
      participant_ids_[participant_idx],
      std::move(op_pb),
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_txn_background_rpc_timeout_ms),
      std::move(participated_cb));
}

void CommitTasks::AbortTxnAsync() {
  // Reset the in-flight counter to indicate we're waiting for this new set of
  // tasks to complete.
  if (participant_ids_.empty()) {
    ScheduleFinalizeAbortTxnWrite();
  } else {
    // NOTE: the final AbortTxnAsyncTask() call may destruct this CommitTask
    // and its members, so cache the participant ID size.
    const auto participant_ids_size = participant_ids_.size();
    ops_in_flight_ = participant_ids_size;
    for (int i = 0; i < participant_ids_size; i++) {
      AbortTxnAsyncTask(i);
    }
  }
}

void CommitTasks::ScheduleBeginAbortTxnWrite() {
  DCHECK_NE(TxnStatePB::UNKNOWN, abort_txn_);
  DCHECK_EQ(0, ops_in_flight_);
  // Submit the task to a threadpool.
  // NOTE: This is called by the reactor thread that catches the BeginCommit
  // response, so we can't do IO in this thread.
  scoped_refptr<CommitTasks> scoped_this(this);
  CHECK_OK(commit_pool_->Submit([this, scoped_this = std::move(scoped_this),
                                 tsm = txn_status_manager_,
                                 txn_id = txn_id_] {
    if (stop_task_ || tsm->shutting_down()) {
      tsm->RemoveCommitTask(txn_id, this);
      return;
    }
    TxnStatusManager::ScopedLeaderSharedLock l(txn_status_manager_);
    if (PREDICT_TRUE(l.first_failed_status().ok())) {
      if (abort_txn_ == ABORT_IN_PROGRESS) {
        TabletServerErrorPB ts_error;
        // Clear out these commit tasks so we can start new ones focused on
        // aborting.
        tsm->RemoveCommitTask(txn_id, this);
        WARN_NOT_OK(tsm->BeginAbortTransaction(txn_id.value(), nullopt, &ts_error),
                    "Error writing to transaction status table");
      } else {
        // It's possible that while we were waiting to be scheduled, a client
        // already called BeginAbortTransaction(). If so, we just need to go
        // about aborting the transaction.
        DCHECK_EQ(ABORTED, abort_txn_);
        AbortTxnAsync();
      }
    }
  }));
}

void CommitTasks::ScheduleFinalizeAbortTxnWrite() {
  // Submit the task to a threadpool.
  // NOTE: This is called by the reactor thread that catches the BeginCommit
  // response, so we can't do IO in this thread.
  DCHECK_EQ(0, ops_in_flight_);
  scoped_refptr<CommitTasks> scoped_this(this);
  CHECK_OK(commit_pool_->Submit([this, scoped_this = std::move(scoped_this),
                                 tsm = txn_status_manager_,
                                 txn_id = txn_id_] {
    if (IsShuttingDownCleanup()) {
      return;
    }
    TxnStatusManager::ScopedLeaderSharedLock l(txn_status_manager_);
    if (PREDICT_TRUE(l.first_failed_status().ok())) {
      WARN_NOT_OK(tsm->FinalizeAbortTransaction(txn_id.value()),
                  "Error writing to transaction status table");
    }

    // Regardless of whether we succeed or fail, remove the commit task.
    // Presumably we failed either because the replica is being shut down, or
    // because we're no longer leader. In either case, the task will be retried
    // once a new leader is elected.
    tsm->RemoveCommitTask(txn_id, this);
  }));
}

void CommitTasks::FinalizeCommitAsync() {
  DCHECK_NE(Timestamp::kInitialTimestamp, commit_timestamp_);
  // Reset the in-flight counter to indicate we're waiting for this new set of
  // tasks to complete.
  auto old_val = ops_in_flight_.exchange(participant_ids_.size());
  DCHECK_EQ(0, old_val);
  // NOTE: the final FinalizeCommitAsyncTask() call may destruct this
  // CommitTask and its members, so cache the participant ID size.
  const auto participant_ids_size = participant_ids_.size();
  ops_in_flight_ = participant_ids_size;
  for (int i = 0; i < participant_ids_size; i++) {
    FinalizeCommitAsyncTask(i);
  }
}

void CommitTasks::ScheduleCompleteCommitWrite() {
  // Submit the task to a threadpool.
  // NOTE: This is called by the reactor thread that catches the FinalizeCommit
  // response, so we can't do IO in this thread.
  DCHECK_EQ(TxnStatePB::UNKNOWN, abort_txn_);
  DCHECK_EQ(0, ops_in_flight_);
  scoped_refptr<CommitTasks> scoped_this(this);
  CHECK_OK(commit_pool_->Submit([this, scoped_this = std::move(scoped_this),
                                 tsm = this->txn_status_manager_,
                                 txn_id = this->txn_id_] {
    MAYBE_INJECT_RANDOM_LATENCY(
        FLAGS_txn_status_manager_inject_latency_finalize_commit_ms);

    if (stop_task_ || tsm->shutting_down()) {
      tsm->RemoveCommitTask(txn_id, this);
      return;
    }
    TxnStatusManager::ScopedLeaderSharedLock l(txn_status_manager_);
    if (PREDICT_TRUE(l.first_failed_status().ok())) {
      TabletServerErrorPB error_pb;
      WARN_NOT_OK(tsm->CompleteCommitTransaction(txn_id.value()),
                  "Error writing to transaction status table");
    }

    // Regardless of whether we succeed or fail, remove the commit task.
    // Presumably we failed either because the replica is being shut down, or
    // because we're no longer leader. In either case, the task will be retried
    // once a new leader is elected.
    tsm->RemoveCommitTask(txn_id, this);
  }));
}

void CommitTasks::ScheduleFinalizeCommitWrite() {
  // Submit the task to a threadpool.
  // NOTE: This is called by the reactor thread that catches the BeginCommit
  // response, so we can't do IO in this thread.
  DCHECK_EQ(0, ops_in_flight_);
  scoped_refptr<CommitTasks> scoped_this(this);
  CHECK_OK(commit_pool_->Submit([this, scoped_this = std::move(scoped_this),
                                 tsm = this->txn_status_manager_,
                                 txn_id = this->txn_id_] {
    MAYBE_INJECT_RANDOM_LATENCY(
        FLAGS_txn_status_manager_inject_latency_finalize_commit_ms);

    if (IsShuttingDownCleanup()) {
      return;
    }
    TxnStatusManager::ScopedLeaderSharedLock l(txn_status_manager_);
    // TODO(awong): the race handling here specific to concurrent aborts is
    // messy. Consider a less ad-hoc way to define on-going tasks.
    // NOTE: the special handling of aborts is only critical in this scheduling
    // to finalize the commit because the only non-OPEN state an abort can
    // occur with is COMMIT_IN_PROGRESS, which is the expected state of the
    // transaction when this is called.
    if (PREDICT_TRUE(l.first_failed_status().ok())) {
      // It's possible that a user called BeginAbortTransaction while we were
      // waiting, so before attempting to commit, with the leader lock held,
      // check if that's the case.
      if (ScheduleAbortIfNecessary()) {
        return;
      }
      TabletServerErrorPB error_pb;
      Status s = tsm->FinalizeCommitTransaction(txn_id.value(), commit_timestamp_, &error_pb);
      if (PREDICT_TRUE(s.ok())) {
        return;
      }
      // It's again possible the FinalizeCommitTransaction call raced with
      // BeginAbortTransaction, resulting in an aborted transaction. If so,
      // schedule an abort.
      if (ScheduleAbortIfNecessary()) {
        return;
      }
      LOG(WARNING) << Substitute("Error writing to transaction status table: $0",
                                 s.ToString());
    }
    tsm->RemoveCommitTask(txn_id.value(), this);
  }));
}

TxnStatusManagerBuildingVisitor::TxnStatusManagerBuildingVisitor()
    : highest_txn_id_(kIdStatusDataReady) {
}

void TxnStatusManagerBuildingVisitor::VisitTransactionEntries(
    int64_t txn_id, TxnStatusEntryPB status_entry_pb,
    vector<ParticipantIdAndPB> participants) {
  scoped_refptr<TransactionEntry> txn = new TransactionEntry(txn_id, status_entry_pb.user());
  {
    TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
    txn_lock.mutable_data()->pb = std::move(status_entry_pb);
    txn_lock.Commit();
  }
  {
    // Lock the transaction while we build the participants.
    TransactionEntryLock txn_lock(txn.get(), LockMode::READ);
    for (auto& [prt_id, prt_entry_pb] : participants) {
      // Register a participant entry for this transaction.
      auto prt = txn->GetOrCreateParticipant(prt_id);
      ParticipantEntryLock l(prt.get(), LockMode::WRITE);
      l.mutable_data()->pb = std::move(prt_entry_pb);
      l.Commit();
    }
  }

  // NOTE: this method isn't meant to be thread-safe, hence the lack of
  // locking.
  EmplaceOrDie(&txns_by_id_, txn_id, std::move(txn));
  highest_txn_id_ = std::max(highest_txn_id_, txn_id);
}

void TxnStatusManagerBuildingVisitor::Release(
    int64_t* highest_txn_id, TransactionsMap* txns_by_id) {
  *highest_txn_id = highest_txn_id_;
  *txns_by_id = std::move(txns_by_id_);
}

////////////////////////////////////////////////////////////
// TxnStatusManager::ScopedLeaderSharedLock
////////////////////////////////////////////////////////////
TxnStatusManager::ScopedLeaderSharedLock::ScopedLeaderSharedLock(
    TxnCoordinator* txn_coordinator)
    : txn_status_manager_(DCHECK_NOTNULL(down_cast<TxnStatusManager*>(txn_coordinator))),
      leader_shared_lock_(txn_status_manager_->leader_lock_, std::try_to_lock),
      replica_status_(Status::Uninitialized(
          "Transaction status tablet replica is not initialized")),
      leader_status_(Status::Uninitialized(
          "Leader status is not initialized")),
      initial_term_(kUninitializedLeaderTerm) {

  int64_t leader_ready_term;
  {
    std::lock_guard<simple_spinlock> l(txn_status_manager_->leader_term_lock_);
    replica_status_ = txn_status_manager_->status_tablet_.tablet_replica_->CheckRunning();
    if (PREDICT_FALSE(!replica_status_.ok() ||
                      FLAGS_txn_status_tablet_inject_uninitialized_leader_status_error)) {
      return;
    }
    leader_ready_term = txn_status_manager_->leader_ready_term_;
  }

  ConsensusStatePB cstate;
  Status s =
      txn_status_manager_->status_tablet_.tablet_replica_->consensus()->ConsensusState(&cstate);
  if (PREDICT_FALSE(!s.ok())) {
    DCHECK(s.IsIllegalState()) << s.ToString();
    replica_status_ = s.CloneAndPrepend("ConsensusState is not available");
    return;
  }
  DCHECK(replica_status_.ok());

  // Check if the transaction status manager is the leader.
  initial_term_ = cstate.current_term();
  const string& uuid = txn_status_manager_->status_tablet_.tablet_replica_->permanent_uuid();
  if (PREDICT_FALSE(cstate.leader_uuid() != uuid)) {
    leader_status_ = Status::IllegalState(
        Substitute("Not the leader. Local UUID: $0, Raft Consensus state: $1",
                   uuid, SecureShortDebugString(cstate)));
    return;
  }
  if (PREDICT_FALSE(leader_ready_term != initial_term_ ||
                    !leader_shared_lock_.owns_lock())) {
    leader_status_ = Status::ServiceUnavailable(
        "Leader not yet ready to serve requests or the leadership has changed");
    return;
  }
  leader_status_ = Status::OK();
}

template<typename RespClass>
bool TxnStatusManager::ScopedLeaderSharedLock::CheckIsInitializedAndIsLeaderOrRespond(
    RespClass* resp, RpcContext* rpc) {
  const Status& s = first_failed_status();
  if (PREDICT_TRUE(s.ok())) {
    return true;
  }

  StatusToPB(s, resp->mutable_error()->mutable_status());
  if (!leader_status_.IsUninitialized()) {
    resp->mutable_error()->set_code(TabletServerErrorPB::NOT_THE_LEADER);
  } else {
    resp->mutable_error()->set_code(TabletServerErrorPB::TABLET_NOT_RUNNING);
  }
  rpc->RespondSuccess();
  return false;
}

// Explicit specialization for callers outside this compilation unit.
#define INITTED_AND_LEADER_OR_RESPOND(RespClass) \
  template bool \
  TxnStatusManager::ScopedLeaderSharedLock::CheckIsInitializedAndIsLeaderOrRespond( \
      RespClass* resp, RpcContext* rpc) /* NOLINT */

INITTED_AND_LEADER_OR_RESPOND(tserver::CoordinateTransactionResponsePB);
#undef INITTED_AND_LEADER_OR_RESPOND

Status TxnStatusManager::LoadFromTabletUnlocked() {
  leader_lock_.AssertAcquiredForWriting();

  if (PREDICT_FALSE(FLAGS_txn_status_tablet_inject_load_failure_error)) {
    return Status::IllegalState("Injected transaction status tablet reload error");
  }

  TxnStatusManagerBuildingVisitor v;
  RETURN_NOT_OK(status_tablet_.VisitTransactions(&v));
  int64_t highest_txn_id;
  TransactionsMap txns_by_id;
  v.Release(&highest_txn_id, &txns_by_id);

  MAYBE_INJECT_RANDOM_LATENCY(
      FLAGS_txn_status_manager_inject_latency_load_from_tablet_ms);

  // TODO(awong): if we can't connect to the masters, consider retrying later.
  // For now, just load the table without starting any background tasks.
  TxnSystemClient* txn_client = nullptr;
  if (PREDICT_TRUE(client_initializer_)) {
    WARN_NOT_OK(client_initializer_->WaitForClient(
        MonoDelta::FromMilliseconds(FLAGS_txn_client_initialization_timeout_ms), &txn_client),
                "Unable to initialize TxnSystemClient");
  }

  unordered_map<int64_t, scoped_refptr<CommitTasks>> new_commits;
  unordered_map<int64_t, scoped_refptr<CommitTasks>> new_finalizes;
  unordered_map<int64_t, scoped_refptr<CommitTasks>> new_aborts;
  if (txn_client) {
    for (const auto& [txn_id, txn_entry] : txns_by_id) {
      const auto& state = txn_entry->state();
      switch (state) {
        case TxnStatePB::COMMIT_IN_PROGRESS:
          new_commits.emplace(txn_id,
              new CommitTasks(txn_id, txn_entry->GetParticipantIds(),
                              txn_client, commit_pool_, this));
          break;
        case TxnStatePB::FINALIZE_IN_PROGRESS: {
          scoped_refptr<CommitTasks> tasks(
              new CommitTasks(txn_id, txn_entry->GetParticipantIds(),
                              txn_client, commit_pool_, this));
          tasks->set_commit_timestamp(Timestamp(txn_entry->commit_timestamp()));
          new_finalizes.emplace(txn_id, std::move(tasks));
          break;
        }
        case TxnStatePB::ABORT_IN_PROGRESS:
          new_aborts.emplace(txn_id,
              new CommitTasks(txn_id, txn_entry->GetParticipantIds(),
                              txn_client, commit_pool_, this));
          break;
        default:
          break;
      }
    }
  }
  unordered_map<int64_t, scoped_refptr<CommitTasks>> commits_in_flight;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    highest_txn_id_ = std::max(highest_txn_id, highest_txn_id_);
    txns_by_id_ = std::move(txns_by_id);
    commits_in_flight = std::move(commits_in_flight_);
    // Stop any previously on-going tasks.
    for (const auto& [_, tasks] : commits_in_flight) {
      tasks->stop();
    }
    if (!new_commits.empty()) {
      LOG(INFO) << Substitute("Starting $0 commit tasks", new_commits.size());
      for (const auto& [_, tasks] : new_commits) {
        tasks->BeginCommitAsync();
      }
    }
    if (!new_finalizes.empty()) {
      LOG(INFO) << Substitute("Starting $0 finalize tasks", new_finalizes.size());
      for (const auto& [_, tasks] : new_finalizes) {
        tasks->FinalizeCommitAsync();
      }
    }
    if (!new_aborts.empty()) {
      LOG(INFO) << Substitute("Starting $0 aborts task", new_aborts.size());
      for (const auto& [_, tasks] : new_aborts) {
        tasks->AbortTxnAsync();
      }
    }
    commits_in_flight_ = std::move(new_commits);
    commits_in_flight_.insert(std::make_move_iterator(new_aborts.begin()),
                              std::make_move_iterator(new_aborts.end()));
  }
  return Status::OK();
}

TxnStatusManager::TxnStatusManager(tablet::TabletReplica* tablet_replica,
                                   TxnSystemClientInitializer* txn_client_initializer,
                                   ThreadPool* commit_pool)
    : client_initializer_(txn_client_initializer),
      commit_pool_(commit_pool),
      shutting_down_(false),
      highest_txn_id_(kIdStatusDataNotLoaded),
      status_tablet_(tablet_replica),
      leader_ready_term_(kUninitializedLeaderTerm),
      leader_lock_(RWMutex::Priority::PREFER_WRITING) {
}

void TxnStatusManager::Shutdown() {
  shutting_down_ = true;
  // Wait for all tasks to complete.
  while (true) {
    int num_tasks;
    {
      std::lock_guard<simple_spinlock> l(lock_);
      num_tasks = commits_in_flight_.size();
      if (num_tasks == 0) {
        return;
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(50));
    KLOG_EVERY_N_SECS(INFO, 10) << Substitute("Waiting for $0 task(s) to stop", num_tasks);
  }
}

TxnStatusManager::~TxnStatusManager() {
  Shutdown();
}

Status TxnStatusManager::LoadFromTablet() {
  // Block new transaction status manager operations, and wait
  // for existing operations to finish.
  std::lock_guard<RWMutex> leader_lock_guard(leader_lock_);
  return LoadFromTabletUnlocked();
}

Status TxnStatusManager::CheckTxnStatusDataLoadedUnlocked(
    TabletServerErrorPB* ts_error) const {
  DCHECK(ts_error);
  DCHECK(lock_.is_locked());
  // TODO(aserbin): this is just to handle requests which come in a short time
  //                interval when the leader replica of the transaction status
  //                tablet is already in RUNNING state, but the records from
  //                the tablet hasn't yet been loaded into the runtime
  //                structures of this TxnStatusManager instance. However,
  //                the case when a former leader replica is queried about the
  //                status of transactions which it is no longer aware of should
  //                be handled separately.
  if (PREDICT_FALSE(highest_txn_id_ <= kIdStatusDataNotLoaded)) {
    // The records from the tablet is not yet be loaded only if the
    // leadership status has not been initialized.
    CHECK(leader_ready_term_ == kUninitializedLeaderTerm);
    return Status::ServiceUnavailable("transaction status data is not loaded");
  }
  auto* consensus = status_tablet_.tablet_replica_->consensus();
  DCHECK(consensus);
  if (consensus->role() != RaftPeerPB::LEADER) {
    static const Status kErrStatus = Status::ServiceUnavailable(
        "txn status tablet replica is not a leader");
    TabletServerErrorPB error;
    StatusToPB(kErrStatus, error.mutable_status());
    error.set_code(TabletServerErrorPB::NOT_THE_LEADER);
    *ts_error = std::move(error);
    return kErrStatus;
  }
  return Status::OK();
}

Status TxnStatusManager::WaitUntilCaughtUpAsLeader(const MonoDelta& timeout) {
  // Verify the current node is indeed the leader.
  ConsensusStatePB cstate;
  RETURN_NOT_OK(status_tablet_.tablet_replica_->consensus()->ConsensusState(&cstate));
  const string& uuid = status_tablet_.tablet_replica_->permanent_uuid();
  if (cstate.leader_uuid() != uuid) {
    return Status::IllegalState(
        Substitute("Node $0 not leader. Raft Consensus state: $1",
                   uuid, SecureShortDebugString(cstate)));
  }

  // Wait for all ops to be committed.
  return status_tablet_.tablet_replica_->op_tracker()->WaitForAllToFinish(timeout);
}

Status TxnStatusManager::GetTransaction(int64_t txn_id,
                                        const optional<string>& user,
                                        scoped_refptr<TransactionEntry>* txn,
                                        TabletServerErrorPB* ts_error) const {
  leader_lock_.AssertAcquiredForReading();
  std::lock_guard<simple_spinlock> l(lock_);

  // First, make sure the transaction status data has been loaded. If not, then
  // the caller might get an unexpected error response and bail instead of
  // retrying a bit later and getting proper response.
  RETURN_NOT_OK(CheckTxnStatusDataLoadedUnlocked(ts_error));

  scoped_refptr<TransactionEntry> ret = FindPtrOrNull(txns_by_id_, txn_id);
  if (PREDICT_FALSE(!ret)) {
    return Status::InvalidArgument(
        Substitute("transaction ID $0 not found, current highest txn ID: $1",
                  txn_id, highest_txn_id_));
  }
  if (PREDICT_FALSE(user && ret->user() != *user)) {
    return Status::NotAuthorized(
        Substitute("transaction ID $0 not owned by $1", txn_id, *user));
  }
  *txn = std::move(ret);
  return Status::OK();
}

void TxnStatusManager::PrepareLeadershipTask() {
  // Return early if the tablet is already not running.
  if (PREDICT_FALSE(status_tablet_.tablet_replica_->IsShuttingDown())) {
    LOG(WARNING) << "Not reloading transaction status tablet metadata, because "
                 << "the tablet is already shutting down or shutdown. ";
    return;
  }
  const RaftConsensus* consensus = status_tablet_.tablet_replica_->consensus();
  const int64_t term_before_wait = consensus->CurrentTerm();
  {
    std::lock_guard<simple_spinlock> l(leader_term_lock_);
    if (leader_ready_term_ == term_before_wait) {
      // The term hasn't changed since the last time this replica was the
      // leader. It's not possible for another replica to be leader for the same
      // term, so there hasn't been any actual leadership change and thus
      // there's no reason to reload the on-disk metadata.
      VLOG(2) << Substitute("Term $0 hasn't changed, ignoring dirty callback",
                            term_before_wait);
      return;
    }
  }
  LOG(INFO) << "Waiting until node catch up with all replicated operations in previous term...";
  Status s = WaitUntilCaughtUpAsLeader(
      MonoDelta::FromMilliseconds(FLAGS_txn_status_tablet_failover_catchup_timeout_ms));
  if (PREDICT_FALSE(!s.ok() || FLAGS_txn_status_tablet_failover_inject_timeout_error)) {
    WARN_NOT_OK(s, "Failed waiting for node to catch up after leader election");
    // Even when we get a time out error here, it is ok to return. Since the client
    // will get a ServiceUnavailable error and retry.
    return;
  }

  const int64_t term = consensus->CurrentTerm();
  if (term_before_wait != term) {
    // If we got elected leader again while waiting to catch up then we will
    // get another callback to reload the metadata, so bail.
    LOG(INFO) << Substitute("Term changed from $0 to $1 while waiting for replica "
                            "leader catchup. Not loading transaction status manager "
                            "metadata", term_before_wait, term);
    return;
  }

  {
    // This lambda returns the result of calling the 'func'. If the returned status
    // is non-OK, the caller should bail on the leadership preparation task. Non-OK
    // status is not considered fatal, because errors on preparing transaction status
    // table only affect transactional operations and clients can retry in such case.
    const auto check = [this](
        const std::function<Status()> func,
        const RaftConsensus& consensus,
        int64_t start_term,
        const char* op_description) {

      leader_lock_.AssertAcquiredForWriting();
      const Status s = func();
      if (s.ok()) {
        // Not an error at all.
        return s;
      }

      const int64_t term = consensus.CurrentTerm();
      // If the term has changed we assume the new leader is about to do the
      // necessary work in its leadership preparation task. Otherwise, log
      // a warning.
      if (term != start_term) {
        LOG(INFO) << Substitute("$0 interrupted; change in term detected: $1 vs $2: $3",
                                op_description, start_term, term, s.ToString());
      } else {
        LOG(WARNING) << Substitute("$0 failed: $1", op_description, s.ToString());
      }
      return s;
    };

    // Block new operations, and wait for existing operations to finish.
    std::lock_guard<RWMutex> leader_lock_guard(leader_lock_);

    static const char* const kLoadMetaOpDescription =
        "Loading transaction status metadata into memory";
    LOG(INFO) << kLoadMetaOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, kLoadMetaOpDescription) {
      if (!check([this]() { return this->LoadFromTabletUnlocked(); },
                 *consensus, term, kLoadMetaOpDescription).ok()) {
        return;
      }
    }
  }

  std::lock_guard<simple_spinlock> l(leader_term_lock_);
  leader_ready_term_ = term;
}

// NOTE: In this method, the idea is to try setting the 'highest_seen_txn_id'
//       on return in most cases. Sending back the most recent highest
//       transaction identifier helps to avoid extra RPC calls from
//       TxnManager to TxnStatusManager in case of contention. Since we use
//       a trial-and-error approach to assign transaction identifiers,
//       in case of higher contention outdated and not assigned
//       highest_seen_txn_id would cause at least one extra round-trip between
//       TxnManager and TxnStatusManager to come up with a valid identifier
//       for a transaction.
Status TxnStatusManager::BeginTransaction(int64_t txn_id,
                                          const string& user,
                                          int64_t* highest_seen_txn_id,
                                          TabletServerErrorPB* ts_error) {
  leader_lock_.AssertAcquiredForReading();
  {
    std::lock_guard<simple_spinlock> l(lock_);

    // First, make sure the transaction status data has been loaded.
    // If not, then there is chance that, being a leader, this replica might
    // register a transaction with the identifier which is lower than the
    // identifiers of already registered transactions.
    //
    // If this check fails, don not set the 'highest_seen_txn_id' because
    // 'highest_txn_id_' doesn't contain any meaningful value yet.
    RETURN_NOT_OK(CheckTxnStatusDataLoadedUnlocked(ts_error));

    // Second, make sure the requested ID is viable.
    if (PREDICT_FALSE(txn_id <= highest_txn_id_)) {
      if (highest_seen_txn_id) {
        *highest_seen_txn_id = highest_txn_id_;
      }
      return Status::InvalidArgument(
          Substitute("transaction ID $0 is not higher than the highest ID so far: $1",
                     txn_id, highest_txn_id_));
    }
    highest_txn_id_ = txn_id;
  }

  // NOTE: it's fine if these underlying tablet ops race with one another --
  // since we've serialized the transaction ID checking above, we're guaranteed
  // that at most one call to start a given transaction ID can succeed.

  // This ScopedCleanup instance is to set 'highest_seen_txn_id' if writing
  // the entry into the txn status tablet fails.
  auto cleanup = MakeScopedCleanup([&]() {
    if (highest_seen_txn_id) {
      std::lock_guard<simple_spinlock> l(lock_);
      *highest_seen_txn_id = highest_txn_id_;
    }
  });
  // Write an entry to the status tablet for this transaction.
  const auto start_timestamp = time(nullptr);
  RETURN_NOT_OK(status_tablet_.AddNewTransaction(txn_id, user, start_timestamp, ts_error));

  // Now that we've successfully persisted the new transaction ID, initialize
  // the in-memory state and make it visible to clients.
  scoped_refptr<TransactionEntry> txn = new TransactionEntry(txn_id, user);
  {
    TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
    txn_lock.mutable_data()->pb.set_state(TxnStatePB::OPEN);
    txn_lock.mutable_data()->pb.set_user(user);
    txn_lock.mutable_data()->pb.set_start_timestamp(start_timestamp);
    txn_lock.mutable_data()->pb.set_last_transition_timestamp(start_timestamp);
    txn_lock.Commit();
  }
  std::lock_guard<simple_spinlock> l(lock_);
  EmplaceOrDie(&txns_by_id_, txn_id, std::move(txn));
  if (highest_seen_txn_id) {
    *highest_seen_txn_id = highest_txn_id_;
  }
  // Avoid acquiring the lock again: 'highest_seen_txn_id' has already been set.
  cleanup.cancel();

  return Status::OK();
}

void CommitTasks::BeginCommitAsync() {
  if (participant_ids_.empty()) {
    // If there are no participants for this transaction; just change its state
    // to committed.
    ScheduleCompleteCommitWrite();
  } else {
    // If there are some participants, schedule beginning commit tasks so
    // we can determine a finalized commit timestamp.
    //
    // NOTE: the final BeginCommitAsyncTask() call may destruct this CommitTask
    // and its members, so cache the participant ID size.
    //
    // TODO(awong): consider an approach in which clients propagate
    // timestamps in such a way that the client's call to begin commit
    // includes the expected finalized commit timestamp.
    const auto participant_ids_size = participant_ids_.size();
    for (int i = 0; i < participant_ids_size; i++) {
      BeginCommitAsyncTask(i);
    }
  }
}

Status TxnStatusManager::BeginCommitTransaction(int64_t txn_id, const string& user,
                                                TabletServerErrorPB* ts_error) {
  leader_lock_.AssertAcquiredForReading();
  TxnSystemClient* txn_client;
  if (PREDICT_TRUE(FLAGS_txn_schedule_background_tasks)) {
    RETURN_NOT_OK(client_initializer_->GetClient(&txn_client));
  }

  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::COMMIT_IN_PROGRESS ||
      state == TxnStatePB::FINALIZE_IN_PROGRESS ||
      state == TxnStatePB::COMMITTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::OPEN)) {
    return ReportIllegalTxnState(Substitute("transaction ID $0 is not open: $1",
                                            txn_id, SecureShortDebugString(pb)),
                                 ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_last_transition_timestamp(time(nullptr));
  mutable_data->pb.set_state(TxnStatePB::COMMIT_IN_PROGRESS);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, ts_error));

  if (PREDICT_TRUE(FLAGS_txn_schedule_background_tasks)) {
    auto participant_ids = txn->GetParticipantIds();
    std::unique_lock<simple_spinlock> l(lock_);
    auto [map_iter, emplaced] = commits_in_flight_.emplace(txn_id,
        new CommitTasks(txn_id, std::move(participant_ids),
                        txn_client, commit_pool_, this));
    l.unlock();
    if (emplaced) {
      map_iter->second->BeginCommitAsync();
    }
  }
  txn_lock.Commit();

  return Status::OK();
}

Status TxnStatusManager::FinalizeCommitTransaction(
    int64_t txn_id,
    Timestamp commit_timestamp,
    TabletServerErrorPB* ts_error) {
  leader_lock_.AssertAcquiredForReading();
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, nullopt, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::FINALIZE_IN_PROGRESS ||
      state == TxnStatePB::COMMITTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::COMMIT_IN_PROGRESS)) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is not committing: $1",
                   txn_id, SecureShortDebugString(pb)),
        ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_last_transition_timestamp(time(nullptr));
  mutable_data->pb.set_state(TxnStatePB::FINALIZE_IN_PROGRESS);
  mutable_data->pb.set_commit_timestamp(commit_timestamp.value());
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(
      txn_id, mutable_data->pb, ts_error));

  if (PREDICT_TRUE(FLAGS_txn_schedule_background_tasks)) {
    std::lock_guard<simple_spinlock> l(lock_);
    auto& task = FindOrDie(commits_in_flight_, txn_id);
    task->FinalizeCommitAsync();
  }

  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::CompleteCommitTransaction(int64_t txn_id) {
  leader_lock_.AssertAcquiredForReading();
  scoped_refptr<TransactionEntry> txn;
  TabletServerErrorPB ts_error;
  RETURN_NOT_OK(GetTransaction(txn_id, nullopt, &txn, &ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  DCHECK(pb.has_state());
  const auto state = pb.state();
  if (state == TxnStatePB::COMMITTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::COMMIT_IN_PROGRESS &&
                    state != TxnStatePB::FINALIZE_IN_PROGRESS)) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is not finalizing its commit: $1",
                   txn_id, SecureShortDebugString(pb)),
        &ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_last_transition_timestamp(time(nullptr));
  mutable_data->pb.set_state(TxnStatePB::COMMITTED);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(
      txn_id, mutable_data->pb, &ts_error));

  {
    std::lock_guard<simple_spinlock> l(lock_);
    commits_in_flight_.erase(txn_id);
  }

  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::FinalizeAbortTransaction(int64_t txn_id) {
  leader_lock_.AssertAcquiredForReading();
  TabletServerErrorPB ts_error;
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, /*user*/nullopt, &txn, &ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::ABORTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::ABORT_IN_PROGRESS)) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 cannot be aborted: $1",
                   txn_id, SecureShortDebugString(pb)),
        &ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_last_transition_timestamp(time(nullptr));
  mutable_data->pb.set_state(TxnStatePB::ABORTED);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, &ts_error));
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::BeginAbortTransaction(int64_t txn_id,
                                               const optional<string>& user,
                                               TabletServerErrorPB* ts_error) {

  leader_lock_.AssertAcquiredForReading();
  TxnSystemClient* txn_client;
  if (PREDICT_TRUE(FLAGS_txn_schedule_background_tasks)) {
    RETURN_NOT_OK(client_initializer_->GetClient(&txn_client));
  }
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::ABORT_IN_PROGRESS) {
    if (PREDICT_TRUE(FLAGS_txn_schedule_background_tasks)) {
      // It's possible we don't have any tasks running even though we're in
      // ABORT_IN_PROGRESS, e.g. if we're in the middle of aborting a commit
      // (and have removed the commit tasks), while at the same time, we've
      // just served a client-initiated abort and so the state is already
      // ABORT_IN_PROGRESS. If so, we should start abort tasks.
      std::unique_lock<simple_spinlock> l(lock_);
      if (PREDICT_FALSE(!ContainsKey(commits_in_flight_, txn_id))) {
        auto participant_ids = txn->GetParticipantIds();
        auto tasks = EmplaceOrDie(&commits_in_flight_, txn_id,
          new CommitTasks(txn_id, std::move(participant_ids),
                          txn_client, commit_pool_, this));
        l.unlock();
        tasks->AbortTxnAsync();
      }
    }
    return Status::OK();
  }
  if (state == TxnStatePB::ABORTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::OPEN &&
                    state != TxnStatePB::COMMIT_IN_PROGRESS)) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 cannot be aborted: $1",
                   txn_id, SecureShortDebugString(pb)),
        ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_last_transition_timestamp(time(nullptr));
  mutable_data->pb.set_state(TxnStatePB::ABORT_IN_PROGRESS);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, ts_error));

  if (PREDICT_TRUE(FLAGS_txn_schedule_background_tasks)) {
    auto participant_ids = txn->GetParticipantIds();
    std::unique_lock<simple_spinlock> l(lock_);
    auto [map_iter, emplaced] = commits_in_flight_.emplace(txn_id,
        new CommitTasks(txn_id, std::move(participant_ids),
                        txn_client, commit_pool_, this));
    l.unlock();
    if (emplaced) {
      // If we didn't have commit tasks on-going, finalize the abort on
      // participants.
      map_iter->second->AbortTxnAsync();
    } else {
      // If we did have commit tasks, set them up so they finalize the abort.
      map_iter->second->SetNeedsFinalizeAbort();
    }
  }
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::AbortTransaction(int64_t txn_id,
                                          const string& user,
                                          TabletServerErrorPB* ts_error) {
  return BeginAbortTransaction(txn_id, user, ts_error);
}

Status TxnStatusManager::GetTransactionStatus(
    int64_t txn_id,
    const string& user,
    TxnStatusEntryPB* txn_status,
    TabletServerErrorPB* ts_error) {
  DCHECK(txn_status);
  leader_lock_.AssertAcquiredForReading();
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::READ);
  const auto& pb = txn_lock.data().pb;
  DCHECK(pb.has_user());
  txn_status->set_user(pb.user());
  DCHECK(pb.has_state());
  txn_status->set_state(pb.state());
  if (pb.has_commit_timestamp()) {
    txn_status->set_commit_timestamp(pb.commit_timestamp());
  }

  return Status::OK();
}

Status TxnStatusManager::KeepTransactionAlive(int64_t txn_id,
                                              const string& user,
                                              TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  // It's a read (not write) lock because the last heartbeat time isn't
  // persisted into the transaction status tablet. In other words, the last
  // heartbeat time is a purely run-time piece of information for a
  // TransactionEntry.
  TransactionEntryLock txn_lock(txn.get(), LockMode::READ);

  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  // Keepalive updates are not required for a transaction in COMMIT_IN_PROGRESS
  // state. The system takes care of a transaction once the client side
  // initiates the commit phase.
  if (state == TxnStatePB::COMMIT_IN_PROGRESS ||
      state == TxnStatePB::FINALIZE_IN_PROGRESS) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is in commit phase: $1",
                   txn_id, SecureShortDebugString(pb)),
        ts_error);
  }
  if (state != TxnStatePB::OPEN) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is not available for further commits: $1",
                   txn_id, SecureShortDebugString(pb)),
        ts_error);
  }
  DCHECK_EQ(TxnStatePB::OPEN, state);
  txn->SetLastHeartbeatTime(MonoTime::Now());

  return Status::OK();
}

Status TxnStatusManager::RegisterParticipant(
    int64_t txn_id,
    const string& tablet_id,
    const string& user,
    TabletServerErrorPB* ts_error) {
  leader_lock_.AssertAcquiredForReading();
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  // Lock the transaction in read mode and check that it's open. If the
  // transaction isn't open, e.g. because a commit is already in progress,
  // return an error.
  TransactionEntryLock txn_lock(txn.get(), LockMode::READ);
  const auto& txn_state = txn_lock.data().pb.state();
  if (PREDICT_FALSE(txn_state != TxnStatePB::OPEN)) {
    return ReportIllegalTxnState(Substitute("transaction ID $0 not open: $1",
                                            txn_id, TxnStatePB_Name(txn_state)),
                                 ts_error);
  }

  auto participant = txn->GetOrCreateParticipant(tablet_id);
  ParticipantEntryLock prt_lock(participant.get(), LockMode::WRITE);
  const auto& prt_state = prt_lock.data().pb.state();
  if (prt_state == TxnStatePB::OPEN) {
    // If an open participant already exists, there's nothing more to do.
    return Status::OK();
  }
  if (PREDICT_FALSE(prt_state != TxnStatePB::UNKNOWN)) {
    // If the participant is otherwise initialized, e.g. aborted, committing,
    // etc, adding the participant again should fail.
    return Status::IllegalState("participant entry already exists");
  }
  prt_lock.mutable_data()->pb.set_state(TxnStatePB::OPEN);

  // Write the new participant entry.
  RETURN_NOT_OK(status_tablet_.AddNewParticipant(txn_id, tablet_id, ts_error));

  // Now that we've persisted the new participant to disk, update the in-memory
  // state to denote the participant is open.
  prt_lock.Commit();
  return Status::OK();
}

void TxnStatusManager::AbortStaleTransactions() {
  leader_lock_.AssertAcquiredForReading();
  const MonoDelta max_staleness_interval =
      MonoDelta::FromMilliseconds(FLAGS_txn_keepalive_interval_ms);
  TransactionsMap txns_by_id;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    for (const auto& elem : txns_by_id_) {
      const auto state = elem.second->state();
      // The tracker is interested only in open transactions. It's not concerned
      // about transactions in terminal states (i.e. COMMITTED, ABORTED): there
      // is nothing can be done with those. As for transactions in
      // COMMIT_IN_PROGRESS state, the system should be take care of those
      // without any participation from the client side, so txn keepalive
      // messages are not required while the system tries to finalize those.
      if (state == TxnStatePB::OPEN) {
        txns_by_id.emplace(elem.first, elem.second);
      }
    }
  }
  const MonoTime now = MonoTime::Now();
  for (auto& elem : txns_by_id) {
    const auto& txn_id = elem.first;
    const auto& txn_entry = elem.second;
    const auto staleness_interval = now - txn_entry->last_heartbeat_time();
    if (staleness_interval > max_staleness_interval) {
      TabletServerErrorPB error;
      auto s = AbortTransaction(txn_id, txn_entry->user(), &error);
      if (PREDICT_TRUE(s.ok())) {
        LOG(INFO) << Substitute(
            "automatically aborted stale txn (ID $0) past $1 from "
            "last keepalive heartbeat (effective timeout is $2)",
            txn_id, staleness_interval.ToString(),
            max_staleness_interval.ToString());
      } else {
        LOG(WARNING) << Substitute(
            "failed to abort stale txn (ID $0) past $1 from "
            "last keepalive heartbeat (effective timeout is $2): $3",
            txn_id, staleness_interval.ToString(),
            max_staleness_interval.ToString(), s.ToString());
        auto* consensus = DCHECK_NOTNULL(status_tablet_.tablet_replica_->consensus());
        if (consensus->role() != RaftPeerPB::LEADER ||
            !status_tablet_.tablet_replica()->CheckRunning().ok()) {
          // If the replica is no longer a leader at this point, there is
          // no sense in processing the rest of the entries.
          LOG(INFO) << "skipping staleness check for the rest of in-flight "
                       "txn records since this txn status tablet replica "
                       "is no longer a leader or not running";
          return;
        }
      }
    }
  }
}

ParticipantIdsByTxnId TxnStatusManager::GetParticipantsByTxnIdForTests() const {
  ParticipantIdsByTxnId ret;
  std::lock_guard<simple_spinlock> l(lock_);
  for (const auto& [id, txn] : txns_by_id_) {
    vector<string> prt_ids = txn->GetParticipantIds();
    std::sort(prt_ids.begin(), prt_ids.end());
    EmplaceOrDie(&ret, id, std::move(prt_ids));
  }
  return ret;
}

} // namespace transactions
} // namespace kudu
