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
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

DEFINE_uint32(txn_keepalive_interval_ms, 30000,
              "Maximum interval (in milliseconds) between subsequent "
              "keep-alive heartbeats to let the transaction status manager "
              "know that a transaction is not abandoned. If the transaction "
              "status manager does not receive a keepalive message for a "
              "longer interval than the specified, the transaction is "
              "automatically aborted.");
TAG_FLAG(txn_keepalive_interval_ms, experimental);
TAG_FLAG(txn_keepalive_interval_ms, runtime);

DEFINE_int32(txn_status_manager_inject_latency_load_from_tablet_ms, 0,
             "Injects a random latency between 0 and this many milliseconds "
             "when loading data from the txn status tablet replica backing "
             "the instance of TxnStatusManager. This is a test-only flag, "
             "do not use in production.");
TAG_FLAG(txn_status_manager_inject_latency_load_from_tablet_ms, hidden);
TAG_FLAG(txn_status_manager_inject_latency_load_from_tablet_ms, unsafe);

DEFINE_uint32(txn_staleness_tracker_interval_ms, 10000,
              "Period (in milliseconds) of the task that tracks and aborts "
              "stale/abandoned transactions. If this flag is set to 0, "
              "TxnStatusManager doesn't automatically abort stale/abandoned "
              "transactions even if no keepalive messages are received for "
              "longer than defined by the --txn_keepalive_interval_ms flag.");
TAG_FLAG(txn_staleness_tracker_interval_ms, experimental);
TAG_FLAG(txn_staleness_tracker_interval_ms, runtime);

using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::ParticipantIdsByTxnId;
using kudu::tserver::TabletServerErrorPB;
using kudu::consensus::RaftPeerPB;
using std::string;
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
    for (auto& participant_and_state : participants) {
      const auto& prt_id = participant_and_state.first;
      auto& prt_entry_pb = participant_and_state.second;

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

TxnStatusManager::TxnStatusManager(tablet::TabletReplica* tablet_replica)
    : highest_txn_id_(kIdStatusDataNotLoaded),
      status_tablet_(tablet_replica) {
}

Status TxnStatusManager::LoadFromTablet() {
  TxnStatusManagerBuildingVisitor v;
  RETURN_NOT_OK(status_tablet_.VisitTransactions(&v));
  int64_t highest_txn_id;
  TransactionsMap txns_by_id;
  v.Release(&highest_txn_id, &txns_by_id);

  MAYBE_INJECT_RANDOM_LATENCY(
      FLAGS_txn_status_manager_inject_latency_load_from_tablet_ms);

  std::lock_guard<simple_spinlock> l(lock_);
  highest_txn_id_ = std::max(highest_txn_id, highest_txn_id_);
  txns_by_id_ = std::move(txns_by_id);

  return Status::OK();
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

Status TxnStatusManager::GetTransaction(int64_t txn_id,
                                        const boost::optional<string>& user,
                                        scoped_refptr<TransactionEntry>* txn,
                                        TabletServerErrorPB* ts_error) const {
  std::lock_guard<simple_spinlock> l(lock_);

  // First, make sure the transaction status data has been loaded. If not, then
  // the caller might get an unexpected error response and bail instead of
  // retrying a bit later and getting proper response.
  RETURN_NOT_OK(CheckTxnStatusDataLoadedUnlocked(ts_error));

  scoped_refptr<TransactionEntry> ret = FindPtrOrNull(txns_by_id_, txn_id);
  if (PREDICT_FALSE(!ret)) {
    return Status::NotFound(
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
    // TODO(awong): reduce the "damage" from followers getting requests by
    // checking for leadership before doing anything. As is, if this replica
    // isn't the leader, we may aggressively burn through transaction IDs.
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
  RETURN_NOT_OK(status_tablet_.AddNewTransaction(txn_id, user, ts_error));

  // Now that we've successfully persisted the new transaction ID, initialize
  // the in-memory state and make it visible to clients.
  scoped_refptr<TransactionEntry> txn = new TransactionEntry(txn_id, user);
  {
    TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
    txn_lock.mutable_data()->pb.set_state(TxnStatePB::OPEN);
    txn_lock.mutable_data()->pb.set_user(user);
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

Status TxnStatusManager::BeginCommitTransaction(int64_t txn_id, const string& user,
                                                TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::COMMIT_IN_PROGRESS) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::OPEN)) {
    return ReportIllegalTxnState(Substitute("transaction ID $0 is not open: $1",
                                            txn_id, SecureShortDebugString(pb)),
                                 ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_state(TxnStatePB::COMMIT_IN_PROGRESS);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, ts_error));
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::FinalizeCommitTransaction(
    int64_t txn_id,
    TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, boost::none, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::COMMITTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::COMMIT_IN_PROGRESS)) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is not committing: $1",
                   txn_id, SecureShortDebugString(pb)),
        ts_error);
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_state(TxnStatePB::COMMITTED);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(
      txn_id, mutable_data->pb, ts_error));
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::AbortTransaction(int64_t txn_id,
                                          const std::string& user,
                                          TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
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
  mutable_data->pb.set_state(TxnStatePB::ABORTED);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, ts_error));
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::GetTransactionStatus(
    int64_t txn_id,
    const std::string& user,
    transactions::TxnStatusEntryPB* txn_status,
    TabletServerErrorPB* ts_error) {
  DCHECK(txn_status);
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn, ts_error));

  TransactionEntryLock txn_lock(txn.get(), LockMode::READ);
  const auto& pb = txn_lock.data().pb;
  DCHECK(pb.has_user());
  txn_status->set_user(pb.user());
  DCHECK(pb.has_state());
  txn_status->set_state(pb.state());

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
  if (state != TxnStatePB::OPEN &&
      state != TxnStatePB::COMMIT_IN_PROGRESS) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is already in terminal state: $1",
                   txn_id, SecureShortDebugString(pb)),
        ts_error);
  }
  // Keepalive updates are not required for a transaction in COMMIT_IN_PROGRESS
  // state. The system takes care of a transaction once the client side
  // initiates the commit phase.
  if (state == TxnStatePB::COMMIT_IN_PROGRESS) {
    return ReportIllegalTxnState(
        Substitute("transaction ID $0 is in commit phase: $1",
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
  const MonoDelta max_staleness_interval =
      MonoDelta::FromMilliseconds(FLAGS_txn_keepalive_interval_ms);

  auto* consensus = status_tablet_.tablet_replica_->consensus();
  DCHECK(consensus);
  if (consensus->role() != RaftPeerPB::LEADER) {
    // Only leader replicas abort stale transactions registered with them.
    // As of now, keep-alive requests are sent only to leader replicas, so only
    // they have up-to-date information about the liveliness of corresponding
    // transactions.
    //
    // If a non-leader replica errorneously (due to a network partition and
    // the absence of leader leases) tried to abort a transaction, it would fail
    // because aborting a transaction means writing into the transaction status
    // tablet, so a non-leader replica's write attempt would be rejected by
    // the Raft consensus protocol.
    return;
  }
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
  for (const auto& id_and_txn : txns_by_id_) {
    const auto& txn = id_and_txn.second;
    vector<string> prt_ids = txn->GetParticipantIds();
    std::sort(prt_ids.begin(), prt_ids.end());
    EmplaceOrDie(&ret, id_and_txn.first, std::move(prt_ids));
  }
  return ret;
}

} // namespace transactions
} // namespace kudu
