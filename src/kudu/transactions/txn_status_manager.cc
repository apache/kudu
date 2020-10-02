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
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::ParticipantIdsByTxnId;
using kudu::tserver::TabletServerErrorPB;
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

  std::lock_guard<simple_spinlock> l(lock_);
  highest_txn_id_ = std::max(highest_txn_id, highest_txn_id_);
  txns_by_id_ = std::move(txns_by_id);
  return Status::OK();
}

Status TxnStatusManager::CheckTxnStatusDataLoadedUnlocked() const {
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
  return Status::OK();
}

Status TxnStatusManager::GetTransaction(int64_t txn_id,
                                        const boost::optional<string>& user,
                                        scoped_refptr<TransactionEntry>* txn) const {
  std::lock_guard<simple_spinlock> l(lock_);

  // First, make sure the transaction status data has been loaded. If not, then
  // the caller might get an unexpected error response and bail instead of
  // retrying a bit later and getting proper response.
  RETURN_NOT_OK(CheckTxnStatusDataLoadedUnlocked());

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

Status TxnStatusManager::BeginTransaction(int64_t txn_id, const string& user,
                                          TabletServerErrorPB* ts_error) {
  {
    std::lock_guard<simple_spinlock> l(lock_);
    // First, make sure the transaction status data has been loaded.
    // If not, then there is chance that, being a leader, this replica might
    // register a transaction with the identifier which is lower than the
    // identifiers of already registered transactions.
    RETURN_NOT_OK(CheckTxnStatusDataLoadedUnlocked());

    // Second, make sure the requested ID is viable.
    if (PREDICT_FALSE(txn_id <= highest_txn_id_)) {
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
  return Status::OK();
}

Status TxnStatusManager::BeginCommitTransaction(int64_t txn_id, const string& user,
                                                TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::COMMIT_IN_PROGRESS) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::OPEN)) {
    return Status::IllegalState(
        Substitute("transaction ID $0 is not open: $1",
                   txn_id, SecureShortDebugString(pb)));
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_state(TxnStatePB::COMMIT_IN_PROGRESS);
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, ts_error));
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::FinalizeCommitTransaction(int64_t txn_id) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, boost::none, &txn));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::COMMITTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::COMMIT_IN_PROGRESS)) {
    return Status::IllegalState(
        Substitute("transaction ID $0 is not committing: $1",
                   txn_id, SecureShortDebugString(pb)));
  }
  auto* mutable_data = txn_lock.mutable_data();
  mutable_data->pb.set_state(TxnStatePB::COMMITTED);
  TabletServerErrorPB ts_error;
  RETURN_NOT_OK(status_tablet_.UpdateTransaction(txn_id, mutable_data->pb, &ts_error));
  txn_lock.Commit();
  return Status::OK();
}

Status TxnStatusManager::AbortTransaction(int64_t txn_id, const std::string& user,
                                          TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn));

  TransactionEntryLock txn_lock(txn.get(), LockMode::WRITE);
  const auto& pb = txn_lock.data().pb;
  const auto& state = pb.state();
  if (state == TxnStatePB::ABORTED) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state != TxnStatePB::OPEN &&
      state != TxnStatePB::COMMIT_IN_PROGRESS)) {
    return Status::IllegalState(
        Substitute("transaction ID $0 cannot be aborted: $1",
                   txn_id, SecureShortDebugString(pb)));
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
    transactions::TxnStatusEntryPB* txn_status) {
  DCHECK(txn_status);
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn));

  TransactionEntryLock txn_lock(txn.get(), LockMode::READ);
  const auto& pb = txn_lock.data().pb;
  DCHECK(pb.has_user());
  txn_status->set_user(pb.user());
  DCHECK(pb.has_state());
  txn_status->set_state(pb.state());

  return Status::OK();
}

Status TxnStatusManager::RegisterParticipant(int64_t txn_id, const string& tablet_id,
                                             const string& user, TabletServerErrorPB* ts_error) {
  scoped_refptr<TransactionEntry> txn;
  RETURN_NOT_OK(GetTransaction(txn_id, user, &txn));

  // Lock the transaction in read mode and check that it's open. If the
  // transaction isn't open, e.g. because a commit is already in progress,
  // return an error.
  TransactionEntryLock txn_lock(txn.get(), LockMode::READ);
  const auto& txn_state = txn_lock.data().pb.state();
  if (PREDICT_FALSE(txn_state != TxnStatePB::OPEN)) {
    return Status::IllegalState(
        Substitute("transaction ID $0 not open: $1",
                   txn_id, TxnStatePB_Name(txn_state)));
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
