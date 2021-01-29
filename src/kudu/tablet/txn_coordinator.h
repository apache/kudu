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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {

namespace transactions {
class TxnStatusEntryPB;
} // namespace transactions

namespace tserver {
class TabletServerErrorPB;
} // namespace tserver

namespace tablet {

class TabletReplica;

// Maps the transaction ID to the transaction's participants' tablet IDs. This
// is convenient to use in testing, given its relative ease of construction.
typedef std::map<int64_t, std::vector<std::string>> ParticipantIdsByTxnId;

// Manages ongoing transactions and participants thereof.
class TxnCoordinator {
 public:
  virtual ~TxnCoordinator() {}

  // Perform necessary work to prepare for running in the leader role.
  // It's about reload tablet metadata into memory and do other work
  // to update the internal state of the coordinator upon becoming
  // the leader.
  virtual void PrepareLeadershipTask() = 0;

  // Starts a transaction with the given ID as the given user.
  //
  // Returns any replication-layer errors (e.g. not-the-leader errors) in
  // 'ts_error'. If there was otherwise a logical error with the request (e.g.
  // transaction already exists), returns an error without populating
  // 'ts_error'. The 'highest_seen_txn_id' output parameter is populated with
  // the highest seen txn_id so far: that's so for success and all error cases.
  virtual Status BeginTransaction(int64_t txn_id,
                                  const std::string& user,
                                  int64_t* highest_seen_txn_id,
                                  tserver::TabletServerErrorPB* ts_error) = 0;

  // Begins committing the given transaction as the given user.
  //
  // Returns any replication-layer errors (e.g. not-the-leader errors) in
  // 'ts_error'. If there was otherwise a logical error with the request (e.g.
  // no such transaction), returns an error without populating 'ts_error'.
  virtual Status BeginCommitTransaction(int64_t txn_id, const std::string& user,
                                        tserver::TabletServerErrorPB* ts_error) = 0;

  // Finalizes the commit of the transaction.
  //
  // Returns any replication-layer errors (e.g. not-the-leader errors) in
  // 'ts_error'. If there was otherwise a logical error with the request (e.g.
  // no such transaction), returns an error without populating 'ts_error'.
  //
  // TODO(awong): add a commit timestamp.
  virtual Status FinalizeCommitTransaction(
      int64_t txn_id, tserver::TabletServerErrorPB* ts_error) = 0;

  // Aborts the given transaction as the given user.
  //
  // Returns any replication-layer errors (e.g. not-the-leader errors) in
  // 'ts_error'. If there was otherwise a logical error with the request (e.g.
  // no such transaction), returns an error without populating 'ts_error'.
  virtual Status AbortTransaction(int64_t txn_id, const std::string& user,
                                  tserver::TabletServerErrorPB* ts_error) = 0;

  // Retrieves the status entry for the specified transaction, returning
  // Status::OK() in case of success with 'txn_status' populated. In case of
  // error, returns non-OK status. 'ts_error' is used to return not-the-leader
  // error to let the caller know that the call reached a non-leader replica and
  // it's not up to its purpose, so the client needs to retry the call against
  // a leader replica. In the latter case, the method returns
  // Status::ServiceUnavailable().
  virtual Status GetTransactionStatus(
      int64_t txn_id,
      const std::string& user,
      transactions::TxnStatusEntryPB* txn_status,
      tserver::TabletServerErrorPB* ts_error) = 0;

  // Process keep-alive heartbeat for the specified transaction as the given
  // user. This is to keep the transaction "alive", so the transaction would
  // not be aborted automatically due to staleness.
  virtual Status KeepTransactionAlive(int64_t txn_id,
                                      const std::string& user,
                                      tserver::TabletServerErrorPB* ts_error) = 0;

  // Registers a participant tablet ID to the given transaction ID as the given
  // user.
  //
  // Returns any replication-layer errors (e.g. not-the-leader errors) in
  // 'ts_error'. If there was otherwise a logical error with the request (e.g.
  // no such transaction), returns an error without populating 'ts_error'.
  virtual Status RegisterParticipant(int64_t txn_id, const std::string& tablet_id,
                                     const std::string& user,
                                     tserver::TabletServerErrorPB* ts_error) = 0;

  // Abort transactions in non-terminal state which are considered 'stale'.
  // The 'staleness' of a transaction is determined by particular
  // implementation. This method can be called periodically to get rid of
  // adandoned transactions.
  virtual void AbortStaleTransactions() = 0;

  // The highest transaction ID seen by this coordinator.
  virtual int64_t highest_txn_id() const = 0;

  // Populates a map from transaction ID to the list of participants associated
  // with that transaction ID.
  virtual ParticipantIdsByTxnId GetParticipantsByTxnIdForTests() const = 0;
};

class TxnCoordinatorFactory {
 public:
  virtual std::unique_ptr<TxnCoordinator> Create(TabletReplica* replica) = 0;
};

} // namespace tablet
} // namespace kudu
