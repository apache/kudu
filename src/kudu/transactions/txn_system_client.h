// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {
class HostPort;
class Timestamp;
class ThreadPool;

namespace client {
class KuduClient;
class KuduTable;
} // namespace client

namespace itest {
class TxnStatusTableITest;
class TxnStatusTableITest_TestProtectCreateAndAlter_Test;
class TxnStatusTableITest_CheckOpenTxnStatusTable_Test;
} // namespace itest

namespace rpc {
class Messenger;
} // namespace rpc

namespace tablet {
class TxnMetadataPB;
} // namespace tablet

namespace tserver {
class CoordinatorOpPB;
class CoordinatorOpResultPB;
class ParticipantOpPB;
} // namespace tserver

namespace transactions {

class TxnStatusEntryPB;

// Wrapper around a KuduClient used by Kudu for making transaction-related
// calls to various servers.
class TxnSystemClient {
 public:
  static Status Create(const std::vector<HostPort>& master_addrs,
                       const std::string& sasl_protocol_name,
                       std::unique_ptr<TxnSystemClient>* sys_client);

  // Creates the transaction status table with a single range partition of the
  // given upper bound.
  Status CreateTxnStatusTable(int64_t initial_upper_bound, int num_replicas = 1) {
    return CreateTxnStatusTableWithClient(initial_upper_bound, num_replicas, client_.get());
  }

  // Adds a new range to the transaction status table with the given bounds.
  //
  // TODO(awong): when we implement cleaning up of fully quiesced (i.e. fully
  // committed or fully aborted) transaction ID ranges, add an API to drop
  // entire ranges.
  Status AddTxnStatusTableRange(int64_t lower_bound, int64_t upper_bound) {
    return AddTxnStatusTableRangeWithClient(lower_bound, upper_bound, client_.get());
  }

  // TODO(awong): in the methods below with 'timeout' parameter,
  //              pass a deadline instead of a timeout so we can more easily
  //              associate it with potential user-specified deadlines.

  // Attempts to create a transaction with the given 'txn_id'.
  // Returns an error if the transaction ID has already been taken, or if there
  // was an error writing to the transaction status table. In success case
  // or in case of conflicting txn_id, the 'highest_seen_txn_id' output
  // parameter (if not null) is set to the highest transaction identifier
  // observed by corresponding TxnStatusManager. Otherwise, the
  // 'highest_seen_txn_id' parameter is unset (e.g., in case of the requeset
  // to TxnStatusManager timed out). The 'keep_alive_ms' output parameter is
  // populated with number of milliseconds for the transaction's keep-alive
  // interval in case of success, otherwise it is not set.
  Status BeginTransaction(int64_t txn_id, const
                          std::string& user,
                          uint32_t* txn_keepalive_ms = nullptr,
                          int64_t* highest_seen_txn_id = nullptr,
                          MonoDelta timeout = MonoDelta());

  // Attempts to register the given participant with the given transaction.
  // Returns an error if the transaction hasn't yet been started, or if the
  // 'user' isn't permitted to modify the transaction.
  Status RegisterParticipant(int64_t txn_id, const std::string& participant_id,
                             const std::string& user,
                             MonoDelta timeout = MonoDelta());

  // Initiates committing a transaction with the given identifier.
  Status BeginCommitTransaction(int64_t txn_id,
                                const std::string& user,
                                MonoDelta timeout = MonoDelta());

  // Aborts a transaction with the given identifier.
  Status AbortTransaction(int64_t txn_id,
                          const std::string& user,
                          MonoDelta timeout = MonoDelta());

  // Retrieves transactions status. On success, returns Status::OK() and stores
  // the result status in the 'txn_status' output parameter. On failure,
  // returns corresponding Status.
  Status GetTransactionStatus(int64_t txn_id,
                              const std::string& user,
                              TxnStatusEntryPB* txn_status,
                              MonoDelta timeout = MonoDelta());

  // Send keep-alive heartbeat for the specified transaction as the given user.
  Status KeepTransactionAlive(int64_t txn_id,
                              const std::string& user,
                              MonoDelta timeout = MonoDelta());

  // Opens the transaction status table, refreshing metadata with that from the
  // masters.
  Status OpenTxnStatusTable();

  // Check if the transaction status table is already open, returning
  // Status::OK() if so. Otherwise, open the transaction status table. In the
  // latter case, the result status of opening the table is returned.
  Status CheckOpenTxnStatusTable();

  // Sends an RPC to the leader of the given tablet to participate in a
  // transaction.
  //
  // If this is a BEGIN_COMMIT op, 'begin_commit_timestamp' is populated on success
  // with the timestamp used to replicate the op on the participant.
  Status ParticipateInTransaction(const std::string& tablet_id,
                                  const tserver::ParticipantOpPB& participant_op,
                                  const MonoDelta& timeout,
                                  Timestamp* begin_commit_timestamp = nullptr,
                                  tablet::TxnMetadataPB* metadata_pb = nullptr);
  void ParticipateInTransactionAsync(const std::string& tablet_id,
                                     tserver::ParticipantOpPB participant_op,
                                     const MonoDelta& timeout,
                                     StatusCallback cb,
                                     Timestamp* begin_commit_timestamp = nullptr,
                                     tablet::TxnMetadataPB* metadata_pb = nullptr);
 private:

  friend class itest::TxnStatusTableITest;
  FRIEND_TEST(itest::TxnStatusTableITest, TestProtectCreateAndAlter);
  FRIEND_TEST(itest::TxnStatusTableITest, CheckOpenTxnStatusTable);

  explicit TxnSystemClient(client::sp::shared_ptr<client::KuduClient> client)
      : client_(std::move(client)) {}

  static Status CreateTxnStatusTableWithClient(int64_t initial_upper_bound, int num_replicas,
                                               client::KuduClient* client);
  static Status AddTxnStatusTableRangeWithClient(int64_t lower_bound, int64_t upper_bound,
                                                 client::KuduClient* client);

  Status CoordinateTransactionAsync(tserver::CoordinatorOpPB coordinate_txn_op,
                                    const MonoDelta& timeout,
                                    const StatusCallback& cb,
                                    tserver::CoordinatorOpResultPB* result = nullptr);

  client::sp::shared_ptr<client::KuduTable> txn_status_table() {
    std::lock_guard<simple_spinlock> l(table_lock_);
    return txn_status_table_;
  }

  client::sp::shared_ptr<client::KuduClient> client_;

  simple_spinlock table_lock_;
  client::sp::shared_ptr<client::KuduTable> txn_status_table_;
};

// Wrapper around a TxnSystemClient that allows callers to asynchronously
// create a client.
// TODO(awong): the problem at hand is similar to TxnManager initialization,
// minus table creation. Refactor for code reuse?
class TxnSystemClientInitializer {
 public:
  TxnSystemClientInitializer();
  ~TxnSystemClientInitializer();

  // Starts attempts to initialize the transaction system client.
  Status Init(const std::shared_ptr<rpc::Messenger>& messenger,
              std::vector<HostPort> master_addrs);

  // Returns a ServiceUnavailable error if the client has not yet been
  // initialized or if Shutdown() has been called. Otherwise, returns OK and
  // sets 'client'. Callers should ensure that 'client' is only used while the
  // TxnSystemClientInitializer is still in scope.
  Status GetClient(TxnSystemClient** client) const;

  // Like the above, but retries periodically for the client to be initialized
  // for up to 'timeout', returning a TimedOut error if unable to. Returns a
  // ServiceUnavailable error if the initializer has been being shut down.
  Status WaitForClient(const MonoDelta& timeout, TxnSystemClient** client) const;

  // Stops the initialization, preventing success of further calls to
  // GetClient().
  void Shutdown();

 private:
  // Whether or not 'txn_client_' has been initialized.
  std::atomic<bool> init_complete_;

  // Whether or not the client initializer is shutting down, in which case
  // attempts to access 'txn_client_' should fail.
  std::atomic<bool> shutting_down_;

  // Threadpool on which to schedule attempts to initialize 'txn_client_'.
  std::unique_ptr<ThreadPool> txn_client_init_pool_;

  // The TxnSystemClient, initialized asynchronously via calls to Init().
  std::unique_ptr<TxnSystemClient> txn_client_;
};

} // namespace transactions
} // namespace kudu
