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

#include <cstdint>
#include <list>
#include <memory>
#include <string>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/txn_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

struct KeepaliveRpcCtx;

// This class hides the implementation details of the publicly exposed
// kudu::client::KuduTransaction::SerializationOptions class.
class KuduTransaction::SerializationOptions::Data {
 public:
  Data()
     : enable_keepalive_(false) {
  }
  ~Data() = default;

  bool enable_keepalive_;
};

// This class implements the functionality of kudu::client::KuduTransaction.
class KuduTransaction::Data {
 public:
  explicit Data(const sp::shared_ptr<KuduClient>& client);

  // A utility method returning the period for sending txn keepalive messages
  // (i.e. issuing KeepTransactionAlive() RPCs) for this multi-row transaction.
  MonoDelta GetKeepaliveRpcPeriod() const;

  // A utility method returning the timeout for KeepTransactionAlive() RPCs.
  MonoDelta GetKeepaliveRpcTimeout() const;

  Status CreateSession(sp::shared_ptr<KuduSession>* session);

  Status Begin(const sp::shared_ptr<KuduTransaction>& txn);

  // Transaction commit mode.
  enum class CommitMode {
    // Only start/initiate the commit phase, don't wait for the completion.
    START_ONLY,

    // Start the commit phase and wait until it succeeds or fails.
    WAIT_FOR_COMPLETION,
  };

  Status Commit(CommitMode mode);
  Status IsCommitComplete(bool* is_complete, Status* completion_status);

  Status Rollback();

  Status Serialize(std::string* serialized_txn,
                   const SerializationOptions& options) const;
  static Status Deserialize(const sp::shared_ptr<KuduClient>& client,
                            const std::string& serialized_txn,
                            sp::shared_ptr<KuduTransaction>* txn);

  static Status IsCommitCompleteImpl(
      KuduClient* client,
      const MonoTime& deadline,
      const TxnId& txn_id,
      bool* is_complete,
      Status* completion_status);

  static Status WaitForTxnCommitToFinalize(
      KuduClient* client, const MonoTime& deadline, const TxnId& txn_id);

  // The self-rescheduling task to send KeepTransactionAlive() RPC periodically
  // for a transaction. The task re-schedules itself as needed.
  // The 'status' parameter is to report on the status of scheduling the task
  // on reactor.
  static void SendTxnKeepAliveTask(const Status& status,
                                   sp::weak_ptr<KuduTransaction> weak_txn);

  // This helper member function is to analyze the status of the
  // KeepTransactionAlive() RPC and re-schedule the SendTxnKeepAliveTask(),
  // if necessary. The 'status' parameter contains the status of
  // KeepTransactionAlive() RPC called by SendTxnKeepAliveTask(). As soon as
  // TxnManager returns Status::IllegalState() or Status::Aborted(), the task
  // stops rescheduling itself, so transactions in terminal states are no longer
  // receiving keepalive heartbeats from the client.
  static void TxnKeepAliveCb(const Status& status,
                             sp::shared_ptr<KeepaliveRpcCtx> ctx);

  sp::weak_ptr<KuduClient> weak_client_;
  uint32_t txn_keep_alive_ms_;
  TxnId txn_id_;

  // Sessions created in the context of this transaction. This per-transaction
  // information is retained to flush all sessions created in the context
  // of this transaction upon calling Commit().
  std::list<sp::shared_ptr<KuduSession>> txn_sessions_;

  // Whether the commit of the transaction identified by 'txn_id_' is about
  // to start or has already started. Guarded by 'commit_started_lock_'.
  bool commit_started_;

  // Protects against concurrent access to 'commit_started_' and
  // 'txn_sessions_' above.
  simple_spinlock commit_started_lock_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu
