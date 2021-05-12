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

#include "kudu/client/transaction-internal.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/txn_manager_proxy_rpc.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/master/txn_manager.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using kudu::client::internal::AsyncRandomTxnManagerRpc;
using kudu::rpc::BackoffType;
using kudu::transactions::AbortTransactionRequestPB;
using kudu::transactions::AbortTransactionResponsePB;
using kudu::transactions::BeginTransactionRequestPB;
using kudu::transactions::BeginTransactionResponsePB;
using kudu::transactions::CommitTransactionRequestPB;
using kudu::transactions::CommitTransactionResponsePB;
using kudu::transactions::GetTransactionStateRequestPB;
using kudu::transactions::GetTransactionStateResponsePB;
using kudu::transactions::KeepTransactionAliveRequestPB;
using kudu::transactions::KeepTransactionAliveResponsePB;
using kudu::transactions::TxnManagerServiceProxy;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnTokenPB;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace client {

namespace {
MonoTime GetRpcDeadline(const KuduClient* c) {
  return MonoTime::Now() + c->default_admin_operation_timeout();
}
} // anonymous namespace

KuduTransaction::Data::Data(const sp::shared_ptr<KuduClient>& client)
    : weak_client_(client),
      txn_keep_alive_ms_(0),
      commit_started_(false) {
  CHECK(client);
}

MonoDelta KuduTransaction::Data::GetKeepaliveRpcPeriod() const {
  // Ideally, it would be enough to send a heartbeat message every
  // txn_keep_alive_ms_ interval, but given scheduling irregularities,
  // client node timer's precision, and various network delays and latencies,
  // it's safer to schedule sending keepalive messages from the client side
  // more frequently.
  return MonoDelta::FromMilliseconds(
      std::max<uint32_t>(1, txn_keep_alive_ms_ / 2));
}

MonoDelta KuduTransaction::Data::GetKeepaliveRpcTimeout() const {
  // If something goes wrong and keepalive RPC fails, it should be possible
  // to retry sending keepalive message a couple of times before the transaction
  // is automatically aborted by the backend after not receiving keepalive
  // messages for longer than the keepalive timeout for the transaction.
  return MonoDelta::FromMilliseconds(
      std::max<uint32_t>(1, txn_keep_alive_ms_ / 4));
}

Status KuduTransaction::Data::CreateSession(sp::shared_ptr<KuduSession>* session) {
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }
  // We could check for the transaction status here before trying to return
  // a session for a transaction that has been committed or abored already.
  // However, it would mean to incur an extra RPC to TxnManager which isn't
  // a good idea if thinking about this at scale. So, since tablet servers
  // should perform the same kind of verification while processing write
  // operations issued from the context of this session anyways,
  // there isn't much sense duplicating that at the client side.
  sp::shared_ptr<KuduSession> ret(new KuduSession(c, txn_id_));
  ret->data_->Init(ret);

  {
    std::lock_guard<simple_spinlock> l(commit_started_lock_);
    if (PREDICT_FALSE(commit_started_)) {
      return Status::IllegalState("transaction commit has already started");
    }
    // Store the information about the newly created session: this is needed
    // to automatically flush every associated session upon call of
    // KuduTransaction::Commit() for the corresponding transaction handle.
    txn_sessions_.emplace_back(ret);
  }

  *session = std::move(ret);
  return Status::OK();
}

Status KuduTransaction::Data::Begin(const sp::shared_ptr<KuduTransaction>& txn) {
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }

  BeginTransactionResponsePB resp;
  {
    Synchronizer sync;
    BeginTransactionRequestPB req;
    AsyncRandomTxnManagerRpc<BeginTransactionRequestPB,
                             BeginTransactionResponsePB> rpc(
        GetRpcDeadline(c.get()), c.get(), BackoffType::EXPONENTIAL,
        std::move(req), &resp, &TxnManagerServiceProxy::BeginTransactionAsync,
        "BeginTransaction", sync.AsStatusCallback());
    rpc.SendRpc();
    RETURN_NOT_OK(sync.Wait());
  }
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  DCHECK(resp.has_txn_id());
  txn_id_ = resp.txn_id();
  DCHECK(txn_id_.IsValid());
  DCHECK(resp.has_keepalive_millis());
  txn_keep_alive_ms_ = resp.keepalive_millis();
  DCHECK_GT(txn_keep_alive_ms_, 0);

  // Start sending regular heartbeats for the new transaction.
  auto m = c->data_->messenger_;
  if (PREDICT_FALSE(!m)) {
    return Status::IllegalState("null messenger in Kudu client");
  }

  sp::weak_ptr<KuduTransaction> weak_txn(txn);
  m->ScheduleOnReactor(
      [weak_txn](const Status& s) {
        SendTxnKeepAliveTask(s, weak_txn);
      },
      GetKeepaliveRpcPeriod());

  return Status::OK();
}

Status KuduTransaction::Data::Commit(CommitMode mode) {
  DCHECK(txn_id_.IsValid());
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }

  {
    std::lock_guard<simple_spinlock> l(commit_started_lock_);
    commit_started_ = true;
  }
  // In case of 'synchronous' commit mode (i.e. if waiting for the transaction
  // to finalize), flush all transactional sessions created out of this handle.
  // In case of 'asynchronous' commit mode, make sure no transactional session
  // contains pending operations.
  for (auto& session : txn_sessions_) {
    if (mode == CommitMode::WAIT_FOR_COMPLETION) {
      RETURN_NOT_OK(session->Flush());
    } else {
      if (session->HasPendingOperations()) {
        return Status::IllegalState(
            "cannot start committing transaction: at least one transactional "
            "session has write operations pending");
      }
    }
  }

  const auto deadline = GetRpcDeadline(c.get());
  CommitTransactionResponsePB resp;
  {
    Synchronizer sync;
    CommitTransactionRequestPB req;
    req.set_txn_id(txn_id_);
    AsyncRandomTxnManagerRpc<CommitTransactionRequestPB,
                             CommitTransactionResponsePB> rpc(
        deadline, c.get(), BackoffType::EXPONENTIAL,
        std::move(req), &resp, &TxnManagerServiceProxy::CommitTransactionAsync,
        "CommitTransaction", sync.AsStatusCallback());
    rpc.SendRpc();
    RETURN_NOT_OK(sync.Wait());
  }
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (mode == CommitMode::WAIT_FOR_COMPLETION) {
    RETURN_NOT_OK(WaitForTxnCommitToFinalize(c.get(), deadline, txn_id_));
  }
  return Status::OK();
}

Status KuduTransaction::Data::IsCommitComplete(
    bool* is_complete, Status* completion_status) {
  DCHECK(is_complete);
  DCHECK(completion_status);
  DCHECK(txn_id_.IsValid());
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }
  const auto deadline = GetRpcDeadline(c.get());
  return IsCommitCompleteImpl(
      c.get(), deadline, txn_id_, is_complete, completion_status);
}

Status KuduTransaction::Data::Rollback() {
  DCHECK(txn_id_.IsValid());
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }

  AbortTransactionResponsePB resp;
  {
    Synchronizer sync;
    AbortTransactionRequestPB req;
    req.set_txn_id(txn_id_);
    AsyncRandomTxnManagerRpc<AbortTransactionRequestPB,
                             AbortTransactionResponsePB> rpc(
        GetRpcDeadline(c.get()), c.get(), BackoffType::EXPONENTIAL,
        std::move(req), &resp, &TxnManagerServiceProxy::AbortTransactionAsync,
        "AbortTransaction", sync.AsStatusCallback());
    rpc.SendRpc();
    RETURN_NOT_OK(sync.Wait());
  }
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status KuduTransaction::Data::Serialize(
    string* serialized_txn,
    const SerializationOptions& options) const {
  DCHECK(serialized_txn);
  DCHECK(txn_id_.IsValid());
  TxnTokenPB token;
  token.set_txn_id(txn_id_);
  token.set_keepalive_millis(txn_keep_alive_ms_);
  token.set_enable_keepalive(options.keepalive());
  if (!token.SerializeToString(serialized_txn)) {
    return Status::Corruption("unable to serialize transaction information");
  }
  return Status::OK();
}

Status KuduTransaction::Data::Deserialize(
    const sp::shared_ptr<KuduClient>& client,
    const string& serialized_txn,
    sp::shared_ptr<KuduTransaction>* txn) {
  DCHECK(client);

  // TODO(aserbin): should the owner of the transaction be taken into account
  //                as well, i.e. not allow other than the user that created
  //                the transaction to deserialize its transaction token?
  TxnTokenPB token;
  if (!token.ParseFromString(serialized_txn)) {
    return Status::Corruption("unable to deserialize transaction information");
  }
  if (!token.has_txn_id()) {
    return Status::Corruption("transaction identifier is missing");
  }
  if (!token.has_keepalive_millis()) {
    return Status::Corruption("keepalive information is missing");
  }

  sp::shared_ptr<KuduTransaction> ret(new KuduTransaction(client));
  ret->data_->txn_keep_alive_ms_ = token.keepalive_millis();
  DCHECK_GT(ret->data_->txn_keep_alive_ms_, 0);
  ret->data_->txn_id_ = token.txn_id();
  DCHECK(ret->data_->txn_id_.IsValid());

  // Start sending periodic txn keepalive requests for the deserialized
  // transaction, as specified in the source txn token.
  if (token.has_enable_keepalive() && token.enable_keepalive()) {
    auto m = client->data_->messenger_;
    if (PREDICT_TRUE(m)) {
      sp::weak_ptr<KuduTransaction> weak_txn(ret);
      m->ScheduleOnReactor(
          [weak_txn](const Status& s) {
            SendTxnKeepAliveTask(s, weak_txn);
          },
          ret->data_->GetKeepaliveRpcPeriod());
    }
  }

  *txn = std::move(ret);
  return Status::OK();
}

Status KuduTransaction::Data::IsCommitCompleteImpl(
    KuduClient* client,
    const MonoTime& deadline,
    const TxnId& txn_id,
    bool* is_complete,
    Status* completion_status) {
  DCHECK(client);
  GetTransactionStateResponsePB resp;
  {
    Synchronizer sync;
    GetTransactionStateRequestPB req;
    req.set_txn_id(txn_id);
    AsyncRandomTxnManagerRpc<GetTransactionStateRequestPB,
                             GetTransactionStateResponsePB> rpc(
        deadline, client, BackoffType::EXPONENTIAL, std::move(req), &resp,
        &TxnManagerServiceProxy::GetTransactionStateAsync, "GetTransactionState",
        sync.AsStatusCallback());
    rpc.SendRpc();
    RETURN_NOT_OK(sync.Wait());
  }
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  DCHECK(resp.has_state());
  const auto state = resp.state();
  switch (state) {
    case TxnStatePB::OPEN:
      *is_complete = false;
      *completion_status = Status::IllegalState("transaction is still open");
      break;
    case TxnStatePB::ABORT_IN_PROGRESS:
      *is_complete = false;
      *completion_status = Status::Aborted("transaction is being aborted");
      break;
    case TxnStatePB::ABORTED:
      *is_complete = true;
      *completion_status = Status::Aborted("transaction has been aborted");
      break;
    case TxnStatePB::FINALIZE_IN_PROGRESS:
    case TxnStatePB::COMMIT_IN_PROGRESS:
      *is_complete = false;
      *completion_status = Status::Incomplete("commit is still in progress");
      break;
    case TxnStatePB::COMMITTED:
      *is_complete = true;
      *completion_status = Status::OK();
      break;
    default: {
        auto errmsg = Substitute("$0: unknown transaction state", state);
        LOG(DFATAL) << errmsg;
        return Status::IllegalState(errmsg);
      }
  }
  // Update last observed timestamp -- this is necessary to achieve consistency
  // in the READ_YOUR_WRITES mode when trying to read back the data of a
  // transaction that has just been committed. The commit phase might take some
  // time and may even be retried when the involved participants are not
  // available, so even the client has observed timestamps for all the write
  // operations sent in the context this transaction, the maximum timestamp
  // among all the transaction participants might be far ahead of the latest
  // timestamp observed by the client while performing the transactional write
  // operations.
  // NOTE: an empty transaction doesn't have the commit timestamp assigned.
  if (resp.has_commit_timestamp()) {
    DCHECK(state == TxnStatePB::COMMITTED ||
           state == TxnStatePB::FINALIZE_IN_PROGRESS);
    client->data_->UpdateLatestObservedTimestamp(resp.commit_timestamp());
  }
  return Status::OK();
}

Status KuduTransaction::Data::WaitForTxnCommitToFinalize(
    KuduClient* client, const MonoTime& deadline, const TxnId& txn_id) {
  return RetryFunc(
      deadline,
      "waiting for transaction commit to be completed",
      "timed out waiting for transaction commit to finalize",
      [&](const MonoTime& deadline, bool* retry) {
        bool is_complete = false;
        Status status;
        const auto s = KuduTransaction::Data::IsCommitCompleteImpl(
            client, deadline, txn_id, &is_complete, &status);
        if (!s.ok()) {
          *retry = false;
          return s;
        }
        *retry = !is_complete;
        return status;
      });
}

// A structure to pass around metadata on KeepTransactionAlive() when invoking
// the RPC asynchronously.
struct KeepaliveRpcCtx {
  KeepTransactionAliveResponsePB resp;
  unique_ptr<AsyncRandomTxnManagerRpc<KeepTransactionAliveRequestPB,
                                      KeepTransactionAliveResponsePB>> rpc;
  sp::weak_ptr<KuduTransaction> weak_txn;
  sp::shared_ptr<KuduClient> client;
};

void KuduTransaction::Data::SendTxnKeepAliveTask(
    const Status& status,
    sp::weak_ptr<KuduTransaction> weak_txn) {
  VLOG(2) << Substitute("SendTxnKeepAliveTask() is run");
  if (PREDICT_FALSE(!status.ok())) {
    // This means there was an error executing the task on reactor. As of now,
    // this can only happen if the reactor is being shutdown and the task is
    // de-scheduled from the queue, so the only possible error status here is
    // Status::Aborted().
    VLOG(1) << Substitute("SendTxnKeepAliveTask did not run: $0",
                          status.ToString());
    DCHECK(status.IsAborted());
    return;
  }

  // Check if the transaction object is still around.
  sp::shared_ptr<KuduTransaction> txn(weak_txn.lock());
  if (PREDICT_FALSE(!txn)) {
    return;
  }

  auto c = txn->data_->weak_client_.lock();
  if (PREDICT_FALSE(!c)) {
    return;
  }

  const auto& txn_id = txn->data_->txn_id_;
  auto deadline = MonoTime::Now() + txn->data_->GetKeepaliveRpcTimeout();

  sp::shared_ptr<KeepaliveRpcCtx> ctx(new KeepaliveRpcCtx);
  ctx->weak_txn = weak_txn;

  // We need to ensure the KuduClient outlives each RPC, so maintain a
  // reference to the KuduClient until the callback is called.
  ctx->client = c;

  {
    KeepTransactionAliveRequestPB req;
    req.set_txn_id(txn_id);
    unique_ptr<AsyncRandomTxnManagerRpc<KeepTransactionAliveRequestPB,
                                        KeepTransactionAliveResponsePB>> rpc(
        new AsyncRandomTxnManagerRpc<KeepTransactionAliveRequestPB,
                                     KeepTransactionAliveResponsePB>(
            deadline, c.get(), BackoffType::LINEAR, std::move(req), &ctx->resp,
            &TxnManagerServiceProxy::KeepTransactionAliveAsync,
            "KeepTransactionAlive",
            [ctx](const Status& s) {
              TxnKeepAliveCb(s, std::move(ctx));
            }));
    ctx->rpc = std::move(rpc);
  }

  // Send the RPC and handle the response asynchronously.
  ctx->rpc->SendRpc();
}

void KuduTransaction::Data::TxnKeepAliveCb(
    const Status& s, sp::shared_ptr<KeepaliveRpcCtx> ctx) {
  // Break the circular reference to 'ctx'. The circular reference is there
  // because KeepaliveRpcCtx::rpc captures the 'ctx' for ResponseCallback.
  ctx->rpc.reset();

  // Check if the transaction object is still around.
  sp::shared_ptr<KuduTransaction> txn(ctx->weak_txn.lock());
  if (PREDICT_FALSE(!txn)) {
    return;
  }
  const auto& resp = ctx->resp;
  if (s.ok() && resp.has_error()) {
    auto s = StatusFromPB(resp.error().status());
  }
  const auto& txn_id = txn->data_->txn_id_;
  if (s.IsIllegalState() || s.IsAborted()) {
    // Transaction's state changed a to terminal one, no need to send
    // keepalive requests anymore.
    VLOG(1) << Substitute("KeepTransactionAlive() returned $0: "
                          "stopping keepalive requests for transaction ID $1",
                          s.ToString(), txn_id.value());
    return;
  }

  // Re-schedule the task, so it will send another keepalive heartbeat as
  // necessary.
  //
  // If there was an error with the prior request, send the next one sooner
  // since one heartbeat has just been missed. If the prior request timed out,
  // send the next one as soon as possible. It's been a long interval since the
  // previous successfully sent keepalive message and it's been a long enough
  // interval since issuing preivous KeepTransactionAlive() RPC, so this should
  // not put too much pressure on the cluster (assuming the keepalive intervals
  // for a transaction is in order of a minute). Sending next message sooner
  // increases the chances of eventually getting through in such a case.
  const auto next_run_after =
      s.ok() ? txn->data_->GetKeepaliveRpcPeriod()
             : (s.IsTimedOut() ? MonoDelta::FromMilliseconds(1)
                               : txn->data_->GetKeepaliveRpcTimeout());
  DCHECK_GE(txn->data_->GetKeepaliveRpcPeriod().ToMilliseconds(),
            txn->data_->GetKeepaliveRpcTimeout().ToMilliseconds());
  auto m = ctx->client->data_->messenger_;
  if (PREDICT_TRUE(m)) {
    auto weak_txn = ctx->weak_txn;
    m->ScheduleOnReactor(
        [weak_txn](const Status& s) {
          SendTxnKeepAliveTask(s, weak_txn);
        },
        next_run_after);
  }
}

} // namespace client
} // namespace kudu
