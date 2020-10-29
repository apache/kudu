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

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/txn_manager_proxy_rpc.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/master/txn_manager.proxy.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

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
using kudu::transactions::TxnManagerServiceProxy;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnTokenPB;
using std::string;
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
      txn_keep_alive_ms_(0) {
  CHECK(client);
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
  *session = std::move(ret);
  return Status::OK();
}

Status KuduTransaction::Data::Begin() {
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }
  const auto deadline = GetRpcDeadline(c.get());
  Synchronizer sync;
  BeginTransactionRequestPB req;
  BeginTransactionResponsePB resp;
  AsyncRandomTxnManagerRpc<
      BeginTransactionRequestPB, BeginTransactionResponsePB> rpc(
      deadline, c.get(), BackoffType::EXPONENTIAL, req, &resp,
      &TxnManagerServiceProxy::BeginTransactionAsync, "BeginTransaction",
      sync.AsStatusCallback());
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  // TODO(aserbin): start sending regular hearbeats for the started transaction
  CHECK(resp.has_txn_id());
  txn_id_ = resp.txn_id();
  CHECK(resp.has_keepalive_millis());
  txn_keep_alive_ms_ = resp.keepalive_millis();
  CHECK(txn_id_.IsValid());

  return Status::OK();
}

Status KuduTransaction::Data::Commit(bool wait) {
  CHECK(txn_id_.IsValid());
  auto c = weak_client_.lock();
  if (!c) {
    return Status::IllegalState("associated KuduClient is gone");
  }
  const auto deadline = GetRpcDeadline(c.get());
  Synchronizer sync;
  CommitTransactionRequestPB req;
  req.set_txn_id(txn_id_);
  CommitTransactionResponsePB resp;
  AsyncRandomTxnManagerRpc<CommitTransactionRequestPB,
                           CommitTransactionResponsePB> rpc(
      deadline, c.get(), BackoffType::EXPONENTIAL, req, &resp,
      &TxnManagerServiceProxy::CommitTransactionAsync, "CommitTransaction",
      sync.AsStatusCallback());
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (wait) {
    RETURN_NOT_OK(WaitForTxnCommitToFinalize(c.get(), deadline, txn_id_));
  }
  return Status::OK();
}

Status KuduTransaction::Data::IsCommitComplete(
    bool* is_complete, Status* completion_status) {
  DCHECK(is_complete);
  DCHECK(completion_status);
  CHECK(txn_id_.IsValid());
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
  const auto deadline = GetRpcDeadline(c.get());
  Synchronizer sync;
  AbortTransactionRequestPB req;
  req.set_txn_id(txn_id_);
  AbortTransactionResponsePB resp;
  AsyncRandomTxnManagerRpc<AbortTransactionRequestPB,
                           AbortTransactionResponsePB> rpc(
      deadline, c.get(), BackoffType::EXPONENTIAL, req, &resp,
      &TxnManagerServiceProxy::AbortTransactionAsync, "AbortTransaction",
      sync.AsStatusCallback());
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status KuduTransaction::Data::Serialize(string* serialized_txn) const {
  CHECK(txn_id_.IsValid());
  DCHECK(serialized_txn);
  TxnTokenPB token;
  token.set_txn_id(txn_id_);
  if (txn_keep_alive_ms_ > 0) {
    token.set_keepalive_millis(txn_keep_alive_ms_);
  }
  if (!token.SerializeToString(serialized_txn)) {
    return Status::Corruption("unable to serialize transaction information");
  }
  return Status::OK();
}

Status KuduTransaction::Data::Deserialize(
    const sp::shared_ptr<KuduClient>& client,
    const string& serialized_txn,
    sp::shared_ptr<KuduTransaction>* txn) {
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
  ret->data_->txn_id_ = token.txn_id();
  ret->data_->txn_keep_alive_ms_ = token.keepalive_millis();
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
  Synchronizer sync;
  GetTransactionStateRequestPB req;
  req.set_txn_id(txn_id);
  GetTransactionStateResponsePB resp;
  AsyncRandomTxnManagerRpc<GetTransactionStateRequestPB,
                           GetTransactionStateResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, &resp,
      &TxnManagerServiceProxy::GetTransactionStateAsync, "GetTransactionState",
      sync.AsStatusCallback());
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
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
    case TxnStatePB::ABORTED:
      *is_complete = true;
      *completion_status = Status::Aborted("transaction has been aborted");
      break;
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

} // namespace client
} // namespace kudu
