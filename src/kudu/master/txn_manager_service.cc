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

#include "kudu/master/txn_manager_service.h"

#include <cstdint>
#include <string>

#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/port.h"
#include "kudu/master/master.h"
#include "kudu/master/txn_manager.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/server_base.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/status.h"

using kudu::master::Master;
using kudu::rpc::RpcContext;
using kudu::server::ServerBase;
using kudu::transactions::TxnStatusEntryPB;

using std::string;

namespace kudu {
namespace transactions {

namespace {

// If 's' is not OK and 'resp' has no application specific error set,
// set the error field of 'resp' to match 's' and set the code to
// UNKNOWN_ERROR.
template<class RespClass>
void CheckRespErrorOrSetUnknown(const Status& s, RespClass* resp) {
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
    resp->mutable_error()->set_code(TxnManagerErrorPB::UNKNOWN_ERROR);
  }
}

const auto kMissingTxnId =
    Status::InvalidArgument("missing transaction identifier");

} // anonymous namespace

TxnManagerServiceImpl::TxnManagerServiceImpl(Master* server)
    : TxnManagerServiceIf(server->metric_entity(), server->result_tracker()),
      server_(server) {
}

void TxnManagerServiceImpl::BeginTransaction(
    const BeginTransactionRequestPB* /* req */,
    BeginTransactionResponsePB* resp,
    RpcContext* ctx) {
  int64_t txn_id;
  uint32_t txn_keepalive_ms;
  const auto s = server_->txn_manager()->BeginTransaction(
      ctx->remote_user().username(),
      ctx->GetClientDeadline(),
      &txn_id,
      &txn_keepalive_ms);
  if (PREDICT_TRUE(s.ok())) {
    resp->set_txn_id(txn_id);
    resp->set_keepalive_millis(txn_keepalive_ms);
  }
  CheckRespErrorOrSetUnknown(s, resp);
  return ctx->RespondSuccess();
}

void TxnManagerServiceImpl::CommitTransaction(
    const CommitTransactionRequestPB* req,
    CommitTransactionResponsePB* resp,
    RpcContext* ctx) {
  if (!req->has_txn_id()) {
    CheckRespErrorOrSetUnknown(kMissingTxnId, resp);
    return ctx->RespondSuccess();
  }
  // Initiate the commit phase for the transaction. The caller can check for the
  // completion of the commit phase using the GetTransactionState() RPC.
  const auto s = server_->txn_manager()->CommitTransaction(
      req->txn_id(),
      ctx->remote_user().username(),
      ctx->GetClientDeadline());
  CheckRespErrorOrSetUnknown(s, resp);
  return ctx->RespondSuccess();
}

void TxnManagerServiceImpl::GetTransactionState(
    const GetTransactionStateRequestPB* req,
    GetTransactionStateResponsePB* resp,
    rpc::RpcContext* ctx) {
  if (!req->has_txn_id()) {
    CheckRespErrorOrSetUnknown(kMissingTxnId, resp);
    return ctx->RespondSuccess();
  }
  TxnStatusEntryPB txn_status;
  const auto s = server_->txn_manager()->GetTransactionState(
      req->txn_id(),
      ctx->remote_user().username(),
      ctx->GetClientDeadline(),
      &txn_status);
  if (PREDICT_TRUE(s.ok())) {
    DCHECK(txn_status.has_state());
    resp->set_state(txn_status.state());
    // An empty transaction doesn't have a commit timestamp; only non-empty ones
    // have their commit timestamps assigned and persisted.
    if (txn_status.has_commit_timestamp()) {
      DCHECK(txn_status.state() == TxnStatePB::COMMITTED ||
             txn_status.state() == TxnStatePB::FINALIZE_IN_PROGRESS);
      resp->set_commit_timestamp(txn_status.commit_timestamp());
    }
  }
  CheckRespErrorOrSetUnknown(s, resp);
  return ctx->RespondSuccess();
}

void TxnManagerServiceImpl::AbortTransaction(
    const AbortTransactionRequestPB* req,
    AbortTransactionResponsePB* resp,
    RpcContext* ctx) {
  if (!req->has_txn_id()) {
    CheckRespErrorOrSetUnknown(kMissingTxnId, resp);
    return ctx->RespondSuccess();
  }
  const auto s = server_->txn_manager()->AbortTransaction(
      req->txn_id(),
      ctx->remote_user().username(),
      ctx->GetClientDeadline());
  CheckRespErrorOrSetUnknown(s, resp);
  return ctx->RespondSuccess();
}

void TxnManagerServiceImpl::KeepTransactionAlive(
    const KeepTransactionAliveRequestPB* req,
    KeepTransactionAliveResponsePB* resp,
    rpc::RpcContext* ctx) {
  if (!req->has_txn_id()) {
    CheckRespErrorOrSetUnknown(kMissingTxnId, resp);
    return ctx->RespondSuccess();
  }
  const auto s = server_->txn_manager()->KeepTransactionAlive(
      req->txn_id(),
      ctx->remote_user().username(),
      ctx->GetClientDeadline());
  CheckRespErrorOrSetUnknown(s, resp);
  return ctx->RespondSuccess();
}

bool TxnManagerServiceImpl::AuthorizeClient(
    const google::protobuf::Message* /* req */,
    google::protobuf::Message* /* resp */,
    RpcContext* ctx) {
  return server_->Authorize(ctx, ServerBase::SUPER_USER | ServerBase::USER);
}

} // namespace transactions
} // namespace kudu
