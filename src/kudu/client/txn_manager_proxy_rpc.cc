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

#include "kudu/client/txn_manager_proxy_rpc.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using kudu::rpc::BackoffType;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::ResponseCallback;
using kudu::rpc::Rpc;
using kudu::rpc::RpcController;
using kudu::transactions::AbortTransactionRequestPB;
using kudu::transactions::AbortTransactionResponsePB;
using kudu::transactions::BeginTransactionRequestPB;
using kudu::transactions::BeginTransactionResponsePB;
using kudu::transactions::CommitTransactionRequestPB;
using kudu::transactions::CommitTransactionResponsePB;
using kudu::transactions::GetTransactionStateRequestPB;
using kudu::transactions::GetTransactionStateResponsePB;
using kudu::transactions::TxnManagerServiceProxy;
using std::string;
using strings::Substitute;

namespace kudu {
namespace client {
namespace internal {

template <class ReqClass, class RespClass>
AsyncRandomTxnManagerRpc<ReqClass, RespClass>::AsyncRandomTxnManagerRpc(
    const MonoTime& deadline,
    KuduClient* client,
    BackoffType backoff,
    const ReqClass& req,
    RespClass* resp,
    const std::function<void(TxnManagerServiceProxy*,
                             const ReqClass&, RespClass*,
                             RpcController*,
                             const ResponseCallback&)>& func,
    string rpc_name,
    StatusCallback user_cb)
    : Rpc(deadline, client->data_->messenger_, backoff),
      client_(client), req_(&req), resp_(resp), func_(func),
      rpc_name_(std::move(rpc_name)),
      user_cb_(std::move(user_cb)) {
  DCHECK(deadline.Initialized());
}

template <class ReqClass, class RespClass>
void AsyncRandomTxnManagerRpc<ReqClass, RespClass>::SendRpc() {
  // Reset the deadline for a single RPC, and make sure it hasn't passed.
  // Ensure the RPC neither passes the default RPC deadline nor our overall
  // deadline.
  MonoTime now = MonoTime::Now();
  MonoTime deadline = std::min(retrier().deadline(),
                               now + client_->default_rpc_timeout());
  if (deadline < now) {
    SendRpcCb(Status::TimedOut("timed out after deadline expired"));
    return;
  }
  RpcController* controller = mutable_retrier()->mutable_controller();
  controller->Reset();
  controller->set_deadline(deadline);
  func_(client_->data_->txn_manager_proxy().get(), *req_, resp_, controller,
        [this]() { this->SendRpcCb(Status::OK()); });
}

template <class ReqClass, class RespClass>
string AsyncRandomTxnManagerRpc<ReqClass, RespClass>::ToString() const {
  return rpc_name_;
}

template <class ReqClass, class RespClass>
void AsyncRandomTxnManagerRpc<ReqClass, RespClass>::SendRpcCb(const Status& status) {
  Status s = status;
  if (RetryIfNecessary(&s)) {
    return;
  }
  // Pull out any application-level errors and return them to the user.
  if (s.ok() && resp_->has_error()) {
    s = StatusFromPB(resp_->error().status());
  }
  user_cb_(s);
}

template <class ReqClass, class RespClass>
void AsyncRandomTxnManagerRpc<ReqClass, RespClass>::ResetTxnManagerAndRetry(
    const Status& status) {
  // TODO(aserbin): implement switching to other TxnManager in case if
  //                many are available.
  mutable_retrier()->DelayedRetry(this, status);
}

template <class ReqClass, class RespClass>
bool AsyncRandomTxnManagerRpc<ReqClass, RespClass>::RetryIfNecessary(
    Status* status) {
  const auto retry_warning =
      Substitute("re-attempting $0 request to one of TxnManagers", rpc_name_);
  auto warn_on_retry = MakeScopedCleanup([&retry_warning] {
    // NOTE: we pass in a ref to 'retry_warning' rather than evaluating it here
    // because any of the below retries may end up completing and calling
    // 'user_cb_', which may very well destroy this Rpc object and render
    // 'rpc_name_' and 'client_' inaccessible.
    KLOG_EVERY_N_SECS(WARNING, 1) << retry_warning;
  });
  const bool multi_txn_manager = client_->IsMultiMaster();
  // Pull out the RPC status.
  Status s = *status;
  if (s.ok()) {
    s = retrier().controller().status();
  }

  // First, parse any RPC errors that may have occurred during the connection
  // negotiation.

  // TODO(aserbin): address the case of expired authn token

  // Service unavailable errors during negotiation may indicate that current
  // TxnManager could not verify authn token. Simply retry the operation with
  // another TxnManager (if available) or re-send the RPC to the same TxnManager
  // a bit later.
  if (s.IsServiceUnavailable()) {
    if (multi_txn_manager) {
      ResetTxnManagerAndRetry(s);
    } else {
      mutable_retrier()->DelayedRetry(this, s);
    }
    return true;
  }

  // Network errors may be caused by errors in connecting sockets, which could
  // mean a TxnManager is down or doesn't exist. If there's another TxnManager
  // to connect to, connect to it. Otherwise, don't bother retrying.
  if (s.IsNetworkError()) {
    if (multi_txn_manager) {
      ResetTxnManagerAndRetry(s);
      return true;
    }
  }
  if (s.IsTimedOut()) {
    // If establishing connection failed with time out error before the overall
    // deadline for RPC operation, retry the operation; if multiple TxnManagers
    // are available, try with another one.
    if (MonoTime::Now() < retrier().deadline()) {
      if (multi_txn_manager) {
        ResetTxnManagerAndRetry(s);
      } else {
        mutable_retrier()->DelayedRetry(this, s);
      }
      return true;
    }
    // And if we've passed the overall deadline, we shouldn't retry.
    s = s.CloneAndPrepend(Substitute("$0 timed out after deadline expired", rpc_name_));
  }

  // Next, parse RPC errors that happened after the connection succeeded.
  // Note: RemoteErrors from the controller are guaranteed to also return error
  // responses, per RpcController's contract (see rpc_controller.h).
  const ErrorStatusPB* err = retrier().controller().error_response();
  if (s.IsRemoteError()) {
    CHECK(err);
    if (err->has_code() &&
        (err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY ||
         err->code() == ErrorStatusPB::ERROR_UNAVAILABLE)) {
      // If TxnManager is too busy, try another one if it's around or try again
      // later with the same TxnManager.
      if (multi_txn_manager) {
        ResetTxnManagerAndRetry(s);
      } else {
        mutable_retrier()->DelayedRetry(this, s);
      }
      return true;
    }
    if (err->unsupported_feature_flags_size() > 0) {
      s = Status::NotSupported(Substitute("cluster does not support $0",
                                          rpc_name_));
    }
  }

  warn_on_retry.cancel();
  *status = s;
  return false;
}

template class AsyncRandomTxnManagerRpc<AbortTransactionRequestPB,
                                        AbortTransactionResponsePB>;
template class AsyncRandomTxnManagerRpc<BeginTransactionRequestPB,
                                        BeginTransactionResponsePB>;
template class AsyncRandomTxnManagerRpc<CommitTransactionRequestPB,
                                        CommitTransactionResponsePB>;
template class AsyncRandomTxnManagerRpc<GetTransactionStateRequestPB,
                                        GetTransactionStateResponsePB>;

} // namespace internal
} // namespace client
} // namespace kudu
