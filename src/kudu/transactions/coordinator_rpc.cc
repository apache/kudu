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

#include "kudu/transactions/coordinator_rpc.h"

#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/retriable_rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using kudu::client::internal::MetaCacheServerPicker;
using kudu::client::internal::RemoteTabletServer;
using kudu::client::KuduClient;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::CredentialsPolicy;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::ResponseCallback;
using kudu::rpc::RetriableRpc;
using kudu::rpc::RetriableRpcStatus;
using kudu::tserver::CoordinatorOpResultPB;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
class MonoTime;
namespace transactions {

CoordinatorRpc* CoordinatorRpc::NewRpc(
    unique_ptr<TxnStatusTabletContext> ctx,
    const MonoTime& deadline,
    StatusCallback cb,
    CoordinatorOpResultPB* op_result) {
  KuduClient* client = ctx->table->client();
  scoped_refptr<MetaCacheServerPicker> server_picker(
      new MetaCacheServerPicker(client,
                                client->data_->meta_cache_,
                                ctx->table.get(),
                                ctx->tablet.get()));
  CoordinatorRpc* rpc = new CoordinatorRpc(std::move(ctx),
                                           server_picker,
                                           deadline,
                                           std::move(cb),
                                           op_result);
  return rpc;
}

string CoordinatorRpc::ToString() const {
  return Substitute("CoordinateTransaction($0)", SecureShortDebugString(req_));
}

void CoordinatorRpc::Finish(const Status& status) {
  // Free memory upon completion.
  unique_ptr<CoordinatorRpc> this_instance(this);
  Status final_status = status;
  if (final_status.ok() &&
      resp_.has_op_result() && resp_.op_result().has_op_error()) {
    final_status = StatusFromPB(resp_.op_result().op_error());
  }
  if (resp_.has_op_result() && op_result_) {
    *op_result_ = resp_.op_result();
  }
  cb_(final_status);
}

bool CoordinatorRpc::GetNewAuthnTokenAndRetry() {
  resp_.Clear();
  client_->data_->ConnectToClusterAsync(client_, retrier().deadline(),
      [this] (const Status& s) { this->GotNewAuthnTokenRetryCb(s); },
      CredentialsPolicy::PRIMARY_CREDENTIALS);
  return true;
}

CoordinatorRpc::CoordinatorRpc(unique_ptr<TxnStatusTabletContext> ctx,
                               const scoped_refptr<MetaCacheServerPicker>& replica_picker,
                               const MonoTime& deadline,
                               StatusCallback cb,
                               CoordinatorOpResultPB* op_result)
    : RetriableRpc(replica_picker,
                   DCHECK_NOTNULL(ctx->table)->client()->data_->request_tracker_,
                   deadline,
                   DCHECK_NOTNULL(ctx->table)->client()->data_->messenger_),
      client_(ctx->table->client()),
      table_(std::move(ctx->table)),
      tablet_(std::move(ctx->tablet)),
      cb_(std::move(cb)),
      op_result_(op_result) {
  req_.set_txn_status_tablet_id(tablet_->tablet_id());
  *req_.mutable_op() = std::move(ctx->coordinate_txn_op);
}

void CoordinatorRpc::Try(RemoteTabletServer* replica,
                         const ResponseCallback& callback) {
  replica->admin_proxy()->CoordinateTransactionAsync(
      req_, &resp_, mutable_retrier()->mutable_controller(), callback);
}

// TODO(awong): much of this is borrowed from WriteRpc::AnalyzeResponse(). It'd
// be nice to share some code.
RetriableRpcStatus CoordinatorRpc::AnalyzeResponse(const Status& rpc_cb_status) {
  // We only analyze OK statuses if we succeeded to do the tablet lookup. In
  // either case, let's examine whatever errors exist.
  RetriableRpcStatus result;
  result.status = rpc_cb_status.ok() ? retrier().controller().status() : rpc_cb_status;

  // Check for specific RPC errors.
  if (result.status.IsRemoteError()) {
    const ErrorStatusPB* err = mutable_retrier()->controller().error_response();
    if (err && err->has_code()) {
      switch (err->code()) {
        case ErrorStatusPB::ERROR_SERVER_TOO_BUSY:
        case ErrorStatusPB::ERROR_UNAVAILABLE:
          result.result = RetriableRpcStatus::SERVICE_UNAVAILABLE;
          return result;
        default:
          break;
      }
    }
  }

  // TODO(awong): it might be easier to understand if the resulting expected
  // action were encoded in these status enums, e.g. RETRY_SAME_SERVER.
  if (result.status.IsServiceUnavailable()) {
    result.result = RetriableRpcStatus::SERVICE_UNAVAILABLE;
    return result;
  }

  // Check whether we need to get a new authentication token.
  if (result.status.IsNotAuthorized()) {
    const ErrorStatusPB* err = mutable_retrier()->controller().error_response();
    if (err && err->has_code() &&
        err->code() == ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN) {
      result.result = RetriableRpcStatus::INVALID_AUTHENTICATION_TOKEN;
      return result;
    }
  }

  // If we couldn't connect to the server, e.g. it was down, failover to a
  // different replica.
  if (result.status.IsNetworkError()) {
    result.result = RetriableRpcStatus::SERVER_NOT_ACCESSIBLE;
    return result;
  }

  // We're done parsing the RPC controller errors. Unwrap the tserver response
  // errors -- from here on out, the result status will be the response error.
  if (result.status.ok() && resp_.has_error()) {
    result.status = StatusFromPB(resp_.error().status());
  }

  // If we get TABLET_NOT_FOUND, the replica we thought was leader has been
  // deleted.
  if (resp_.has_error() &&
      resp_.error().code() == tserver::TabletServerErrorPB::TABLET_NOT_FOUND) {
    result.result = RetriableRpcStatus::RESOURCE_NOT_FOUND;
    return result;
  }

  if (result.status.IsIllegalState() || result.status.IsAborted()) {
    result.result = RetriableRpcStatus::REPLICA_NOT_LEADER;
    return result;
  }

  // Handle the connection negotiation failure case if overall RPC's timeout
  // hasn't expired yet: if the connection negotiation returned non-OK status,
  // mark the server as not accessible and rely on the RetriableRpc's logic
  // to switch to an alternative tablet replica.
  //
  // NOTE: Connection negotiation errors related to security are handled in the
  //       code above: see the handlers for IsNotAuthorized(), IsRemoteError().
  if (!rpc_cb_status.IsTimedOut() && !result.status.ok() &&
      mutable_retrier()->controller().negotiation_failed()) {
    result.result = RetriableRpcStatus::SERVER_NOT_ACCESSIBLE;
    return result;
  }

  if (result.status.ok()) {
    result.result = RetriableRpcStatus::OK;
  } else {
    result.result = RetriableRpcStatus::NON_RETRIABLE_ERROR;
  }
  return result;
}

} // namespace transactions
} // namespace kudu
