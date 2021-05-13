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

#include "kudu/transactions/participant_rpc.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/retriable_rpc.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using kudu::client::internal::MetaCacheServerPicker;
using kudu::client::internal::RemoteTabletServer;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::CredentialsPolicy;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::ResponseCallback;
using kudu::rpc::RetriableRpc;
using kudu::rpc::RetriableRpcStatus;
using kudu::tablet::TxnMetadataPB;
using kudu::tserver::TabletServerErrorPB;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
class MonoTime;

namespace transactions {

ParticipantRpc* ParticipantRpc::NewRpc(
    unique_ptr<TxnParticipantContext> ctx,
    const MonoTime& deadline,
    StatusCallback user_cb,
    Timestamp* begin_commit_timestamp,
    TxnMetadataPB* metadata_pb) {
  scoped_refptr<MetaCacheServerPicker> server_picker(
      new MetaCacheServerPicker(ctx->client,
                                ctx->client->data_->meta_cache_,
                                /*table*/nullptr,  // Lookup by tablet ID.
                                DCHECK_NOTNULL(ctx->tablet.get())));
  return new ParticipantRpc(std::move(ctx),
                            std::move(server_picker),
                            deadline,
                            std::move(user_cb),
                            begin_commit_timestamp,
                            metadata_pb);
}

ParticipantRpc::ParticipantRpc(unique_ptr<TxnParticipantContext> ctx,
                               scoped_refptr<MetaCacheServerPicker> replica_picker,
                               const MonoTime& deadline,
                               StatusCallback user_cb,
                               Timestamp* begin_commit_timestamp,
                               TxnMetadataPB* metadata_pb)
    : RetriableRpc(std::move(replica_picker), ctx->client->data_->request_tracker_,
                   deadline, ctx->client->data_->messenger_),
      client_(ctx->client),
      tablet_(std::move(ctx->tablet)),
      user_cb_(std::move(user_cb)),
      begin_commit_timestamp_(begin_commit_timestamp),
      metadata_pb_(metadata_pb) {
  req_.set_tablet_id(tablet_->tablet_id());
  *req_.mutable_op() = std::move(ctx->participant_op);
}

string ParticipantRpc::ToString() const {
  return Substitute("ParticipateInTransaction($0)", SecureShortDebugString(req_));
}

void ParticipantRpc::Try(RemoteTabletServer* replica,
                         const ResponseCallback& callback) {
  // NOTE: 'callback' is typically SendRpcCb(), which calls AnalyzeResponse().
  replica->admin_proxy()->ParticipateInTransactionAsync(
      req_, &resp_, mutable_retrier()->mutable_controller(), callback);
}

RetriableRpcStatus ParticipantRpc::AnalyzeResponse(const Status& rpc_cb_status) {
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
        // NOTE: When can get an ERROR_NO_SUCH_SERVICE if we send a request as
        // a tablet server is being destructed. Just retry. Even if this were a
        // legitimate "no such service" error, we'll eventually time out.
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

  if (resp_.has_error()) {
    const auto code = resp_.error().code();
    switch (code) {
      // If we get TABLET_NOT_FOUND, the replica we thought was leader
      // has been deleted.
      case TabletServerErrorPB::TABLET_NOT_FOUND:
      case TabletServerErrorPB::TABLET_FAILED:
        result.result = RetriableRpcStatus::RESOURCE_NOT_FOUND;
        return result;
      case TabletServerErrorPB::TABLET_NOT_RUNNING:
      case TabletServerErrorPB::THROTTLED:
      case TabletServerErrorPB::TXN_LOCKED_RETRY_OP:
        result.result = RetriableRpcStatus::SERVICE_UNAVAILABLE;
        return result;
      case TabletServerErrorPB::NOT_THE_LEADER:
        result.result = RetriableRpcStatus::REPLICA_NOT_LEADER;
        return result;
      case TabletServerErrorPB::TXN_ILLEGAL_STATE:
      case TabletServerErrorPB::TXN_LOCKED_ABORT:
        result.result = RetriableRpcStatus::NON_RETRIABLE_ERROR;
        return result;
      case TabletServerErrorPB::TXN_OP_ALREADY_APPLIED:
        // TXN_OP_ALREADY_APPLIED means the op succeeded, so stop retrying and
        // return success.
        result.result = RetriableRpcStatus::OK;
        result.status = Status::OK();
        return result;
      case TabletServerErrorPB::UNKNOWN_ERROR:
      default:
        // The rest is handled in the code below.
        break;
    }
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

void ParticipantRpc::Finish(const Status& status) {
  // Free memory upon completion.
  unique_ptr<ParticipantRpc> delete_me(this);
  if (status.ok()) {
    if (begin_commit_timestamp_ && resp_.has_timestamp()) {
      *begin_commit_timestamp_ = Timestamp(resp_.timestamp());
    }
    if (metadata_pb_ && resp_.has_metadata()) {
      *metadata_pb_ = resp_.metadata();
    }
  }
  user_cb_(status);
}

bool ParticipantRpc::GetNewAuthnTokenAndRetry() {
  resp_.Clear();
  client_->data_->ConnectToClusterAsync(client_, retrier().deadline(),
      [this] (const Status& s) { this->GotNewAuthnTokenRetryCb(s); },
      CredentialsPolicy::PRIMARY_CREDENTIALS);
  return true;
}

} // namespace transactions
} // namespace kudu
