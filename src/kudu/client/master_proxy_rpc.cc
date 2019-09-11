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
#include "kudu/client/master_proxy_rpc.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

using master::AlterTableRequestPB;
using master::AlterTableResponsePB;
using master::CreateTableRequestPB;
using master::CreateTableResponsePB;
using master::DeleteTableRequestPB;
using master::DeleteTableResponsePB;
using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using master::GetTabletLocationsRequestPB;
using master::GetTabletLocationsResponsePB;
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::GetTableSchemaRequestPB;
using master::GetTableSchemaResponsePB;
using master::GetTableStatisticsRequestPB;
using master::GetTableStatisticsResponsePB;
using master::ListMastersRequestPB;
using master::ListMastersResponsePB;
using master::ListTablesRequestPB;
using master::ListTablesResponsePB;
using master::ListTabletServersRequestPB;
using master::ListTabletServersResponsePB;
using master::MasterServiceProxy;
using master::MasterErrorPB;
using master::ReplaceTabletRequestPB;
using master::ReplaceTabletResponsePB;
using rpc::BackoffType;
using rpc::CredentialsPolicy;
using rpc::ErrorStatusPB;
using rpc::ResponseCallback;
using rpc::Rpc;
using rpc::RpcController;

namespace client {
namespace internal {

template <class ReqClass, class RespClass>
AsyncLeaderMasterRpc<ReqClass, RespClass>::AsyncLeaderMasterRpc(
    const MonoTime& deadline,
    KuduClient* client,
    BackoffType backoff,
    const ReqClass& req,
    RespClass* resp,
    const std::function<void(MasterServiceProxy*,
                             const ReqClass&, RespClass*,
                             RpcController*,
                             const ResponseCallback&)>& func,
    string rpc_name,
    StatusCallback user_cb,
    vector<uint32_t> required_feature_flags)
  : Rpc(deadline, client->data_->messenger_, backoff),
    client_(client), req_(&req), resp_(resp), func_(func),
    rpc_name_(std::move(rpc_name)),
    user_cb_(std::move(user_cb)),
    required_feature_flags_(std::move(required_feature_flags)) {
  DCHECK(deadline.Initialized());
}

template <class ReqClass, class RespClass>
void AsyncLeaderMasterRpc<ReqClass, RespClass>::SendRpc() {
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
  for (uint32_t required_feature_flag : required_feature_flags_) {
    controller->RequireServerFeature(required_feature_flag);
  }
  func_(client_->data_->master_proxy().get(), *req_, resp_, controller,
      std::bind(&AsyncLeaderMasterRpc<ReqClass, RespClass>::SendRpcCb,
          this, Status::OK()));
}

template <class ReqClass, class RespClass>
string AsyncLeaderMasterRpc<ReqClass, RespClass>::ToString() const {
  return rpc_name_;
}

template <class ReqClass, class RespClass>
void AsyncLeaderMasterRpc<ReqClass, RespClass>::SendRpcCb(const Status& status) {
  Status s = status;
  if (RetryOrReconnectIfNecessary(&s)) {
    return;
  }
  // Pull out any application-level errors and return them to the user.
  if (s.ok() && resp_->has_error()) {
    s = StatusFromPB(resp_->error().status());
  }
  user_cb_.Run(s);
}

template <class ReqClass, class RespClass>
void AsyncLeaderMasterRpc<ReqClass, RespClass>::NewLeaderMasterDeterminedCb(
    CredentialsPolicy creds_policy, const Status& status) {
  if (status.ok()) {
    SendRpc();
    return;
  }
  // If we got a NotAuthorized error, we most likely had an invalid
  // authentication token. Retry using primary credentials.
  if (status.IsNotAuthorized() && creds_policy != CredentialsPolicy::PRIMARY_CREDENTIALS) {
    KLOG_EVERY_N_SECS(WARNING, 1) << "Failed to connect to the cluster for a new authn token: "
                                  << status.ToString();
    ResetMasterLeaderAndRetry(CredentialsPolicy::PRIMARY_CREDENTIALS);
    return;
  }
  KLOG_EVERY_N_SECS(WARNING, 1) << "Failed to determine new leader Master: " << status.ToString();
  mutable_retrier()->DelayedRetry(this, status);
}

template <class ReqClass, class RespClass>
void AsyncLeaderMasterRpc<ReqClass, RespClass>::ResetMasterLeaderAndRetry(
    CredentialsPolicy creds_policy) {
  // TODO(aserbin): refactor ConnectToClusterAsync to purge cached master proxy
  // in case of NOT_THE_LEADER error and update it to handle
  // FATAL_INVALID_AUTHENTICATION_TOKEN error as well.
  client_->data_->ConnectToClusterAsync(
      client_, retrier().deadline(),
      Bind(&AsyncLeaderMasterRpc<ReqClass, RespClass>::NewLeaderMasterDeterminedCb,
           Unretained(this), creds_policy),
      creds_policy);
}

template <class ReqClass, class RespClass>
bool AsyncLeaderMasterRpc<ReqClass, RespClass>::RetryOrReconnectIfNecessary(
    Status* status) {
  const string retry_warning = Substitute("Re-attempting $0 request to leader Master ($1)",
      rpc_name_, client_->data_->leader_master_hostport().ToString());
  auto warn_on_retry = MakeScopedCleanup([&retry_warning] {
    // NOTE: we pass in a ref to 'retry_warning' rather than evaluating it here
    // because any of the below retries may end up completing and calling
    // 'user_cb_', which may very well destroy this Rpc object and render
    // 'rpc_name_' and 'client_' inaccessible.
    KLOG_EVERY_N_SECS(WARNING, 1) << retry_warning;
  });
  // Pull out the RPC status.
  const bool is_multi_master = client_->IsMultiMaster();
  Status s = *status;
  if (s.ok()) {
    s = retrier().controller().status();
  }

  // First, parse any RPC errors that may have occurred during the connection
  // negotiation.

  // Authorization errors during negotiation generally indicate failure to
  // authenticate. If that failure was due to an invalid authn token,
  // try to get a new one by establising a new connection to the master.
  const ErrorStatusPB* err = retrier().controller().error_response();
  if (s.IsNotAuthorized()) {
    if (err && err->has_code() &&
        err->code() == ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN) {
      ResetMasterLeaderAndRetry(CredentialsPolicy::PRIMARY_CREDENTIALS);
      return true;
    }
  }
  // Service unavailable errors during negotiation may indicate that the master
  // hasn't yet been updated with the most recent TSK.
  if (s.IsServiceUnavailable()) {
    if (is_multi_master) {
      ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
    } else {
      mutable_retrier()->DelayedRetry(this, s);
    }
    return true;
  }
  // Network errors may be caused by errors in connecting sockets, which could
  // mean a master is down or doesn't exist. If there's another master to
  // connect to, connect to it. Otherwise, don't bother retrying.
  if (s.IsNetworkError()) {
    if (is_multi_master) {
      ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
      return true;
    }
  }
  if (s.IsTimedOut()) {
    // If we timed out before the deadline and there's still time left for the
    // operation, try to reconnect to the master(s).
    if (MonoTime::Now() < retrier().deadline()) {
      if (is_multi_master) {
        ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
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
  if (s.IsRemoteError()) {
    CHECK(err);
    if (err->has_code() &&
        (err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY ||
         err->code() == ErrorStatusPB::ERROR_UNAVAILABLE)) {
      // If the server is too busy, try again later.
      mutable_retrier()->DelayedRetry(this, s);
      return true;
    }
    if (err->unsupported_feature_flags_size() > 0) {
      s = Status::NotSupported(Substitute("Cluster is does not support $0",
                                          rpc_name_));
    }
  }

  // Finally, parse generic application errors that we might expect from the
  // master(s).
  if (s.ok() && resp_->has_error()) {
    if (resp_->error().code() == MasterErrorPB::NOT_THE_LEADER ||
        resp_->error().code() == MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
      if (is_multi_master) {
        ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
      } else {
        mutable_retrier()->DelayedRetry(this, s);
      }
      return true;
    }
  }

  warn_on_retry.cancel();
  *status = s;
  return false;
}

template class AsyncLeaderMasterRpc<CreateTableRequestPB, CreateTableResponsePB>;
template class AsyncLeaderMasterRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB>;
template class AsyncLeaderMasterRpc<DeleteTableRequestPB, DeleteTableResponsePB>;
template class AsyncLeaderMasterRpc<AlterTableRequestPB, AlterTableResponsePB>;
template class AsyncLeaderMasterRpc<IsAlterTableDoneRequestPB, IsAlterTableDoneResponsePB>;
template class AsyncLeaderMasterRpc<GetTableSchemaRequestPB, GetTableSchemaResponsePB>;
template class AsyncLeaderMasterRpc<GetTableLocationsRequestPB, GetTableLocationsResponsePB>;
template class AsyncLeaderMasterRpc<GetTabletLocationsRequestPB, GetTabletLocationsResponsePB>;
template class AsyncLeaderMasterRpc<GetTableStatisticsRequestPB, GetTableStatisticsResponsePB>;
template class AsyncLeaderMasterRpc<ListTablesRequestPB, ListTablesResponsePB>;
template class AsyncLeaderMasterRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>;
template class AsyncLeaderMasterRpc<ListMastersRequestPB, ListMastersResponsePB>;
template class AsyncLeaderMasterRpc<ReplaceTabletRequestPB, ReplaceTabletResponsePB>;

} // namespace internal
} // namespace client
} // namespace kudu

