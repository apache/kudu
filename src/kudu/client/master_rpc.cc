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
//
// This module is internal to the client and not a public API.

#include "kudu/client/master_rpc.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using kudu::consensus::RaftPeerPB;
using kudu::master::ConnectToMasterRequestPB;
using kudu::master::ConnectToMasterResponsePB;
using kudu::master::GetMasterRegistrationRequestPB;
using kudu::master::GetMasterRegistrationResponsePB;
using kudu::master::MasterErrorPB;
using kudu::master::MasterServiceProxy;
using kudu::rpc::BackoffType;
using kudu::rpc::CredentialsPolicy;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::Messenger;
using kudu::rpc::Rpc;
using kudu::rpc::RpcController;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {
namespace internal {

////////////////////////////////////////////////////////////
// GetMasterRegistrationRpc
////////////////////////////////////////////////////////////
namespace {

// An RPC for trying to connect via a particular Master.
class ConnectToMasterRpc : public Rpc {
 public:

  // Create a wrapper object for a retriable ConnectToMaster RPC
  // to 'addr'. The result is stored in 'out', which must be a valid
  // pointer for the lifetime of this object.
  //
  // Invokes 'user_cb' upon failure or success of the RPC call.
  ConnectToMasterRpc(StatusCallback user_cb,
                     pair<Sockaddr, string> addr_with_name,
                     const MonoTime& deadline,
                     std::shared_ptr<rpc::Messenger> messenger,
                     rpc::UserCredentials user_credentials,
                     CredentialsPolicy creds_policy,
                     ConnectToMasterResponsePB* out);

  ~ConnectToMasterRpc();

  virtual void SendRpc() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  const StatusCallback user_cb_;

  // The resolved address to try to connect to, along with its original specified hostname.
  const pair<Sockaddr, string> addr_with_name_;

  // The client user credentials.
  const rpc::UserCredentials user_credentials_;

  // Owned by the caller of this RPC, not this instance.
  ConnectToMasterResponsePB* out_;

  // When connecting to Kudu <1.3 masters, the ConnectToMaster
  // RPC is not supported. Instead we use GetMasterRegistration(),
  // store the response here, and convert it to look like the new
  // style response.
  GetMasterRegistrationResponsePB old_rpc_resp_;
  bool trying_old_rpc_ = false;
};


ConnectToMasterRpc::ConnectToMasterRpc(StatusCallback user_cb,
    pair<Sockaddr, string> addr_with_name,
    const MonoTime& deadline,
    shared_ptr<Messenger> messenger,
    rpc::UserCredentials user_credentials,
    rpc::CredentialsPolicy creds_policy,
    ConnectToMasterResponsePB* out)
      : Rpc(deadline, std::move(messenger), BackoffType::LINEAR),
        user_cb_(std::move(user_cb)),
        addr_with_name_(std::move(addr_with_name)),
        user_credentials_(std::move(user_credentials)),
        out_(DCHECK_NOTNULL(out)) {
  mutable_retrier()->mutable_controller()->set_credentials_policy(creds_policy);
}

ConnectToMasterRpc::~ConnectToMasterRpc() {
}

void ConnectToMasterRpc::SendRpc() {
  MasterServiceProxy proxy(retrier().messenger(), addr_with_name_.first, addr_with_name_.second);
  proxy.set_user_credentials(user_credentials_);
  rpc::RpcController* controller = mutable_retrier()->mutable_controller();
  // TODO(todd): should this be setting an RPC call deadline based on 'deadline'?
  // it doesn't seem to be.
  if (!trying_old_rpc_) {
    ConnectToMasterRequestPB req;
    controller->RequireServerFeature(master::MasterFeatures::CONNECT_TO_MASTER);
    proxy.ConnectToMasterAsync(req, out_, controller,
                               [this]() { this->SendRpcCb(Status::OK()); });
  } else {
    GetMasterRegistrationRequestPB req;
    proxy.GetMasterRegistrationAsync(req, &old_rpc_resp_, controller,
                                     [this]() { this->SendRpcCb(Status::OK()); });
  }
}

string ConnectToMasterRpc::ToString() const {
  return strings::Substitute("ConnectToMasterRpc(address: $0:$1, num_attempts: $2)",
                             addr_with_name_.second, addr_with_name_.first.port(),
                             num_attempts());
}

void ConnectToMasterRpc::SendRpcCb(const Status& status) {
  // NOTE: 'status' here may actually come from RpcRetrier::DelayedRetryCb if
  // retrying from DelayedRetry() below. If we successfully send an RPC, it
  // will be Status::OK.
  //
  // TODO(todd): this is the most confusing code I've ever seen...
  unique_ptr<ConnectToMasterRpc> deleter(this);
  Status new_status = status;

  rpc::RpcController* rpc = mutable_retrier()->mutable_controller();
  if (new_status.ok()) {
    new_status = rpc->status();
    if (new_status.IsRemoteError()) {
      const ErrorStatusPB* err = rpc->error_response();
      // The UNAVAILABLE code is a broader counterpart of the SERVER_TOO_BUSY.
      // In both cases it's necessary to retry a bit later.
      if (err && err->has_code() &&
          (err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY ||
           err->code() == ErrorStatusPB::ERROR_UNAVAILABLE)) {
        mutable_retrier()->DelayedRetry(this, new_status);
        ignore_result(deleter.release());
        return;
      }
    }
  }

  if (!trying_old_rpc_ &&
      new_status.IsRemoteError() &&
      rpc->error_response()->unsupported_feature_flags_size() > 0) {
    VLOG(1) << "Connecting to an old-version cluster which does not support ConnectToCluster(). "
            << "Falling back to GetMasterRegistration().";
    trying_old_rpc_ = true;
    // retry immediately.
    ignore_result(deleter.release());
    rpc->Reset();
    SendRpc();
    return;
  }

  // If we sent the old RPC, then translate its response to the new RPC.
  if (trying_old_rpc_) {
    out_->Clear();
    if (old_rpc_resp_.has_error()) {
      out_->set_allocated_error(old_rpc_resp_.release_error());
    }
    if (old_rpc_resp_.has_role()) {
      out_->set_role(old_rpc_resp_.role());
    }
  }

  if (new_status.ok() && out_->has_error()) {
    if (out_->error().code() == MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
      // If CatalogManager is not initialized, treat the node as a
      // FOLLOWER for the time being, as currently this RPC is only
      // used for the purposes of finding the leader master.
      out_->set_role(RaftPeerPB::FOLLOWER);
      new_status = Status::OK();
    } else {
      new_status = StatusFromPB(out_->error().status());
    }
  }
  user_cb_(new_status);
}

} // anonymous namespace

////////////////////////////////////////////////////////////
// ConnectToClusterRpc
////////////////////////////////////////////////////////////

ConnectToClusterRpc::ConnectToClusterRpc(LeaderCallback user_cb,
                                         vector<pair<Sockaddr, string>> addrs_with_names,
                                         MonoTime deadline,
                                         MonoDelta rpc_timeout,
                                         shared_ptr<Messenger> messenger,
                                         rpc::UserCredentials user_credentials,
                                         rpc::CredentialsPolicy creds_policy)
    : Rpc(deadline, std::move(messenger), BackoffType::LINEAR),
      user_cb_(std::move(user_cb)),
      addrs_with_names_(std::move(addrs_with_names)),
      user_credentials_(std::move(user_credentials)),
      rpc_timeout_(rpc_timeout),
      pending_responses_(0),
      completed_(false) {
  DCHECK(deadline.Initialized());

  // Using resize instead of reserve to explicitly initialized the values.
  responses_.resize(addrs_with_names_.size());
  mutable_retrier()->mutable_controller()->set_credentials_policy(creds_policy);
}

ConnectToClusterRpc::~ConnectToClusterRpc() {
}

string ConnectToClusterRpc::ToString() const {
  vector<string> addrs_str;
  for (const auto& addr_with_name : addrs_with_names_) {
    addrs_str.emplace_back(Substitute(
        "$0:$1", addr_with_name.second, addr_with_name.first.port()));
  }
  return strings::Substitute("ConnectToClusterRpc(addrs: $0, num_attempts: $1)",
                             JoinStrings(addrs_str, ","),
                             num_attempts());
}

void ConnectToClusterRpc::SendRpc() {
  // Compute the actual deadline to use for each RPC.
  const MonoTime rpc_deadline = MonoTime::Now() + rpc_timeout_;
  const MonoTime actual_deadline = std::min(retrier().deadline(), rpc_deadline);

  std::lock_guard<simple_spinlock> l(lock_);
  for (int i = 0; i < addrs_with_names_.size(); i++) {
    scoped_refptr<ConnectToClusterRpc> self(this);
    ConnectToMasterRpc* rpc = new ConnectToMasterRpc(
        [self, i](const Status& s) { self->SingleNodeCallback(i, s); },
        addrs_with_names_[i],
        actual_deadline,
        retrier().messenger(),
        user_credentials_,
        retrier().controller().credentials_policy(),
        &responses_[i]);
    rpc->SendRpc();
    ++pending_responses_;
  }
}

void ConnectToClusterRpc::SendRpcCb(const Status& status) {
  // To safely retry, we must reset completed_ so that it can be reused in the
  // next round of RPCs.
  //
  // The SendRpcCb invariant (see SingleNodeCallback comments)
  // implies that if we're to retry, we must be the last response. Thus, it is
  // safe to reset completed_ in this case; there's no danger of a late
  // response reading it and entering SendRpcCb inadvertently.
  auto undo_completed = MakeScopedCleanup([&]() {
    std::lock_guard<simple_spinlock> l(lock_);
    completed_ = false;
  });

  // If we've received replies from all of the nodes without finding
  // the leader, or if there were network errors talking to all of the
  // nodes the error is retriable and we can perform a delayed retry.
  if (status.IsNetworkError() || status.IsNotFound()) {
    mutable_retrier()->DelayedRetry(this, status);
    return;
  }

  // If our replies timed out but the deadline hasn't passed, retry.
  if (status.IsTimedOut() && MonoTime::Now() < retrier().deadline()) {
    mutable_retrier()->DelayedRetry(this, status);
    return;
  }

  // We are not retrying.
  undo_completed.cancel();
  if (leader_idx_ != -1) {
    user_cb_(status, addrs_with_names_[leader_idx_], responses_[leader_idx_]);
  } else {
    user_cb_(status, {}, {});
  }
}

void ConnectToClusterRpc::SingleNodeCallback(int master_idx,
                                             const Status& status) {
  const ConnectToMasterResponsePB& resp = responses_[master_idx];

  // TODO(todd): handle the situation where one Master is partitioned from
  // the rest of the Master consensus configuration, all are reachable by the client,
  // and the partitioned node "thinks" it's the leader.
  //
  // The proper way to do so is to add term/index to the responses
  // from the Master, wait for majority of the Masters to respond, and
  // pick the one with the highest term/index as the leader.
  Status new_status = status;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    if (completed_) {
      // If 'user_cb_' has been invoked (see SendRpcCb above), we can
      // stop.
      return;
    }
    if (new_status.ok()) {
      if (resp.role() != RaftPeerPB::LEADER) {
        string msg;
        if (resp.master_addrs_size() > 0 &&
            resp.master_addrs_size() > addrs_with_names_.size()) {
          // If we connected to a non-leader, and it responds that the
          // number of masters in the cluster is more than the client's
          // view of the number of masters, then it's likely the client
          // is mis-configured (i.e with a subset of the masters).
          // We'll include that info in the error message.
          string client_config = JoinMapped(
              addrs_with_names_,
              [](const pair<Sockaddr, string>& addr_with_name) {
                return Substitute("$0:$1", addr_with_name.second, addr_with_name.first.port());
              }, ",");
          string cluster_config = JoinMapped(
              resp.master_addrs(),
              [](const HostPortPB& pb) {
                return Substitute("$0:$1", pb.host(), pb.port());
              }, ",");
          new_status = Status::ConfigurationError(Substitute(
              "no leader master found. Client configured with $0 master(s) ($1) "
              "but cluster indicates it expects $2 master(s) ($3)",
              addrs_with_names_.size(), client_config,
              resp.master_addrs_size(), cluster_config));
        } else {
          // Use a Status::NotFound() to indicate that the node is not
          // the leader: this way, we can handle the case where we've
          // received a reply from all of the nodes in the cluster (no
          // network or other errors encountered), but haven't found a
          // leader (which means that SendRpcCb() above can perform a
          // delayed retry).
          new_status = Status::NotFound("no leader found", ToString());
        }
      } else {
        // We've found a leader.
        leader_idx_ = master_idx;
        completed_ = true;
      }
    }
    --pending_responses_;
    if (!new_status.ok()) {
      if (pending_responses_ > 0) {
        // Don't call SendRpcCb() on error unless we're the last
        // outstanding response: calling SendRpcCb() will trigger
        // a delayed re-try, which don't need to do unless we've
        // been unable to find a leader so far.
        return;
      }
      completed_ = true;
    }
  }
  // Called if the leader has been determined, or if we've received
  // all of the responses.
  SendRpcCb(new_status);
}

} // namespace internal
} // namespace client
} // namespace kudu
