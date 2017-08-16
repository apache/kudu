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
#include <mutex>
#include <ostream>

#include <boost/bind.hpp>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using std::shared_ptr;
using std::string;
using std::vector;

using kudu::consensus::RaftPeerPB;
using kudu::master::ConnectToMasterRequestPB;
using kudu::master::ConnectToMasterResponsePB;
using kudu::master::GetMasterRegistrationRequestPB;
using kudu::master::GetMasterRegistrationResponsePB;
using kudu::master::MasterErrorPB;
using kudu::master::MasterServiceProxy;
using kudu::rpc::CredentialsPolicy;
using kudu::rpc::Messenger;
using kudu::rpc::Rpc;

namespace kudu {
namespace client {
namespace internal {

////////////////////////////////////////////////////////////
// GetMasterRegistrationRpc
////////////////////////////////////////////////////////////
namespace {

// An RPC for trying to connect via a particular Master.
class ConnectToMasterRpc : public rpc::Rpc {
 public:

  // Create a wrapper object for a retriable ConnectToMaster RPC
  // to 'addr'. The result is stored in 'out', which must be a valid
  // pointer for the lifetime of this object.
  //
  // Invokes 'user_cb' upon failure or success of the RPC call.
  ConnectToMasterRpc(StatusCallback user_cb,
                     const Sockaddr& addr,
                     const MonoTime& deadline,
                     std::shared_ptr<rpc::Messenger> messenger,
                     CredentialsPolicy creds_policy,
                     ConnectToMasterResponsePB* out);

  ~ConnectToMasterRpc();

  virtual void SendRpc() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  const StatusCallback user_cb_;
  const Sockaddr addr_;

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
    const Sockaddr& addr,
    const MonoTime& deadline,
    shared_ptr<Messenger> messenger,
    rpc::CredentialsPolicy creds_policy,
    ConnectToMasterResponsePB* out)
      : Rpc(deadline, std::move(messenger)),
        user_cb_(std::move(user_cb)),
        addr_(addr),
        out_(DCHECK_NOTNULL(out)) {
  mutable_retrier()->mutable_controller()->set_credentials_policy(creds_policy);
}

ConnectToMasterRpc::~ConnectToMasterRpc() {
}

void ConnectToMasterRpc::SendRpc() {
  // TODO(KUDU-2032): retain the hostname for addr_
  MasterServiceProxy proxy(retrier().messenger(), addr_, addr_.host());
  rpc::RpcController* controller = mutable_retrier()->mutable_controller();
  // TODO(todd): should this be setting an RPC call deadline based on 'deadline'?
  // it doesn't seem to be.
  if (!trying_old_rpc_) {
    ConnectToMasterRequestPB req;
    controller->RequireServerFeature(master::MasterFeatures::CONNECT_TO_MASTER);
    proxy.ConnectToMasterAsync(req, out_, controller,
                               boost::bind(&ConnectToMasterRpc::SendRpcCb,
                                           this,
                                           Status::OK()));
  } else {
    GetMasterRegistrationRequestPB req;
    proxy.GetMasterRegistrationAsync(req, &old_rpc_resp_, controller,
                                     boost::bind(&ConnectToMasterRpc::SendRpcCb,
                                                 this,
                                                 Status::OK()));
  }
}

string ConnectToMasterRpc::ToString() const {
  return strings::Substitute("ConnectToMasterRpc(address: $0, num_attempts: $1)",
                             addr_.ToString(), num_attempts());
}

void ConnectToMasterRpc::SendRpcCb(const Status& status) {
  // NOTE: 'status' here is actually coming from the RpcRetrier. If we successfully
  // send an RPC, it will be 'Status::OK'.
  // TODO(todd): this is the most confusing code I've ever seen...
  gscoped_ptr<ConnectToMasterRpc> deleter(this);
  Status new_status = status;
  if (new_status.ok() && mutable_retrier()->HandleResponse(this, &new_status)) {
    ignore_result(deleter.release());
    return;
  }

  rpc::RpcController* rpc = mutable_retrier()->mutable_controller();
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
  user_cb_.Run(new_status);
}

} // anonymous namespace

////////////////////////////////////////////////////////////
// ConnectToClusterRpc
////////////////////////////////////////////////////////////

ConnectToClusterRpc::ConnectToClusterRpc(LeaderCallback user_cb,
                                         vector<Sockaddr> addrs,
                                         MonoTime deadline,
                                         MonoDelta rpc_timeout,
                                         shared_ptr<Messenger> messenger,
                                         rpc::CredentialsPolicy creds_policy)
    : Rpc(deadline, std::move(messenger)),
      user_cb_(std::move(user_cb)),
      addrs_(std::move(addrs)),
      rpc_timeout_(rpc_timeout),
      pending_responses_(0),
      completed_(false) {
  DCHECK(deadline.Initialized());

  // Using resize instead of reserve to explicitly initialized the values.
  responses_.resize(addrs_.size());
  mutable_retrier()->mutable_controller()->set_credentials_policy(creds_policy);
}

ConnectToClusterRpc::~ConnectToClusterRpc() {
}

string ConnectToClusterRpc::ToString() const {
  vector<string> sockaddr_str;
  for (const Sockaddr& addr : addrs_) {
    sockaddr_str.push_back(addr.ToString());
  }
  return strings::Substitute("ConnectToClusterRpc(addrs: $0, num_attempts: $1)",
                             JoinStrings(sockaddr_str, ","),
                             num_attempts());
}

void ConnectToClusterRpc::SendRpc() {
  // Compute the actual deadline to use for each RPC.
  const MonoTime rpc_deadline = MonoTime::Now() + rpc_timeout_;
  const MonoTime actual_deadline = std::min(retrier().deadline(), rpc_deadline);

  std::lock_guard<simple_spinlock> l(lock_);
  for (int i = 0; i < addrs_.size(); i++) {
    ConnectToMasterRpc* rpc = new ConnectToMasterRpc(
        Bind(&ConnectToClusterRpc::SingleNodeCallback, this, i),
        addrs_[i],
        actual_deadline,
        retrier().messenger(),
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
    user_cb_(status, addrs_[leader_idx_], responses_[leader_idx_]);
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
        // Use a Status::NotFound() to indicate that the node is not
        // the leader: this way, we can handle the case where we've
        // received a reply from all of the nodes in the cluster (no
        // network or other errors encountered), but haven't found a
        // leader (which means that SendRpcCb() above can perform a
        // delayed retry).
        new_status = Status::NotFound("no leader found: " + ToString());
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
