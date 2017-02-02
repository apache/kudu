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

#include <boost/bind.hpp>
#include <mutex>

#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.proxy.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"


using std::shared_ptr;
using std::string;
using std::vector;

using kudu::consensus::RaftPeerPB;
using kudu::master::GetMasterRegistrationRequestPB;
using kudu::master::GetMasterRegistrationResponsePB;
using kudu::master::MasterErrorPB;
using kudu::master::MasterServiceProxy;
using kudu::rpc::Messenger;
using kudu::rpc::Rpc;

namespace kudu {
namespace client {
namespace internal {

////////////////////////////////////////////////////////////
// GetMasterRegistrationRpc
////////////////////////////////////////////////////////////
namespace {

// An RPC for getting a Master server's registration.
class GetMasterRegistrationRpc : public rpc::Rpc {
 public:

  // Create a wrapper object for a retriable GetMasterRegistration RPC
  // to 'addr'. The result is stored in 'out', which must be a valid
  // pointer for the lifetime of this object.
  //
  // Invokes 'user_cb' upon failure or success of the RPC call.
  GetMasterRegistrationRpc(StatusCallback user_cb,
                           const Sockaddr& addr,
                           const MonoTime& deadline,
                           std::shared_ptr<rpc::Messenger> messenger,
                           ServerEntryPB* out);

  ~GetMasterRegistrationRpc();

  virtual void SendRpc() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  StatusCallback user_cb_;
  Sockaddr addr_;

  ServerEntryPB* out_;

  GetMasterRegistrationResponsePB resp_;
};


GetMasterRegistrationRpc::GetMasterRegistrationRpc(
    StatusCallback user_cb, const Sockaddr& addr, const MonoTime& deadline,
    shared_ptr<Messenger> messenger, ServerEntryPB* out)
    : Rpc(deadline, std::move(messenger)),
      user_cb_(std::move(user_cb)),
      addr_(addr),
      out_(DCHECK_NOTNULL(out)) {}

GetMasterRegistrationRpc::~GetMasterRegistrationRpc() {
}

void GetMasterRegistrationRpc::SendRpc() {
  MasterServiceProxy proxy(retrier().messenger(),
                           addr_);
  GetMasterRegistrationRequestPB req;
  proxy.GetMasterRegistrationAsync(req, &resp_,
                                   mutable_retrier()->mutable_controller(),
                                   boost::bind(&GetMasterRegistrationRpc::SendRpcCb,
                                               this,
                                               Status::OK()));
}

string GetMasterRegistrationRpc::ToString() const {
  return strings::Substitute("GetMasterRegistrationRpc(address: $0, num_attempts: $1)",
                             addr_.ToString(), num_attempts());
}

void GetMasterRegistrationRpc::SendRpcCb(const Status& status) {
  gscoped_ptr<GetMasterRegistrationRpc> deleter(this);
  Status new_status = status;
  if (new_status.ok() && mutable_retrier()->HandleResponse(this, &new_status)) {
    ignore_result(deleter.release());
    return;
  }
  if (new_status.ok() && resp_.has_error()) {
    if (resp_.error().code() == MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
      // If CatalogManager is not initialized, treat the node as a
      // FOLLOWER for the time being, as currently this RPC is only
      // used for the purposes of finding the leader master.
      resp_.set_role(RaftPeerPB::FOLLOWER);
      new_status = Status::OK();
    } else {
      out_->mutable_error()->CopyFrom(resp_.error().status());
      new_status = StatusFromPB(resp_.error().status());
    }
  }
  if (new_status.ok()) {
    out_->mutable_instance_id()->CopyFrom(resp_.instance_id());
    out_->mutable_registration()->CopyFrom(resp_.registration());
    out_->set_role(resp_.role());
  }
  user_cb_.Run(new_status);
}

} // anonymous namespace

////////////////////////////////////////////////////////////
// GetLeaderMasterRpc
////////////////////////////////////////////////////////////

GetLeaderMasterRpc::GetLeaderMasterRpc(LeaderCallback user_cb,
                                       vector<Sockaddr> addrs,
                                       MonoTime deadline,
                                       MonoDelta rpc_timeout,
                                       shared_ptr<Messenger> messenger)
    : Rpc(deadline, std::move(messenger)),
      user_cb_(std::move(user_cb)),
      addrs_(std::move(addrs)),
      rpc_timeout_(rpc_timeout),
      pending_responses_(0),
      completed_(false) {
  DCHECK(deadline.Initialized());

  // Using resize instead of reserve to explicitly initialized the values.
  responses_.resize(addrs_.size());
}

GetLeaderMasterRpc::~GetLeaderMasterRpc() {
}

string GetLeaderMasterRpc::ToString() const {
  vector<string> sockaddr_str;
  for (const Sockaddr& addr : addrs_) {
    sockaddr_str.push_back(addr.ToString());
  }
  return strings::Substitute("GetLeaderMasterRpc(addrs: $0, num_attempts: $1)",
                             JoinStrings(sockaddr_str, ","),
                             num_attempts());
}

void GetLeaderMasterRpc::SendRpc() {
  // Compute the actual deadline to use for each RPC.
  MonoTime rpc_deadline = MonoTime::Now() + rpc_timeout_;
  MonoTime actual_deadline = MonoTime::Earliest(retrier().deadline(),
                                                rpc_deadline);

  std::lock_guard<simple_spinlock> l(lock_);
  for (int i = 0; i < addrs_.size(); i++) {
    GetMasterRegistrationRpc* rpc = new GetMasterRegistrationRpc(
        Bind(&GetLeaderMasterRpc::GetMasterRegistrationRpcCbForNode,
             this, ConstRef(addrs_[i]), ConstRef(responses_[i])),
        addrs_[i],
        actual_deadline,
        retrier().messenger(),
        &responses_[i]);
    rpc->SendRpc();
    ++pending_responses_;
  }
}

void GetLeaderMasterRpc::SendRpcCb(const Status& status) {
  // To safely retry, we must reset completed_ so that it can be reused in the
  // next round of RPCs.
  //
  // The SendRpcCb invariant (see GetMasterRegistrationRpcCbForNode comments)
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
  user_cb_.Run(status, leader_master_);
}

void GetLeaderMasterRpc::GetMasterRegistrationRpcCbForNode(const Sockaddr& node_addr,
                                                           const ServerEntryPB& resp,
                                                           const Status& status) {
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
        leader_master_ = HostPort(node_addr);
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
