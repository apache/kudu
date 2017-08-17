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
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"

namespace kudu {

class Status;

namespace rpc {
class Messenger;
}

namespace client {
namespace internal {

// In parallel, send requests to the specified Master servers until a
// response comes back from the leader of the Master consensus configuration.
//
// In addition to locating the leader, this fetches cluster-wide details
// such as an authentication token for the current user, the cluster's
// CA cert, etc.
//
// If queries have been made to all of the specified servers, but no
// leader has been found, we re-try again (with an increasing delay,
// see: RpcRetrier in kudu/rpc/rpc.{cc,h}) until a specified deadline
// passes or we find a leader.
//
// The RPCs are sent in parallel in order to avoid prolonged delays on
// the client-side that would happen with a serial approach when one
// of the Master servers is slow or stopped (that is, when we have to
// wait for an RPC request to server N to timeout before we can make
// an RPC request to server N+1). This allows for true fault tolerance
// for the Kudu client.
//
// The class is reference counted to avoid a "use-after-free"
// scenario, when responses to the RPC return to the caller _after_ a
// leader has already been found.
class ConnectToClusterRpc : public rpc::Rpc,
                            public RefCountedThreadSafe<ConnectToClusterRpc> {
 public:
  typedef std::function<void(
      const Status& status,
      const std::pair<Sockaddr, std::string> leader_master,
      const master::ConnectToMasterResponsePB& connect_response)> LeaderCallback;
  // The host and port of the leader master server is stored in
  // 'leader_master', which must remain valid for the lifetime of this
  // object.
  //
  // Calls 'user_cb' when the leader is found, or if no leader can be found
  // until 'deadline' passes. Each RPC has 'rpc_timeout' time to complete
  // before it times out and may be retried if 'deadline' has not yet passed.
  ConnectToClusterRpc(LeaderCallback user_cb,
                      std::vector<std::pair<Sockaddr, std::string>> addrs_with_names,
                      MonoTime deadline,
                      MonoDelta rpc_timeout,
                      std::shared_ptr<rpc::Messenger> messenger,
                      rpc::CredentialsPolicy creds_policy =
      rpc::CredentialsPolicy::ANY_CREDENTIALS);

  virtual void SendRpc() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;
 private:
  friend class RefCountedThreadSafe<ConnectToClusterRpc>;
  ~ConnectToClusterRpc();

  virtual void SendRpcCb(const Status& status) OVERRIDE;

  // Invoked when a response comes back from the master with index
  // 'master_idx'.
  //
  // Invokes SendRpcCb if the response indicates that the specified
  // master is a leader, or if responses have been received from all
  // of the Masters.
  void SingleNodeCallback(int master_idx, const Status& status);

  const LeaderCallback user_cb_;

  // The addresses of the masters, along with their original specified names.
  const std::vector<std::pair<Sockaddr, std::string>> addrs_with_names_;

  // The amount of time alloted to each GetMasterRegistration RPC.
  const MonoDelta rpc_timeout_;

  // The received responses. The indexes correspond to 'addrs_'.
  std::vector<master::ConnectToMasterResponsePB> responses_;

  // Number of pending responses.
  int pending_responses_;

  // If true, then we've already executed the user callback and the
  // RPC can be deallocated.
  bool completed_;

  // The index of the master that was determined to be the leader.
  // This corresponds to entries in 'responses_' and 'addrs_'.
  // -1 indicates no leader found.
  int leader_idx_ = -1;

  // Protects 'pending_responses_' and 'completed_'.
  mutable simple_spinlock lock_;
};

} // namespace internal
} // namespace client
} // namespace kudu
