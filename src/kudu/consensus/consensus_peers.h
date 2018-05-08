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

#ifndef KUDU_CONSENSUS_CONSENSUS_PEERS_H_
#define KUDU_CONSENSUS_CONSENSUS_PEERS_H_

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/locks.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {
class ThreadPoolToken;

namespace rpc {
class Messenger;
class PeriodicTimer;
}

namespace consensus {
class PeerMessageQueue;
class PeerProxy;

// A remote peer in consensus.
//
// Leaders use peers to update the remote replicas. Each peer
// may have at most one outstanding request at a time. If a
// request is signaled when there is already one outstanding,
// the request will be generated once the outstanding one finishes.
//
// Peers are owned by the consensus implementation and do not keep
// state aside from the most recent request and response.
//
// Peers are also responsible for sending periodic heartbeats
// to assert liveness of the leader. The peer constructs a heartbeater
// thread to trigger these heartbeats.
//
// The actual request construction is delegated to a PeerMessageQueue
// object, and performed on a thread pool (since it may do IO). When a
// response is received, the peer updates the PeerMessageQueue
// using PeerMessageQueue::ResponseFromPeer(...) on the same thread pool.
class Peer : public std::enable_shared_from_this<Peer> {
 public:
  // Initializes a peer and start sending periodic heartbeats.
  Status Init();

  // Signals that this peer has a new request to replicate/store.
  // 'even_if_queue_empty' indicates whether the peer should force
  // send the request even if the queue is empty. This is used for
  // status-only requests.
  Status SignalRequest(bool even_if_queue_empty = false);

  const RaftPeerPB& peer_pb() const { return peer_pb_; }

  // Stop sending requests and periodic heartbeats.
  //
  // This does not block waiting on any current outstanding requests to finish.
  // However, when they do finish, the results will be disregarded, so this
  // is safe to call at any point.
  //
  // This method must be called before the Peer's associated ThreadPoolToken
  // is destructed. Once this method returns, it is safe to destruct
  // the ThreadPoolToken.
  void Close();

  ~Peer();

  // Creates a new remote peer and makes the queue track it.'
  //
  // Requests to this peer (which may end up doing IO to read non-cached
  // log entries) are assembled on 'raft_pool_token'.
  // Response handling may also involve IO related to log-entry lookups and is
  // also done on 'raft_pool_token'.
  static Status NewRemotePeer(RaftPeerPB peer_pb,
                              std::string tablet_id,
                              std::string leader_uuid,
                              PeerMessageQueue* queue,
                              ThreadPoolToken* raft_pool_token,
                              gscoped_ptr<PeerProxy> proxy,
                              std::shared_ptr<rpc::Messenger> messenger,
                              std::shared_ptr<Peer>* peer);

 private:
  Peer(RaftPeerPB peer_pb,
       std::string tablet_id,
       std::string leader_uuid,
       PeerMessageQueue* queue,
       ThreadPoolToken* raft_pool_token,
       gscoped_ptr<PeerProxy> proxy,
       std::shared_ptr<rpc::Messenger> messenger);

  void SendNextRequest(bool even_if_queue_empty);

  // Signals that a response was received from the peer.
  //
  // This method is called from the reactor thread and calls
  // DoProcessResponse() on raft_pool_token_ to do any work that requires IO or
  // lock-taking.
  void ProcessResponse();

  // Run on 'raft_pool_token'. Does response handling that requires IO or may block.
  void DoProcessResponse();

  // Fetch the desired tablet copy request from the queue and set up
  // tc_request_ appropriately.
  //
  // Returns a bad Status if tablet copy is disabled, or if the
  // request cannot be generated for some reason.
  Status PrepareTabletCopyRequest();

  // Handle RPC callback from initiating tablet copy.
  void ProcessTabletCopyResponse();

  // Signals there was an error sending the request to the peer.
  void ProcessResponseError(const Status& status);

  std::string LogPrefixUnlocked() const;

  const std::string& tablet_id() const { return tablet_id_; }

  const std::string tablet_id_;
  const std::string leader_uuid_;

  RaftPeerPB peer_pb_;

  gscoped_ptr<PeerProxy> proxy_;

  PeerMessageQueue* queue_;
  uint64_t failed_attempts_;

  // The latest consensus update request and response.
  ConsensusRequestPB request_;
  ConsensusResponsePB response_;

  // The latest tablet copy request and response.
  StartTabletCopyRequestPB tc_request_;
  StartTabletCopyResponsePB tc_response_;

  // Reference-counted pointers to any ReplicateMsgs which are in-flight to the peer. We
  // may have loaded these messages from the LogCache, in which case we are potentially
  // sharing the same object as other peers. Since the PB request_ itself can't hold
  // reference counts, this holds them.
  std::vector<ReplicateRefPtr> replicate_msg_refs_;

  rpc::RpcController controller_;

  std::shared_ptr<rpc::Messenger> messenger_;

  // Thread pool token used to construct requests to this peer.
  //
  // RaftConsensus owns this shared token and is responsible for destroying it.
  ThreadPoolToken* raft_pool_token_;

  // Repeating timer responsible for scheduling heartbeats to this peer.
  std::shared_ptr<rpc::PeriodicTimer> heartbeater_;

  // lock that protects Peer state changes, initialization, etc.
  mutable simple_spinlock peer_lock_;
  bool request_pending_ = false;
  bool closed_ = false;
  bool has_sent_first_request_ = false;

};

// A proxy to another peer. Usually a thin wrapper around an rpc proxy but can
// be replaced for tests.
class PeerProxy {
 public:
  virtual ~PeerProxy() {}

  // Sends a request, asynchronously, to a remote peer.
  virtual void UpdateAsync(const ConsensusRequestPB* request,
                           ConsensusResponsePB* response,
                           rpc::RpcController* controller,
                           const rpc::ResponseCallback& callback) = 0;

  // Sends a RequestConsensusVote to a remote peer.
  virtual void RequestConsensusVoteAsync(const VoteRequestPB* request,
                                         VoteResponsePB* response,
                                         rpc::RpcController* controller,
                                         const rpc::ResponseCallback& callback) = 0;

  // Instructs a peer to begin a tablet copy session.
  virtual void StartTabletCopy(const StartTabletCopyRequestPB* request,
                                    StartTabletCopyResponsePB* response,
                                    rpc::RpcController* controller,
                                    const rpc::ResponseCallback& callback) {
    LOG(DFATAL) << "Not implemented";
  }

  // Remote endpoint or description of the peer.
  virtual std::string PeerName() const = 0;
};

// A peer proxy factory. Usually just obtains peers through the rpc implementation
// but can be replaced for tests.
class PeerProxyFactory {
 public:

  virtual Status NewProxy(const RaftPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) = 0;

  virtual ~PeerProxyFactory() {}

  virtual const std::shared_ptr<rpc::Messenger>& messenger() const = 0;
};

// PeerProxy implementation that does RPC calls
class RpcPeerProxy : public PeerProxy {
 public:
  RpcPeerProxy(gscoped_ptr<HostPort> hostport,
               gscoped_ptr<ConsensusServiceProxy> consensus_proxy);

  void UpdateAsync(const ConsensusRequestPB* request,
                   ConsensusResponsePB* response,
                   rpc::RpcController* controller,
                   const rpc::ResponseCallback& callback) override;

  void RequestConsensusVoteAsync(const VoteRequestPB* request,
                                 VoteResponsePB* response,
                                 rpc::RpcController* controller,
                                 const rpc::ResponseCallback& callback) override;

  void StartTabletCopy(const StartTabletCopyRequestPB* request,
                       StartTabletCopyResponsePB* response,
                       rpc::RpcController* controller,
                       const rpc::ResponseCallback& callback) override;

  std::string PeerName() const override;

 private:
  gscoped_ptr<HostPort> hostport_;
  gscoped_ptr<ConsensusServiceProxy> consensus_proxy_;
};

// PeerProxyFactory implementation that generates RPCPeerProxies
class RpcPeerProxyFactory : public PeerProxyFactory {
 public:
  explicit RpcPeerProxyFactory(std::shared_ptr<rpc::Messenger> messenger);

  Status NewProxy(const RaftPeerPB& peer_pb,
                  gscoped_ptr<PeerProxy>* proxy) override;

  ~RpcPeerProxyFactory();

  const std::shared_ptr<rpc::Messenger>& messenger() const override {
    return messenger_;
  }

 private:
  std::shared_ptr<rpc::Messenger> messenger_;
};

// Query the consensus service at last known host/port that is
// specified in 'remote_peer' and set the 'permanent_uuid' field based
// on the response.
Status SetPermanentUuidForRemotePeer(const std::shared_ptr<rpc::Messenger>& messenger,
                                     RaftPeerPB* remote_peer);

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_PEERS_H_ */
