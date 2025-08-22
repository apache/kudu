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

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"

namespace kudu {
class DnsResolver;
class MetricEntity;
class ThreadPool;
class ThreadPoolToken;

namespace rpc {
class Messenger;
class PeriodicTimer;
class RpcController;
}  // namespace rpc

namespace consensus {
class BatchedNoOpConsensusResponsePB;
class MultiRaftConsensusResponsePB;
class RaftPeerPB;

typedef std::unique_ptr<ConsensusServiceProxy> ConsensusServiceProxyPtr;

using HeartbeatResponseCallback = std::function<void(const rpc::RpcController&,
                                                     const MultiRaftConsensusResponsePB&,
                                                     const BatchedNoOpConsensusResponsePB*)>;

// - MultiRaftHeartbeatBatcher is responsible for batching the processing
//  and sending of no-op heartbeats, saving cpu and network resources.
// - Peers can request (using Subscribe) to be called back on a
//   {raft_heartbeat_interval_ms +/- multi_raft_heartbeat_window_ms} interval,
//   together with other peers that are connected to the same host.
//   Average interval over a long time will be raft_heartbeat_interval_ms.
// - This will allow for less context switching and a bit smaller network
//   overhead, as some fields can be shared, and rpc calls can be batched.
// - The subscribed peers will be called back with the a MultiRaftConsensusData
//   object, and it is the responsibility of the called peer to:
//     + Correctly append to the MultiRaftConsensusData object.
//     + Fill the shared fields if needed.
//     + If it would do anything with significant cpu usage (pending items in the queue),
//       it should submit an other task to the raft pool, and return early, not to
//       block the other peers in the same batch.
struct MultiRaftConsensusData;

class MultiRaftHeartbeatBatcher : public std::enable_shared_from_this<MultiRaftHeartbeatBatcher> {
 public:
  MultiRaftHeartbeatBatcher(const kudu::HostPort& hostport,
                            ::kudu::DnsResolver* dns_resolver,
                            std::shared_ptr<rpc::Messenger> messenger,
                            MonoDelta flush_interval,
                            std::shared_ptr<ThreadPoolToken> raft_pool_token);

  ~MultiRaftHeartbeatBatcher() = default;

  using PeriodicHeartbeater = std::function<void(MultiRaftConsensusData*)>;
  // Subscribe for periodic callbacks batched with other peers.
  // Returns a unique id for the peer, which can be used to unsubscribe.
  uint64_t Subscribe(const PeriodicHeartbeater& heartbeater);
  void Unsubscribe(uint64_t id);

 private:
  friend class MultiRaftManager;

  // Collect the heartbeats due in the next flush interval.
  // Submit a task to the Raft pool to send them in a single RPC call per batch.
  void PrepareNextBatch();

  void Shutdown();

  void StartTimer();

  void MultiRaftUpdateHeartbeatResponseCallback(std::shared_ptr<MultiRaftConsensusData> data);

  void SendOutScheduled(const std::vector<PeriodicHeartbeater>& scheduled_callbacks);

  std::shared_ptr<rpc::PeriodicTimer> heartbeat_timer_;
  std::shared_ptr<rpc::Messenger> messenger_;

  ConsensusServiceProxyPtr consensus_proxy_;

  uint64_t next_id_ = 1;
  std::unordered_map<int, PeriodicHeartbeater> peers_;
  // Next required heartbeat time for a peer. queue_ is ordered by time.
  // After calling a peer, push it to the end of the queue with its next due time.
  // Since all callbacks share the same period (raft_heartbeat_interval_ms),
  // a simple deque is sufficient and faster than a priority queue.
  struct Callback {
    MonoTime time; // Time for the next heartbeat for this peer.
    uint64_t id; // id of the peer inside peers_.
  };
  std::deque<Callback> queue_;
  // Protects queue_ and peers_.
  // Peers might subscribe concurrently, and PrepareNextBatch also uses both of
  // these members.
  std::mutex heartbeater_lock_;

  const MonoDelta batch_time_window_;
  std::shared_ptr<ThreadPoolToken> raft_pool_token_;
  bool closed_;
};

using MultiRaftHeartbeatBatcherPtr = std::shared_ptr<MultiRaftHeartbeatBatcher>;

// MultiRaftManager is responsible for managing all MultiRaftHeartbeatBatchers
// for a given TServer (utilizes a mapping between a HostPort and the corresponding batcher).
// MultiRaftManager allows multiple peers to share the same batcher
// if they are connected to the same remote host.
class MultiRaftManager : public std::enable_shared_from_this<MultiRaftManager> {
 public:
  MultiRaftManager(kudu::DnsResolver* dns_resolver, const scoped_refptr<MetricEntity>& entity);
  ~MultiRaftManager() = default;

  void Init(const std::shared_ptr<rpc::Messenger>& messenger, ThreadPool* raft_pool);

  void Shutdown();

  MultiRaftHeartbeatBatcherPtr AddOrGetBatcher(const kudu::consensus::RaftPeerPB& remote_peer_pb);

 private:
  std::shared_ptr<rpc::Messenger> messenger_;

  kudu::DnsResolver* dns_resolver_;

  ThreadPool* raft_pool_ = nullptr;

  // Protects raft_pool_ and batchers_ during
  // concurrent calls of AddOrGetBatcher.
  std::mutex mutex_;

  // Uses a weak_ptr to allow deallocation of unused batchers once no more
  // consensus peers use them (and to stop the periodic timer).
  // The MultiRaftHeartbeatBatcher destructor might be called on the raft pool
  // using the same token. It is not safe to destruct the token there, so we
  // keep it alive separately. Keeping one unused token per decommissioned host
  // until restart should not cause any problems.
  // TODO (martonka): Create a periodic timer to clean up tokens for dead hosts.
  // Unless the cluster goes through millions of servers without restarting this
  // TServer, this is not a problem. However, running a periodic cleanup timer
  // (once per hour/day/week is enough) would be nice.
  struct BatcherAndPoolToken {
    std::weak_ptr<MultiRaftHeartbeatBatcher> batcher;
    std::shared_ptr<ThreadPoolToken> raft_pool_token;
    BatcherAndPoolToken(std::shared_ptr<MultiRaftHeartbeatBatcher> b,
                  std::shared_ptr<ThreadPoolToken> t)
        : batcher(b), raft_pool_token(std::move(t)) {}
  };
  std::unordered_map<HostPort, BatcherAndPoolToken, HostPortHasher> batchers_;

  const MonoDelta batch_time_window_;

  bool closed_;
};

}  // namespace consensus
}  // namespace kudu
