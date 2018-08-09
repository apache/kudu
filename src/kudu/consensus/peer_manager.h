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
#ifndef KUDU_CONSENSUS_PEER_MANAGER_H
#define KUDU_CONSENSUS_PEER_MANAGER_H

#include <memory>
#include <string>
#include <unordered_map>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

class ThreadPoolToken;

namespace log {
class Log;
} // namespace log

namespace consensus {

class Peer;
class PeerMessageQueue;
class PeerProxyFactory;
class RaftConfigPB;

// Manages the remote peers that pull data from the local queue and send updates to the
// remote machines.
class PeerManager {
 public:
  // All of the raw pointer arguments are not owned by the PeerManager
  // and must live at least as long as the PeerManager.
  PeerManager(std::string tablet_id,
              std::string local_uuid,
              PeerProxyFactory* peer_proxy_factory,
              PeerMessageQueue* queue,
              ThreadPoolToken* raft_pool_token,
              scoped_refptr<log::Log> log);

  ~PeerManager();

  // Updates 'peers_' according to the new configuration config.
  Status UpdateRaftConfig(const RaftConfigPB& config);

  // Signals all peers of the current configuration that there is a new request pending.
  void SignalRequest(bool force_if_queue_empty = false);

  // Start an election on the peer with UUID 'uuid'.
  Status StartElection(const std::string& uuid);

  // Closes all peers.
  void Close();

 private:
  std::string GetLogPrefix() const;

  typedef std::unordered_map<std::string, std::shared_ptr<Peer>> PeersMap;
  const std::string tablet_id_;
  const std::string local_uuid_;
  PeerProxyFactory* peer_proxy_factory_;
  PeerMessageQueue* queue_;
  ThreadPoolToken* raft_pool_token_;
  scoped_refptr<log::Log> log_;
  PeersMap peers_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(PeerManager);
};



} // namespace consensus
} // namespace kudu
#endif /* KUDU_CONSENSUS_PEER_MANAGER_H */
