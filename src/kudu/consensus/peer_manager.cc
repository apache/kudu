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

#include "kudu/consensus/peer_manager.h"

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/multi_raft_batcher.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"

using kudu::log::Log;
using kudu::pb_util::SecureShortDebugString;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace consensus {

PeerManager::PeerManager(string tablet_id,
                         string local_uuid,
                         PeerProxyFactory* peer_proxy_factory,
                         PeerMessageQueue* queue,
                         consensus::MultiRaftManager* multi_raft_manager,
                         ThreadPoolToken* raft_pool_token,
                         scoped_refptr<log::Log> log)
    : tablet_id_(std::move(tablet_id)),
      local_uuid_(std::move(local_uuid)),
      peer_proxy_factory_(peer_proxy_factory),
      queue_(queue),
      raft_pool_token_(raft_pool_token),
      log_(std::move(log)),
      multi_raft_manager_(multi_raft_manager) {
}

PeerManager::~PeerManager() {
  Close();
}

void PeerManager::UpdateRaftConfig(const RaftConfigPB& config) {
  VLOG(1) << "Updating peers from new config: " << SecureShortDebugString(config);

  std::lock_guard lock(lock_);
  // Create new peers
  for (const RaftPeerPB& peer_pb : config.peers()) {
    if (ContainsKey(peers_, peer_pb.permanent_uuid())) {
      continue;
    }
    if (peer_pb.permanent_uuid() == local_uuid_) {
      continue;
    }

    VLOG(1) << GetLogPrefix() << "Adding remote peer. Peer: " << SecureShortDebugString(peer_pb);
    shared_ptr<Peer> remote_peer;
    std::shared_ptr<MultiRaftHeartbeatBatcher> multi_raft_batcher = nullptr;
    if (multi_raft_manager_) {
      multi_raft_batcher = multi_raft_manager_->AddOrGetBatcher(peer_pb);
    }
    Peer::NewRemotePeer(peer_pb,
                        tablet_id_,
                        local_uuid_,
                        queue_,
                        multi_raft_batcher,
                        raft_pool_token_,
                        peer_proxy_factory_,
                        &remote_peer);
    peers_.emplace(peer_pb.permanent_uuid(), std::move(remote_peer));
  }
}

void PeerManager::SignalRequest(bool force_if_queue_empty) {
  std::lock_guard lock(lock_);
  for (auto iter = peers_.begin(); iter != peers_.end();) {
    const auto s = iter->second->SignalRequest(force_if_queue_empty);
    if (PREDICT_TRUE(s.ok())) {
      ++iter;
      continue;
    }
    const auto& peer_info = SecureShortDebugString(iter->second->peer_pb());
    if (!s.IsIllegalState()) {
      WARN_NOT_OK(s, Substitute("$0: SignalRequest failed at peer $1",
                                GetLogPrefix(), peer_info));
    } else {
      WARN_NOT_OK(s, Substitute("$0: SignalRequest failed, removing peer $1",
                                GetLogPrefix(), peer_info));
      peers_.erase(iter++);
    }
  }
}

Status PeerManager::StartElection(const string& uuid) {
  shared_ptr<Peer> peer;
  {
    std::lock_guard lock(lock_);
    peer = FindPtrOrNull(peers_, uuid);
  }
  if (!peer) {
    return Status::NotFound("unknown peer");
  }
  peer->StartElection();
  return Status::OK();
}

void PeerManager::Close() {
  std::lock_guard lock(lock_);
  for (const auto& entry : peers_) {
    entry.second->Close();
  }
  peers_.clear();
}

string PeerManager::GetLogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, local_uuid_);
}

} // namespace consensus
} // namespace kudu
