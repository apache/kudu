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

#include <mutex>

#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace consensus {

using log::Log;
using strings::Substitute;

PeerManager::PeerManager(std::string tablet_id,
                         std::string local_uuid,
                         PeerProxyFactory* peer_proxy_factory,
                         PeerMessageQueue* queue,
                         ThreadPool* request_thread_pool,
                         const scoped_refptr<log::Log>& log)
    : tablet_id_(std::move(tablet_id)),
      local_uuid_(std::move(local_uuid)),
      peer_proxy_factory_(peer_proxy_factory),
      queue_(queue),
      thread_pool_(request_thread_pool),
      log_(log) {
}

PeerManager::~PeerManager() {
  Close();
}

Status PeerManager::UpdateRaftConfig(const RaftConfigPB& config) {
  VLOG(1) << "Updating peers from new config: " << config.ShortDebugString();

  std::lock_guard<simple_spinlock> lock(lock_);
  // Create new peers
  for (const RaftPeerPB& peer_pb : config.peers()) {
    if (ContainsKey(peers_, peer_pb.permanent_uuid())) {
      continue;
    }
    if (peer_pb.permanent_uuid() == local_uuid_) {
      continue;
    }

    VLOG(1) << GetLogPrefix() << "Adding remote peer. Peer: " << peer_pb.ShortDebugString();
    gscoped_ptr<PeerProxy> peer_proxy;
    RETURN_NOT_OK_PREPEND(peer_proxy_factory_->NewProxy(peer_pb, &peer_proxy),
                          "Could not obtain a remote proxy to the peer.");

    std::shared_ptr<Peer> remote_peer;
    RETURN_NOT_OK(Peer::NewRemotePeer(peer_pb,
                                      tablet_id_,
                                      local_uuid_,
                                      queue_,
                                      thread_pool_,
                                      std::move(peer_proxy),
                                      &remote_peer));
    peers_.emplace(peer_pb.permanent_uuid(), std::move(remote_peer));
  }

  return Status::OK();
}

void PeerManager::SignalRequest(bool force_if_queue_empty) {
  std::lock_guard<simple_spinlock> lock(lock_);
  for (auto iter = peers_.begin(); iter != peers_.end();) {
    Status s = (*iter).second->SignalRequest(force_if_queue_empty);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << GetLogPrefix()
                   << "Peer was closed, removing from peers. Peer: "
                   << (*iter).second->peer_pb().ShortDebugString();
      peers_.erase(iter++);
    } else {
      ++iter;
    }
  }
}

void PeerManager::Close() {
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    for (const auto& entry : peers_) {
      entry.second->Close();
    }
    peers_.clear();
  }
}

std::string PeerManager::GetLogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, local_uuid_);
}

} // namespace consensus
} // namespace kudu
