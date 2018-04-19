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
#include <mutex>
#include <ostream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"

using kudu::log::Log;
using kudu::pb_util::SecureShortDebugString;
using strings::Substitute;

namespace kudu {
namespace consensus {

PeerManager::PeerManager(std::string tablet_id,
                         std::string local_uuid,
                         PeerProxyFactory* peer_proxy_factory,
                         PeerMessageQueue* queue,
                         ThreadPoolToken* raft_pool_token,
                         const scoped_refptr<log::Log>& log)
    : tablet_id_(std::move(tablet_id)),
      local_uuid_(std::move(local_uuid)),
      peer_proxy_factory_(peer_proxy_factory),
      queue_(queue),
      raft_pool_token_(raft_pool_token),
      log_(log) {
}

PeerManager::~PeerManager() {
  Close();
}

Status PeerManager::UpdateRaftConfig(const RaftConfigPB& config) {
  VLOG(1) << "Updating peers from new config: " << SecureShortDebugString(config);

  std::lock_guard<simple_spinlock> lock(lock_);
  // Create new peers
  for (const RaftPeerPB& peer_pb : config.peers()) {
    if (ContainsKey(peers_, peer_pb.permanent_uuid())) {
      continue;
    }
    if (peer_pb.permanent_uuid() == local_uuid_) {
      continue;
    }

    VLOG(1) << GetLogPrefix() << "Adding remote peer. Peer: " << SecureShortDebugString(peer_pb);
    gscoped_ptr<PeerProxy> peer_proxy;
    RETURN_NOT_OK_PREPEND(peer_proxy_factory_->NewProxy(peer_pb, &peer_proxy),
                          "Could not obtain a remote proxy to the peer.");

    std::shared_ptr<Peer> remote_peer;
    RETURN_NOT_OK(Peer::NewRemotePeer(peer_pb,
                                      tablet_id_,
                                      local_uuid_,
                                      queue_,
                                      raft_pool_token_,
                                      std::move(peer_proxy),
                                      peer_proxy_factory_->messenger(),
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
                   << SecureShortDebugString((*iter).second->peer_pb());
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
