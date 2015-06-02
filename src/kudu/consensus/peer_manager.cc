// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/peer_manager.h"

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

PeerManager::PeerManager(const std::string tablet_id,
                         const std::string local_uuid,
                         PeerProxyFactory* peer_proxy_factory,
                         PeerMessageQueue* queue,
                         ThreadPool* request_thread_pool,
                         const scoped_refptr<log::Log>& log)
    : tablet_id_(tablet_id),
      local_uuid_(local_uuid),
      peer_proxy_factory_(peer_proxy_factory),
      queue_(queue),
      thread_pool_(request_thread_pool),
      log_(log) {
}

PeerManager::~PeerManager() {
  Close();
}

Status PeerManager::UpdateRaftConfig(const RaftConfigPB& config) {
  unordered_set<string> new_peers;

  VLOG(1) << "Updating peers from new config: " << config.ShortDebugString();

  boost::lock_guard<simple_spinlock> lock(lock_);
  // Create new peers
  BOOST_FOREACH(const RaftPeerPB& peer_pb, config.peers()) {
    new_peers.insert(peer_pb.permanent_uuid());
    Peer* peer = FindPtrOrNull(peers_, peer_pb.permanent_uuid());
    if (peer != NULL) {
      continue;
    }
    if (peer_pb.permanent_uuid() == local_uuid_) {
      continue;
    }

    VLOG(1) << GetLogPrefix() << "Adding remote peer. Peer: " << peer_pb.ShortDebugString();
    gscoped_ptr<PeerProxy> peer_proxy;
    RETURN_NOT_OK_PREPEND(peer_proxy_factory_->NewProxy(peer_pb, &peer_proxy),
                          "Could not obtain a remote proxy to the peer.");

    gscoped_ptr<Peer> remote_peer;
    RETURN_NOT_OK(Peer::NewRemotePeer(peer_pb,
                                      tablet_id_,
                                      local_uuid_,
                                      queue_,
                                      thread_pool_,
                                      peer_proxy.Pass(),
                                      &remote_peer));
    InsertOrDie(&peers_, peer_pb.permanent_uuid(), remote_peer.release());
  }

  return Status::OK();
}

void PeerManager::SignalRequest(bool force_if_queue_empty) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  PeersMap::iterator iter = peers_.begin();
    for (; iter != peers_.end(); iter++) {
      Status s = (*iter).second->SignalRequest(force_if_queue_empty);
      if (PREDICT_FALSE(!s.ok())) {
        LOG(WARNING) << GetLogPrefix() << "Peer was closed, removing from peers. Peer: "
            << (*iter).second->peer_pb().ShortDebugString();
        peers_.erase(iter);
      }
    }
}

void PeerManager::Close() {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    BOOST_FOREACH(const PeersMap::value_type& entry, peers_) {
      entry.second->Close();
    }
    STLDeleteValues(&peers_);
  }
}

std::string PeerManager::GetLogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, local_uuid_);
}

} // namespace consensus
} // namespace kudu
