// Copyright (c) 2014, Cloudera Inc.

#include "kudu/consensus/quorum_util.h"

#include <boost/foreach.hpp>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using std::string;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using strings::Substitute;

Status MakePeerLeaderInQuorum(const string& peer_uuid,
                              const QuorumPB& old_quorum,
                              QuorumPB* new_quorum) {
  new_quorum->Clear();
  new_quorum->CopyFrom(old_quorum);
  new_quorum->clear_peers();
  bool found_peer = false;
  BOOST_FOREACH(const QuorumPeerPB& old_peer, old_quorum.peers()) {
    QuorumPeerPB* new_peer = new_quorum->add_peers();
    new_peer->CopyFrom(old_peer);
    if (new_peer->permanent_uuid() == peer_uuid) {
      new_peer->set_role(QuorumPeerPB::LEADER);
      found_peer = true;
      continue;
    }
    if (new_peer->role() == QuorumPeerPB::LEADER ||
        new_peer->role() == QuorumPeerPB::CANDIDATE) {
      new_peer->set_role(QuorumPeerPB::FOLLOWER);
    }
  }
  if (!found_peer) {
    return Status::IllegalState(Substitute("Cannot find peer: $0 in quorum: $1",
                                           peer_uuid, old_quorum.ShortDebugString()));
  }
  return Status::OK();
}

metadata::QuorumPeerPB::Role GetRoleInQuorum(const std::string& permanent_uuid,
                                             const metadata::QuorumPB& quorum) {
  BOOST_FOREACH(const metadata::QuorumPeerPB& peer, quorum.peers()) {
    if (peer.permanent_uuid() == permanent_uuid) {
      return peer.role();
    }
  }
  return metadata::QuorumPeerPB::NON_PARTICIPANT;
}

} // namespace consensus
}  // namespace kudu
