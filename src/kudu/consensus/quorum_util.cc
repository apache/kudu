// Copyright (c) 2014, Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/quorum_util.h"

#include <boost/foreach.hpp>
#include <set>
#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using std::string;
using strings::Substitute;

bool IsVotingRole(const QuorumPeerPB::Role role) {
  switch (role) {
    case QuorumPeerPB::LEADER:
    case QuorumPeerPB::CANDIDATE:
    case QuorumPeerPB::FOLLOWER:
      // Above 3 cases should all fall through.
      return true;
    default:
      return false;
  }
}

Status GivePeerRoleInQuorum(const string& peer_uuid,
                            QuorumPeerPB::Role new_role,
                            const QuorumPB& old_quorum,
                            QuorumPB* new_quorum) {
  new_quorum->CopyFrom(old_quorum);
  new_quorum->clear_peers();
  bool found_peer = false;
  BOOST_FOREACH(const QuorumPeerPB& old_peer, old_quorum.peers()) {
    QuorumPeerPB* new_peer = new_quorum->add_peers();
    new_peer->CopyFrom(old_peer);

    // Assume new role for local peer.
    if (new_peer->permanent_uuid() == peer_uuid) {
      if (PREDICT_FALSE(found_peer)) {
        return Status::IllegalState(
            Substitute("Peer $0 found in quorum multiple times: $1",
                       peer_uuid, old_quorum.ShortDebugString()));
      }
      found_peer = true;
      new_peer->set_role(new_role);
      continue;
    }

    // Demote any other leaders/candidates to followers.
    if (new_peer->role() == QuorumPeerPB::LEADER ||
        new_peer->role() == QuorumPeerPB::CANDIDATE) {
      new_peer->set_role(QuorumPeerPB::FOLLOWER);
    }
  }
  if (!found_peer) {
    return Status::IllegalState(Substitute("Cannot find peer $0 in quorum: $1",
                                           peer_uuid, old_quorum.ShortDebugString()));
  }
  return Status::OK();
}

void SetAllQuorumVotersToFollower(const QuorumPB& old_quorum,
                                  QuorumPB* new_quorum) {
  new_quorum->CopyFrom(old_quorum);
  new_quorum->clear_peers();
  BOOST_FOREACH(const QuorumPeerPB& old_peer, old_quorum.peers()) {
    QuorumPeerPB* new_peer = new_quorum->add_peers();
    new_peer->CopyFrom(old_peer);
    if (IsVotingRole(new_peer->role())) {
      new_peer->set_role(QuorumPeerPB::FOLLOWER);
    }
  }
}

QuorumPeerPB::Role GetRoleInQuorum(const std::string& permanent_uuid,
                                   const QuorumPB& quorum) {
  BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
    if (peer.permanent_uuid() == permanent_uuid) {
      return peer.role();
    }
  }
  return QuorumPeerPB::NON_PARTICIPANT;
}

Status VerifyQuorum(const QuorumPB& quorum, QuorumPBType type) {
  std::set<string> uuids;
  bool found_leader = false;
  if (quorum.peers_size() == 0) {
    return Status::IllegalState(
        Substitute("Quorum must have at least one peer. Quorum: $0",
                   quorum.ShortDebugString()));
  }

  if (!quorum.has_local()) {
    return Status::IllegalState(
        Substitute("Quorum must specify whether it is local. Quorum: ",
                   quorum.ShortDebugString()));
  }

  if (type == COMMITTED_QUORUM) {
    // Committed quorums must have 'opid_index' populated.
    if (!quorum.has_opid_index()) {
      return Status::IllegalState(
          Substitute("Committed quorums must have opid_index set. Quorum: $0",
                     quorum.ShortDebugString()));
    }
  } else if (type == UNCOMMITTED_QUORUM) {
    // Uncommitted quorums must *not* have 'opid_index' populated.
    if (quorum.has_opid_index()) {
      return Status::IllegalState(
          Substitute("Uncommitted quorums must not have opid_index set. Quorum: $0",
                     quorum.ShortDebugString()));
    }
  }

  // Local quorums must have only one peer and it may or may not
  // have an address.
  if (quorum.local()) {
    if (quorum.peers_size() != 1) {
      return Status::IllegalState(
          Substitute("Local quorums must have 1 and only one peer. Quorum: ",
                     quorum.ShortDebugString()));
    }
    if (!quorum.peers(0).has_permanent_uuid() ||
        quorum.peers(0).permanent_uuid() == "") {
      return Status::IllegalState(
          Substitute("Local peer must have an UUID. Quorum: ",
                     quorum.ShortDebugString()));
    }
    return Status::OK();
  }

  BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
    if (!peer.has_permanent_uuid() || peer.permanent_uuid() == "") {
      return Status::IllegalState(Substitute("One peer didn't have an uuid or had the empty"
          " string. Quorum: $0", quorum.ShortDebugString()));
    }
    if (uuids.count(peer.permanent_uuid()) == 1) {
      return Status::IllegalState(
          Substitute("Found two peers with uuid: $0. Quorum: $1",
                     peer.permanent_uuid(), quorum.ShortDebugString()));
    }
    uuids.insert(peer.permanent_uuid());

    if (!peer.has_last_known_addr()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no address. Quorum: $1",
                     peer.permanent_uuid(), quorum.ShortDebugString()));
    }
    if (!peer.has_role()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no role. Quorum: $1", peer.permanent_uuid(),
                     quorum.ShortDebugString()));
    }
    if (peer.role() == QuorumPeerPB::LEADER
        || peer.role() == QuorumPeerPB::CANDIDATE) {
      if (!found_leader) {
        found_leader = true;
        continue;
      }
      return Status::IllegalState(
          Substitute("Found two peers with LEADER/CANDIDATE role. Quorum: $0",
                     quorum.ShortDebugString()));
    }
    if (peer.role() == QuorumPeerPB::LEARNER) {
      return Status::IllegalState(
          Substitute(
              "Peer: $0 has LEARNER role but this isn't supported yet. Quorum: $1",
              peer.permanent_uuid(), quorum.ShortDebugString()));
    }
  }
  return Status::OK();
}

} // namespace consensus
}  // namespace kudu
