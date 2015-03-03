// Copyright (c) 2014, Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/quorum_util.h"

#include <boost/foreach.hpp>
#include <set>
#include <string>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using std::string;
using strings::Substitute;

bool IsQuorumMember(const std::string& uuid, const QuorumPB& quorum) {
  BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
    if (peer.permanent_uuid() == uuid) {
      return true;
    }
  }
  return false;
}

bool IsQuorumVoter(const std::string& uuid, const QuorumPB& quorum) {
  BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
    if (peer.permanent_uuid() == uuid) {
      return peer.member_type() == QuorumPeerPB::VOTER;
    }
  }
  return false;
}

int CountVoters(const QuorumPB& quorum) {
  int voters = 0;
  BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
    if (peer.member_type() == QuorumPeerPB::VOTER) {
      voters++;
    }
  }
  return voters;
}

int MajoritySize(int num_voters) {
  DCHECK_GE(num_voters, 1);
  return (num_voters / 2) + 1;
}

QuorumPeerPB::Role GetConsensusRole(const std::string& permanent_uuid,
                                    const ConsensusStatePB& cstate) {
  if (cstate.leader_uuid() == permanent_uuid) {
    if (IsQuorumVoter(permanent_uuid, cstate.quorum())) {
      return QuorumPeerPB::LEADER;
    }
    return QuorumPeerPB::NON_PARTICIPANT;
  }

  BOOST_FOREACH(const QuorumPeerPB& peer, cstate.quorum().peers()) {
    if (peer.permanent_uuid() == permanent_uuid) {
      switch (peer.member_type()) {
        case QuorumPeerPB::VOTER:
          return QuorumPeerPB::FOLLOWER;
        default:
          return QuorumPeerPB::LEARNER;
      }
    }
  }
  return QuorumPeerPB::NON_PARTICIPANT;
}

Status VerifyQuorum(const QuorumPB& quorum, QuorumPBType type) {
  std::set<string> uuids;
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
    if (ContainsKey(uuids, peer.permanent_uuid())) {
      return Status::IllegalState(
          Substitute("Found multiple peers with uuid: $0. Quorum: $1",
                     peer.permanent_uuid(), quorum.ShortDebugString()));
    }
    uuids.insert(peer.permanent_uuid());

    if (!peer.has_last_known_addr()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no address. Quorum: $1",
                     peer.permanent_uuid(), quorum.ShortDebugString()));
    }
    if (!peer.has_member_type()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no member type set. Quorum: $1", peer.permanent_uuid(),
                     quorum.ShortDebugString()));
    }
    if (peer.member_type() == QuorumPeerPB::NON_VOTER) {
      return Status::IllegalState(
          Substitute(
              "Peer: $0 is a NON_VOTER, but this isn't supported yet. Quorum: $1",
              peer.permanent_uuid(), quorum.ShortDebugString()));
    }
  }

  return Status::OK();
}

Status VerifyConsensusState(const ConsensusStatePB& cstate, QuorumPBType type) {
  if (!cstate.has_current_term()) {
    return Status::IllegalState("ConsensusStatePB missing current_term", cstate.ShortDebugString());
  }
  if (!cstate.has_quorum()) {
    return Status::IllegalState("ConsensusStatePB missing quorum", cstate.ShortDebugString());
  }
  RETURN_NOT_OK(VerifyQuorum(cstate.quorum(), type));

  if (cstate.has_leader_uuid() && !cstate.leader_uuid().empty()) {
    if (!IsQuorumVoter(cstate.leader_uuid(), cstate.quorum())) {
      return Status::IllegalState(
          Substitute("Leader with UUID $0 is not a VOTER in the quorum! Consensus state: $1",
                     cstate.leader_uuid(), cstate.ShortDebugString()));
    }
  }

  return Status::OK();
}

} // namespace consensus
}  // namespace kudu
