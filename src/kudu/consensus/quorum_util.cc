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

using google::protobuf::RepeatedPtrField;
using std::string;
using strings::Substitute;

bool IsRaftConfigMember(const std::string& uuid, const RaftConfigPB& config) {
  BOOST_FOREACH(const RaftPeerPB& peer, config.peers()) {
    if (peer.permanent_uuid() == uuid) {
      return true;
    }
  }
  return false;
}

bool IsRaftConfigVoter(const std::string& uuid, const RaftConfigPB& config) {
  BOOST_FOREACH(const RaftPeerPB& peer, config.peers()) {
    if (peer.permanent_uuid() == uuid) {
      return peer.member_type() == RaftPeerPB::VOTER;
    }
  }
  return false;
}

Status GetRaftConfigMember(const RaftConfigPB& config,
                           const std::string& uuid,
                           RaftPeerPB* peer_pb) {
  BOOST_FOREACH(const RaftPeerPB& peer, config.peers()) {
    if (peer.permanent_uuid() == uuid) {
      *peer_pb = peer;
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute("Peer with uuid $0 not found in consensus config", uuid));
}

Status GetRaftConfigLeader(const ConsensusStatePB& cstate, RaftPeerPB* peer_pb) {
  if (!cstate.has_leader_uuid() || cstate.leader_uuid().empty()) {
    return Status::NotFound("Consensus config has no leader");
  }
  return GetRaftConfigMember(cstate.config(), cstate.leader_uuid(), peer_pb);
}

bool RemoveFromRaftConfig(RaftConfigPB* config, const string& uuid) {
  RepeatedPtrField<RaftPeerPB> modified_peers;
  bool removed = false;
  BOOST_FOREACH(const RaftPeerPB& peer, config->peers()) {
    if (peer.permanent_uuid() == uuid) {
      removed = true;
      continue;
    }
    *modified_peers.Add() = peer;
  }
  if (!removed) return false;
  config->mutable_peers()->Swap(&modified_peers);
  return true;
}

int CountVoters(const RaftConfigPB& config) {
  int voters = 0;
  BOOST_FOREACH(const RaftPeerPB& peer, config.peers()) {
    if (peer.member_type() == RaftPeerPB::VOTER) {
      voters++;
    }
  }
  return voters;
}

int MajoritySize(int num_voters) {
  DCHECK_GE(num_voters, 1);
  return (num_voters / 2) + 1;
}

RaftPeerPB::Role GetConsensusRole(const std::string& permanent_uuid,
                                    const ConsensusStatePB& cstate) {
  if (cstate.leader_uuid() == permanent_uuid) {
    if (IsRaftConfigVoter(permanent_uuid, cstate.config())) {
      return RaftPeerPB::LEADER;
    }
    return RaftPeerPB::NON_PARTICIPANT;
  }

  BOOST_FOREACH(const RaftPeerPB& peer, cstate.config().peers()) {
    if (peer.permanent_uuid() == permanent_uuid) {
      switch (peer.member_type()) {
        case RaftPeerPB::VOTER:
          return RaftPeerPB::FOLLOWER;
        default:
          return RaftPeerPB::LEARNER;
      }
    }
  }
  return RaftPeerPB::NON_PARTICIPANT;
}

Status VerifyRaftConfig(const RaftConfigPB& config, RaftConfigState type) {
  std::set<string> uuids;
  if (config.peers_size() == 0) {
    return Status::IllegalState(
        Substitute("RaftConfig must have at least one peer. RaftConfig: $0",
                   config.ShortDebugString()));
  }

  if (!config.has_local()) {
    return Status::IllegalState(
        Substitute("RaftConfig must specify whether it is local. RaftConfig: ",
                   config.ShortDebugString()));
  }

  if (type == COMMITTED_QUORUM) {
    // Committed configurations must have 'opid_index' populated.
    if (!config.has_opid_index()) {
      return Status::IllegalState(
          Substitute("Committed configs must have opid_index set. RaftConfig: $0",
                     config.ShortDebugString()));
    }
  } else if (type == UNCOMMITTED_QUORUM) {
    // Uncommitted configurations must *not* have 'opid_index' populated.
    if (config.has_opid_index()) {
      return Status::IllegalState(
          Substitute("Uncommitted configs must not have opid_index set. RaftConfig: $0",
                     config.ShortDebugString()));
    }
  }

  // Local configurations must have only one peer and it may or may not
  // have an address.
  if (config.local()) {
    if (config.peers_size() != 1) {
      return Status::IllegalState(
          Substitute("Local configs must have 1 and only one peer. RaftConfig: ",
                     config.ShortDebugString()));
    }
    if (!config.peers(0).has_permanent_uuid() ||
        config.peers(0).permanent_uuid() == "") {
      return Status::IllegalState(
          Substitute("Local peer must have an UUID. RaftConfig: ",
                     config.ShortDebugString()));
    }
    return Status::OK();
  }

  BOOST_FOREACH(const RaftPeerPB& peer, config.peers()) {
    if (!peer.has_permanent_uuid() || peer.permanent_uuid() == "") {
      return Status::IllegalState(Substitute("One peer didn't have an uuid or had the empty"
          " string. RaftConfig: $0", config.ShortDebugString()));
    }
    if (ContainsKey(uuids, peer.permanent_uuid())) {
      return Status::IllegalState(
          Substitute("Found multiple peers with uuid: $0. RaftConfig: $1",
                     peer.permanent_uuid(), config.ShortDebugString()));
    }
    uuids.insert(peer.permanent_uuid());

    if (!peer.has_last_known_addr()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no address. RaftConfig: $1",
                     peer.permanent_uuid(), config.ShortDebugString()));
    }
    if (!peer.has_member_type()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no member type set. RaftConfig: $1", peer.permanent_uuid(),
                     config.ShortDebugString()));
    }
    if (peer.member_type() == RaftPeerPB::NON_VOTER) {
      return Status::IllegalState(
          Substitute(
              "Peer: $0 is a NON_VOTER, but this isn't supported yet. RaftConfig: $1",
              peer.permanent_uuid(), config.ShortDebugString()));
    }
  }

  return Status::OK();
}

Status VerifyConsensusState(const ConsensusStatePB& cstate, RaftConfigState type) {
  if (!cstate.has_current_term()) {
    return Status::IllegalState("ConsensusStatePB missing current_term", cstate.ShortDebugString());
  }
  if (!cstate.has_config()) {
    return Status::IllegalState("ConsensusStatePB missing config", cstate.ShortDebugString());
  }
  RETURN_NOT_OK(VerifyRaftConfig(cstate.config(), type));

  if (cstate.has_leader_uuid() && !cstate.leader_uuid().empty()) {
    if (!IsRaftConfigVoter(cstate.leader_uuid(), cstate.config())) {
      return Status::IllegalState(
          Substitute("Leader with UUID $0 is not a VOTER in the config! Consensus state: $1",
                     cstate.leader_uuid(), cstate.ShortDebugString()));
    }
  }

  return Status::OK();
}

} // namespace consensus
}  // namespace kudu
