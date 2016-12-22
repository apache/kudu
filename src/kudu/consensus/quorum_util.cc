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
#include "kudu/consensus/quorum_util.h"

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using google::protobuf::RepeatedPtrField;
using std::map;
using std::pair;
using std::string;
using strings::Substitute;

bool IsRaftConfigMember(const std::string& uuid, const RaftConfigPB& config) {
  for (const RaftPeerPB& peer : config.peers()) {
    if (peer.permanent_uuid() == uuid) {
      return true;
    }
  }
  return false;
}

bool IsRaftConfigVoter(const std::string& uuid, const RaftConfigPB& config) {
  for (const RaftPeerPB& peer : config.peers()) {
    if (peer.permanent_uuid() == uuid) {
      return peer.member_type() == RaftPeerPB::VOTER;
    }
  }
  return false;
}

Status GetRaftConfigMember(const RaftConfigPB& config,
                           const std::string& uuid,
                           RaftPeerPB* peer_pb) {
  for (const RaftPeerPB& peer : config.peers()) {
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
  for (const RaftPeerPB& peer : config->peers()) {
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
  for (const RaftPeerPB& peer : config.peers()) {
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

RaftPeerPB::Role GetConsensusRole(const std::string& uuid,
                                    const ConsensusStatePB& cstate) {
  if (cstate.leader_uuid() == uuid) {
    if (IsRaftConfigVoter(uuid, cstate.config())) {
      return RaftPeerPB::LEADER;
    }
    return RaftPeerPB::NON_PARTICIPANT;
  }

  for (const RaftPeerPB& peer : cstate.config().peers()) {
    if (peer.permanent_uuid() == uuid) {
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
                   SecureShortDebugString(config)));
  }

  // All configurations must have 'opid_index' populated.
  if (!config.has_opid_index()) {
    return Status::IllegalState(
        Substitute("Configs must have opid_index set. RaftConfig: $0",
                   SecureShortDebugString(config)));
  }

  for (const RaftPeerPB& peer : config.peers()) {
    if (!peer.has_permanent_uuid() || peer.permanent_uuid() == "") {
      return Status::IllegalState(Substitute("One peer didn't have an uuid or had the empty"
          " string. RaftConfig: $0", SecureShortDebugString(config)));
    }
    if (ContainsKey(uuids, peer.permanent_uuid())) {
      return Status::IllegalState(
          Substitute("Found multiple peers with uuid: $0. RaftConfig: $1",
                     peer.permanent_uuid(), SecureShortDebugString(config)));
    }
    uuids.insert(peer.permanent_uuid());

    if (config.peers_size() > 1 && !peer.has_last_known_addr()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no address. RaftConfig: $1",
                     peer.permanent_uuid(), SecureShortDebugString(config)));
    }
    if (!peer.has_member_type()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no member type set. RaftConfig: $1", peer.permanent_uuid(),
                     SecureShortDebugString(config)));
    }
    if (peer.member_type() == RaftPeerPB::NON_VOTER) {
      return Status::IllegalState(
          Substitute(
              "Peer: $0 is a NON_VOTER, but this isn't supported yet. RaftConfig: $1",
              peer.permanent_uuid(), SecureShortDebugString(config)));
    }
  }

  return Status::OK();
}

Status VerifyConsensusState(const ConsensusStatePB& cstate, RaftConfigState type) {
  if (!cstate.has_current_term()) {
    return Status::IllegalState("ConsensusStatePB missing current_term",
                                SecureShortDebugString(cstate));
  }
  if (!cstate.has_config()) {
    return Status::IllegalState("ConsensusStatePB missing config", SecureShortDebugString(cstate));
  }
  RETURN_NOT_OK(VerifyRaftConfig(cstate.config(), type));

  if (cstate.has_leader_uuid() && !cstate.leader_uuid().empty()) {
    if (!IsRaftConfigVoter(cstate.leader_uuid(), cstate.config())) {
      return Status::IllegalState(
          Substitute("Leader with UUID $0 is not a VOTER in the config! Consensus state: $1",
                     cstate.leader_uuid(), SecureShortDebugString(cstate)));
    }
  }

  return Status::OK();
}

std::string DiffRaftConfigs(const RaftConfigPB& old_config,
                            const RaftConfigPB& new_config) {
  // Create dummy ConsensusState objects so we can reuse the code
  // from the below function.
  ConsensusStatePB old_state;
  old_state.mutable_config()->CopyFrom(old_config);
  ConsensusStatePB new_state;
  new_state.mutable_config()->CopyFrom(new_config);

  return DiffConsensusStates(old_state, new_state);
}

string DiffConsensusStates(const ConsensusStatePB& old_state,
                           const ConsensusStatePB& new_state) {
  bool leader_changed = old_state.leader_uuid() != new_state.leader_uuid();
  bool term_changed = old_state.current_term() != new_state.current_term();
  bool config_changed = old_state.config().opid_index() != new_state.config().opid_index();

  // Construct a map from Peer UUID to '<old peer, new peer>' pairs.
  // Due to the default construction nature of std::map and std::pair, if a peer
  // is present in one configuration but not the other, we'll end up with an empty
  // protobuf in that element of the pair.
  map<string, pair<RaftPeerPB, RaftPeerPB>> peer_infos;
  for (const auto& p : old_state.config().peers()) {
    peer_infos[p.permanent_uuid()].first = p;
  }
  for (const auto& p : new_state.config().peers()) {
    peer_infos[p.permanent_uuid()].second = p;
  }

  // Now collect strings representing the changes.
  vector<string> change_strs;
  if (config_changed) {
    change_strs.push_back(
        Substitute("config changed from index $0 to $1",
                   old_state.config().opid_index(),
                   new_state.config().opid_index()));
  }

  if (term_changed) {
    change_strs.push_back(
        Substitute("term changed from $0 to $1",
                   old_state.current_term(),
                   new_state.current_term()));
  }

  if (leader_changed) {
    string old_leader = "<none>";
    string new_leader = "<none>";
    if (old_state.has_leader_uuid()) {
      old_leader = Substitute("$0 ($1)",
                              old_state.leader_uuid(),
                              peer_infos[old_state.leader_uuid()].first.last_known_addr().host());
    }
    if (new_state.has_leader_uuid()) {
      new_leader = Substitute("$0 ($1)",
                              new_state.leader_uuid(),
                              peer_infos[new_state.leader_uuid()].second.last_known_addr().host());
    }

    change_strs.push_back(Substitute("leader changed from $0 to $1",
                                     old_leader, new_leader));
  }

  for (const auto& e : peer_infos) {
    const auto& old_peer = e.second.first;
    const auto& new_peer = e.second.second;
    if (old_peer.has_permanent_uuid() && !new_peer.has_permanent_uuid()) {
      change_strs.push_back(
          Substitute("$0 $1 ($2) evicted",
                     RaftPeerPB_MemberType_Name(old_peer.member_type()),
                     old_peer.permanent_uuid(),
                     old_peer.last_known_addr().host()));
    } else if (!old_peer.has_permanent_uuid() && new_peer.has_permanent_uuid()) {
      change_strs.push_back(
          Substitute("$0 $1 ($2) added",
                     RaftPeerPB_MemberType_Name(new_peer.member_type()),
                     new_peer.permanent_uuid(),
                     new_peer.last_known_addr().host()));
    } else if (old_peer.has_permanent_uuid() && new_peer.has_permanent_uuid()) {
      if (old_peer.member_type() != new_peer.member_type()) {
        change_strs.push_back(
            Substitute("$0 ($1) changed from $2 to $3",
                       old_peer.permanent_uuid(),
                       old_peer.last_known_addr().host(),
                       RaftPeerPB_MemberType_Name(old_peer.member_type()),
                       RaftPeerPB_MemberType_Name(new_peer.member_type())));
      }
    }
  }

  // We expect to have detected some differences above, but in case
  // someone forgets to update this function when adding a new field,
  // it's still useful to report some change unless the protobufs are identical.
  // So, we fall back to just dumping the before/after debug strings.
  if (change_strs.empty()) {
    if (SecureShortDebugString(old_state) == SecureShortDebugString(new_state)) {
      return "no change";
    }
    return Substitute("change from {$0} to {$1}",
                      SecureShortDebugString(old_state),
                      SecureShortDebugString(new_state));
  }


  return JoinStrings(change_strs, ", ");

}

}  // namespace consensus
}  // namespace kudu
