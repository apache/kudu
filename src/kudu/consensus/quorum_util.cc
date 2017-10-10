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

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using google::protobuf::RepeatedPtrField;
using kudu::pb_util::SecureShortDebugString;
using std::map;
using std::pair;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace consensus {

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

bool IsVoterRole(RaftPeerPB::Role role) {
  return role == RaftPeerPB::LEADER || role == RaftPeerPB::FOLLOWER;
}

Status GetRaftConfigMember(RaftConfigPB* config,
                           const std::string& uuid,
                           RaftPeerPB** peer_pb) {
  for (RaftPeerPB& peer : *config->mutable_peers()) {
    if (peer.permanent_uuid() == uuid) {
      *peer_pb = &peer;
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute("Peer with uuid $0 not found in consensus config", uuid));
}

Status GetRaftConfigLeader(ConsensusStatePB* cstate, RaftPeerPB** peer_pb) {
  if (cstate->leader_uuid().empty()) {
    return Status::NotFound("Consensus config has no leader");
  }
  return GetRaftConfigMember(cstate->mutable_committed_config(), cstate->leader_uuid(), peer_pb);
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

RaftPeerPB::Role GetConsensusRole(const std::string& uuid, const ConsensusStatePB& cstate) {
  // The active config is the pending config if there is one, else it's the committed config.
  const RaftConfigPB& config = cstate.has_pending_config() ?
                               cstate.pending_config() :
                               cstate.committed_config();
  if (cstate.leader_uuid() == uuid) {
    if (IsRaftConfigVoter(uuid, config)) {
      return RaftPeerPB::LEADER;
    }
    return RaftPeerPB::NON_PARTICIPANT;
  }

  for (const RaftPeerPB& peer : config.peers()) {
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

Status VerifyRaftConfig(const RaftConfigPB& config) {
  std::set<string> uuids;
  if (config.peers().empty()) {
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
    if (!peer.has_permanent_uuid() || peer.permanent_uuid().empty()) {
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
  }

  return Status::OK();
}

Status VerifyConsensusState(const ConsensusStatePB& cstate) {
  if (!cstate.has_current_term()) {
    return Status::IllegalState("ConsensusStatePB missing current_term",
                                SecureShortDebugString(cstate));
  }
  if (!cstate.has_committed_config()) {
    return Status::IllegalState("ConsensusStatePB missing config", SecureShortDebugString(cstate));
  }
  RETURN_NOT_OK(VerifyRaftConfig(cstate.committed_config()));
  if (cstate.has_pending_config()) {
    RETURN_NOT_OK(VerifyRaftConfig(cstate.pending_config()));
  }

  if (!cstate.leader_uuid().empty()) {
    if (!IsRaftConfigVoter(cstate.leader_uuid(), cstate.committed_config())
        && cstate.has_pending_config()
        && !IsRaftConfigVoter(cstate.leader_uuid(), cstate.pending_config())) {
      return Status::IllegalState(
          Substitute("Leader with UUID $0 is not a VOTER in the committed or pending config! "
                     "Consensus state: $1",
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
  old_state.mutable_committed_config()->CopyFrom(old_config);
  ConsensusStatePB new_state;
  new_state.mutable_committed_config()->CopyFrom(new_config);

  return DiffConsensusStates(old_state, new_state);
}

namespace {

// A mapping from peer UUID to to <old peer, new peer> pairs.
typedef map<string, pair<RaftPeerPB, RaftPeerPB>> PeerInfoMap;

bool DiffPeers(const PeerInfoMap& peer_infos,
               vector<string>* change_strs) {
  bool changes = false;
  for (const auto& e : peer_infos) {
    const auto& old_peer = e.second.first;
    const auto& new_peer = e.second.second;
    if (old_peer.has_permanent_uuid() && !new_peer.has_permanent_uuid()) {
      changes = true;
      change_strs->push_back(
        Substitute("$0 $1 ($2) evicted",
                   RaftPeerPB_MemberType_Name(old_peer.member_type()),
                   old_peer.permanent_uuid(),
                   old_peer.last_known_addr().host()));
    } else if (!old_peer.has_permanent_uuid() && new_peer.has_permanent_uuid()) {
      changes = true;
      change_strs->push_back(
        Substitute("$0 $1 ($2) added",
                   RaftPeerPB_MemberType_Name(new_peer.member_type()),
                   new_peer.permanent_uuid(),
                   new_peer.last_known_addr().host()));
    } else if (old_peer.has_permanent_uuid() && new_peer.has_permanent_uuid()) {
      changes = true;
      if (old_peer.member_type() != new_peer.member_type()) {
        change_strs->push_back(
          Substitute("$0 ($1) changed from $2 to $3",
                     old_peer.permanent_uuid(),
                     old_peer.last_known_addr().host(),
                     RaftPeerPB_MemberType_Name(old_peer.member_type()),
                     RaftPeerPB_MemberType_Name(new_peer.member_type())));
      }
    }
  }
  return changes;
}

string PeersString(const RaftConfigPB& config) {
  vector<string> strs;
  for (const auto& p : config.peers()) {
    strs.push_back(Substitute("$0 $1 ($2)",
                              RaftPeerPB_MemberType_Name(p.member_type()),
                              p.permanent_uuid(),
                              p.last_known_addr().host()));
  }
  return JoinStrings(strs, ", ");
}

} // anonymous namespace

string DiffConsensusStates(const ConsensusStatePB& old_state,
                           const ConsensusStatePB& new_state) {
  bool leader_changed = old_state.leader_uuid() != new_state.leader_uuid();
  bool term_changed = old_state.current_term() != new_state.current_term();
  bool config_changed =
      old_state.committed_config().opid_index() != new_state.committed_config().opid_index();

  bool pending_config_gained = !old_state.has_pending_config() && new_state.has_pending_config();
  bool pending_config_lost = old_state.has_pending_config() && !new_state.has_pending_config();

  // Construct a map from Peer UUID to '<old peer, new peer>' pairs.
  // Due to the default construction nature of std::map and std::pair, if a peer
  // is present in one configuration but not the other, we'll end up with an empty
  // protobuf in that element of the pair.
  PeerInfoMap committed_peer_infos;
  for (const auto& p : old_state.committed_config().peers()) {
    committed_peer_infos[p.permanent_uuid()].first = p;
  }
  for (const auto& p : new_state.committed_config().peers()) {
    committed_peer_infos[p.permanent_uuid()].second = p;
  }

  // Now collect strings representing the changes.
  vector<string> change_strs;
  if (config_changed) {
    change_strs.push_back(
      Substitute("config changed from index $0 to $1",
                 old_state.committed_config().opid_index(),
                 new_state.committed_config().opid_index()));
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
    if (!old_state.leader_uuid().empty()) {
      old_leader = Substitute("$0 ($1)",
                              old_state.leader_uuid(),
                              committed_peer_infos[old_state.leader_uuid()].first
                                  .last_known_addr().host());
    }
    if (!new_state.leader_uuid().empty()) {
      new_leader = Substitute("$0 ($1)",
                              new_state.leader_uuid(),
                              committed_peer_infos[new_state.leader_uuid()].second
                                  .last_known_addr().host());
    }

    change_strs.push_back(Substitute("leader changed from $0 to $1",
                                     old_leader, new_leader));
  }

  DiffPeers(committed_peer_infos, &change_strs);

  if (pending_config_gained) {
    change_strs.push_back(Substitute("now has a pending config: $0",
                                     PeersString(new_state.pending_config())));
  }
  if (pending_config_lost) {
    change_strs.push_back(Substitute("no longer has a pending config: $0",
                                     PeersString(old_state.pending_config())));
  }

  // A pending config doesn't have a committed opid_index yet, so we determine if there's a change
  // by computing the peer differences.
  if (old_state.has_pending_config() && new_state.has_pending_config()) {
    PeerInfoMap pending_peer_infos;
    for (const auto &p : old_state.pending_config().peers()) {
      pending_peer_infos[p.permanent_uuid()].first = p;
    }
    for (const auto &p : new_state.pending_config().peers()) {
      pending_peer_infos[p.permanent_uuid()].second = p;
    }

    vector<string> pending_change_strs;
    if (DiffPeers(pending_peer_infos, &pending_change_strs)) {
      change_strs.emplace_back("pending config changed");
      change_strs.insert(change_strs.end(), pending_change_strs.cbegin(),
                         pending_change_strs.cend());
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
