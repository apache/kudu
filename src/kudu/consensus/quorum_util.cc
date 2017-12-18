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
#include <ostream>
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
using kudu::pb_util::SecureDebugString;
using std::map;
using std::pair;
using std::set;
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

bool ReplicaTypesEqual(const RaftPeerPB& peer1, const RaftPeerPB& peer2) {
  // TODO(mpercy): Include comparison of replica intentions once they are
  // implemented.
  return peer1.member_type() == peer2.member_type();
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

// The decision is based on:
//
//   * the number of voter replicas in definitively bad shape and replicas
//     marked with the REPLACE attribute
//
//   * the number of non-voter replicas marked with the PROMOTE=true attribute
//     in good or possibly good state.
//
// This is because a replica with UNKNOWN reported health state might actually
// be in good shape. If so, then adding a new replica would lead to
// over-provisioning. This logic assumes that a non-voter replica does not
// stay in unknown state for eternity -- the leader replica should take care of
// that and eventually update the health status either to 'HEALTHY' or 'FAILED'.
//
// TODO(aserbin): add a test scenario for the leader replica's logic to cover
//                the latter case.
bool ShouldAddReplica(const RaftConfigPB& config, int replication_factor) {
  int num_voters_total = 0;
  int num_voters_healthy = 0;
  int num_voters_need_replacement = 0;
  int num_non_voters_to_promote = 0;

  // While working with the optional fields related to per-replica health status
  // and attributes, has_a_field()-like methods are not called because of
  // the appropriate default values of those fields.
  VLOG(2) << "config to evaluate: " << SecureDebugString(config);
  for (const RaftPeerPB& peer : config.peers()) {
    switch (peer.member_type()) {
      case RaftPeerPB::VOTER:
        ++num_voters_total;
        if (peer.attrs().replace() ||
            peer.health_report().overall_health() == HealthReportPB::FAILED) {
          ++num_voters_need_replacement;
        }
        if (peer.health_report().overall_health() == HealthReportPB::HEALTHY) {
          ++num_voters_healthy;
        }
        break;
      case RaftPeerPB::NON_VOTER:
        if (peer.attrs().promote() &&
            peer.health_report().overall_health() != HealthReportPB::FAILED) {
          ++num_non_voters_to_promote;
        }
        break;
      default:
        LOG(DFATAL) << peer.member_type() << ": unsupported member type";
        break;
    }
  }

  // Whether the configuration is under-replicated: the projected number of
  // viable replicas is less than the required replication factor.
  const bool is_under_replicated = replication_factor >
      num_voters_total - num_voters_need_replacement + num_non_voters_to_promote;

  // Whether it's time to add a new replica: the tablet Raft configuration might
  // be under-replicated, but it does not make much sense trying to add a new
  // replica if the configuration change cannot be committed.
  const bool should_add_replica = is_under_replicated &&
      num_voters_healthy >= MajoritySize(num_voters_total);

  VLOG(2) << "decision: the config is" << (is_under_replicated ? " " : " not ")
          << "under-replicated; should" << (should_add_replica ? " " : " not ")
          << "add a non-voter replica";
  return should_add_replica;
}

// Whether there is an excess replica to evict.
bool ShouldEvictReplica(const RaftConfigPB& config,
                     const string& leader_uuid,
                     int replication_factor,
                     string* uuid_to_evict) {
  // If there is no leader, we can't evict anybody.
  if (leader_uuid.empty()) return false;

  int num_non_voters_total = 0;

  int num_voters_healthy = 0;
  int num_voters_total = 0;
  int num_voters_with_replace = 0;
  int num_voters_viable = 0;

  string non_voter_any;
  string non_voter_failed;
  string non_voter_unknown_health;

  string voter_any;
  string voter_failed;
  string voter_replace;
  string voter_unknown_health;

  bool leader_with_replace = false;

  // While working with the optional fields related to per-replica health status
  // and attributes, has_a_field()-like methods are not called because of
  // the appropriate default values of those fields.
  VLOG(2) << "config to evaluate: " << SecureDebugString(config);
  for (const RaftPeerPB& peer : config.peers()) {
    DCHECK(peer.has_permanent_uuid() && !peer.permanent_uuid().empty());
    const string& peer_uuid = peer.permanent_uuid();
    const bool failed = peer.health_report().overall_health() == HealthReportPB::FAILED;
    const bool healthy = peer.health_report().overall_health() == HealthReportPB::HEALTHY;
    const bool unknown = !peer.has_health_report() ||
        !peer.health_report().has_overall_health() ||
        peer.health_report().overall_health() == HealthReportPB::UNKNOWN;
    const bool has_replace = peer.attrs().replace();

    switch (peer.member_type()) {
      case RaftPeerPB::VOTER:
        // A leader should always report itself as being healthy.
        DCHECK(peer_uuid != leader_uuid || healthy) << Substitute(
            "$0: leader reported as not healthy; config: $1",
            peer_uuid, SecureShortDebugString(config));

        ++num_voters_total;
        if (healthy) {
          ++num_voters_healthy;
          if (!has_replace) {
            ++num_voters_viable;
          }
        }
        if (has_replace) {
          ++num_voters_with_replace;
          if (peer_uuid == leader_uuid) {
            leader_with_replace = true;
          }
        }
        if (peer_uuid == leader_uuid) {
          // Everything below is to keep track of replicas to evict; the leader
          // replica is not to be evicted.
          break;
        }
        if (failed && has_replace) {
          voter_failed = voter_replace = peer_uuid;
        }
        if (failed && voter_failed.empty()) {
          voter_failed = peer_uuid;
        }
        if (unknown && has_replace &&
            (voter_replace.empty() || voter_replace != voter_failed)) {
          voter_replace = voter_unknown_health = peer_uuid;
        }
        if (has_replace && voter_replace.empty()) {
          voter_replace = peer_uuid;
        }
        if (unknown && voter_unknown_health.empty()) {
          voter_unknown_health = peer_uuid;
        }
        voter_any = peer_uuid;
        break;

      case RaftPeerPB::NON_VOTER:
        ++num_non_voters_total;
        DCHECK_NE(peer_uuid, leader_uuid) << peer_uuid
            << ": non-voter as a leader; " << SecureShortDebugString(config);
        if (failed && non_voter_failed.empty()) {
          non_voter_failed = peer_uuid;
        }
        if (unknown && non_voter_unknown_health.empty()) {
          non_voter_unknown_health = peer_uuid;
        }
        non_voter_any = peer_uuid;
        break;

      default:
        LOG(DFATAL) << peer.member_type() << ": unsupported member type";
        break;
    }
  }

  // A few sanity checks: the leader replica UUID should not be among those
  // to evict.
  DCHECK_NE(leader_uuid, voter_any);
  DCHECK_NE(leader_uuid, voter_failed);
  DCHECK_NE(leader_uuid, voter_replace);
  DCHECK_NE(leader_uuid, voter_unknown_health);

  // A conservative approach is used when evicting replicas. In short, the
  // removal of replicas from the tablet without exact knowledge of their health
  // status could lead to removing the healthy ones and keeping the failed
  // ones, or attempting a config change operation that cannot be committed.
  // From the other side, if the number of voter replicas in good health is
  // greater or equal to the required replication factor, a replica with any
  // health status can be safely evicted without compromising the availability
  // of the tablet. Also, the eviction policy is more liberal when dealing with
  // failed replicas: if the total number of voter replicas is greater than or
  // equal to the required replication factor, the failed replicas are evicted
  // aggressively. The latter is to avoid polluting the tablet servers with
  // failed replicas, limiting the available places for new non-voter replicas
  // created to replace failed ones. See below for more details.
  //
  // * A non-voter replica may be evicted regardless of its health status
  //   if the number of voter replicas in good health without the 'replace'
  //   attribute is greater than or equal to the required replication factor.
  //   The idea is to not evict non-voter replicas that might be needed to reach
  //   the required replication factor, while a present non-voter replica could
  //   be a good fit to replace a voter replica, if needed.
  //
  // * A non-voter replica with FAILED health status may be evicted if the
  //   number of voter replicas in good health without the 'replace' attribute
  //   is greater than or equal to a strict majority of voter replicas. The idea
  //   is to avoid polluting available tablet servers with failed non-voter
  //   replicas, while replacing failed non-voters with healthy non-voters as
  //   aggressively as possible. Also, we want to be sure that an eviction can
  //   succeed before initiating it.
  //
  // * A voter replica may be evicted regardless of its health status
  //   if after the eviction the number of voter replicas in good health will be
  //   greater than or equal to the required replication factor and the leader
  //   replica itself is not marked with the 'replace' attribute set. The latter
  //   part of the condition emerges from the following observations:
  //     ** By definition, a voter replica marked with the 'replace' attribute
  //        should be eventually evicted from the Raft group.
  //     ** If all voter replicas are in good health and their total count is
  //        greater than the target replication and only a single one is marked
  //        with the 'replace' attribute, that's the replica to be evicted.
  //     ** Kudu Raft implementation does not support evicting the leader of
  //        a Raft group.
  //    So, the removal of a leader replica marked with the 'replace' attribute
  //    is postponed until the leader replica steps down and becomes a follower.
  //
  // * A voter replica with FAILED health may be evicted only if the total
  //   number of voter replicas is greater than the required replication factor
  //   and the number of *other* voter replicas in good health without the
  //   'replace' attribute is greater than or equal to a strict majority of
  //   voter replicas.
  //
  // * A voter replica in good health marked with the 'replace' attribute may be
  //   evicted when the number of replicas in good health after the eviction
  //   is greater than or equal to the required replication factor.

  bool need_to_evict_non_voter = false;

  // Check if there is any excess non-voter replica. We add non-voter replicas
  // to replace non-viable (i.e. failed or explicitly marked for eviction) ones.
  need_to_evict_non_voter |=
      num_voters_viable >= replication_factor &&
      num_non_voters_total > 0;

  // Some non-voter replica has failed: we want to remove those aggressively.
  // This is to avoid polluting tablet servers with failed replicas. Otherwise,
  // it may be a situation when it's impossible to add a new non-voter replica
  // to replace failed ones.
  need_to_evict_non_voter |= !non_voter_failed.empty();

  // All the non-voter-related sub-cases are applicable only when there is at
  // least one non-voter replica and a majority of voter replicas are on-line
  // to commit the Raft configuration change.
  const bool can_evict_non_voter = need_to_evict_non_voter &&
      num_voters_healthy >= MajoritySize(num_voters_total);

  bool need_to_evict_voter = false;

  // The abundant case: can evict any voter replica. The code below will select
  // the most appropriate candidate.
  need_to_evict_voter |= num_voters_viable > replication_factor;

  // Some voter replica has failed: we want to remove those aggressively.
  // This is to avoid polluting tablet servers with failed replicas. Otherwise,
  // it may be a situation when it's impossible to add a new non-voter replica
  // to replace failed ones.
  need_to_evict_voter |= !voter_failed.empty();

  // In case if we already have enough healthy replicas running, it's safe to
  // get rid of replicas with unknown health state.
  need_to_evict_voter |=
      num_voters_viable >= replication_factor &&
      !voter_unknown_health.empty();

  // Working with the replicas marked with the 'replace' attribute:
  // the case when too many replicas are marked with the 'replace' attribute
  // while all required replicas are healthy.
  need_to_evict_voter |= (num_voters_healthy >= replication_factor) &&
      ((num_voters_with_replace > replication_factor) ||
       (num_voters_with_replace >= replication_factor && num_voters_viable > 0));

  // Working with the replicas marked with the 'replace' attribute:
  // the case where a few replicas are marked with the 'replace' attribute
  // while all required replicas are healthy.
  //
  // In the special case when the leader replica is the only one marked with the
  // 'replace' attribute, the leader replica cannot be evicted.
  need_to_evict_voter |=
      !(num_voters_with_replace == 1 && leader_with_replace) &&
      (num_voters_with_replace > 0 && num_voters_healthy > replication_factor);

  // The voter-related sub-cases are applicable only when the total number of
  // voter replicas is greater than the target replication factor and a majority
  // of voter replicas are on-line to commit the Raft configuration change.
  const bool can_evict_voter = need_to_evict_voter &&
      num_voters_total > replication_factor &&
      num_voters_healthy >= MajoritySize(num_voters_total - 1);

  const bool can_evict = can_evict_non_voter || can_evict_voter;
  string to_evict;
  if (can_evict_non_voter) {
    // First try to evict an excess non-voter, if present. This might help to
    // avoid IO load due to a tablet copy in progress.
    //
    // Non-voter candidates for eviction (in decreasing priority):
    //   * failed
    //   * in unknown health state
    //   * any other
    if (!non_voter_failed.empty()) {
      to_evict = non_voter_failed;
    } else if (!non_voter_unknown_health.empty()) {
      to_evict = non_voter_unknown_health;
    } else {
      to_evict = non_voter_any;
    }
  } else if (can_evict_voter) {
    // Next try to evict an excess voter.
    //
    // Voter candidates for eviction (in decreasing priority):
    //   * failed and having the attribute REPLACE set
    //   * failed
    //   * having the attribute REPLACE set
    //   * in unknown health state
    //   * any other
    if (!voter_failed.empty()) {
      to_evict = voter_failed;
    } else if (!voter_replace.empty()) {
      to_evict = voter_replace;
    } else if (!voter_unknown_health.empty()) {
      to_evict = voter_unknown_health;
    } else {
      to_evict = voter_any;
    }
  }

  if (can_evict) {
    DCHECK(!to_evict.empty());
    DCHECK_NE(leader_uuid, to_evict);
    if (uuid_to_evict) {
      *uuid_to_evict = to_evict;
    }
  }
  VLOG(2) << "decision: can"
          << (can_evict ? "" : "not") << " evict replica "
          << (can_evict ? to_evict : "");

  return can_evict;
}

}  // namespace consensus
}  // namespace kudu
