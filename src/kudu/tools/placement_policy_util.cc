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

#include "kudu/tools/placement_policy_util.h"

#include <cstdint>
#include <functional>
#include <map>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tools/rebalance_algo.h"
#include "kudu/util/status.h"

using std::multimap;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// Given the information on a placement policy violation for the specified
// tablet, find the best replica to mark with the REPLACE attribute.
Status FindBestReplicaToReplace(
    const PlacementPolicyViolationInfo& info,
    const ClusterLocalityInfo& locality_info,
    const TabletsPlacementInfo& tablets_info,
    Rebalancer::ReplicaMove* replica_to_replace) {
  DCHECK(replica_to_replace);

  const auto& ts_id_by_location = locality_info.servers_by_location;
  // If the cluster has a single location, it's impossible to move any replica
  // to other location.
  if (ts_id_by_location.size() == 1) {
    return Status::ConfigurationError(Substitute(
        "cannot change tablet replica distribution to conform with "
        "the placement policy constraints: cluster is configured to have "
        "only one location '$0'", info.majority_location));
  }

  const auto& tablet_id = info.tablet_id;
  const auto location_num = ts_id_by_location.size();

  // If a total number of locations is 2, it's impossible to make its replica
  // distribution conform with the placement policy constraints.
  const auto& table_id = FindOrDie(tablets_info.tablet_to_table_id, tablet_id);
  const auto& table_info = FindOrDie(tablets_info.tables_info, table_id);

  // The replication factor of 2 is a special case: any possible replica
  // distribution is a violation of placement policy, and it's impossible
  // to fix it regardless of number of locations in the cluster.
  if (table_info.replication_factor == 2) {
    return Status::ConfigurationError(Substitute(
        "tablet $0 (table name '$1'): replica distribution cannot conform "
        "with the placement policy constraints since its replication "
        "factor is $2",
        tablet_id, table_info.name,
        table_info.replication_factor));
  }

  // There are a few edge cases which are most likely to occur, so let's have
  // a special error message for those. In these cases there are too few
  // locations relative to the replication factor, so it's impossible to find
  // any replica movements to satisfy the placement policy constraints.
  //
  // One interesting case placing replicas of a tablet with RF=4 in a cluster
  // with 3 locations. In that case, it's impossible to place the replicas to
  // satisfy the placement policy's constraints, since any possible replicas
  // placement does not allow to have the majority of the replicas online
  // if any single location becomes unavailable. Below is the all the possible
  // replica distributions for that case (modulo permutations of locations):
  // if the first location becomes unavailable, the majority of the replicas
  // is lost and the tablet becomes unavailable.
  //
  //   4 + 0 + 0
  //   3 + 1 + 0
  //   2 + 1 + 1
  //
  // Note that with 3 locations and higher replication factors (5, 6, etc.),
  // there is always a way to place tablet replicas to conform with the
  // restriction mentioned above.
  if (location_num == 2 ||
      (location_num == 3 && table_info.replication_factor == 4)) {
    return Status::ConfigurationError(Substitute(
        "tablet $0 (table name '$1'): replica distribution cannot conform "
        "with the placement policy constraints since its replication "
        "factor is $2 and there are $3 locations in the cluster",
        tablet_id, table_info.name,
        table_info.replication_factor, location_num));
  }

  const auto& location = info.majority_location;
  const auto& per_location_replica_info = FindOrDie(
      tablets_info.tablet_location_info, tablet_id);

  bool have_location_to_move_replica = false;
  if (per_location_replica_info.size() <
      locality_info.servers_by_location.size()) {
    // Have at least one extra location without replicas of the tablet.
    have_location_to_move_replica = true;
  } else {
    // Check if there is a location to host an extra replica of the tablet.
    for (const auto& elem : per_location_replica_info) {
      const auto& loc = elem.first;
      const auto loc_replica_count = elem.second;
      // Check whether the location can accomodate one more replica while not
      // becoming 'the majority' location.
      if (loc_replica_count + 1 >=
          consensus::MajoritySize(table_info.replication_factor)) {
        continue;
      }
      // The source location is among would-be-majority ones because it hosts
      // the majority of replicas already.
      CHECK_NE(location, loc) << Substitute("location '$0' must have "
                                            "the majority of replicas", loc);
      const auto& servers = FindOrDie(locality_info.servers_by_location, loc);
      if (servers.size() > loc_replica_count) {
        // There is an extra tablet server to host another replica in this
        // location.
        have_location_to_move_replica = true;
        break;
      }
    }
  }
  if (!have_location_to_move_replica) {
    return Status::NotFound(Substitute(
        "there isn't a single candidate location to move replica of tablet $0 "
        "from the majority location '$1'", tablet_id, info.majority_location));
  }

  // Identifiers of the tablet servers that host the candidate replicas to be
  // kicked out.
  vector<string> ts_id_candidates;

  // The identifier of the leader replica, if it's in the source location.
  string ts_id_leader_replica;

  const auto& tablet_info = FindOrDie(tablets_info.tablets_info, tablet_id);
  const auto& ts_at_location = FindOrDie(ts_id_by_location, location);
  for (const auto& replica_info : tablet_info.replicas_info) {
    const auto& ts_id = replica_info.ts_uuid;
    if (!ContainsKey(ts_at_location, ts_id)) {
      continue;
    }
    ts_id_candidates.emplace_back(ts_id);
    if (replica_info.role == ReplicaRole::LEADER) {
      DCHECK(ts_id_leader_replica.empty());
      ts_id_leader_replica = ts_id;
    }
  }

  CHECK(!ts_id_candidates.empty()) << Substitute(
      "no replicas found to remove from location '$0' to fix placement policy "
      "violation for tablet $1", location, tablet_id);

  // Build auxiliary map to find most loaded tablet servers.
  const auto& replica_num_by_ts_id = tablets_info.replica_num_by_ts_id;
  multimap<int32_t, string, std::greater<int32_t>> servers_by_replica_num;
  for (const auto& ts_id : ts_id_candidates) {
    const auto replica_num = FindOrDie(replica_num_by_ts_id, ts_id);
    servers_by_replica_num.emplace(replica_num, ts_id);
  }

  // Prefer most loaded tablet servers for the replica-to-remove candidates.
  string replica_id;
  for (const auto& elem : servers_by_replica_num) {
    replica_id = elem.second;
    // Prefer non-leader replicas for the replica-to-remove candidates: removing
    // a leader replica might require currently active clients to reconnect.
    if (replica_id != ts_id_leader_replica) {
      break;
    }
  }
  CHECK(!replica_id.empty());

  *replica_to_replace = { tablet_id, replica_id, "", tablet_info.config_idx };
  return Status::OK();
}

} // anonymous namespace


Status BuildTabletsPlacementInfo(const ClusterRawInfo& raw_info,
                                 TabletsPlacementInfo* info) {
  DCHECK(info);

  unordered_map<string, TableInfo> tables_info;
  for (const auto& table_summary : raw_info.table_summaries) {
    const auto& table_id = table_summary.id;
    TableInfo table_info{ table_summary.name, table_summary.replication_factor };
    EmplaceOrDie(&tables_info, table_id, std::move(table_info));
  }

  // Build utility map: tablet server identifier to location (it's used below).
  unordered_map<string, string> location_by_ts_id;
  for (const auto& summary : raw_info.tserver_summaries) {
    const auto& ts_id = summary.uuid;
    const auto& ts_location = summary.ts_location;
    VLOG(1) << "found tserver " << ts_id
            << " at location '" << ts_location << "'";
    EmplaceOrDie(&location_by_ts_id, ts_id, ts_location);
  }

  decltype(TabletsPlacementInfo::replica_num_by_ts_id) replica_num_by_ts_id;
  decltype(TabletsPlacementInfo::tablet_to_table_id) tablet_to_table_id;
  decltype(TabletsPlacementInfo::tablets_info) tablets_info;
  decltype(TabletsPlacementInfo::tablet_location_info) tablet_location_info;
  for (const auto& tablet_summary : raw_info.tablet_summaries) {
    const auto& tablet_id = tablet_summary.id;
    if (tablet_summary.result != KsckCheckResult::HEALTHY) {
      // TODO(aserbin): should this be reported as some transient condition
      //                to be taken into account? E.g., a tablet might be
      //                in process of copying data to a new replica to replace
      //                another replica which violates the placement policy.
      VLOG(1) << Substitute("tablet $0: not considering replicas for movement "
                            "since the tablet's status is '$1'",
                            tablet_id,
                            KsckCheckResultToString(tablet_summary.result));
      continue;
    }
    EmplaceOrDie(&tablet_to_table_id, tablet_id, tablet_summary.table_id);

    TabletInfo tablet_info;
    for (const auto& replica_info : tablet_summary.replicas) {
      TabletReplicaInfo info;
      info.ts_uuid = replica_info.ts_uuid;
      if (replica_info.is_leader) {
        info.role = ReplicaRole::LEADER;
      } else {
        info.role = replica_info.is_voter ? ReplicaRole::FOLLOWER_VOTER
                                          : ReplicaRole::FOLLOWER_NONVOTER;
      }
      if (replica_info.is_leader && replica_info.consensus_state) {
        const auto& cstate = *replica_info.consensus_state;
        if (cstate.opid_index) {
          tablet_info.config_idx = *cstate.opid_index;
        }
      }
      ++LookupOrEmplace(&replica_num_by_ts_id, replica_info.ts_uuid, 0);

      // Populate ClusterLocationInfo::tablet_location_info.
      auto& count_by_location = LookupOrEmplace(&tablet_location_info,
                                                tablet_id,
                                                unordered_map<string, int>());
      const auto& location = FindOrDie(location_by_ts_id, info.ts_uuid);
      ++LookupOrEmplace(&count_by_location, location, 0);
      tablet_info.replicas_info.emplace_back(std::move(info));
    }
    EmplaceOrDie(&tablets_info, tablet_id, std::move(tablet_info));
  }

  TabletsPlacementInfo result_info;
  result_info.tablets_info = std::move(tablets_info);
  result_info.tables_info = std::move(tables_info);
  result_info.tablet_to_table_id = std::move(tablet_to_table_id);
  result_info.tablet_location_info = std::move(tablet_location_info);
  result_info.replica_num_by_ts_id = std::move(replica_num_by_ts_id);
  *info = std::move(result_info);

  return Status::OK();
}

// Search for violations of placement policy given the information on tablet
// replica distribution in the cluster.
Status DetectPlacementPolicyViolations(
    const TabletsPlacementInfo& placement_info,
    vector<PlacementPolicyViolationInfo>* result_info) {
  DCHECK(result_info);

  // Information on tablets whose replicas violate placement policies.
  vector<PlacementPolicyViolationInfo> info;

  for (const auto& tablet_loc_elem : placement_info.tablet_location_info) {
    const auto& tablet_id = tablet_loc_elem.first;
    const auto& tablet_loc_info = tablet_loc_elem.second;

    const auto& table_id = FindOrDie(placement_info.tablet_to_table_id, tablet_id);
    const auto& table_info = FindOrDie(placement_info.tables_info, table_id);
    const auto rep_factor = table_info.replication_factor;

    // Maximum number of replicas, per location.
    int max_replicas_num = 0;
    string max_replicas_location;
    for (const auto& elem : tablet_loc_info) {
      const auto& location = elem.first;
      const auto replica_num = elem.second;
      if (max_replicas_num < replica_num) {
        max_replicas_num = replica_num;
        max_replicas_location = location;
      }
    }

    CHECK_GE(rep_factor, 1);
    DCHECK_GE(max_replicas_num, 1);
    DCHECK(!max_replicas_location.empty());
    const auto majority_size = consensus::MajoritySize(rep_factor);

    // The idea behind the placement policies is to keep the majority
    // of replicas alive if any single location fails.
    bool is_policy_violated = false;
    if (rep_factor % 2 == 0) {
      // In case of RF=2*N, losing at least N replicas means losing
      // the majority.
      if (rep_factor / 2 <= max_replicas_num) {
        is_policy_violated = true;
        LOG(INFO) << Substitute(
            "tablet $0: detected $1 of $2 replicas at location $3",
            tablet_id, max_replicas_num, rep_factor, max_replicas_location);
      }
    } else if (rep_factor > 1 && majority_size <= max_replicas_num) {
      // In case of RF=2*N+1, losing at least N+1 replicas means losing
      // the majority.
      is_policy_violated = true;
      LOG(INFO) << Substitute(
          "tablet $0: detected majority of replicas ($1 of $2) at location $3",
          tablet_id, max_replicas_num, rep_factor, max_replicas_location);
    }
    if (is_policy_violated) {
      info.push_back({ tablet_id, max_replicas_location, max_replicas_num });
    }
  }

  *result_info = std::move(info);

  return Status::OK();
}

// Find moves to correct policy violations. It's about finding the replicas to
// be marked with the REPLACE attribute: the catalog manager will do the rest
// to move the replicas elsewhere in accordance with the placement policies.
Status FindMovesToReimposePlacementPolicy(
    const TabletsPlacementInfo& placement_info,
    const ClusterLocalityInfo& locality_info,
    const vector<PlacementPolicyViolationInfo>& violations_info,
    std::vector<Rebalancer::ReplicaMove>* replicas_to_remove) {
  DCHECK(replicas_to_remove);
  if (violations_info.empty()) {
    replicas_to_remove->clear();
    return Status::OK();
  }

  vector<Rebalancer::ReplicaMove> best_moves;
  for (const auto& info : violations_info) {
    // There might be more than one move in total to fix a placement policy
    // violation for a tablet, but no more than one replica per tablet is moved
    // between locations at once. The process of correcting placement violations
    // is iterative: the number of iterations per tablet is limited by its
    // replication factor (precisely, the upper limit is RF/2).
    Rebalancer::ReplicaMove move;
    const auto s = FindBestReplicaToReplace(
        info, locality_info, placement_info, &move);
    if (s.IsConfigurationError()) {
      // Best effort to fix violations in case of misconfigured clusters.
      continue;
    }
    RETURN_NOT_OK(s);
    best_moves.emplace_back(std::move(move));
  }
  *replicas_to_remove = std::move(best_moves);

  return Status::OK();
}

} // namespace tools
} // namespace kudu
