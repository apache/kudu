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
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/rebalance/rebalancer.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rebalance {

struct ClusterLocalityInfo;

// Below are the structures to describe Kudu entities such as tablet replicas,
// tablets, and tables from the perspective of the placement policy constraints.

enum class ReplicaRole {
  LEADER,
  FOLLOWER_VOTER,
  FOLLOWER_NONVOTER,
};

struct TabletReplicaInfo {
  std::string ts_uuid;
  ReplicaRole role;
};

struct TabletInfo {
  std::vector<TabletReplicaInfo> replicas_info;
  boost::optional<int64_t> config_idx;  // For CAS-like change of Raft configs.
};

struct TableInfo {
  std::string name;
  int replication_factor;
};

// Information to describe tablet replica distribution on the cluster
// from the perspective of the placement policy constraints.
struct TabletsPlacementInfo {
  // Tablet replica distribution information among locations:
  // tablet_id --> { loc0: k, loc1: l, ..., locN: m }
  std::unordered_map<std::string, std::unordered_map<std::string, int>>
      tablet_location_info;

  // tablet_id --> tablet information
  std::unordered_map<std::string, TabletInfo> tablets_info;

  // table_id --> table information
  std::unordered_map<std::string, TableInfo> tables_info;

  // Dictionary: mapping tablet_id into its table_id.
  std::unordered_map<std::string, std::string> tablet_to_table_id;

  // tserver_id --> total number of tablet replicas at the tserver.
  std::unordered_map<std::string, int> replica_num_by_ts_id;
};

// Convert ClusterRawInfo into TabletsPlacementInfo. The 'moves_in_progress'
// parameter contains information on the replica moves which have been scheduled
// by a caller and still in progress: those are considered as successfully
// completed and applied to the 'raw_info' when building TabletsPlacementInfo
// for the specified 'raw_info' input.
Status BuildTabletsPlacementInfo(
    const ClusterRawInfo& raw_info,
    const Rebalancer::MovesInProgress& moves_in_progress,
    TabletsPlacementInfo* info);

// Information on a violation of the basic placement policy constraint.
// The basic constraint is: for any tablet, no location should contain
// the majority of the replicas of the tablet.
struct PlacementPolicyViolationInfo {
  std::string tablet_id;
  std::string majority_location;
  int replicas_num_at_majority_location;
};

// Given the information on replica placement in the cluster, detect violations
// of the placement policy constraints and output that information into the
// 'result_info' out parameter.
Status DetectPlacementPolicyViolations(
    const TabletsPlacementInfo& placement_info,
    std::vector<PlacementPolicyViolationInfo>* result_info);

// Given the information on tablet replica distribution for a cluster and
// list of placement policy violations, find "best candidate" replicas to move,
// reinstating the placement policy for corresponding tablets. The function
// outputs only one (and the best) candidate replica per reported violation,
// even if multiple replicas have to be moved to bring replica distribution
// in compliance with the policy. It's assumed that the process of reimposing
// the placement policy for the cluster is iterative, so it's necessary to
// call DetectPlacementPolicyViolations() followed by
// FindMovesToReimposePlacementPolicy() multiple times, until no policy
// violations are reported or no candidates are found.
Status FindMovesToReimposePlacementPolicy(
    const TabletsPlacementInfo& placement_info,
    const ClusterLocalityInfo& locality_info,
    const std::vector<PlacementPolicyViolationInfo>& violations_info,
    std::vector<Rebalancer::ReplicaMove>* replicas_to_remove);

} // namespace rebalance
} // namespace kudu
