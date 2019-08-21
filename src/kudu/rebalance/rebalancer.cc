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

#include "kudu/rebalance/rebalancer.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rebalance/placement_policy_util.h"
#include "kudu/rebalance/rebalance_algo.h"
#include "kudu/util/status.h"

using kudu::cluster_summary::HealthCheckResult;
using kudu::cluster_summary::HealthCheckResultToString;
using kudu::cluster_summary::ServerHealth;

using std::numeric_limits;
using std::set;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rebalance {

Rebalancer::Config::Config(
    vector<string> ignored_tservers_param,
    vector<string> master_addresses,
    vector<string> table_filters,
    size_t max_moves_per_server,
    size_t max_staleness_interval_sec,
    int64_t max_run_time_sec,
    bool move_rf1_replicas,
    bool output_replica_distribution_details,
    bool run_policy_fixer,
    bool run_cross_location_rebalancing,
    bool run_intra_location_rebalancing,
    double load_imbalance_threshold)
    : ignored_tservers(ignored_tservers_param.begin(), ignored_tservers_param.end()),
      master_addresses(std::move(master_addresses)),
      table_filters(std::move(table_filters)),
      max_moves_per_server(max_moves_per_server),
      max_staleness_interval_sec(max_staleness_interval_sec),
      max_run_time_sec(max_run_time_sec),
      move_rf1_replicas(move_rf1_replicas),
      output_replica_distribution_details(output_replica_distribution_details),
      run_policy_fixer(run_policy_fixer),
      run_cross_location_rebalancing(run_cross_location_rebalancing),
      run_intra_location_rebalancing(run_intra_location_rebalancing),
      load_imbalance_threshold(load_imbalance_threshold) {
  DCHECK_GE(max_moves_per_server, 0);
}

Rebalancer::Rebalancer(Config config)
    : config_(std::move(config)) {
}

// Given high-level description of moves, find tablets with replicas at the
// corresponding tablet servers to satisfy those high-level descriptions.
// The idea is to find all tablets of the specified table that would have a
// replica at the source server, but would not have a replica at the destination
// server. That is to satisfy the restriction of having no more than one replica
// of the same tablet per server.
//
// An additional constraint: it's better not to move leader replicas, if
// possible. If a client has a write operation in progress, moving leader
// replicas of affected tablets would make the client to re-resolve new leaders
// and retry the operations. Moving leader replicas is used as last resort
// when no other candidates are left.
Status Rebalancer::FindReplicas(const TableReplicaMove& move,
                                const ClusterRawInfo& raw_info,
                                vector<string>* tablet_ids) {
  const auto& table_id = move.table_id;

  // Tablet ids of replicas on the source tserver that are non-leaders.
  vector<string> tablet_uuids_src;
  // Tablet ids of replicas on the source tserver that are leaders.
  vector<string> tablet_uuids_src_leaders;
  // UUIDs of tablets of the selected table at the destination tserver.
  vector<string> tablet_uuids_dst;

  for (const auto& tablet_summary : raw_info.tablet_summaries) {
    if (tablet_summary.table_id != table_id) {
      continue;
    }
    if (tablet_summary.result != HealthCheckResult::HEALTHY) {
      VLOG(1) << Substitute("table $0: not considering replicas of tablet $1 "
                            "as candidates for movement since the tablet's "
                            "status is '$2'",
                            table_id, tablet_summary.id,
                            HealthCheckResultToString(tablet_summary.result));
      continue;
    }
    for (const auto& replica_summary : tablet_summary.replicas) {
      if (replica_summary.ts_uuid != move.from &&
          replica_summary.ts_uuid != move.to) {
        continue;
      }
      if (!replica_summary.ts_healthy) {
        VLOG(1) << Substitute("table $0: not considering replica movement "
                              "from $1 to $2 since server $3 is not healthy",
                              table_id,
                              move.from, move.to, replica_summary.ts_uuid);
        continue;
      }
      if (replica_summary.ts_uuid == move.from) {
        if (replica_summary.is_leader) {
          tablet_uuids_src_leaders.emplace_back(tablet_summary.id);
        } else {
          tablet_uuids_src.emplace_back(tablet_summary.id);
        }
      } else {
        DCHECK_EQ(move.to, replica_summary.ts_uuid);
        tablet_uuids_dst.emplace_back(tablet_summary.id);
      }
    }
  }
  sort(tablet_uuids_src.begin(), tablet_uuids_src.end());
  sort(tablet_uuids_dst.begin(), tablet_uuids_dst.end());

  vector<string> tablet_uuids;
  set_difference(
      tablet_uuids_src.begin(), tablet_uuids_src.end(),
      tablet_uuids_dst.begin(), tablet_uuids_dst.end(),
      inserter(tablet_uuids, tablet_uuids.begin()));

  if (!tablet_uuids.empty()) {
    // If there are tablets with non-leader replicas at the source server,
    // those are the best candidates for movement.
    tablet_ids->swap(tablet_uuids);
    return Status::OK();
  }

  // If no tablets with non-leader replicas were found, resort to tablets with
  // leader replicas at the source server.
  DCHECK(tablet_uuids.empty());
  sort(tablet_uuids_src_leaders.begin(), tablet_uuids_src_leaders.end());
  set_difference(
      tablet_uuids_src_leaders.begin(), tablet_uuids_src_leaders.end(),
      tablet_uuids_dst.begin(), tablet_uuids_dst.end(),
      inserter(tablet_uuids, tablet_uuids.begin()));

  tablet_ids->swap(tablet_uuids);

  return Status::OK();
}

void Rebalancer::FilterMoves(const MovesInProgress& scheduled_moves,
                             vector<ReplicaMove>* replica_moves) {
  unordered_set<string> tablet_uuids;
  vector<ReplicaMove> filtered_replica_moves;
  for (auto& move_op : *replica_moves) {
    const auto& tablet_uuid = move_op.tablet_uuid;
    if (ContainsKey(scheduled_moves, tablet_uuid)) {
      // There is a move operation in progress for the tablet, don't schedule
      // another one.
      continue;
    }
    if (PREDICT_TRUE(tablet_uuids.emplace(tablet_uuid).second)) {
      filtered_replica_moves.emplace_back(std::move(move_op));
    } else {
      // Rationale behind the unique tablet constraint: the implementation of
      // the Run() method is designed to re-order operations suggested by the
      // high-level algorithm to use the op-count-per-tablet-server capacity
      // as much as possible. Right now, the RunStep() method outputs only one
      // move operation per tablet in every batch. The code below is to
      // enforce the contract between Run() and RunStep() methods.
      LOG(DFATAL) << "detected multiple replica move operations for the same "
                     "tablet " << tablet_uuid;
    }
  }
  *replica_moves = std::move(filtered_replica_moves);
}

Status Rebalancer::FilterCrossLocationTabletCandidates(
    const unordered_map<string, string>& location_by_ts_id,
    const TabletsPlacementInfo& placement_info,
    const TableReplicaMove& move,
    vector<string>* tablet_ids) {
  DCHECK(tablet_ids);

  if (tablet_ids->empty()) {
    // Nothing to filter.
    return Status::OK();
  }

  const auto& dst_location = FindOrDie(location_by_ts_id, move.to);
  const auto& src_location = FindOrDie(location_by_ts_id, move.from);

  // Sanity check: the source and the destination tablet servers should be
  // in different locations.
  if (src_location == dst_location) {
    return Status::InvalidArgument(Substitute(
        "moving replicas of table $0: the same location '$1' for both "
        "the source ($2) and the destination ($3) tablet servers",
         move.table_id, src_location, move.from, move.to));
  }
  if (dst_location.empty()) {
    // The destination location is not specified, so no restrictions on the
    // destination location to check for.
    return Status::OK();
  }

  vector<string> tablet_ids_filtered;
  for (auto& tablet_id : *tablet_ids) {
    const auto& replica_count_info = FindOrDie(
        placement_info.tablet_location_info, tablet_id);
    const auto* count_ptr = FindOrNull(replica_count_info, dst_location);
    if (count_ptr == nullptr) {
      // Nothing else to clarify: not a single replica in the destnation
      // location for this candidate tablet.
      tablet_ids_filtered.emplace_back(std::move(tablet_id));
      continue;
    }
    const auto location_replica_num = *count_ptr;
    const auto& table_id = FindOrDie(placement_info.tablet_to_table_id, tablet_id);
    const auto& table_info = FindOrDie(placement_info.tables_info, table_id);
    const auto rf = table_info.replication_factor;
    // In case of RF=2*N+1, losing (N + 1) replicas means losing the majority.
    // In case of RF=2*N, losing at least N replicas means losing the majority.
    const auto replica_num_threshold = rf % 2 ? consensus::MajoritySize(rf)
                                              : rf / 2;
    if (location_replica_num + 1 >= replica_num_threshold) {
      VLOG(1) << Substitute("destination location '$0' for candidate tablet $1 "
                            "already contains $2 of $3 replicas",
                            dst_location, tablet_id, location_replica_num, rf);
      continue;
    }
    // No majority of replicas in the destination location: it's OK candidate.
    tablet_ids_filtered.emplace_back(std::move(tablet_id));
  }

  *tablet_ids = std::move(tablet_ids_filtered);

  return Status::OK();
}

Status Rebalancer::BuildClusterInfo(const ClusterRawInfo& raw_info,
                                    const MovesInProgress& moves_in_progress,
                                    ClusterInfo* info) const {
  DCHECK(info);

  // tserver UUID --> total replica count of all table's tablets at the server
  typedef unordered_map<string, int32_t> TableReplicasAtServer;

  // The result information to build.
  ClusterInfo result_info;

  unordered_map<string, int32_t> tserver_replicas_count;
  unordered_map<string, TableReplicasAtServer> table_replicas_info;

  // Build a set of tables with RF=1 (single replica tables).
  unordered_set<string> rf1_tables;
  if (!config_.move_rf1_replicas) {
    for (const auto& s : raw_info.table_summaries) {
      if (s.replication_factor == 1) {
        rf1_tables.emplace(s.id);
      }
    }
  }

  auto& ts_uuids_by_location = result_info.locality.servers_by_location;
  auto& location_by_ts_uuid = result_info.locality.location_by_ts_id;
  for (const auto& summary : raw_info.tserver_summaries) {
    const auto& ts_id = summary.uuid;
    const auto& ts_location = summary.ts_location;
    VLOG(1) << Substitute("found tserver $0 at location '$1'", ts_id, ts_location);
    EmplaceOrDie(&location_by_ts_uuid, ts_id, ts_location);
    auto& ts_ids = LookupOrEmplace(&ts_uuids_by_location,
                                   ts_location, set<string>());
    InsertOrDie(&ts_ids, ts_id);
  }

  for (const auto& s : raw_info.tserver_summaries) {
    if (s.health != ServerHealth::HEALTHY) {
      LOG(INFO) << Substitute("skipping tablet server $0 ($1) because of its "
                              "non-HEALTHY status ($2)",
                              s.uuid, s.address,
                              ServerHealthToString(s.health));
      continue;
    }
    tserver_replicas_count.emplace(s.uuid, 0);
  }

  for (const auto& tablet : raw_info.tablet_summaries) {
    if (!config_.move_rf1_replicas) {
      if (rf1_tables.find(tablet.table_id) != rf1_tables.end()) {
        LOG(INFO) << Substitute("tablet $0 of table '$1' ($2) has single replica, skipping",
                                tablet.id, tablet.table_name, tablet.table_id);
        continue;
      }
    }

    // Check if it's one of the tablets which are currently being rebalanced.
    // If so, interpret the move as successfully completed, updating the
    // replica counts correspondingly.
    const auto it_pending_moves = moves_in_progress.find(tablet.id);
    if (it_pending_moves != moves_in_progress.end()) {
      const auto& move_info = it_pending_moves->second;
      bool is_target_replica_present = false;
      // Verify that the target replica is present in the config.
      for (const auto& tr : tablet.replicas) {
        if (tr.ts_uuid == move_info.ts_uuid_to) {
          is_target_replica_present = true;
          break;
        }
      }
      // If the target replica is present, it will be processed in the code
      // below. Otherwise, it's necessary to pretend as if the target replica
      // is in the config already: the idea is to count in the absent target
      // replica as if the movement has successfully completed already.
      auto it = tserver_replicas_count.find(move_info.ts_uuid_to);
      if (!is_target_replica_present && !move_info.ts_uuid_to.empty() &&
          it != tserver_replicas_count.end()) {
        it->second++;
        auto table_ins = table_replicas_info.emplace(
            tablet.table_id, TableReplicasAtServer());
        TableReplicasAtServer& replicas_at_server = table_ins.first->second;

        auto replicas_ins = replicas_at_server.emplace(move_info.ts_uuid_to, 0);
        replicas_ins.first->second++;
      }
    }

    for (const auto& ri : tablet.replicas) {
      // Increment total count of replicas at the tablet server.
      auto it = tserver_replicas_count.find(ri.ts_uuid);
      if (it == tserver_replicas_count.end()) {
        string msg = Substitute("skipping replica at tserver $0", ri.ts_uuid);
        if (ri.ts_address) {
          msg += " (" + *ri.ts_address + ")";
        }
        msg += " since it's not reported among known tservers";
        LOG(INFO) << msg;
        continue;
      }
      bool do_count_replica = true;
      if (it_pending_moves != moves_in_progress.end()) {
        const auto& move_info = it_pending_moves->second;
        if (move_info.ts_uuid_from == ri.ts_uuid) {
          DCHECK(!ri.ts_uuid.empty());
          // The source replica of the scheduled replica movement operation
          // are still in the config. Interpreting the move as successfully
          // completed, so the source replica should not be counted in.
          do_count_replica = false;
        }
      }
      if (do_count_replica) {
        it->second++;
      }

      auto table_ins = table_replicas_info.emplace(
          tablet.table_id, TableReplicasAtServer());
      TableReplicasAtServer& replicas_at_server = table_ins.first->second;

      auto replicas_ins = replicas_at_server.emplace(ri.ts_uuid, 0);
      if (do_count_replica) {
        replicas_ins.first->second++;
      }
    }
  }

  // Check for the consistency of information derived from the health report.
  for (const auto& elem : tserver_replicas_count) {
    const auto& ts_uuid = elem.first;
    int32_t count_by_table_info = 0;
    for (auto& e : table_replicas_info) {
      count_by_table_info += e.second[ts_uuid];
    }
    if (elem.second != count_by_table_info) {
      return Status::Corruption("inconsistent cluster state returned by check");
    }
  }

  // Populate ClusterBalanceInfo::servers_by_total_replica_count
  auto& servers_by_count = result_info.balance.servers_by_total_replica_count;
  for (const auto& elem : tserver_replicas_count) {
    servers_by_count.emplace(elem.second, elem.first);
  }

  // Populate ClusterBalanceInfo::table_info_by_skew
  auto& table_info_by_skew = result_info.balance.table_info_by_skew;
  for (const auto& elem : table_replicas_info) {
    const auto& table_id = elem.first;
    int32_t max_count = numeric_limits<int32_t>::min();
    int32_t min_count = numeric_limits<int32_t>::max();
    TableBalanceInfo tbi;
    tbi.table_id = table_id;
    for (const auto& e : elem.second) {
      const auto& ts_uuid = e.first;
      const auto replica_count = e.second;
      tbi.servers_by_replica_count.emplace(replica_count, ts_uuid);
      max_count = std::max(replica_count, max_count);
      min_count = std::min(replica_count, min_count);
    }
    table_info_by_skew.emplace(max_count - min_count, std::move(tbi));
  }
  // TODO(aserbin): add sanity checks on the result.
  *info = std::move(result_info);

  return Status::OK();
}


} // namespace rebalance
} // namespace kudu
