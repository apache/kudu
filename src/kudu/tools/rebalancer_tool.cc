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

#include "kudu/tools/rebalancer_tool.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/master/master.pb.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rebalance/placement_policy_util.h"
#include "kudu/rebalance/rebalance_algo.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tools/tool_replica_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

using kudu::cluster_summary::ServerHealth;
using kudu::cluster_summary::ServerHealthSummary;
using kudu::cluster_summary::TableSummary;
using kudu::cluster_summary::TabletSummary;
using kudu::master::TServerStatePB;
using kudu::rebalance::BuildTabletExtraInfoMap;
using kudu::rebalance::ClusterInfo;
using kudu::rebalance::ClusterRawInfo;
using kudu::rebalance::PlacementPolicyViolationInfo;
using kudu::rebalance::Rebalancer;
using kudu::rebalance::SelectReplicaToMove;
using kudu::rebalance::ServersByCountMap;
using kudu::rebalance::TableBalanceInfo;
using kudu::rebalance::TableReplicaMove;
using kudu::rebalance::TabletExtraInfo;
using kudu::rebalance::TabletsPlacementInfo;

using std::accumulate;
using std::endl;
using std::back_inserter;
using std::inserter;
using std::ostream;
using std::map;
using std::pair;
using std::nullopt;
using std::optional;
using std::set;
using std::shared_ptr;
using std::sort;
using std::string;
using std::to_string;
using std::transform;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

RebalancerTool::RebalancerTool(const Config& config)
    : Rebalancer(config) {
}

Status RebalancerTool::PrintStats(ostream& out) {
  // First, report on the current balance state of the cluster.
  ClusterRawInfo raw_info;
  RETURN_NOT_OK(GetClusterRawInfo(nullopt, &raw_info));

  ClusterInfo ci;
  RETURN_NOT_OK(BuildClusterInfo(raw_info, MovesInProgress(), &ci));

  const auto& ts_id_by_location = ci.locality.servers_by_location;
  if (ts_id_by_location.empty()) {
    // Nothing to report about: there are no tablet servers reported.
    out << "an empty cluster" << endl;
    return Status::OK();
  }

  // Print information about tservers need to empty.
  RETURN_NOT_OK(PrintIgnoredTserversStats(raw_info, out));

  if (ts_id_by_location.size() == 1) {
    // That's about printing information about the whole cluster.
    return PrintLocationBalanceStats(ts_id_by_location.begin()->first,
                                     raw_info, ci, out);
  }

  // The stats are more detailed in the case of a multi-location cluster.
  DCHECK_GT(ts_id_by_location.size(), 1);

  // 1. Print information about cross-location balance.
  RETURN_NOT_OK(PrintCrossLocationBalanceStats(ci, out));

  // 2. Iterating over locations in the cluster, print per-location balance
  //    information. Since the ts_id_by_location is not sorted, let's first
  //    create a sorted list of locations so the ouput would be sorted by
  //    location.
  vector<string> locations;
  locations.reserve(ts_id_by_location.size());
  transform(ts_id_by_location.cbegin(), ts_id_by_location.cend(),
            back_inserter(locations),
            [](const unordered_map<string, set<string>>::value_type& elem) {
              return elem.first;
            });
  sort(locations.begin(), locations.end());

  for (const auto& location : locations) {
    shared_lock<decltype(ksck_lock_)> guard(ksck_lock_);
    ClusterRawInfo raw_info;
    RETURN_NOT_OK(KsckResultsToClusterRawInfo(location, ksck_->results(), &raw_info));
    ClusterInfo ci;
    RETURN_NOT_OK(BuildClusterInfo(raw_info, MovesInProgress(), &ci));
    RETURN_NOT_OK(PrintLocationBalanceStats(location, raw_info, ci, out));
  }

  // 3. Print information about placement policy violations.
  RETURN_NOT_OK(PrintPolicyViolationInfo(raw_info, out));

  return Status::OK();
}

Status RebalancerTool::Run(RunStatus* result_status, size_t* moves_count) {
  DCHECK(result_status);
  *result_status = RunStatus::UNKNOWN;

  optional<MonoTime> deadline;
  if (config_.max_run_time_sec != 0) {
    deadline = MonoTime::Now() + MonoDelta::FromSeconds(config_.max_run_time_sec);
  }

  ClusterRawInfo raw_info;
  {
    shared_lock<decltype(ksck_lock_)> guard(ksck_lock_);
    RETURN_NOT_OK(KsckResultsToClusterRawInfo(
        nullopt, ksck_->results(), &raw_info));
  }

  ClusterInfo ci;
  RETURN_NOT_OK(BuildClusterInfo(raw_info, MovesInProgress(), &ci));

  const auto& ts_id_by_location = ci.locality.servers_by_location;
  if (ts_id_by_location.empty()) {
    // Empty cluster: no tablet servers reported.
    if (moves_count != nullptr) {
      *moves_count = 0;
    }
    *result_status = RunStatus::CLUSTER_IS_BALANCED;
    LOG(INFO) << "no tablet servers are reported: nothing to balance";
    return Status::OK();
  }

  size_t moves_count_total = 0;
  if (config_.move_replicas_from_ignored_tservers) {
    // Move replicas from healthy ignored tservers to other healthy tservers.
    LOG(INFO) << "replacing replicas on healthy ignored tservers";
    IgnoredTserversRunner runner(
        this, config_.ignored_tservers, config_.max_moves_per_server, deadline);
    RETURN_NOT_OK(runner.Init(config_.master_addresses));
    RETURN_NOT_OK(RunWith(&runner, result_status));
    moves_count_total += runner.moves_count();
  }
  if (ts_id_by_location.size() == 1) {
    const auto& location = ts_id_by_location.cbegin()->first;
    const auto& table_filters = config_.table_filters;
    const auto& msg = table_filters.empty()
        ? "running whole-cluster rebalancing"
        : Substitute("running rebalancing for tables: $0",
                     JoinStrings(table_filters, ","));
    LOG(INFO) << msg;
    IntraLocationRunner runner(
        this, config_.ignored_tservers, config_.max_moves_per_server, deadline, location);
    RETURN_NOT_OK(runner.Init(config_.master_addresses));
    RETURN_NOT_OK(RunWith(&runner, result_status));
    moves_count_total += runner.moves_count();
  } else {
    // The essence of location-aware balancing:
    //   1. Find tablets whose replicas placed in such a way that their
    //      distribution violates the main constraint of the placement policy.
    //      For each non-conforming tablet, move its replicas to restore
    //      the placement policy restrictions. In other words, if a location has
    //      more than the majority of replicas for some tablet,
    //      move the replicas of the tablet to other locations.
    //   2. For every tablet whose replica placement does not violate the
    //      placement policy constraints, balance the load among locations.
    //   3. Balance replica distribution within every location. This is a.k.a.
    //      intra-location balancing. The intra-location balancing involves
    //      moving replicas only within location, no replicas are moved between
    //      locations.
    if (config_.run_policy_fixer) {
      // Fix placement policy violations, if any.
      LOG(INFO) << "fixing placement policy violations";
      PolicyFixer runner(
          this, config_.ignored_tservers, config_.max_moves_per_server, deadline);
      RETURN_NOT_OK(runner.Init(config_.master_addresses));
      RETURN_NOT_OK(RunWith(&runner, result_status));
      moves_count_total += runner.moves_count();
    }
    if (config_.run_cross_location_rebalancing) {
      // Run the rebalancing across locations (inter-location rebalancing).
      LOG(INFO) << "running cross-location rebalancing";
      CrossLocationRunner runner(this,
                                 config_.ignored_tservers,
                                 config_.max_moves_per_server,
                                 config_.load_imbalance_threshold,
                                 deadline);
      RETURN_NOT_OK(runner.Init(config_.master_addresses));
      RETURN_NOT_OK(RunWith(&runner, result_status));
      moves_count_total += runner.moves_count();
    }
    if (config_.run_intra_location_rebalancing && !ts_id_by_location.empty()) {
      const size_t locations_num = ts_id_by_location.size();
      DCHECK_GT(locations_num, 0);

      vector<RunStatus> location_run_status(locations_num, RunStatus::UNKNOWN);
      vector<Status> location_status(locations_num, Status::OK());
      vector<size_t> location_moves_count(locations_num, 0);
      vector<string> location_by_idx(locations_num);

      // Thread pool to run intra-location rebalancing tasks in parallel. Since
      // the location assignment provides non-intersecting sets of servers, it's
      // possible to independently move replicas within different locations.
      // The pool is automatically shutdown in its destructor.
      unique_ptr<ThreadPool> rebalance_pool;
      RETURN_NOT_OK(ThreadPoolBuilder("intra-location-rebalancing")
                    .set_trace_metric_prefix("rebalancer")
                    .set_max_threads(
                        config_.intra_location_rebalancing_concurrency == 0
                            ? base::NumCPUs()
                            : config_.intra_location_rebalancing_concurrency)
                    .Build(&rebalance_pool));

      // Run the rebalancing within every location (intra-location rebalancing).
      size_t location_idx = 0;
      for (const auto& elem : ts_id_by_location) {
        auto location = elem.first;
        location_by_idx[location_idx] = location;
        LOG(INFO) << Substitute(
            "starting rebalancing within location '$0'", location);
        RETURN_NOT_OK(rebalance_pool->Submit(
            [this, deadline, location = std::move(location),
             &config = std::as_const(config_),
             &location_status = location_status[location_idx],
             &location_moves_count = location_moves_count[location_idx],
             &location_run_status = location_run_status[location_idx]]() mutable {
          IntraLocationRunner runner(this,
                                     config.ignored_tservers,
                                     config.max_moves_per_server,
                                     deadline,
                                     std::move(location));
          if (const auto& s = runner.Init(config.master_addresses); !s.ok()) {
            location_status = s;
            return;
          }
          if (const auto& s = RunWith(&runner, &location_run_status); !s.ok()) {
            location_status = s;
            return;
          }
          location_moves_count = runner.moves_count();
        }));
        ++location_idx;
      }
      // Wait for the completion of the rebalancing process in every location.
      rebalance_pool->Wait();

      size_t location_balancing_moves = 0;
      Status status;
      RunStatus result_run_status = RunStatus::UNKNOWN;
      for (size_t location_idx = 0; location_idx < locations_num; ++location_idx) {
        // This 'for' cycle scope contains logic to compose the overall status
        // of the intra-location rebalancing based on the statuses of
        // the individual per-location rebalancing tasks.
        const auto& s = location_status[location_idx];
        if (s.ok()) {
          const auto rs = location_run_status[location_idx];
          DCHECK(rs != RunStatus::UNKNOWN);
          if (result_run_status == RunStatus::UNKNOWN ||
              result_run_status == RunStatus::CLUSTER_IS_BALANCED) {
            result_run_status = rs;
          }
          location_balancing_moves += location_moves_count[location_idx];
        } else {
          auto s_with_location_info = s.CloneAndPrepend(Substitute(
              "location $0", location_by_idx[location_idx]));
          if (status.ok()) {
            // Update the overall status to be first seen non-OK status.
            status = s_with_location_info;
          } else {
            // Update the overall status to add info on next non-OK status;
            status = status.CloneAndAppend(s_with_location_info.message());
          }
        }
      }
      // Check for the status and bail out if there was an error.
      RETURN_NOT_OK(status);

      moves_count_total += location_balancing_moves;
      *result_status = result_run_status;
    }
  }
  if (moves_count != nullptr) {
    *moves_count = moves_count_total;
  }

  return Status::OK();
}

Status RebalancerTool::KsckResultsToClusterRawInfo(const optional<string>& location,
                                                   const KsckResults& ksck_info,
                                                   ClusterRawInfo* raw_info) {
  DCHECK(raw_info);
  const auto& cluster_status = ksck_info.cluster_status;

  // Check whether all ignored tservers in the config are valid.
  if (!config_.ignored_tservers.empty()) {
    unordered_set<string> known_tservers;
    for (const auto& ts_summary : ksck_info.cluster_status.tserver_summaries) {
      known_tservers.emplace(ts_summary.uuid);
    }
    for (const auto& ts : config_.ignored_tservers) {
      if (!ContainsKey(known_tservers, ts)) {
        return Status::InvalidArgument(
            Substitute("ignored tserver $0 is not reported among known tservers", ts));
      }
    }
  }

  // Filter out entities that are not relevant to the specified location.
  vector<ServerHealthSummary> tserver_summaries;
  tserver_summaries.reserve(cluster_status.tserver_summaries.size());

  vector<TabletSummary> tablet_summaries;
  tablet_summaries.reserve(cluster_status.tablet_summaries.size());

  vector<TableSummary> table_summaries;
  table_summaries.reserve(cluster_status.table_summaries.size() +
                          cluster_status.system_table_summaries.size());

  if (!location) {
    // Information on the whole cluster.
    tserver_summaries = cluster_status.tserver_summaries;
    tablet_summaries = cluster_status.tablet_summaries;
    table_summaries = cluster_status.table_summaries;
    for (const auto& sys_table : cluster_status.system_table_summaries) {
      table_summaries.emplace_back(sys_table);
    }
  } else {
    // Information on the specified location only: filter out non-relevant info.
    const auto& location_str =  *location;

    unordered_set<string> ts_ids_at_location;
    for (const auto& summary : cluster_status.tserver_summaries) {
      if (summary.ts_location == location_str) {
        tserver_summaries.push_back(summary);
        InsertOrDie(&ts_ids_at_location, summary.uuid);
      }
    }

    unordered_set<string> table_ids_at_location;
    for (const auto& summary : cluster_status.tablet_summaries) {
      const auto& replicas = summary.replicas;
      decltype(summary.replicas) replicas_at_location;
      replicas_at_location.reserve(replicas.size());
      for (const auto& replica : replicas) {
        if (ContainsKey(ts_ids_at_location, replica.ts_uuid)) {
          replicas_at_location.push_back(replica);
        }
      }
      if (!replicas_at_location.empty()) {
        table_ids_at_location.insert(summary.table_id);
        tablet_summaries.push_back(summary);
        tablet_summaries.back().replicas = std::move(replicas_at_location);
      }
    }

    for (const auto& summary : cluster_status.table_summaries) {
      if (ContainsKey(table_ids_at_location, summary.id)) {
        table_summaries.push_back(summary);
      }
    }
    for (const auto& summary : cluster_status.system_table_summaries) {
      if (ContainsKey(table_ids_at_location, summary.id)) {
        table_summaries.push_back(summary);
      }
    }
  }

  unordered_set<string> tservers_in_maintenance_mode;
  for (const auto& ts : tserver_summaries) {
    if (ContainsKeyValuePair(ksck_info.ts_states, ts.uuid, TServerStatePB::MAINTENANCE_MODE)) {
      tservers_in_maintenance_mode.emplace(ts.uuid);
    }
  }

  raw_info->tserver_summaries = std::move(tserver_summaries);
  raw_info->table_summaries = std::move(table_summaries);
  raw_info->tablet_summaries = std::move(tablet_summaries);
  raw_info->tservers_in_maintenance_mode = std::move(tservers_in_maintenance_mode);

  return Status::OK();
}

Status RebalancerTool::PrintIgnoredTserversStats(const ClusterRawInfo& raw_info,
                                                 ostream& out) const {
  if (config_.ignored_tservers.empty() || !config_.move_replicas_from_ignored_tservers) {
    return Status::OK();
  }
  unordered_set<string> tservers_to_empty;
  GetTServersToEmpty(raw_info, &tservers_to_empty);
  TServersToEmptyMap tservers_to_empty_map;
  BuildTServersToEmptyInfo(raw_info, MovesInProgress(), tservers_to_empty, &tservers_to_empty_map);
  out << "Per-server replica distribution summary for tservers_to_empty:" << endl;
  DataTable summary({"Server UUID", "Replica Count"});
  for (const auto& ts : tservers_to_empty) {
    auto* tablets = FindOrNull(tservers_to_empty_map, ts);
    summary.AddRow({ts, tablets ? to_string(tablets->size()) : "0"});
  }
  RETURN_NOT_OK(summary.PrintTo(out));
  out << endl;

  return Status::OK();
}

Status RebalancerTool::PrintCrossLocationBalanceStats(const ClusterInfo& ci,
                                                      ostream& out) const {
  // Print location load information.
  map<string, int64_t> replicas_num_by_location;
  for (const auto& elem : ci.balance.servers_by_total_replica_count) {
    const auto& location = FindOrDie(ci.locality.location_by_ts_id, elem.second);
    LookupOrEmplace(&replicas_num_by_location, location, 0) += elem.first;
  }
  out << "Locations load summary:" << endl;
  DataTable location_load_summary({"Location", "Load"});
  for (const auto& elem : replicas_num_by_location) {
    const auto& location = elem.first;
    const auto servers_num =
        FindOrDie(ci.locality.servers_by_location, location).size();
    CHECK_GT(servers_num, 0);
    double location_load = static_cast<double>(elem.second) / servers_num;
    location_load_summary.AddRow({ location, to_string(location_load) });
  }
  RETURN_NOT_OK(location_load_summary.PrintTo(out));
  out << endl;

  return Status::OK();
}

Status RebalancerTool::PrintLocationBalanceStats(const string& location,
                                                 const ClusterRawInfo& raw_info,
                                                 const ClusterInfo& ci,
                                                 ostream& out) const {
  if (!location.empty()) {
    out << "--------------------------------------------------" << endl;
    out << "Location: " << location << endl;
    out << "--------------------------------------------------" << endl;
  }

  // Build dictionary to resolve tablet server UUID into its RPC address.
  unordered_map<string, string> tserver_endpoints;
  {
    const auto& tserver_summaries = raw_info.tserver_summaries;
    for (const auto& summary : tserver_summaries) {
      tserver_endpoints.emplace(summary.uuid, summary.address);
    }
  }

  // Per-server replica distribution stats.
  {
    out << "Per-server replica distribution summary:" << endl;
    DataTable summary({"Statistic", "Value"});

    const auto& servers_load_info = ci.balance.servers_by_total_replica_count;
    if (servers_load_info.empty()) {
      summary.AddRow({ "N/A", "N/A" });
    } else {
      const int64_t total_replica_count = accumulate(
          servers_load_info.begin(), servers_load_info.end(), 0L,
          [](int64_t sum, const pair<int32_t, string>& elem) {
            return sum + elem.first;
          });

      const auto min_replica_count = servers_load_info.begin()->first;
      const auto max_replica_count = servers_load_info.rbegin()->first;
      const double avg_replica_count =
          1.0 * total_replica_count / servers_load_info.size();

      summary.AddRow({ "Minimum Replica Count", to_string(min_replica_count) });
      summary.AddRow({ "Maximum Replica Count", to_string(max_replica_count) });
      summary.AddRow({ "Average Replica Count", to_string(avg_replica_count) });
    }
    RETURN_NOT_OK(summary.PrintTo(out));
    out << endl;

    if (config_.output_replica_distribution_details) {
      out << "Per-server replica distribution details:" << endl;
      DataTable servers_info({ "UUID", "Address", "Replica Count" });
      for (const auto& [load, id] : servers_load_info) {
        servers_info.AddRow({ id, tserver_endpoints[id], to_string(load) });
      }
      RETURN_NOT_OK(servers_info.PrintTo(out));
      out << endl;
    }
  }

  // Per-table replica distribution stats.
  {
    out << "Per-table replica distribution summary:" << endl;
    DataTable summary({ "Replica Skew", "Value" });
    const auto& table_skew_info = ci.balance.table_info_by_skew;
    if (table_skew_info.empty()) {
      summary.AddRow({ "N/A", "N/A" });
    } else {
      const auto min_table_skew = table_skew_info.begin()->first;
      const auto max_table_skew = table_skew_info.rbegin()->first;
      const int64_t sum_table_skew = accumulate(
          table_skew_info.begin(), table_skew_info.end(), 0L,
          [](int64_t sum, const pair<int32_t, TableBalanceInfo>& elem) {
            return sum + elem.first;
          });
      double avg_table_skew = 1.0 * sum_table_skew / table_skew_info.size();

      summary.AddRow({ "Minimum", to_string(min_table_skew) });
      summary.AddRow({ "Maximum", to_string(max_table_skew) });
      summary.AddRow({ "Average", to_string(avg_table_skew) });
    }
    RETURN_NOT_OK(summary.PrintTo(out));
    out << endl;

    if (config_.output_replica_distribution_details) {
      const auto& table_summaries = raw_info.table_summaries;
      unordered_map<string, const TableSummary*> table_info;
      for (const auto& summary : table_summaries) {
        table_info.emplace(summary.id, &summary);
      }
      if (config_.enable_range_rebalancing) {
        out << "Per-range replica distribution details for tables" << endl;

        // Build mapping {table_id, tag} --> per-server replica count map.
        // Using ordered dictionary since it's targeted for printing later.
        map<pair<string, string>, map<string, size_t>> range_dist_stats;
        for (const auto& [_, balance_info] : table_skew_info) {
          const auto& table_id = balance_info.table_id;
          const auto& tag = balance_info.tag;
          auto it = range_dist_stats.emplace(
              std::make_pair(table_id, tag), map<string, size_t>{});
          const auto& server_info = balance_info.servers_by_replica_count;
          for (const auto& [count, server_uuid] : server_info) {
            auto count_it = it.first->second.emplace(server_uuid, 0).first;
            count_it->second += count;
          }
        }

        // Build the mapping for the per-range skew summary table, i.e.
        // {tablet_id, tag} --> {num_of_replicas, per_server_replica_skew}.
        map<pair<string, string>, pair<size_t, size_t>> range_skew_stats;
        for (const auto& [table_range, per_server_stats] : range_dist_stats) {
          size_t total_count = 0;
          size_t min_per_server_count = std::numeric_limits<size_t>::max();
          size_t max_per_server_count = std::numeric_limits<size_t>::min();
          for (const auto& [server_uuid, replica_count] : per_server_stats) {
            total_count += replica_count;
            if (replica_count > max_per_server_count) {
              max_per_server_count = replica_count;
            }
            if (replica_count < min_per_server_count) {
              min_per_server_count = replica_count;
            }
          }
          size_t skew = max_per_server_count - min_per_server_count;
          range_skew_stats.emplace(table_range, std::make_pair(total_count, skew));
        }

        string prev_table_id;
        for (const auto& [table_info, per_server_stats] : range_dist_stats) {
          const auto& table_id = table_info.first;
          const auto& table_range = table_info.second;
          if (prev_table_id != table_id) {
            prev_table_id = table_id;
            out << endl << "Table: " << table_id << endl << endl;
            out << "Number of tablet replicas at servers for each range" << endl;
            DataTable range_skew_summary_table(
                { "Max Skew", "Total Count", "Range Start Key" });
            const auto it_begin = range_skew_stats.find(table_info);
            for (auto it = it_begin; it != range_skew_stats.end(); ++it) {
              const auto& cur_table_id = it->first.first;
              if (cur_table_id != table_id) {
                break;
              }
              const auto& range = it->first.second;
              const auto replica_count = it->second.first;
              const auto replica_skew = it->second.second;
              range_skew_summary_table.AddRow(
                  { to_string(replica_skew), to_string(replica_count), range });
            }
            RETURN_NOT_OK(range_skew_summary_table.PrintTo(out));
            out << endl;
          }
          out << "Range start key: '" << table_range << "'" << endl;
          DataTable skew_table({ "UUID", "Server address", "Replica Count" });
          for (const auto& stat : per_server_stats) {
            const auto& srv_uuid = stat.first;
            const auto& srv_address = FindOrDie(tserver_endpoints, srv_uuid);
            skew_table.AddRow({ srv_uuid, srv_address, to_string(stat.second) });
          }
          RETURN_NOT_OK(skew_table.PrintTo(out));
          out << endl;
        }
      } else {
        out << "Per-table replica distribution details:" << endl;
        DataTable skew_table(
            { "Table Id", "Replica Count", "Replica Skew", "Table Name" });
        for (const auto& [skew, balance_info] : table_skew_info) {
          const auto& table_id = balance_info.table_id;
          const auto it = table_info.find(table_id);
          const auto* table_summary =
              (it == table_info.end()) ? nullptr : it->second;
          const auto& table_name = table_summary ? table_summary->name : "";
          const auto total_replica_count = table_summary
              ? table_summary->replication_factor * table_summary->TotalTablets()
              : 0;
          skew_table.AddRow({ table_id,
                              to_string(total_replica_count),
                              to_string(skew),
                              table_name });
        }
        RETURN_NOT_OK(skew_table.PrintTo(out));
      }
      out << endl;
    }
  }

  return Status::OK();
}

Status RebalancerTool::PrintPolicyViolationInfo(const ClusterRawInfo& raw_info,
                                                ostream& out) const {
  TabletsPlacementInfo placement_info;
  RETURN_NOT_OK(BuildTabletsPlacementInfo(
      raw_info, MovesInProgress(), &placement_info));
  vector<PlacementPolicyViolationInfo> ppvi;
  RETURN_NOT_OK(DetectPlacementPolicyViolations(placement_info, &ppvi));
  out << "Placement policy violations:" << endl;
  if (ppvi.empty()) {
    out << "  none" << endl << endl;
    return Status::OK();
  }

  DataTable summary({ "Location",
                      "Number of non-complying tables",
                      "Number of non-complying tablets" });
  typedef pair<unordered_set<string>, unordered_set<string>> TableTabletIds;
  // Location --> sets of identifiers of tables and tablets hosted by the
  // tablet servers at the location. The summary is sorted by location.
  map<string, TableTabletIds> info_by_location;
  for (const auto& info : ppvi) {
    const auto& table_id = FindOrDie(placement_info.tablet_to_table_id,
                                     info.tablet_id);
    auto& elem = LookupOrEmplace(&info_by_location,
                                 info.majority_location, TableTabletIds());
    elem.first.emplace(table_id);
    elem.second.emplace(info.tablet_id);
  }
  for (const auto& elem : info_by_location) {
    summary.AddRow({ elem.first,
                     to_string(elem.second.first.size()),
                     to_string(elem.second.second.size()) });
  }
  RETURN_NOT_OK(summary.PrintTo(out));
  out << endl;
  // If requested, print details on detected policy violations.
  if (config_.output_replica_distribution_details) {
    out << "Placement policy violation details:" << endl;
    DataTable stats(
        { "Location", "Table Name", "Tablet", "RF", "Replicas at location" });
    for (const auto& info : ppvi) {
      const auto& table_id = FindOrDie(placement_info.tablet_to_table_id,
                                       info.tablet_id);
      const auto& table_info = FindOrDie(placement_info.tables_info, table_id);
      stats.AddRow({ info.majority_location,
                     table_info.name,
                     info.tablet_id,
                     to_string(table_info.replication_factor),
                     to_string(info.replicas_num_at_majority_location) });
    }
    RETURN_NOT_OK(stats.PrintTo(out));
    out << endl;
  }

  return Status::OK();
}

Status RebalancerTool::RunWith(Runner* runner, RunStatus* result_status) {
  const MonoDelta max_staleness_delta =
      MonoDelta::FromSeconds(config_.max_staleness_interval_sec);
  MonoTime staleness_start = MonoTime::Now();
  bool is_timed_out = false;
  bool resync_state = false;
  while (!is_timed_out) {
    if (resync_state) {
      resync_state = false;
      MonoDelta staleness_delta = MonoTime::Now() - staleness_start;
      if (staleness_delta > max_staleness_delta) {
        LOG(INFO) << Substitute("detected a staleness period of $0",
                                staleness_delta.ToString());
        return Status::Incomplete(Substitute(
            "stalled with no progress for more than $0 seconds, aborting",
            max_staleness_delta.ToString()));
      }
      // The actual re-synchronization happens during GetNextMoves() below:
      // updated info is collected from the cluster and fed into the algorithm.
      LOG(INFO) << "re-synchronizing cluster state";
    }

    bool has_more_moves = false;
    RETURN_NOT_OK(runner->GetNextMoves(&has_more_moves));
    if (!has_more_moves) {
      // No moves are left, done!
      break;
    }

    bool has_errors = false;
    while (!is_timed_out) {
      bool is_scheduled = runner->ScheduleNextMove(&has_errors, &is_timed_out);
      resync_state |= has_errors;
      if (resync_state || is_timed_out) {
        break;
      }
      if (is_scheduled) {
        // Reset the start of the staleness interval: there was some progress
        // in scheduling new move operations.
        staleness_start = MonoTime::Now();

        // Continue scheduling available move operations while there is enough
        // capacity, i.e. until number of pending move operations on every
        // involved tablet server reaches max_moves_per_server. Once no more
        // operations can be scheduled, it's time to check for their status.
        continue;
      }

      // Poll for the status of pending operations. If some of the in-flight
      // operations are completed, it might be possible to schedule new ones
      // by calling Runner::ScheduleNextMove().
      bool has_pending_moves = false;
      bool has_updates =
          runner->UpdateMovesInProgressStatus(&has_errors, &is_timed_out, &has_pending_moves);
      if (has_updates) {
        // Reset the start of the staleness interval: there was some updates
        // on the status of scheduled move operations.
        staleness_start = MonoTime::Now();
        // Continue scheduling available move operations.
        continue;
      }
      resync_state |= has_errors;
      if (resync_state || is_timed_out || !has_pending_moves) {
        // If there were errors while trying to get the statuses of pending
        // operations it's necessary to re-synchronize the state of the cluster:
        // most likely something has changed, so it's better to get a new set
        // of planned moves. Also, do the same if not a single pending move has left.
        break;
      }

      // Sleep a bit before going next cycle of status polling.
      SleepFor(MonoDelta::FromMilliseconds(200));
    }
  }

  *result_status = is_timed_out ? RunStatus::TIMED_OUT
                                : RunStatus::CLUSTER_IS_BALANCED;
  return Status::OK();
}

Status RebalancerTool::GetClusterRawInfo(const optional<string>& location,
                                         ClusterRawInfo* raw_info) {
  RETURN_NOT_OK(RefreshKsckResults());
  shared_lock<decltype(ksck_lock_)> guard(ksck_lock_);
  return KsckResultsToClusterRawInfo(location, ksck_->results(), raw_info);
}

Status RebalancerTool::RefreshKsckResults() {
  std::unique_lock<std::mutex> refresh_guard(ksck_refresh_lock_);
  if (ksck_refreshing_) {
    // Other thread is already refreshing the ksck info.
    ksck_refresh_cv_.wait(refresh_guard, [this]{ return !ksck_refreshing_; });
    return ksck_refresh_status_;
  }

  // This thread will be refreshing the ksck info.
  ksck_refreshing_ = true;
  refresh_guard.unlock();

  Status refresh_status;
  SCOPED_CLEANUP({
    refresh_guard.lock();
    ksck_refresh_status_ = refresh_status;
    ksck_refreshing_ = false;
    refresh_guard.unlock();
    ksck_refresh_cv_.notify_all();
  });
  shared_ptr<KsckCluster> cluster;
  const auto s = RemoteKsckCluster::Build(config_.master_addresses, &cluster);
  if (!s.ok()) {
    refresh_status = s.CloneAndPrepend("unable to build KsckCluster");
    return refresh_status;
  }
  cluster->set_table_filters(config_.table_filters);

  {
    unique_ptr<Ksck> new_ksck(new Ksck(cluster));
    ignore_result(new_ksck->Run());
    std::lock_guard<decltype(ksck_lock_)> guard(ksck_lock_);
    ksck_ = std::move(new_ksck);
  }
  return refresh_status;
}

RebalancerTool::BaseRunner::BaseRunner(RebalancerTool* rebalancer,
                                       std::unordered_set<std::string> ignored_tservers,
                                       size_t max_moves_per_server,
                                       optional<MonoTime> deadline)
    : rebalancer_(rebalancer),
      ignored_tservers_(std::move(ignored_tservers)),
      max_moves_per_server_(max_moves_per_server),
      deadline_(std::move(deadline)),
      moves_count_(0) {
  CHECK(rebalancer_);
}

Status RebalancerTool::BaseRunner::Init(vector<string> master_addresses) {
  DCHECK_EQ(0, moves_count_);
  DCHECK(op_count_per_ts_.empty());
  DCHECK(ts_per_op_count_.empty());
  DCHECK(master_addresses_.empty());
  DCHECK(client_.get() == nullptr);
  master_addresses_ = std::move(master_addresses);
  return CreateKuduClient(master_addresses_, &client_);
}

Status RebalancerTool::BaseRunner::GetNextMoves(bool* has_moves) {
  vector<Rebalancer::ReplicaMove> replica_moves;
  RETURN_NOT_OK(GetNextMovesImpl(&replica_moves));
  if (replica_moves.empty() && scheduled_moves_.empty()) {
    *has_moves = false;
    return Status::OK();
  }

  // The GetNextMovesImpl() method prescribes replica movements using simplified
  // logic that doesn't know about best practices of safe and robust Raft
  // configuration changes. Here it's necessary to filter out moves for tablets
  // which already have operations in progress. The idea is simple: don't start
  // another operation for a tablet when there is still a pending operation
  // for that tablet.
  Rebalancer::FilterMoves(scheduled_moves_, &replica_moves);
  LoadMoves(std::move(replica_moves));

  // TODO(aserbin): this method reports on availability of move operations
  //                via the 'has_moves' parameter even if all of those were
  //                actually filtered out by the FilterMoves() method.
  //                Would it be more convenient to report only on the new,
  //                not-yet-in-progress operations and check for the presence
  //                of the scheduled moves at the upper level?
  *has_moves = true;
  return Status::OK();
}

void RebalancerTool::BaseRunner::UpdateOnMoveCompleted(const string& ts_uuid) {
  const auto op_count = op_count_per_ts_[ts_uuid]--;
  const auto op_range = ts_per_op_count_.equal_range(op_count);
  bool ts_per_op_count_updated = false;
  for (auto it = op_range.first; it != op_range.second; ++it) {
    if (it->second == ts_uuid) {
      ts_per_op_count_.erase(it);
      ts_per_op_count_.emplace(op_count - 1, ts_uuid);
      ts_per_op_count_updated = true;
      break;
    }
  }
  DCHECK(ts_per_op_count_updated);
}

Status RebalancerTool::BaseRunner::CheckTabletServers(const ClusterRawInfo& raw_info) {
  // For simplicity, allow to run the rebalancing only when all tablet servers
  // are in good shape (except those specified in 'ignored_tservers').
  // Otherwise, the rebalancing might interfere with the
  // automatic re-replication or get unexpected errors while moving replicas.
  for (const auto& s : raw_info.tserver_summaries) {
    if (s.health != ServerHealth::HEALTHY && !ContainsKey(ignored_tservers_, s.uuid)) {
      return Status::IllegalState(Substitute("tablet server $0 ($1): unacceptable health status $2",
                                             s.uuid,
                                             s.address,
                                             ServerHealthToString(s.health)));
    }
  }
  if (rebalancer_->config_.force_rebalance_replicas_on_maintenance_tservers) {
    return Status::OK();
  }
  // Avoid moving replicas to tablet servers that are set maintenance mode.
  for (const string& ts_uuid : raw_info.tservers_in_maintenance_mode) {
    if (!ContainsKey(ignored_tservers_, ts_uuid)) {
      return Status::IllegalState(
          Substitute("tablet server $0: unacceptable state MAINTENANCE_MODE.\n"
                     "You can continue rebalancing in one of the following ways:\n"
                     "1. Set the tserver uuid into the '--ignored_tservers' flag to ignored it.\n"
                     "2. Set '--force_rebalance_replicas_on_maintenance_tservers' to force "
                     "rebalancing replicas among all known tservers.\n"
                     "3. Exit maintenance mode on the tserver.",
                     ts_uuid));
    }
  }
  return Status::OK();
}

RebalancerTool::AlgoBasedRunner::AlgoBasedRunner(
    RebalancerTool* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    optional<MonoTime> deadline)
    : BaseRunner(rebalancer,
                 std::move(ignored_tservers),
                 max_moves_per_server,
                 std::move(deadline)),
      random_generator_(random_device_()) {
}

Status RebalancerTool::AlgoBasedRunner::Init(vector<string> master_addresses) {
  DCHECK(src_op_indices_.empty());
  DCHECK(dst_op_indices_.empty());
  DCHECK(scheduled_moves_.empty());
  return BaseRunner::Init(std::move(master_addresses));
}

void RebalancerTool::AlgoBasedRunner::LoadMoves(vector<Rebalancer::ReplicaMove> replica_moves) {
  // The moves to schedule (used by subsequent calls to ScheduleNextMove()).
  replica_moves_.swap(replica_moves);

  // Prepare helper containers.
  src_op_indices_.clear();
  dst_op_indices_.clear();
  op_count_per_ts_.clear();
  ts_per_op_count_.clear();

  // If there are any scheduled moves, it's necessary to count them in
  // to properly handle the 'maximum moves per server' constraint.
  unordered_map<string, int32_t> ts_pending_op_count;
  for (auto it = scheduled_moves_.begin(); it != scheduled_moves_.end(); ++it) {
    ++ts_pending_op_count[it->second.ts_uuid_from];
    ++ts_pending_op_count[it->second.ts_uuid_to];
  }

  // These two references is to make the compiler happy with the lambda below.
  auto& op_count_per_ts = op_count_per_ts_;
  auto& ts_per_op_count = ts_per_op_count_;
  const auto set_op_count = [&ts_pending_op_count,
      &op_count_per_ts, &ts_per_op_count](const string& ts_uuid) {
    auto it = ts_pending_op_count.find(ts_uuid);
    if (it == ts_pending_op_count.end()) {
      // No operations for tablet server ts_uuid yet.
      if (op_count_per_ts.emplace(ts_uuid, 0).second) {
        ts_per_op_count.emplace(0, ts_uuid);
      }
    } else {
      // There are pending operations for tablet server ts_uuid: set the number
      // operations at the tablet server ts_uuid as calculated above with
      // ts_pending_op_count.
      if (op_count_per_ts.emplace(ts_uuid, it->second).second) {
        ts_per_op_count.emplace(it->second, ts_uuid);
      }
      // Once set into op_count_per_ts and ts_per_op_count, this information
      // is no longer needed. In addition, these elements are removed to leave
      // only pending operations those do not intersect with the batch of newly
      // loaded operations.
      ts_pending_op_count.erase(it);
    }
  };

  // Process move operations from the batch of newly loaded ones.
  for (size_t i = 0; i < replica_moves_.size(); ++i) {
    const auto& elem = replica_moves_[i];
    src_op_indices_.emplace(elem.ts_uuid_from, set<size_t>()).first->
        second.emplace(i);
    set_op_count(elem.ts_uuid_from);

    dst_op_indices_.emplace(elem.ts_uuid_to, set<size_t>()).first->
        second.emplace(i);
    set_op_count(elem.ts_uuid_to);
  }

  // Process pending/scheduled move operations which do not intersect
  // with the batch of newly loaded ones.
  for (const auto& elem : ts_pending_op_count) {
    auto op_inserted = op_count_per_ts.emplace(elem.first, elem.second).second;
    DCHECK(op_inserted);
    ts_per_op_count.emplace(elem.second, elem.first);
  }
}

bool RebalancerTool::AlgoBasedRunner::ScheduleNextMove(bool* has_errors,
                                                       bool* timed_out) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  *has_errors = false;
  *timed_out = false;

  if (deadline_ && MonoTime::Now() >= *deadline_) {
    *timed_out = true;
    return false;
  }

  // Scheduling one operation per step. Once operation is scheduled, it's
  // necessary to update the ts_per_op_count_ container right after scheduling
  // to avoid oversubscribing of the tablet servers.
  size_t op_idx;
  if (!FindNextMove(&op_idx)) {
    // Nothing to schedule yet: unfruitful outcome. Need to wait until there is
    // an available slot at a tablet server.
    return false;
  }

  // Try to schedule next move operation.
  DCHECK_LT(op_idx, replica_moves_.size());
  const auto& info = replica_moves_[op_idx];
  const auto& tablet_id = info.tablet_uuid;
  const auto& src_ts_uuid = info.ts_uuid_from;
  const auto& dst_ts_uuid = info.ts_uuid_to;

  Status s = ScheduleReplicaMove(master_addresses_, client_,
                                 tablet_id, src_ts_uuid, dst_ts_uuid);
  if (s.ok()) {
    UpdateOnMoveScheduled(op_idx, tablet_id, src_ts_uuid, dst_ts_uuid, true);
    LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move scheduled",
                            tablet_id, src_ts_uuid, dst_ts_uuid);
    // Successfully scheduled move operation.
    return true;
  }

  // The source replica is not found in the tablet's consensus config
  // or the tablet does not exit anymore. The replica might already
  // moved because of some other concurrent activity, e.g.
  // re-replication, another rebalancing session in progress, etc.
  LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move ignored: $3",
                          tablet_id, src_ts_uuid, dst_ts_uuid, s.ToString());
  UpdateOnMoveScheduled(op_idx, tablet_id, src_ts_uuid, dst_ts_uuid, false);
  // Failed to schedule move operation due to an error.
  *has_errors = true;
  return false;
}

bool RebalancerTool::AlgoBasedRunner::UpdateMovesInProgressStatus(
    bool* has_errors, bool* timed_out, bool* has_pending_moves) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  DCHECK(has_pending_moves);

  // Update the statuses of the in-progress move operations.
  auto has_updates = false;
  auto error_count = 0;
  for (auto it = scheduled_moves_.begin(); it != scheduled_moves_.end(); ) {
    if (deadline_ && MonoTime::Now() >= *deadline_) {
      *timed_out = true;
      break;
    }
    const auto& tablet_id = it->first;
    DCHECK_EQ(tablet_id, it->second.tablet_uuid);
    const auto& src_ts_uuid = it->second.ts_uuid_from;
    const auto& dst_ts_uuid = it->second.ts_uuid_to;
    auto is_complete = false;
    Status move_status;
    const Status s = CheckCompleteMove(master_addresses_, client_,
                                       tablet_id, src_ts_uuid, dst_ts_uuid,
                                       &is_complete, &move_status);
    *has_pending_moves |= s.ok();
    if (!s.ok()) {
      // There was an error while fetching the status of this move operation.
      // Since the actual status of the move is not known, don't update the
      // stats on pending operations per server. The higher-level should handle
      // this situation after returning from this method, re-synchronizing
      // the state of the cluster.
      ++error_count;
      LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move is abandoned: $3",
                              tablet_id, src_ts_uuid, dst_ts_uuid, s.ToString());
      // Erase the element and advance the iterator.
      it = scheduled_moves_.erase(it);
      continue;
    }
    if (is_complete) {
      // The move has completed (success or failure): update the stats on the
      // pending operations per server.
      ++moves_count_;
      has_updates = true;
      UpdateOnMoveCompleted(it->second.ts_uuid_from);
      UpdateOnMoveCompleted(it->second.ts_uuid_to);
      LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move completed: $3",
                              tablet_id, src_ts_uuid, dst_ts_uuid,
                              move_status.ToString());
      // Erase the element and advance the iterator.
      it = scheduled_moves_.erase(it);
      continue;
    }
    // There was an update on the status of the move operation and it hasn't
    // completed yet. Let's poll for the status of the rest.
    ++it;
  }
  *has_errors = (error_count != 0);
  return has_updates;
}

// Run one step of the rebalancer. Due to the inherent restrictions of the
// rebalancing engine, no more than one replica per tablet is moved during
// one step of the rebalancing.
Status RebalancerTool::AlgoBasedRunner::GetNextMovesImpl(
    vector<Rebalancer::ReplicaMove>* replica_moves) {
  const auto& loc = location();
  ClusterRawInfo raw_info;
  RETURN_NOT_OK(rebalancer_->GetClusterRawInfo(loc, &raw_info));
  RETURN_NOT_OK(CheckTabletServers(raw_info));

  TabletsPlacementInfo tpi;
  if (!loc) {
    RETURN_NOT_OK(BuildTabletsPlacementInfo(raw_info, scheduled_moves_, &tpi));
  }

  unordered_map<string, TabletExtraInfo> extra_info_by_tablet_id;
  BuildTabletExtraInfoMap(raw_info, &extra_info_by_tablet_id);

  // The number of operations to output by the algorithm. Those will be
  // translated into concrete tablet replica movement operations, the output of
  // this method.
  const size_t max_moves = max_moves_per_server_ *
      raw_info.tserver_summaries.size() * 5;

  replica_moves->clear();
  vector<TableReplicaMove> moves;
  ClusterInfo cluster_info;
  RETURN_NOT_OK(rebalancer_->BuildClusterInfo(
      raw_info, scheduled_moves_, &cluster_info));
  RETURN_NOT_OK(algorithm()->GetNextMoves(cluster_info, max_moves, &moves));
  if (moves.empty()) {
    // No suitable moves were found: the cluster described by the 'cluster_info'
    // is balanced, assuming the pending moves, if any, will succeed.
    return Status::OK();
  }
  unordered_set<string> tablets_in_move;
  transform(scheduled_moves_.begin(), scheduled_moves_.end(),
            inserter(tablets_in_move, tablets_in_move.begin()),
            [](const Rebalancer::MovesInProgress::value_type& elem) {
              return elem.first;
            });
  for (const auto& move : moves) {
    vector<string> tablet_ids;
    rebalancer_->FindReplicas(move, raw_info, &tablet_ids);
    if (!loc) {
      // In case of cross-location (a.k.a. inter-location) rebalancing it is
      // necessary to make sure the majority of replicas would not end up
      // at the same location after the move. If so, remove those tablets
      // from the list of candidates.
      RETURN_NOT_OK(FilterCrossLocationTabletCandidates(
          cluster_info.locality.location_by_ts_id, tpi, move, &tablet_ids));
    }
    // This will return Status::NotFound if no replica can be moved.
    // In that case, we just continue through the loop.
    WARN_NOT_OK(SelectReplicaToMove(move, extra_info_by_tablet_id,
                                    &random_generator_, std::move(tablet_ids),
                                    &tablets_in_move, replica_moves),
                "No replica could be moved this iteration");
  }

  return Status::OK();
}

bool RebalancerTool::AlgoBasedRunner::FindNextMove(size_t* op_idx) {
  vector<size_t> op_indices;
  for (auto it = ts_per_op_count_.begin(); op_indices.empty() &&
       it != ts_per_op_count_.end() && it->first < max_moves_per_server_; ++it) {
    const auto& uuid_0 = it->second;

    auto it_1 = it;
    ++it_1;
    for (; op_indices.empty() && it_1 != ts_per_op_count_.end() &&
         it_1->first < max_moves_per_server_; ++it_1) {
      const auto& uuid_1 = it_1->second;

      // Check for available operations where uuid_0, uuid_1 would be
      // source or destination servers correspondingly.
      {
        const auto it_src = src_op_indices_.find(uuid_0);
        const auto it_dst = dst_op_indices_.find(uuid_1);
        if (it_src != src_op_indices_.end() &&
            it_dst != dst_op_indices_.end()) {
          set_intersection(it_src->second.begin(), it_src->second.end(),
                           it_dst->second.begin(), it_dst->second.end(),
                           back_inserter(op_indices));
        }
      }
      // It's enough to find just one move.
      if (!op_indices.empty()) {
        break;
      }
      {
        const auto it_src = src_op_indices_.find(uuid_1);
        const auto it_dst = dst_op_indices_.find(uuid_0);
        if (it_src != src_op_indices_.end() &&
            it_dst != dst_op_indices_.end()) {
          set_intersection(it_src->second.begin(), it_src->second.end(),
                           it_dst->second.begin(), it_dst->second.end(),
                           back_inserter(op_indices));
        }
      }
    }
  }
  if (!op_indices.empty() && op_idx) {
    *op_idx = op_indices.front();
  }
  return !op_indices.empty();
}

void RebalancerTool::AlgoBasedRunner::UpdateOnMoveScheduled(
    size_t idx,
    const string& tablet_uuid,
    const string& src_ts_uuid,
    const string& dst_ts_uuid,
    bool is_success) {
  if (is_success) {
    Rebalancer::ReplicaMove move_info = { tablet_uuid, src_ts_uuid, dst_ts_uuid };
    // Only one replica of a tablet can be moved at a time.
    EmplaceOrDie(&scheduled_moves_, tablet_uuid, std::move(move_info));
  }
  UpdateOnMoveScheduledImpl(idx, src_ts_uuid, is_success, &src_op_indices_);
  UpdateOnMoveScheduledImpl(idx, dst_ts_uuid, is_success, &dst_op_indices_);
}

void RebalancerTool::AlgoBasedRunner::UpdateOnMoveScheduledImpl(
    size_t idx,
    const string& ts_uuid,
    bool is_success,
    std::unordered_map<std::string, std::set<size_t>>* op_indices) {
  DCHECK(op_indices);
  auto& indices = (*op_indices)[ts_uuid];
  auto erased = indices.erase(idx);
  DCHECK_EQ(1, erased);
  if (indices.empty()) {
    op_indices->erase(ts_uuid);
  }
  if (is_success) {
    const auto op_count = op_count_per_ts_[ts_uuid]++;
    const auto op_range = ts_per_op_count_.equal_range(op_count);
    bool ts_op_count_updated = false;
    for (auto it = op_range.first; it != op_range.second; ++it) {
      if (it->second == ts_uuid) {
        ts_per_op_count_.erase(it);
        ts_per_op_count_.emplace(op_count + 1, ts_uuid);
        ts_op_count_updated = true;
        break;
      }
    }
    DCHECK(ts_op_count_updated);
  }
}

RebalancerTool::IntraLocationRunner::IntraLocationRunner(
    RebalancerTool* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    optional<MonoTime> deadline,
    std::string location)
    : AlgoBasedRunner(rebalancer,
                      std::move(ignored_tservers),
                      max_moves_per_server,
                      std::move(deadline)),
      location_(std::move(location)) {
}

RebalancerTool::CrossLocationRunner::CrossLocationRunner(
    RebalancerTool* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    double load_imbalance_threshold,
    optional<MonoTime> deadline)
    : AlgoBasedRunner(rebalancer,
                      std::move(ignored_tservers),
                      max_moves_per_server,
                      std::move(deadline)),
      algorithm_(load_imbalance_threshold) {
}

RebalancerTool::ReplaceBasedRunner::ReplaceBasedRunner(
    RebalancerTool* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    optional<MonoTime> deadline)
    : BaseRunner(rebalancer,
                 std::move(ignored_tservers),
                 max_moves_per_server,
                 std::move(deadline)) {
}

Status RebalancerTool::ReplaceBasedRunner::Init(vector<string> master_addresses) {
  DCHECK(moves_to_schedule_.empty());
  return BaseRunner::Init(std::move(master_addresses));
}

void RebalancerTool::ReplaceBasedRunner::LoadMoves(
    vector<Rebalancer::ReplicaMove> replica_moves) {
  // Replace the list of moves operations to schedule. Even if it's not empty,
  // some elements of it might be irrelevant anyway, so there is no need to
  // keep any since the new information is the most up-to-date. The input list
  // is already filtered and should not contain any operations which are
  // tracked as already scheduled ones.
  moves_to_schedule_.clear();

  for (auto& move_info : replica_moves) {
    auto ts_uuid = move_info.ts_uuid_from;
    DCHECK(!ts_uuid.empty());
    moves_to_schedule_.emplace(std::move(ts_uuid), std::move(move_info));
  }

  // Refresh the helper containers.
  for (const auto& elem : moves_to_schedule_) {
    const auto& ts_uuid = elem.first;
    DCHECK(!ts_uuid.empty());
    if (op_count_per_ts_.emplace(ts_uuid, 0).second) {
      // No operations for tablet server ts_uuid: add ts_per_op_count_ entry.
      ts_per_op_count_.emplace(0, ts_uuid);
    }
  }
}

bool RebalancerTool::ReplaceBasedRunner::ScheduleNextMove(bool* has_errors,
                                                          bool* timed_out) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  *has_errors = false;
  *timed_out = false;

  if (deadline_ && MonoTime::Now() >= *deadline_) {
    *timed_out = true;
    return false;
  }

  Rebalancer::ReplicaMove move_info;
  if (!FindNextMove(&move_info)) {
    return false;
  }

  // Find a move that doesn't have its tserver UUID in scheduled_moves_.
  const auto s = SetReplace(client_,
                            move_info.tablet_uuid,
                            move_info.ts_uuid_from,
                            move_info.config_opid_idx);
  if (!s.ok()) {
    *has_errors = true;
    return false;
  }
  LOG(INFO) << Substitute("tablet $0: '$1' -> '?' move scheduled",
                          move_info.tablet_uuid, move_info.ts_uuid_from);
  UpdateOnMoveScheduled(std::move(move_info));
  return true;
}

bool RebalancerTool::ReplaceBasedRunner::UpdateMovesInProgressStatus(
    bool* has_errors, bool* timed_out, bool* has_pending_moves) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  DCHECK(has_pending_moves);

  // Update the statuses of the in-progress move operations.
  auto has_updates = false;
  auto error_count = 0;
  for (auto it = scheduled_moves_.begin(); it != scheduled_moves_.end(); ) {
    if (deadline_ && MonoTime::Now() >= *deadline_) {
      *timed_out = true;
      break;
    }
    bool is_complete;
    Status completion_status;
    const auto& tablet_id = it->second.tablet_uuid;
    const auto& ts_uuid = it->second.ts_uuid_from;
    auto s = CheckCompleteReplace(client_, tablet_id, ts_uuid,
                                  &is_complete, &completion_status);
    *has_pending_moves |= s.ok();
    if (!s.ok()) {
      // Update on the movement status has failed: remove the move operation
      // as if it didn't exist. Once the cluster status is re-synchronized,
      // the corresponding operation will be scheduled again, if needed.
      ++error_count;
      LOG(INFO) << Substitute("tablet $0: '$1' -> '?' move is abandoned: $2",
                              tablet_id, ts_uuid, s.ToString());
      it = scheduled_moves_.erase(it);
      continue;
    }
    if (is_complete) {
      // The replacement has completed (success or failure): update the stats
      // on the pending operations per server.
      ++moves_count_;
      LOG(INFO) << Substitute("tablet $0: '$1' -> '?' move completed: $2",
                              tablet_id, ts_uuid, completion_status.ToString());
      UpdateOnMoveCompleted(ts_uuid);
      it = scheduled_moves_.erase(it);
      continue;
    }
    ++it;
  }

  *has_errors = (error_count > 0);
  return has_updates;
}

Status RebalancerTool::ReplaceBasedRunner::GetNextMovesImpl(
    vector<Rebalancer::ReplicaMove>* replica_moves) {
  ClusterRawInfo raw_info;
  RETURN_NOT_OK(rebalancer_->GetClusterRawInfo(nullopt, &raw_info));
  RETURN_NOT_OK(CheckTabletServers(raw_info));

  ClusterInfo ci;
  RETURN_NOT_OK(rebalancer_->BuildClusterInfo(raw_info, scheduled_moves_, &ci));
  return GetReplaceMoves(ci, raw_info, replica_moves);
}

bool RebalancerTool::ReplaceBasedRunner::FindNextMove(Rebalancer::ReplicaMove* move) {
  DCHECK(move);
  // use pessimistic /2 limit for max_moves_per_server_ since the
  // desitnation servers for the move of the replica marked with
  // the REPLACE attribute is not known.

  // Load the least loaded (in terms of scheduled moves) tablet servers first.
  for (auto it = ts_per_op_count_.begin(); it != ts_per_op_count_.end() &&
       it->first <= max_moves_per_server_ / 2; ++it) {
    const auto& ts_uuid = it->second;
    if (FindCopy(moves_to_schedule_, ts_uuid, move)) {
      return true;
    }
  }
  return false;
}

void RebalancerTool::ReplaceBasedRunner::UpdateOnMoveScheduled(Rebalancer::ReplicaMove move) {
  const auto tablet_uuid = move.tablet_uuid;
  const auto ts_uuid = move.ts_uuid_from;

  // Add information on scheduled move into the scheduled_moves_.
  // Only one replica of a tablet can be moved at a time.
  EmplaceOrDie(&scheduled_moves_, tablet_uuid, std::move(move));

  // Remove the element from moves_to_schedule_.
  bool erased = false;
  auto range = moves_to_schedule_.equal_range(ts_uuid);
  for (auto it = range.first; it != range.second; ++it) {
    if (tablet_uuid == it->second.tablet_uuid) {
      moves_to_schedule_.erase(it);
      erased = true;
      break;
    }
  }
  CHECK(erased) << Substitute("T $0 P $1: move information not found", tablet_uuid, ts_uuid);

  // Update helper containers.
  const auto op_count = op_count_per_ts_[ts_uuid]++;
  const auto op_range = ts_per_op_count_.equal_range(op_count);
  bool ts_op_count_updated = false;
  for (auto it = op_range.first; it != op_range.second; ++it) {
    if (it->second == ts_uuid) {
      ts_per_op_count_.erase(it);
      ts_per_op_count_.emplace(op_count + 1, ts_uuid);
      ts_op_count_updated = true;
      break;
    }
  }
  DCHECK(ts_op_count_updated);
}

RebalancerTool::PolicyFixer::PolicyFixer(
    RebalancerTool* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    optional<MonoTime> deadline)
    : ReplaceBasedRunner(rebalancer,
                         std::move(ignored_tservers),
                         max_moves_per_server,
                         std::move(deadline)) {
}

Status RebalancerTool::PolicyFixer::GetReplaceMoves(
    const rebalance::ClusterInfo& ci,
    const rebalance::ClusterRawInfo& raw_info,
    vector<Rebalancer::ReplicaMove>* replica_moves) {
  TabletsPlacementInfo placement_info;
  RETURN_NOT_OK(
      BuildTabletsPlacementInfo(raw_info, scheduled_moves_, &placement_info));

  vector<PlacementPolicyViolationInfo> ppvi;
  RETURN_NOT_OK(DetectPlacementPolicyViolations(placement_info, &ppvi));

  // Filter out all reported violations which are already taken care of.
  // The idea is to have not more than one pending operation per tablet.
  {
    decltype(ppvi) ppvi_filtered;
    for (auto& info : ppvi) {
      if (ContainsKey(scheduled_moves_, info.tablet_id)) {
        continue;
      }
      ppvi_filtered.emplace_back(std::move(info));
    }
    ppvi = std::move(ppvi_filtered);
  }

  RETURN_NOT_OK(FindMovesToReimposePlacementPolicy(
      placement_info, ci.locality, ppvi, replica_moves));

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    for (const auto& info : ppvi) {
      VLOG(1) << Substitute("policy violation at location '$0': tablet $1",
                            info.majority_location, info.tablet_id);
    }
    for (const auto& move : *replica_moves) {
      VLOG(1) << Substitute("policy fix for tablet $0: replica to remove $1",
                            move.tablet_uuid, move.ts_uuid_from);
    }
  }

  return Status::OK();
}

RebalancerTool::IgnoredTserversRunner::IgnoredTserversRunner(
    RebalancerTool* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    optional<MonoTime> deadline)
    : ReplaceBasedRunner(rebalancer,
                         std::move(ignored_tservers),
                         max_moves_per_server,
                         std::move(deadline)),
      random_generator_(random_device_()) {
}

Status RebalancerTool::IgnoredTserversRunner::GetReplaceMoves(
    const rebalance::ClusterInfo& ci,
    const rebalance::ClusterRawInfo& raw_info,
    vector<Rebalancer::ReplicaMove>* replica_moves) {
  unordered_set<string> tservers_to_empty;
  rebalancer_->GetTServersToEmpty(raw_info, &tservers_to_empty);
  RETURN_NOT_OK(CheckIgnoredTServers(raw_info, ci, tservers_to_empty));

  TServersToEmptyMap tservers_to_empty_map;
  BuildTServersToEmptyInfo(raw_info, scheduled_moves_, tservers_to_empty, &tservers_to_empty_map);

  GetMovesFromIgnoredTservers(tservers_to_empty_map, replica_moves);
  return Status::OK();
}

Status RebalancerTool::IgnoredTserversRunner::CheckIgnoredTServers(
    const ClusterRawInfo& raw_info,
    const ClusterInfo& ci,
    const unordered_set<string>& tservers_to_empty) {
  if (tservers_to_empty.empty()) {
    return Status::OK();
  }

  // Check whether it is possible to move replicas emptying some tablet servers.
  int remaining_tservers_count = ci.balance.servers_by_total_replica_count.size();
  int max_replication_factor = 0;
  for (const auto& s : raw_info.table_summaries) {
    max_replication_factor = std::max(max_replication_factor, s.replication_factor);
  }
  if (remaining_tservers_count < max_replication_factor) {
    return Status::InvalidArgument(
        Substitute("Too many ignored tservers; "
                   "$0 healthy non-ignored servers exist but $1 are required.",
                   remaining_tservers_count,
                   max_replication_factor));
  }

  // Make sure tablet servers that we need to empty are set maintenance mode.
  for (const auto& ts : tservers_to_empty) {
    if (!ContainsKey(raw_info.tservers_in_maintenance_mode, ts)) {
      return Status::IllegalState(
          Substitute("You should set maintenance mode for tablet server $0 first", ts));
    }
  }

  return Status::OK();
}

void RebalancerTool::IgnoredTserversRunner::GetMovesFromIgnoredTservers(
    const TServersToEmptyMap& ignored_tservers_info,
    vector<Rebalancer::ReplicaMove>* replica_moves) {
  DCHECK(replica_moves);
  if (ignored_tservers_info.empty()) {
    return;
  }

  unordered_set<string> tablets_in_move;
  transform(scheduled_moves_.begin(), scheduled_moves_.end(),
            inserter(tablets_in_move, tablets_in_move.begin()),
            [](const Rebalancer::MovesInProgress::value_type& elem) {
              return elem.first;
            });

  vector<Rebalancer::ReplicaMove> result_moves;
  for (const auto& elem : ignored_tservers_info) {
    auto tablets_info = elem.second;
    // Some tablets are randomly picked to move from ignored tservers in a batch.
    // This method will output sufficient tablet replica movement operations
    // to avoid repeated calculations.
    shuffle(tablets_info.begin(), tablets_info.end(), random_generator_);
    for (int i = 0; i < tablets_info.size() && i < max_moves_per_server_ * 5; ++i) {
      if (ContainsKey(tablets_in_move, tablets_info[i].tablet_id)) {
        continue;
      }
      tablets_in_move.emplace(tablets_info[i].tablet_id);
      ReplicaMove move = {tablets_info[i].tablet_id, elem.first, "", tablets_info[i].config_idx};
      result_moves.emplace_back(std::move(move));
    }
  }
  *replica_moves = std::move(result_moves);
}

} // namespace tools
} // namespace kudu
