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

#include "kudu/rebalance/rebalance_algo.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using std::back_inserter;
using std::endl;
using std::multimap;
using std::numeric_limits;
using std::ostringstream;
using std::set_intersection;
using std::shuffle;
using std::sort;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rebalance {

namespace {

// Applies to 'm' a move of a replica from the tablet server with id 'src' to
// the tablet server with id 'dst' by decrementing the count of 'src' and
// incrementing the count of 'dst'.
// Returns Status::NotFound if either 'src' or 'dst' is not present in 'm'.
Status MoveOneReplica(const string& src,
                      const string& dst,
                      ServersByCountMap* m) {
  bool found_src = false;
  bool found_dst = false;
  int32_t count_src = 0;
  int32_t count_dst = 0;
  for (auto it = m->begin(); it != m->end(); ) {
    if (it->second != src && it->second != dst) {
      ++it;
      continue;
    }
    auto count = it->first;
    if (it->second == src) {
      found_src = true;
      count_src = count;
    } else {
      DCHECK_EQ(dst, it->second);
      found_dst = true;
      count_dst = count;
    }
    it = m->erase(it);
  }
  if (!found_src) {
    if (found_dst) {
      // Preserving the original data in the container.
      m->emplace(count_dst, dst);
    }
    return Status::NotFound("no per-server counts for replica", src);
  }
  if (!found_dst) {
    if (found_src) {
      // Preserving the original data in the container.
      m->emplace(count_src, src);
    }
    return Status::NotFound("no per-server counts for replica", dst);
  }

  // Moving replica from 'src' to 'dst', updating the counter correspondingly.
  m->emplace(count_src - 1, src);
  m->emplace(count_dst + 1, dst);
  return Status::OK();
}
} // anonymous namespace

Status RebalancingAlgo::GetNextMoves(const ClusterInfo& cluster_info,
                                     int max_moves_num,
                                     vector<TableReplicaMove>* moves) {
  DCHECK_LE(0, max_moves_num);
  DCHECK(moves);

  // Value of '0' is a shortcut for 'the possible maximum'.
  if (max_moves_num == 0) {
    max_moves_num = numeric_limits<decltype(max_moves_num)>::max();
  }
  moves->clear();

  const auto& balance = cluster_info.balance;
  if (balance.table_info_by_skew.empty()) {
    // Check for the consistency of the 'cluster_info' parameter: if no
    // information is given on the table skew, table count for all the tablet
    // servers should be 0.
    for (const auto& elem : balance.servers_by_total_replica_count) {
      if (elem.first != 0) {
        return Status::InvalidArgument(Substitute(
            "non-zero table count ($0) on tablet server ($1) while no table "
            "skew information in ClusterBalanceInfo", elem.first, elem.second));
      }
    }
    // Nothing to balance: cluster is empty. Leave 'moves' empty and return.
    return Status::OK();
  }

  // Copy cluster_info so we can apply moves to the copy.
  ClusterInfo info(cluster_info);
  for (decltype(max_moves_num) i = 0; i < max_moves_num; ++i) {
    boost::optional<TableReplicaMove> move;
    RETURN_NOT_OK(GetNextMove(info, &move));
    if (!move) {
      // No replicas to move.
      break;
    }
    RETURN_NOT_OK(ApplyMove(*move, &info.balance));
    moves->push_back(std::move(*move));
  }
  return Status::OK();
}

Status RebalancingAlgo::ApplyMove(const TableReplicaMove& move,
                                  ClusterBalanceInfo* balance_info) {
  // Copy cluster_info so we can apply moves to the copy.
  ClusterBalanceInfo info(*DCHECK_NOTNULL(balance_info));

  // Update the total counts.
  RETURN_NOT_OK_PREPEND(
      MoveOneReplica(move.from, move.to, &info.servers_by_total_replica_count),
      Substitute("missing information on table $0", move.table_id));

  // Find the balance info for the table.
  auto& table_info_by_skew = info.table_info_by_skew;
  TableBalanceInfo table_info;
  bool found_table_info = false;
  for (auto it = table_info_by_skew.begin(); it != table_info_by_skew.end(); ) {
    TableBalanceInfo& info = it->second;
    if (info.table_id != move.table_id) {
      ++it;
      continue;
    }
    std::swap(info, table_info);
    it = table_info_by_skew.erase(it);
    found_table_info = true;
    break;
  }
  if (!found_table_info) {
    return Status::NotFound(Substitute(
        "missing table info for table $0", move.table_id));
  }

  // Update the table counts.
  RETURN_NOT_OK_PREPEND(
      MoveOneReplica(move.from, move.to, &table_info.servers_by_replica_count),
      Substitute("missing information on table $0", move.table_id));

  const auto max_count = table_info.servers_by_replica_count.rbegin()->first;
  const auto min_count = table_info.servers_by_replica_count.begin()->first;
  DCHECK_GE(max_count, min_count);

  const int32_t skew = max_count - min_count;
  table_info_by_skew.emplace(skew, std::move(table_info));
  *balance_info = std::move(info);

  return Status::OK();
}

TwoDimensionalGreedyAlgo::TwoDimensionalGreedyAlgo(EqualSkewOption opt)
    : equal_skew_opt_(opt),
      generator_(random_device_()) {
}

Status TwoDimensionalGreedyAlgo::GetNextMove(
    const ClusterInfo& cluster_info,
    boost::optional<TableReplicaMove>* move) {
  DCHECK(move);
  // Set the output to none: this fits the short-circuit cases when there is
  // an issue with the parameters or there aren't any moves to return.
  *move = boost::none;

  const auto& balance_info = cluster_info.balance;
  // Due to the nature of the table_info_by_skew container, the very last
  // range represents the most unbalanced tables.
  const auto& table_info_by_skew = balance_info.table_info_by_skew;
  if (table_info_by_skew.empty()) {
    return Status::InvalidArgument("no table balance information");
  }
  const auto max_table_skew = table_info_by_skew.rbegin()->first;

  const auto& servers_by_total_replica_count =
      balance_info.servers_by_total_replica_count;
  if (servers_by_total_replica_count.empty()) {
    return Status::InvalidArgument("no per-server replica count information");
  }
  const auto max_server_skew =
      servers_by_total_replica_count.rbegin()->first -
      servers_by_total_replica_count.begin()->first;

  if (max_table_skew == 0) {
    // Every table is balanced and any move will unbalance a table, so there
    // is no potential for the greedy algorithm to balance the cluster.
    return Status::OK();
  }
  if (max_table_skew <= 1 && max_server_skew <= 1) {
    // Every table is balanced and the cluster as a whole is balanced.
    return Status::OK();
  }

  // Among the tables with maximum skew, attempt to pick a table where there is
  // a move that improves the table skew and the cluster skew, if possible. If
  // not, attempt to pick a move that improves the table skew. If all tables
  // are balanced, attempt to pick a move that preserves table balance and
  // improves cluster skew.
  const auto range = table_info_by_skew.equal_range(max_table_skew);
  for (auto it = range.first; it != range.second; ++it) {
    const TableBalanceInfo& tbi = it->second;
    const auto& servers_by_table_replica_count = tbi.servers_by_replica_count;
    if (servers_by_table_replica_count.empty()) {
      return Status::InvalidArgument(Substitute(
          "no information on replicas of table $0", tbi.table_id));
    }

    const auto min_replica_count = servers_by_table_replica_count.begin()->first;
    const auto max_replica_count = servers_by_table_replica_count.rbegin()->first;
    VLOG(1) << Substitute(
        "balancing table $0 with replica count skew $1 "
        "(min_replica_count: $2, max_replica_count: $3)",
        tbi.table_id, table_info_by_skew.rbegin()->first,
        min_replica_count, max_replica_count);

    // Compute the intersection of the tablet servers most loaded for the table
    // with the tablet servers most loaded overall, and likewise for least loaded.
    // These are our ideal candidates for moving from and to, respectively.
    int32_t max_count_table;
    int32_t max_count_total;
    vector<string> max_loaded;
    vector<string> max_loaded_intersection;
    RETURN_NOT_OK(GetIntersection(
        ExtremumType::MAX,
        servers_by_table_replica_count, servers_by_total_replica_count,
        &max_count_table, &max_count_total,
        &max_loaded, &max_loaded_intersection));
    int32_t min_count_table;
    int32_t min_count_total;
    vector<string> min_loaded;
    vector<string> min_loaded_intersection;
    RETURN_NOT_OK(GetIntersection(
        ExtremumType::MIN,
        servers_by_table_replica_count, servers_by_total_replica_count,
        &min_count_table, &min_count_total,
        &min_loaded, &min_loaded_intersection));

    VLOG(1) << Substitute("table-wise  : min_count: $0, max_count: $1",
                          min_count_table, max_count_table);
    VLOG(1) << Substitute("cluster-wise: min_count: $0, max_count: $1",
                          min_count_total, max_count_total);
    if (PREDICT_FALSE(VLOG_IS_ON(1))) {
      ostringstream s;
      s << "[ ";
      for (const auto& e : max_loaded_intersection) {
        s << e << " ";
      }
      s << "]";
      VLOG(1) << "max_loaded_intersection: " << s.str();

      s.str("");
      s << "[ ";
      for (const auto& e : min_loaded_intersection) {
        s << e << " ";
      }
      s << "]";
      VLOG(1) << "min_loaded_intersection: " << s.str();
    }
    // Do not move replicas of a balanced table if the least (most) loaded
    // servers overall do not intersect the servers hosting the least (most)
    // replicas of the table. Moving a replica in that case might keep the
    // cluster skew the same or make it worse while keeping the table balanced.
    if ((max_count_table <= min_count_table + 1) &&
        (min_loaded_intersection.empty() || max_loaded_intersection.empty())) {
      continue;
    }
    if (equal_skew_opt_ == EqualSkewOption::PICK_RANDOM) {
      shuffle(min_loaded.begin(), min_loaded.end(), generator_);
      shuffle(min_loaded_intersection.begin(), min_loaded_intersection.end(),
              generator_);
      shuffle(max_loaded.begin(), max_loaded.end(), generator_);
      shuffle(max_loaded_intersection.begin(), max_loaded_intersection.end(),
              generator_);
    }
    const auto& min_loaded_uuid = min_loaded_intersection.empty()
        ? min_loaded.front() : min_loaded_intersection.front();
    const auto& max_loaded_uuid = max_loaded_intersection.empty()
        ? max_loaded.back() : max_loaded_intersection.back();
    VLOG(1) << Substitute("min_loaded_uuid: $0, max_loaded_uuid: $1",
                          min_loaded_uuid, max_loaded_uuid);
    if (min_loaded_uuid == max_loaded_uuid) {
      // Nothing to move.
      continue;
    }

    // Move a replica of the selected table from a most loaded server to a
    // least loaded server.
    *move = { tbi.table_id, max_loaded_uuid, min_loaded_uuid };
    break;
  }

  return Status::OK();
}

Status TwoDimensionalGreedyAlgo::GetIntersection(
    ExtremumType extremum,
    const ServersByCountMap& servers_by_table_replica_count,
    const ServersByCountMap& servers_by_total_replica_count,
    int32_t* replica_count_table,
    int32_t* replica_count_total,
    vector<string>* server_uuids,
    vector<string>* intersection) {
  DCHECK(extremum == ExtremumType::MIN || extremum == ExtremumType::MAX);
  DCHECK(replica_count_table);
  DCHECK(replica_count_total);
  DCHECK(server_uuids);
  DCHECK(intersection);
  if (servers_by_table_replica_count.empty()) {
    return Status::InvalidArgument("no information on table replica count");
  }
  if (servers_by_total_replica_count.empty()) {
    return Status::InvalidArgument("no information on total replica count");
  }

  vector<string> server_uuids_table;
  RETURN_NOT_OK(GetMinMaxLoadedServers(
      servers_by_table_replica_count, extremum, replica_count_table,
      &server_uuids_table));
  sort(server_uuids_table.begin(), server_uuids_table.end());

  vector<string> server_uuids_total;
  RETURN_NOT_OK(GetMinMaxLoadedServers(
      servers_by_total_replica_count, extremum, replica_count_total,
      &server_uuids_total));
  sort(server_uuids_total.begin(), server_uuids_total.end());

  intersection->clear();
  set_intersection(
      server_uuids_table.begin(), server_uuids_table.end(),
      server_uuids_total.begin(), server_uuids_total.end(),
      back_inserter(*intersection));
  server_uuids->swap(server_uuids_table);

  return Status::OK();
}

Status TwoDimensionalGreedyAlgo::GetMinMaxLoadedServers(
    const ServersByCountMap& servers_by_replica_count,
    ExtremumType extremum,
    int32_t* replica_count,
    vector<string>* server_uuids) {
  DCHECK(extremum == ExtremumType::MIN || extremum == ExtremumType::MAX);
  DCHECK(replica_count);
  DCHECK(server_uuids);

  if (servers_by_replica_count.empty()) {
    return Status::InvalidArgument("no balance information");
  }
  const auto count = (extremum == ExtremumType::MIN)
      ? servers_by_replica_count.begin()->first
      : servers_by_replica_count.rbegin()->first;
  const auto range = servers_by_replica_count.equal_range(count);
  std::transform(range.first, range.second, back_inserter(*server_uuids),
                 [](const ServersByCountMap::value_type& elem) {
                   return elem.second;
                 });
  *replica_count = count;

  return Status::OK();
}

LocationBalancingAlgo::LocationBalancingAlgo(double load_imbalance_threshold)
    : load_imbalance_threshold_(load_imbalance_threshold) {
}

Status LocationBalancingAlgo::GetNextMove(
    const ClusterInfo& cluster_info,
    boost::optional<TableReplicaMove>* move) {
  DCHECK(move);
  *move = boost::none;

  // Per-table information on locations load.
  // TODO(aserbin): maybe, move this container into ClusterInfo?
  unordered_map<string, multimap<double, string>> location_load_info_by_table;

  // A dictionary to map location-wise load imbalance into table identifier.
  // The most imbalanced tables come last.
  multimap<double, string> table_id_by_load_imbalance;
  for (const auto& elem : cluster_info.balance.table_info_by_skew) {
    const auto& table_info = elem.second;
    // Number of replicas of all tablets comprising the table, per location.
    unordered_map<string, int32_t> replica_num_per_location;
    for (const auto& elem : table_info.servers_by_replica_count) {
      auto replica_count = elem.first;
      const auto& ts_id = elem.second;
      const auto& location =
          FindOrDie(cluster_info.locality.location_by_ts_id, ts_id);
      LookupOrEmplace(&replica_num_per_location, location, 0) += replica_count;
    }
    multimap<double, string> location_by_load;
    for (const auto& elem : replica_num_per_location) {
      const auto& location = elem.first;
      double replica_num = static_cast<double>(elem.second);
      auto ts_num = FindOrDie(cluster_info.locality.servers_by_location,
                              location).size();
      CHECK_NE(0, ts_num);
      location_by_load.emplace(replica_num / ts_num, location);
    }

    const auto& table_id = table_info.table_id;
    const auto load_min = location_by_load.cbegin()->first;
    const auto load_max = location_by_load.crbegin()->first;
    const auto imbalance = load_max - load_min;
    DCHECK(!std::isnan(imbalance));
    table_id_by_load_imbalance.emplace(imbalance, table_id);
    EmplaceOrDie(&location_load_info_by_table,
                 table_id, std::move(location_by_load));
  }

  string imbalanced_table_id;
  if (!IsBalancingNeeded(table_id_by_load_imbalance, &imbalanced_table_id)) {
    // Nothing to do: all tables are location-balanced enough.
    return Status::OK();
  }

  // Work on the most location-wise unbalanced tables first.
  const auto& load_info = FindOrDie(
      location_load_info_by_table, imbalanced_table_id);

  vector<string> loc_loaded_least;
  {
    const auto min_range = load_info.equal_range(load_info.cbegin()->first);
    for (auto it = min_range.first; it != min_range.second; ++it) {
      loc_loaded_least.push_back(it->second);
    }
  }
  DCHECK(!loc_loaded_least.empty());

  vector<string> loc_loaded_most;
  {
    const auto max_range = load_info.equal_range(load_info.crbegin()->first);
    for (auto it = max_range.first; it != max_range.second; ++it) {
      loc_loaded_most.push_back(it->second);
    }
  }
  DCHECK(!loc_loaded_most.empty());

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    ostringstream s;
    s << "[ ";
    for (const auto& loc : loc_loaded_least) {
      s << loc << " ";
    }
    s << "]";
    VLOG(1) << "loc_loaded_least: " << s.str();

    s.str("");
    s << "[ ";
    for (const auto& loc : loc_loaded_most) {
      s << loc << " ";
    }
    s << "]";
    VLOG(1) << "loc_leaded_most: " << s.str();
  }

  return FindBestMove(imbalanced_table_id, loc_loaded_least, loc_loaded_most,
                      cluster_info, move);
}

bool LocationBalancingAlgo::IsBalancingNeeded(
    const TableByLoadImbalance& imbalance_info,
    string* most_imbalanced_table_id) const {
  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    ostringstream ss;
    ss << "Table imbalance report: " << endl;
    for (const auto& elem : imbalance_info) {
      ss << "  " << elem.second << ": " << elem.first << endl;
    }
    VLOG(1) << ss.str();
  }

  if (imbalance_info.empty()) {
    // Nothing to do -- an empty cluster.
    return false;
  }

  // Evaluate the maximum existing imbalance: is it possible to move replicas
  // between tablet servers in different locations to make the skew less?
  //
  // Empirically, the imbalance threshold is to detect 'good enough' vs 'ideal'
  // cases, like (b) vs (a) in the class-wide comment. In other words, this
  // delta if the minimum load imbalance down to which it makes sense to try
  // cross-location rebalancing.
  //
  // The information on the most imbalanced table is in the last element
  // of the map.
  const auto it = imbalance_info.crbegin();
  const auto imbalance = it->first;
  if (imbalance > load_imbalance_threshold_) {
    *most_imbalanced_table_id = it->second;
    return true;
  }
  return false;
}

// Given the set of the most and the least table-wise loaded locations, choose
// the source and destination tablet server to move a replica of the specified
// tablet to improve per-table location load balance as much as possible.
Status LocationBalancingAlgo::FindBestMove(
    const string& table_id,
    const vector<string>& loc_loaded_least,
    const vector<string>& loc_loaded_most,
    const ClusterInfo& cluster_info,
    boost::optional<TableReplicaMove>* move) {
  // Among the available candidate locations, prefer those having the most and
  // least loaded tablet servers in terms of total number of hosted replicas.
  // The rationale is that the per-table location load is a relative metric
  // (i.e. number of table replicas / number of tablet servers), but it's
  // always beneficial to have less loaded servers in absolute terms.
  //
  // If there are multiple candidate tablet servers with the same extremum load,
  // choose among them randomly.
  //
  // TODO(aserbin): implement fine-grained logic to select the best move among
  //                the available candidates, if multiple choices are available.
  //                For example, among candidates with the same number of
  //                replicas, prefer candidates where the movement from one
  //                server to another also improves the table-wise skew within
  //                the destination location.
  //

  // Building auxiliary containers.
  // TODO(aserbin): refactor and move some of those into the ClusterBalanceInfo.
  typedef std::unordered_map<std::string, int32_t> ServerLoadMap;
  ServerLoadMap load_by_ts;
  for (const auto& elem : cluster_info.balance.servers_by_total_replica_count) {
    EmplaceOrDie(&load_by_ts, elem.second, elem.first);
  }

  // Least loaded tablet servers from the destination locations.
  multimap<int32_t, string> ts_id_by_load_least;
  for (const auto& loc : loc_loaded_least) {
    const auto& loc_ts_ids =
        FindOrDie(cluster_info.locality.servers_by_location, loc);
    for (const auto& ts_id : loc_ts_ids) {
      ts_id_by_load_least.emplace(FindOrDie(load_by_ts, ts_id), ts_id);
    }
  }
  // TODO(aserbin): separate into a function or lambda.
  const auto min_load = ts_id_by_load_least.cbegin()->first;
  const auto min_range = ts_id_by_load_least.equal_range(min_load);
  auto it_min = min_range.first;
#if 0
  // TODO(aserbin): add randomness
  const auto distance_min = distance(min_range.first, min_range.second);
  std::advance(it_min, Uniform(distance_min));
  CHECK_NE(min_range.second, it_min);
#endif
  const auto& dst_ts_id = it_min->second;

  // Most loaded tablet servers from the source locations.
  multimap<int32_t, string, std::greater<int32_t>> ts_id_by_load_most;
  for (const auto& loc : loc_loaded_most) {
    const auto& loc_ts_ids =
        FindOrDie(cluster_info.locality.servers_by_location, loc);
    for (const auto& ts_id : loc_ts_ids) {
      ts_id_by_load_most.emplace(FindOrDie(load_by_ts, ts_id), ts_id);
    }
  }
  const auto max_load = ts_id_by_load_most.cbegin()->first;
  const auto max_range = ts_id_by_load_most.equal_range(max_load);
  auto it_max = max_range.first;
#if 0
  // TODO(aserbin): add randomness
  const auto distance_max = distance(max_range.first, max_range.second);
  std::advance(it_max, Uniform(distance_max));
  CHECK_NE(max_range.second, it_max);
#endif
  const auto& src_ts_id = it_max->second;
  CHECK_NE(src_ts_id, dst_ts_id);

  *move = { table_id, src_ts_id, dst_ts_id };

  return Status::OK();
}

} // namespace rebalance
} // namespace kudu
