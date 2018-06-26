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

#include "kudu/tools/rebalance_algo.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using std::back_inserter;
using std::cout;
using std::endl;
using std::make_pair;
using std::multimap;
using std::ostringstream;
using std::set_intersection;
using std::shared_ptr;
using std::shuffle;
using std::sort;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

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

Status RebalancingAlgo::GetNextMoves(const ClusterBalanceInfo& cluster_info,
                                     int max_moves_num,
                                     vector<TableReplicaMove>* moves) {
  DCHECK_LE(0, max_moves_num);
  DCHECK(moves);

  // Value of '0' is a shortcut for 'the possible maximum'.
  if (max_moves_num == 0) {
    max_moves_num = std::numeric_limits<decltype(max_moves_num)>::max();
  }
  moves->clear();

  if (cluster_info.table_info_by_skew.empty()) {
    // Check for the consistency of the 'cluster_info' parameter: if no
    // information is given on the table skew, table count for all the tablet
    // servers should be 0.
    for (const auto& elem : cluster_info.servers_by_total_replica_count) {
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
  ClusterBalanceInfo info(cluster_info);
  for (decltype(max_moves_num) i = 0; i < max_moves_num; ++i) {
    boost::optional<TableReplicaMove> move;
    RETURN_NOT_OK(GetNextMove(info, &move));
    if (!move) {
      // No replicas to move.
      break;
    }
    RETURN_NOT_OK(ApplyMove(*move, &info));
    moves->push_back(std::move(*move));
  }
  return Status::OK();
}

Status RebalancingAlgo::ApplyMove(const TableReplicaMove& move,
                                  ClusterBalanceInfo* cluster_info) {
  // Copy cluster_info so we can apply moves to the copy.
  ClusterBalanceInfo info(*DCHECK_NOTNULL(cluster_info));

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
  std::swap(*cluster_info, info);

  return Status::OK();
}

TwoDimensionalGreedyAlgo::TwoDimensionalGreedyAlgo(EqualSkewOption opt)
    : equal_skew_opt_(opt),
      random_device_(),
      generator_(random_device_()) {
}

Status TwoDimensionalGreedyAlgo::GetNextMove(
    const ClusterBalanceInfo& cluster_info,
    boost::optional<TableReplicaMove>* move) {
  DCHECK(move);
  // Set the output to none: this fits the short-circuit cases when there is
  // an issue with the parameters or there aren't any moves to return.
  *move = boost::none;

  // Due to the nature of the table_info_by_skew container, the very last
  // range represents the most unbalanced tables.
  const auto& table_info_by_skew = cluster_info.table_info_by_skew;
  if (table_info_by_skew.empty()) {
    return Status::InvalidArgument("no table balance information");
  }
  const auto max_table_skew = table_info_by_skew.rbegin()->first;

  const auto& servers_by_total_replica_count =
      cluster_info.servers_by_total_replica_count;
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

} // namespace tools
} // namespace kudu
