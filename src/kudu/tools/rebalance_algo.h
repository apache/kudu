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
#include <map>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/util/status.h"

namespace boost {
template <class T> class optional;
} // namespace boost

namespace kudu {
namespace tools {

// A map from a count of replicas to a server identifier. The "reversed"
// relationship facilitates finding the servers with the maximum and minimum
// replica counts.
typedef std::multimap<int32_t, std::string> ServersByCountMap;

// Balance information for a table.
struct TableBalanceInfo {
  std::string table_id;

  // Mapping table replica count -> tablet server.
  //
  // The table replica count of a tablet server is defined as the number of
  // replicas belonging to the table hosted on the tablet server.
  ServersByCountMap servers_by_replica_count;
};

// Balance information for a cluster.
struct ClusterBalanceInfo {
  // Mapping table skew -> table balance information. The "reversed"
  // relationship facilitates finding the most and least skewed tables.
  //
  // The skew of a table is defined as the difference between its most
  // occupied and least occupied tablet servers. Skew is considered to be
  // improved both when the number of pairs of tablet servers exhibiting max
  // skew between them decreases, or when the skew decreases.
  std::multimap<int32_t, TableBalanceInfo> table_info_by_skew;

  // Mapping total replica count -> tablet server identifier.
  //
  // The total replica count of a tablet server is defined as the total number
  // of replicas hosted on the tablet server.
  ServersByCountMap servers_by_total_replica_count;
};

struct ClusterLocalityInfo {
  // Location-related information: distribution of tablet servers by locations.
  // Mapping 'location' --> 'identifiers of tablet servers in the location'.
  std::unordered_map<std::string, std::set<std::string>> servers_by_location;

  // Mapping 'tablet server identifier' --> 'location'.
  std::unordered_map<std::string, std::string> location_by_ts_id;
};

// A directive to move some replica of a table between two tablet servers.
struct TableReplicaMove {
  std::string table_id;
  std::string from;     // Unique identifier of the source tablet server.
  std::string to;       // Unique identifier of the target tablet server.
};

// A rebalancing algorithm, which orders replica moves aiming to balance a
// cluster. The definition of "balance" depends on the algorithm.
class RebalancingAlgo {
 public:
  virtual ~RebalancingAlgo() = default;

  // The top-level method of the algorithm. Using information on the current
  // balance state of the cluster in 'cluster_info', the algorithm populates
  // the output parameter 'moves' with no more than 'max_moves_num' replica
  // moves that aim to balance the cluster. Due to code conventions, 'int' is
  // used instead of 'size_t' as 'max_moves_num' type; 'max_moves_num' must be
  // non-negative value, where value of '0' is a shortcut for
  // 'the possible maximum'.
  //
  // Once this method returns Status::OK() and leaves 'moves' empty, the cluster
  // is considered balanced.
  //
  // 'moves' must be non-NULL.
  virtual Status GetNextMoves(const ClusterBalanceInfo& cluster_info,
                              int max_moves_num,
                              std::vector<TableReplicaMove>* moves);
 protected:
  // Get the next rebalancing move from the algorithm. If there is no such move,
  // the 'move' output parameter is set to 'boost::none'.
  //
  // 'move' must be non-NULL.
  virtual Status GetNextMove(const ClusterBalanceInfo& cluster_info,
                             boost::optional<TableReplicaMove>* move) = 0;

  // Update the balance state in 'cluster_info' with the outcome of the move
  // 'move'. 'cluster_info' is an in-out parameter.
  //
  // 'cluster_info' must be non-NULL.
  static Status ApplyMove(const TableReplicaMove& move,
                          ClusterBalanceInfo* cluster_info);
};

// A two-dimensional greedy rebalancing algorithm. From among moves that
// decrease the skew of a most skewed table, it prefers ones that reduce the
// skew of the cluster. A cluster is considered balanced when the skew of every
// table is <= 1 and the skew of the cluster is <= 1.
//
// The skew of the cluster is defined as the difference between the maximum
// total replica count over all tablet servers and the minimum total replica
// count over all tablet servers.
class TwoDimensionalGreedyAlgo : public RebalancingAlgo {
 public:
  // Policies for picking one element from equal-skew choices.
  enum class EqualSkewOption {
    PICK_FIRST,
    PICK_RANDOM,
  };
  explicit TwoDimensionalGreedyAlgo(
      EqualSkewOption opt = EqualSkewOption::PICK_RANDOM);

  Status GetNextMove(const ClusterBalanceInfo& cluster_info,
                     boost::optional<TableReplicaMove>* move) override;

 private:
  enum class ExtremumType { MAX, MIN, };

  FRIEND_TEST(RebalanceAlgoUnitTest, RandomizedTest);
  FRIEND_TEST(RebalanceAlgoUnitTest, EmptyClusterBalanceInfoGetNextMove);

  // Compute the intersection of the least or most loaded tablet servers for a
  // table with the least or most loaded tablet servers in the cluster:
  // 'intersection' is populated with the ids of the tablet servers in the
  // intersection of those two sets. The minimum or maximum number of replicas
  // per tablet server for the table and for the cluster are output into
  // 'replica_count_table' and 'replica_count_total', respectively. Identifiers
  // of the tablet servers with '*replica_count_table' replicas of the table are
  // output into the 'server_uuids' parameter. Whether to consider most or least
  // loaded servers is controlled by 'extremum'. An empty 'intersection' on
  // return means no intersection was found for the mentioned sets of the
  // extremally loaded servers: in that case optimizing the load by table would
  // not affect the extreme load by server.
  //
  // None of the output parameters may be NULL.
  Status GetIntersection(
      ExtremumType extremum,
      const ServersByCountMap& servers_by_table_replica_count,
      const ServersByCountMap& servers_by_total_replica_count,
      int32_t* replica_count_table,
      int32_t* replica_count_total,
      std::vector<std::string>* server_uuids,
      std::vector<std::string>* intersection);

  // As determined by 'extremum', find the tablet servers with the minimum or
  // maximum count in 'server_by_replica_counts'. The extremal count will be set
  // in 'replica_count', while 'server_uuids' will be populated with the servers
  // that host the extremal replica count.
  //
  // Neither of the output parameters may be NULL.
  Status GetMinMaxLoadedServers(
      const ServersByCountMap& servers_by_replica_count,
      ExtremumType extremum,
      int32_t* replica_count,
      std::vector<std::string>* server_uuids);

  const EqualSkewOption equal_skew_opt_;
  std::random_device random_device_;
  std::mt19937 generator_;
};

} // namespace tools
} // namespace kudu
