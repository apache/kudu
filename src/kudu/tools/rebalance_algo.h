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

// Locality information for a cluster: distribution of tablet servers among
// locations.
struct ClusterLocalityInfo {
  // Location-related information: distribution of tablet servers by locations.
  // Mapping 'location' --> 'identifiers of tablet servers in the location'.
  std::unordered_map<std::string, std::set<std::string>> servers_by_location;

  // Mapping 'tablet server identifier' --> 'location'.
  std::unordered_map<std::string, std::string> location_by_ts_id;
};

// Information on a cluster as input for various rebalancing algorithms.
struct ClusterInfo {
  ClusterBalanceInfo balance;
  ClusterLocalityInfo locality;
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
  virtual Status GetNextMoves(const ClusterInfo& cluster_info,
                              int max_moves_num,
                              std::vector<TableReplicaMove>* moves);
 protected:
  // Get the next rebalancing move from the algorithm. If there is no such move,
  // the 'move' output parameter is set to 'boost::none'.
  //
  // 'move' must be non-NULL.
  virtual Status GetNextMove(const ClusterInfo& cluster_info,
                             boost::optional<TableReplicaMove>* move) = 0;

  // Update the balance state in 'balance_info' with the outcome of the move
  // 'move'. 'balance_info' is an in-out parameter and must be non-NULL.
  static Status ApplyMove(const TableReplicaMove& move,
                          ClusterBalanceInfo* balance_info);
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

 protected:
  Status GetNextMove(const ClusterInfo& cluster_info,
                     boost::optional<TableReplicaMove>* move) override;

 private:
  enum class ExtremumType { MAX, MIN, };

  FRIEND_TEST(RebalanceAlgoUnitTest, RandomizedTest);
  FRIEND_TEST(RebalanceAlgoUnitTest, EmptyBalanceInfoGetNextMove);
  FRIEND_TEST(RebalanceAlgoUnitTest, EmptyClusterInfoGetNextMove);

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

// Algorithm to balance among locations in the cluster.
//
// The inter-location rebalancing is to minimize location load skew per table.
// The idea is to equalize the density of the distribution of each table across
// locations.
//
// Q: Why is it beneficial to equalize the density of table replicas across
//    locations?
// A: Assuming the homogeneous structure of the cluster (e.g., that's about
//    having machines of the same hardware specs across the cluster) and
//    uniform distribution of requests among all tables in the cluster
//    (the latter is questionable, but in Kudu there isn't currently a way
//    to specify any deviations anyway), that gives better usage
//    of the available hardware resources.
//
// NOTE: probably, in the future we might add a notion of some preference in
//       table placements regarding selected locations.
//
// Q: What is per-table location load skew?
// A: Consider number of replicas per location for tablets comprising
//    a table T. Assume we have locations L_0, ..., L_n, where
//    replica_num(T, L_0), ..., replica_num(T, L_n) are numbers of replicas
//    of T's tablets at corresponding locations. We want to make the following
//    ratios to devicate as less as possible:
//
//    replica_num(T, L_0) / ts_num(L_0), ..., replica_num(T, L_n) / ts_num(L_n)
//
// ******* Some Examples *******
//
// Tablet T of replication factor 5, and locations L_0, ..., L_4. Consider
// the following tablet servers disposition:
//
//   ts_num(L_0): 2
//   ts_num(L_1): 2
//   ts_num(L_2): 1
//   ts_num(L_3): 1
//   ts_num(L_4): 1
//
// What distribution of replicas is preferred for a tablet t0 of table T?
//  (a) { L_0: 1, L_1: 1, L_2: 1, L_3: 1, L_4: 1 }
//          skew 0.5: { 0.5, 0.5, 1.0, 1.0, 1.0 }
//
//  (b) { L_0: 2, L_1: 2, L_2: 1, L_3: 0, L_4: 0 }
//          skew 1.0 : { 1.0, 1.0, 1.0, 0.0, 0.0 }
//
// The main idea is to prevent moving tablets if the distribution is 'good
// enough'. E.g., the distribution of (b) is acceptable if the rebalancer finds
// the replicas already placed like that, and it should not try to move
// the replicas to achieve the ideal distribution of (a).
//
// How about:
//  (c) { L_0: 0, L_1: 0, L_2: 1, L_3: 2, L_4: 2 }
//          skew 2.0: { 0.0, 0.0, 1.0, 2.0, 2.0 }
//
// We want to move replicas to make the distribution (c) more balanced;
// 2 movements gives us the 'ideal' location-wise replica placement.
class LocationBalancingAlgo : public RebalancingAlgo {
 public:
  explicit LocationBalancingAlgo(double load_imbalance_threshold);

 protected:
  Status GetNextMove(const ClusterInfo& cluster_info,
                     boost::optional<TableReplicaMove>* move) override;
 private:
  FRIEND_TEST(RebalanceAlgoUnitTest, RandomizedTest);
  typedef std::multimap<double, std::string> TableByLoadImbalance;

  // Check if any rebalancing is needed across cluster locations based on the
  // information provided by the 'imbalance_info' parameter. Returns 'true'
  // if rebalancing is needed, 'false' otherwise. Upon returning 'true',
  // the identifier of the most cross-location imbalanced table is output into
  // the 'most_imbalanced_table_id' parameter (which must not be null).
  bool IsBalancingNeeded(const TableByLoadImbalance& imbalance_info,
                         std::string* most_imbalanced_table_id) const;

  // Given the set of the most and the least table-wise loaded locations, choose
  // the source and destination tablet server to move a replica of the specified
  // tablet to improve per-table location load balance as much as possible.
  // If no replica can be moved to balance the load, the 'move' output parameter
  // is set to 'boost::none'.
  Status FindBestMove(const std::string& table_id,
      const std::vector<std::string>& loc_loaded_least,
      const std::vector<std::string>& loc_loaded_most,
      const ClusterInfo& cluster_info,
      boost::optional<TableReplicaMove>* move);

  const double load_imbalance_threshold_;
};

} // namespace tools
} // namespace kudu
