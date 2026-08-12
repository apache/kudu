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
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

// IWYU pragma: keep.
namespace kudu {
namespace rebalance {
struct TestClusterConfig;
} // namespace rebalance
} // namespace kudu

#define VERIFY_MOVES(test_config) \
  do { \
    for (auto idx = 0; idx < ARRAYSIZE((test_config)); ++idx) { \
      SCOPED_TRACE(Substitute("test config index: $0", idx)); \
      NO_FATALS(VerifyRebalancingMoves((test_config)[idx])); \
    } \
  } while (false)

#define VERIFY_LOCATION_BALANCING_MOVES(test_config) \
  do { \
    for (auto idx = 0; idx < ARRAYSIZE((test_config)); ++idx) { \
      SCOPED_TRACE(Substitute("test config index: $0", idx)); \
      NO_FATALS(VerifyLocationRebalancingMoves((test_config)[idx])); \
    } \
  } while (false)

using std::endl;
using std::nullopt;
using std::optional;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::sort;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rebalance {

struct TablePerServerReplicas {
  const string table_id;
  const string tag;

  // Number of replicas of this table on each server in the cluster.
  // By definition, the indices in this container correspond to indices
  // in TestClusterConfig::tserver_uuids.
  const vector<size_t> num_replicas_by_server;
};

// Whether the order of the moves in the reference results should be verified
// against the actual moves.
enum class MovesOrderingComparison {
  IGNORE,
  VERIFY,
};
struct ReferenceComparisonOptions {
  // Constructor to initialize the options by default.
  // NOLINTNEXTLINE(google-explicit-constructor)
  ReferenceComparisonOptions(MovesOrderingComparison moves_ordering =
      MovesOrderingComparison::VERIFY)
      : moves_ordering(moves_ordering) {
  }

  const MovesOrderingComparison moves_ordering;
};

// Structure to describe rebalancing-related state of the cluster expressively
// enough for the tests.
struct TestClusterConfig {
  // Distribution of tablet servers by locations. If the map is empty, it's
  // interpreted as if the cluster does not have any locations specified
  // (i.e. all the tablet servers are all in the same unnamed location).
  const unordered_map<string, set<string>> servers_by_location;

  // UUIDs of tablet servers; every element must be unique.
  const vector<string> tserver_uuids;

  // Distribution of tablet replicas across the tablet servers. The following
  // constraints should be in place:
  //   * for each t in table_replicas:
  //       t.num_replicas_by_server.size() == tserver_uuids.size()
  const vector<TablePerServerReplicas> table_replicas;

  // The expected replica movements: the reference output of the algorithm
  // to compare with.
  const vector<TableReplicaMove> expected_moves;

  // Options controlling how the reference and the actual results are compared.
  const ReferenceComparisonOptions ref_comparison_options;
};

bool operator==(const TableReplicaMove& lhs, const TableReplicaMove& rhs) {
  return
      lhs.table_id == rhs.table_id &&
      lhs.tag == rhs.tag &&
      lhs.from == rhs.from &&
      lhs.to == rhs.to;
}

ostream& operator<<(ostream& o, const TableReplicaMove& move) {
  o << move.table_id << " (" << move.tag << ") " << ":"
    << move.from << "->" << move.to;
  return o;
}

// Transform the definition of the test cluster into the ClusterInfo
// that is consumed by the rebalancing algorithm.
void ClusterConfigToClusterInfo(const TestClusterConfig& tcc,
                                ClusterInfo* cluster_info) {
  // First verify that the configuration of the test cluster is valid.
  set<pair<string, string>> table_ids_and_tags;
  for (const auto& table_replica_info : tcc.table_replicas) {
    CHECK_EQ(tcc.tserver_uuids.size(),
             table_replica_info.num_replicas_by_server.size());
    table_ids_and_tags.emplace(
        table_replica_info.table_id, table_replica_info.tag);
  }
  CHECK_EQ(table_ids_and_tags.size(), tcc.table_replicas.size());
  {
    // Check for uniqueness of the tablet servers' identifiers.
    set<string> uuids(tcc.tserver_uuids.begin(), tcc.tserver_uuids.end());
    CHECK_EQ(tcc.tserver_uuids.size(), uuids.size());
  }

  ClusterInfo result;
  auto& balance = result.balance;
  for (size_t tserver_idx = 0; tserver_idx < tcc.tserver_uuids.size();
       ++tserver_idx) {
    // Total replica count at the tablet server.
    size_t count = 0;
    for (const auto& table_replica_info: tcc.table_replicas) {
      count += table_replica_info.num_replicas_by_server[tserver_idx];
    }
    balance.servers_by_total_replica_count.emplace(
        count, tcc.tserver_uuids[tserver_idx]);
  }

  auto& table_info_by_skew = balance.table_info_by_skew;
  for (size_t table_idx = 0; table_idx < tcc.table_replicas.size(); ++table_idx) {
    // Replicas of the current table per tablet server.
    const vector<size_t>& replicas_count =
        tcc.table_replicas[table_idx].num_replicas_by_server;
    TableBalanceInfo info;
    info.table_id = tcc.table_replicas[table_idx].table_id;
    info.tag = tcc.table_replicas[table_idx].tag;
    for (size_t tserver_idx = 0; tserver_idx < replicas_count.size(); ++tserver_idx) {
      auto count = replicas_count[tserver_idx];
      info.servers_by_replica_count.emplace(count, tcc.tserver_uuids[tserver_idx]);
    }
    size_t max_count = info.servers_by_replica_count.rbegin()->first;
    size_t min_count = info.servers_by_replica_count.begin()->first;
    CHECK_GE(max_count, min_count);
    table_info_by_skew.emplace(max_count - min_count, std::move(info));
  }

  // TODO(aserbin): add a consistency check on location-related fields.
  auto& locality = result.locality;
  locality.servers_by_location = tcc.servers_by_location;
  for (const auto& elem : tcc.servers_by_location) {
    const auto& location = elem.first;
    const auto& server_ids = elem.second;
    for (const auto& server_id : server_ids) {
      EmplaceOrDie(&locality.location_by_ts_id, server_id, location);
    }
  }

  *cluster_info = std::move(result);
}

void VerifyRebalancingMoves(const TestClusterConfig& cfg) {
  vector<TableReplicaMove> moves;
  {
    ClusterInfo ci;
    ClusterConfigToClusterInfo(cfg, &ci);
    TwoDimensionalGreedyAlgo algo(
        TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST);
    ASSERT_OK(algo.GetNextMoves(ci, 0, &moves));
  }
  EXPECT_EQ(cfg.expected_moves, moves);
}

// Similar to VerifyRebalancingMoves(), but related to locations rebalancing.
void VerifyLocationRebalancingMoves(const TestClusterConfig& cfg) {
  vector<TableReplicaMove> moves;
  {
    ClusterInfo ci;
    ClusterConfigToClusterInfo(cfg, &ci);
    LocationBalancingAlgo algo(1.0, /*prefer_follower_moves=*/true);
    ASSERT_OK(algo.GetNextMoves(ci, 0, &moves));
  }
  switch (cfg.ref_comparison_options.moves_ordering) {
    case MovesOrderingComparison::IGNORE:
      {
        // The case when the order of moves is not important. For the
        // rebalancing algorithms, it's natural to re-order contiguous moves
        // of the same weight. This is because of:
        //   a) randomly choosing among multiple options of the same weight
        //   b) iterating over elements of a hash container keyed by the weight
        //      of a move.
        // Here it's necessary to normalize both the reference and the actual
        // results before performing element-to-element comparison.
        vector<TableReplicaMove> ref_moves(cfg.expected_moves);
        const auto kMovesComparator = [](const TableReplicaMove& lhs,
                                         const TableReplicaMove& rhs) {
          if (lhs.table_id != rhs.table_id) {
            return lhs.table_id < rhs.table_id;
          }
          if (lhs.tag != rhs.tag) {
            return lhs.tag < rhs.tag;
          }
          if (lhs.from != rhs.from) {
            return lhs.from < rhs.from;
          }
          return lhs.to < rhs.to;
        };
        sort(ref_moves.begin(), ref_moves.end(), kMovesComparator);
        sort(moves.begin(), moves.end(), kMovesComparator);
        EXPECT_EQ(ref_moves, moves);
      }
      break;
    case MovesOrderingComparison::VERIFY:
      EXPECT_EQ(cfg.expected_moves, moves);
      break;
    default:
      FAIL() << "unexpected reference comparison style";
      break;
  }
}

// Is 'cbi' balanced according to the two-dimensional greedy algorithm?
bool IsBalanced(const ClusterBalanceInfo& cbi) {
  if (cbi.table_info_by_skew.empty()) {
    return true;
  }
  auto max_table_skew = cbi.table_info_by_skew.rbegin()->first;
  auto cluster_skew = cbi.servers_by_total_replica_count.rbegin()->first -
                      cbi.servers_by_total_replica_count.begin()->first;
  return (max_table_skew <= 1) && (cluster_skew <= 1);
}

string TestClusterConfigToDebugString(const TestClusterConfig& cfg) {
  ostringstream oss;
  oss << Substitute("TestClusterConfig: $0 tservers, $0 tables",
                    cfg.table_replicas.size(), cfg.tserver_uuids.size()) << endl;
  for (const auto& t : cfg.table_replicas) {
    oss << Substitute("table $0: [", t.table_id);
    for (auto i = 0; i < t.num_replicas_by_server.size(); i++) {
      if (i > 0) {
        oss << ", ";
      }
      oss << Substitute("ts $0: $1",
                        cfg.tserver_uuids[i], t.num_replicas_by_server[i]);
    }
    oss << "]" << endl;
  }
  return oss.str();
}

// Test the behavior of the algorithm when no input information is given.
TEST(RebalanceAlgoUnitTest, EmptyClusterInfoGetNextMoves) {
  vector<TableReplicaMove> moves;
  const ClusterInfo info = {};
  ASSERT_OK(TwoDimensionalGreedyAlgo().GetNextMoves(info, 0, &moves));
  EXPECT_TRUE(moves.empty());
}

// Test the behavior of the algorithm when no tablet skew information
// is provided in the ClusterBalanceInfo structure.
TEST(RebalanceAlgoUnitTest, NoTableSkewInClusterBalanceInfoGetNextMoves) {
  {
    vector<TableReplicaMove> moves;
    const ClusterInfo info = { { {}, { { 0, "ts_0" } } } };
    ASSERT_OK(TwoDimensionalGreedyAlgo().GetNextMoves(info, 0, &moves));
    EXPECT_TRUE(moves.empty());
  }

  {
    vector<TableReplicaMove> moves;
    const ClusterInfo info = { { {}, { { 1, "ts_0" }, } } };
    const auto s = TwoDimensionalGreedyAlgo().GetNextMoves(info, 0, &moves);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_MATCHES(s.ToString(),
        "non-zero table count .* on tablet server .* while no table "
        "skew information in ClusterBalanceInfo");
  }
}

// Test the behavior of the internal (non-public) algorithm's method
// GetNextMove() when no input information is given.
TEST(RebalanceAlgoUnitTest, EmptyBalanceInfoGetNextMove) {
  optional<TableReplicaMove> move;
  const ClusterInfo info = {};
  const auto s = TwoDimensionalGreedyAlgo().GetNextMove(info, &move);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_EQ(nullopt, move);
}

// Workaround for older libstdc++ (like on RH/CentOS 6). In case of newer
// libstdc++/libc++ '{}' works as needed for an empty unordered map.
static const decltype(TestClusterConfig::servers_by_location) kNoLocations;

TEST(RebalanceAlgoUnitTest, PreferFollowerMovesByTableAndTag) {
  const TestClusterConfig kConfig = {
    kNoLocations,
    { "0", "1", "2", },
    {
      { "A", "R0", { 2, 0, 0, } },
      { "A", "R1", { 2, 0, 0, } },
    },
    {},
  };

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);
  ci.ts_with_followers_by_table_and_tag[TableIdAndTag{ "A", "R1" }].insert("0");

  {
    vector<TableReplicaMove> moves;
    TwoDimensionalGreedyAlgo algo(
        TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST,
        true);
    ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
    ASSERT_EQ(1, moves.size());
    ASSERT_EQ((TableReplicaMove{ "A", "R1", "0", "1" }), moves[0]);
  }

  {
    vector<TableReplicaMove> moves;
    TwoDimensionalGreedyAlgo algo(
        TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST,
        false);
    ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
    ASSERT_EQ(1, moves.size());
    ASSERT_EQ((TableReplicaMove{ "A", "R0", "0", "1" }), moves[0]);
  }
}

// When ts_with_followers_by_table_and_tag is empty (as from ClusterConfigToClusterInfo
// without BuildClusterInfo), no source qualifies as having followers; with
// prefer_follower_moves=true the algorithm must still emit a move via leader_fallback.
TEST(RebalanceAlgoUnitTest, PreferFollowerMovesLeaderFallbackWhenFollowerMapEmpty) {
  const TestClusterConfig kConfig = {
    kNoLocations,
    { "0", "1", "2", },
    {
      { "A", "", { 2, 0, 0, } },
    },
    {},
  };

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);
  ASSERT_TRUE(ci.ts_with_followers_by_table_and_tag.empty());

  vector<TableReplicaMove> moves;
  TwoDimensionalGreedyAlgo algo(
      TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST,
      true);
  ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
  ASSERT_EQ(1, moves.size());
  ASSERT_EQ((TableReplicaMove{ "A", "", "0", "1" }), moves[0]);
}

// Among tables tied for the maximum skew, the PICK_RANDOM policy must break the
// tie randomly rather than always selecting the first table in the multimap's
// internal (insertion) order. With three equally-skewed tables, repeated
// single-move runs should select more than one distinct table.
TEST(RebalanceAlgoUnitTest, EqualSkewTieBreakingIsRandomized) {
  const TestClusterConfig kConfig = {
    kNoLocations,
    { "0", "1", "2", },
    {
      { "A", "", { 2, 0, 0, } },
      { "B", "", { 2, 0, 0, } },
      { "C", "", { 2, 0, 0, } },
    },
    {},
  };

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);

  TwoDimensionalGreedyAlgo algo(
      TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_RANDOM);
  set<string> chosen_tables;
  for (int i = 0; i < 100; ++i) {
    vector<TableReplicaMove> moves;
    ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
    ASSERT_EQ(1, moves.size());
    chosen_tables.insert(moves[0].table_id);
  }
  // With 3 tied tables and 100 trials, the odds of only ever selecting a single
  // table by chance are ~3 * (1/3)^100, i.e. effectively zero.
  EXPECT_GT(chosen_tables.size(), 1);
}

// The PICK_FIRST policy must preserve the original deterministic tie-breaking:
// the first equal-skew table in the multimap order ("A" here) is always chosen.
TEST(RebalanceAlgoUnitTest, EqualSkewTieBreakingPickFirstIsDeterministic) {
  const TestClusterConfig kConfig = {
    kNoLocations,
    { "0", "1", "2", },
    {
      { "A", "", { 2, 0, 0, } },
      { "B", "", { 2, 0, 0, } },
      { "C", "", { 2, 0, 0, } },
    },
    {},
  };

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);

  TwoDimensionalGreedyAlgo algo(
      TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST);
  for (int i = 0; i < 20; ++i) {
    vector<TableReplicaMove> moves;
    ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
    ASSERT_EQ(1, moves.size());
    EXPECT_EQ("A", moves[0].table_id);
  }
}

// Various scenarios of balanced configurations where no moves are expected
// to happen.
TEST(RebalanceAlgoUnitTest, AlreadyBalanced) {
  // The configurations are already balanced, no moves should be attempted.
  const TestClusterConfig kConfigs[] = {
    {
      // A single tablet server with a single replica of the only table.
      kNoLocations,
      { "0", },
      {
        { "A", "", { 1 } },
      },
    },
    {
      // A single tablet server in the cluster that hosts all replicas.
      kNoLocations,
      { "0", },
      {
        { "A", "", { 1 } },
        { "B", "", { 10 } },
        { "C", "", { 100 } },
      },
    },
    {
      // Single table and 2 TS: 100 and 99 replicas at each.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 100, 99, } },
      },
    },
    {
      // Table- and cluster-wise balanced configuration with one-off skew.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 1, 1, } },
        { "B", "", { 1, 2, } },
      },
    },
    {
      // A configuration which has zero skew cluster-wise, while the table-wise
      // balance has one-off skew: the algorithm should not try to correct
      // the latter.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 1, 2, } },
        { "B", "", { 1, 2, } },
        { "C", "", { 1, 0, } },
        { "D", "", { 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 0, } },
        { "B", "", { 0, 1, 0, } },
        { "C", "", { 0, 0, 1, } },
      },
    },
    {
      // A simple balanced case: 3 tablet servers, 3 tables with
      // one replica per server.
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 1, 1, } },
        { "B", "", { 1, 1, 1, } },
        { "C", "", { 1, 1, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 0, 1, 1, } },
        { "B", "", { 1, 0, 1, } },
        { "C", "", { 1, 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 1, 1, } },
        { "B", "", { 1, 2, 1, } },
        { "C", "", { 1, 1, 2, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 1, 0, } },
        { "B", "", { 1, 1, 0, } },
        { "C", "", { 1, 0, 1, } },
        { "D", "", { 1, 0, 1, } },
        { "E", "", { 0, 1, 1, } },
        { "F", "", { 0, 1, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 1, } },
        { "B", "", { 1, 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "B", "", { 1, 0, 1, } },
        { "A", "", { 1, 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 2, 1, } },
        { "B", "", { 1, 0, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 2, 1, } },
        { "B", "", { 1, 1, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 2, 1, } },
        { "B", "", { 0, 0, 1, } },
        { "C", "", { 0, 0, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 0, 1, 0, } },
        { "B", "", { 1, 0, 1, } },
        { "C", "", { 1, 0, 1, } },
      },
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Various scenarios of balanced configurations where no moves are expected
// to happen, exercising range balancing.
TEST(RebalanceAlgoUnitTest, AlreadyBalancedRanges) {
  // The configurations are already balanced, no moves should be attempted.
  const TestClusterConfig kConfigs[] = {
    {
      // A single tablet server with a single replica of the only table.
      kNoLocations,
      { "0", },
      {
        { "A", "R0", { 1 } },
      },
    },
    {
      // A single tablet server in the cluster that hosts all replicas.
      kNoLocations,
      { "0", },
      {
        { "A", "R0", { 1 } },
        { "B", "R0", { 10 } },
        { "C", "R0", { 100 } },
      },
    },
    {
      // A single tablet server in the cluster that hosts all replicas.
      kNoLocations,
      { "0", },
      {
        { "A", "R0", { 1 } },
        { "B", "R1", { 10 } },
        { "C", "R2", { 100 } },
      },
    },
    {
      // Single table and 2 TS: 100 and 99 replicas at each.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 100, 99, } },
      },
    },
    {
      // Table- and cluster-wise balanced configuration with one-off skew.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 1, 1, } },
        { "B", "R0", { 1, 2, } },
      },
    },
    {
      // Table- and cluster-wise balanced configuration with one-off skew.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 1, 1, } },
        { "B", "R1", { 1, 2, } },
      },
    },
    {
      // A configuration which has zero skew cluster-wise, while the table-wise
      // balance has one-off skew: the algorithm should not try to correct
      // the latter.
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 1, 2, } },
        { "B", "R1", { 1, 2, } },
        { "C", "R2", { 1, 0, } },
        { "D", "R3", { 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 0, 0, } },
        { "B", "R0", { 0, 1, 0, } },
        { "C", "R1", { 0, 0, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 0, 0, } },
        { "B", "R1", { 0, 1, 0, } },
        { "C", "R2", { 0, 0, 1, } },
      },
    },
    {
      // A simple balanced case: 3 tablet servers, 3 tables with
      // one replica per server.
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 1, 1, } },
        { "B", "R1", { 1, 1, 1, } },
        { "C", "R2", { 1, 1, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 0, 1, 1, } },
        { "B", "R1", { 1, 0, 1, } },
        { "C", "R2", { 1, 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 2, 1, 1, } },
        { "B", "R1", { 1, 2, 1, } },
        { "C", "R2", { 1, 1, 2, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 1, 0, } },
        { "B", "R1", { 1, 1, 0, } },
        { "C", "R2", { 1, 0, 1, } },
        { "D", "R1", { 1, 0, 1, } },
        { "E", "R2", { 0, 1, 1, } },
        { "F", "R0", { 0, 1, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 0, 1, } },
        { "B", "R1", { 1, 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "B", "R0", { 1, 0, 1, } },
        { "A", "R1", { 1, 1, 0, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 2, 2, 1, } },
        { "B", "R1", { 1, 0, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R1", { 2, 2, 1, } },
        { "B", "R0", { 1, 1, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 2, 2, 1, } },
        { "B", "R1", { 0, 0, 1, } },
        { "C", "R1", { 0, 0, 1, } },
      },
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R1", { 0, 1, 0, } },
        { "B", "R0", { 1, 0, 1, } },
        { "C", "R2", { 1, 0, 1, } },
      },
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Set of scenarios where the distribution of replicas is table-wise balanced
// but not yet cluster-wise balanced, requiring just a few replica moves
// to achieve both table- and cluster-wise balance state.
TEST(RebalanceAlgoUnitTest, TableWiseBalanced) {
  const TestClusterConfig kConfigs[] = {
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 100, 99, } },
        { "B", "", { 100, 99, } },
      },
      { { "A", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 1, 2, } },
        { "B", "", { 1, 2, } },
        { "C", "", { 1, 0, } },
        { "D", "", { 0, 1, } },
      },
      { { "A", "", "1", "0" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 0, } },
        { "B", "", { 0, 1, 0, } },
        { "C", "", { 1, 0, 0, } },
      },
      { { "A", "", "0", "2" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 1, 1, } },
        { "B", "", { 0, 1, 1, } },
        { "C", "", { 0, 0, 1, } },
      },
      { { "B", "", "2", "0" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 1, 0, } },
        { "B", "", { 1, 0, 1, } },
        { "C", "", { 1, 0, 1, } },
      },
      { { "B", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "C", "", { 1, 0, 1, } },
        { "B", "", { 1, 0, 1, } },
        { "A", "", { 1, 1, 0, } },
      },
      { { "C", "", "0", "1" }, }
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Simple table-wise balanced configuration to have just one one-move
// to make them cluster-wise balanced as well.
TEST(RebalanceAlgoUnitTest, OneMoveNoCycling) {
  // The internals of the algorithm might depend on the table UUID ordering,
  // that's why multiples of virtually same configuration.
  const TestClusterConfig kConfigs[] = {
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 1, } },
        { "B", "", { 1, 0, 1, } },
        { "C", "", { 1, 1, 0, } },
      },
      { { "A", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 1, } },
        { "C", "", { 1, 0, 1, } },
        { "B", "", { 1, 1, 0, } },
      },
      { { "A", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "B", "", { 1, 0, 1, } },
        { "C", "", { 1, 0, 1, } },
        { "A", "", { 1, 1, 0, } },
      },
      { { "B", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "B", "", { 1, 0, 1, } },
        { "A", "", { 1, 0, 1, } },
        { "C", "", { 1, 1, 0, } },
      },
      { { "B", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "C", "", { 1, 0, 1, } },
        { "A", "", { 1, 0, 1, } },
        { "B", "", { 1, 1, 0, } },
      },
      { { "C", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "C", "", { 1, 0, 1, } },
        { "B", "", { 1, 0, 1, } },
        { "A", "", { 1, 1, 0, } },
      },
      { { "C", "", "0", "1" }, }
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Set of scenarios where the distribution of table replicas is cluster-wise
// balanced, but not table-wise balanced, requiring just few moves to make it
// both table- and cluster-wise balanced.
TEST(RebalanceAlgoUnitTest, ClusterWiseBalanced) {
  const TestClusterConfig kConfigs[] = {
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 2, 0, } },
        { "B", "", { 1, 2, } },
      },
      {
        { "A", "", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 1, 2, } },
        { "B", "", { 2, 0, } },
        { "C", "", { 1, 2, } },
      },
      {
        { "B", "", "0", "1" },
        { "A", "", "1", "0" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 1, 0, } },
        { "B", "", { 0, 1, 2, } },
      },
      {
        { "A", "", "0", "2" },
        { "B", "", "2", "0" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 1, 0, } },
        { "B", "", { 0, 1, 2, } },
        { "C", "", { 1, 1, 2, } },
      },
      {
        { "A", "", "0", "2" },
        { "B", "", "2", "0" },
      }
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Unbalanced (both table- and cluster-wise) and simple enough configurations
// to make them balanced moving just few replicas.
TEST(RebalanceAlgoUnitTest, FewMoves) {
  const TestClusterConfig kConfigs[] = {
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 2, 0, } },
      },
      { { "A", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 3, 0, } },
      },
      { { "A", "", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 4, 0, } },
      },
      {
        { "A", "", "0", "1" },
        { "A", "", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 1, 2, } },
        { "B", "", { 2, 0, } },
        { "C", "", { 2, 1, } },
      },
      {
        { "B", "", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "", { 4, 0, } },
        { "B", "", { 1, 3, } },
      },
      {
        { "A", "", "0", "1" },
        { "B", "", "1", "0" },
        { "A", "", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 4, 2, 0, } },
        { "B", "", { 2, 1, 0, } },
        { "C", "", { 1, 1, 1, } },
      },
      {
        { "A", "", "0", "2" },
        { "B", "", "0", "2" },
        { "A", "", "0", "2" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 2, 1, 0, } },
        { "B", "", { 3, 2, 1, } },
        { "C", "", { 2, 3, 5, } },
      },
      {
        { "C", "", "2", "0" },
        { "A", "", "0", "2" },
        { "B", "", "0", "2" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 5, 1, 0, } },
      },
      {
        { "A", "", "0", "2" },
        { "A", "", "0", "1" },
        { "A", "", "0", "2" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "", { 5, 1, 0, } },
        { "B", "", { 0, 1, 5, } },
      },
      {
        { "A", "", "0", "2" },
        { "B", "", "2", "0" },
        { "A", "", "0", "1" },
        { "B", "", "2", "1" },
        { "A", "", "0", "2" },
        { "B", "", "2", "0" },
      }
    },
  };
  VERIFY_MOVES(kConfigs);
}

TEST(RebalanceAlgoUnitTest, FewMovesSameTableRanges) {
  const TestClusterConfig kConfigs[] = {
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 2, 0, } },
      },
      { { "A", "R0", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 3, 0, } },
      },
      { { "A", "R0", "0", "1" }, }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 4, 0, } },
        { "A", "", { 1, 3, } },
      },
      {
        { "A", "R0", "0", "1" },
        { "A", "", "1", "0" },
        { "A", "R0", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 1, 2, } },
        { "A", "R1", { 2, 0, } },
        { "A", "R2", { 2, 1, } },
        { "A", "", { 2, 2, } },
      },
      {
        { "A", "R1", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", },
      {
        { "A", "R0", { 4, 0, } },
        { "A", "R1", { 1, 3, } },
        { "A", "", { 0, 2, } },
      },
      {
        { "A", "R0", "0", "1" },
        { "A", "R1", "1", "0" },
        { "A", "", "1", "0" },
        { "A", "R0", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 4, 2, 0, } },
        { "A", "R1", { 2, 1, 0, } },
        { "A", "R2", { 1, 1, 1, } },
        { "A", "", { 0, 2, 1, } },
      },
      {
        { "A", "R0", "0", "2" },
        { "A", "R1", "0", "2" },
        { "A", "", "1", "0" },
        { "A", "R0", "0", "2" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 2, 1, 0, } },
        { "A", "R1", { 3, 2, 1, } },
        { "A", "R2", { 2, 3, 5, } },
        { "A", "", { 6, 0, 0, } },
      },
      {
        { "A", "", "0", "1" },
        { "A", "", "0", "2" },
        { "A", "R2", "2", "0" },
        { "A", "", "0", "2" },
        { "A", "R0", "0", "2" },
        { "A", "R1", "0", "2" },
        { "A", "", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 5, 1, 0, } },
        { "A", "", { 2, 1, 0, } },
      },
      {
        { "A", "R0", "0", "2" },
        { "A", "R0", "0", "2" },
        { "A", "", "0", "2" },
        { "A", "R0", "0", "1" },
      }
    },
    {
      kNoLocations,
      { "0", "1", "2", },
      {
        { "A", "R0", { 5, 1, 0, } },
        { "A", "R1", { 0, 1, 5, } },
      },
      {
        { "A", "R0", "0", "2" },
        { "A", "R1", "2", "0" },
        { "A", "R0", "0", "1" },
        { "A", "R1", "2", "1" },
        { "A", "R0", "0", "2" },
        { "A", "R1", "2", "0" },
      }
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Unbalanced (both table- and cluster-wise) and simple enough configurations to
// make them balanced moving many replicas around.
TEST(RebalanceAlgoUnitTest, ManyMoves) {
  const TestClusterConfig kConfig = {
    kNoLocations,
    { "0", "1", "2", },
    {
      { "A", "", { 100, 400, 100, } },
    },
  };
  constexpr size_t kExpectedMovesNum = 200;

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);

  vector<TableReplicaMove> ref_moves;
  for (size_t i = 0; i < kExpectedMovesNum; ++i) {
    if (i % 2) {
      ref_moves.push_back({ "A", "", "1", "2" });
    } else {
      ref_moves.push_back({ "A", "", "1", "0" });
    }
  }

  TwoDimensionalGreedyAlgo algo(
      TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST);
  vector<TableReplicaMove> moves;
  ASSERT_OK(algo.GetNextMoves(ci, 0, &moves));
  EXPECT_EQ(ref_moves, moves);
}

TEST(RebalanceAlgoUnitTest, RandomizedTest) {
  const auto num_iters = AllowSlowTests() ? 1000 : 100;
  Random r(SeedRandom());
  const auto max_tservers = 10;
  const auto max_tables = 10;
  const auto max_replicas_per_table_and_tserver = 25;
  for (auto i = 0; i < num_iters; i++) {
    // Generate a random cluster config.
    const auto num_tservers = 1 + r.Uniform(max_tservers);
    const auto num_tables = 1 + r.Uniform(max_tables);
    vector<string> tserver_uuids;
    tserver_uuids.reserve(num_tservers);
    for (auto i = 0; i < num_tservers; i++) {
      tserver_uuids.push_back(Substitute("$0", i));
    }
    vector<TablePerServerReplicas> table_replicas;
    table_replicas.reserve(num_tables);
    for (auto i = 0; i < num_tables; i++) {
      vector<size_t> num_replicas_per_server;
      num_replicas_per_server.reserve(num_tservers);
      for (auto j = 0; j < num_tservers; j++) {
        num_replicas_per_server.push_back(
            r.Uniform(1 + max_replicas_per_table_and_tserver));
      }
      table_replicas.push_back(TablePerServerReplicas{
          Substitute("$0", i),
          std::to_string(r.Next()), // a randomized tag
          std::move(num_replicas_per_server),
      });
    }
    TestClusterConfig cfg{
      kNoLocations,
      std::move(tserver_uuids),
      std::move(table_replicas),
      {}  // This tests checks achievement of balance, not the path to it.
    };

    // Make sure the rebalancing algorithm can balance the config.
    {
      SCOPED_TRACE(TestClusterConfigToDebugString(cfg));
      ClusterInfo ci;
      ClusterConfigToClusterInfo(cfg, &ci);
      TwoDimensionalGreedyAlgo algo;
      optional<TableReplicaMove> move;
      // Set a generous upper bound on the number of moves allowed before we
      // conclude the algorithm is not converging.
      // We shouldn't need to do more moves than there are replicas.
      int num_moves_ub = num_tservers * num_tables * max_replicas_per_table_and_tserver;
      int num_moves = 0;
      while (!IsBalanced(ci.balance)) {
        ASSERT_OK(algo.GetNextMove(ci, &move));
        ASSERT_OK(TwoDimensionalGreedyAlgo::ApplyMove(*move, &ci.balance));
        ASSERT_GE(num_moves_ub, ++num_moves)
            << "Too many moves! The algorithm is likely stuck";
      }
    }
  }
}

// Location-based rebalancing, the case of few moves because of slight (if any)
// location load imbalance.
TEST(RebalanceAlgoUnitTest, LocationBalancingFewMoves) {
  const TestClusterConfig kConfigs[] = {
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 1, 0, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 0, 0, 1, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 1, 1, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 1, 1, 1, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 2, 1, 0, } }, },
      { { "A", "", "0", "2" }, }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 1, 1, 2, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 2, 1, 3, } }, },
      { { "A", "", "2", "1" }, }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 2, 4, 0, } }, },
      {
        { "A", "", "1", "2" },
        { "A", "", "1", "2" },
      }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5" },
      { { "A", "", { 1, 1, 1, 1, 1, 1, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5" },
      { { "A", "", { 2, 0, 4, 0, 0, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5", },
      { { "A", "", { 0, 1, 1, 1, 1, 1, } }, },
      {}
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5", },
      { { "A", "", { 0, 5, 0, 0, 0, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5", },
      { { "A", "", { 2, 1, 1, 1, 1, 0, } }, },
      { { "A", "", "0", "5" }, }
    },
  };
  VERIFY_LOCATION_BALANCING_MOVES(kConfigs);
}

TEST(RebalanceAlgoUnitTest, LocationBalancingRangesFewMoves) {
  const TestClusterConfig kConfigs[] = {
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "R0", { 2, 1, 0, } },
        { "A", "R1", { 2, 1, 0, } },
        { "A", "R2", { 1, 1, 4, } },
      },
      {
        { "A", "R0", "0", "2" },
        { "A", "R1", "0", "2" },
        { "A", "R2", "2", "0" },
        { "A", "R2", "2", "1" },
      },
      { MovesOrderingComparison::IGNORE }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 2, 0 } },
        { "A", "R1", { 2, 0, 1 } },
        { "A", "R2", { 2, 1, 0 } },
      },
      {
        { "A", "R0", "0", "2" },
        { "A", "R2", "0", "2" },
      },
      { MovesOrderingComparison::IGNORE }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "R0", { 1, 0, 0, } },
        { "A", "R1", { 3, 0, 0, } },
        { "A", "R2", { 0, 0, 2, } },
      },
      {
        { "A", "R2", "2", "1" },
        { "A", "R1", "0", "2" },
      }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "R0", { 0, 0, 5, } },
      },
      {
        { "A", "R0", "2", "0" },
        { "A", "R0", "2", "1" },
        { "A", "R0", "2", "0" },
      }
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "R0", { 1, 1, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", "1", }, },
        { "L1", { "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5" },
      { { "A", "", { 2, 0, 4, 0, 0, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5", },
      { { "A", "R0", { 0, 1, 1, 1, 1, 1, } }, },
      {}
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5", },
      { { "A", "R0", { 0, 5, 0, 0, 0, 0, } }, },
      {}
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", "2", "3", "4", "5", }, },
      },
      { "0", "1", "2", "3", "4", "5", },
      { { "A", "R0", { 2, 1, 1, 1, 1, 0, } }, },
      { { "A", "R0", "0", "5" }, }
    },
  };
  VERIFY_LOCATION_BALANCING_MOVES(kConfigs);
}

// A simple location-based rebalancing scenario, a single table.
TEST(RebalanceAlgoUnitTest, LocationBalancingSimpleST) {
  const TestClusterConfig kConfigs[] = {
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 2, 1, 0, } }, },
      { { "A", "", "0", "2" }, }
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      { { "A", "", { 6, 0, 0, } }, },
      {
        { "A", "", "0", "1" },
        { "A", "", "0", "2" },
        { "A", "", "0", "1" },
        { "A", "", "0", "2" },
      },
      { MovesOrderingComparison::IGNORE }
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 0, } },
      },
      {}
    },
  };
  VERIFY_LOCATION_BALANCING_MOVES(kConfigs);
}

// A simple location-based rebalancing scenario, multiple tables.
TEST(RebalanceAlgoUnitTest, LocationBalancingSimpleMT) {
  const TestClusterConfig kConfigs[] = {
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "", { 2, 1, 1, } },
        { "B", "", { 0, 0, 2, } },
      },
      { { "B", "", "2", "1" }, }
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "", { 2, 1, 0, } },
        { "B", "", { 0, 0, 3, } },
      },
      {
        { "B", "", "2", "1" },
        { "B", "", "2", "0" },
        { "A", "", "0", "2" },
      }
    },
    {
      {
        { "L0", { "0", }, },
        { "L1", { "1", }, },
        { "L2", { "2", }, },
      },
      { "0", "1", "2", },
      {
        { "A", "", { 1, 0, 0, } },
        { "B", "", { 1, 1, 2, } },
        { "C", "", { 10, 9, 10, } },
      },
      {}
    },
  };
  VERIFY_LOCATION_BALANCING_MOVES(kConfigs);
}

// LocationBalancingAlgo must prefer a source that hosts a non-leader replica
// of the target table+tag when 'prefer_follower_moves' is enabled and multiple
// equally most-loaded sources exist in the source location(s). When the flag
// is disabled, the algorithm falls back to its original deterministic choice
// (the first candidate in the internal multimap order).
TEST(RebalanceAlgoUnitTest, LocationBalancingPrefersFollowerSource) {
  // Location L0 has two servers ("0", "1") equally loaded (2 replicas each).
  // Location L1 has two servers ("2", "3") with no replicas. So L0 is the most
  // loaded location and both "0" and "1" tie for the max total load; only "1"
  // is registered as hosting a follower replica of the target table+tag.
  const TestClusterConfig kConfig = {
    {
      { "L0", { "0", "1", }, },
      { "L1", { "2", "3", }, },
    },
    { "0", "1", "2", "3", },
    { { "A", "", { 2, 2, 0, 0, } }, },
    {}
  };

  {
    // prefer_follower_moves = true: source must be "1".
    ClusterInfo ci;
    ClusterConfigToClusterInfo(kConfig, &ci);
    ci.ts_with_followers_by_table_and_tag[TableIdAndTag{ "A", "" }].insert("1");

    vector<TableReplicaMove> moves;
    LocationBalancingAlgo algo(1.0, /*prefer_follower_moves=*/true);
    ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
    ASSERT_EQ(1, moves.size());
    EXPECT_EQ("A", moves[0].table_id);
    EXPECT_EQ("", moves[0].tag);
    EXPECT_EQ("1", moves[0].from);
    // Destination must be one of the least-loaded servers in L1.
    EXPECT_TRUE(moves[0].to == "2" || moves[0].to == "3") << moves[0].to;
  }

  {
    // prefer_follower_moves = false: preserves the original tie-breaking, which
    // picks the first entry (server "0") in the max-load multimap range.
    ClusterInfo ci;
    ClusterConfigToClusterInfo(kConfig, &ci);
    ci.ts_with_followers_by_table_and_tag[TableIdAndTag{ "A", "" }].insert("1");

    vector<TableReplicaMove> moves;
    LocationBalancingAlgo algo(1.0, /*prefer_follower_moves=*/false);
    ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
    ASSERT_EQ(1, moves.size());
    EXPECT_EQ("0", moves[0].from);
    EXPECT_TRUE(moves[0].to == "2" || moves[0].to == "3") << moves[0].to;
  }
}

// When the follower map is empty (e.g. a hand-built ClusterInfo, matching the
// pre-BuildClusterInfo path), LocationBalancingAlgo must still emit a move via
// the leader fallback even with 'prefer_follower_moves' enabled. This mirrors
// the behavior of TwoDimensionalGreedyAlgo covered by
// PreferFollowerMovesLeaderFallbackWhenFollowerMapEmpty.
TEST(RebalanceAlgoUnitTest, LocationBalancingLeaderFallbackWhenFollowerMapEmpty) {
  const TestClusterConfig kConfig = {
    {
      { "L0", { "0", "1", }, },
      { "L1", { "2", "3", }, },
    },
    { "0", "1", "2", "3", },
    { { "A", "", { 2, 2, 0, 0, } }, },
    {}
  };

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);
  ASSERT_TRUE(ci.ts_with_followers_by_table_and_tag.empty());

  vector<TableReplicaMove> moves;
  LocationBalancingAlgo algo(1.0, /*prefer_follower_moves=*/true);
  ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
  ASSERT_EQ(1, moves.size());
  // Without any follower information, source falls back to the first entry
  // in the max-load multimap range (server "0").
  EXPECT_EQ("0", moves[0].from);
  EXPECT_TRUE(moves[0].to == "2" || moves[0].to == "3") << moves[0].to;
}

// If the follower map has an entry for the target table+tag but the set does
// not contain any of the equally most-loaded sources, LocationBalancingAlgo
// must still emit a move via the leader fallback.
TEST(RebalanceAlgoUnitTest, LocationBalancingLeaderFallbackWhenNoSourceHasFollowers) {
  const TestClusterConfig kConfig = {
    {
      { "L0", { "0", "1", }, },
      { "L1", { "2", "3", }, },
    },
    { "0", "1", "2", "3", },
    { { "A", "", { 2, 2, 0, 0, } }, },
    {}
  };

  ClusterInfo ci;
  ClusterConfigToClusterInfo(kConfig, &ci);
  // The follower entry exists but names a server that is not a candidate
  // source (server "2" is in the least-loaded location).
  ci.ts_with_followers_by_table_and_tag[TableIdAndTag{ "A", "" }].insert("2");

  vector<TableReplicaMove> moves;
  LocationBalancingAlgo algo(1.0, /*prefer_follower_moves=*/true);
  ASSERT_OK(algo.GetNextMoves(ci, 1, &moves));
  ASSERT_EQ(1, moves.size());
  EXPECT_EQ("0", moves[0].from);
  EXPECT_TRUE(moves[0].to == "2" || moves[0].to == "3") << moves[0].to;
}

} // namespace rebalance
} // namespace kudu
