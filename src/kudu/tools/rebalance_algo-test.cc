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
#include <cstddef>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace tools {
struct TestClusterConfig;
}  // namespace tools
}  // namespace kudu

#define VERIFY_MOVES(test_config) \
  do { \
    for (auto idx = 0; idx < ARRAYSIZE((test_config)); ++idx) { \
      SCOPED_TRACE(Substitute("test config index: $0", idx)); \
      NO_FATALS(VerifyRebalancingMoves((test_config)[idx])); \
    } \
  } while (false)

using std::ostream;
using std::set;
using std::sort;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

struct TablePerServerReplicas {
  const string table_id;

  // Number of replicas of this table on each server in the cluster.
  const vector<size_t> num_replicas_by_server;
};

// Structure to describe rebalancing-related state of the cluster expressively
// enough for the tests.
struct TestClusterConfig {
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
};

bool operator==(const TableReplicaMove& lhs, const TableReplicaMove& rhs) {
  return
      lhs.table_id == rhs.table_id &&
      lhs.from == rhs.from &&
      lhs.to == rhs.to;
}

ostream& operator<<(ostream& o, const TableReplicaMove& move) {
  o << move.table_id << ":" << move.from << "->" << move.to;
  return o;
}

// Transform the definition of the test cluster into the ClusterBalanceInfo
// that is consumed by the rebalancing algorithm.
void ClusterConfigToClusterBalanceInfo(const TestClusterConfig& tcc,
                                       ClusterBalanceInfo* cbi) {
  // First verify that the configuration of the test cluster is valid.
  set<string> table_ids;
  for (const auto& table_replica_info : tcc.table_replicas) {
    CHECK_EQ(tcc.tserver_uuids.size(),
             table_replica_info.num_replicas_by_server.size());
    table_ids.emplace(table_replica_info.table_id);
  }
  CHECK_EQ(table_ids.size(), tcc.table_replicas.size());
  {
    // Check for uniqueness of the tablet servers' identifiers.
    set<string> uuids(tcc.tserver_uuids.begin(), tcc.tserver_uuids.end());
    CHECK_EQ(tcc.tserver_uuids.size(), uuids.size());
  }

  ClusterBalanceInfo result;
  for (size_t tserver_idx = 0; tserver_idx < tcc.tserver_uuids.size();
       ++tserver_idx) {
    // Total replica count at the tablet server.
    size_t count = 0;
    for (const auto& table_replica_info: tcc.table_replicas) {
      count += table_replica_info.num_replicas_by_server[tserver_idx];
    }
    result.servers_by_total_replica_count.emplace(count, tcc.tserver_uuids[tserver_idx]);
  }

  auto& table_info_by_skew = result.table_info_by_skew;
  for (size_t table_idx = 0; table_idx < tcc.table_replicas.size(); ++table_idx) {
    // Replicas of the current table per tablet server.
    const vector<size_t>& replicas_count =
        tcc.table_replicas[table_idx].num_replicas_by_server;
    TableBalanceInfo info;
    info.table_id = tcc.table_replicas[table_idx].table_id;
    for (size_t tserver_idx = 0; tserver_idx < replicas_count.size(); ++tserver_idx) {
      auto count = replicas_count[tserver_idx];
      info.servers_by_replica_count.emplace(count, tcc.tserver_uuids[tserver_idx]);
    }
    size_t max_count = info.servers_by_replica_count.rbegin()->first;
    size_t min_count = info.servers_by_replica_count.begin()->first;
    CHECK_GE(max_count, min_count);
    table_info_by_skew.emplace(max_count - min_count, std::move(info));
  }
  *cbi = std::move(result);
}

void VerifyRebalancingMoves(const TestClusterConfig& cfg) {
  vector<TableReplicaMove> moves;
  {
    ClusterBalanceInfo cbi;
    ClusterConfigToClusterBalanceInfo(cfg, &cbi);
    TwoDimensionalGreedyAlgo algo(
        TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST);
    ASSERT_OK(algo.GetNextMoves(std::move(cbi), 0, &moves));
  }
  EXPECT_EQ(cfg.expected_moves, moves);
}

// Test the behavior of the algorithm when no input information is given.
TEST(RebalanceAlgoUnitTest, EmptyClusterBalanceInfo) {
  TwoDimensionalGreedyAlgo algo;
  vector<TableReplicaMove> moves;
  ClusterBalanceInfo info;
  Status s = algo.GetNextMoves(info, 1, &moves);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_TRUE(moves.empty());
}

// Various scenarios of balanced configurations where no moves are expected
// to happen.
TEST(RebalanceAlgoUnitTest, AlreadyBalanced) {
  // The configurations are already balanced, no moves should be attempted.
  const TestClusterConfig kConfigs[] = {
    {
      // A single tablet server with a single replica of the only table.
      { "0", },
      {
        { "A", { 1 } },
      },
    },
    {
      // A single tablet server in the cluster that hosts all replicas.
      { "0", },
      {
        { "A", { 1 } },
        { "B", { 10 } },
        { "C", { 100 } },
      },
    },
    {
      // Single table and 2 TS: 100 and 99 replicas at each.
      { "0", "1", },
      {
        { "A", { 100, 99, } },
      },
    },
    {
      // Table- and cluster-wise balanced configuration with one-off skew.
      { "0", "1", },
      {
        { "A", { 1, 1, } },
        { "B", { 1, 2, } },
      },
    },
    {
      // A configuration which has zero skew cluster-wise, while the table-wise
      // balance has one-off skew: the algorithm should not try to correct
      // the latter.
      { "0", "1", },
      {
        { "A", { 1, 2, } },
        { "B", { 1, 2, } },
        { "C", { 1, 0, } },
        { "D", { 1, 0, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 0, 0, } },
        { "B", { 0, 1, 0, } },
        { "C", { 0, 0, 1, } },
      },
    },
    {
      // A simple balanced case: 3 tablet servers, 3 tables with
      // one replica per server.
      { "0", "1", "2", },
      {
        { "A", { 1, 1, 1, } },
        { "B", { 1, 1, 1, } },
        { "C", { 1, 1, 1, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 0, 1, 1, } },
        { "B", { 1, 0, 1, } },
        { "C", { 1, 1, 0, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 1, 1, } },
        { "B", { 1, 2, 1, } },
        { "C", { 1, 1, 2, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 1, 0, } },
        { "B", { 1, 1, 0, } },
        { "C", { 1, 0, 1, } },
        { "D", { 1, 0, 1, } },
        { "E", { 0, 1, 1, } },
        { "F", { 0, 1, 1, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 0, 1, } },
        { "B", { 1, 1, 0, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "B", { 1, 0, 1, } },
        { "A", { 1, 1, 0, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 2, 1, } },
        { "B", { 1, 0, 1, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 2, 1, } },
        { "B", { 1, 1, 1, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 2, 1, } },
        { "B", { 0, 0, 1, } },
        { "C", { 0, 0, 1, } },
      },
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 0, 1, 0, } },
        { "B", { 1, 0, 1, } },
        { "C", { 1, 0, 1, } },
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
      { "0", "1", },
      {
        { "A", { 100, 99, } },
        { "B", { 100, 99, } },
      },
      { { "A", "0", "1" }, }
    },
    {
      { "0", "1", },
      {
        { "A", { 1, 2, } },
        { "B", { 1, 2, } },
        { "C", { 1, 0, } },
        { "D", { 0, 1, } },
      },
      { { "A", "1", "0" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 0, 0, } },
        { "B", { 0, 1, 0, } },
        { "C", { 1, 0, 0, } },
      },
      { { "A", "0", "2" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 1, 1, } },
        { "B", { 0, 1, 1, } },
        { "C", { 0, 0, 1, } },
      },
      { { "B", "2", "0" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 1, 0, } },
        { "B", { 1, 0, 1, } },
        { "C", { 1, 0, 1, } },
      },
      { { "B", "0", "1" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "C", { 1, 0, 1, } },
        { "B", { 1, 0, 1, } },
        { "A", { 1, 1, 0, } },
      },
      { { "C", "0", "1" }, }
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
      { "0", "1", "2", },
      {
        { "A", { 1, 0, 1, } },
        { "B", { 1, 0, 1, } },
        { "C", { 1, 1, 0, } },
      },
      { { "A", "0", "1" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 1, 0, 1, } },
        { "C", { 1, 0, 1, } },
        { "B", { 1, 1, 0, } },
      },
      { { "A", "0", "1" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "B", { 1, 0, 1, } },
        { "C", { 1, 0, 1, } },
        { "A", { 1, 1, 0, } },
      },
      { { "B", "0", "1" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "B", { 1, 0, 1, } },
        { "A", { 1, 0, 1, } },
        { "C", { 1, 1, 0, } },
      },
      { { "B", "0", "1" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "C", { 1, 0, 1, } },
        { "A", { 1, 0, 1, } },
        { "B", { 1, 1, 0, } },
      },
      { { "C", "0", "1" }, }
    },
    {
      { "0", "1", "2", },
      {
        { "C", { 1, 0, 1, } },
        { "B", { 1, 0, 1, } },
        { "A", { 1, 1, 0, } },
      },
      { { "C", "0", "1" }, }
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
      { "0", "1", },
      {
        { "A", { 2, 0, } },
        { "B", { 1, 2, } },
      },
      {
        { "A", "0", "1" },
      }
    },
    {
      { "0", "1", },
      {
        { "A", { 1, 2, } },
        { "B", { 2, 0, } },
        { "C", { 1, 2, } },
      },
      {
        { "B", "0", "1" },
        { "A", "1", "0" },
      }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 1, 0, } },
        { "B", { 0, 1, 2, } },
      },
      {
        { "A", "0", "2" },
        { "B", "2", "0" },
      }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 1, 0, } },
        { "B", { 0, 1, 2, } },
        { "C", { 1, 1, 2, } },
      },
      {
        { "A", "0", "2" },
        { "B", "2", "0" },
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
      { "0", "1", },
      {
        { "A", { 2, 0, } },
      },
      { { "A", "0", "1" }, }
    },
    {
      { "0", "1", },
      {
        { "A", { 3, 0, } },
      },
      { { "A", "0", "1" }, }
    },
    {
      { "0", "1", },
      {
        { "A", { 4, 0, } },
      },
      {
        { "A", "0", "1" },
        { "A", "0", "1" },
      }
    },
    {
      { "0", "1", },
      {
        { "A", { 1, 2, } },
        { "B", { 2, 0, } },
        { "C", { 2, 1, } },
      },
      {
        { "B", "0", "1" },
      }
    },
    {
      { "0", "1", },
      {
        { "A", { 4, 0, } },
        { "B", { 1, 3, } },
      },
      {
        { "A", "0", "1" },
        { "B", "1", "0" },
        { "A", "0", "1" },
      }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 4, 2, 0, } },
        { "B", { 2, 1, 0, } },
        { "C", { 1, 1, 1, } },
      },
      {
        { "A", "0", "2" },
        { "B", "0", "2" },
        { "A", "0", "2" },
      }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 2, 1, 0, } },
        { "B", { 3, 2, 1, } },
        { "C", { 2, 3, 5, } },
      },
      {
        { "C", "2", "0" },
        { "A", "0", "2" },
        { "B", "0", "2" },
      }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 5, 1, 0, } },
      },
      {
        { "A", "0", "2" },
        { "A", "0", "1" },
        { "A", "0", "2" },
      }
    },
    {
      { "0", "1", "2", },
      {
        { "A", { 5, 1, 0, } },
        { "B", { 0, 1, 5, } },
      },
      {
        { "A", "0", "2" },
        { "B", "2", "0" },
        { "A", "0", "1" },
        { "B", "2", "1" },
        { "A", "0", "2" },
        { "B", "2", "0" },
      }
    },
  };
  VERIFY_MOVES(kConfigs);
}

// Unbalanced (both table- and cluster-wise) and simple enough configurations to
// make them balanced moving many replicas around.
TEST(RebalanceAlgoUnitTest, ManyMoves) {
  const TestClusterConfig kConfig = {
    { "0", "1", "2", },
    {
      { "A", { 100, 400, 100, } },
    },
  };
  constexpr size_t kExpectedMovesNum = 200;

  ClusterBalanceInfo cbi;
  ClusterConfigToClusterBalanceInfo(kConfig, &cbi);

  vector<TableReplicaMove> ref_moves;
  for (size_t i = 0; i < kExpectedMovesNum; ++i) {
    if (i % 2) {
      ref_moves.push_back({ "A", "1", "2" });
    } else {
      ref_moves.push_back({ "A", "1", "0" });
    }
  }

  TwoDimensionalGreedyAlgo algo(
      TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_FIRST);
  vector<TableReplicaMove> moves;
  ASSERT_OK(algo.GetNextMoves(cbi, 0, &moves));
  EXPECT_EQ(ref_moves, moves);
}

} // namespace tools
} // namespace kudu
