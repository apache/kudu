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

#include "kudu/tools/location_maintenance.h"

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h"

using kudu::cluster_summary::ConsensusConfigType;
using kudu::cluster_summary::ConsensusState;
using kudu::cluster_summary::HealthCheckResult;
using kudu::cluster_summary::ReplicaSummary;
using kudu::cluster_summary::TabletSummary;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// A replica's tablet server, role, and server health. Health only matters for
// voters outside the target location.
struct ReplicaSpec {
  string ts_uuid;
  bool is_voter = true;
  bool ts_healthy = true;
};

ReplicaSummary MakeReplica(const ReplicaSpec& spec) {
  ReplicaSummary r;
  r.ts_uuid = spec.ts_uuid;
  r.is_voter = spec.is_voter;
  r.ts_healthy = spec.ts_healthy;
  return r;
}

TabletSummary MakeTablet(const string& tablet_id,
                         const string& table_id,
                         const string& table_name,
                         const vector<ReplicaSpec>& replicas,
                         HealthCheckResult result = HealthCheckResult::HEALTHY) {
  TabletSummary t;
  t.id = tablet_id;
  t.table_id = table_id;
  t.table_name = table_name;
  t.result = result;
  for (const auto& spec : replicas) {
    t.replicas.emplace_back(MakeReplica(spec));
  }
  return t;
}

// Three locations with three tservers each: ts-<loc>-<i> in /L<loc>.
unordered_map<string, string> ThreeLocationsByThree() {
  unordered_map<string, string> loc_by_ts;
  for (int l = 0; l < 3; l++) {
    for (int i = 0; i < 3; i++) {
      loc_by_ts.emplace(Substitute("ts-$0-$1", l, i), Substitute("/L$0", l));
    }
  }
  return loc_by_ts;
}

} // anonymous namespace

// One of three voters on the target: the two left are a majority.
TEST(LocationMaintenanceTest, OneOfThreeVotersOnTarget) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-1-0"}, {"ts-2-0"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_EQ("/L0", v.location);
  ASSERT_TRUE(v.safe);
  ASSERT_TRUE(v.blocking.empty());
}

// Two of three voters on the target: the one left isn't a majority.
TEST(LocationMaintenanceTest, TwoOfThreeVotersOnTarget) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-2-0"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_FALSE(v.safe);
  ASSERT_EQ(1, v.blocking.size());
  const auto& bt = v.blocking[0];
  ASSERT_EQ("tablet-0", bt.tablet_id);
  ASSERT_EQ("a", bt.table_name);
  ASSERT_EQ(3, bt.rf);
  ASSERT_EQ(2, bt.voters_on_target);
  ASSERT_EQ(1, bt.healthy_voters_off_target);
  ASSERT_EQ(0, bt.non_voters_on_target);
}

// All three voters on the target.
TEST(LocationMaintenanceTest, AllVotersOnTarget) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-0-2"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_FALSE(v.safe);
  ASSERT_EQ(1, v.blocking.size());
  ASSERT_EQ(3, v.blocking[0].voters_on_target);
  ASSERT_EQ(0, v.blocking[0].healthy_voters_off_target);
}

// RF=5 needs 3 votes, so two voters on the target is fine and three isn't.
TEST(LocationMaintenanceTest, ReplicationFactorFive) {
  unordered_map<string, string> loc_by_ts;
  for (int i = 0; i < 3; i++) {
    loc_by_ts.emplace(Substitute("ts-0-$0", i), "/L0");
  }
  for (int i = 0; i < 3; i++) {
    loc_by_ts.emplace(Substitute("ts-1-$0", i), "/L1");
  }
  const unordered_map<string, int> rf_by_table = {{"table-a", 5}};

  {
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0"}, {"ts-0-1"}, {"ts-1-0"}, {"ts-1-1"}, {"ts-1-2"}}),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
    ASSERT_TRUE(v.blocking.empty());
  }
  {
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0"}, {"ts-0-1"}, {"ts-0-2"}, {"ts-1-0"}, {"ts-1-1"}}),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_FALSE(v.safe);
    ASSERT_EQ(1, v.blocking.size());
    ASSERT_EQ(5, v.blocking[0].rf);
    ASSERT_EQ(3, v.blocking[0].voters_on_target);
    ASSERT_EQ(2, v.blocking[0].healthy_voters_off_target);
  }
}

// An RF=1 tablet blocks the location holding its only replica.
TEST(LocationMaintenanceTest, ReplicationFactorOneInLocation) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {
      {"table-rf1", 1},
      {"table-rf3", 3},
  };
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-rf1", "rf1", {{"ts-0-0"}}),
      MakeTablet("tablet-1", "table-rf3", "rf3",
                 {{"ts-0-1"}, {"ts-1-0"}, {"ts-2-0"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_FALSE(v.safe);
  ASSERT_EQ(1, v.blocking.size());
  const auto& bt = v.blocking[0];
  ASSERT_EQ("tablet-0", bt.tablet_id);
  ASSERT_EQ(1, bt.rf);
  ASSERT_EQ(1, bt.majority);
  ASSERT_EQ(1, bt.voters_on_target);
  ASSERT_EQ(0, bt.healthy_voters_off_target);
}

// An RF=1 tablet outside the location doesn't affect it.
TEST(LocationMaintenanceTest, ReplicationFactorOneOutsideLocation) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {
      {"table-rf1", 1},
      {"table-rf3", 3},
  };
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-rf1", "rf1", {{"ts-1-0"}}),
      MakeTablet("tablet-1", "table-rf3", "rf3",
                 {{"ts-0-0"}, {"ts-1-1"}, {"ts-2-0"}}),
  };

  for (const auto& location : {"/L0", "/L2"}) {
    SCOPED_TRACE(location);
    const auto v = EvaluateLocation(location, tablets, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
    ASSERT_TRUE(v.blocking.empty());
  }
}

// While a replacement replica is being added, an RF=3 tablet has four voters
// and needs three votes, so the two left outside the target aren't enough.
TEST(LocationMaintenanceTest, ExtraVoterWhileReReplicating) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-1-0"}, {"ts-2-0"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_FALSE(v.safe);
  ASSERT_EQ(1, v.blocking.size());
  const auto& bt = v.blocking[0];
  ASSERT_EQ(3, bt.rf);
  ASSERT_EQ(4, bt.voters_in_config);
  ASSERT_EQ(3, bt.majority);
  ASSERT_EQ(2, bt.voters_on_target);
  ASSERT_EQ(2, bt.healthy_voters_off_target);
}

// An under-replicated tablet still needs a majority of its RF. An RF=5 tablet
// down to three voters needs all three, so it can't lose even one.
TEST(LocationMaintenanceTest, UnderReplicatedKeepsReplicationFactorMajority) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 5}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-1-0"}, {"ts-2-0"}},
                 HealthCheckResult::UNDER_REPLICATED),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_FALSE(v.safe);
  ASSERT_EQ(1, v.blocking.size());
  const auto& bt = v.blocking[0];
  ASSERT_EQ(5, bt.rf);
  ASSERT_EQ(3, bt.voters_in_config);
  ASSERT_EQ(3, bt.majority);
  ASSERT_EQ(1, bt.voters_on_target);
  ASSERT_EQ(2, bt.healthy_voters_off_target);
}

// Non-voters don't count toward quorum, wherever they are.
TEST(LocationMaintenanceTest, NonVotersDontCountTowardQuorum) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};

  {
    // The only replica outside the target is a non-voter.
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0"}, {"ts-0-1"}, {"ts-1-0", /*is_voter=*/false}},
                   HealthCheckResult::UNDER_REPLICATED),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_FALSE(v.safe);
    ASSERT_EQ(1, v.blocking.size());
    ASSERT_EQ(2, v.blocking[0].voters_on_target);
    ASSERT_EQ(0, v.blocking[0].healthy_voters_off_target);
    ASSERT_EQ(0, v.blocking[0].non_voters_on_target);
  }
  {
    // A non-voter on the target doesn't matter.
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0", /*is_voter=*/false}, {"ts-1-0"}, {"ts-2-0"}}),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
    ASSERT_TRUE(v.blocking.empty());
  }
  {
    // Non-voters on the target are reported when the tablet blocks.
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0"}, {"ts-0-1"}, {"ts-0-2", /*is_voter=*/false},
                    {"ts-1-0"}},
                   HealthCheckResult::RECOVERING),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_FALSE(v.safe);
    ASSERT_EQ(1, v.blocking.size());
    ASSERT_EQ(2, v.blocking[0].voters_on_target);
    ASSERT_EQ(1, v.blocking[0].healthy_voters_off_target);
    ASSERT_EQ(1, v.blocking[0].non_voters_on_target);
  }
}

// Voters on unhealthy tablet servers outside the location don't count. Inside
// the location, health doesn't matter.
TEST(LocationMaintenanceTest, UnhealthyOffTargetVoterDoesntCount) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};

  {
    // Only one voter outside the target is healthy.
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0"},
                    {"ts-1-0"},
                    {"ts-2-0", /*is_voter=*/true, /*ts_healthy=*/false}},
                   HealthCheckResult::UNDER_REPLICATED),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_FALSE(v.safe);
    ASSERT_EQ(1, v.blocking.size());
    ASSERT_EQ(1, v.blocking[0].voters_on_target);
    ASSERT_EQ(1, v.blocking[0].healthy_voters_off_target);
  }
  {
    // An unhealthy voter on the target doesn't change anything.
    const vector<TabletSummary> tablets = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0", /*is_voter=*/true, /*ts_healthy=*/false},
                    {"ts-1-0"},
                    {"ts-2-0"}},
                   HealthCheckResult::UNDER_REPLICATED),
    };
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
    ASSERT_TRUE(v.blocking.empty());
  }
}

// Tablets of tables with no known RF are skipped.
TEST(LocationMaintenanceTest, UnknownTableIdIsSkipped) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      // Would block if its RF were known.
      MakeTablet("tablet-0", "table-missing", "missing",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-0-2"}}),
      MakeTablet("tablet-1", "table-a", "a",
                 {{"ts-0-0"}, {"ts-1-0"}, {"ts-2-0"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_TRUE(v.safe);
  ASSERT_TRUE(v.blocking.empty());
}

// A tablet server missing from 'loc_by_ts' is treated as being in the ""
// location. The tool doesn't allow that, but EvaluateLocation() copes anyway.
TEST(LocationMaintenanceTest, UnassignedLocation) {
  const unordered_map<string, string> loc_by_ts = {
      {"ts-0-0", "/L0"},
      {"ts-1-0", "/L1"},
      // ts-unassigned is deliberately absent.
  };
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-1-0"}, {"ts-unassigned"}}),
  };

  {
    // Taking /L0 offline leaves two of three voters.
    const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
  }
  {
    // So does taking the "" location offline.
    const auto v = EvaluateLocation("", tablets, loc_by_ts, rf_by_table);
    ASSERT_EQ("", v.location);
    ASSERT_TRUE(v.safe);
  }
  {
    // Unless the other two voters are unhealthy.
    const vector<TabletSummary> unhealthy = {
        MakeTablet("tablet-0", "table-a", "a",
                   {{"ts-0-0", /*is_voter=*/true, /*ts_healthy=*/false},
                    {"ts-1-0", /*is_voter=*/true, /*ts_healthy=*/false},
                    {"ts-unassigned"}},
                   HealthCheckResult::UNAVAILABLE),
    };
    const auto v = EvaluateLocation("", unhealthy, loc_by_ts, rf_by_table);
    ASSERT_FALSE(v.safe);
    ASSERT_EQ(1, v.blocking.size());
    ASSERT_EQ(1, v.blocking[0].voters_on_target);
    ASSERT_EQ(0, v.blocking[0].healthy_voters_off_target);
  }
}

// A location with no replicas, or a cluster with no tablets, is safe.
TEST(LocationMaintenanceTest, NoReplicasInLocation) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {{"table-a", 3}};
  const vector<TabletSummary> tablets = {
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-1-0"}, {"ts-2-0"}}),
  };

  {
    const auto v = EvaluateLocation("/L-nonexistent", tablets, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
    ASSERT_TRUE(v.blocking.empty());
  }
  {
    const auto v = EvaluateLocation("/L0", {}, loc_by_ts, rf_by_table);
    ASSERT_TRUE(v.safe);
    ASSERT_TRUE(v.blocking.empty());
  }
}

// Every blocking tablet is reported, and no safe ones.
TEST(LocationMaintenanceTest, ReportsAllBlockingTablets) {
  const auto loc_by_ts = ThreeLocationsByThree();
  const unordered_map<string, int> rf_by_table = {
      {"table-a", 3},
      {"table-b", 5},
  };
  const vector<TabletSummary> tablets = {
      // Safe.
      MakeTablet("tablet-0", "table-a", "a",
                 {{"ts-0-0"}, {"ts-1-0"}, {"ts-2-0"}}),
      // Blocks: 2 of 3 voters on the target.
      MakeTablet("tablet-1", "table-a", "a",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-1-0"}}),
      // Blocks: 3 of 5 voters on the target.
      MakeTablet("tablet-2", "table-b", "b",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-0-2"}, {"ts-1-0"}, {"ts-2-0"}}),
      // Safe: 2 of 5 voters on the target.
      MakeTablet("tablet-3", "table-b", "b",
                 {{"ts-0-0"}, {"ts-0-1"}, {"ts-1-0"}, {"ts-1-1"}, {"ts-2-0"}}),
  };

  const auto v = EvaluateLocation("/L0", tablets, loc_by_ts, rf_by_table);
  ASSERT_FALSE(v.safe);
  ASSERT_EQ(2, v.blocking.size());
  ASSERT_EQ("tablet-1", v.blocking[0].tablet_id);
  ASSERT_EQ(3, v.blocking[0].rf);
  ASSERT_EQ("tablet-2", v.blocking[1].tablet_id);
  ASSERT_EQ(5, v.blocking[1].rf);
}

// A tablet is in flux if any replica reports a config change in progress.
// Replicas with no consensus state, e.g. on unhealthy servers, are ignored.
TEST(LocationMaintenanceTest, FindTabletsInFlux) {
  const auto with_cstate = [](TabletSummary t, ConsensusState cstate) {
    t.replicas[0].consensus_state = std::move(cstate);
    return t;
  };
  ConsensusState settled(ConsensusConfigType::COMMITTED, /*term=*/1,
                         /*opid_index=*/std::nullopt, /*leader_uuid=*/"ts-0-0",
                         {"ts-0-0", "ts-1-0", "ts-2-0"}, /*non_voters=*/{});
  ConsensusState pending = settled;
  pending.type = ConsensusConfigType::PENDING;
  ConsensusState replacing = settled;
  replacing.replace_uuids.insert("ts-0-0");
  ConsensusState promoting = settled;
  promoting.non_voter_uuids.insert("ts-0-1");

  const auto tablet = [](const string& id) {
    return MakeTablet(id, "table-a", "a", {{"ts-0-0"}, {"ts-1-0"}, {"ts-2-0"}});
  };
  const vector<TabletSummary> tablets = {
      tablet("no-cstate"),
      with_cstate(tablet("settled"), settled),
      with_cstate(tablet("pending"), pending),
      with_cstate(tablet("replacing"), replacing),
      with_cstate(tablet("promoting"), promoting),
  };
  ASSERT_EQ(vector<string>({"pending", "replacing", "promoting"}),
            FindTabletsInFlux(tablets));
  ASSERT_TRUE(FindTabletsInFlux({}).empty());
}

} // namespace tools
} // namespace kudu
