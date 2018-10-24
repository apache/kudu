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

#include "kudu/tools/placement_policy_util.h"

#include <cstdint>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/rebalance_algo.h"
#include "kudu/tools/rebalancer.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

// TODO(aserbin): consider renaming the structures below XxxInfo --> Xxx

// Information on a table.
struct TestTableInfo {
  const string id;
  const int replication_factor;
  const vector<string> tablet_ids;
};

// Information on tablet servers in the same location.
struct TabletServerLocationInfo {
  const string location;
  const vector<string> ts_ids;
};

// Information on tablet replicas hosted by a tablet server.
struct TabletServerReplicasInfo {
  const string ts_id;
  const vector<string> tablet_ids;
};

// Describes a cluster with emphasis on the placement policy constraints.
struct TestClusterConfig {
  // The input information on the test cluster.
  const vector<TestTableInfo> tables_info;
  const vector<TabletServerLocationInfo> ts_location_info;
  const vector<TabletServerReplicasInfo> replicas_info;

  // The expected information on placement policy violations and moves
  // to correct those.
  const vector<PlacementPolicyViolationInfo> reference_violations_info;
  const vector<Rebalancer::ReplicaMove> reference_replicas_to_remove;
};

// Transform the definition of the test cluster into the ClusterLocalityInfo
// and TabletsPlacementInfo.
void ClusterConfigToClusterPlacementInfo(const TestClusterConfig& tcc,
                                         ClusterLocalityInfo* cli,
                                         TabletsPlacementInfo* tpi) {
  // TODO(aserbin): add sanity checks on the results
  ClusterLocalityInfo result_cli;
  TabletsPlacementInfo result_tpi;

  for (const auto& table_info : tcc.tables_info) {
    TableInfo info;
    info.name = table_info.id;
    info.replication_factor = table_info.replication_factor;
    EmplaceOrDie(&result_tpi.tables_info, table_info.id, std::move(info));
    for (const auto& tablet_id : table_info.tablet_ids) {
      EmplaceOrDie(&result_tpi.tablet_to_table_id, tablet_id, table_info.id);
    }
  }

  for (const auto& location_info : tcc.ts_location_info) {
    const auto& location = location_info.location;
    // Populate ts_uuids_by_location.
    auto& ts_uuids = LookupOrEmplace(&result_cli.servers_by_location,
                                     location, set<string>());
    ts_uuids.insert(location_info.ts_ids.begin(), location_info.ts_ids.end());
    for (const auto& ts_id : ts_uuids) {
      EmplaceOrDie(&result_cli.location_by_ts_id, ts_id, location);
    }
  }

  for (const auto& replica_info : tcc.replicas_info) {
    const auto& ts_location = FindOrDie(result_cli.location_by_ts_id,
                                        replica_info.ts_id);
    // Populate replica_num_by_ts_id.
    EmplaceOrDie(&result_tpi.replica_num_by_ts_id,
                 replica_info.ts_id, replica_info.tablet_ids.size());
    for (const auto& tablet_id : replica_info.tablet_ids) {
      // Populate tablets_info.
      auto& tablet_info = LookupOrEmplace(&result_tpi.tablets_info,
                                          tablet_id, TabletInfo());
      tablet_info.config_idx = 0; // hard-coded for this type of test
      auto& ri = tablet_info.replicas_info;
      // For these tests, the first tablet in the list is assigned leader role.
      ri.push_back(TabletReplicaInfo{
          replica_info.ts_id,
          ri.empty() ? ReplicaRole::LEADER : ReplicaRole::FOLLOWER_VOTER });
      // Populate tablet_location_info.
      auto& count_by_location = LookupOrEmplace(
          &result_tpi.tablet_location_info, tablet_id, unordered_map<string, int>());
      ++LookupOrEmplace(&count_by_location, ts_location, 0);
    }
  }
  *cli = std::move(result_cli);
  *tpi = std::move(result_tpi);
}

// TODO(aserbin): is it needed at all?
bool operator==(const PlacementPolicyViolationInfo& lhs,
                const PlacementPolicyViolationInfo& rhs) {
  return lhs.tablet_id == rhs.tablet_id &&
      lhs.majority_location == rhs.majority_location &&
      lhs.replicas_num_at_majority_location ==
          rhs.replicas_num_at_majority_location &&
      lhs.replication_factor == rhs.replication_factor;
}

ostream& operator<<(ostream& s, const PlacementPolicyViolationInfo& info) {
  s << "{tablet_id: " << info.tablet_id
    << ", location: " << info.majority_location << "}";
  return s;
}

bool operator==(const Rebalancer::ReplicaMove& lhs,
                const Rebalancer::ReplicaMove& rhs) {
  CHECK(lhs.ts_uuid_to.empty());
  CHECK(rhs.ts_uuid_to.empty());
  // The config_opid_idx field is ingored in tests for brevity.
  return lhs.tablet_uuid == rhs.tablet_uuid &&
      lhs.ts_uuid_from == rhs.ts_uuid_from;
}

ostream& operator<<(ostream& s, const Rebalancer::ReplicaMove& info) {
  CHECK(info.ts_uuid_to.empty());
  s << "{tablet_id: " << info.tablet_uuid
    << ", ts_id: " << info.ts_uuid_from << "}";
  return s;
}

// The order of elements in the container of reported violations
// and the container of replica movements to fix the latter doesn't
// matter: they are independent by definition (because they are reported
// in per-tablet way) and either container must not have multiple entries
// per tablet anyway. For the ease of comparison with reference results,
// let's build ordered map out of those, where the key is tablet id
// and the value is the information on the violation or candidate replica
// movement.
typedef map<string, PlacementPolicyViolationInfo> PPVIMap;
typedef map<string, Rebalancer::ReplicaMove> ReplicaMovesMap;

void ViolationInfoVectorToMap(
    const vector<PlacementPolicyViolationInfo>& infos,
    PPVIMap* result) {
  PPVIMap ret;
  for (const auto& info : infos) {
    ASSERT_TRUE(EmplaceIfNotPresent(&ret, info.tablet_id, info));
  }
  *result = std::move(ret);
}

void ReplicaMoveVectorToMap(
    const vector<Rebalancer::ReplicaMove>& infos,
    ReplicaMovesMap* result) {
  ReplicaMovesMap ret;
  for (const auto& info : infos) {
    ASSERT_TRUE(EmplaceIfNotPresent(&ret, info.tablet_uuid, info));
  }
  *result = std::move(ret);
}

void CheckEqual(const vector<PlacementPolicyViolationInfo>& lhs,
                const vector<PlacementPolicyViolationInfo>& rhs) {
  PPVIMap lhs_map;
  NO_FATALS(ViolationInfoVectorToMap(lhs, &lhs_map));
  PPVIMap rhs_map;
  NO_FATALS(ViolationInfoVectorToMap(rhs, &rhs_map));
  ASSERT_EQ(lhs_map, rhs_map);
}

void CheckEqual(const vector<Rebalancer::ReplicaMove>& lhs,
                const vector<Rebalancer::ReplicaMove>& rhs) {
  ReplicaMovesMap lhs_map;
  NO_FATALS(ReplicaMoveVectorToMap(lhs, &lhs_map));
  ReplicaMovesMap rhs_map;
  NO_FATALS(ReplicaMoveVectorToMap(rhs, &rhs_map));
  ASSERT_EQ(lhs_map, rhs_map);
}

// A shortcut for DetectPlacementPolicyViolations() followed by
// FindMovesToReimposePlacementPolicy().
Status FindMovesToFixPolicyViolations(
    const TabletsPlacementInfo& placement_info,
    const ClusterLocalityInfo& locality_info,
    vector<PlacementPolicyViolationInfo>* violations_info,
    std::vector<Rebalancer::ReplicaMove>* replicas_to_remove) {
  DCHECK(replicas_to_remove);

  vector<PlacementPolicyViolationInfo> violations;
  RETURN_NOT_OK(DetectPlacementPolicyViolations(placement_info, &violations));

  if (violations.empty()) {
    // Nothing to do: no placement policy violations found.
    if (violations_info) {
      violations_info->clear();
    }
    replicas_to_remove->clear();
    return Status::OK();
  }
  RETURN_NOT_OK(FindMovesToReimposePlacementPolicy(
      placement_info, locality_info, violations, replicas_to_remove));
  *violations_info = std::move(violations);

  return Status::OK();
}

class ClusterLocationTest : public ::testing::Test {
 protected:
  void RunTest(const vector<TestClusterConfig>& test_configs) {
    for (auto idx = 0; idx < test_configs.size(); ++idx) {
      SCOPED_TRACE(Substitute("test config index: $0", idx));
      const auto& cfg = test_configs[idx];

      ClusterLocalityInfo cli;
      TabletsPlacementInfo tpi;
      ClusterConfigToClusterPlacementInfo(cfg, &cli, &tpi);

      vector<PlacementPolicyViolationInfo> violations;
      vector<Rebalancer::ReplicaMove> moves;
      ASSERT_OK(FindMovesToFixPolicyViolations(tpi, cli, &violations, &moves));

      NO_FATALS(CheckEqual(cfg.reference_violations_info, violations));
      NO_FATALS(CheckEqual(cfg.reference_replicas_to_remove, moves));
    }
  }
};

TEST_F(ClusterLocationTest, PlacementPolicyViolationsNone) {
  const vector<TestClusterConfig> configs = {
    // Single-replica tablets, all in one location: no violation to report.
    {
      {
        { "T0", 1, { "t0", "t1", "t2", } },
      },
      {
        { "L0", { "A", } },
        { "L1", { "B", } },
        { "L2", { "C", } },
      },
      {
        { "A", { "t0", "t1", "t2", } },
        { "B", {} },
        { "C", {} },
      },
    },

    // One RF=3 tablet, one replica per location: no violations to report.
    {
      {
        { "T0", 3, { "t0", } },
      },
      {
        { "L0", { "A", } },
        { "L1", { "B", } },
        { "L2", { "C", } },
      },
      {
        { "A", { "t0", } },
        { "B", { "t0", } },
        { "C", { "t0", } },
      },
    },
  };
  NO_FATALS(RunTest(configs));
}

TEST_F(ClusterLocationTest, PlacementPolicyViolationsSimple) {
  const vector<TestClusterConfig> configs = {
    // One RF=3 table with one tablet, all the replicas in one of the three
    // locations. In addition, one RF=1 table with one tablet has its single
    // replica in the same location.
    {
      {
        { "T0", 3, { "t0", } },
        { "X0", 1, { "x0", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", } },
        { "L2", { "E", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", "x0", } },
        { "D", {} },
        { "E", {} },
      },
      { { "t0", "L0" }, },
      { { "t0", "C" }, }
    },

    // One RF=3 tablet, two locations.
    {
      {
        { "T0", 3, { "t0", } },
      },
      {
        { "L0", { "A", "B", } },
        { "L1", { "C", } },
      },
      {
        { "A", { "t0", } },
        { "B", { "t0", } },
        { "C", { "t0", } },
      },
      { { "t0", "L0" }, },
      {},
    },

    // One RF=3 tablet, majority of replicas in one of three locations.
    {
      {
        { "T0", 3, { "t0", } },
      },
      {
        { "L0", { "A", "B", } },
        { "L1", { "C", } },
        { "L2", { "D", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } },
        { "C", { "t0", } },
        { "D", {} },
      },
      { { "t0", "L0" }, },
      { { "t0", "B" }, }
    },
  };
  NO_FATALS(RunTest(configs));
}

TEST_F(ClusterLocationTest, PlacementPolicyViolationsMixed) {
  const vector<TestClusterConfig> configs = {
    // Two tables: one of RF=3 and another of RF=1. For both two tablets of the
    // former, the replica distribution violates the placement policy.
    {
      {
        { "T0", 3, { "t0", "t1", } },
        { "X0", 1, { "x0", "x1", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", } },
        { "L2", { "F", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", "x0", } }, { "C", { "t0", } },
        { "D", { "t1", "x1", } }, { "E", { "t1", } },
        { "F", { "t1", } },
      },
      { { "t0", "L0" }, { "t1", "L1" }, },
      { { "t0", "B" }, { "t1", "E" } }
    },

    // Four RF=3 tablets: the replica placement of two tablets is OK,
    // but the placement of the two others' violates the placement policy.
    {
      {
        { "T0", 3, { "t0", "t1", "t2", "t3", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", } },
        { "L2", { "F", } },
      },
      {
        { "A", { "t0", "t2", } }, { "B", { "t0", "t3", } }, { "C", { "t0", } },
        { "D", { "t1", "t2", } }, { "E", { "t1", "t3", } },
        { "F", { "t1", "t2", "t3", } },
      },
      { { "t0", "L0" }, { "t1", "L1" }, },
      { { "t0", "B" }, { "t1", "E" } }
    },
  };
  NO_FATALS(RunTest(configs));
}

// That's a scenario to verify how FixPlacementPolicyViolations() works when
// there isn't a candidate replica to move. In general (i.e. in case of RF
// higher than 3), the existence of more than two locations does not guarantee
// there is always a way to distribute tablet replicas across locations
// so that no location has the majority of replicas.
TEST_F(ClusterLocationTest, NoCandidateMovesToFixPolicyViolations) {
  const vector<TestClusterConfig> configs = {
    // One RF=5 tablet with the distribution of its replica placement violating
    // the placement policy.
    {
      {
        { "T0", 5, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", "D", } },
        { "L1", { "E", } },
        { "L2", { "F", } },
      },
      {
        // Tablet server D doesn't host any replica of t0.
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", } },
        { "E", { "t0", } },
        { "F", { "t0", } },
      },
      { { "t0", "L0" }, },
      {},
    },
    // One RF=7 tablet with the distribution of its replica placement violating
    // the placement policy.
    {
      {
        { "T0", 7, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", "D", "E", } },
        { "L1", { "F", } },
        { "L2", { "G", } },
        { "L3", { "H", } },
      },
      {
        // Tablet server E doesn't host any replica of t0. The idea is to
        // verify that FindMovesToReimposePlacementPolicy() does not command
        // moving a replica within the same location to E or from F, G, or H
        // to F.
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", } },
            { "D", { "t0", } },
        { "F", { "t0", } },
        { "G", { "t0", } },
        { "H", { "t0", } },
      },
      { { "t0", "L0" }, },
      {},
    },
    {
      // One RF=6 tablet with replica placement violating the placement policy.
      {
        { "T0", 6, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", } },
        { "L2", { "F", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", } },
        { "D", { "t0", } }, { "E", { "t0", } },
        { "F", { "t0", } },
      },
      { { "t0", "L0" }, },
      {}
    },
  };
  for (auto idx = 0; idx < configs.size(); ++idx) {
    SCOPED_TRACE(Substitute("test config index: $0", idx));
    const auto& cfg = configs[idx];

    ClusterLocalityInfo cli;
    TabletsPlacementInfo tpi;
    ClusterConfigToClusterPlacementInfo(cfg, &cli, &tpi);

    vector<PlacementPolicyViolationInfo> violations;
    ASSERT_OK(DetectPlacementPolicyViolations(tpi, &violations));
    NO_FATALS(CheckEqual(cfg.reference_violations_info, violations));

    vector<Rebalancer::ReplicaMove> moves;
    auto s = FindMovesToReimposePlacementPolicy(tpi, cli, violations, &moves);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_TRUE(moves.empty());
  }
}

TEST_F(ClusterLocationTest, PlacementPolicyViolationsEvenRFEdgeCases) {
  const vector<TestClusterConfig> configs = {
    {
      // One location, RF=2 and RF=4.
      {
        { "T0", 2, { "t0", } },
        { "T1", 4, { "t1", } },
      },
      {
        { "L0", { "A", "B", "C", "D", "E", } },
      },
      {
        { "A", { "t0", "t1", } },
        { "B", { "t0", "t1", } },
        { "C", { "t1", } },
        { "D", { "t1", } },
      },
      { { "t0", "L0" }, { "t1", "L0" }, },
      {}
    },
    {
      // Two locations, RF=2.
      {
        { "T0", 2, { "t0", "t1", } },
      },
      {
        { "L0", { "A", "B", } },
        { "L1", { "D", "E", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } },
        { "D", { "t1", } }, { "E", { "t1", } },
      },
      { { "t0", "L0" }, { "t1", "L1" }, },
      {}
    },
    {
      // Two locations, RF=2 and RF=4.
      {
        { "T0", 2, { "t0", } },
        { "T1", 4, { "t1", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", "F", } },
      },
      {
        { "A", { "t0", "t1", } }, { "B", { "t0", "t1", } },
        { "D", { "t1", } }, { "E", { "t1", } },
      },
      { { "t0", "L0" }, { "t1", "L1" }, },
      {}
    },
    {
      // Two locations, two tablets, RF=2.
      {
        { "T0", 2, { "t0", "t1", } },
      },
      {
        { "L0", { "A", "B", } },
        { "L1", { "C", "D", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } },
        { "C", { "t1", } }, { "D", { "t1", } },
      },
      { { "t0", "L0" }, { "t1", "L1" }, },
      {}
    },
    {
      // Three locations, RF=4.
      {
        { "T0", 4, { "t0", } },
      },
      {
        { "L0", { "A", "B", } },
        { "L1", { "D", "E", } },
        { "L2", { "F", "G", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } },
        { "D", { "t0", } },
        { "F", { "t0", } },
      },
      { { "t0", "L0" }, },
      {}
    },
  };
  NO_FATALS(RunTest(configs));
}

TEST_F(ClusterLocationTest, PlacementPolicyViolationsEvenRF) {
  const vector<TestClusterConfig> configs = {
    {
      // Three locations, RF=6.
      {
        { "T0", 6, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", "F", } },
        { "L2", { "G", "H", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", } },
        { "D", { "t0", } }, { "F", { "t0", } },
        { "H", { "t0", } },
      },
      { { "t0", "L0" }, },
      { { "t0", "B" }, }
    },
    {
      // Three locations, RF=8.
      {
        { "T0", 8, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", "F", "G", } },
        { "L2", { "H", "J", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", } },
        { "D", { "t0", } }, { "E", { "t0", } }, { "F", { "t0", } },
        { "G", { "t0", } },
        { "H", { "t0", } },
      },
      { { "t0", "L1" }, },
      { { "t0", "D" }, }
    },
  };
  NO_FATALS(RunTest(configs));
}

TEST_F(ClusterLocationTest, PlacementPolicyViolationsNoneEvenRF) {
  const vector<TestClusterConfig> configs = {
    {
      // Three locations, RF=6.
      {
        { "T0", 6, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", "F", } },
        { "L2", { "G", "H", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } },
        { "D", { "t0", } }, { "E", { "t0", } },
        { "G", { "t0", } }, { "H", { "t0", } },
      },
      {},
      {}
    },
    {
      // Three locations, RF=8.
      {
        { "T0", 8, { "t0", } },
      },
      {
        { "L0", { "A", "B", "C", } },
        { "L1", { "D", "E", "F", } },
        { "L2", { "G", "H", } },
      },
      {
        { "A", { "t0", } }, { "B", { "t0", } }, { "C", { "t0", } },
        { "D", { "t0", } }, { "E", { "t0", } }, { "F", { "t0", } },
        { "G", { "t0", } }, { "H", { "t0", } },
      },
      {},
      {}
    },
  };
  NO_FATALS(RunTest(configs));
}

} // namespace tools
} // namespace kudu
