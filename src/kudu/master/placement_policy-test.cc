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

#include "kudu/master/placement_policy.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::initializer_list;
using std::map;
using std::multiset;
using std::make_optional;
using std::nullopt;
using std::optional;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

// Fixture to run placement policy-related scenarios.
class PlacementPolicyTest : public ::testing::Test {
 public:
  struct TSInfo {
    // TS identifier
    const string id;
    // number of tablet replicas hosted by TS
    const size_t replica_num;
    // number of tablet replicas in each dimension
    TabletNumByDimensionMap replica_num_by_dimension;
    // number of tablet replicas in each table and all its ranges.
    TabletNumByRangePerTableMap replica_num_by_range_and_table;
  };

  struct LocationInfo {
    const string id;                // location identifier
    vector<TSInfo> tablet_servers;  // tablet servers in the location
  };

  typedef map<string, shared_ptr<TSDescriptor>> TSDescriptorsMap;

  PlacementPolicyTest()
      : rng_(GetRandomSeed32()) {
  }

  const TSDescriptorVector& descriptors() const { return descriptors_; }

  ThreadSafeRandom* rng() { return &rng_; }

  // Get tablet server descriptors for the specified tablet server UUIDs.
  TSDescriptorVector GetDescriptors(const vector<string>& uuids) const {
    TSDescriptorVector result;
    // O(n^2) is not the best way to do this, but it's OK for test purposes.
    for (const auto& uuid : uuids) {
      for (const auto& desc : descriptors_) {
        if (uuid == desc->permanent_uuid()) {
          result.push_back(desc);
          break;
        }
      }
    }
    CHECK_EQ(uuids.size(), result.size());
    return result;
  }

 protected:
  // Convert the information on the cluster into TSDescriptorVector.
  static Status PopulateCluster(const vector<LocationInfo>& cluster_info,
                                TSDescriptorVector* descs) {
    TSDescriptorVector ts_descriptors;
    for (const auto& location_info : cluster_info) {
      const auto& ts_infos = location_info.tablet_servers;
      for (const auto& ts : ts_infos) {
        auto tsd(TSDescriptor::make_shared(ts.id));
        tsd->set_num_live_replicas(ts.replica_num);
        tsd->location_.emplace(location_info.id);
        tsd->set_num_live_replicas_by_dimension(ts.replica_num_by_dimension);
        for (const auto& [table, range_map] : ts.replica_num_by_range_and_table) {
          tsd->set_num_live_replicas_by_range_per_table(table, range_map);
        }
        ts_descriptors.emplace_back(std::move(tsd));
      }
    }

    *descs = std::move(ts_descriptors);
    return Status::OK();
  }

  Status Prepare(const vector<LocationInfo>& cluster_info) {
    return PopulateCluster(cluster_info, &descriptors_);
  }

  // In tests, working with multiset instead of ReplicaLocationsInfo proves
  // to be a bit handier.
  Status SelectLocations(int nreplicas,
                         multiset<string>* locations) {
    PlacementPolicy policy(descriptors_, &rng_);
    PlacementPolicy::ReplicaLocationsInfo info;
    RETURN_NOT_OK(policy.SelectReplicaLocations(nreplicas, &info));
    for (const auto& elem : info) {
      for (auto i = 0; i < elem.second; ++i) {
        locations->emplace(elem.first);
      }
    }
    return Status::OK();
  }

  static Status TSDescriptorVectorToMap(const TSDescriptorVector& v_descs,
                                        TSDescriptorsMap* m_descs) {
    for (const auto& desc : v_descs) {
      const auto& uuid = desc->permanent_uuid();
      if (!m_descs->emplace(uuid, desc).second) {
        return Status::IllegalState(
            Substitute("$0: TS descriptors with duplicate UUID", uuid));
      }
    }
    return Status::OK();
  }

 private:
  ThreadSafeRandom rng_;
  TSDescriptorVector descriptors_;
};

TEST_F(PlacementPolicyTest, SelectLocationsEdgeCases) {
  // 'No location case': expecting backward compatible behavior with
  // non-location-aware logic.
  const vector<LocationInfo> cluster_info = {
    { "", { { "ts0", 0 }, { "ts1", 10 }, { "ts2", 1 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  {
    multiset<string> locations;
    auto s = SelectLocations(3, &locations);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(3, locations.size());
    EXPECT_EQ(3, locations.count(""));
  }

  {
    multiset<string> locations;
    auto s = SelectLocations(5, &locations);
    ASSERT_TRUE(locations.empty());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
        "could not find next location after placing 3 out of 5 tablet replicas");
  }
}

TEST_F(PlacementPolicyTest, SelectLocations0) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 0 }, { "A_ts1", 10 }, } },
    { "B", { { "B_ts0", 0 }, { "B_ts1", 1 }, } },
    { "C", {} },
  };
  ASSERT_OK(Prepare(cluster_info));

  {
    // No replicas are slated to be hosted by an empty location.
    multiset<string> locations;
    auto s = SelectLocations(2, &locations);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(2, locations.size());
    EXPECT_EQ(1, locations.count("A"));
    EXPECT_EQ(1, locations.count("B"));
  }

  {
    // Choosing location B over A because B has lower weight.
    multiset<string> locations;
    auto s = SelectLocations(3, &locations);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(3, locations.size());
    EXPECT_EQ(1, locations.count("A"));
    EXPECT_EQ(2, locations.count("B"));
  }

  {
    multiset<string> locations;
    auto s = SelectLocations(4, &locations);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(4, locations.size());
    EXPECT_EQ(2, locations.count("A"));
    EXPECT_EQ(2, locations.count("B"));
  }

  {
    multiset<string> locations;
    auto s = SelectLocations(5, &locations);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
        "could not find next location after placing 4 out of 5 tablet replicas");
  }
}

TEST_F(PlacementPolicyTest, SelectLocations1) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 0 }, { "A_ts1", 10 }, { "A_ts2", 1 }, { "A_ts3", 3 }, } },
    { "B", { { "B_ts0", 0 }, { "B_ts1", 1 }, { "B_ts2", 2 }, } },
    { "C", { { "C_ts0", 0 }, { "C_ts1", 5}, } },
    { "D", {} },
  };
  ASSERT_OK(Prepare(cluster_info));

  {
    multiset<string> locations;
    auto s = SelectLocations(3, &locations);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(3, locations.size());
    EXPECT_EQ(1, locations.count("A"));
    EXPECT_EQ(1, locations.count("B"));
    EXPECT_EQ(1, locations.count("C"));
  }

  for (auto loc_num : { 2, 3, 4, 5, 6, 7, 8, 9 }) {
    multiset<string> locations;
    auto s = SelectLocations(loc_num, &locations);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(loc_num, locations.size());
    for (const auto& loc : locations) {
      EXPECT_LT(locations.count(loc), loc_num / 2 + 1)
          << loc << ": location is slated to contain the majority of replicas";
    }
  }
}

TEST_F(PlacementPolicyTest, PlaceExtraTabletReplicaNoLoc) {
  // 'No location case': expecting backward compatible behavior with
  // non-location-aware logic.
  const vector<LocationInfo> cluster_info = {
    {
      "",
      {
        { "ts0", 0 },
        { "ts1", 10, { { "labelA", 10 } } },
        { "ts2", 1, { { "labelA", 1 } } },
      }
    },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  for (const auto& label : initializer_list<optional<string>>{ nullopt, string("labelA") }) {
    {
      TSDescriptorVector existing(all.begin(), all.end());
      existing.pop_back();
      shared_ptr<TSDescriptor> extra_ts;
      ASSERT_OK(policy.PlaceExtraTabletReplica(existing, label, nullopt, nullopt, &extra_ts));
      ASSERT_TRUE(extra_ts);
      ASSERT_EQ("ts2", extra_ts->permanent_uuid());
    }

    {
      TSDescriptorVector existing(all.begin(), all.end());
      existing.pop_back();
      existing.pop_back();
      shared_ptr<TSDescriptor> extra_ts;
      ASSERT_OK(policy.PlaceExtraTabletReplica(existing, label, nullopt, nullopt, &extra_ts));
      ASSERT_TRUE(extra_ts);
      ASSERT_EQ("ts2", extra_ts->permanent_uuid());
    }

    {
      TSDescriptorVector existing(all.begin(), all.end());
      shared_ptr<TSDescriptor> extra_ts;
      const auto s = policy.PlaceExtraTabletReplica(existing, label, nullopt, nullopt, &extra_ts);
      ASSERT_TRUE(s.IsNotFound()) << s.ToString();
      ASSERT_FALSE(extra_ts);
      ASSERT_STR_CONTAINS(s.ToString(),
                          "could not select location for extra replica");
    }
  }
}

TEST_F(PlacementPolicyTest, PlaceTabletReplicasNoLoc) {
  // 'No location case': expecting backward-compatible behavior with the
  // legacy (i.e. non-location-aware) logic.
  const vector<LocationInfo> cluster_info = {
    {
      "",
      {
        { "ts0", 0 },
        { "ts1", 10, { { "labelA", 10 } } },
        { "ts2", 1, { { "labelA", 1 } } },
      }
    },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  for (const auto& label : initializer_list<optional<string>>{ nullopt, string("labelA") }) {
    // Ask just for a single replica.
    {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(1, label, nullopt, nullopt, &result));
      ASSERT_EQ(1, result.size());
      TSDescriptorsMap m;
      ASSERT_OK(TSDescriptorVectorToMap(result, &m));
      ASSERT_EQ(1, m.size());
      // "Power of Two Choices" should select less loaded servers.
      ASSERT_TRUE(m.count("ts0") == 1 || m.count("ts2") == 1);
      ASSERT_EQ(0, m.count("ts1"));
    }

    // Ask for number of replicas equal to the number of available tablet servers.
    {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(3, label, nullopt, nullopt, &result));
      ASSERT_EQ(3, result.size());
      TSDescriptorsMap m;
      ASSERT_OK(TSDescriptorVectorToMap(result, &m));
      ASSERT_EQ(1, m.count("ts0"));
      ASSERT_EQ(1, m.count("ts1"));
      ASSERT_EQ(1, m.count("ts2"));
    }

    // Try to ask for too many replicas when too few tablet servers are available.
    {
      TSDescriptorVector result;
      auto s = policy.PlaceTabletReplicas(4, label, nullopt, nullopt, &result);
      ASSERT_TRUE(s.IsNotFound()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(),
                          "could not find next location after placing "
                          "3 out of 4 tablet replicas");
      ASSERT_TRUE(result.empty());
    }
  }
}

TEST_F(PlacementPolicyTest, PlaceTabletReplicas) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 2 }, { "A_ts1", 1 }, { "A_ts2", 3 }, } },
    { "B", { { "B_ts0", 1 }, { "B_ts1", 2 }, { "B_ts2", 3 }, } },
    { "C", { { "C_ts0", 10 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  // Ask for number of replicas equal to the number of available locations.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(3, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(3, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0") + m.count("A_ts1"));
    ASSERT_EQ(1, m.count("B_ts0") + m.count("B_ts1"));
    ASSERT_EQ(0, m.count("A_ts2"));
    ASSERT_EQ(0, m.count("B_ts2"));
    ASSERT_EQ(1, m.count("C_ts0"));
  }

  // Make sure no location contains the majority of replicas when there is
  // enough locations to spread the replicas.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(5, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(5, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(1, m.count("A_ts1"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("B_ts1"));
    ASSERT_EQ(1, m.count("C_ts0"));
  }

  // Ask for number of replicas greater than the number of tablet servers.
  {
    TSDescriptorVector result;
    auto s = policy.PlaceTabletReplicas(8, nullopt, nullopt, nullopt, &result);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "could not find next location after placing "
                        "7 out of 8 tablet replicas");
    ASSERT_TRUE(result.empty());
  }
}

TEST_F(PlacementPolicyTest, PlaceTabletReplicasOneTSPerLocation) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 1 }, } },
    { "B", { { "B_ts0", 2 }, } },
    { "C", { { "C_ts0", 0 }, } },
    { "D", { { "D_ts0", 3 }, } },
    { "E", { { "E_ts0", 4 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  // Ask for number of replicas equal to the number of available locations.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(3, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(3, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(0, m.count("D_ts0"));
    ASSERT_EQ(0, m.count("E_ts0"));
  }

  // Ask for number of replicas equal to the number of available locations.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(5, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(5, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(1, m.count("D_ts0"));
    ASSERT_EQ(1, m.count("E_ts0"));
  }

  // Ask for number of replicas greater than the number of tablet servers.
  {
    TSDescriptorVector result;
    auto s = policy.PlaceTabletReplicas(6, nullopt, nullopt, nullopt, &result);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "could not find next location after placing "
                        "5 out of 6 tablet replicas");
    ASSERT_TRUE(result.empty());
  }
}

TEST_F(PlacementPolicyTest, PlaceTabletReplicasBalancingLocations) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 0 }, { "A_ts1", 1 }, } },
    { "B", { { "B_ts0", 1 }, { "B_ts1", 2 }, } },
    { "C", { { "C_ts0", 2 }, } },
    { "D", { { "D_ts0", 3 }, } },
    { "E", { { "E_ts0", 10 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  // Make sure no location contains the majority of replicas.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(3, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(3, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(0, m.count("A_ts1"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(0, m.count("B_ts1"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(0, m.count("D_ts0"));
    ASSERT_EQ(0, m.count("E_ts0"));
  }

  // Current location selection algorithm loads the locations evenly.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(5, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(5, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(0, m.count("D_ts0"));
    ASSERT_EQ(0, m.count("E_ts0"));
  }

  // Place as many tablet replicas as possible given the number of tablet
  // servers in the cluster.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(7, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(7, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    for (const auto& uuid : { "A_ts0", "A_ts1", "B_ts0", "B_ts1", "C_ts0",
                              "D_ts0", "E_ts0" }) {
      ASSERT_EQ(1, m.count(uuid));
    }
  }
}

// A few test scenarios to verify the functionality of PlaceExtraTabletReplica()
// in the presence of more than two locations. The use cases behind are
// automatic re-replication and replica movement.
TEST_F(PlacementPolicyTest, PlaceExtraTabletReplicaViolatedPolicy) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 10 }, { "A_ts1", 9 }, { "A_ts2", 8 }, { "A_ts3", 0 }, } },
    { "B", { { "B_ts0", 25 }, { "B_ts1", 50 }, { "B_ts2", 100 }, } },
    { "C", { { "C_ts0", 0 }, { "C_ts1", 1 }, { "C_ts2", 2 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  {
    // Start with some replica distribution that violates the very basic
    // constraint of no-majority-in-single-location. In this scenario, all three
    // replicas are in same location. Let's make sure that an additional replica
    // is placed elsewhere even if a spare tablet server is available
    // in the same location.
    const auto existing = GetDescriptors({ "A_ts0", "A_ts1", "A_ts2", });
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, nullopt, nullopt, nullopt, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // Within location a replica is placed by the 'power of 2' algorithm.
    ASSERT_TRUE(extra_ts->permanent_uuid() == "C_ts0" ||
                extra_ts->permanent_uuid() == "C_ts1")
        << extra_ts->permanent_uuid();

  }

  {
    // Make sure the replica placement algorithm avoid placing an extra replica
    // into the least loaded location if the no-majority-in-single-location
    // constraint would be violated.
    const auto existing = GetDescriptors({ "A_ts0", "A_ts1", "C_ts1", "C_ts2", });
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, nullopt, nullopt, nullopt, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // Within location a replica is placed by the 'power of 2' algorithm.
    ASSERT_TRUE(extra_ts->permanent_uuid() == "B_ts0" ||
                extra_ts->permanent_uuid() == "B_ts1")
        << extra_ts->permanent_uuid();
  }
}

// Test for randomness while selecting among locations of the same load.
TEST_F(PlacementPolicyTest, SelectLocationRandomnessForExtraReplica) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 1 }, { "A_ts1", 1 }, { "A_ts2", 1 }, } },
    { "B", { { "B_ts0", 1 }, { "B_ts1", 1 }, { "B_ts2", 1 }, } },
    { "C", { { "C_ts0", 1 }, { "C_ts1", 1 }, { "C_ts2", 1 }, } },
  };

  // Information on replicas already slated for placement.
  const PlacementPolicy::ReplicaLocationsInfo info = {
    { "A", 1 }, { "B", 1 }, { "C", 1 },
  };

  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  map<string, int> locations_stats;
  // Test for uniform distribution of the selected locations if selecting among
  // locations of the same load.
  for (auto i = 0; i < 3000; ++i) {
    string location;
    ASSERT_OK(policy.SelectLocation(3, info, &location));
    ++locations_stats[location];
  }
  ASSERT_EQ(3, locations_stats.size());
  EXPECT_LT(500, locations_stats["A"]);
  EXPECT_GT(1500, locations_stats["A"]);
  EXPECT_LT(500, locations_stats["B"]);
  EXPECT_GT(1500, locations_stats["B"]);
  EXPECT_LT(500, locations_stats["C"]);
  EXPECT_GT(1500, locations_stats["C"]);
}

// Place replicas into an empty cluster with 5 locations and 7 tablet servers.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasEmptyCluster_L5_TS7) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 0 }, { "A_ts1", 0 }, } },
    { "B", { { "B_ts0", 0 }, { "B_ts1", 0 }, } },
    { "C", { { "C_ts0", 0 }, } },
    { "D", { { "D_ts0", 0 }, } },
    { "E", { { "E_ts0", 0 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  // Less tablet replicas than available locations.
  {
    static constexpr auto num_replicas = 3;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    // Make sure the placement of replicas conforms with the main constraint:
    // no location should contain the majority of replicas, so no location
    // should get two replicas.
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_GE(1, m.count("A_ts0") + m.count("A_ts1"));
    ASSERT_GE(1, m.count("B_ts0") + m.count("B_ts1"));
    ASSERT_GE(1, m.count("C_ts0"));
    ASSERT_GE(1, m.count("D_ts0"));
    ASSERT_GE(1, m.count("E_ts0"));
    ASSERT_EQ(num_replicas,
              m.count("A_ts0") + m.count("A_ts1") +
              m.count("B_ts0") + m.count("B_ts1") +
              m.count("C_ts0") + m.count("D_ts0") + m.count("E_ts0"));
  }

  // Current location selection algorithm loads the locations evenly.
  {
    static constexpr auto num_replicas = 5;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    // Make sure the replicas are placed evently accross available locations.
    ASSERT_EQ(1, m.count("A_ts0") + m.count("A_ts1"));
    ASSERT_EQ(1, m.count("B_ts0") + m.count("B_ts1"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(1, m.count("D_ts0"));
    ASSERT_EQ(1, m.count("E_ts0"));
  }

  // Place as many tablet replicas as possible given the number of tablet
  // servers in the cluster.
  {
    static constexpr auto num_replicas = 7;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    for (const auto& uuid : { "A_ts0", "A_ts1", "B_ts0", "B_ts1",
                              "C_ts0", "D_ts0", "E_ts0" }) {
      ASSERT_EQ(1, m.count(uuid));
    }
  }

  // Ask for number of replicas greater than the number of tablet servers.
  {
    static constexpr auto num_replicas = 9;
    TSDescriptorVector result;
    auto s = policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    const string ref_msg = Substitute(
        "could not find next location after placing 7 out of $0 tablet replicas",
        num_replicas);
    ASSERT_STR_CONTAINS(s.ToString(), ref_msg);
    ASSERT_TRUE(result.empty());
  }
}

// Verify the functionality of PlaceExtraTabletReplica() in the presence of more
// than two locations when there is room to balance tablet replica distribution.
// The use case behind is rebalancing the replica distribution location-wise.
TEST_F(PlacementPolicyTest, PlaceExtraTablet_L5_TS10_RF5) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 1 }, { "A_ts1", 1 }, { "A_ts2", 0 }, } },
    { "B", { { "B_ts0", 2 }, { "B_ts1", 1 }, { "B_ts2", 1 }, { "B_ts3", 0 }, } },
    { "C", { { "C_ts0", 1 }, } },
    { "D", { { "D_ts0", 2 }, } },
    { "E", { { "E_ts0", 3 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  {
    // A RF=5 tablet, with the 'ideal' replica distribution: one replica
    // per location.
    const auto existing = GetDescriptors(
        { "A_ts0", "B_ts0", "C_ts0", "D_ts0", "E_ts0", });
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, nullopt, nullopt, nullopt, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // The location with the lowest load is selected for the extra replica.
    ASSERT_EQ("A_ts2", extra_ts->permanent_uuid());
  }
  {
    // A RF=5 tablet, with 'not ideal' replica distribution
    // (where the 'ideal' distribution would be one replica per location).
    const auto existing = GetDescriptors(
        { "A_ts0", "A_ts1", "B_ts0", "B_ts1", "C_ts0", });
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, nullopt, nullopt, nullopt, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // The location with the lowest load is selected for the extra replica.
    ASSERT_EQ("D_ts0", extra_ts->permanent_uuid());
  }
  {
    // A RF=5 tablet, with the replica distribution violating the placement
    // policy constraints.
    const auto existing = GetDescriptors(
        { "A_ts0", "B_ts0", "B_ts1", "B_ts2", "E_ts0", });
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, nullopt, nullopt, nullopt, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // Among the locations where an additional replica can be placed,
    // location A and location C have the least load. As for the preferences
    // within location A, the replica is placed using power-of-two choices among
    // the available servers (A_ts0 is not available since it hosts one replica
    // already). The important thing is to make sure an extra replica is not put
    // into location B, where it's already the majority of replicas for RF=5.
    ASSERT_TRUE(
        extra_ts->permanent_uuid() == "A_ts2" ||
        extra_ts->permanent_uuid() == "C_ts0") << extra_ts->permanent_uuid();
  }
}

// This test verifies how evenly an extra replica is placed among the available
// tablet servers in the least loaded locations.
TEST_F(PlacementPolicyTest, PlaceExtraTablet_L5_TS16_RF5) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 1 }, { "A_ts1", 0 }, { "A_ts2", 0 }, { "A_ts3", 0 }, } },
    { "B", { { "B_ts0", 1 }, { "B_ts1", 0 }, { "B_ts2", 0 }, { "B_ts3", 0 }, } },
    { "C", { { "C_ts0", 1 }, { "C_ts1", 1 }, { "C_ts2", 0 }, } },
    { "D", { { "D_ts0", 1 }, { "D_ts1", 1 }, { "D_ts2", 0 }, } },
    { "E", { { "E_ts0", 1 }, { "E_ts1", 0 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  // Slightly non-optimal replica placement.
  const auto existing = GetDescriptors(
      { "C_ts0", "C_ts1", "D_ts0", "D_ts1", "E_ts0", });

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  // Test for uniform distribution of the selected locations if selecting among
  // locations of the same load.
  map<string, int> placement_stats;
  for (auto i = 0; i < 6000; ++i) {
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, nullopt, nullopt, nullopt, &extra_ts));
    ASSERT_TRUE(extra_ts);
    const auto& ts_uuid = extra_ts->permanent_uuid();
    ASSERT_TRUE(ts_uuid == "A_ts1" ||
                ts_uuid == "A_ts2" ||
                ts_uuid == "A_ts3" ||
                ts_uuid == "B_ts1" ||
                ts_uuid == "B_ts2" ||
                ts_uuid == "B_ts3")
        << extra_ts->permanent_uuid();

    ++placement_stats[ts_uuid];
  }
  ASSERT_EQ(6, placement_stats.size());
  EXPECT_LT(500, placement_stats["A_ts1"]);
  EXPECT_GT(1500, placement_stats["A_ts1"]);
  EXPECT_LT(500, placement_stats["A_ts2"]);
  EXPECT_GT(1500, placement_stats["A_ts2"]);
  EXPECT_LT(500, placement_stats["A_ts3"]);
  EXPECT_GT(1500, placement_stats["A_ts3"]);
  EXPECT_LT(500, placement_stats["B_ts1"]);
  EXPECT_GT(1500, placement_stats["B_ts1"]);
  EXPECT_LT(500, placement_stats["B_ts2"]);
  EXPECT_GT(1500, placement_stats["B_ts2"]);
  EXPECT_LT(500, placement_stats["B_ts3"]);
  EXPECT_GT(1500, placement_stats["B_ts3"]);
}

// Even RF case: edge cases with 2 locations.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasEmptyCluster_L2_EvenRFEdgeCase) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 10 }, { "A_ts1", 10 }, { "A_ts2", 10 }, } },
    { "B", { { "B_ts0", 0 }, { "B_ts1", 0 }, { "B_ts2", 1 }, { "B_ts3", 10 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  {
    static constexpr auto num_replicas = 2;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_EQ(1, m.count("B_ts0") + m.count("B_ts1") + m.count("B_ts2"));
  }
  {
    static constexpr auto num_replicas = 4;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(2, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_EQ(2, m.count("B_ts0") + m.count("B_ts1") + m.count("B_ts2"));
  }
  {
    static constexpr auto num_replicas = 6;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(1, m.count("A_ts1"));
    ASSERT_EQ(1, m.count("A_ts2"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("B_ts1"));
    ASSERT_EQ(1, m.count("B_ts2") + m.count("B_ts3"));
  }
}

// Odd RF case: edge cases with 2 locations.
// Make sure replicas are placed into both locations, even if ideal density
// distribution would have them in a single location.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasCluster_L2_OddRFEdgeCase) {
  const vector<LocationInfo> cluster_info = {
      { "A", { { "A_ts0", 10 }, { "A_ts1", 10 }, { "A_ts2", 10 }, } },
      { "B", { { "B_ts0", 9 }, { "B_ts1", 9 }, { "B_ts2", 9 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  {
    static constexpr auto num_replicas = 3;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_EQ(2, m.count("B_ts0") + m.count("B_ts1") + m.count("B_ts2"));
  }

  {
    static constexpr auto num_replicas = 5;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(2, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_EQ(3, m.count("B_ts0") + m.count("B_ts1") + m.count("B_ts2"));
  }
}

// Even RF case: place tablet replicas into a cluster with 3 locations.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasEmptyCluster_L3_EvenRF) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 10 }, { "A_ts1", 10 }, { "A_ts2", 10 }, } },
    { "B", { { "B_ts0", 0 }, { "B_ts1", 0 }, { "B_ts2", 10 }, } },
    { "C", { { "C_ts0", 0 }, { "C_ts1", 0 }, { "C_ts2", 10 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  {
    static constexpr auto num_replicas = 2;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    // Two location are to have one replica, one location to have nullopt.
    ASSERT_GE(1, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_GE(1, m.count("B_ts0") + m.count("B_ts1"));
    ASSERT_GE(1, m.count("C_ts0") + m.count("C_ts1"));
  }
  {
    static constexpr auto num_replicas = 4;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    // One location is to have two replicas, the rest are to have just one.
    ASSERT_LE(1, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_GE(2, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_LE(1, m.count("B_ts0") + m.count("B_ts1"));
    ASSERT_GE(2, m.count("B_ts0") + m.count("B_ts1"));
    ASSERT_LE(1, m.count("C_ts0") + m.count("C_ts1"));
    ASSERT_GE(2, m.count("C_ts0") + m.count("C_ts1"));
  }
  {
    static constexpr auto num_replicas = 6;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(2, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("B_ts1"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(1, m.count("C_ts1"));
  }
  {
    static constexpr auto num_replicas = 8;
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(num_replicas, nullopt, nullopt, nullopt, &result));
    ASSERT_EQ(num_replicas, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(2, m.count("A_ts0") + m.count("A_ts1") + m.count("A_ts2"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("B_ts1"));
    ASSERT_EQ(1, m.count("B_ts2"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(1, m.count("C_ts1"));
    ASSERT_EQ(1, m.count("C_ts2"));
  }
}

// In a Kudu cluster with newly added tablet servers, add tablet replicas with and without
// dimension label and verify the result distribution of the replicas. Check for 1) overall
// distribution of replicas 2) distribution of replicas within the specified dimension.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithNewTabletServers) {
  const vector<LocationInfo> cluster_info = {
      {
        "",
        {
          { "ts0", 0 }, // a new tablet server
          { "ts1", 0 }, // a new tablet server
          { "ts2", 1000, { { "label0", 1000 }, } },
          { "ts3", 1000, { { "label0", 1000 }, } },
          { "ts4", 2000, { { "label0", 2000 }, } },
          { "ts5", 2000, { { "label0", 2000 }, } },
        }
      },
  };

  auto calc_stddev_func = [](const map<string, int>& placement_stats, double mean_per_ts) {
    double sum_squared_deviation = 0;
    for (const auto& stat : placement_stats) {
      int num_ts = stat.second;
      double deviation = static_cast<double>(num_ts) - mean_per_ts;
      sum_squared_deviation += deviation * deviation;
    }
    return sqrt(sum_squared_deviation / (mean_per_ts - 1));
  };

  // Place three replicas a thousand times.
  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());

    map<string, int> placement_stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      // Get the number of tablet replicas on tablet server.
      ASSERT_OK(policy.PlaceTabletReplicas(3, nullopt, nullopt, nullopt, &result));
      ASSERT_EQ(3, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++placement_stats[ts_uuid];
      }
    }

    ASSERT_EQ(6, placement_stats.size());
    const double kMeanPerServer = 3000 / 6.0;
    double stddev = calc_stddev_func(placement_stats, kMeanPerServer);
    ASSERT_GE(stddev, 20.0);
  }

  // Place three replicas with dimension labels a thousand times.
  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());

    for (const auto& label : { "label1", "label2", "label3", "label4", "label5" }) {
      map<string, int> placement_stats;
      for (auto i = 0; i < 1000; ++i) {
        TSDescriptorVector result;
        ASSERT_OK(policy.PlaceTabletReplicas(
            3, make_optional(string(label)), nullopt, nullopt, &result));
        ASSERT_EQ(3, result.size());
        for (const auto& ts : result) {
          const auto& ts_uuid = ts->permanent_uuid();
          ++placement_stats[ts_uuid];
        }
      }

      ASSERT_EQ(6, placement_stats.size());
      const double kMeanPerServer = 3000 / 6.0;
      double stddev = calc_stddev_func(placement_stats, kMeanPerServer);
      ASSERT_LE(stddev, 3.0);
    }
  }
}

// RF is 1 while number of TS is 2, extreme case where one empty tablet server gets a majority
// of the replicas placed but with range aware replica placement it's more balanced.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithRangesExtremeCase) {
  const int nReplicationFactor = 1;
  const int kNumServers = 2;
  const vector<LocationInfo> cluster_info = {
      {
          "",
          {
              { "ts0", 0 },
              { "ts1", 500 },
          }
      },
  };

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, nullopt, nullopt, &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      const auto& ts_uuid = result[0]->permanent_uuid();
      ++stats[ts_uuid];
    }
    ASSERT_EQ(kNumServers, stats.size());
    ASSERT_LE(748, stats["ts0"]);
    ASSERT_GE(752, stats["ts0"]);
    ASSERT_LE(248, stats["ts1"]);
    ASSERT_GE(252, stats["ts1"]);
  }

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, "a1", "t1", &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      const auto& ts_uuid = result[0]->permanent_uuid();
      ++stats[ts_uuid];
    }
    ASSERT_EQ(kNumServers, stats.size());
    ASSERT_EQ(500, stats["ts0"]);
    ASSERT_EQ(500, stats["ts1"]);
  }
}

// RF is equal to the number of tablet servers, so replica placement will be same with ranges.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithRanges) {
  const int nReplicationFactor = 3;
  const int kNumServers = 3;
  TabletNumByRangeMap range({ {"a1", 500} });
  const vector<LocationInfo> cluster_info = {
      {
          "",
          {
              { "ts0", 0 },
              { "ts1", 0 },
              { "ts2", 500, { }, { {"t1", range }, } },
          }
      },
  };

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, nullopt, nullopt, &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++stats[ts_uuid];
      }
    }
    ASSERT_EQ(kNumServers, stats.size());
    ASSERT_EQ(1000, stats["ts0"]);
    ASSERT_EQ(1000, stats["ts1"]);
    ASSERT_EQ(1000, stats["ts2"]);
  }

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, "a1", "t1", &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++stats[ts_uuid];
      }
    }
    ASSERT_EQ(kNumServers, stats.size());
    ASSERT_EQ(1000, stats["ts0"]);
    ASSERT_EQ(1000, stats["ts1"]);
    ASSERT_EQ(1000, stats["ts2"]);
  }
}

// RF is 3 while number of tablet servers is 4 with 2 of them empty and the other 2 loaded with
// 500 replicas, although only one tablet server has replicas with the same range that is being
// added. The expectation is that the tablet server with replicas not from that range will be
// treated similar to the empty tablet servers. This test case also ensures that ranges with the
// same start key from two different tables can properly be differentiated.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithRangesAndTables) {
  const int nReplicationFactor = 3;
  const int kNumServers = 4;
  TabletNumByRangeMap range({ {"a1", 500} });
  const vector<LocationInfo> cluster_info = {
      {
          "",
          {
              { "ts0", 0 },
              { "ts1", 0 },
              { "ts2", 500, { }, { {"t2", range }, } },
              { "ts3", 500, { }, { {"t1", range }, } },
          }
      },
  };

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, nullopt, nullopt, &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++stats[ts_uuid];
      }
    }
    ASSERT_EQ(kNumServers, stats.size());
    ASSERT_EQ(1000, stats["ts0"]);
    ASSERT_EQ(1000, stats["ts1"]);
    ASSERT_EQ(500, stats["ts2"]);
    ASSERT_EQ(500, stats["ts3"]);
  }

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, "a1", "t1", &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++stats[ts_uuid];
      }
    }
    ASSERT_EQ(kNumServers, stats.size());
    vector<string> tservers_no_load = {"ts0", "ts1", "ts2"};
    for (const auto& tserver : tservers_no_load) {
      ASSERT_LE(871, stats[tserver]);
      ASSERT_GE(881, stats[tserver]);
    }
    ASSERT_LE(368, stats["ts3"]);
    ASSERT_GE(378, stats["ts3"]);
  }
}

// RF is 3 while number of tablet servers is 6. Half of them are empty while the other half contain
// an increasing number of replicas. One of them does not contain replicas of the range that is
// being placed thus it is expected to be treated like the empty tablet servers when placing
// replicas by range.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithRangesAndTablesWithDoubleTServers) {
  const int nReplicationFactor = 3;
  const int kNumServers = 6;
  TabletNumByRangeMap range1({ {"b1", 250} });
  TabletNumByRangeMap range2({ {"a1", 375} });
  TabletNumByRangeMap range3({ {"a1", 500} });
  const vector<LocationInfo> cluster_info = {
      {
          "",
          {
              { "ts0", 0 },
              { "ts1", 0 },
              { "ts2", 0 },
              { "ts3", 250, { }, { {"t1", range1 }, } },
              { "ts4", 375, { }, { {"t1", range2 }, } },
              { "ts5", 500, { }, { {"t1", range3 }, } },
          }
      },
  };

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, nullopt, nullopt, &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++stats[ts_uuid];
      }
    }
    ASSERT_EQ(kNumServers, stats.size());
    vector<string> tservers_no_load = {"ts0", "ts1", "ts2"};
    for (const auto& tserver : tservers_no_load) {
      ASSERT_LE(682, stats[tserver]);
      ASSERT_GE(691, stats[tserver]);
    }
    ASSERT_LE(432, stats["ts3"]);
    ASSERT_GE(441, stats["ts3"]);
    ASSERT_LE(306, stats["ts4"]);
    ASSERT_GE(315, stats["ts4"]);
    ASSERT_LE(182, stats["ts5"]);
    ASSERT_GE(191, stats["ts5"]);
  }

  {
    ASSERT_OK(Prepare(cluster_info));
    const auto& all = descriptors();
    PlacementPolicy policy(all, rng());
    map<string, int> stats;
    for (auto i = 0; i < 1000; ++i) {
      TSDescriptorVector result;
      ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, "a1", "t1", &result));
      ASSERT_EQ(nReplicationFactor, result.size());
      for (const auto& ts : result) {
        const auto& ts_uuid = ts->permanent_uuid();
        ++stats[ts_uuid];
      }
    }
    ASSERT_EQ(kNumServers, stats.size());
    vector<string> tservers_no_load = {"ts0", "ts1", "ts2", "ts3"};
    for (const auto& tserver : tservers_no_load) {
      ASSERT_LE(642, stats[tserver]);
      ASSERT_GE(652, stats[tserver]);
    }
    ASSERT_LE(264, stats["ts4"]);
    ASSERT_GE(274, stats["ts4"]);
    ASSERT_LE(137, stats["ts5"]);
    ASSERT_GE(147, stats["ts5"]);
  }
}

// With RF = 3, 6 tablet servers, and a well-balanced cluster initially, this test case adds
// several range partitions for the existing table and for a new table. All these range partitions
// have the same start key and twice as many replicas for the new table will be added. These
// replicas are expected to be distributed evenly per table.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithSeveralRanges) {
  const int nReplicationFactor = 3;
  const int kNumServers = 6;
  TabletNumByRangeMap ranges_per_table({ {"a1", 250}, {"b1", 250} });
  const vector<LocationInfo> cluster_info = {
      {
          "",
          {
              { "ts0", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts1", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts2", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts3", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts4", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts5", 500, { }, { {"t1", ranges_per_table }, } },
          }
      },
  };

  ASSERT_OK(Prepare(cluster_info));
  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());
  typedef std::unordered_map<string, std::unordered_map<string, int>> replicas_per_range_per_table;
  std::unordered_map<string, replicas_per_range_per_table> stats;
  vector<string> ranges = {"a1", "b1", "c1", "d1", "e1", "f1"};
  vector<std::pair<string, int>> tables = {{"t1", 500}, {"t2", 1000}};
  for (const auto& table : tables) {
    for (const auto& range : ranges) {
      for (auto i = 0; i < table.second; ++i) {
        TSDescriptorVector result;
        ASSERT_OK(
            policy.PlaceTabletReplicas(nReplicationFactor, nullopt, range, table.first, &result));
        ASSERT_EQ(nReplicationFactor, result.size());
        for (const auto& ts : result) {
          const auto& ts_uuid = ts->permanent_uuid();
          ++stats[ts_uuid][table.first][range];
        }
      }
    }
  }

  ASSERT_EQ(kNumServers, stats.size());
  for (const auto& stat : stats) {
    int tserver_replicas = 0;
    // Verifies that two tables exist on each tserver.
    ASSERT_EQ(2, stat.second.size());
    for (const auto& table : stat.second) {
      // Verifies that the six ranges exist for each table on each tserver.
      ASSERT_EQ(6, table.second.size());
      for (const auto& ranges : table.second) {
        // Since we placed twice as many replicas for table "t2", we expect twice as many replicas.
        if (table.first == "t2") {
          ASSERT_EQ(500, ranges.second);
        } else {
          ASSERT_EQ(250, ranges.second);
        }
        tserver_replicas += ranges.second;
      }
    }
    // Verifies that 4500 replicas are placed on each tserver.
    ASSERT_EQ(4500, tserver_replicas);
  }
}

// With RF = 3, 6 tablet servers, and an imbalanced cluster by tables initially, this test case adds
// several range partitions for the existing table and for a new table.
TEST_F(PlacementPolicyTest, PlaceTabletReplicasWithImbalanceByTable) {
  const int nReplicationFactor = 3;
  const int kNumServers = 6;
  TabletNumByRangeMap ranges_per_table({ {"a1", 250}, {"b1", 250} });
  const vector<LocationInfo> cluster_info = {
      {
          "",
          {
              { "ts0", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts1", 500, { }, { {"t2", ranges_per_table }, } },
              { "ts2", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts3", 500, { }, { {"t2", ranges_per_table }, } },
              { "ts4", 500, { }, { {"t1", ranges_per_table }, } },
              { "ts5", 500, { }, { {"t2", ranges_per_table }, } },
          }
      },
  };

  ASSERT_OK(Prepare(cluster_info));
  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());
  typedef std::unordered_map<string, std::unordered_map<string, int>> replicas_per_range_per_table;
  std::unordered_map<string, replicas_per_range_per_table> stats;
  vector<string> ranges = {"a1", "b1", "c1", "d1", "e1", "f1"};
  vector<string> tables = {"t1", "t2"};
  for (const auto& table : tables) {
    for (const auto& range : ranges) {
      for (auto i = 0; i < 500; ++i) {
        TSDescriptorVector result;
        ASSERT_OK(policy.PlaceTabletReplicas(nReplicationFactor, nullopt, range, table, &result));
        ASSERT_EQ(nReplicationFactor, result.size());
        for (const auto& ts : result) {
          const auto& ts_uuid = ts->permanent_uuid();
          ++stats[ts_uuid][table][range];
        }
      }
    }
  }

  ASSERT_EQ(kNumServers, stats.size());
  vector<string> balanced_ranges = {"c1", "d1", "e1", "f1"};
  vector<string> tservers1 = {"ts0", "ts2", "ts4"};
  for (const auto& stat : stats) {
    int tserver_replicas = 0;
    // Verifies that two tables exist on each tserver.
    ASSERT_EQ(2, stat.second.size());
    for (const auto& table : stat.second) {
      // Verifies that the six ranges exist for each table on each tserver.
      ASSERT_EQ(6, table.second.size());
      for (const auto& ranges : table.second) {
        if (std::find(balanced_ranges.begin(),
                      balanced_ranges.end(), ranges.first) != balanced_ranges.end()) {
          ASSERT_LE(245, ranges.second);
          ASSERT_GE(255, ranges.second);
        } else if (std::find(tservers1.begin(),
                           tservers1.end(), stat.first) != tservers1.end()) {
          // Only range of "a1" or "b1" now. Checks which tservers and for table "t1" or "t2".
          if (table.first == "t1") {
            ASSERT_LE(120, ranges.second);
            ASSERT_GE(130, ranges.second);
          } else {
            ASSERT_LE(370, ranges.second);
            ASSERT_GE(380, ranges.second);
          }
        } else {
          // Ranges "a1" or "b1" for tservers "ts1", "ts3", "ts5". Checks for table "t1" or "t2".
          if (table.first == "t1") {
            ASSERT_LE(370, ranges.second);
            ASSERT_GE(380, ranges.second);
          } else {
            ASSERT_LE(120, ranges.second);
            ASSERT_GE(130, ranges.second);
          }
        }
        tserver_replicas += ranges.second;
      }
    }
    // Verifies that around 3000 replicas are placed on each tserver.
    ASSERT_LE(2990, tserver_replicas);
    ASSERT_GE(3110, tserver_replicas);
  }
}

} // namespace master
} // namespace kudu
