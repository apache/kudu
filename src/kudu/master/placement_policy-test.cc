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

#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::make_shared;
using std::map;
using std::multiset;
using std::set;
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
    const string id;          // TS identifier
    const size_t replica_num; // number of tablet replicas hosted by TS
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
    // O(n^2) is not the best way to do this, but it's OK the test purposes.
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
        shared_ptr<TSDescriptor> tsd(new TSDescriptor(ts.id));
        tsd->set_num_live_replicas(ts.replica_num);
        tsd->location_.emplace(location_info.id);
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
    { "", { { "ts0", 0 }, { "ts1", 10 }, { "ts2", 1 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  {
    TSDescriptorVector existing(all.begin(), all.end());
    existing.pop_back();
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, &extra_ts));
    ASSERT_TRUE(extra_ts);
    ASSERT_EQ("ts2", extra_ts->permanent_uuid());
  }

  {
    TSDescriptorVector existing(all.begin(), all.end());
    existing.pop_back();
    existing.pop_back();
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, &extra_ts));
    ASSERT_TRUE(extra_ts);
    ASSERT_EQ("ts2", extra_ts->permanent_uuid());
  }

  {
    TSDescriptorVector existing(all.begin(), all.end());
    shared_ptr<TSDescriptor> extra_ts;
    const auto s = policy.PlaceExtraTabletReplica(existing, &extra_ts);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_FALSE(extra_ts);
    ASSERT_STR_CONTAINS(s.ToString(),
                        "could not select location for extra replica");
  }
}

TEST_F(PlacementPolicyTest, PlaceTabletReplicasNoLoc) {
  // 'No location case': expecting backward-compatible behavior with the
  // legacy (i.e. non-location-aware) logic.
  const vector<LocationInfo> cluster_info = {
    { "", { { "ts0", 0 }, { "ts1", 10 }, { "ts2", 1 }, } },
  };
  ASSERT_OK(Prepare(cluster_info));

  const auto& all = descriptors();
  PlacementPolicy policy(all, rng());

  // Ask just for a single replica.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(1, &result));
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
    ASSERT_OK(policy.PlaceTabletReplicas(3, &result));
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
    auto s = policy.PlaceTabletReplicas(4, &result);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "could not find next location after placing "
                        "3 out of 4 tablet replicas");
    ASSERT_TRUE(result.empty());
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
    ASSERT_OK(policy.PlaceTabletReplicas(3, &result));
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
    ASSERT_OK(policy.PlaceTabletReplicas(5, &result));
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
    auto s = policy.PlaceTabletReplicas(8, &result);
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
    ASSERT_OK(policy.PlaceTabletReplicas(3, &result));
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
    ASSERT_OK(policy.PlaceTabletReplicas(5, &result));
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
    auto s = policy.PlaceTabletReplicas(6, &result);
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
    ASSERT_OK(policy.PlaceTabletReplicas(3, &result));
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
    ASSERT_OK(policy.PlaceTabletReplicas(5, &result));
    ASSERT_EQ(5, result.size());
    TSDescriptorsMap m;
    ASSERT_OK(TSDescriptorVectorToMap(result, &m));
    ASSERT_EQ(1, m.count("A_ts0"));
    ASSERT_EQ(1, m.count("B_ts0"));
    ASSERT_EQ(1, m.count("C_ts0"));
    ASSERT_EQ(0, m.count("D_ts0"));
    ASSERT_EQ(0, m.count("E_ts0"));
  }

  // Place as many talbet replicas as possible given the number of tablet
  // servers in the cluster.
  {
    TSDescriptorVector result;
    ASSERT_OK(policy.PlaceTabletReplicas(7, &result));
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
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // Within location a replica is placed by the 'power of 2' algorithm.
    ASSERT_TRUE(extra_ts->permanent_uuid() == "C_ts0" ||
                extra_ts->permanent_uuid() == "C_ts1");

  }

  {
    // Make sure the replica placement algorithm avoid placing an extra replica
    // into the least loaded location if the no-majority-in-single-location
    // constraint would be violated.
    const auto existing = GetDescriptors({ "A_ts0", "A_ts1", "C_ts1", "C_ts2", });
    shared_ptr<TSDescriptor> extra_ts;
    ASSERT_OK(policy.PlaceExtraTabletReplica(existing, &extra_ts));
    ASSERT_TRUE(extra_ts);
    // Within location a replica is placed by the 'power of 2' algorithm.
    ASSERT_TRUE(extra_ts->permanent_uuid() == "B_ts0" ||
                extra_ts->permanent_uuid() == "B_ts1");
  }
}

// Test for randomness while selecting among locations of the same load.
TEST_F(PlacementPolicyTest, SelectLocationTest) {
  const vector<LocationInfo> cluster_info = {
    { "A", { { "A_ts0", 1 }, { "A_ts1", 1 }, { "A_ts2", 1 }, } },
    { "B", { { "B_ts0", 1 }, { "B_ts1", 1 }, { "B_ts2", 1 }, } },
    { "C", { { "C_ts0", 1 }, { "C_ts1", 1 }, { "C_ts2", 1 }, } },
  };
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

} // namespace master
} // namespace kudu
