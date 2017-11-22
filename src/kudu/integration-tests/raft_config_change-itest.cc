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

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::consensus::COMMITTED_OPID;
using kudu::consensus::RaftPeerAttrsPB;
using kudu::consensus::RaftPeerPB;
using kudu::itest::TServerDetails;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

class RaftConfigChangeITest : public ExternalMiniClusterITestBase {
};

// Regression test for KUDU-2147. In this test we cause an initial leader to
// heartbeat to a master with a new configuration change immediately after it
// has lost its leadership, reporting a new configuration with no leader. The
// master should update its record of which replica is the leader after a new
// leader is elected in that term.
//
// Steps followed in this test:
//
// 1. Inject latency into TS heartbeats to the master and reduce Raft leader
//    heartbeat intervals to followers to be able to carefully control the
//    sequence of events in this test.
// 2. Evict a follower from the config.
// 3. Immediately start an election on the remaining follower in the config.
//    We know that this follower should win the election because in order to
//    commit a removal from 3 voters, the removal op must be replicated to both
//    of the two remaining voters in the config.
// 4. The master will get the config change heartbeat from the initial leader,
//    which will indicate a new term and no current leader.
// 5. The master will then get a heartbeat from the new leader to report that
//    it's now the leader of the config.
// 6. Once the master knows who the new leader is, the master will instruct the
//    new leader to add a new replica to its config to bring it back up to 3
//    voters. That new replica will be the follower that we evicted in step 2.
// 7. If that succeeds then the leader will replicate the eviction, the
//    leader's own no-op, and the adding-back-in of the evicted replica to that
//    evicted replica's log.
// 8. Once that process completes, all 3 replicas will have identical logs,
//    which is what we wait for at the end of the test.
TEST_F(RaftConfigChangeITest, TestKudu2147) {
  if (!AllowSlowTests()) {
    // This test injects seconds of latency so can take a while to converge.
    LOG(WARNING) << "Skipping test in fast-test mode.";
    return;
  }
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  // Slow down leader heartbeats so that in the explicit election below, the
  // second leader does not immediately heartbeat to the initial leader. If
  // that occurs, the initial leader will not report to the master that there
  // is currently no leader.
  NO_FATALS(StartCluster({"--raft_heartbeat_interval_ms=3000"}));

  // Create a new table.
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.batches_completed(), 10);
  });
  workload.StopAndJoin();

  // The table should have replicas on three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(itest::GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                                     kTimeout, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size()); // Only 1 tablet.
  ASSERT_EQ(3, table_locations.tablet_locations().begin()->replicas_size()); // 3 replicas.
  string tablet_id = table_locations.tablet_locations().begin()->tablet_id();

  // Wait for all 3 replicas to converge before we start the test.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));

  // Retry as needed to counter normal leader election activity.
  ASSERT_EVENTUALLY([&] {
    // Find initial leader.
    TServerDetails* leader;
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader));
    ASSERT_OK(WaitForOpFromCurrentTerm(leader, tablet_id, COMMITTED_OPID, kTimeout));

    vector<TServerDetails*> followers;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      // Dynamically set the latency injection flag to induce TS -> master
      // heartbeat delays. The leader delays its master heartbeats by 2 sec
      // each time; followers delay their master heartbeats by even longer.
      int ms_delay = cluster_->tablet_server(i)->uuid() == leader->uuid() ? 2000 : 5000;
      ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i),
                "heartbeat_inject_latency_before_heartbeat_ms",
                Substitute("$0", ms_delay)));
      // Keep track of the followers.
      if (cluster_->tablet_server(i)->uuid() != leader->uuid()) {
        followers.push_back(ts_map_[cluster_->tablet_server(i)->uuid()]);
      }
    }
    ASSERT_EQ(2, followers.size());

    // Now that heartbeat injection is enabled, evict one follower and trigger
    // an election on another follower immediately thereafter.
    ASSERT_OK(itest::RemoveServer(leader, tablet_id, followers[0], kTimeout));

    // Immediately start an election on the remaining follower. This will cause
    // the initial leader's term to rev and it will have to step down. When it
    // sends a tablet report to the master with the new configuration excluding
    // the removed tablet it will report an unknown leader in the new term.
    ASSERT_OK(itest::StartElection(followers[1], tablet_id, kTimeout));
  });

  // Wait until the master re-adds the evicted replica and it is back up and
  // running. If the master hit KUDU-2147, this would fail because the master
  // would be unable to add the removed server back, and that replica would be
  // missing the config change op that removed it from the config.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));
}

// Test automatic promotion of a non-voter replica in a 3-4-3 re-replication
// (KUDU-1097) paradigm.
TEST_F(RaftConfigChangeITest, TestNonVoterPromotion) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  // Enable 3-4-3 re-replication.
  NO_FATALS(StartCluster({"--raft_prepare_replacement_before_eviction=true"},
                         {"--raft_prepare_replacement_before_eviction=true",
                          "--catalog_manager_evict_excess_replicas=false"},
                         /*num_tablet_servers=*/ 4));
  TestWorkload workload(cluster_.get());
  workload.Setup();

  // The table should initially have replicas on three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(itest::GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                                     kTimeout, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size()); // Only 1 tablet.
  ASSERT_EQ(3, table_locations.tablet_locations().begin()->replicas_size()); // 3 replicas.
  string tablet_id = table_locations.tablet_locations().begin()->tablet_id();

  // Find the TS that does not have a replica.
  unordered_set<string> initial_replicas;
  for (const auto& replica : table_locations.tablet_locations().begin()->replicas()) {
    initial_replicas.insert(replica.ts_info().permanent_uuid());
  }
  TServerDetails* new_replica = nullptr;
  for (const auto& entry : ts_map_) {
    if (!ContainsKey(initial_replicas, entry.first)) {
      new_replica = entry.second;
      break;
    }
  }
  ASSERT_NE(nullptr, new_replica);

  TServerDetails* leader_replica = nullptr;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_replica));
    ASSERT_NE(new_replica, leader_replica);

    // Add the 4th replica as a NON_VOTER with promote=true.
    RaftPeerAttrsPB attrs;
    attrs.set_promote(true);
    ASSERT_OK(AddServer(leader_replica, tablet_id, new_replica,
                        RaftPeerPB::NON_VOTER, kTimeout, attrs));
  });

  // Wait for there to be 4 voters in the config.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(/*config_size=*/ 4,
                                                leader_replica,
                                                tablet_id,
                                                kTimeout));

  NO_FATALS(cluster_->AssertNoCrashes());
}

} // namespace kudu
