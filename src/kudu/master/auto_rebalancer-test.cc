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
#include "kudu/master/auto_rebalancer.h"

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(auto_rebalancing_enabled);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(tablet_copy_download_file_inject_latency_ms);
DECLARE_int32(tserver_unresponsive_timeout_ms);
DECLARE_uint32(auto_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_max_moves_per_server);
DECLARE_uint32(auto_rebalancing_wait_for_replica_moves_seconds);

METRIC_DECLARE_gauge_int32(tablet_copy_open_client_sessions);

namespace {

// Some of these tests will set a low Raft timeout to move election traffic
// along more quickly. When built with TSAN, this can lead to timeouts, so ease
// up a bit.
#ifdef THREAD_SANITIZER
  constexpr int kLowRaftTimeout = 300;
#else
  constexpr int kLowRaftTimeout = 100;
#endif

} // anonymous namespace


namespace kudu {
namespace master {

class AutoRebalancerTest : public KuduTest {
  public:

  Status CreateAndStartCluster() {
    FLAGS_auto_rebalancing_interval_seconds = 1; // Shorten for testing.
    FLAGS_auto_rebalancing_wait_for_replica_moves_seconds = 0; // Shorten for testing.
    FLAGS_auto_rebalancing_enabled = true; // Enable for testing.
    cluster_.reset(new InternalMiniCluster(env_, cluster_opts_));
    return cluster_->Start();
  }

  // This function will assign tservers to available locations as
  // evenly as possible.
  void AssignLocationsEvenly(int num_locations) {
    const int num_tservers = cluster_->num_tablet_servers();
    ASSERT_GE(num_tservers, num_locations);

    int master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&master_idx));

    TSDescriptorVector descs;

    ASSERT_EVENTUALLY([&] {
      cluster_->mini_master(master_idx)->master()->ts_manager()->
        GetAllDescriptors(&descs);
      ASSERT_EQ(num_tservers, descs.size());
    });

    // Assign num_locations unique locations to the first num_locations tservers.
    for (int i = 0; i < num_tservers; ++i) {
      descs[i]->AssignLocationForTesting(Substitute("L$0", i % num_locations));
    }
  }

  // This function expects there to be more tservers than available
  // locations. As many tservers as possible will be assigned unique
  // locations, then any additional tservers will all be assigned
  // to the first location.
  void AssignLocationsWithSkew(int num_locations) {
    const int num_tservers = cluster_->num_tablet_servers();
    ASSERT_GT(num_tservers, num_locations);

    int master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&master_idx));

    TSDescriptorVector descs;

    ASSERT_EVENTUALLY([&] {
      cluster_->mini_master(master_idx)->master()->ts_manager()->
        GetAllDescriptors(&descs);
      ASSERT_EQ(num_tservers, descs.size());
    });

    // Assign num_locations unique locations to the first num_locations tservers.
    for (int i = 0; i < num_locations; ++i) {
      descs[i]->AssignLocationForTesting(Substitute("L$0", i));
    }

    for (int i = num_locations; i < num_tservers; ++i) {
      descs[i]->AssignLocationForTesting(Substitute("L$0", 0));
    }
  }

  // Make sure the leader master has begun the auto-rebalancing thread.
  void CheckAutoRebalancerStarted() {
    ASSERT_EVENTUALLY([&] {
      int leader_idx;
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
      ASSERT_LT(0, NumLoopIterations(leader_idx));
    });
  }

  // Make sure the auto-rebalancing loop has iterated a few times,
  // and no moves were scheduled.
  void CheckNoMovesScheduled() {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    auto initial_loop_iterations = NumLoopIterations(leader_idx);

    ASSERT_EVENTUALLY([&] {
      ASSERT_LT(initial_loop_iterations + 3, NumLoopIterations(leader_idx));
      ASSERT_EQ(0, NumMovesScheduled(leader_idx));
    });
  }

  void CheckSomeMovesScheduled() {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    auto initial_loop_iterations = NumLoopIterations(leader_idx);

    ASSERT_EVENTUALLY([&] {
      ASSERT_LT(initial_loop_iterations, NumLoopIterations(leader_idx));
      ASSERT_LT(0, NumMovesScheduled(leader_idx));
    });
  }

  int NumLoopIterations(int master_idx) {
    DCHECK(cluster_ != nullptr);
    return cluster_->mini_master(master_idx)->master()->catalog_manager()->
        auto_rebalancer()->number_of_loop_iterations_for_test_;
  }

  int NumMovesScheduled(int master_idx) {
    DCHECK(cluster_ != nullptr);
    return cluster_->mini_master(master_idx)->master()->catalog_manager()->
        auto_rebalancer()->moves_scheduled_this_round_for_test_;
  }

  void SetupWorkLoad(int num_tablets, int num_replicas) {
    workload_.reset(new TestWorkload(cluster_.get()));
    workload_->set_num_tablets(num_tablets);
    workload_->set_num_replicas(num_replicas);
    workload_->Setup();
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  protected:
    unique_ptr<InternalMiniCluster> cluster_;
    InternalMiniClusterOptions cluster_opts_;
    unique_ptr<TestWorkload> workload_;
  };

// Make sure that only the leader master is doing auto-rebalancing.
TEST_F(AutoRebalancerTest, OnlyLeaderDoesAutoRebalancing) {
  const int kNumMasters = 3;
  const int kNumTservers = 3;
  const int kNumTablets = 4;
  cluster_opts_.num_masters = kNumMasters;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/1);

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  for (int i = 0; i < kNumMasters; i++) {
    if (i == leader_idx) {
      ASSERT_EVENTUALLY([&] {
        ASSERT_LT(0, NumLoopIterations(i));
      });
    } else {
      ASSERT_EVENTUALLY([&] {
        ASSERT_EQ(0, NumMovesScheduled(i));
      });
    }
  }
}

// If the leader master goes down, the next elected master should perform
// auto-rebalancing.
TEST_F(AutoRebalancerTest, NextLeaderResumesAutoRebalancing) {
  const int kNumMasters = 3;
  const int kNumTservers = 3;
  const int kNumTablets = 3;
  cluster_opts_.num_masters = kNumMasters;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/1);

  // Verify that non-leaders are not performing rebalancing,
  // then take down the leader master.
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  for (int i = 0; i < kNumMasters; i++) {
    if (i != leader_idx) {
      ASSERT_EQ(0, NumLoopIterations(i));
    }
  }
  cluster_->mini_master(leader_idx)->Shutdown();

  // Let another master become leader and resume auto-rebalancing.
  // Number of auto-rebalancing iterations should increase again.
  int new_leader_idx;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&new_leader_idx));
    ASSERT_NE(leader_idx, new_leader_idx);
    auto iterations = NumLoopIterations(new_leader_idx);
    ASSERT_LT(0, iterations);
  });
}

// Create a cluster that is initially balanced.
// Bring up another tserver, and verify that moves are scheduled,
// since the cluster is no longer balanced.
TEST_F(AutoRebalancerTest, MovesScheduledIfAddTserver) {
  const int kNumTservers = 3;
  const int kNumTablets = 2;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  NO_FATALS(CheckNoMovesScheduled());

  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(CheckSomeMovesScheduled());
}

// A cluster with no tservers is balanced.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfNoTservers) {
  const int kNumTservers = 0;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());
  NO_FATALS(CheckNoMovesScheduled());
}

// A cluster with no tablets is balanced.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfNoTablets) {
  const int kNumTservers = 3;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());
  NO_FATALS(CheckNoMovesScheduled());
}

// Assign each tserver to its own location.
// A cluster with location load skew = 1 is balanced.
// In this test, 3 tservers should have load = 1, 1 tserver should have load = 0.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfLocationLoadSkewedByOne) {
  const int kNumTservers = 4;
  const int kNumTablets = 1;
  const int kNumLocations = kNumTservers;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  NO_FATALS(AssignLocationsEvenly(kNumLocations));

  const auto timeout = MonoDelta::FromSeconds(30);
  vector<master::ListTabletServersResponsePB_Entry> tservers;
  ASSERT_OK(itest::ListTabletServers(cluster_->master_proxy(),
                                     timeout,
                                     &tservers));
  set<string> locations;
  for (const auto& tserver : tservers) {
    const auto& ts_location = tserver.location();
    locations.insert(ts_location);
    ASSERT_FALSE(ts_location.empty());
  }
  ASSERT_EQ(kNumTservers, locations.size());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  NO_FATALS(CheckNoMovesScheduled());
}

// Assign tservers to one of two locations.
// If placement policy can never be satisfied, the auto-rebalancer should
// not attempt to schedule any replica movements.
// In this test, the tablet should have the majority (2/3) of its replicas
// in the same location. This violation cannot be fixed.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfCannotFixPlacementPolicy) {
  const int kNumTservers = 3;
  const int kNumTablets = 1;
  const int kNumLocations = 2;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  NO_FATALS(AssignLocationsWithSkew(kNumLocations));

  const auto timeout = MonoDelta::FromSeconds(30);
  vector<master::ListTabletServersResponsePB_Entry> tservers;
  ASSERT_OK(itest::ListTabletServers(cluster_->master_proxy(),
                                     timeout,
                                     &tservers));
  set<string> locations;
  for (const auto& tserver : tservers) {
    const auto& ts_location = tserver.location();
    locations.insert(ts_location);
    ASSERT_FALSE(ts_location.empty());
  }
  ASSERT_EQ(kNumLocations, locations.size());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  NO_FATALS(CheckNoMovesScheduled());
}

// Verify that each server's number of tablet copying sessions
// doesn't exceed the value in the flag auto_rebalancing_max_moves_per_server.
TEST_F(AutoRebalancerTest, TestMaxMovesPerServer) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Make tablet replica copying slow, to make it easier to spot violations.
  FLAGS_tablet_copy_download_file_inject_latency_ms = 2000;

  const int kNumOrigTservers = 3;
  const int kNumAdditionalTservers = 3;
  const int kNumTablets = 12;

  cluster_opts_.num_tablet_servers = kNumOrigTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);
  workload_->Start();
  ASSERT_EVENTUALLY([&]() {
      ASSERT_LE(1000, workload_->rows_inserted());
  });
  workload_->StopAndJoin();

  // Bring up additional tservers, to trigger tablet replica copying.
  for (int i = 0; i < kNumAdditionalTservers; ++i) {
    ASSERT_OK(cluster_->AddTabletServer());
  }
  NO_FATALS(CheckSomeMovesScheduled());

  // Check metric 'tablet_copy_open_client_sessions', which must be
  // less than the auto_rebalancing_max_moves_per_server, for each tserver.
  for (int i = 0; i < kNumOrigTservers + kNumAdditionalTservers; ++i) {
    auto metric = cluster_->mini_tablet_server(i)->server()->metric_entity();
    int open_client_sessions = METRIC_tablet_copy_open_client_sessions.
        Instantiate(metric, 0)->value();

    // Check that the total number of sessions does not excceed the flag.
    ASSERT_GE(FLAGS_auto_rebalancing_max_moves_per_server, open_client_sessions);
  }
}

// Attempt rebalancing a cluster with unstable Raft configurations.
TEST_F(AutoRebalancerTest, AutoRebalancingUnstableCluster) {
  // Set a low Raft heartbeat.
  FLAGS_raft_heartbeat_interval_ms = kLowRaftTimeout;

  const int kNumTservers = 3;
  const int kNumTablets = 3;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  // Inject latency to make tablets' Raft configurations unstable due to
  // frequent leader re-elections.
  FLAGS_consensus_inject_latency_ms_in_notifications = kLowRaftTimeout;

  // Bring up an additional tserver, to trigger rebalancing.
  // Moves should be scheduled, even if they cannot be completed because of
  // the frequent leadership changes.
  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(CheckSomeMovesScheduled());
}

// A cluster that cannot become healthy and meet the replication factor
// will not attempt rebalancing.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfCannotMeetReplicationFactor) {
  const int kNumTservers = 3;
  const int kNumTablets = 1;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  // Take down a tserver, to make it impossible to meet RF=3 with 2 tservers.
  NO_FATALS(cluster_->mini_tablet_server(0)->Shutdown());
  NO_FATALS(CheckNoMovesScheduled());
}

// Verify that movement of replicas to meet the replication factor
// does not count towards rebalancing, i.e. the auto-rebalancer will
// not consider recovering replicas as candidates for replica movement.
TEST_F(AutoRebalancerTest, NoRebalancingIfReplicasRecovering) {
  // Set a low timeout for an unresponsive tserver to be presumed dead by the master.
  FLAGS_tserver_unresponsive_timeout_ms = 1000;

  // Shorten the interval for recovery re-replication to begin.
  FLAGS_follower_unavailable_considered_failed_sec = 1;

  const int kNumTservers = 3;
  const int kNumTablets = 4;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  // Add another tserver; immediately take down one of the original tservers.
  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(cluster_->mini_tablet_server(0)->Shutdown());
  // Wait for recovery re-replication to start.
  SleepFor(MonoDelta::FromSeconds(
      FLAGS_follower_unavailable_considered_failed_sec));

  // No rebalancing should occur while there are under-replicated replicas.
  NO_FATALS(CheckNoMovesScheduled());
}

// Make sure the auto-rebalancer reports the failure of scheduled replica movements,
// in the case that tablet server failure is not yet accounted for by the TSManager.
TEST_F(AutoRebalancerTest, TestHandlingFailedTservers) {
  // Set a high timeout for an unresponsive tserver to be presumed dead,
  // so the TSManager believes it is still available.
  FLAGS_tserver_unresponsive_timeout_ms = 120 * 1000;

  const int kNumTservers = 3;
  const int kNumTablets = 4;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  SetupWorkLoad(kNumTablets, /*num_replicas*/3);

  // Take down all the original tservers.
  for (int i = 0; i < kNumTservers; ++i) {
    NO_FATALS(cluster_->mini_tablet_server(i)->Shutdown());
  }

  // Capture the glog output to ensure failed replica movements occur
  // and the warnings are logged.
  StringVectorSink pre_capture_logs;
  {
    ScopedRegisterSink reg(&pre_capture_logs);
    // Bring up a new tserver.
    ASSERT_OK(cluster_->AddTabletServer());

    // The TSManager should still believe the original tservers are available,
    // so the auto-rebalancer should attempt to schedule replica moves from those
    // tservers to the new one.
    NO_FATALS(CheckSomeMovesScheduled());
  }
  ASSERT_STRINGS_ANY_MATCH(pre_capture_logs.logged_msgs(),
                           "scheduled replica move failed to complete: Network error");

  // Wait for the TSManager to realize that the original tservers are unavailable.
  FLAGS_tserver_unresponsive_timeout_ms = 5 * 1000;
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_tserver_unresponsive_timeout_ms));

  // Bring back the tservers.
  for (int i = 0; i < kNumTservers; ++i) {
    NO_FATALS(cluster_->mini_tablet_server(i)->Restart());
  }

  // Make sure the auto-rebalancer thread schedules replica moves because of the
  // re-available tservers.
  StringVectorSink post_capture_logs;
  {
    ScopedRegisterSink reg(&post_capture_logs);
    NO_FATALS(CheckSomeMovesScheduled());
  }

  // These replica moves should not fail because the tservers are unavailable.
  for (const auto& str : post_capture_logs.logged_msgs()) {
    ASSERT_STR_NOT_CONTAINS(str, "scheduled replica move failed to complete: Network error");
  }
}
} // namespace master
} // namespace kudu
