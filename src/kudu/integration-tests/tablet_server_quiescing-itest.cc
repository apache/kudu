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

#include <atomic>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(tablet_copy_download_file_inject_latency_ms);
DECLARE_int32(tablet_copy_transfer_chunk_size_bytes);
DECLARE_int32(scanner_default_batch_size_bytes);
DECLARE_int32(scanner_ttl_ms);
DECLARE_int32(raft_heartbeat_interval_ms);

METRIC_DECLARE_histogram(handler_latency_kudu_consensus_ConsensusService_RunLeaderElection);

using kudu::client::KuduClient;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::tools::RunActionPrependStdoutStderr;
using kudu::tools::RunKuduTool;
using kudu::tserver::MiniTabletServer;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

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
namespace itest {

class TServerQuiescingITest : public MiniClusterITestBase {
 public:
  // Creates a table with 'num_tablets' partitions and as many replicas as
  // there are tablet servers, waiting for the tablets to show up on each
  // server before returning. Populates 'tablet_ids' with the tablet IDs.
  void CreateWorkloadTable(int num_tablets, vector<string>* tablet_ids = nullptr) {
    TestWorkload workload(cluster_.get());
    workload.set_num_replicas(cluster_->num_tablet_servers());
    workload.set_num_tablets(num_tablets);
    workload.Setup();
    ASSERT_EVENTUALLY([&] {
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        auto* ts = cluster_->mini_tablet_server(i);
        ASSERT_EQ(num_tablets, ts->server()->tablet_manager()->GetNumLiveTablets());
      }
      if (tablet_ids) {
        *tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
      }
    });
  }

  // Creates a read-write workload that doesn't use a fault-tolerant scanner.
  // Not using a FT scanner means:
  // - Remote errors when writing won't automatically be retried, so we must
  //   permit these if we want to run the workload while restarting a tserver.
  //   We may get a remote error if the tserver is reachable but shutting down
  //   (this isn't the case in production where we just kill the process).
  // - The number of rows returned may not be consistent with what we've
  //   already written.
  unique_ptr<TestWorkload> CreateFaultIntolerantRWWorkload() const {
    unique_ptr<TestWorkload> rw_workload(new TestWorkload(cluster_.get()));
    rw_workload->set_scanner_fault_tolerant(false);
    rw_workload->set_num_replicas(cluster_->num_tablet_servers());
    rw_workload->set_num_read_threads(3);
    rw_workload->set_num_write_threads(3);
    rw_workload->set_verify_num_rows(false);
    // NOTE: this doesn't affect scans at all.
    rw_workload->set_remote_error_allowed(true);
    return rw_workload;
  }
};

// Test that a quiescing server won't trigger an election by natural means (i.e.
// by detecting a Raft timeout).
TEST_F(TServerQuiescingITest, TestQuiescingServerDoesntTriggerElections) {
  const int kNumReplicas = 3;
  const int kNumTablets = 10;
  // This test will change leaders frequently, so set a low Raft heartbeat.
  FLAGS_raft_heartbeat_interval_ms = kLowRaftTimeout;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas.
  vector<string> tablet_ids;
  NO_FATALS(CreateWorkloadTable(kNumTablets, &tablet_ids));

  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  // Wait for all of our relicas to have leaders.
  for (const auto& tablet_id : tablet_ids) {
    TServerDetails* leader_details;
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
    });
    LOG(INFO) << Substitute("Tablet $0 has leader $1", tablet_id, leader_details->uuid());
  }

  auto* ts = cluster_->mini_tablet_server(0);
  LOG(INFO) << Substitute("Quiescing ts $0", ts->uuid());
  *ts->server()->mutable_quiescing() = true;

  // Soon enough, elections will occur, and our quiescing server will cease to
  // be leader.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, ts->server()->num_raft_leaders()->value());
  });

  // Cause a bunch of elections.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms;

  // When we stop quiescing the server, we should eventually see some
  // leadership return to the server.
  *ts->server()->mutable_quiescing() = false;
  ASSERT_EVENTUALLY([&] {
    ASSERT_LT(0, ts->server()->num_raft_leaders()->value());
  });
}

// Test that after quiescing a tablet's leader, leadership will be transferred
// elsewhere.
TEST_F(TServerQuiescingITest, TestQuiescingLeaderTransfersLeadership) {
  const int kNumReplicas = 3;
  NO_FATALS(StartCluster(kNumReplicas));
  vector<string> tablet_ids;
  NO_FATALS(CreateWorkloadTable(/*num_tablets*/1, &tablet_ids));
  string tablet_id = tablet_ids[0];

  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  TServerDetails* leader_details;
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));

  // Start quiescing the leader.
  const auto& orig_leader_uuid = leader_details->uuid();
  auto* leader_ts = cluster_->mini_tablet_server_by_uuid(orig_leader_uuid);
  *leader_ts->server()->mutable_quiescing() = true;

  // The leader tserver will relinquish leadership soon enough.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
    ASSERT_NE(orig_leader_uuid, leader_details->uuid());
  });
}

// Test that even if a majority of replicas are quiescing, a tablet is still
// able to elect a leader.
TEST_F(TServerQuiescingITest, TestMajorityQuiescingElectsLeader) {
  const int kNumReplicas = 3;
  FLAGS_raft_heartbeat_interval_ms = kLowRaftTimeout;
  NO_FATALS(StartCluster(kNumReplicas));
  vector<string> tablet_ids;
  NO_FATALS(CreateWorkloadTable(/*num_tablets*/1, &tablet_ids));
  string tablet_id = tablet_ids[0];

  // Start quiescing all but the first tserver.
  for (int i = 1; i < kNumReplicas; i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = true;
  }

  // Eventually the first tserver will be elected leader.
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  TServerDetails* leader_details;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
    ASSERT_EQ(leader_details->uuid(), cluster_->mini_tablet_server(0)->uuid());
  });
}

// Test that when we're quiescing a tserver, we don't accept new scan requests,
// Even with non-FT scanners, if we restart a quiescing tserver that has
// completed its scans, on-going read workloads are not affected.
TEST_F(TServerQuiescingITest, TestDoesntAllowNewScans) {
  const int kNumReplicas = 3;
  // Set a tiny batch size to encourage many batches for a single scan. This
  // will emulate longer-running scans.
  FLAGS_scanner_default_batch_size_bytes = 1;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas and start a workload without fault
  // tolerant scans.
  auto rw_workload = CreateFaultIntolerantRWWorkload();
  rw_workload->Setup();
  rw_workload->Start();

  // Wait for some scans to begin.
  auto* ts = cluster_->mini_tablet_server(0);
  ASSERT_EVENTUALLY([&] {
    ASSERT_LT(0, ts->server()->scanner_manager()->CountActiveScanners());
  });

  // Mark a tablet server as quiescing. It should eventually stop serving scans.
  *ts->server()->mutable_quiescing() = true;
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, ts->server()->scanner_manager()->CountActiveScanners());
  });

  // Stopping the quiesced tablet server shouldn't affect the ongoing read
  // workload, since there are no scans on that server.
  ts->Shutdown();
  NO_FATALS(rw_workload->StopAndJoin());
}

// Test that when we're doing a leader-only non-FT scan and we quiesce the
// leaders, we eventually stop seeing scans on that server.
TEST_F(TServerQuiescingITest, TestDoesntAllowNewScansLeadersOnly) {
  const int kNumReplicas = 3;
  // This test will change leaders frequently, so set a low Raft heartbeat.
  FLAGS_raft_heartbeat_interval_ms = kLowRaftTimeout;
  // Set a tiny batch size to encourage many batches for a single scan. This
  // will emulate long-running scans.
  FLAGS_scanner_default_batch_size_bytes = 1;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas.
  auto rw_workload = CreateFaultIntolerantRWWorkload();
  rw_workload->set_scanner_selection(client::KuduClient::LEADER_ONLY);
  rw_workload->Setup();
  rw_workload->Start();

  // Wait for the scans to begin.
  MiniTabletServer* ts = nullptr;
  ASSERT_EVENTUALLY([&] {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* tserver = cluster_->mini_tablet_server(i);
      if (tserver->server()->scanner_manager()->CountActiveScanners() > 0) {
        ts = tserver;
        break;
      }
    }
    ASSERT_NE(nullptr, ts);
  });

  // Mark one of the tablet servers with scans as quiescing. It should
  // eventually stop serving scans.
  *ts->server()->mutable_quiescing() = true;
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, ts->server()->scanner_manager()->CountActiveScanners());
  });
  ts->Shutdown();

  // Stopping the quiesced tablet server shouldn't affect the ongoing read workload.
  NO_FATALS(rw_workload->StopAndJoin());
}

// Test that when all followers are behind (e.g. because the others are down),
// the leader, even while quiescing, will remain leader.
TEST_F(TServerQuiescingITest, TestQuiesceLeaderWhileFollowersCatchingUp) {
  const int kNumReplicas = 3;
  FLAGS_raft_heartbeat_interval_ms = kLowRaftTimeout;
  NO_FATALS(StartCluster(kNumReplicas));
  auto rw_workload = CreateFaultIntolerantRWWorkload();
  rw_workload->set_num_tablets(1);
  rw_workload->Setup();
  rw_workload->Start();
  while (rw_workload->rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  TServerDetails* leader_details;
  const auto kTimeout = MonoDelta::FromSeconds(10);
  const string tablet_id = cluster_->mini_tablet_server(0)->ListTablets()[0];
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
  const string leader_uuid = leader_details->uuid();

  // Slow down tablet copies so our leader will be catching up followers long
  // enough for us to observe.
  FLAGS_tablet_copy_transfer_chunk_size_bytes = 512;
  FLAGS_tablet_copy_download_file_inject_latency_ms = 500;

  // Stop our writes and delete the replicas on the follower servers, setting
  // them up for tablet copies.
  NO_FATALS(rw_workload->StopAndJoin());
  for (const auto& ts_and_details : ts_map_) {
    const auto& ts_uuid = ts_and_details.first;
    if (ts_uuid != leader_uuid) {
      const auto* ts_details = ts_and_details.second;
      ASSERT_OK(DeleteTablet(ts_details, tablet_id,
                             tablet::TabletDataState::TABLET_DATA_TOMBSTONED,
                             kTimeout));
      ASSERT_EVENTUALLY([&] {
        vector<string> running_tablets;
        ASSERT_OK(ListRunningTabletIds(ts_details, kTimeout, &running_tablets));
        ASSERT_EQ(0, running_tablets.size());
      });
    }
  }
  // Quiesce the leader and wait for a bit. While the leader is catching up
  // replicas, it shouldn't relinquish leadership.
  auto* leader_ts = cluster_->mini_tablet_server_by_uuid(leader_uuid);
  *leader_ts->server()->mutable_quiescing() = true;
  SleepFor(MonoDelta::FromSeconds(3));
  ASSERT_EQ(1, leader_ts->server()->num_raft_leaders()->value());
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
  ASSERT_EQ(leader_uuid, leader_details->uuid());

  // Once we let the copy finish, the leader should relinquish leadership.
  FLAGS_tablet_copy_download_file_inject_latency_ms = 0;
  FLAGS_tablet_copy_transfer_chunk_size_bytes = 4 * 1024 * 1024;
  for (const auto& ts_and_details : ts_map_) {
    ASSERT_EVENTUALLY([&] {
      vector<string> running_tablets;
      ASSERT_OK(ListRunningTabletIds(ts_and_details.second, kTimeout, &running_tablets));
      ASSERT_EQ(1, running_tablets.size());
    });
  }
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, leader_ts->server()->num_raft_leaders()->value());
    TServerDetails* new_leader_details;
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &new_leader_details));
    ASSERT_NE(leader_uuid, new_leader_details->uuid());
  });
}

// Basic test that we see the quiescing state change in the server.
TEST_F(TServerQuiescingITest, TestQuiescingToolBasics) {
  NO_FATALS(StartCluster(1));
  auto* ts = cluster_->mini_tablet_server(0);
  auto rw_workload = CreateFaultIntolerantRWWorkload();
  rw_workload->Setup();
  // NOTE: if this value is too high, this test can become flaky, since the
  // degrees of freedom in the number of active scanners will be high.
  rw_workload->set_num_read_threads(1);
  ASSERT_FALSE(ts->server()->quiescing());
  const auto& master_addr = cluster_->mini_master()->bound_rpc_addr().ToString();
  // First, call the start tool a couple of times.
  for (int i = 0; i < 2; i++) {
    ASSERT_OK(RunActionPrependStdoutStderr(
        Substitute("tserver quiesce start $0", ts->bound_rpc_addr().ToString())));
    ASSERT_TRUE(ts->server()->quiescing());

    // Running the status tool should report what we expect and not change the
    // state.
    string stdout;
    ASSERT_OK(RunKuduTool({ "tserver", "quiesce", "status", ts->bound_rpc_addr().ToString() },
                          &stdout));
    ASSERT_STR_CONTAINS(stdout,
        " Quiescing | Tablet Leaders | Active Scanners\n"
        "-----------+----------------+-----------------\n"
        " true      |       1        |       0");
    ASSERT_TRUE(ts->server()->quiescing());

    // Same with ksck.
    ASSERT_OK(RunKuduTool({ "cluster", "ksck", master_addr }, &stdout));
    ASSERT_STR_MATCHES(stdout,
        ".* Quiescing | Tablet Leaders | Active Scanners\n"
        ".*-----------+----------------+-----------------\n"
        ".* true      |       1        |      0");
    ASSERT_TRUE(ts->server()->quiescing());
  }
  ASSERT_OK(RunActionPrependStdoutStderr(
      Substitute("tserver quiesce stop $0", ts->bound_rpc_addr().ToString())));
  string stdout;
  ASSERT_OK(RunKuduTool({ "tserver", "quiesce", "status", ts->bound_rpc_addr().ToString() },
                        &stdout));
  ASSERT_STR_CONTAINS(stdout,
      " Quiescing | Tablet Leaders | Active Scanners\n"
      "-----------+----------------+-----------------\n"
      " false     |       1        |       0");
  ASSERT_FALSE(ts->server()->quiescing());

  // When there aren't quiescing tservers, ksck won't report the quiescing
  // status, but it will still report related info...
  ASSERT_OK(RunKuduTool({ "cluster", "ksck", master_addr }, &stdout));
  ASSERT_STR_MATCHES(stdout,
      ".* Tablet Leaders | Active Scanners\n"
      ".*----------------+-----------------\n"
      ".*       1        |      0");
  ASSERT_STR_NOT_CONTAINS(stdout, "Quiescing");
  ASSERT_FALSE(ts->server()->quiescing());

  // ... until the user doesn't want to see that.
  ASSERT_OK(RunKuduTool({ "cluster", "ksck", "--noquiescing_info", master_addr }, &stdout));
  ASSERT_STR_NOT_CONTAINS(stdout, "Quiescing");
  ASSERT_STR_NOT_CONTAINS(stdout, "Tablet Leaders");
  ASSERT_STR_NOT_CONTAINS(stdout, "Active Scanners");
  ASSERT_FALSE(ts->server()->quiescing());


  // Now try getting the status with some scanners.
  // Set a low batch size so we'll be more likely to catch scanners in the act.
  FLAGS_scanner_default_batch_size_bytes = 1;
  rw_workload->Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(RunKuduTool({ "tserver", "quiesce", "status", ts->bound_rpc_addr().ToString() },
                          &stdout));
    ASSERT_STR_CONTAINS(stdout, Substitute(
        " Quiescing | Tablet Leaders | Active Scanners\n"
        "-----------+----------------+-----------------\n"
        " false     |       1        |       $0",
        ts->server()->scanner_manager()->CountActiveScanners()));
    ASSERT_OK(RunKuduTool({ "cluster", "ksck", master_addr }, &stdout));
    ASSERT_STR_MATCHES(stdout, Substitute(
        ".* Tablet Leaders | Active Scanners\n"
        ".*----------------+-----------------\n"
        ".*       1        |      $0",
        ts->server()->scanner_manager()->CountActiveScanners()));
  });
  ASSERT_FALSE(ts->server()->quiescing());
  NO_FATALS(rw_workload->StopAndJoin());

  // Now try starting again but expecting errors.
  Status s = RunActionPrependStdoutStderr(
      Substitute("tserver quiesce start $0 --error_if_not_fully_quiesced",
                 ts->bound_rpc_addr().ToString()));
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "not fully quiesced");
  ASSERT_TRUE(ts->server()->quiescing());
}

// Basic test to ensure the quiescing tooling works as expected.
TEST_F(TServerQuiescingITest, TestQuiesceAndStopTool) {
  const int kNumReplicas = 3;
  // Set a tiny batch size to encourage many batches for a single scan. This
  // will emulate long-running scans.
  FLAGS_scanner_default_batch_size_bytes = 100;
  NO_FATALS(StartCluster(kNumReplicas));
  MiniTabletServer* leader_ts;
  auto rw_workload = CreateFaultIntolerantRWWorkload();
  rw_workload->set_scanner_selection(client::KuduClient::LEADER_ONLY);
  rw_workload->Setup();
  rw_workload->Start();
  while (rw_workload->rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  // Pick a tablet server with a leader.
  TServerDetails* leader_details;
  const auto kTimeout = MonoDelta::FromSeconds(10);
  const string tablet_id = cluster_->mini_tablet_server(0)->ListTablets()[0];
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
  const string leader_uuid = leader_details->uuid();

  // The tablet server should have some leaders, and will eventually serve some scans.
  leader_ts = cluster_->mini_tablet_server_by_uuid(leader_uuid);
  ASSERT_LT(0, leader_ts->server()->num_raft_leaders()->value());
  ASSERT_EVENTUALLY([&] {
    ASSERT_LT(0, leader_ts->server()->scanner_manager()->CountActiveScanners());
  });

  // Now quiesce the server. At first, the tool should fail because there are
  // still leaders on the server, though it should have successfully begun
  // quiescing.
  Status s = RunActionPrependStdoutStderr(
      Substitute("tserver quiesce start $0 --error_if_not_fully_quiesced",
                 leader_ts->bound_rpc_addr().ToString()));
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "not fully quiesced");
  ASSERT_TRUE(leader_ts->server()->quiescing());
  // We must retry until the tool returns success, indicating the server is
  // fully quiesced.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(RunActionPrependStdoutStderr(
        Substitute("tserver quiesce start $0 --error_if_not_fully_quiesced",
                   leader_ts->bound_rpc_addr().ToString())));
  });

  // The server should be quiesced fully.
  ASSERT_EQ(0, leader_ts->server()->num_raft_leaders()->value());
  ASSERT_EQ(0, leader_ts->server()->scanner_manager()->CountActiveScanners());

  // The 'stop_quiescing' tool should yield a non-quiescing server.
  ASSERT_OK(RunActionPrependStdoutStderr(
      Substitute("tserver quiesce stop $0", leader_ts->bound_rpc_addr().ToString())));
  ASSERT_FALSE(leader_ts->server()->quiescing());
  NO_FATALS(rw_workload->StopAndJoin());
}

class TServerQuiescingParamITest : public TServerQuiescingITest,
                                   public testing::WithParamInterface<int> {};

// Test that a quiescing server won't trigger an election, even when prompted
// via RPC.
TEST_P(TServerQuiescingParamITest, TestQuiescingServerRejectsElectionRequests) {
  const int kNumReplicas = GetParam();
  NO_FATALS(StartCluster(kNumReplicas));

  // We'll trigger elections manually, so turn off leader failure detection.
  FLAGS_enable_leader_failure_detection = false;
  FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader = false;

  vector<string> tablet_ids;
  NO_FATALS(CreateWorkloadTable(/*num_tablets*/1, &tablet_ids));
  string tablet_id = tablet_ids[0];

  // First, do a sanity check that we don't have a leader.
  MonoDelta kLeaderTimeout = MonoDelta::FromMilliseconds(500);
  TServerDetails* leader_details;
  Status s = FindTabletLeader(ts_map_, tablet_id, kLeaderTimeout, &leader_details);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Unable to find leader");

  // Quiesce one of the tablet servers and try prompting it to become leader.
  // This should fail outright.
  auto* ts = cluster_->mini_tablet_server(0);
  *ts->server()->mutable_quiescing() = true;
  s = StartElection(FindOrDie(ts_map_, ts->uuid()), tablet_id, kLeaderTimeout);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "leader elections are disabled");

  // And we should still have no leader.
  s = FindTabletLeader(ts_map_, tablet_id, kLeaderTimeout, &leader_details);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Unable to find leader");
}

// Test that if all tservers are quiescing, there will be no leaders elected.
TEST_P(TServerQuiescingParamITest, TestNoElectionsForNewReplicas) {
  // NOTE: this test will prevent leaders of our new tablets. In practice,
  // users should have tablet creation not wait to finish if all tservers are
  // being quiesced.
  FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader = false;
  const int kNumReplicas = GetParam();
  const int kNumTablets = 10;
  NO_FATALS(StartCluster(kNumReplicas));

  // Quiesce every tablet server.
  for (int i = 0; i < kNumReplicas; i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = true;
  }

  NO_FATALS(CreateWorkloadTable(kNumTablets));

  // Sleep for a bit to let any would-be elections happen.
  SleepFor(MonoDelta::FromSeconds(1));

  // Since we've quiesced all our servers, none should have leaders.
  for (int i = 0; i < kNumReplicas; i++) {
    ASSERT_EQ(0, cluster_->mini_tablet_server(i)->server()->num_raft_leaders()->value());
  }

  // Now stop quiescing the servers and ensure that we eventually start getting
  // leaders again.
  for (int i = 0; i < kNumReplicas; i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = false;
  }
  ASSERT_EVENTUALLY([&] {
    int num_leaders = 0;
    for (int i = 0; i < kNumReplicas; i++) {
      num_leaders += cluster_->mini_tablet_server(i)->server()->num_raft_leaders()->value();
    }
    ASSERT_EQ(kNumTablets, num_leaders);
  });
}

// Test that scans are opaquely retried when sent to quiescing servers. If all
// servers are quiescing, the scans will eventually time out; if any are not
// quiescing, all scans will be directed at the non-quiescing server.
TEST_P(TServerQuiescingParamITest, TestScansRetry) {
  const int kNumReplicas = GetParam();
  NO_FATALS(StartCluster(kNumReplicas));
  string table_name;
  {
    auto rw_workload = CreateFaultIntolerantRWWorkload();
    rw_workload->Setup();
    rw_workload->Start();
    table_name = rw_workload->table_name();
    while (rw_workload->rows_inserted() < 10000) {
      SleepFor(MonoDelta::FromMilliseconds(100));
    }
    NO_FATALS(rw_workload->StopAndJoin());
  }
  // Quiesce every tablet server.
  for (int i = 0; i < kNumReplicas; i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = true;
  }
  // This should result in a failure to start scanning anything.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetTimeoutMillis(1000));
  {
    Status s = scanner.Open();
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "exceeded configured scan timeout");
  }

  // Now stop quiescing one of the servers. Our scans should succeed. Set a
  // small batch size so our scanner remains active.
  FLAGS_scanner_default_batch_size_bytes = 1;
  // Make our scanner expire really quickly so we can test that we can keep the
  // scanner alive even while the tserver is quiescing.
  FLAGS_scanner_ttl_ms = 1000;
  auto* ts = cluster_->mini_tablet_server(0)->server();
  *ts->mutable_quiescing() = false;
  KuduScanBatch batch;
  ASSERT_OK(scanner.Open());
  ASSERT_OK(scanner.NextBatch(&batch));

  // Keep the scanner alive, even as we're quiescing.
  const auto past_original_expiration =
      MonoTime::Now() + MonoDelta::FromMilliseconds(2 * FLAGS_scanner_ttl_ms);
  while (MonoTime::Now() < past_original_expiration) {
    ASSERT_EQ(1, ts->scanner_manager()->CountActiveScanners());
    ASSERT_OK(scanner.KeepAlive());
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
}

// Test that when all the tablet servers hosting a replica are quiescing, we
// can still write (assuming a leader had previously been elected).
TEST_P(TServerQuiescingParamITest, TestWriteWhileAllQuiescing) {
  const int kNumReplicas = GetParam();
  NO_FATALS(StartCluster(kNumReplicas));
  auto start_write_workload = [&] {
    // Start up a workload with some writes, with no write error tolerance.
    unique_ptr<TestWorkload> workload(new TestWorkload(cluster_.get()));
    workload->set_num_replicas(kNumReplicas);
    workload->set_num_write_threads(3);
    workload->set_num_tablets(1);
    workload->Setup();
    workload->Start();
    return workload;
  };
  auto first_workload = start_write_workload();
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    vector<string> tablet_ids;
    tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
    ASSERT_EQ(1, tablet_ids.size());
    tablet_id = tablet_ids[0];
  });

  TServerDetails* leader_details;
  const auto kLeaderTimeout = MonoDelta::FromSeconds(10);
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kLeaderTimeout, &leader_details));

  // Now quiesce all the tablet servers.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = true;
  }
  // Counts the number of times we were requested to start elections
  // cluster-wide.
  auto get_num_elections = [&] () {
    int num_elections = 0;
    for (int i = 0; i < kNumReplicas; i++) {
      auto* ts = cluster_->mini_tablet_server(i)->server();
      scoped_refptr<Histogram> hist(ts->metric_entity()->FindOrCreateHistogram(
          &METRIC_handler_latency_kudu_consensus_ConsensusService_RunLeaderElection));
      num_elections++;
    }
    return num_elections;
  };

  int initial_num_elections = get_num_elections();
  ASSERT_LT(0, initial_num_elections);

  // We should continue to write uninterrupted.
  int start_rows = first_workload->rows_inserted();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(first_workload->rows_inserted(), start_rows + 1000);
  });

  // We also should not have triggered any elections.
  ASSERT_EQ(initial_num_elections, get_num_elections());
}

TEST_P(TServerQuiescingParamITest, TestAbruptStepdownWhileAllQuiescing) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const int kNumReplicas = GetParam();
  NO_FATALS(StartCluster(kNumReplicas));
  vector<string> tablet_ids;
  NO_FATALS(CreateWorkloadTable(/*num_tablets*/1, &tablet_ids));

  // Ensure we get a leader.
  TServerDetails* leader_details;
  const auto kLeaderTimeout = MonoDelta::FromSeconds(10);
  const auto& tablet_id = tablet_ids[0];
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kLeaderTimeout, &leader_details));

  // Now quiesce all the tablet servers.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = true;
  }
  // Abruptly step down our tablet servers. We could find the leader and just
  // step it down, but it's hard to guarantee that the found leader is of the
  // latest term.
  //
  // So to be sure, try on all our replicas -- eventually we'll step down on
  // the latest leader, and we won't be able to elect a new leader since all
  // servers are quiescing.
  ASSERT_EVENTUALLY([&] {
    bool stepped_down = false;
    for (const auto& ts_and_details : ts_map_) {
      Status s = LeaderStepDown(ts_and_details.second, tablet_id, kLeaderTimeout);
      if (s.ok()) {
        stepped_down = true;
        break;
      }
      LOG(INFO) << Substitute("Request to step down failed on $0: $1",
                              ts_and_details.first, s.ToString());
    }
    ASSERT_TRUE(stepped_down);

    // There should be no leaders.
    Status s = FindTabletLeader(ts_map_, tablet_id, kLeaderTimeout, &leader_details);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  });
}

INSTANTIATE_TEST_CASE_P(NumReplicas, TServerQuiescingParamITest, ::testing::Values(1, 3));

} // namespace itest
} // namespace kudu
