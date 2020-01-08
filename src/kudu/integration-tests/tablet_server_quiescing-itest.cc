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
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(scanner_default_batch_size_bytes);
DECLARE_int32(scanner_ttl_ms);
DECLARE_int32(raft_heartbeat_interval_ms);

using kudu::client::KuduClient;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::tserver::MiniTabletServer;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

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
  FLAGS_raft_heartbeat_interval_ms = 100;
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

  // Cause a bunch of elections.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms;

  // Soon enough, elections will occur, and our quiescing server will cease to
  // be leader.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, ts->server()->num_raft_leaders()->value());
  });

  // When we stop quiescing the server, we should eventually see some
  // leadership return to the server.
  *ts->server()->mutable_quiescing() = false;
  ASSERT_EVENTUALLY([&] {
    ASSERT_LT(0, ts->server()->num_raft_leaders()->value());
  });
}

// Test that even if a majority of replicas are quiescing, a tablet is still
// able to elect a leader.
TEST_F(TServerQuiescingITest, TestMajorityQuiescingElectsLeader) {
  const int kNumReplicas = 3;
  FLAGS_raft_heartbeat_interval_ms = 50;
  NO_FATALS(StartCluster(kNumReplicas));
  vector<string> tablet_ids;
  NO_FATALS(CreateWorkloadTable(/*num_tablets*/1, &tablet_ids));
  string tablet_id = tablet_ids[0];

  // Start quiescing all but the first tserver.
  for (int i = 1; i < kNumReplicas; i++) {
    *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = true;
  }

  // Cause a bunch of elections.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms;

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
  FLAGS_raft_heartbeat_interval_ms = 100;
  // Set a tiny batch size to encourage many batches for a single scan. This
  // will emulate long-running scans.
  FLAGS_scanner_default_batch_size_bytes = 1;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas.
  auto rw_workload = CreateFaultIntolerantRWWorkload();
  rw_workload->set_scanner_selection(client::KuduClient::LEADER_ONLY);
  rw_workload->Setup();
  rw_workload->Start();

  // Inject a bunch of leader elections to stress leadership changes.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms;

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
  // users should have tablet creation not wait to finish if there all tservers
  // are being quiesced.
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

INSTANTIATE_TEST_CASE_P(NumReplicas, TServerQuiescingParamITest, ::testing::Values(1, 3));

} // namespace itest
} // namespace kudu
