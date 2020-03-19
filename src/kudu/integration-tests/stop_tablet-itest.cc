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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);

namespace kudu {
namespace tserver {

using client::sp::shared_ptr;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduScanner;
using client::KuduTable;
using itest::FindTabletLeader;
using itest::TServerDetails;
using std::string;
using std::vector;
using strings::Substitute;
using tablet::TabletReplica;
using tserver::MiniTabletServer;

enum ReplicaType {
  LEADER, FOLLOWER
};

class StopTabletITest : public MiniClusterITestBase,
                        public ::testing::WithParamInterface<ReplicaType> {
 public:
  void SetUp() override {
    // To spur more flushes, which may be hampered by stopped tablets, set
    // flags to flush a lot.
    FLAGS_flush_threshold_mb = 1;
    FLAGS_flush_threshold_secs = 1;
  }

  // Returns the index of the tablet server housing the LEADER or a FOLLOWER
  // for 'tablet_id', based on the test parameter.
  Status GetTServerNumToFail(const string& tablet_id, int* ret) {
    RETURN_NOT_OK(GetLeaderTServerNum(tablet_id, ret));
    *ret += GetParam() == LEADER ? 0 : 1;
    *ret %= cluster_->num_tablet_servers();
    return Status::OK();
  }

  // Stop the tablet at the specified tablet server.
  void StopTablet(const string& tablet_id, int tserver_num = 0) {
    scoped_refptr<TabletReplica> replica;
    MiniTabletServer* ts = cluster_->mini_tablet_server(tserver_num);
    LOG(INFO) << Substitute("Stopping T $0 P $1", tablet_id, ts->uuid());
    ASSERT_OK(ts->server()->tablet_manager()->GetTabletReplica(tablet_id, &replica));
    replica->tablet()->Stop();

    // We need to also stop replica ops since, upon running with a stopped
    // tablet, they'll abort immediately and hog the maintenance threads. In
    // production, this isn't an issue because replica ops are expected to be
    // unregistered shortly after stopping the tablet.
    replica->CancelMaintenanceOpsForTests();
  }

  // Starts a new cluster and creates a tablet with the specified number of
  // replicas. Waits until a replica is running on at least one server before
  // returning with the tablet id.
  void StartClusterAndLoadTablet(int num_replicas, string* tablet_id) {
    NO_FATALS(StartCluster(num_replicas));

    TestWorkload writes(cluster_.get());
    writes.set_num_replicas(num_replicas);
    writes.Setup();

    // Get the tablet id of the written tablet.
    MiniTabletServer* ts = cluster_->mini_tablet_server(0);
    ASSERT_EVENTUALLY([&] {
      vector<string> tablet_ids = ts->ListTablets();
      ASSERT_EQ(1, tablet_ids.size());
      *tablet_id = tablet_ids[0];
    });

    // Wait until the replica is running.
    AssertEventually([&] {
      scoped_refptr<TabletReplica> replica;
      ASSERT_OK(ts->server()->tablet_manager()->GetTabletReplica(*tablet_id, &replica));
      ASSERT_EQ(replica->state(), tablet::RUNNING);
    }, MonoDelta::FromSeconds(60));
  }

 private:
  // Returns the index of the tablet server housing the LEADER for 'tablet_id',
  Status GetLeaderTServerNum(const string& tablet_id, int* leader_num) {
    TServerDetails* leader_details;
    RETURN_NOT_OK(FindTabletLeader(
        ts_map_, tablet_id, MonoDelta::FromSeconds(10), &leader_details));
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      MiniTabletServer* candidate = cluster_->mini_tablet_server(i);
      if (candidate->uuid() ==
          leader_details->instance_id.permanent_uuid()) {
        *leader_num = i;
        return Status::OK();
      }
    }
    return Status::NotFound("Could not find the tablet leader");
  }
};

TEST_P(StopTabletITest, TestSingleStoppedTabletsDontScan) {
  const int kNumReplicas = 1;
  const string& kTableName = "foo";
  string tablet_id;
  NO_FATALS(StartCluster(kNumReplicas));

  // First, write some rows to the tablet.
  TestWorkload write_workload(cluster_.get());
  int64_t kTargetNumRows = 10000;
  write_workload.set_timeout_allowed(true);
  write_workload.set_num_replicas(kNumReplicas);
  write_workload.set_table_name(kTableName);
  write_workload.Setup();
  write_workload.Start();
  while (write_workload.rows_inserted() < kTargetNumRows) {
    SleepFor(MonoDelta::FromMilliseconds(50));
    KLOG_EVERY_N_SECS(INFO, 1) << Substitute("$0 / $1 rows inserted",
        write_workload.rows_inserted(), kTargetNumRows);
  }
  write_workload.StopAndJoin();
  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  ASSERT_EVENTUALLY([&] {
    vector<string> tablet_ids = ts->ListTablets();
    ASSERT_EQ(1, tablet_ids.size());
    tablet_id = tablet_ids[0];
  });

  // Create a scanner for the tablet.
  shared_ptr<KuduTable> table;
  shared_ptr<KuduClient> client;
  KuduClientBuilder client_builder;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  while (!table.get()) {
    client.get()->OpenTable(kTableName, &table).ToString();
  }

  // Now stop the tablet.
  NO_FATALS(StopTablet(tablet_id, 0));

  // Try using the scanner. Since there is only a single tablet, and since the
  // tablet has been stopped, it shouldn't return anything.
  // Even a fault tolerant scanner will not be able to do anything.
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetTimeoutMillis(500));
  ASSERT_OK(scanner.SetFaultTolerant());
  Status s = scanner.Open();
  LOG(INFO) << "Scanner opened with status: " << s.ToString();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
}

// Ensure that stopping a tablet will not prevent scans from completing.
TEST_P(StopTabletITest, TestStoppedTabletsScansGetRedirected) {
  // We use multiple replicas so we can fail one and ensure that our reads can
  // still go through (i.e. the test workload won't error out because it can't
  // open a scanner or somesuch).
  const int kNumReplicas = 3;
  string tablet_id;
  NO_FATALS(StartClusterAndLoadTablet(kNumReplicas, &tablet_id));

  // Write some initial data to the tablet.
  TestWorkload rw_workload(cluster_.get());
  int64_t kTargetNumRows = 10000;
  rw_workload.set_num_read_threads(1);
  rw_workload.set_timeout_allowed(true);
  rw_workload.set_num_replicas(kNumReplicas);
  rw_workload.Setup();
  rw_workload.Start();
  while (rw_workload.rows_inserted() < kTargetNumRows) {
    SleepFor(MonoDelta::FromMilliseconds(50));
    KLOG_EVERY_N_SECS(INFO, 1) << Substitute("$0 / $1 rows inserted",
        rw_workload.rows_inserted(), kTargetNumRows);
  }

  // Stop the replica.
  int replica_to_fail;
  ASSERT_OK(GetTServerNumToFail(tablet_id, &replica_to_fail));
  ASSERT_GE(replica_to_fail, 0);
  NO_FATALS(StopTablet(tablet_id, replica_to_fail));

  // Allow some time to continue scanning.
  SleepFor(MonoDelta::FromSeconds(10));
  NO_FATALS(rw_workload.StopAndJoin());
}

TEST_P(StopTabletITest, TestStoppedTabletsDontWrite) {
  const int kNumReplicas = 3;
  string tablet_id;
  NO_FATALS(StartClusterAndLoadTablet(kNumReplicas, &tablet_id));

  TestWorkload writes(cluster_.get());
  writes.set_timeout_allowed(true);
  writes.Setup();
  writes.Start();

  int64_t init_rows_snapshot = writes.rows_inserted();
  int64_t target_before_stop = init_rows_snapshot + 10000;

  // Wait for some rows to be inserted.
  while (writes.rows_inserted() < target_before_stop) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }

  int replica_to_fail;
  ASSERT_OK(GetTServerNumToFail(tablet_id, &replica_to_fail));
  ASSERT_GE(replica_to_fail, 0);

  NO_FATALS(StopTablet(tablet_id, replica_to_fail));
  if (GetParam() == LEADER) {
    LOG(INFO) << "Waiting for writes to settle...";
    // We've stopped the leader. Wait a bit for any writes that may have been
    // already Committing to respond.
    SleepFor(MonoDelta::FromSeconds(10));
    int64_t num_rows_after_stop = writes.rows_inserted();

    // Now that the leader has been stopped, wait longer and ensure that no
    // more writes get through.
    // NOTE: a new leader will not be elected because the Tablet has only been
    // stopped; the TabletReplica is not shutdown and can still heartbeat.
    LOG(INFO) << "Waiting to ensure no more writes come in...";
    SleepFor(MonoDelta::FromSeconds(5));
    ASSERT_EQ(num_rows_after_stop, writes.rows_inserted());
    LOG(INFO) << "Success! No more writes came in!";
  } else {
    // A stopped follower will not prevent ops from being written since there
    // should still be a healthy majority.
    int target_after_stop = writes.rows_inserted() + 10000;
    while (writes.rows_inserted() < target_after_stop) {
      SleepFor(MonoDelta::FromMilliseconds(50));
      KLOG_EVERY_N_SECS(INFO, 1) << Substitute("$0 / $1 rows inserted",
          writes.rows_inserted(), target_after_stop);
    }
  }
  writes.StopAndJoin();
}

TEST_P(StopTabletITest, TestShutdownWhileWriting) {
  const int kNumReplicas = 3;
  string tablet_id;
  NO_FATALS(StartClusterAndLoadTablet(kNumReplicas, &tablet_id));

  TestWorkload writes(cluster_.get());
  writes.set_timeout_allowed(true);
  // TODO(todd): why does the client get a RemoteError in this test?
  writes.set_remote_error_allowed(true);
  writes.set_write_timeout_millis(500);
#ifndef THREAD_SANITIZER // high thread count is too slow in TSAN.
  writes.set_num_write_threads(base::NumCPUs());
#endif
  writes.Setup();
  writes.Start();

  // Wait at least a few seconds so that maintenance ops can get scheduled.
  SleepFor(MonoDelta::FromSeconds(2));

  // Ensure we inserted at least some number of rows.
  while (writes.rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }

  for (int i = 0; i < 3; i++) {
    cluster_->mini_tablet_server(i)->Shutdown();
  }
  writes.StopAndJoin();

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(cluster_->mini_tablet_server(i)->Restart());
  }

  ClusterVerifier cv(cluster_.get());
  NO_FATALS(cv.CheckCluster());
}

INSTANTIATE_TEST_CASE_P(StopTablets, StopTabletITest, ::testing::Values(LEADER, FOLLOWER));

}  // namespace tserver
}  // namespace kudu
