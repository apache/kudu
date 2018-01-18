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
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(fs_wal_dir_reserved_bytes);

using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletReplica;
using kudu::itest::DeleteTablet;
using std::atomic;
using std::string;
using std::thread;
using std::vector;

namespace kudu {

class DeleteTabletITest : public MiniClusterITestBase {
};

// Test deleting a failed replica. Regression test for KUDU-1607.
TEST_F(DeleteTabletITest, TestDeleteFailedReplica) {
  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 1));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  auto* mts = cluster_->mini_tablet_server(0);
  auto* ts = ts_map_[mts->uuid()];

  scoped_refptr<TabletReplica> tablet_replica;
  ASSERT_EVENTUALLY([&] {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    mts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(1, tablet_replicas.size());
    tablet_replica = tablet_replicas[0];
  });

  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // We need blocks on-disk for this regression test, so force an MRS flush.
  ASSERT_OK(tablet_replica->tablet()->Flush());

  // Shut down the master so it doesn't crash the test process when we make the
  // disk appear to be full.
  cluster_->mini_master()->Shutdown();

  // Shut down the TS and restart it after changing flags to ensure no data can
  // be written during tablet bootstrap.
  tablet_replica.reset();
  mts->Shutdown();
  FLAGS_fs_wal_dir_reserved_bytes = INT64_MAX;
  ASSERT_OK(mts->Restart());
  Status s = mts->server()->tablet_manager()->WaitForAllBootstrapsToFinish();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient disk space");

  // Tablet bootstrap failure should result in tablet status == FAILED.
  {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    mts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(1, tablet_replicas.size());
    tablet_replica = tablet_replicas[0];
    ASSERT_EQ(tablet::FAILED, tablet_replica->state());
  }

  // We should still be able to delete the failed tablet.
  ASSERT_OK(DeleteTablet(ts, tablet_replica->tablet_id(), tablet::TABLET_DATA_DELETED,
                         MonoDelta::FromSeconds(30)));
  ASSERT_EVENTUALLY([&] {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    mts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(0, tablet_replicas.size());
  });
}

// Test that a leader election will not cause a crash when a tablet is deleted.
// Regression test for KUDU-2118.
TEST_F(DeleteTabletITest, TestLeaderElectionDuringDeleteTablet) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumTablets = 10;
  NO_FATALS(StartCluster());
  TestWorkload workload(cluster_.get());
  workload.set_num_tablets(kNumTablets);
  workload.Setup();
  vector<string> tablet_ids;
  ASSERT_EVENTUALLY([&] {
    tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
    ASSERT_EQ(kNumTablets, tablet_ids.size()) << tablet_ids;
  });

  // Start threads that start leader elections.
  vector<thread> threads;
  atomic<bool> running(true);
  auto* ts = ts_map_[cluster_->mini_tablet_server(0)->uuid()];
  for (const string& tablet_id : tablet_ids) {
    threads.emplace_back([ts, tablet_id, &running, &kTimeout] {
      while (running) {
        itest::StartElection(ts, tablet_id, kTimeout); // Ignore result.
      }
    });
  }

  // Sequentially delete all of the tablets.
  for (const string& tablet_id : tablet_ids) {
    ASSERT_OK(DeleteTablet(ts, tablet_id, TABLET_DATA_DELETED, kTimeout));
  }

  // Wait until all tablets are deleted.
  ASSERT_EVENTUALLY([&] {
    tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
    ASSERT_EQ(0, tablet_ids.size()) << tablet_ids;
  });

  // Stop all the election threads.
  running = false;
  for (auto& t : threads) {
    t.join();
  }
}

// Regression test for KUDU-2126 : Ensure that a DeleteTablet() RPC
// on a tombstoned tablet does not cause any fsyncs.
TEST_F(DeleteTabletITest, TestNoOpDeleteTabletRPC) {
  // Setup the Mini Cluster
  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 1));

  // Determine the Mini Cluster Cluster workload
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();
  workload.Start();
  workload.StopAndJoin();

  auto* mts = cluster_->mini_tablet_server(0);
  auto* ts = ts_map_[mts->uuid()];

  // Verify number of tablets in the cluster
  vector<string> tablet_ids = mts->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  const string& tablet_id = tablet_ids[0];

  // Get the Tablet Replica
  scoped_refptr<TabletReplica> tablet_replica;
  vector<scoped_refptr<TabletReplica>> tablet_replicas;
  mts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
  ASSERT_EQ(1, tablet_replicas.size());
  tablet_replica = tablet_replicas[0];
  auto flush_count = [&]() {
    return tablet_replica->tablet_metadata()->flush_count_for_tests();
  };

  // Kill the master, so we can send the DeleteTablet RPC
  // without any interference
  cluster_->mini_master()->Shutdown();

  // Send the first DeleteTablet RPC
  int flush_count_before = flush_count();
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  ASSERT_OK(DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));
  int flush_count_after = flush_count();

  ASSERT_EQ(2, flush_count_after - flush_count_before);

  // Send the second DeleteTablet RPC
  flush_count_before = flush_count();
  ASSERT_OK(DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));
  flush_count_after = flush_count();

  ASSERT_EQ(0, flush_count_after - flush_count_before);
}

} // namespace kudu
