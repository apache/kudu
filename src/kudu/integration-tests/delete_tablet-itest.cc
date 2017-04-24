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

#include <vector>

#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/mini_master.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"

DECLARE_int64(fs_wal_dir_reserved_bytes);

using kudu::tablet::TabletPeer;
using std::vector;

namespace kudu {

class DeleteTabletITest : public MiniClusterITestBase {
};

// Test deleting a failed replica. Regression test for KUDU-1607.
TEST_F(DeleteTabletITest, TestDeleteFailedReplica) {
  NO_FATALS(StartCluster(1)); // 1 TS.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  std::unordered_map<std::string, itest::TServerDetails*> ts_map;
  ValueDeleter del(&ts_map);
  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy(),
                                         cluster_->messenger(),
                                         &ts_map));
  auto* mts = cluster_->mini_tablet_server(0);
  auto* ts = ts_map[mts->uuid()];

  scoped_refptr<TabletPeer> tablet_peer;
  ASSERT_EVENTUALLY([&] {
    vector<scoped_refptr<TabletPeer>> tablet_peers;
    mts->server()->tablet_manager()->GetTabletPeers(&tablet_peers);
    ASSERT_EQ(1, tablet_peers.size());
    tablet_peer = tablet_peers[0];
  });

  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // We need blocks on-disk for this regression test, so force an MRS flush.
  ASSERT_OK(tablet_peer->tablet()->Flush());

  // Shut down the master so it doesn't crash the test process when we make the
  // disk appear to be full.
  cluster_->mini_master()->Shutdown();

  // Shut down the TS and restart it after changing flags to ensure no data can
  // be written during tablet bootstrap.
  mts->Shutdown();
  FLAGS_fs_wal_dir_reserved_bytes = INT64_MAX;
  ASSERT_OK(mts->Restart());
  Status s = mts->server()->tablet_manager()->WaitForAllBootstrapsToFinish();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient disk space");

  // Tablet bootstrap failure should result in tablet status == FAILED.
  {
    vector<scoped_refptr<TabletPeer>> tablet_peers;
    mts->server()->tablet_manager()->GetTabletPeers(&tablet_peers);
    ASSERT_EQ(1, tablet_peers.size());
    tablet_peer = tablet_peers[0];
    ASSERT_EQ(tablet::FAILED, tablet_peer->state());
  }

  // We should still be able to delete the failed tablet.
  ASSERT_OK(itest::DeleteTablet(ts, tablet_peer->tablet_id(), tablet::TABLET_DATA_DELETED,
                                boost::none, MonoDelta::FromSeconds(30)));
  ASSERT_EVENTUALLY([&] {
    vector<scoped_refptr<TabletPeer>> tablet_peers;
    mts->server()->tablet_manager()->GetTabletPeers(&tablet_peers);
    ASSERT_EQ(0, tablet_peers.size());
  });
}

} // namespace kudu
