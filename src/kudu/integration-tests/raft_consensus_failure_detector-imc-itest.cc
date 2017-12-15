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
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/periodic.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_evict_excess_replicas);

using kudu::consensus::RaftPeerPB;
using kudu::itest::WaitForServersToAgree;
using kudu::tablet::TabletReplica;
using std::string;
using std::vector;

namespace kudu {

class RaftConsensusFailureDetectorIMCTest : public MiniClusterITestBase {
};

// Ensure that the failure detector is enabled for non-leader voters, and
// disabled for leaders and non-voters. Ensure this persists after a
// configuration change.
// Regression test for KUDU-2229.
TEST_F(RaftConsensusFailureDetectorIMCTest, TestFailureDetectorActivation) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  // If running with the 3-4-3 replication scheme, the catalog manager removes
  // excess replicas, so it's necessary to disable that default behavior since
  // this test adds an extra non-voter to validate its leader failure detector
  // status.
  FLAGS_catalog_manager_evict_excess_replicas = false;

  const int kNumReplicas = 3;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/ kNumReplicas + 1));
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  while (workload.batches_completed() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Identify the tablet id and locate the replicas.
  string tablet_id;
  itest::TabletReplicaMap replica_map_;
  string missing_replica_uuid;
  ASSERT_EVENTUALLY([&] {
    replica_map_.clear();
    missing_replica_uuid.clear();
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      auto tablet_ids = ts->ListTablets();
      if (tablet_ids.empty()) {
        missing_replica_uuid = ts->uuid();
        continue;
      }
      ASSERT_EQ(1, tablet_ids.size());
      if (!tablet_id.empty()) {
        ASSERT_EQ(tablet_id, tablet_ids[0]);
      }
      tablet_id = tablet_ids[0];
      replica_map_.emplace(tablet_id, ts_map_[ts->uuid()]);
    }
    ASSERT_EQ(kNumReplicas, replica_map_.count(tablet_id));
    ASSERT_FALSE(tablet_id.empty());
  });

  auto active_ts_map = ts_map_;
  ASSERT_EQ(1, active_ts_map.erase(missing_replica_uuid));

  // Ensure all servers are up to date.
  ASSERT_OK(WaitForServersToAgree(kTimeout, active_ts_map, tablet_id,
                                  /*minimum_index=*/ 1));

  // Ensure that the failure detector activation state is consistent with the
  // rule that only non-leader VOTER replicas should have failure detection
  // enabled. Leaders and NON_VOTER replicas should not enable the FD.
  auto validate_failure_detector_status = [&](const itest::TabletServerMap& ts_map) {
    for (const auto& entry : ts_map) {
      const auto& uuid = entry.first;
      auto mini_ts = cluster_->mini_tablet_server_by_uuid(uuid);
      vector<scoped_refptr<TabletReplica>> replicas;
      mini_ts->server()->tablet_manager()->GetTabletReplicas(&replicas);
      ASSERT_EQ(1, replicas.size());
      const auto& replica = replicas[0];
      ASSERT_EQ(replica->consensus()->role() == RaftPeerPB::FOLLOWER,
                replica->consensus()->GetFailureDetectorForTests()->started());
    }
  };

  NO_FATALS(validate_failure_detector_status(active_ts_map));

  // Add a new non-voter.
  ASSERT_EVENTUALLY([&] {
    itest::TServerDetails* leader;
    ASSERT_OK(FindTabletLeader(active_ts_map, tablet_id, kTimeout, &leader));
    ASSERT_OK(AddServer(leader, tablet_id,
                        ts_map_[missing_replica_uuid],
                        RaftPeerPB::NON_VOTER, kTimeout));
  });
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id,
                                  /*minimum_index=*/ 2));

  NO_FATALS(validate_failure_detector_status(ts_map_));
}

} // namespace kudu
