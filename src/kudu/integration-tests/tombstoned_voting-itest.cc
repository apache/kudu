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
#include <set>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::consensus::MakeOpId;
using kudu::itest::TServerDetails;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletSuperBlockPB;
using std::set;
using std::string;

namespace kudu {

class TombstonedVotingITest : public ExternalMiniClusterITestBase {
};

// Test that a replica that crashes during a first-time tablet copy after
// persisting a superblock but before persisting a cmeta file will be
// tombstoned and able to vote after the tablet server is restarted.
// See KUDU-2123.
TEST_F(TombstonedVotingITest, TestTombstonedReplicaWithoutCMetaCanVote) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  // Cause tablet copy operations to crash the target.
  NO_FATALS(StartCluster({"--tablet_copy_fault_crash_before_write_cmeta=1.0"}, {},
                         /*num_tablet_servers=*/ 4));
  TestWorkload workload(cluster_.get());
  workload.Setup();
  // Load some data and ensure we have elected a leader.
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(10, workload.batches_completed());
  });
  workload.StopAndJoin();

  // Wait for all 3 replicas to come up and figure out where they landed.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(itest::GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                                     kTimeout, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size());
  string tablet_id = table_locations.tablet_locations(0).tablet_id();
  set<string> replica_uuids;
  for (const auto& replica : table_locations.tablet_locations(0).replicas()) {
    replica_uuids.insert(replica.ts_info().permanent_uuid());
  }

  // Figure out which tablet server didn't get a replica. We will use it for
  // testing.
  string new_replica_uuid;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    if (!ContainsKey(replica_uuids, cluster_->tablet_server(i)->uuid())) {
      new_replica_uuid = cluster_->tablet_server(i)->uuid();
      break;
    }
  }
  ASSERT_FALSE(new_replica_uuid.empty());
  auto new_replica_ts = ts_map_[new_replica_uuid];
  auto new_replica_ets = cluster_->tablet_server_by_uuid(new_replica_uuid);

  // Initiating a tablet copy operation will crash the target replica before
  // it gets a chance to persist a cmeta file. But it will have a superblock.
  ASSERT_EVENTUALLY([&] {
    TServerDetails* leader_ts;
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_ts));
    ExternalTabletServer* leader_ets = cluster_->tablet_server_by_uuid(leader_ts->uuid());
    Status s = itest::StartTabletCopy(new_replica_ts, tablet_id, leader_ts->uuid(),
                                      leader_ets->bound_rpc_hostport(),
                                      /*caller_term=*/ 0, // Any term will do.
                                      kTimeout);
    ASSERT_TRUE(!s.ok()) << s.ToString(); // Crashed.
    ASSERT_OK(new_replica_ets->WaitForInjectedCrash(MonoDelta::FromSeconds(2)));
  });

  cluster_->Shutdown();

  // Verify that there is a superblock but no cmeta.
  int new_replica_idx = cluster_->tablet_server_index_by_uuid(new_replica_uuid);
  ASSERT_OK(inspect_->CheckTabletDataStateOnTS(new_replica_idx, tablet_id,
                                               { TABLET_DATA_COPYING }));
  ASSERT_FALSE(inspect_->DoesConsensusMetaExistForTabletOnTS(new_replica_idx, tablet_id));

  // Restart only the replica that was the target of the tablet copy. It should
  // be able to vote.
  ASSERT_OK(new_replica_ets->Restart());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(new_replica_idx, tablet_id,
                                                 { TABLET_DATA_TOMBSTONED },
                                                 kTimeout));
  TabletSuperBlockPB superblock_pb;
  ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(new_replica_idx, tablet_id, &superblock_pb));

  // The tombstoned replica should be using 1.0 as its last-logged OpId.
  ASSERT_TRUE(superblock_pb.has_tombstone_last_logged_opid());
  ASSERT_OPID_EQ(MakeOpId(1, 0), superblock_pb.tombstone_last_logged_opid());

  const string kCandidateUuid = "X";
  const int64_t kCandidateTerm = 2;

  // We may need to retry due to waiting for RaftConsensus to initialize.
  ASSERT_EVENTUALLY([&] {
    // Initially, even when the replica is running, cmeta should not exist on disk.
    ASSERT_FALSE(inspect_->DoesConsensusMetaExistForTabletOnTS(new_replica_idx, tablet_id));

    // Should vote no to OpId 0.0 because it's smaller than 1.0.
    Status s = itest::RequestVote(new_replica_ts, tablet_id, kCandidateUuid,
                                  kCandidateTerm,
                                  /*last_logged_opid=*/ MakeOpId(0, 0),
                                  /*ignore_live_leader=*/ true,
                                  /*is_pre_election=*/ true,
                                  MonoDelta::FromSeconds(5));
    ASSERT_FALSE(s.ok()) << s.ToString();
    ASSERT_STR_MATCHES(s.ToString(), "Denying vote.*greater than that of the candidate");

    // Should vote yes to 2.2 because it's larger than 1.0.
    s = itest::RequestVote(new_replica_ts, tablet_id, kCandidateUuid,
                           kCandidateTerm,
                           /*last_logged_opid=*/ MakeOpId(2, 2),
                           /*ignore_live_leader=*/ true,
                           /*is_pre_election=*/ true,
                           MonoDelta::FromSeconds(5));
    ASSERT_TRUE(s.ok()) << s.ToString();

    // After voting yes, cmeta should exist.
    ASSERT_FALSE(inspect_->DoesConsensusMetaExistForTabletOnTS(new_replica_idx, tablet_id));
  });
}

} // namespace kudu
