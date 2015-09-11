// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/optional.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"

using boost::assign::list_of;
using kudu::consensus::RaftPeerPB;
using kudu::itest::TServerDetails;
using kudu::tablet::TABLET_DATA_READY;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tserver::ListTabletsResponsePB;
using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {

class TabletReplacementITest : public ExternalMiniClusterITestBase {
};

// Test that the Master will tombstone a newly-evicted replica.
// Then, test that the Master will NOT tombstone a newly-added replica that is
// not part of the committed config yet (only the pending config).
TEST_F(TabletReplacementITest, TestMasterTombstoneEvictedReplica) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags = list_of("--enable_leader_failure_detection=false");
  int num_tservers = 5;
  vector<string> master_flags = list_of("--master_add_server_when_underreplicated=false");
  NO_FATALS(StartCluster(ts_flags, master_flags, num_tservers));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(num_tservers);
  workload.Setup(); // Easy way to create a new tablet.

  const int kLeaderIndex = 0;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  const int kFollowerIndex = 4;
  TServerDetails* follower_ts = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(itest::WaitForNumTabletsOnTS(leader_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ASSERT_OK(itest::StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(itest::WaitForServersToAgree(timeout, ts_map_, tablet_id, 1)); // Wait for NO_OP.

  // Remove a follower from the config.
  ASSERT_OK(itest::RemoveServer(leader_ts, tablet_id, follower_ts, boost::none, timeout));

  // Wait for the Master to tombstone the replica.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kFollowerIndex, tablet_id, TABLET_DATA_TOMBSTONED,
                                                 timeout));

  if (!AllowSlowTests()) {
    // The rest of this test has multi-second waits, so we do it in slow test mode.
    LOG(INFO) << "Not verifying that a newly-added replica won't be tombstoned in fast-test mode";
    return;
  }

  // Shut down a majority of followers (3 servers) and then try to add the
  // follower back to the config. This will cause the config change to end up
  // in a pending state.
  unordered_map<string, itest::TServerDetails*> active_ts_map = ts_map_;
  for (int i = 1; i <= 3; i++) {
    cluster_->tablet_server(i)->Shutdown();
    ASSERT_EQ(1, active_ts_map.erase(cluster_->tablet_server(i)->uuid()));
  }
  // This will time out, but should take effect.
  Status s = itest::AddServer(leader_ts, tablet_id, follower_ts, RaftPeerPB::VOTER,
                              boost::none, MonoDelta::FromSeconds(5));
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kFollowerIndex, tablet_id, TABLET_DATA_READY,
                                                 timeout));
  ASSERT_OK(itest::WaitForServersToAgree(timeout, active_ts_map, tablet_id, 3));

  // Sleep for a few more seconds and check again to ensure that the Master
  // didn't end up tombstoning the replica.
  SleepFor(MonoDelta::FromSeconds(3));
  ASSERT_OK(inspect_->CheckTabletDataStateOnTS(kFollowerIndex, tablet_id, TABLET_DATA_READY));
}

// Ensure that the Master will tombstone a replica if it reports in with an old
// config. This tests a slightly different code path in the catalog manager
// than TestMasterTombstoneEvictedReplica does.
TEST_F(TabletReplacementITest, TestMasterTombstoneOldReplicaOnReport) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags = list_of("--enable_leader_failure_detection=false");
  vector<string> master_flags = list_of("--master_add_server_when_underreplicated=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  TestWorkload workload(cluster_.get());
  workload.Setup(); // Easy way to create a new tablet.

  const int kLeaderIndex = 0;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  const int kFollowerIndex = 2;
  TServerDetails* follower_ts = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(itest::WaitForNumTabletsOnTS(leader_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ASSERT_OK(itest::StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(itest::WaitForServersToAgree(timeout, ts_map_, tablet_id, 1)); // Wait for NO_OP.

  // Shut down the follower to be removed, then remove it from the config.
  // We will wait for the Master to be notified of the config change, then shut
  // down the rest of the cluster and bring the follower back up. The follower
  // will heartbeat to the Master and then be tombstoned.
  cluster_->tablet_server(kFollowerIndex)->Shutdown();

  // Remove the follower from the config and wait for the Master to notice the
  // config change.
  ASSERT_OK(itest::RemoveServer(leader_ts, tablet_id, follower_ts, boost::none, timeout));
  ASSERT_OK(itest::WaitForNumVotersInConfigOnMaster(cluster_->master_proxy(), tablet_id, 2,
                                                    timeout));

  // Shut down the remaining tablet servers and restart the dead one.
  cluster_->tablet_server(0)->Shutdown();
  cluster_->tablet_server(1)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kFollowerIndex)->Restart());

  // Wait for the Master to tombstone the revived follower.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kFollowerIndex, tablet_id, TABLET_DATA_TOMBSTONED,
                                                 timeout));
}

// Test that unreachable followers are evicted and replaced.
TEST_F(TabletReplacementITest, TestEvictAndReplaceDeadFollower) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags = list_of("--enable_leader_failure_detection=false")
                                   ("--follower_unavailable_considered_failed_sec=5");
  NO_FATALS(StartCluster(ts_flags));

  TestWorkload workload(cluster_.get());
  workload.Setup(); // Easy way to create a new tablet.

  const int kLeaderIndex = 0;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  const int kFollowerIndex = 2;

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(itest::WaitForNumTabletsOnTS(leader_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ASSERT_OK(itest::StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(itest::WaitForServersToAgree(timeout, ts_map_, tablet_id, 1)); // Wait for NO_OP.

  // Shut down the follower to be removed. It should be evicted.
  cluster_->tablet_server(kFollowerIndex)->Shutdown();

  // With a RemoveServer and AddServer, the opid_index of the committed config will be 3.
  ASSERT_OK(itest::WaitUntilCommittedConfigOpidIndexIs(3, leader_ts, tablet_id, timeout));
  ASSERT_OK(cluster_->tablet_server(kFollowerIndex)->Restart());
}

} // namespace kudu
