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
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(allow_unsafe_replication_factor);
DECLARE_bool(enable_tablet_copy);
DECLARE_bool(raft_enable_tombstoned_voting);

using kudu::consensus::MakeOpId;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::OpId;
using kudu::consensus::RECEIVED_OPID;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RaftPeerPB;
using kudu::itest::DeleteTablet;
using kudu::itest::TServerDetails;
using kudu::itest::WaitForServersToAgree;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletReplica;
using kudu::tablet::TabletStatePB;
using kudu::tserver::TSTabletManager;
using std::string;
using std::vector;

namespace kudu {

class TombstonedVotingIMCITest : public MiniClusterITestBase {
};

// Ensure that a tombstoned replica cannot vote after we call Shutdown() on it.
TEST_F(TombstonedVotingIMCITest, TestNoVoteAfterShutdown) {
  // This test waits for several seconds, so only run it in slow mode.
  if (!AllowSlowTests()) return;

  FLAGS_allow_unsafe_replication_factor = true; // Allow an even replication factor.
  FLAGS_enable_tablet_copy = false; // Tablet copy would interfere with this test.

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 2));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2); // Two servers and replicas makes the test easy to debug.
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 50) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  workload.StopAndJoin();

  // Figure out the tablet id to mess with.
  vector<string> tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  const string& tablet_id = tablet_ids[0];

  // Ensure all servers are up to date.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, workload.batches_completed()));

  // Manually tombstone the replica on TS1, start an election on TS0, and wait
  // until TS0 gets elected. If TS0 gets elected then TS1 was able to vote
  // while tombstoned.
  TSTabletManager* ts_tablet_manager = cluster_->mini_tablet_server(1)->server()->tablet_manager();
  scoped_refptr<TabletReplica> ts1_replica;
  ASSERT_OK(ts_tablet_manager->GetTabletReplica(tablet_id, &ts1_replica));

  // Tombstone TS1's replica.
  LOG(INFO) << "Tombstoning ts1...";
  ASSERT_OK(ts_tablet_manager->DeleteTablet(tablet_id, TABLET_DATA_TOMBSTONED,
                                            boost::none));
  ASSERT_EQ(TabletStatePB::STOPPED, ts1_replica->state());

  scoped_refptr<TabletReplica> ts0_replica;
  ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletReplica(
      tablet_id, &ts0_replica));
  LeaderStepDownResponsePB resp;
  ts0_replica->consensus()->StepDown(&resp); // Ignore result, in case TS1 was the leader.
  ASSERT_EQ(RaftPeerPB::FOLLOWER, ts0_replica->consensus()->role());
  ASSERT_OK(ts0_replica->consensus()->StartElection(
      RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE, RaftConsensus::EXTERNAL_REQUEST));

  // Wait until TS0 is leader.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(RaftPeerPB::LEADER, ts0_replica->consensus()->role());
  });

  // Now shut down TS1. This will ensure that TS0 cannot get re-elected.
  LOG(INFO) << "Shutting down ts1...";
  ts1_replica->Shutdown();

  // Start another election and wait for some time to see if it can get elected.
  ASSERT_OK(ts0_replica->consensus()->StepDown(&resp));
  ASSERT_OK(ts0_replica->consensus()->StartElection(
      RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE, RaftConsensus::EXTERNAL_REQUEST));

  // Wait for some time to ensure TS0 cannot get elected.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);
  while (MonoTime::Now() < deadline) {
    ASSERT_EQ(RaftPeerPB::FOLLOWER, ts0_replica->consensus()->role());
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
}

// Test that a tombstoned replica will vote correctly.
// This is implemented by directly exercising the RPC API with different vote request parameters.
TEST_F(TombstonedVotingIMCITest, TestVotingLogic) {
  // This test waits for several seconds, so only run it in slow mode.
  if (!AllowSlowTests()) return;

  FLAGS_allow_unsafe_replication_factor = true; // Allow an even replication factor.
  FLAGS_enable_tablet_copy = false; // Tablet copy would interfere with this test.

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 2));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2); // Two servers and replicas makes the test easy to debug.
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 50) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  workload.StopAndJoin();

  // Figure out the tablet id to mess with.
  vector<string> tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  const string& tablet_id = tablet_ids[0];

  // Ensure all servers are up to date.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, workload.batches_completed()));

  // Shut down TS0 so it doesn't interfere with our testing.
  cluster_->mini_tablet_server(0)->Shutdown();

  // Figure out the last logged opid of TS1.
  OpId last_logged_opid;
  ASSERT_OK(itest::GetLastOpIdForReplica(tablet_id,
                                         ts_map_[cluster_->mini_tablet_server(1)->uuid()],
                                         RECEIVED_OPID,
                                         kTimeout,
                                         &last_logged_opid));

  // Tombstone TS1 (actually, the tablet replica hosted on TS1).
  ASSERT_OK(DeleteTablet(ts_map_[cluster_->mini_tablet_server(1)->uuid()],
                         tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  // Loop this series of tests twice: the first time without restarting the TS,
  // the 2nd time after a restart.
  for (int i = 0; i < 2; i++) {
    if (i == 1) {
      // Restart tablet server #1 on the 2nd loop.
      LOG(INFO) << "Restarting TS1...";
      cluster_->mini_tablet_server(1)->Shutdown();
      ASSERT_OK(cluster_->mini_tablet_server(1)->Restart());
      ASSERT_OK(cluster_->mini_tablet_server(1)->WaitStarted());
    }

    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(1)->server()->tablet_manager()->GetTabletReplica(
        tablet_id, &replica));
    ASSERT_EQ(i == 0 ? tablet::STOPPED : tablet::INITIALIZED, replica->state());

    int64_t current_term = replica->consensus()->CurrentTerm();
    current_term++;

    // Ask TS1 for a vote that should be granted (new term, acceptable opid).
    // Note: peers are required to vote regardless of whether they recognize the
    // candidate's UUID or not, so the ID used here ("A") is not important.
    TServerDetails* ts1_ets = ts_map_[cluster_->mini_tablet_server(1)->uuid()];
    ASSERT_OK(itest::RequestVote(ts1_ets, tablet_id, "A", current_term, last_logged_opid,
                                /*ignore_live_leader=*/ true, /*is_pre_election=*/ false, kTimeout))

    // Ask TS1 for a vote that should be denied (different candidate, same term).
    Status s = itest::RequestVote(ts1_ets, tablet_id, "B", current_term, last_logged_opid,
                                  /*ignore_live_leader=*/ true, /*is_pre_election=*/ false,
                                  kTimeout);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Already voted for candidate A in this term");

    // Ask TS1 for a vote that should be denied (old term).
    s = itest::RequestVote(ts1_ets, tablet_id, "B", current_term - 1, last_logged_opid,
                          /*ignore_live_leader=*/ true, /*is_pre_election=*/ false, kTimeout);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_MATCHES(s.ToString(), "Denying vote to candidate B for earlier term");

    // Increment term.
    current_term++;
    OpId old_opid = MakeOpId(last_logged_opid.term(), last_logged_opid.index() - 1);

    // Ask TS1 for a vote that should be denied (old last-logged opid).
    s = itest::RequestVote(ts1_ets, tablet_id, "B", current_term, old_opid,
                          /*ignore_live_leader=*/ true, /*is_pre_election=*/ false, kTimeout);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_MATCHES(s.ToString(),
                      "Denying vote to candidate B.*greater than that of the candidate");

    // Ask for a successful vote for candidate B.
    ASSERT_OK(itest::RequestVote(ts1_ets, tablet_id, "B", current_term, last_logged_opid,
                                /*ignore_live_leader=*/ true, /*is_pre_election=*/ false, kTimeout))
  }
}

// Disable tombstoned voting and ensure that an election that would require it fails.
TEST_F(TombstonedVotingIMCITest, TestNoVoteIfTombstonedVotingDisabled) {
  // This test waits for several seconds, so only run it in slow mode.
  if (!AllowSlowTests()) return;

  FLAGS_raft_enable_tombstoned_voting = false; // Disable tombstoned voting.
  FLAGS_allow_unsafe_replication_factor = true; // Allow an even replication factor.
  FLAGS_enable_tablet_copy = false; // Tablet copy would interfere with this test.

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 2));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2); // Two servers and replicas makes the test easy to debug.
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 50) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  workload.StopAndJoin();

  // Figure out the tablet id to mess with.
  vector<string> tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  const string& tablet_id = tablet_ids[0];

  // Ensure all servers are up to date.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, workload.batches_completed()));

  // Tombstone TS1 and try to get TS0 to vote for it.
  TServerDetails* ts1 = ts_map_[cluster_->mini_tablet_server(1)->uuid()];
  ASSERT_OK(DeleteTablet(ts1, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  scoped_refptr<TabletReplica> ts0_replica;
  ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletReplica(
      tablet_id, &ts0_replica));
  LeaderStepDownResponsePB resp;
  ts0_replica->consensus()->StepDown(&resp); // Ignore result, in case TS1 was the leader.
  ASSERT_OK(ts0_replica->consensus()->StartElection(
      RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE, RaftConsensus::EXTERNAL_REQUEST));

  // Wait for some time to ensure TS0 cannot get elected.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);
  while (MonoTime::Now() < deadline) {
    ASSERT_EQ(RaftPeerPB::FOLLOWER, ts0_replica->consensus()->role());
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
}

// Test that a replica will not vote while tombstoned if it was deleted while
// the last-logged opid was unknown. This may occur if a tablet is tombstoned
// while in a FAILED state.
TEST_F(TombstonedVotingIMCITest, TestNoVoteIfNoLastLoggedOpId) {
  if (!AllowSlowTests()) return; // This test waits for several seconds.

  FLAGS_allow_unsafe_replication_factor = true; // Allow an even replication factor.

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 2));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2); // Two servers and replicas makes the test easy to debug.
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 50) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  tserver::MiniTabletServer* ts0 = cluster_->mini_tablet_server(0);
  string ts0_uuid = ts0->uuid();
  tserver::MiniTabletServer* ts1 = cluster_->mini_tablet_server(1);
  string ts1_uuid = ts0->uuid();

  // Determine the tablet id.
  vector<string> tablet_ids = ts0->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  const string& tablet_id = tablet_ids[0];

  // Ensure all servers are in sync.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, workload.batches_completed()));

  // Shut down each TS, then corrupt the TS0 cmeta.
  string ts0_cmeta_path = ts0->server()->fs_manager()->GetConsensusMetadataPath(tablet_id);
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster_->mini_tablet_server(i)->Shutdown();
  }

  std::unique_ptr<WritableFile> file;
  ASSERT_OK(env_->NewWritableFile(ts0_cmeta_path, &file));
  ASSERT_OK(file->Append("\0"));
  ASSERT_OK(file->Close());

  // Restart each TS so it comes back up on the same ports.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->mini_tablet_server(i)->Restart());
  }

  // Wait until the tablet is in FAILED state.
  ASSERT_OK(itest::WaitUntilTabletInState(ts_map_[ts0_uuid], tablet_id, TabletStatePB::FAILED,
                                          kTimeout));
  scoped_refptr<TabletReplica> replica;
  ASSERT_TRUE(ts0->server() != nullptr);
  ASSERT_TRUE(ts0->server()->tablet_manager() != nullptr);
  ASSERT_TRUE(ts0->server()->tablet_manager()->LookupTablet(tablet_id, &replica));
  ASSERT_EQ(tablet::FAILED, replica->state());

  // Now tombstone the failed replica on TS0.
  ASSERT_OK(DeleteTablet(ts_map_[ts0_uuid], tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  // Wait until TS1 is running.
  ASSERT_EVENTUALLY([&] {
    TSTabletManager* tablet_manager = ts1->server()->tablet_manager();
    ASSERT_TRUE(tablet_manager->LookupTablet(tablet_id, &replica));
    ASSERT_EQ(tablet::RUNNING, replica->state());
  });

  // Ensure that TS1 cannot become leader because TS0 will not vote.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);
  while (MonoTime::Now() < deadline) {
    scoped_refptr<TabletReplica> replica;
    TSTabletManager* tablet_manager = ts1->server()->tablet_manager();
    ASSERT_TRUE(tablet_manager != nullptr);
    ASSERT_TRUE(tablet_manager->LookupTablet(tablet_id, &replica));
    std::shared_ptr<RaftConsensus> consensus = replica->shared_consensus();
    if (consensus) {
      ASSERT_EQ(RaftPeerPB::FOLLOWER, consensus->role());
    }
  }
}

enum RestartAfterTombstone {
  kNoRestart,
  kRestart,
};

class TsRecoveryTombstonedIMCITest : public MiniClusterITestBase,
                                     public ::testing::WithParamInterface<RestartAfterTombstone> {
};

INSTANTIATE_TEST_CASE_P(Restart, TsRecoveryTombstonedIMCITest,
                        ::testing::Values(kNoRestart, kRestart));

// Basic tombstoned voting test.
TEST_P(TsRecoveryTombstonedIMCITest, TestTombstonedVoter) {
  const RestartAfterTombstone to_restart = GetParam();
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  FLAGS_allow_unsafe_replication_factor = true; // Allow an even replication factor.
  NO_FATALS(StartCluster(/*num_tablet_servers=*/ 2));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2); // Two servers and replicas makes the test easy to debug.
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 50) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  workload.StopAndJoin();

  // Figure out the tablet id to Tablet Copy.
  vector<string> tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  const string& tablet_id = tablet_ids[0];

  // Ensure all servers are up to date.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, workload.batches_completed()));

  auto live_ts_map = ts_map_;
  ASSERT_EQ(1, live_ts_map.erase(cluster_->mini_tablet_server(1)->uuid()));

  // Shut down TS 0 then tombstone TS 1. Restart TS 0.
  // TS 0 should get a vote from TS 1 and then make a copy on TS 1, bringing
  // the cluster back up to full strength.
  LOG(INFO) << "shutting down TS " << cluster_->mini_tablet_server(0)->uuid();
  cluster_->mini_tablet_server(0)->Shutdown();

  LOG(INFO) << "tombstoning replica on TS " << cluster_->mini_tablet_server(1)->uuid();
  TServerDetails* ts1 = ts_map_[cluster_->mini_tablet_server(1)->uuid()];
  ASSERT_OK(DeleteTablet(ts1, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  if (to_restart == kRestart) {
    LOG(INFO) << "restarting tombstoned TS " << cluster_->mini_tablet_server(1)->uuid();
    cluster_->mini_tablet_server(1)->Shutdown();
    ASSERT_OK(cluster_->mini_tablet_server(1)->Restart());
  }

  LOG(INFO) << "restarting TS " << cluster_->mini_tablet_server(1)->uuid();
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());

  // Wait for the tablet copy to complete.
  LOG(INFO) << "waiting for leader election and tablet copy to complete...";
  ASSERT_OK(WaitForServersToAgree(kTimeout, live_ts_map, tablet_id, workload.batches_completed()));

  LOG(INFO) << "attempting to write a few more rows...";

  // Write a little bit more.
  int target_rows = workload.rows_inserted() + 100;
  workload.Start();
  while (workload.rows_inserted() < target_rows) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  workload.StopAndJoin();

  // Do a final verification that the servers match.
  LOG(INFO) << "waiting for final agreement...";
  ASSERT_OK(WaitForServersToAgree(kTimeout, live_ts_map, tablet_id, workload.batches_completed()));
}

} // namespace kudu
