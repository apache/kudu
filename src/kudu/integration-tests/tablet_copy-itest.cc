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
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/util/barrier.h"
#include "kudu/util/metrics.h"

DEFINE_int32(test_num_threads, 16,
             "Number of test threads to launch");
DEFINE_int32(test_delete_leader_num_iters, 3,
             "Number of iterations to run in TestDeleteLeaderDuringTabletCopyStressTest.");
DEFINE_int32(test_delete_leader_min_rows_per_iter, 20,
             "Number of writer threads in TestDeleteLeaderDuringTabletCopyStressTest.");
DEFINE_int32(test_delete_leader_payload_bytes, 16 * 1024,
             "Payload byte size in TestDeleteLeaderDuringTabletCopyStressTest.");
DEFINE_int32(test_delete_leader_num_writer_threads, 1,
             "Number of writer threads in TestDeleteLeaderDuringTabletCopyStressTest.");

using kudu::client::KuduSchema;
using kudu::client::KuduSchemaFromSchema;
using kudu::client::KuduTableCreator;
using kudu::itest::TServerDetails;
using kudu::itest::WaitForNumTabletServers;
using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::ListTabletsResponsePB_StatusAndSchemaPB;
using kudu::tserver::TabletCopyClient;
using std::set;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_kudu_consensus_ConsensusService_UpdateConsensus);
METRIC_DECLARE_counter(glog_info_messages);
METRIC_DECLARE_counter(glog_warning_messages);
METRIC_DECLARE_counter(glog_error_messages);
METRIC_DECLARE_gauge_uint64(log_block_manager_blocks_under_management);

namespace kudu {

class TabletCopyITest : public ExternalMiniClusterITestBase {
};

// If a rogue (a.k.a. zombie) leader tries to replace a tombstoned
// tablet via Tablet Copy, make sure its term isn't older than the latest term
// we observed. If it is older, make sure we reject the request, to avoid allowing old
// leaders to create a parallel universe. This is possible because config
// change could cause nodes to move around. The term check is reasonable
// because only one node can be elected leader for a given term.
//
// A leader can "go rogue" due to a VM pause, CTRL-z, partition, etc.
TEST_F(TabletCopyITest, TestRejectRogueLeader) {
  // This test pauses for at least 10 seconds. Only run in slow-test mode.
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  const int kTsIndex = 0; // We'll test with the first TS.
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];

  TestWorkload workload(cluster_.get());
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader for term 1, then run some data through the cluster.
  int zombie_leader_index = 1;
  string zombie_leader_uuid = cluster_->tablet_server(zombie_leader_index)->uuid();
  ASSERT_OK(itest::StartElection(ts_map_[zombie_leader_uuid], tablet_id, timeout));
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));

  // Come out of the blue and try to initiate Tablet Copy from a running server while
  // specifying an old term. That running server should reject the request.
  // We are essentially masquerading as a rogue leader here.
  Status s = itest::StartTabletCopy(ts, tablet_id, zombie_leader_uuid,
                                         HostPort(cluster_->tablet_server(1)->bound_rpc_addr()),
                                         0, // Say I'm from term 0.
                                         timeout);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "term 0, which is lower than last-logged term 1");

  // Now pause the actual leader so we can bring him back as a zombie later.
  ASSERT_OK(cluster_->tablet_server(zombie_leader_index)->Pause());

  // Trigger TS 2 to become leader of term 2.
  int new_leader_index = 2;
  string new_leader_uuid = cluster_->tablet_server(new_leader_index)->uuid();
  ASSERT_OK(itest::StartElection(ts_map_[new_leader_uuid], tablet_id, timeout));
  ASSERT_OK(itest::WaitUntilLeader(ts_map_[new_leader_uuid], tablet_id, timeout));

  unordered_map<string, TServerDetails*> active_ts_map = ts_map_;
  ASSERT_EQ(1, active_ts_map.erase(zombie_leader_uuid));

  // Wait for the NO_OP entry from the term 2 election to propagate to the
  // remaining nodes' logs so that we are guaranteed to reject the rogue
  // leader's tablet copy request when we bring it back online.
  int log_index = workload.batches_completed() + 2; // 2 terms == 2 additional NO_OP entries.
  ASSERT_OK(WaitForServersToAgree(timeout, active_ts_map, tablet_id, log_index));
  // Write more rows to the new leader.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Now kill the new leader and tombstone the replica on TS 0.
  cluster_->tablet_server(new_leader_index)->Shutdown();
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));

  // Zombies!!! Resume the rogue zombie leader.
  // He should attempt to tablet copy TS 0 but fail.
  ASSERT_OK(cluster_->tablet_server(zombie_leader_index)->Resume());

  // Loop for a few seconds to ensure that the tablet doesn't transition to READY.
  MonoTime deadline = MonoTime::Now();
  deadline.AddDelta(MonoDelta::FromSeconds(5));
  while (MonoTime::Now() < deadline) {
    ASSERT_OK(itest::ListTablets(ts, timeout, &tablets));
    ASSERT_EQ(1, tablets.size());
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, tablets[0].tablet_status().tablet_data_state());
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Force the rogue leader to step down.
  // Then, send a tablet copy start request from a "fake" leader that
  // sends an up-to-date term in the RB request but the actual term stored
  // in the copy source's consensus metadata would still be old.
  LOG(INFO) << "Forcing rogue leader T " << tablet_id << " P " << zombie_leader_uuid
            << " to step down...";
  ASSERT_OK(itest::LeaderStepDown(ts_map_[zombie_leader_uuid], tablet_id, timeout));
  ExternalTabletServer* zombie_ets = cluster_->tablet_server(zombie_leader_index);
  // It's not necessarily part of the API but this could return faliure due to
  // rejecting the remote. We intend to make that part async though, so ignoring
  // this return value in this test.
  ignore_result(itest::StartTabletCopy(ts, tablet_id, zombie_leader_uuid,
                                            HostPort(zombie_ets->bound_rpc_addr()),
                                            2, // Say I'm from term 2.
                                            timeout));

  // Wait another few seconds to be sure the tablet copy is rejected.
  deadline = MonoTime::Now();
  deadline.AddDelta(MonoDelta::FromSeconds(5));
  while (MonoTime::Now() < deadline) {
    ASSERT_OK(itest::ListTablets(ts, timeout, &tablets));
    ASSERT_EQ(1, tablets.size());
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, tablets[0].tablet_status().tablet_data_state());
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
}

// Perform ListTablets on a peer while peer is going through tablet copy.
// This serves as a unit test for a tsan data race fix (KUDU-1500).
// This test is only optimistic in that, we hope to catch the
// state transition on the follower during the tablet-copy and thereby
// exercising the data races observed by tsan earlier.
//
// Step 1: Start the cluster and pump some workload.
// Step 2: Tombstone the tablet on a selected follower.
// Step 3: Create some threads which issue ListTablets to that follower
//         in a loop until the follower finishes tablet copy.
TEST_F(TabletCopyITest, TestListTabletsDuringTabletCopy) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kTsIndex = 1; // Pick a random TS.
  NO_FATALS(StartCluster());

  // Populate a tablet with some data.
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Ensure all the servers agree before we proceed.
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id,
                                  workload.batches_completed()));

  int leader_index;
  int follower_index;
  TServerDetails* leader_ts;
  TServerDetails* follower_ts;

  // Find out who is leader so that we can tombstone a follower
  // and let the tablet copy be kicked off by the leader.
  // We could have tombstoned the leader too and let the election ensue
  // followed by tablet copy of the tombstoned replica. But, we can keep
  // the test more focused this way by not triggering any election thereby
  // reducing overall test time as well.
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_ts));
  leader_index = cluster_->tablet_server_index_by_uuid(leader_ts->uuid());
  follower_index = (leader_index + 1) % cluster_->num_tablet_servers();
  follower_ts = ts_map_[cluster_->tablet_server(follower_index)->uuid()];

  vector<std::thread> threads;
  std::atomic<bool> finish(false);
  Barrier barrier(FLAGS_test_num_threads + 1); // include main thread

  // Threads to issue ListTablets RPCs to follower.
  for (int i = 0; i < FLAGS_test_num_threads; i++) {
    threads.emplace_back([&]() {
      barrier.Wait();
      do {
        vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
        CHECK_OK(ListTablets(ts, MonoDelta::FromSeconds(30), &tablets));
      } while (!finish.load(std::memory_order_relaxed));
    });
  }

  // Wait until all ListTablets RPC threads are fired up.
  barrier.Wait();

  // Tombstone the tablet on the follower.
  LOG(INFO) << "Tombstoning follower tablet " << tablet_id
            << " on TS " << follower_ts->uuid();
  ASSERT_OK(itest::DeleteTablet(follower_ts, tablet_id,
                                TABLET_DATA_TOMBSTONED,
                                boost::none, kTimeout));

  // A new good copy should automatically get created via tablet copy.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
      follower_index, tablet_id,
      { tablet::TABLET_DATA_READY },
      kTimeout));

  // Wait for all threads to finish.
  finish.store(true, std::memory_order_relaxed);
  for (auto& t : threads) {
    t.join();
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

// Start tablet copy session and delete the tablet in the middle.
// It should actually be possible to complete copying in such a case, because
// when a Tablet Copy session is started on the "source" server, all of
// the relevant files are either read or opened, meaning that an in-progress
// Tablet Copy can complete even after a tablet is officially "deleted" on
// the source server. This is also a regression test for KUDU-1009.
TEST_F(TabletCopyITest, TestDeleteTabletDuringTabletCopy) {
  MonoDelta timeout = MonoDelta::FromSeconds(10);
  const int kTsIndex = 0; // We'll test with the first TS.
  NO_FATALS(StartCluster());

  // Populate a tablet with some data.
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Ensure all the servers agree before we proceed.
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));

  // Set up an FsManager to use with the TabletCopyClient.
  FsManagerOpts opts;
  string testbase = GetTestPath("fake-ts");
  ASSERT_OK(env_->CreateDir(testbase));
  opts.wal_path = JoinPathSegments(testbase, "wals");
  opts.data_paths.push_back(JoinPathSegments(testbase, "data-0"));
  gscoped_ptr<FsManager> fs_manager(new FsManager(env_, opts));
  ASSERT_OK(fs_manager->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager->Open());

  {
    // Start up a TabletCopyClient and open a tablet copy session.
    TabletCopyClient tc_client(tablet_id, fs_manager.get(),
                                    cluster_->messenger());
    scoped_refptr<tablet::TabletMetadata> meta;
    ASSERT_OK(tc_client.Start(cluster_->tablet_server(kTsIndex)->bound_rpc_hostport(),
                              &meta));

    // Tombstone the tablet on the remote!
    ASSERT_OK(itest::DeleteTablet(ts, tablet_id,
                                  TABLET_DATA_TOMBSTONED, boost::none, timeout));

    // Now finish copying!
    ASSERT_OK(tc_client.FetchAll(nullptr /* no listener */));
    ASSERT_OK(tc_client.Finish());

    // Run destructor, which closes the remote session.
  }

  SleepFor(MonoDelta::FromMilliseconds(50));  // Give a little time for a crash (KUDU-1009).
  ASSERT_TRUE(cluster_->tablet_server(kTsIndex)->IsProcessAlive());
}

// This test ensures that a leader can Tablet Copy on top of a tombstoned replica
// that has a higher term recorded in the replica's consensus metadata if the
// replica's last-logged opid has the same term (or less) as the leader serving
// as the tablet copy source. When a tablet is tombstoned, its last-logged
// opid is stored in a field its on-disk superblock.
TEST_F(TabletCopyITest, TestTabletCopyFollowerWithHigherTerm) {
  vector<string> ts_flags = { "--enable_leader_failure_detection=false" };
  vector<string> master_flags = {
      "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
      "--allow_unsafe_replication_factor=true"
  };
  const int kNumTabletServers = 2;
  NO_FATALS(StartCluster(ts_flags, master_flags, kNumTabletServers));

  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  const int kFollowerIndex = 0;
  TServerDetails* follower_ts = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2);
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(follower_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader for term 1, then run some data through the cluster.
  const int kLeaderIndex = 1;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  ASSERT_OK(itest::StartElection(leader_ts, tablet_id, timeout));
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));

  // Pause the leader and increment the term on the follower by starting an
  // election on the follower. The election will fail asynchronously but we
  // just wait until we see that its term has incremented.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Pause());
  ASSERT_OK(itest::StartElection(follower_ts, tablet_id, timeout));
  int64_t term = 0;
  for (int i = 0; i < 1000; i++) {
    consensus::ConsensusStatePB cstate;
    ASSERT_OK(itest::GetConsensusState(follower_ts, tablet_id, timeout, &cstate));
    term = cstate.current_term();
    if (term == 2) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_EQ(2, term);

  // Now tombstone the follower.
  ASSERT_OK(itest::DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none,
                                timeout));

  // Restart the follower's TS so that the leader's TS won't get its queued
  // vote request messages. This is a hack but seems to work.
  cluster_->tablet_server(kFollowerIndex)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kFollowerIndex)->Restart());

  // Now wake the leader. It should detect that the follower needs to be
  // copied and proceed to bring it back up to date.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Resume());

  // Wait for the follower to come back up.
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));
}

// Test that multiple concurrent tablet copys do not cause problems.
// This is a regression test for KUDU-951, in which concurrent sessions on
// multiple tablets between the same tablet copy client host and source host
// could corrupt each other.
TEST_F(TabletCopyITest, TestConcurrentTabletCopys) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  ts_flags.push_back("--log_cache_size_limit_mb=1");
  ts_flags.push_back("--log_segment_size_mb=1");
  ts_flags.push_back("--log_async_preallocate_segments=false");
  ts_flags.push_back("--log_min_segments_to_retain=100");
  ts_flags.push_back("--flush_threshold_mb=0"); // Constantly flush.
  ts_flags.push_back("--maintenance_manager_polling_interval_ms=10");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  const MonoDelta timeout = MonoDelta::FromSeconds(60);

  // Create a table with several tablets. These will all be simultaneously
  // copied to a single target node from the same leader host.
  const int kNumTablets = 10;
  KuduSchema client_schema(KuduSchemaFromSchema(GetSimpleTestSchema()));
  vector<const KuduPartialRow*> splits;
  for (int i = 0; i < kNumTablets - 1; i++) {
    KuduPartialRow* row = client_schema.NewRow();
    ASSERT_OK(row->SetInt32(0, numeric_limits<int32_t>::max() / kNumTablets * (i + 1)));
    splits.push_back(row);
  }
  gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(TestWorkload::kDefaultTableName)
                          .split_rows(splits)
                          .schema(&client_schema)
                          .set_range_partition_columns({ "key" })
                          .num_replicas(3)
                          .Create());

  const int kTsIndex = 0; // We'll test with the first TS.
  TServerDetails* target_ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];

  // Figure out the tablet ids of the created tablets.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(target_ts, kNumTablets, timeout, &tablets));

  vector<string> tablet_ids;
  for (const ListTabletsResponsePB::StatusAndSchemaPB& t : tablets) {
    tablet_ids.push_back(t.tablet_status().tablet_id());
  }

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    for (const string& tablet_id : tablet_ids) {
      ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                              tablet_id, timeout));
    }
  }

  // Elect leaders on each tablet for term 1. All leaders will be on TS 1.
  const int kLeaderIndex = 1;
  const string kLeaderUuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  for (const string& tablet_id : tablet_ids) {
    ASSERT_OK(itest::StartElection(ts_map_[kLeaderUuid], tablet_id, timeout));
  }

  TestWorkload workload(cluster_.get());
  workload.set_write_timeout_millis(10000);
  workload.set_timeout_allowed(true);
  workload.set_write_batch_size(10);
  workload.set_num_write_threads(10);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 20000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  for (const string& tablet_id : tablet_ids) {
    ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1));
  }

  // Now pause the leader so we can tombstone the tablets.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Pause());

  for (const string& tablet_id : tablet_ids) {
    LOG(INFO) << "Tombstoning tablet " << tablet_id << " on TS " << target_ts->uuid();
    ASSERT_OK(itest::DeleteTablet(target_ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none,
                                  MonoDelta::FromSeconds(10)));
  }

  // Unpause the leader TS and wait for it to initiate Tablet Copy and replace the tombstoned
  // tablets, in parallel.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Resume());
  for (const string& tablet_id : tablet_ids) {
    ASSERT_OK(itest::WaitUntilTabletRunning(target_ts, tablet_id, timeout));
  }

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(), ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));
}

// Test that repeatedly runs a load, tombstones a follower, then tombstones the
// leader while the follower is copying. Regression test for
// KUDU-1047.
TEST_F(TabletCopyITest, TestDeleteLeaderDuringTabletCopyStressTest) {
  // This test takes a while due to failure detection.
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  const MonoDelta timeout = MonoDelta::FromSeconds(60);
  NO_FATALS(StartCluster(vector<string>(), vector<string>(), 5));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(5);
  workload.set_payload_bytes(FLAGS_test_delete_leader_payload_bytes);
  workload.set_num_write_threads(FLAGS_test_delete_leader_num_writer_threads);
  workload.set_write_batch_size(1);
  workload.set_write_timeout_millis(10000);
  workload.set_timeout_allowed(true);
  workload.set_not_found_allowed(true);
  workload.Setup();

  // Figure out the tablet id.
  const int kTsIndex = 0;
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  int leader_index = -1;
  int follower_index = -1;
  TServerDetails* leader_ts = nullptr;
  TServerDetails* follower_ts = nullptr;

  for (int i = 0; i < FLAGS_test_delete_leader_num_iters; i++) {
    LOG(INFO) << "Iteration " << (i + 1);
    int rows_previously_inserted = workload.rows_inserted();

    // Find out who's leader.
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, timeout, &leader_ts));
    leader_index = cluster_->tablet_server_index_by_uuid(leader_ts->uuid());

    // Select an arbitrary follower.
    follower_index = (leader_index + 1) % cluster_->num_tablet_servers();
    follower_ts = ts_map_[cluster_->tablet_server(follower_index)->uuid()];

    // Spin up the workload.
    workload.Start();
    while (workload.rows_inserted() < rows_previously_inserted +
                                      FLAGS_test_delete_leader_min_rows_per_iter) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    // Tombstone the follower.
    LOG(INFO) << "Tombstoning follower tablet " << tablet_id << " on TS " << follower_ts->uuid();
    ASSERT_OK(itest::DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none,
                                  timeout));

    // Wait for tablet copy to start.
    ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
        follower_index, tablet_id,
        { tablet::TABLET_DATA_COPYING, tablet::TABLET_DATA_READY },
        timeout));

    // Tombstone the leader.
    LOG(INFO) << "Tombstoning leader tablet " << tablet_id << " on TS " << leader_ts->uuid();
    ASSERT_OK(itest::DeleteTablet(leader_ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none,
                                  timeout));

    // Quiesce and rebuild to full strength. This involves electing a new
    // leader from the remaining three, which requires a unanimous vote, and
    // that leader then copying the old leader.
    workload.StopAndJoin();
    ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1));
  }

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(), ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));
}

namespace {
int64_t CountBlocksUnderManagement(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(ets->GetInt64Metric(
               &METRIC_ENTITY_server,
               "kudu.tabletserver",
               &METRIC_log_block_manager_blocks_under_management,
               "value",
               &ret));
  return ret;
}
int64_t CountUpdateConsensusCalls(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(ets->GetInt64Metric(
               &METRIC_ENTITY_server,
               "kudu.tabletserver",
               &METRIC_handler_latency_kudu_consensus_ConsensusService_UpdateConsensus,
               "total_count",
               &ret));
  return ret;
}
int64_t CountLogMessages(ExternalTabletServer* ets) {
  int64_t total = 0;

  int64_t count;
  CHECK_OK(ets->GetInt64Metric(
               &METRIC_ENTITY_server,
               "kudu.tabletserver",
               &METRIC_glog_info_messages,
               "value",
               &count));
  total += count;

  CHECK_OK(ets->GetInt64Metric(
               &METRIC_ENTITY_server,
               "kudu.tabletserver",
               &METRIC_glog_warning_messages,
               "value",
               &count));
  total += count;

  CHECK_OK(ets->GetInt64Metric(
               &METRIC_ENTITY_server,
               "kudu.tabletserver",
               &METRIC_glog_error_messages,
               "value",
               &count));
  total += count;

  return total;
}
} // anonymous namespace

// Test that if tablet copy is disabled by a flag, we don't get into
// tight loops after a tablet is deleted. This is a regression test for situation
// similar to the bug described in KUDU-821: we were previously handling a missing
// tablet within consensus in such a way that we'd immediately send another RPC.
TEST_F(TabletCopyITest, TestDisableTabletCopy_NoTightLoopWhenTabletDeleted) {
  MonoDelta timeout = MonoDelta::FromSeconds(10);
  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  ts_flags.push_back("--enable_tablet_copy=false");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ExternalTabletServer* replica_ets = cluster_->tablet_server(1);
  TServerDetails* replica_ts = ts_map_[replica_ets->uuid()];
  ASSERT_OK(WaitForNumTabletsOnTS(replica_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ExternalTabletServer* leader_ts = cluster_->tablet_server(0);
  ASSERT_OK(itest::StartElection(ts_map_[leader_ts->uuid()], tablet_id, timeout));

  // Start writing, wait for some rows to be inserted.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Tombstone the tablet on one of the servers (TS 1)
  ASSERT_OK(itest::DeleteTablet(replica_ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none,
                                timeout));

  // Ensure that, if we sleep for a second while still doing writes to the leader:
  // a) we don't spew logs on the leader side
  // b) we don't get hit with a lot of UpdateConsensus calls on the replica.
  MonoTime start = MonoTime::Now();
  int64_t num_update_rpcs_initial = CountUpdateConsensusCalls(replica_ets);
  int64_t num_logs_initial = CountLogMessages(leader_ts);

  SleepFor(MonoDelta::FromSeconds(1));
  int64_t num_update_rpcs_after_sleep = CountUpdateConsensusCalls(replica_ets);
  int64_t num_logs_after_sleep = CountLogMessages(leader_ts);
  MonoTime end = MonoTime::Now();
  MonoDelta elapsed = end - start;

  // Calculate rate per second of RPCs and log messages
  int64_t update_rpcs_per_second =
      (num_update_rpcs_after_sleep - num_update_rpcs_initial) / elapsed.ToSeconds();
  EXPECT_LT(update_rpcs_per_second, 20);
  double num_logs_per_second =
      (num_logs_after_sleep - num_logs_initial) / elapsed.ToSeconds();
  EXPECT_LT(num_logs_per_second, 60); // We might occasionally get unrelated log messages.
}

// Test that if a Tablet Copy is taking a long time but the client peer is still responsive,
// the leader won't mark it as failed.
TEST_F(TabletCopyITest, TestSlowCopyDoesntFail) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  ts_flags.push_back("--tablet_copy_dowload_file_inject_latency_ms=5000");
  ts_flags.push_back("--follower_unavailable_considered_failed_sec=2");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ExternalTabletServer* replica_ets = cluster_->tablet_server(1);
  TServerDetails* replica_ts = ts_map_[replica_ets->uuid()];
  ASSERT_OK(WaitForNumTabletsOnTS(replica_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ExternalTabletServer* leader_ts = cluster_->tablet_server(0);
  ASSERT_OK(itest::StartElection(ts_map_[leader_ts->uuid()], tablet_id, timeout));

  // Start writing, wait for some rows to be inserted.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }


  // Tombstone the follower.
  LOG(INFO) << "Tombstoning follower tablet " << tablet_id << " on TS " << replica_ts->uuid();
  ASSERT_OK(itest::DeleteTablet(replica_ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none,
                                timeout));

  // Wait for tablet copy to start.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(1, tablet_id,
                                                 { tablet::TABLET_DATA_COPYING }, timeout));

  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1));

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(), ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));
}

// Attempting to start Tablet Copy on a tablet that was deleted with
// TABLET_DATA_DELETED should fail. This behavior helps avoid thrashing when
// a follower tablet is deleted and the leader notices before it has processed
// its own DeleteTablet RPC, thinking that it needs to bring its follower back.
TEST_F(TabletCopyITest, TestTabletCopyingDeletedTabletFails) {
  // Delete the leader with TABLET_DATA_DELETED.
  // Attempt to manually copy a replica to the leader from a follower.
  // Should get an error saying it's illegal.

  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  NO_FATALS(StartCluster({"--enable_leader_failure_detection=false"},
                         {"--catalog_manager_wait_for_new_tablets_to_elect_leader=false"}));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(3);
  workload.Setup();

  TServerDetails* leader = ts_map_[cluster_->tablet_server(0)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(leader, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, kTimeout));
  }

  // Elect a leader for term 1, then run some data through the cluster.
  ASSERT_OK(itest::StartElection(leader, tablet_id, kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));

  // Now delete the leader with TABLET_DATA_DELETED. We should not be able to
  // bring back the leader after that until restarting the process.
  ASSERT_OK(itest::DeleteTablet(leader, tablet_id, TABLET_DATA_DELETED, boost::none, kTimeout));

  Status s = itest::StartTabletCopy(leader, tablet_id,
                                         cluster_->tablet_server(1)->uuid(),
                                         HostPort(cluster_->tablet_server(1)->bound_rpc_addr()),
                                         1, // We are in term 1.
                                         kTimeout);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Cannot transition from state TABLET_DATA_DELETED");

  // Restart the server so that it won't remember the tablet was permanently
  // deleted and we can tablet copy the server again.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  ASSERT_OK(itest::StartTabletCopy(leader, tablet_id,
                                        cluster_->tablet_server(1)->uuid(),
                                        HostPort(cluster_->tablet_server(1)->bound_rpc_addr()),
                                        1, // We are in term 1.
                                        kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));
}

// Test that the tablet copy thread pool being full results in throttling and
// backpressure on the callers.
TEST_F(TabletCopyITest, TestTabletCopyThrottling) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumTablets = 4;
  // We want 2 tablet servers and we don't want the master to interfere when we
  // forcibly make copies of tablets onto servers it doesn't know about.
  // We also want to make sure only one tablet copy is possible at a given time
  // in order to test the throttling.
  NO_FATALS(StartCluster({"--num_tablets_to_copy_simultaneously=1"},
                         {"--master_tombstone_evicted_tablet_replicas=false"},
                         2));
  // Shut down the 2nd tablet server; we'll create tablets on the first one.
  cluster_->tablet_server(1)->Shutdown();

  // Restart the Master so it doesn't try to assign tablets to the dead TS.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(cluster_->WaitForTabletServerCount(1, kTimeout));

  // Write a bunch of data to the first tablet server.
  TestWorkload workload(cluster_.get());
  workload.set_num_write_threads(8);
  workload.set_num_replicas(1);
  workload.set_num_tablets(kNumTablets);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  TServerDetails* ts0 = ts_map_[cluster_->tablet_server(0)->uuid()];
  TServerDetails* ts1 = ts_map_[cluster_->tablet_server(1)->uuid()];

  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablet_states;
  ASSERT_OK(WaitForNumTabletsOnTS(ts0, kNumTablets, kTimeout, &tablet_states));

  // Gather the set of tablet IDs.
  set<string> tablets;
  for (const auto& state : tablet_states) {
    tablets.insert(state.tablet_status().tablet_id());
  }
  ASSERT_EQ(4, tablets.size());

  workload.StopAndJoin();

  // Now we attempt to copy all of that data over to the 2nd tablet server.
  // We will attempt to copy 4 tablets simultanously, but because we have tuned
  // the number of tablet copy threads down to 1, we should get at least one
  // ServiceUnavailable error.
  ASSERT_OK(cluster_->tablet_server(1)->Restart());

  HostPort ts0_hostport;
  ASSERT_OK(HostPortFromPB(ts0->registration.rpc_addresses(0), &ts0_hostport));

  // Attempt to copy all of the tablets from TS0 to TS1 in parallel. Tablet
  // copies are repeated periodically until complete.
  int num_service_unavailable = 0;
  int num_inprogress = 0;
  while (!tablets.empty()) {
    for (auto tablet_id = tablets.begin(); tablet_id != tablets.end(); ) {
      tserver::TabletServerErrorPB::Code error_code = tserver::TabletServerErrorPB::UNKNOWN_ERROR;
      Status s = StartTabletCopy(ts1, *tablet_id, ts0->uuid(),
                                 ts0_hostport, 0, kTimeout, &error_code);
      if (!s.ok()) {
        switch (error_code) {
          case tserver::TabletServerErrorPB::THROTTLED:
            // The tablet copy threadpool is full.
            ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
            ++num_service_unavailable;
            break;
          case tserver::TabletServerErrorPB::ALREADY_INPROGRESS:
            // The tablet is already being copied
            ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
            ++num_inprogress;
            break;
          case tserver::TabletServerErrorPB::INVALID_CONFIG:
            // The tablet copy has already completed, and the remote tablet now
            // has the correct term (not 0).
            ASSERT_STR_MATCHES(s.ToString(),
                               "Leader has replica of tablet .* with term 0, "
                               "which is lower than last-logged term .* on local replica. "
                               "Rejecting tablet copy request");
            tablet_id = tablets.erase(tablet_id);
            continue;
          default:
            FAIL() << "Unexpected tablet copy failure: " << s.ToString()
                  << ": " << tserver::TabletServerErrorPB::Code_Name(error_code);
        }
      }
      ++tablet_id;
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  // Ensure that we get at least one service unavailable and copy in progress responses.
  ASSERT_GT(num_service_unavailable, 0);
  ASSERT_GT(num_inprogress, 0);
  LOG(INFO) << "Number of Service unavailable responses: " << num_service_unavailable;
  LOG(INFO) << "Number of in progress responses: " << num_inprogress;
}

// This test uses CountBlocksUnderManagement() which only works with the
// LogBlockManager.
#ifndef __APPLE__

class BadTabletCopyITest : public TabletCopyITest,
                           public ::testing::WithParamInterface<const char*> {
 protected:
  // Helper function to load a table with the specified amount of data and
  // ensure the specified number of blocks are persisted on one of the replicas.
  void LoadTable(TestWorkload* workload, int min_rows, int min_blocks);
};

// Tablet copy should either trigger a crash or a session timeout on the source
// server.
const char* kFlagFaultOnFetch = "fault_crash_on_handle_tc_fetch_data";
const char* kFlagEarlyTimeout = "tablet_copy_early_session_timeout_prob";
const char* tablet_copy_failure_flags[] = { kFlagFaultOnFetch, kFlagEarlyTimeout };
INSTANTIATE_TEST_CASE_P(FaultFlags, BadTabletCopyITest,
                        ::testing::ValuesIn(tablet_copy_failure_flags));

void BadTabletCopyITest::LoadTable(TestWorkload* workload, int min_rows, int min_blocks) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  // Always include TS 1 and check its blocks. This is always included.
  const int kReplicaIdx = 1;

  // Find the replicas for the table.
  string tablet_id;
  itest::TabletServerMap replicas;
  for (const auto& entry : ts_map_) {
    TServerDetails* ts = entry.second;
    vector<ListTabletsResponsePB_StatusAndSchemaPB> tablets;
    Status s = ListTablets(ts, kTimeout, &tablets);
    if (!s.ok()) {
      continue;
    }
    for (const auto& tablet : tablets) {
      if (tablet.tablet_status().table_name() == workload->table_name()) {
        replicas.insert(entry);
        tablet_id = tablet.tablet_status().tablet_id();
      }
    }
  }
  ASSERT_EQ(3, replicas.size());
  ASSERT_TRUE(ContainsKey(replicas, cluster_->tablet_server(kReplicaIdx)->uuid()));

  int64_t before_blocks = CountBlocksUnderManagement(cluster_->tablet_server(kReplicaIdx));
  int64_t after_blocks;
  int64_t blocks_diff;
  workload->Start();
  do {
    after_blocks = CountBlocksUnderManagement(cluster_->tablet_server(kReplicaIdx));
    blocks_diff = after_blocks - before_blocks;
    KLOG_EVERY_N_SECS(INFO, 1) << "Blocks diff: " << blocks_diff;
    SleepFor(MonoDelta::FromMilliseconds(10));
  } while (workload->rows_inserted() < min_rows || blocks_diff < min_blocks);
  workload->StopAndJoin();

  // Ensure all 3 replicas have replicated the same data.
  ASSERT_OK(WaitForServersToAgree(kTimeout, replicas, tablet_id, 0));
}

// Ensure that a tablet copy failure results in no orphaned blocks and no data loss.
TEST_P(BadTabletCopyITest, TestBadCopy) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "Not running " << CURRENT_TEST_NAME() << " because it is a slow test.";
    return;
  }

  // Load 2 tablets with 3 replicas each across 3 tablet servers s.t. we end up
  // with a replication distribution like: ([A], [A,B], [A,B], [B]).
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumTabletServers = 4;
  const int kMinRows = 10000;
  const int kMinBlocks = 10;
  const string& failure_flag = GetParam();
  StartCluster(
      {
        // Flush aggressively to create many blocks.
        "--flush_threshold_mb=0",
        "--maintenance_manager_polling_interval_ms=10",
        // Wait only 10 seconds before evicting a failed replica.
        "--follower_unavailable_considered_failed_sec=10",
        Substitute("--$0=1.0", failure_flag),
      }, {
        // Wait only 5 seconds before master decides TS not eligible for a replica.
        "--tserver_unresponsive_timeout_ms=5000",
      }, kNumTabletServers);

  // Don't allow TS 3 to get a copy of Table A.
  cluster_->tablet_server(3)->Shutdown();
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(WaitForNumTabletServers(cluster_->master_proxy(), 3, kTimeout));

  // Create Table A.
  TestWorkload workload1(cluster_.get());
  const char* kTableA = "table_a";
  workload1.set_table_name(kTableA);
  workload1.Setup();
  ASSERT_OK(cluster_->tablet_server(3)->Restart());

  // Load Table A.
  LoadTable(&workload1, kMinRows, kMinBlocks);

  // Don't allow TS 0 to get a copy of Table B.
  cluster_->tablet_server(0)->Shutdown();
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(WaitForNumTabletServers(cluster_->master_proxy(), 3, kTimeout));

  // Create Table B.
  TestWorkload workload2(cluster_.get());
  const char* kTableB = "table_b";
  workload2.set_table_name(kTableB);
  workload2.Setup();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  // Load Table B.
  LoadTable(&workload2, kMinRows, kMinBlocks);

  // Shut down replica with only [A].
  cluster_->tablet_server(0)->Shutdown();

  // The leader of A will evict TS 0 and the master will trigger re-replication to TS 3.

  if (failure_flag == kFlagFaultOnFetch) {
    // Tablet copy will cause the leader tablet server for A (either TS 1 or TS 2)
    // to crash because of --fault_crash_on_handle_tc_fetch_data=1.0
    // This will also cause the tablet copy session to fail and the new replica
    // on TS 3 to be tombstoned.
    bool crashed = false;
    while (!crashed) {
      for (int i : {1, 2}) {
        Status s = cluster_->tablet_server(i)
            ->WaitForInjectedCrash(MonoDelta::FromMilliseconds(10));
        if (s.ok()) {
          crashed = true;
          break;
        }
      }
    }
  } else {
    // The early timeout flag will also cause the tablet copy to fail, but
    // without crashing the source TS.
    ASSERT_EQ(kFlagEarlyTimeout, failure_flag);
  }

  // Wait for TS 3 to tombstone the replica that failed to copy.
  TServerDetails* ts = ts_map_[cluster_->tablet_server(3)->uuid()];
  AssertEventually([&] {
    vector<tserver::ListTabletsResponsePB_StatusAndSchemaPB> tablets;
    ASSERT_OK(ListTablets(ts, MonoDelta::FromSeconds(10), &tablets));
    ASSERT_EQ(2, tablets.size());
    int num_tombstoned = 0;
    for (const auto& t : tablets) {
      if (t.tablet_status().tablet_data_state() == TABLET_DATA_TOMBSTONED) {
        num_tombstoned++;
      }
    }
    ASSERT_EQ(1, num_tombstoned);
  });
  NO_FATALS();

  // Restart the whole cluster without failure injection so that it can finish
  // re-replicating.
  cluster_->Shutdown();
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster_->tablet_server(i)->mutable_flags()
        ->push_back(Substitute("--$0=0.0", failure_flag));
  }
  ASSERT_OK(cluster_->Restart());

  // Run ksck, ensure no data loss.
  LOG(INFO) << "Running ksck...";
  ClusterVerifier v(cluster_.get());
  v.SetVerificationTimeout(kTimeout);
  NO_FATALS(v.CheckCluster());

  LOG(INFO) << "Checking row count...";
  for (TestWorkload* workload : {&workload1, &workload2}) {
    NO_FATALS(v.CheckRowCount(workload->table_name(), ClusterVerifier::AT_LEAST,
                              workload->rows_inserted()));
  }
}

#endif // __APPLE__

} // namespace kudu
