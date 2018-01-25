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
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/barrier.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_int32(tablets_num_running);
METRIC_DECLARE_gauge_int32(tablets_num_bootstrapping);

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
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::COMMITTED_OPID;
using kudu::consensus::ConsensusMetadataManager;
using kudu::consensus::ConsensusMetadataPB;
using kudu::consensus::MakeOpId;
using kudu::consensus::RaftPeerPB;
using kudu::itest::AddServer;
using kudu::itest::DeleteTablet;
using kudu::itest::GetInt64Metric;
using kudu::itest::FindTabletLeader;
using kudu::itest::StartElection;
using kudu::itest::StartTabletCopy;
using kudu::itest::TServerDetails;
using kudu::itest::WaitForNumTabletServers;
using kudu::itest::WaitUntilTabletRunning;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_READY;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletDataState;
using kudu::tablet::TabletSuperBlockPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::ListTabletsResponsePB_StatusAndSchemaPB;
using kudu::tserver::TabletCopyClient;
using std::atomic;
using std::lock_guard;
using std::mutex;
using std::set;
using std::string;
using std::thread;
using std::unordered_map;
using std::vector;
using strings::Substitute;

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_histogram(handler_latency_kudu_consensus_ConsensusService_UpdateConsensus);
METRIC_DECLARE_counter(glog_info_messages);
METRIC_DECLARE_counter(glog_warning_messages);
METRIC_DECLARE_counter(glog_error_messages);
METRIC_DECLARE_counter(rows_inserted);
METRIC_DECLARE_counter(tablet_copy_bytes_fetched);
METRIC_DECLARE_counter(tablet_copy_bytes_sent);
METRIC_DECLARE_gauge_int32(tablet_copy_open_client_sessions);
METRIC_DECLARE_gauge_int32(tablet_copy_open_source_sessions);
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
  ts_flags.emplace_back("--enable_leader_failure_detection=false");
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  const int kTsIndex = 0; // We'll test with the first TS.
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];

  TestWorkload workload(cluster_.get());
  // We need to allow timeouts because the client may maintain a connection
  // with the rogue leader, which will result in a timeout.
  workload.set_write_timeout_millis(5000);
  workload.set_timeout_allowed(true);
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  LOG(INFO) << "loading data...";

  // Elect a leader for term 1, then run some data through the cluster.
  int zombie_leader_index = 1;
  string zombie_leader_uuid = cluster_->tablet_server(zombie_leader_index)->uuid();
  ASSERT_OK(StartElection(ts_map_[zombie_leader_uuid], tablet_id, timeout));
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.rows_inserted(), 100);
  });
  workload.StopAndJoin();

  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));

  // Come out of the blue and try to initiate Tablet Copy from a running server while
  // specifying an old term. That running server should reject the request.
  // We are essentially masquerading as a rogue leader here.
  Status s = StartTabletCopy(ts, tablet_id, zombie_leader_uuid,
                             HostPort(cluster_->tablet_server(1)->bound_rpc_addr()),
                             /*caller_term=*/ 0, // Say I'm from term 0.
                             timeout);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "term 0, which is lower than last-logged term 1");

  // Now pause the actual leader so we can bring him back as a zombie later.
  ASSERT_OK(cluster_->tablet_server(zombie_leader_index)->Pause());

  // Trigger TS 2 to become leader of term 2.
  int new_leader_index = 2;
  string new_leader_uuid = cluster_->tablet_server(new_leader_index)->uuid();
  ASSERT_OK(StartElection(ts_map_[new_leader_uuid], tablet_id, timeout));
  ASSERT_OK(itest::WaitUntilLeader(ts_map_[new_leader_uuid], tablet_id, timeout));
  LOG(INFO) << "successfully elected new leader";

  unordered_map<string, TServerDetails*> active_ts_map = ts_map_;
  ASSERT_EQ(1, active_ts_map.erase(zombie_leader_uuid));

  // Wait for the NO_OP entry from the term 2 election to propagate to the
  // remaining nodes' logs so that we are guaranteed to reject the rogue
  // leader's tablet copy request when we bring it back online.
  int log_index = workload.batches_completed() + 2; // 2 terms == 2 additional NO_OP entries.
  ASSERT_OK(WaitForServersToAgree(timeout, active_ts_map, tablet_id, log_index));

  // Write more rows to the new leader.
  LOG(INFO) << "restarting workload...";
  int64_t prev_inserted = workload.rows_inserted();
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.rows_inserted(), prev_inserted + 100);
  });
  workload.StopAndJoin();

  // Now kill the new leader and tombstone the replica on TS 0.
  LOG(INFO) << "shutting down new leader" << new_leader_uuid << "...";
  cluster_->tablet_server(new_leader_index)->Shutdown();

  LOG(INFO) << "tombstoning original follower...";
  ASSERT_OK(DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

  // Zombies!!! Resume the rogue zombie leader.
  // He should attempt to tablet copy TS 0 but fail.
  LOG(INFO) << "unpausing old (rogue) leader " << zombie_leader_uuid << "...";
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

  LOG(INFO) << "the rogue leader was not able to tablet copy the tombstoned follower";

  // Force the rogue leader to step down.
  // Then, send a tablet copy start request from a "fake" leader that
  // sends an up-to-date term in the RB request but the actual term stored
  // in the copy source's consensus metadata would still be old.
  LOG(INFO) << "forcing rogue leader T " << tablet_id << " P " << zombie_leader_uuid
            << " to step down...";
  ASSERT_OK(itest::LeaderStepDown(ts_map_[zombie_leader_uuid], tablet_id, timeout));
  ExternalTabletServer* zombie_ets = cluster_->tablet_server(zombie_leader_index);
  // It's not necessarily part of the API but this could return faliure due to
  // rejecting the remote. We intend to make that part async though, so ignoring
  // this return value in this test.
  ignore_result(StartTabletCopy(ts, tablet_id, zombie_leader_uuid,
                                HostPort(zombie_ets->bound_rpc_addr()),
                                /* caller_term=*/ 2, // Say I'm from term 2.
                                timeout));

  // Wait another few seconds to be sure the tablet copy is rejected.
  deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);
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
  ASSERT_OK(DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

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
  opts.wal_root = JoinPathSegments(testbase, "wals");
  opts.data_roots.push_back(JoinPathSegments(testbase, "data-0"));
  gscoped_ptr<FsManager> fs_manager(new FsManager(env_, opts));
  ASSERT_OK(fs_manager->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager->Open());
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(
      new ConsensusMetadataManager(fs_manager.get()));

  {
    // Start up a TabletCopyClient and open a tablet copy session.
    TabletCopyClient tc_client(tablet_id, fs_manager.get(),
                               cmeta_manager, cluster_->messenger(),
                               nullptr /* no metrics */);
    scoped_refptr<tablet::TabletMetadata> meta;
    ASSERT_OK(tc_client.Start(cluster_->tablet_server(kTsIndex)->bound_rpc_hostport(),
                              &meta));

    // Tombstone the tablet on the remote!
    ASSERT_OK(DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

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
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  // Elect a leader for term 1, then run some data through the cluster.
  const int kLeaderIndex = 1;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  ASSERT_OK(StartElection(leader_ts, tablet_id, timeout));
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
  ASSERT_OK(StartElection(follower_ts, tablet_id, timeout));
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
  ASSERT_OK(DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

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

// Test that multiple concurrent tablet copies do not cause problems.
// This is a regression test for KUDU-951, in which concurrent sessions on
// multiple tablets between the same tablet copy client host and source host
// could corrupt each other.
TEST_F(TabletCopyITest, TestConcurrentTabletCopys) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  vector<string> ts_flags, master_flags;
  ts_flags.emplace_back("--enable_leader_failure_detection=false");
  ts_flags.emplace_back("--log_cache_size_limit_mb=1");
  ts_flags.emplace_back("--log_segment_size_mb=1");
  ts_flags.emplace_back("--log_async_preallocate_segments=false");
  ts_flags.emplace_back("--log_min_segments_to_retain=100");
  ts_flags.emplace_back("--flush_threshold_mb=0"); // Constantly flush.
  ts_flags.emplace_back("--maintenance_manager_polling_interval_ms=10");
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  const MonoDelta timeout = MonoDelta::FromSeconds(60);

  // Create a table with several tablets. These will all be simultaneously
  // copied to a single target node from the same leader host.
  const int kNumTablets = 10;
  KuduSchema client_schema(KuduSchemaFromSchema(GetSimpleTestSchema()));
  vector<const KuduPartialRow*> splits;
  for (int i = 0; i < kNumTablets - 1; i++) {
    KuduPartialRow* row = client_schema.NewRow();
    ASSERT_OK(row->SetInt32(0, std::numeric_limits<int32_t>::max() / kNumTablets * (i + 1)));
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
      ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                       tablet_id, timeout));
    }
  }

  // Elect leaders on each tablet for term 1. All leaders will be on TS 1.
  const int kLeaderIndex = 1;
  const string kLeaderUuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  for (const string& tablet_id : tablet_ids) {
    ASSERT_OK(StartElection(ts_map_[kLeaderUuid], tablet_id, timeout));
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
    ASSERT_OK(DeleteTablet(target_ts, tablet_id, TABLET_DATA_TOMBSTONED,
                           MonoDelta::FromSeconds(10)));
  }

  // Unpause the leader TS and wait for it to initiate Tablet Copy and replace the tombstoned
  // tablets, in parallel.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Resume());
  for (const string& tablet_id : tablet_ids) {
    ASSERT_OK(WaitUntilTabletRunning(target_ts, tablet_id, timeout));
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
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
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
    ASSERT_OK(DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

    // Wait for tablet copy to start.
    ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
        follower_index, tablet_id,
        { tablet::TABLET_DATA_COPYING, tablet::TABLET_DATA_READY },
        timeout));

    // Tombstone the leader.
    LOG(INFO) << "Tombstoning leader tablet " << tablet_id << " on TS " << leader_ts->uuid();
    ASSERT_OK(DeleteTablet(leader_ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

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
#ifndef __APPLE__
// CountBlocksUnderManagement() is used by routines which work with the
// LogBlockManager (not used on OS X).
int64_t CountBlocksUnderManagement(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_log_block_manager_blocks_under_management,
      "value",
      &ret));
  return ret;
}
#endif // #ifndef __APPLE__

int64_t CountUpdateConsensusCalls(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
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
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_glog_info_messages,
      "value",
      &count));
  total += count;

  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_glog_warning_messages,
      "value",
      &count));
  total += count;

  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
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
  ts_flags.emplace_back("--enable_leader_failure_detection=false");
  ts_flags.emplace_back("--enable_tablet_copy=false");
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
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
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ExternalTabletServer* leader_ts = cluster_->tablet_server(0);
  ASSERT_OK(StartElection(ts_map_[leader_ts->uuid()], tablet_id, timeout));

  // Start writing, wait for some rows to be inserted.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Tombstone the tablet on one of the servers (TS 1)
  ASSERT_OK(DeleteTablet(replica_ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

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
  ts_flags.emplace_back("--enable_leader_failure_detection=false");
  ts_flags.emplace_back("--tablet_copy_download_file_inject_latency_ms=5000");
  ts_flags.emplace_back("--follower_unavailable_considered_failed_sec=2");
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
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
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ExternalTabletServer* leader_ts = cluster_->tablet_server(0);
  ASSERT_OK(StartElection(ts_map_[leader_ts->uuid()], tablet_id, timeout));

  // Start writing, wait for some rows to be inserted.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }


  // Tombstone the follower.
  LOG(INFO) << "Tombstoning follower tablet " << tablet_id << " on TS " << replica_ts->uuid();
  ASSERT_OK(DeleteTablet(replica_ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

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
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, kTimeout));
  }

  // Elect a leader for term 1, then run some data through the cluster.
  ASSERT_OK(StartElection(leader, tablet_id, kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));

  // Now delete the leader with TABLET_DATA_DELETED. We should not be able to
  // bring back the leader after that until restarting the process.
  ASSERT_OK(DeleteTablet(leader, tablet_id, TABLET_DATA_DELETED, kTimeout));

  Status s = StartTabletCopy(leader, tablet_id,
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

  ASSERT_OK(StartTabletCopy(leader, tablet_id,
                            cluster_->tablet_server(1)->uuid(),
                            HostPort(cluster_->tablet_server(1)->bound_rpc_addr()),
                            1, // We are in term 1.
                            kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));
}

// Test that tablet copy clears the last-logged opid stored in the TabletMetadata.
TEST_F(TabletCopyITest, TestTabletCopyClearsLastLoggedOpId) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  NO_FATALS(StartCluster({"--enable_leader_failure_detection=false"},
                         {"--catalog_manager_wait_for_new_tablets_to_elect_leader=false"}));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(3);
  workload.Setup();

  int leader_index = 0;
  TServerDetails* leader = ts_map_[cluster_->tablet_server(leader_index)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(leader, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, kTimeout));
  }

  // Elect a leader for term 1, then generate some data to copy.
  ASSERT_OK(StartElection(leader, tablet_id, kTimeout));
  workload.Start();
  const int kMinBatches = 10;
  while (workload.batches_completed() < kMinBatches) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));

  // No last-logged opid should initially be stored in the superblock on a brand-new tablet.
  tablet::TabletSuperBlockPB superblock_pb;
  ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(leader_index, tablet_id, &superblock_pb));
  ASSERT_FALSE(superblock_pb.has_tombstone_last_logged_opid());

  // Now tombstone the leader.
  ASSERT_OK(DeleteTablet(leader, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  // We should end up with a last-logged opid in the superblock.
  ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(leader_index, tablet_id, &superblock_pb));
  ASSERT_TRUE(superblock_pb.has_tombstone_last_logged_opid());
  consensus::OpId last_logged_opid = superblock_pb.tombstone_last_logged_opid();
  ASSERT_EQ(1, last_logged_opid.term());
  ASSERT_GT(last_logged_opid.index(), kMinBatches);

  int follower_index = 1;
  ASSERT_OK(StartTabletCopy(leader, tablet_id,
                            cluster_->tablet_server(follower_index)->uuid(),
                            cluster_->tablet_server(follower_index)->bound_rpc_hostport(),
                            1, // We are in term 1.
                            kTimeout));

  ASSERT_EVENTUALLY([&] {
    // Ensure that the last-logged opid has been cleared from the superblock
    // once it persists the TABLET_DATA_READY state to disk.
    ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(leader_index, tablet_id, &superblock_pb));
    ASSERT_EQ(TABLET_DATA_READY, superblock_pb.tablet_data_state());
    ASSERT_FALSE(superblock_pb.has_tombstone_last_logged_opid())
        << SecureShortDebugString(superblock_pb);
  });
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

class TabletCopyFailureITest : public TabletCopyITest,
                               public ::testing::WithParamInterface<const char*> {
};
// Trigger tablet copy failures a couple different ways: session timeout and
// crash. We want all tablet copy attempts to fail after initial setup.
const char* kTabletCopyFailureITestFlags[] = {
  "--tablet_copy_early_session_timeout_prob=1.0",
  "--tablet_copy_fault_crash_on_fetch_all=1.0"
};
INSTANTIATE_TEST_CASE_P(FailureCause, TabletCopyFailureITest,
                        ::testing::ValuesIn(kTabletCopyFailureITestFlags));

// Test that a failed tablet copy of a brand-new replica results in still being
// able to vote while tombstoned.
TEST_P(TabletCopyFailureITest, TestTabletCopyNewReplicaFailureCanVote) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const string& failure_flag = GetParam();
  const vector<string> kMasterFlags = {
    // If running with the 3-4-3 replication scheme, it's necessary to disable
    // the default catalog manager's behavior of adding and evicting tablet
    // replicas: this test scenario manages replicasa on its own.
    "--catalog_manager_evict_excess_replicas=false",
    "--master_add_server_when_underreplicated=false",
  };
  const vector<string> kTserverFlags = {
    failure_flag
  };
  constexpr auto kNumReplicas = 3;
  constexpr auto kNumTabletServers = kNumReplicas + 1;

  NO_FATALS(StartCluster(kTserverFlags, kMasterFlags, kNumTabletServers));

  TestWorkload workload(cluster_.get());
  workload.Setup();

  ASSERT_OK(inspect_->WaitForReplicaCount(kNumReplicas));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(itest::GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                                     kTimeout, master::VOTER_REPLICA, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size());
  string tablet_id = table_locations.tablet_locations(0).tablet_id();
  set<string> replica_uuids;
  for (const auto& replica : table_locations.tablet_locations(0).replicas()) {
    replica_uuids.insert(replica.ts_info().permanent_uuid());
  }

  string new_replica_uuid;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    if (!ContainsKey(replica_uuids, cluster_->tablet_server(i)->uuid())) {
      new_replica_uuid = cluster_->tablet_server(i)->uuid();
      break;
    }
  }
  ASSERT_FALSE(new_replica_uuid.empty());
  auto* new_replica_ts = ts_map_[new_replica_uuid];

  // Allow retries in case the tablet leadership is unstable.
  ASSERT_EVENTUALLY([&] {
    TServerDetails* leader_ts;
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_ts));
    ASSERT_OK(itest::WaitForOpFromCurrentTerm(leader_ts, tablet_id, COMMITTED_OPID, kTimeout));

    // Adding a server will trigger a tablet copy.
    ASSERT_OK(AddServer(leader_ts, tablet_id, new_replica_ts, RaftPeerPB::VOTER, kTimeout));
  });

  // Wait until the new replica has done its initial download, and has either
  // failed or is about to fail (the check is nondeterministic) plus has
  // persisted its cmeta.
  int new_replica_idx = cluster_->tablet_server_index_by_uuid(new_replica_uuid);
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(inspect_->CheckTabletDataStateOnTS(new_replica_idx, tablet_id,
                                                 { TABLET_DATA_COPYING, TABLET_DATA_TOMBSTONED }));
    ConsensusMetadataPB cmeta_pb;
    ASSERT_OK(inspect_->ReadConsensusMetadataOnTS(new_replica_idx, tablet_id, &cmeta_pb));
  });

  // Restart the cluster with 3 voters instead of 4, including the new replica
  // that should be tombstoned. It should be able to vote and get the leader up
  // and running.
  cluster_->Shutdown();
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    // Remove the "early timeout" flag.
    cluster_->tablet_server(i)->mutable_flags()->pop_back();
  }

  TabletSuperBlockPB superblock;
  ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(new_replica_idx, tablet_id, &superblock));
  LOG(INFO) << "Restarting tablet servers. New replica TS UUID: " << new_replica_uuid
            << ", tablet data state: "
            << tablet::TabletDataState_Name(superblock.tablet_data_state())
            << ", last-logged opid: " << superblock.tombstone_last_logged_opid();
  ASSERT_TRUE(superblock.has_tombstone_last_logged_opid());
  ASSERT_OPID_EQ(MakeOpId(1, 0), superblock.tombstone_last_logged_opid());

  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(cluster_->tablet_server_by_uuid(new_replica_uuid)->Restart());
  auto iter = replica_uuids.cbegin();
  ASSERT_OK(cluster_->tablet_server_by_uuid(*iter)->Restart());
  ASSERT_OK(cluster_->tablet_server_by_uuid(*++iter)->Restart());

  // Now wait until we can write.
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.rows_inserted(), 100);
  });
  workload.StopAndJoin();
}

// Test that a failed tablet copy over a tombstoned replica retains the
// last-logged OpId from the tombstoned replica.
TEST_P(TabletCopyFailureITest, TestFailedTabletCopyRetainsLastLoggedOpId) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const string& tablet_copy_failure_flag = GetParam();
  NO_FATALS(StartCluster({"--enable_leader_failure_detection=false",
                          tablet_copy_failure_flag},
                         {"--catalog_manager_wait_for_new_tablets_to_elect_leader=false"}));

  TestWorkload workload(cluster_.get());
  workload.Setup();

  int first_leader_index = 0;
  TServerDetails* first_leader = ts_map_[cluster_->tablet_server(first_leader_index)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(first_leader, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, kTimeout));
  }

  // Elect a leader for term 1, then generate some data to copy.
  ASSERT_OK(StartElection(first_leader, tablet_id, kTimeout));
  workload.Start();
  const int kMinBatches = 10;
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.batches_completed(), kMinBatches);
  });
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));

  // Tombstone the first leader.
  ASSERT_OK(DeleteTablet(first_leader, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  // We should end up with a last-logged opid in the superblock of the first leader.
  tablet::TabletSuperBlockPB superblock_pb;
  ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(first_leader_index, tablet_id, &superblock_pb));
  ASSERT_TRUE(superblock_pb.has_tombstone_last_logged_opid());
  consensus::OpId last_logged_opid = superblock_pb.tombstone_last_logged_opid();
  ASSERT_EQ(1, last_logged_opid.term());
  ASSERT_GT(last_logged_opid.index(), kMinBatches);
  int64_t initial_mtime = inspect_->GetTabletSuperBlockMTimeOrDie(first_leader_index, tablet_id);

  // Elect a new leader.
  int second_leader_index = 1;
  TServerDetails* second_leader = ts_map_[cluster_->tablet_server(second_leader_index)->uuid()];
  ASSERT_OK(StartElection(second_leader, tablet_id, kTimeout));

  // The second leader will initiate a tablet copy on the first leader. Wait
  // for it to fail (via crash or abort).
  ASSERT_EVENTUALLY([&] {
    // Superblock must have been modified.
    int64_t cur_mtime = inspect_->GetTabletSuperBlockMTimeOrDie(first_leader_index, tablet_id);
    ASSERT_GT(cur_mtime, initial_mtime);
    ASSERT_OK(inspect_->CheckTabletDataStateOnTS(first_leader_index, tablet_id,
                                                 { TABLET_DATA_COPYING, TABLET_DATA_TOMBSTONED }));
  });

  // Bounce the cluster to get rid of leaders and reach a steady state.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());

  // After failing the tablet copy, we should retain the old tombstoned
  // tablet's last-logged opid.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(first_leader_index, tablet_id, &superblock_pb));
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, superblock_pb.tablet_data_state());
    ASSERT_TRUE(superblock_pb.has_tombstone_last_logged_opid())
        << SecureShortDebugString(superblock_pb);
    ASSERT_OPID_EQ(last_logged_opid, superblock_pb.tombstone_last_logged_opid());
  });
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
const char* kBadTabletCopyITestFlags[] = { kFlagFaultOnFetch, kFlagEarlyTimeout };
INSTANTIATE_TEST_CASE_P(FaultFlags, BadTabletCopyITest,
                        ::testing::ValuesIn(kBadTabletCopyITestFlags));

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

namespace {
int64_t TabletCopyBytesSent(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_tablet_copy_bytes_sent,
      "value",
      &ret));
  return ret;
}

int64_t TabletCopyBytesFetched(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_tablet_copy_bytes_fetched,
      "value",
      &ret));
  return ret;
}

int64_t TabletCopyOpenSourceSessions(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_tablet_copy_open_source_sessions,
      "value",
      &ret));
  return ret;
}

int64_t TabletCopyOpenClientSessions(ExternalTabletServer* ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_tablet_copy_open_client_sessions,
      "value",
      &ret));
  return ret;
}
} // anonymous namespace

// Test that metrics work correctly with tablet copy.
TEST_F(TabletCopyITest, TestTabletCopyMetrics) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kTsIndex = 1; // Pick a random TS.

  // Slow down tablet copy a little so we observe an active session and client.
  vector<string> ts_flags;
  ts_flags.emplace_back("--tablet_copy_download_file_inject_latency_ms=1000");
  NO_FATALS(StartCluster(ts_flags));

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

  // Find the leader and then tombstone the follower replica, causing a tablet copy.
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_ts));
  leader_index = cluster_->tablet_server_index_by_uuid(leader_ts->uuid());
  follower_index = (leader_index + 1) % cluster_->num_tablet_servers();
  follower_ts = ts_map_[cluster_->tablet_server(follower_index)->uuid()];

  LOG(INFO) << "Tombstoning follower tablet " << tablet_id
            << " on TS " << follower_ts->uuid();
  ASSERT_OK(DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  // Wait for copying to start.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
      follower_index, tablet_id,
      { tablet::TABLET_DATA_COPYING },
      kTimeout));

  // We should see one source session on the leader and one client session on the follower.
  ASSERT_EQ(1, TabletCopyOpenSourceSessions(cluster_->tablet_server(leader_index)));
  ASSERT_EQ(1, TabletCopyOpenClientSessions(cluster_->tablet_server(follower_index)));

  // Wait for copying to finish.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
      follower_index, tablet_id,
      { tablet::TABLET_DATA_READY },
      kTimeout));

  // Check that the final bytes fetched is equal to the final bytes sent, and both are positive.
  int64_t bytes_sent = TabletCopyBytesSent(cluster_->tablet_server(leader_index));
  int64_t bytes_fetched = TabletCopyBytesFetched(cluster_->tablet_server(follower_index));
  ASSERT_GT(bytes_sent, 0);
  ASSERT_EQ(bytes_sent, bytes_fetched);

  // The tablet copy sessions last until bootstrap is complete, so we'll wait for the
  // new replica to catch up then check there are no open sessions.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id,
                                  workload.batches_completed()));

  ASSERT_EQ(0, TabletCopyOpenSourceSessions(cluster_->tablet_server(leader_index)));
  ASSERT_EQ(0, TabletCopyOpenClientSessions(cluster_->tablet_server(follower_index)));

  NO_FATALS(cluster_->AssertNoCrashes());
}

namespace {
int64_t CountRunningTablets(ExternalTabletServer *ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      nullptr,
      &METRIC_tablets_num_running,
      "value",
      &ret));
  return ret;
}

int64_t CountBootstrappingTablets(ExternalTabletServer *ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_server,
      nullptr,
      &METRIC_tablets_num_bootstrapping,
      "value",
      &ret));
  return ret;
}

int64_t CountRowsInserted(ExternalTabletServer *ets) {
  int64_t ret;
  CHECK_OK(GetInt64Metric(
      ets->bound_http_hostport(),
      &METRIC_ENTITY_tablet,
      nullptr,
      &METRIC_rows_inserted,
      "value",
      &ret));
  return ret;
}
} // anonymous namespace

// Check that state metrics work during tablet copy.
TEST_F(TabletCopyITest, TestTabletStateMetricsDuringTabletCopy) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kTsIndex = 1; // Pick a random TS.
  NO_FATALS(StartCluster({ "--tablet_state_walk_min_period_ms=10" }));

  // Populate the tablet with some data.
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Ensure all the servers agree before we proceed.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id,
                                  workload.batches_completed()));

  // Find the leader so we can tombstone a follower, which makes the test faster and more focused.
  int leader_index;
  int follower_index;
  TServerDetails* leader_ts;
  TServerDetails* follower_ts;
  ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_ts));
  leader_index = cluster_->tablet_server_index_by_uuid(leader_ts->uuid());
  follower_index = (leader_index + 1) % cluster_->num_tablet_servers();
  follower_ts = ts_map_[cluster_->tablet_server(follower_index)->uuid()];

  // State: 1 running tablet.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, CountRunningTablets(cluster_->tablet_server(follower_index)));
  });

  // Tombstone the tablet on the follower.
  ASSERT_OK(DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

  // State: 0 running tablets.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, CountRunningTablets(cluster_->tablet_server(follower_index)));
  });

  // Wait for the tablet to be revived via tablet copy.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(follower_index, tablet_id,
                                                 { tablet::TABLET_DATA_READY },
                                                 kTimeout));

  // State: 1 running or bootstrapping tablet.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, CountRunningTablets(cluster_->tablet_server(follower_index)) +
                 CountBootstrappingTablets(cluster_->tablet_server(follower_index)));
  });
}

TEST_F(TabletCopyITest, TestMetricsResetAfterRevival) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kTsIndex = 1; // Pick a random TS.
  NO_FATALS(StartCluster());

  // Populate the tablet with some data.
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Ensure all the servers agree before we proceed.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id,
                                  workload.batches_completed()));

  // Find the leader so we can tombstone a follower, which makes the test faster and more focused.
  int leader_index;
  int follower_index;
  ASSERT_EVENTUALLY([&]() {
    TServerDetails* leader_ts;
    TServerDetails* follower_ts;
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_ts));
    leader_index = cluster_->tablet_server_index_by_uuid(leader_ts->uuid());
    follower_index = (leader_index + 1) % cluster_->num_tablet_servers();
    follower_ts = ts_map_[cluster_->tablet_server(follower_index)->uuid()];

    // Make sure the metrics reflect that some rows have been inserted.
    ASSERT_GT(CountRowsInserted(cluster_->tablet_server(follower_index)), 0);

    // Tombstone the tablet on the follower.
    ASSERT_OK(DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));
  });

  // Don't cache the state metrics or the below assertion won't be accurate.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(follower_index),
            "tablet_state_walk_min_period_ms", "0"));

  // Wait for the tablet to be successfully bootstrapped.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, CountRunningTablets(cluster_->tablet_server(follower_index)));
  });

  // The rows inserted should have reset back to 0.
  ASSERT_EQ(0, CountRowsInserted(cluster_->tablet_server(follower_index)));
}

// Test that tablet copy session initialization can handle concurrency when
// there is latency during session initialization. This is a regression test
// for KUDU-2124.
TEST_F(TabletCopyITest, TestBeginTabletCopySessionConcurrency) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  MonoDelta kInjectedLatency = MonoDelta::FromMilliseconds(1000);

  // Inject latency during session initialization. This should show up in
  // TabletCopyService::BeginTabletCopySession() RPC calls.
  NO_FATALS(StartCluster({Substitute("--tablet_copy_session_inject_latency_on_init_ms=$0",
                                     kInjectedLatency.ToMilliseconds())},
                         {}, /*num_tablet_servers=*/ 1));

  // Create a bunch of tablets to operate on at once.
  const int kNumTablets = 10;
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.set_num_tablets(kNumTablets);
  workload.Setup();

  auto ts = ts_map_[cluster_->tablet_server(0)->uuid()];
  vector<string> tablet_ids;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(itest::ListRunningTabletIds(ts, kTimeout, &tablet_ids));
    ASSERT_EQ(kNumTablets, tablet_ids.size());
  });

  // Spin up a bunch of threads simultaneously trying to begin tablet copy
  // sessions on all of the tablets.
  const int kNumThreads = kNumTablets * 5;
  mutex m;
  vector<MonoDelta> success_latencies; // Latency of successful opens.
  vector<MonoDelta> failure_latencies; // Latency of failed opens.
  vector<thread> threads;
  for (int i = 0; i < kNumThreads; i++) {
    string tablet_id = tablet_ids[i % kNumTablets];
    threads.emplace_back([&, ts, tablet_id] {
      while (true) {
        MonoTime start = MonoTime::Now();
        Status s = itest::BeginTabletCopySession(ts, tablet_id, "dummy-uuid", kTimeout);
        MonoDelta duration = MonoTime::Now() - start;
        lock_guard<mutex> l(m);
        if (s.ok()) {
          success_latencies.push_back(duration);
          return;
        }
        failure_latencies.push_back(duration);
        VLOG(1) << tablet_id << ": " << s.ToString();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // All of the successfully initialized tablet sessions should have taken at
  // least the amount of time that we slept for.
  int num_slower_than_latency = 0;
  for (const auto& lat : success_latencies) {
    if (lat >= kInjectedLatency) num_slower_than_latency++;
  }
  // We should have had exactly # tablets sessions that were slow due to the
  // latency injection.
  ASSERT_EQ(kNumTablets, num_slower_than_latency);

  // All of the failed tablet session initialization sessions should have been
  // relatively quicker than our injected latency, demonstrating that they did
  // not wait on the session lock.
  for (const auto& lat : failure_latencies) {
    ASSERT_LT(lat, kInjectedLatency);
  }
}

} // namespace kudu
