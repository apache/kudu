// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <string>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tserver/remote_bootstrap_client.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaFromSchema;
using kudu::client::KuduTableCreator;
using kudu::itest::TServerDetails;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::RemoteBootstrapClient;
using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {

class RemoteBootstrapITest : public KuduTest {
 public:
  virtual void TearDown() OVERRIDE {
    if (HasFatalFailure()) {
      for (int i = 0; i < 3; i++) {
        if (!cluster_->tablet_server(i)->IsProcessAlive()) {
          LOG(INFO) << "Tablet server " << i << " is not running. Cannot dump its stacks.";
          continue;
        }
        LOG(INFO) << "Attempting to dump stacks of TS " << i
                  << " with UUID " << cluster_->tablet_server(i)->uuid()
                  << " and pid " << cluster_->tablet_server(i)->pid();
        WARN_NOT_OK(PstackWatcher::DumpPidStacks(cluster_->tablet_server(i)->pid()),
                    "Couldn't dump stacks");
      }
    }
    if (cluster_) cluster_->Shutdown();
    KuduTest::TearDown();
    STLDeleteValues(&ts_map_);
  }

 protected:
  void StartCluster(const vector<string>& extra_tserver_flags = vector<string>());

  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  unordered_map<string, TServerDetails*> ts_map_;
};

void RemoteBootstrapITest::StartCluster(const vector<string>& extra_tserver_flags) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 3;
  opts.extra_tserver_flags = extra_tserver_flags;
  cluster_.reset(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy().get(),
                                          cluster_->messenger(),
                                          &ts_map_));
  KuduClientBuilder builder;
  ASSERT_OK(cluster_->CreateClient(builder, &client_));
}

// If a rogue (a.k.a. zombie) leader tries to remote bootstrap a tombstoned
// tablet, make sure its term isn't older than the latest term we observed.
// If it is older, make sure we reject the request, to avoid allowing old
// leaders to create a parallel universe. This is possible because config
// change could cause nodes to move around. The term check is reasonable
// because only one node can be elected leader for a given term.
//
// A leader can "go rogue" due to a VM pause, CTRL-z, partition, etc.
TEST_F(RemoteBootstrapITest, TestRejectRogueLeader) {
  // This test pauses for at least 10 seconds. Only run in slow-test mode.
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  vector<string> extra_tserver_flags;
  extra_tserver_flags.push_back("--enable_leader_failure_detection=false");
  NO_FATALS(StartCluster(extra_tserver_flags));

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

  // Come out of the blue and try to remotely bootstrap a running server while
  // specifying an old term. That running server should reject the request.
  // We are essentially masquerading as a rogue leader here.
  Status s = itest::StartRemoteBootstrap(ts, tablet_id, zombie_leader_uuid,
                                         HostPort(cluster_->tablet_server(1)->bound_rpc_addr()),
                                         0, // Say I'm from term 0.
                                         timeout);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "term 0 lower than term 1");

  // Now pause the actual leader so we can bring him back as a zombie later.
  ASSERT_OK(cluster_->tablet_server(zombie_leader_index)->Pause());

  // Trigger TS 2 to become leader of term 2.
  int new_leader_index = 2;
  string new_leader_uuid = cluster_->tablet_server(new_leader_index)->uuid();
  ASSERT_OK(itest::StartElection(ts_map_[new_leader_uuid], tablet_id, timeout));
  ASSERT_OK(itest::WaitUntilLeader(ts_map_[new_leader_uuid], tablet_id, timeout));
  workload.Start();
  while (workload.rows_inserted() < 200) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Now kill the new leader and tombstone the replica on TS 0.
  cluster_->tablet_server(new_leader_index)->Shutdown();
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

  // Zombies!!! Resume the rogue zombie leader.
  // He should attempt to remote bootstrap TS 0 but fail.
  ASSERT_OK(cluster_->tablet_server(zombie_leader_index)->Resume());

  // Loop for a few seconds to ensure that the tablet doesn't transition to READY.
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromSeconds(5));
  while (MonoTime::Now(MonoTime::FINE).ComesBefore(deadline)) {
    ASSERT_OK(itest::ListTablets(ts, timeout, &tablets));
    ASSERT_EQ(1, tablets.size());
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, tablets[0].tablet_status().tablet_data_state());
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Force the rogue leader to step down.
  // Then, send a remote bootstrap start request from a "fake" leader that
  // sends an up-to-date term in the RB request but the actual term stored
  // in the bootstrap source's consensus metadata would still be old.
  LOG(INFO) << "Forcing rogue leader T " << tablet_id << " P " << zombie_leader_uuid
            << " to step down...";
  ASSERT_OK(itest::LeaderStepDown(ts_map_[zombie_leader_uuid], tablet_id, timeout));
  ExternalTabletServer* zombie_ets = cluster_->tablet_server(zombie_leader_index);
  // It's not necessarily part of the API but this could return faliure due to
  // rejecting the remote. We intend to make that part async though, so ignoring
  // this return value in this test.
  ignore_result(itest::StartRemoteBootstrap(ts, tablet_id, zombie_leader_uuid,
                                            HostPort(zombie_ets->bound_rpc_addr()),
                                            2, // Say I'm from term 2.
                                            timeout));

  // Wait another few seconds to be sure the remote bootstrap is rejected.
  deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromSeconds(5));
  while (MonoTime::Now(MonoTime::FINE).ComesBefore(deadline)) {
    ASSERT_OK(itest::ListTablets(ts, timeout, &tablets));
    ASSERT_EQ(1, tablets.size());
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, tablets[0].tablet_status().tablet_data_state());
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
}

// Start remote bootstrap session and delete the tablet in the middle.
// It should actually be possible to complete bootstrap in such a case, because
// when a remote bootstrap session is started on the "source" server, all of
// the relevant files are either read or opened, meaning that an in-progress
// remote bootstrap can complete even after a tablet is officially "deleted" on
// the source server. This is also a regression test for KUDU-1009.
TEST_F(RemoteBootstrapITest, TestDeleteTabletDuringRemoteBootstrap) {
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

  // Set up an FsManager to use with the RemoteBootstrapClient.
  FsManagerOpts opts;
  string testbase = GetTestPath("fake-ts");
  ASSERT_OK(env_->CreateDir(testbase));
  opts.wal_path = JoinPathSegments(testbase, "wals");
  opts.data_paths.push_back(JoinPathSegments(testbase, "data-0"));
  gscoped_ptr<FsManager> fs_manager(new FsManager(env_.get(), opts));
  ASSERT_OK(fs_manager->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager->Open());

  // Start up a RemoteBootstrapClient and open a remote bootstrap session.
  gscoped_ptr<RemoteBootstrapClient> rb_client(
      new RemoteBootstrapClient(tablet_id, fs_manager.get(),
                                cluster_->messenger(), fs_manager->uuid()));
  scoped_refptr<tablet::TabletMetadata> meta;
  ASSERT_OK(rb_client->Start(cluster_->tablet_server(kTsIndex)->uuid(),
                             cluster_->tablet_server(kTsIndex)->bound_rpc_hostport(),
                             &meta));

  // Tombstone the tablet on the remote!
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, timeout));

  // Now finish bootstrapping!
  tablet::TabletStatusListener listener(meta);
  ASSERT_OK(rb_client->FetchAll(&listener));
  ASSERT_OK(rb_client->Finish());

  // Run destructor, which closes the remote session.
  rb_client.reset();
  SleepFor(MonoDelta::FromMilliseconds(50));  // Give a little time for a crash (KUDU-1009).
  ASSERT_TRUE(cluster_->tablet_server(kTsIndex)->IsProcessAlive());
}

// Test that multiple concurrent remote bootstraps do not cause problems.
// This is a regression test for KUDU-951, in which concurrent sessions on
// multiple tablets between the same remote bootstrap client host and remote
// bootstrap source host could corrupt each other.
TEST_F(RemoteBootstrapITest, TestConcurrentRemoteBootstraps) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=false");
  flags.push_back("--log_cache_size_limit_mb=1");
  flags.push_back("--log_segment_size_mb=1");
  flags.push_back("--log_async_preallocate_segments=false");
  flags.push_back("--log_min_segments_to_retain=100");
  flags.push_back("--flush_threshold_mb=0"); // Constantly flush.
  flags.push_back("--maintenance_manager_polling_interval_ms=10");
  NO_FATALS(StartCluster(flags));

  const MonoDelta timeout = MonoDelta::FromSeconds(60);

  // Create a table with several tablets. These will all be simultaneously
  // remotely bootstrapped to a single target node from the same leader host.
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
                          .num_replicas(3)
                          .Create());

  const int kTsIndex = 0; // We'll test with the first TS.
  TServerDetails* target_ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];

  // Figure out the tablet ids of the created tablets.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(target_ts, kNumTablets, timeout, &tablets));

  vector<string> tablet_ids;
  BOOST_FOREACH(const ListTabletsResponsePB::StatusAndSchemaPB& t, tablets) {
    tablet_ids.push_back(t.tablet_status().tablet_id());
  }

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    BOOST_FOREACH(const string& tablet_id, tablet_ids) {
      ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                              tablet_id, timeout));
    }
  }

  // Elect leaders on each tablet for term 1. All leaders will be on TS 1.
  const int kLeaderIndex = 1;
  const string kLeaderUuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  BOOST_FOREACH(const string& tablet_id, tablet_ids) {
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

  BOOST_FOREACH(const string& tablet_id, tablet_ids) {
    ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1));
  }

  // Now pause the leader so we can tombstone the tablets.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Pause());

  BOOST_FOREACH(const string& tablet_id, tablet_ids) {
    LOG(INFO) << "Tombstoning tablet " << tablet_id << " on TS " << target_ts->uuid();
    ASSERT_OK(itest::DeleteTablet(target_ts, tablet_id, TABLET_DATA_TOMBSTONED,
              MonoDelta::FromSeconds(10)));
  }

  // Unpause the leader TS and wait for it to remotely bootstrap the tombstoned
  // tablets, in parallel.
  ASSERT_OK(cluster_->tablet_server(kLeaderIndex)->Resume());
  BOOST_FOREACH(const string& tablet_id, tablet_ids) {
    ASSERT_OK(itest::WaitUntilTabletRunning(target_ts, tablet_id, timeout));
  }

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(), ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));
}

} // namespace kudu
