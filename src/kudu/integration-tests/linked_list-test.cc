// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// This is an integration test similar to TestLoadAndVerify in HBase.
// It creates a table and writes linked lists into it, where each row
// points to the previously written row. For example, a sequence of inserts
// may be:
//
//  rand_key   | link_to   |  insert_ts
//   12345          0           1
//   823          12345         2
//   9999          823          3
// (each insert links to the key of the previous insert)
//
// During insertion, a configurable number of parallel chains may be inserted.
// To verify, the table is scanned, and we ensure that every key is linked to
// either zero or one times, and no link_to refers to a missing key.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/encoded_key.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/walltime.h"
#include "kudu/integration-tests/linked_list-test-util.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/hdr_histogram.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduSchema;
using std::tr1::shared_ptr;

DEFINE_int32(seconds_to_run, 1, "Number of seconds for which to run the test");

DEFINE_int32(num_chains, 50, "Number of parallel chains to generate");
DEFINE_int32(num_tablets, 3, "Number of tablets over which to split the data");
DEFINE_bool(enable_mutation, true, "Enable periodic mutation of inserted rows");
DEFINE_int32(num_snapshots, 3, "Number of snapshots to verify across replicas and reboots.");

DEFINE_bool(stress_flush_compact, false,
            "Flush and compact way more aggressively to try to find bugs");
DEFINE_bool(stress_wal_gc, false,
            "Set WAL segment size small so that logs will be GCed during the test");

namespace kudu {

class LinkedListTest : public tserver::TabletServerIntegrationTestBase {
 public:
  LinkedListTest() {}

  void SetUp() OVERRIDE {
    TabletServerIntegrationTestBase::SetUp();

    LOG(INFO) << "Linked List Test Configuration:";
    LOG(INFO) << "--------------";
    LOG(INFO) << FLAGS_num_chains << " chains";
    LOG(INFO) << FLAGS_num_tablets << " tablets";
    LOG(INFO) << "Mutations " << (FLAGS_enable_mutation ? "on" : "off");
    LOG(INFO) << "--------------";
  }

  void BuildAndStart() {
    vector<string> common_flags;

    common_flags.push_back("--skip_remove_old_recovery_dir");
    common_flags.push_back("--enable_leader_failure_detection=true");
    common_flags.push_back("--use_hybrid_clock=true");
    common_flags.push_back("--max_clock_sync_error_usec=10000000");

    vector<string> ts_flags(common_flags);
    ts_flags.push_back("--tablet_server_rpc_bind_addresses=127.0.0.1:705${index}");

    if (FLAGS_stress_flush_compact) {
      // Set the flush threshold low so that we have a mix of flushed and unflushed
      // operations in the WAL, when we bootstrap.
      ts_flags.push_back("--flush_threshold_mb=1");
      // Set the compaction budget to be low so that we get multiple passes of compaction
      // instead of selecting all of the rowsets in a single compaction of the whole
      // tablet.
      ts_flags.push_back("--tablet_compaction_budget_mb=4");
    }
    if (FLAGS_stress_wal_gc) {
      // Set the size of the WAL segments low so that some can be GC'd.
      ts_flags.push_back("--log_segment_size_mb=1");
    }

    CreateCluster("linked-list-cluster", ts_flags, common_flags);
    ResetClientAndTester();
    ASSERT_OK(tester_->CreateLinkedListTable());
    WaitForTSAndQuorum();
  }

  void ResetClientAndTester() {
    KuduClientBuilder builder;
    ASSERT_OK(cluster_->CreateClient(builder, &client_));
    tester_.reset(new LinkedListTester(client_, kTableId,
                                       FLAGS_num_chains,
                                       FLAGS_num_tablets,
                                       FLAGS_num_replicas,
                                       FLAGS_enable_mutation));
  }

  void RestartCluster() {
    CHECK(cluster_);
    cluster_->Shutdown(ExternalMiniCluster::TS_ONLY);
    cluster_->Restart();
    ResetClientAndTester();
  }

  // Wait until all of the servers have converged on the same log index.
  // The converged index must be at least equal to 'minimum_index'.
  //
  // Requires that all servers are running. FAIL()s the test if the indexes
  // do not converge within the given timeout.
  void WaitForServersToAgree(const MonoDelta& timeout,
                             int64_t minimum_index);

 protected:
  void AddExtraFlags(const string& flags_str, vector<string>* flags) {
    if (flags_str.empty()) {
      return;
    }
    vector<string> split_flags = strings::Split(flags_str, " ");
    BOOST_FOREACH(const string& flag, split_flags) {
      flags->push_back(flag);
    }
  }

  std::tr1::shared_ptr<KuduClient> client_;
  gscoped_ptr<LinkedListTester> tester_;
};

TEST_F(LinkedListTest, TestLoadAndVerify) {
  OverrideFlagForSlowTests("seconds_to_run", "30");
  OverrideFlagForSlowTests("stress_flush_compact", "true");
  OverrideFlagForSlowTests("stress_wal_gc", "true");
  ASSERT_NO_FATAL_FAILURE(BuildAndStart());

  PeriodicWebUIChecker checker(*cluster_.get(), MonoDelta::FromSeconds(1));

  bool can_kill_ts = FLAGS_num_tablet_servers > 1 && FLAGS_num_replicas > 2;

  int64_t written = 0;
  ASSERT_OK(tester_->LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run),
                                           FLAGS_num_snapshots,
                                           &written));

  // TODO: currently we don't use hybridtime on the C++ client, so it's possible when we
  // scan after writing we may not see all of our writes (we may scan a replica). So,
  // we use WaitAndVerify here instead of a plain Verify.
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size()));

  LOG(INFO) << "Successfully verified " << written << " rows before killing any servers.";

  if (can_kill_ts) {
    // Restart a tserver during a scan to test scanner fault tolerance.
    WaitForTSAndQuorum();
    LOG(INFO) << "Will restart the tablet server during verification scan.";
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written,
                                     boost::bind(
                                         &TabletServerIntegrationTestBase::RestartServerWithUUID,
                                         this, _1)));
    LOG(INFO) << "Done with tserver restart test.";
    ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size()));

    // Kill a tserver during a scan to test scanner fault tolerance.
    // Note that the previously restarted node is likely still be bootstrapping, which makes this
    // even harder.
    LOG(INFO) << "Will kill the tablet server during verification scan.";
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written,
                                     boost::bind(
                                         &TabletServerIntegrationTestBase::KillServerWithUUID,
                                         this, _1)));
    LOG(INFO) << "Done with tserver kill test.";
    ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size()-1));
    ASSERT_NO_FATAL_FAILURE(RestartCluster());
    // Again wait for cluster to finish bootstrapping.
    WaitForTSAndQuorum();

    // Check in-memory state with a downed TS. Scans may try other replicas.
    string tablet = (*tablet_replicas_.begin()).first;
    TServerDetails* leader;
    EXPECT_OK(GetLeaderReplicaWithRetries(tablet, &leader));
    LOG(INFO) << "Killing TS: " << leader->instance_id.permanent_uuid() << ", leader of tablet: "
        << tablet << " and verifying that we can still read all results";
    leader->external_ts->Shutdown();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
    ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size() - 1));
  }

  // Kill and restart the cluster, verify data remains.
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  LOG(INFO) << "Verifying rows after restarting entire cluster.";

  // We need to loop here because the tablet may spend some time in BOOTSTRAPPING state
  // initially after a restart. TODO: Scanner should support its own retries in this circumstance.
  // Remove this loop once client is more fleshed out.
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  // In slow tests mode, we'll wait for a little bit to allow time for the tablet to
  // compact. This is a regression test for bugs where compaction post-bootstrap
  // could cause data loss.
  if (AllowSlowTests()) {
    SleepFor(MonoDelta::FromSeconds(10));
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  }
  ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size()));

  // Check post-replication state with a downed TS.
  if (can_kill_ts) {
    string tablet = (*tablet_replicas_.begin()).first;
    TServerDetails* leader;
    EXPECT_OK(GetLeaderReplicaWithRetries(tablet, &leader));
    LOG(INFO) << "Killing TS: " << leader->instance_id.permanent_uuid() << ", leader of tablet: "
        << tablet << " and verifying that we can still read all results";
    leader->external_ts->Shutdown();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
    ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size() - 1));
  }

  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  // Sleep a little bit, so that the tablet is probably in bootstrapping state.
  SleepFor(MonoDelta::FromMilliseconds(100));

  // Restart while bootstrapping
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  ASSERT_OK(CheckTabletServersAreAlive(tablet_servers_.size()));

  // Dump the performance info at the very end, so it's easy to read. On a failed
  // test, we don't care about this stuff anyway.
  tester_->DumpInsertHistogram(true);
}

void LinkedListTest::WaitForServersToAgree(const MonoDelta& timeout,
                                           int64_t minimum_index) {
  MonoTime now = MonoTime::Now(MonoTime::COARSE);
  MonoTime deadline = now;
  deadline.AddDelta(timeout);

  for (int i = 1; now.ComesBefore(deadline); i++) {
    string tablet_id = tablet_replicas_.begin()->first;
    vector<TServerDetails*> servers;
    AppendValuesFromMap(tablet_servers_, &servers);
    vector<consensus::OpId> ids;
    ASSERT_OK(GetLastOpIdForEachReplica(tablet_id, servers, &ids));

    if (ids[0].index() == ids[1].index() &&
        ids[1].index() == ids[2].index()) {
      ASSERT_GE(ids[0].index(), minimum_index)
        << "Log indexes converged, but did not reach expected minimum value";
      return;
    } else {
      LOG(INFO) << "Not converged yet: " << ids;
      SleepFor(MonoDelta::FromMilliseconds(std::min(i * 100, 1000)));
    }

    now = MonoTime::Now(MonoTime::COARSE);
  }
  FAIL() << "Servers never converged";
}

// This test loads the linked list while one of the servers is down.
// Once the loading is complete, the server is started back up and
// we wait for it to catch up. Then we shut down the other two servers
// and verify that the data is correct on the server which caught up.
TEST_F(LinkedListTest, TestLoadWhileOneServerDownAndVerify) {
  OverrideFlagForSlowTests("seconds_to_run", "30");

  if (!FLAGS_ts_flags.empty()) {
    FLAGS_ts_flags += " ";
  }

  FLAGS_ts_flags += "--log_cache_size_limit_mb=2";
  FLAGS_ts_flags += " --global_log_cache_size_limit_mb=4";

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_tablets = 1;
  ASSERT_NO_FATAL_FAILURE(BuildAndStart());

  // Load the data with one of the three servers down.
  cluster_->tablet_server(0)->Shutdown();

  int64_t written = 0;
  ASSERT_OK(tester_->LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run),
                                           FLAGS_num_snapshots,
                                           &written));

  // Start back up the server that missed all of the data being loaded. It should be
  // able to stream the data back from the other server which is still up.
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  // We'll give the tablets 5 seconds to start up regardless of how long we
  // inserted for. This prevents flakiness in TSAN builds in particular.
  const int kBaseTimeToWaitSecs = 5;
  const int kWaitTime = FLAGS_seconds_to_run + kBaseTimeToWaitSecs;
  ASSERT_NO_FATAL_FAILURE(WaitForServersToAgree(
                            MonoDelta::FromSeconds(kWaitTime),
                            written / FLAGS_num_chains));

  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
}

} // namespace kudu
