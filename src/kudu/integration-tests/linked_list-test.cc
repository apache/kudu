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

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <utility>
#include <vector>

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

DEFINE_int32(seconds_to_run, 0, "Number of seconds for which to run the test "
             "(default 0 autoselects based on test mode)");
enum {
  kDefaultRunTimeSlow = 30,
  kDefaultRunTimeFast = 1
};

DEFINE_int32(num_chains, 50, "Number of parallel chains to generate");
DEFINE_int32(num_tablets, 3, "Number of tablets over which to split the data");
DEFINE_bool(enable_mutation, false, "Enable periodic mutation of inserted rows");

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

    BuildAndStart();
  }

  void BuildAndStart() {
    vector<string> flags;
    flags.push_back("--skip_remove_old_recovery_dir");
    flags.push_back("--tablet_server_rpc_bind_addresses=127.0.0.1:705${index}");
    flags.push_back("--enable_leader_failure_detection=true");

    if (AllowSlowTests()) {
      // Set the flush threshold low so that we have a mix of flushed and unflushed
      // operations in the WAL, when we bootstrap.
      flags.push_back("--flush_threshold_mb=1");
      // Set the size of the WAL segments low so that some can be GC'd.
      flags.push_back("--log_segment_size_mb=1");
    }

    CreateCluster("linked-list-cluster", flags);
    ResetClientAndTester();
    ASSERT_STATUS_OK(tester_->CreateLinkedListTable());
    WaitForTSAndQuorum();
  }

  void ResetClientAndTester() {
    KuduClientBuilder builder;
    ASSERT_STATUS_OK(cluster_->CreateClient(builder, &client_));
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
  if (FLAGS_seconds_to_run == 0) {
    FLAGS_seconds_to_run = AllowSlowTests() ? kDefaultRunTimeSlow : kDefaultRunTimeFast;
  }

  PeriodicWebUIChecker checker(*cluster_.get(), MonoDelta::FromSeconds(1));

  bool can_kill_ts = FLAGS_num_tablet_servers > 1 && FLAGS_num_replicas > 2;

  int64_t written = 0;
  ASSERT_STATUS_OK(tester_->LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run),
                                           &written));

  // TODO: currently we don't use hybridtime on the C++ client, so it's possible when we
  // scan after writing we may not see all of our writes (we may scan a replica). So,
  // we use WaitAndVerify here instead of a plain Verify.
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  LOG(INFO) << "Successfully verified " << written << " rows before killing any servers.";

  // Check in-memory state with a downed TS. Scans may try other replicas.
  if (can_kill_ts) {
    string tablet = (*tablet_replicas_.begin()).first;
    TServerDetails* leader;
    EXPECT_OK(GetLeaderReplicaWithRetries(tablet, &leader));
    LOG(INFO) << "Killing TS: " << leader->instance_id.permanent_uuid() << ", leader of tablet: "
        << tablet << " and verifying that we can still read all results";
    leader->external_ts->Shutdown();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  }

  // Kill and restart the cluster, verify data remains.
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  LOG(INFO) << "Verifying rows after restarting entire cluster.";

  // We need to loop here because the tablet may spend some time in BOOTSTRAPPING state
  // initially after a restart. TODO: Scanner should support its own retries in this circumstance.
  // Remove this loop once client is more fleshed out.
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  // Check post-replication state with a downed TS.
  if (can_kill_ts) {
    string tablet = (*tablet_replicas_.begin()).first;
    TServerDetails* leader;
    EXPECT_OK(GetLeaderReplicaWithRetries(tablet, &leader));
    LOG(INFO) << "Killing TS: " << leader->instance_id.permanent_uuid() << ", leader of tablet: "
        << tablet << " and verifying that we can still read all results";
    leader->external_ts->Shutdown();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  }

  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  // Sleep a little bit, so that the tablet is probably in bootstrapping state.
  SleepFor(MonoDelta::FromMilliseconds(100));

  // Restart while bootstrapping
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  // Dump the performance info at the very end, so it's easy to read. On a failed
  // test, we don't care about this stuff anyway.
  tester_->DumpInsertHistogram(true);
}

} // namespace kudu
