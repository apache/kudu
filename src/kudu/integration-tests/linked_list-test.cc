// Copyright (c) 2014, Cloudera, inc.
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
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/linked_list-test-util.h"
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
DEFINE_int32(num_tablet_servers, 3, "Number of tablet servers to start");
DEFINE_int32(num_replicas, 3, "Number of replicas per tablet server");
DEFINE_bool(enable_mutation, false, "Enable periodic mutation of inserted rows");
DEFINE_string(ts_flags, "", "Flags to pass through to tablet servers");

namespace kudu {

class LinkedListTest : public KuduTest {
 public:
  LinkedListTest() {}

  void SetUp() OVERRIDE {
    KuduTest::SetUp();
    SeedRandom();

    LOG(INFO) << "Configuration:";
    LOG(INFO) << "--------------";
    LOG(INFO) << FLAGS_num_chains << " chains";
    LOG(INFO) << FLAGS_num_tablets << " tablets";
    LOG(INFO) << FLAGS_num_tablet_servers << " tablet servers";
    LOG(INFO) << FLAGS_num_replicas << " replicas per TS";
    LOG(INFO) << "Mutations " << (FLAGS_enable_mutation ? "on" : "off");
    LOG(INFO) << "--------------";
    RestartCluster();
  }

  void RestartCluster() {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = FLAGS_num_tablet_servers;
    opts.data_root = GetTestPath("linked-list-cluster");
    opts.extra_tserver_flags.push_back("--skip_remove_old_recovery_dir");
    opts.extra_tserver_flags.push_back("--tablet_server_rpc_bind_addresses=127.0.0.1:705${index}");
    if (!FLAGS_ts_flags.empty()) {
      vector<string> flags = strings::Split(FLAGS_ts_flags, " ");
      BOOST_FOREACH(const string& flag, flags) {
        opts.extra_tserver_flags.push_back(flag);
      }
    }
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_STATUS_OK(cluster_->Start());
    KuduClientBuilder builder;
    ASSERT_STATUS_OK(cluster_->CreateClient(builder, &client_));
    tester_.reset(new LinkedListTester(client_, kTableName,
                                       FLAGS_num_chains,
                                       FLAGS_num_tablets,
                                       FLAGS_num_replicas,
                                       FLAGS_enable_mutation));
  }

 protected:
  static const char* kTableName;
  gscoped_ptr<ExternalMiniCluster> cluster_;
  std::tr1::shared_ptr<KuduClient> client_;
  gscoped_ptr<LinkedListTester> tester_;
};

const char *LinkedListTest::kTableName = "linked_list";

TEST_F(LinkedListTest, TestLoadAndVerify) {
  if (FLAGS_seconds_to_run == 0) {
    FLAGS_seconds_to_run = AllowSlowTests() ? kDefaultRunTimeSlow : kDefaultRunTimeFast;
  }

  ASSERT_STATUS_OK(tester_->CreateLinkedListTable());

  int64_t written = 0;
  ASSERT_STATUS_OK(tester_->LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run),
                                           &written));

  // TODO: currently we don't use hybridtime on the C++ client, so it's possible when we
  // scan after writing we may not see all of our writes (we may scan a replica). So,
  // we use WaitAndVerify here instead of a plain Verify.
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  // Check in-memory state with a downed TS. Scans may try other replicas.
  if (FLAGS_num_tablet_servers > 1) {
    cluster_->tablet_server(0)->Shutdown();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  }

  // Kill and restart the cluster, verify data remains.
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  // We need to loop here because the tablet may spend some time in BOOTSTRAPPING state
  // initially after a restart. TODO: Scanner should support its own retries in this circumstance.
  // Remove this loop once client is more fleshed out.
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  // Check post-replication state with a downed TS.
  if (FLAGS_num_tablet_servers > 1) {
    cluster_->tablet_server(0)->Shutdown();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  }

  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  // Sleep a little bit, so that the tablet is proably in bootstrapping state.
  usleep(100 * 1000);
  // Restart while bootstrapping
  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));

  // Dump the performance info at the very end, so it's easy to read. On a failed
  // test, we don't care about this stuff anwyay.
  tester_->DumpInsertHistogram(true);
}

} // namespace kudu
