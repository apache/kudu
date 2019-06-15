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

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/atomic.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::multimap;
using std::set;
using std::string;
using std::vector;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_kudu_tserver_TabletServerAdminService_CreateTablet);

namespace kudu {

using client::KuduSchema;
using cluster::ClusterNodes;

const char* const kTableName = "test-table";

class CreateTableITest : public ExternalMiniClusterITestBase {
};

// Regression test for an issue seen when we fail to create a majority of the
// replicas in a tablet. Previously, we'd still consider the tablet "RUNNING"
// on the master and finish the table creation, even though that tablet would
// be stuck forever with its minority never able to elect a leader.
TEST_F(CreateTableITest, TestCreateWhenMajorityOfReplicasFailCreation) {
  const int kNumReplicas = 3;
  vector<string> ts_flags;
  vector<string> master_flags;
  master_flags.emplace_back("--tablet_creation_timeout_ms=1000");
  NO_FATALS(StartCluster(ts_flags, master_flags, kNumReplicas));

  // Shut down 2/3 of the tablet servers.
  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();

  // Try to create a single-tablet table.
  // This won't succeed because we can't create enough replicas to get
  // a quorum.
  gscoped_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&client_schema)
            .set_range_partition_columns({ "key" })
            .num_replicas(3)
            .wait(false)
            .Create());

  // Sleep until we've seen a couple retries on our live server.
  int64_t num_create_attempts = 0;
  while (num_create_attempts < 3) {
    SleepFor(MonoDelta::FromMilliseconds(100));
    ASSERT_OK(itest::GetInt64Metric(
        cluster_->tablet_server(0)->bound_http_hostport(),
        &METRIC_ENTITY_server,
        "kudu.tabletserver",
        &METRIC_handler_latency_kudu_tserver_TabletServerAdminService_CreateTablet,
        "total_count",
        &num_create_attempts));
    LOG(INFO) << "Waiting for the master to retry creating the tablet 3 times... "
              << num_create_attempts << " RPCs seen so far";

    // The CreateTable operation should still be considered in progress, even though
    // we'll be successful at creating a single replica.
    bool in_progress = false;
    ASSERT_OK(client_->IsCreateTableInProgress(kTableName, &in_progress));
    ASSERT_TRUE(in_progress);
  }

  // Once we restart the servers, we should succeed at creating a healthy
  // replicated tablet.
  ASSERT_OK(cluster_->tablet_server(1)->Restart());
  ASSERT_OK(cluster_->tablet_server(2)->Restart());

  // We should eventually finish the table creation we started earlier.
  bool in_progress = false;
  while (in_progress) {
    LOG(INFO) << "Waiting for the master to successfully create the table...";
    ASSERT_OK(client_->IsCreateTableInProgress(kTableName, &in_progress));
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  // The server that was up from the beginning should be left with only
  // one tablet, eventually, since the tablets which failed to get created
  // properly should get deleted.
  vector<string> tablets;
  int wait_iter = 0;
  while (tablets.size() != 1 && wait_iter++ < 100) {
    LOG(INFO) << "Waiting for only one tablet to be left on TS 0. Currently have: "
              << tablets;
    SleepFor(MonoDelta::FromMilliseconds(100));
    tablets = inspect_->ListTabletsWithDataOnTS(0);
  }
  ASSERT_EQ(1, tablets.size()) << "Tablets on TS0: " << tablets;
}

// Regression test for KUDU-1317. Ensure that, when a table is created,
// the tablets are well spread out across the machines in the cluster and
// that recovery from failures will be well parallelized.
TEST_F(CreateTableITest, TestSpreadReplicasEvenly) {
  const int kNumServers = 10;
  const int kNumTablets = 20;
  NO_FATALS(StartCluster({}, {}, kNumServers));

  gscoped_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&client_schema)
            .set_range_partition_columns({ "key" })
            .num_replicas(3)
            .add_hash_partitions({ "key" }, kNumTablets)
            .Create());

  // Check that the replicas are fairly well spread by computing the standard
  // deviation of the number of replicas per server.
  const double kMeanPerServer = kNumTablets * 3.0 / kNumServers;
  double sum_squared_deviation = 0;
  for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
    int num_replicas = inspect_->ListTabletsOnTS(ts_idx).size();
    LOG(INFO) << "TS " << ts_idx << " has " << num_replicas << " tablets";
    double deviation = static_cast<double>(num_replicas) - kMeanPerServer;
    sum_squared_deviation += deviation * deviation;
  }
  double stddev = sqrt(sum_squared_deviation / (kMeanPerServer - 1));
  LOG(INFO) << "stddev = " << stddev;
  // In 1000 runs of the test, only one run had stddev above 2.0. So, 3.0 should
  // be a safe non-flaky choice.
  ASSERT_LE(stddev, 3.0);

  // Construct a map from tablet ID to the set of servers that each tablet is hosted on.
  multimap<string, int> tablet_to_servers;
  for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
    vector<string> tablets = inspect_->ListTabletsOnTS(ts_idx);
    for (const string& tablet_id : tablets) {
      tablet_to_servers.insert(std::make_pair(tablet_id, ts_idx));
    }
  }

  // For each server, count how many other servers it shares tablets with.
  // This is highly correlated to how well parallelized recovery will be
  // in the case the server crashes.
  int sum_num_peers = 0;
  for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
    vector<string> tablets = inspect_->ListTabletsOnTS(ts_idx);
    set<int> peer_servers;
    for (const string& tablet_id : tablets) {
      auto peer_indexes = tablet_to_servers.equal_range(tablet_id);
      for (auto it = peer_indexes.first; it != peer_indexes.second; ++it) {
        peer_servers.insert(it->second);
      }
    }

    peer_servers.erase(ts_idx);
    LOG(INFO) << "Server " << ts_idx << " has " << peer_servers.size() << " peers";
    sum_num_peers += peer_servers.size();
  }

  // On average, servers should have at least half the other servers as peers.
  double avg_num_peers = static_cast<double>(sum_num_peers) / kNumServers;
  LOG(INFO) << "avg_num_peers = " << avg_num_peers;
  ASSERT_GE(avg_num_peers, kNumServers / 2);
}

static void LookUpRandomKeysLoop(std::shared_ptr<master::MasterServiceProxy> master,
                                 const char* table_name,
                                 AtomicBool* quit) {
  Schema schema(GetSimpleTestSchema());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  gscoped_ptr<KuduPartialRow> r(client_schema.NewRow());

  while (!quit->Load()) {
    master::GetTableLocationsRequestPB req;
    master::GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(table_name);

    // Look up random start and end keys, allowing start > end to ensure that
    // the master correctly handles this case too.
    string start_key;
    string end_key;
    CHECK_OK(r->SetInt32("key", rand() % MathLimits<int32_t>::kMax));
    CHECK_OK(r->EncodeRowKey(req.mutable_partition_key_start()));
    CHECK_OK(r->SetInt32("key", rand() % MathLimits<int32_t>::kMax));
    CHECK_OK(r->EncodeRowKey(req.mutable_partition_key_end()));

    rpc::RpcController rpc;

    // Value doesn't matter; just need something to avoid ugly log messages.
    rpc.set_timeout(MonoDelta::FromSeconds(10));

    Status s = master->GetTableLocations(req, &resp, &rpc);

    // Either the lookup was successful or the master crashed.
    CHECK(s.ok() || s.IsNetworkError());
  }
}

// Regression test for a couple of bugs involving tablet lookups
// concurrent with tablet replacements during table creation.
//
// The first bug would crash the master if the table's key range was
// not fully populated. This corner case can occur when:
// 1. Tablet creation tasks time out because their tservers died, and
// 2. The master fails in replica selection when sending tablet creation tasks
//    for tablets replaced because of #1.
//
// The second bug involved a race condition where a tablet is looked up
// halfway through the process of its being added to the table.
//
// This test replicates these conditions and hammers the master with key
// lookups, attempting to reproduce the master crashes.
TEST_F(CreateTableITest, TestCreateTableWithDeadTServers) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping slow test";
    return;
  }

  const char* kTableName = "test";

  // Start up a cluster and immediately kill the tservers. The master will
  // consider them alive long enough to respond successfully to the client's
  // create table request, but won't actually be able to create the tablets.
  NO_FATALS(StartCluster(
      {},
      {
          // The master should quickly time out create tablet tasks. The
          // tservers will all be dead, so there's no point in waiting long.
          "--tablet_creation_timeout_ms=1000",
          // This timeout needs to be long enough that we don't immediately
          // fail the client's create table request, but short enough that the
          // master considers the tservers unresponsive (and recreates the
          // outstanding table's tablets) during the test.
          "--tserver_unresponsive_timeout_ms=5000" }));
  cluster_->ShutdownNodes(ClusterNodes::TS_ONLY);

  Schema schema(GetSimpleTestSchema());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  gscoped_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());

  // Don't bother waiting for table creation to finish; it'll never happen
  // because all of the tservers are dead.
  CHECK_OK(table_creator->table_name(kTableName)
           .schema(&client_schema)
           .set_range_partition_columns({ "key" })
           .wait(false)
           .Create());

  // Spin off a bunch of threads that repeatedly look up random key ranges in the table.
  AtomicBool quit(false);
  vector<scoped_refptr<Thread>> threads;
  for (int i = 0; i < 16; i++) {
    scoped_refptr<Thread> t;
    ASSERT_OK(Thread::Create("test", "lookup_thread",
                             &LookUpRandomKeysLoop, cluster_->master_proxy(),
                             kTableName, &quit, &t));
    threads.push_back(t);
  }

  // Give the lookup threads some time to crash the master.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(15);
  while (MonoTime::Now() < deadline) {
    ASSERT_TRUE(cluster_->master()->IsProcessAlive()) << "Master crashed!";
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  quit.Store(true);

  for (const auto& t : threads) {
    t->Join();
  }
}

} // namespace kudu
