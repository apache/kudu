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

#include <array>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/substitute.h"
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
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
class HostPort;
}  // namespace kudu

using std::multimap;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_kudu_tserver_TabletServerAdminService_CreateTablet);

namespace kudu {

using client::KuduClient;
using client::KuduSchema;
using cluster::ClusterNodes;

const char* const kTableName = "test-table";

class CreateTableITest : public ExternalMiniClusterITestBase {
};

Status GetNumCreateTabletRPCs(const HostPort& http_hp, int64_t* num_rpcs) {
  return itest::GetInt64Metric(
      http_hp,
      &METRIC_ENTITY_server,
      "kudu.tabletserver",
      &METRIC_handler_latency_kudu_tserver_TabletServerAdminService_CreateTablet,
      "total_count",
      num_rpcs);
}

// Regression test for an issue seen when we fail to create a majority of the
// replicas in a tablet. Previously, we'd still consider the tablet "RUNNING"
// on the master and finish the table creation, even though that tablet would
// be stuck forever with its minority never able to elect a leader.
TEST_F(CreateTableITest, TestCreateWhenMajorityOfReplicasFailCreation) {
  constexpr int kNumReplicas = 3;
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
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&client_schema)
            .set_range_partition_columns({ "key" })
            .num_replicas(kNumReplicas)
            .wait(false)
            .Create());

  // Sleep until we've seen a couple retries on our live server.
  int64_t num_create_attempts = 0;
  while (num_create_attempts < 3) {
    SleepFor(MonoDelta::FromMilliseconds(100));
    ASSERT_OK(GetNumCreateTabletRPCs(cluster_->tablet_server(0)->bound_http_hostport(),
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
  ASSERT_EVENTUALLY([&] {
    bool in_progress = true;
    ASSERT_OK(client_->IsCreateTableInProgress(kTableName, &in_progress));
    ASSERT_FALSE(in_progress);
  });

  // At this point, table has been successfully created. All the tablet servers should be left
  // with only one tablet, eventually, since the tablets which failed to get created properly
  // should get deleted. It's possible there are some delete tablet RPCs still inflight and
  // not processed yet.

  // map of tablet id and count of replicas found.
  std::unordered_map<string, int> tablet_num_replica_map;
  ASSERT_EVENTUALLY([&] {
    tablet_num_replica_map.clear();
    for (int i = 0; i < kNumReplicas; i++) {
      for (const auto& tablet_id : inspect_->ListTabletsWithDataOnTS(i))  {
        tablet_num_replica_map[tablet_id]++;
      }
    }
    LOG(INFO) << strings::Substitute(
        "Waiting for only one tablet to be left with $0 replicas. Currently have: $1 tablet(s)",
        kNumReplicas, tablet_num_replica_map.size());
    ASSERT_EQ(1, tablet_num_replica_map.size());
    // Assertion failure on the line above won't execute lines below under ASSERT_EVENTUALLY.
    // So only one entry is present in the map at this point.
    const auto num_replicas_found = tablet_num_replica_map.begin()->second;
    ASSERT_EQ(kNumReplicas, num_replicas_found);
  });

  // Verify no additional create tablet RPCs are being serviced.
  std::array<int64_t, kNumReplicas> num_create_attempts_arr{};
  for (int i = 0; i < kNumReplicas; i++) {
    ASSERT_OK(GetNumCreateTabletRPCs(cluster_->tablet_server(i)->bound_http_hostport(),
                                     &num_create_attempts_arr[i]));
  }
  for (int repeat_count = 0; repeat_count < 10; repeat_count++) {
    SleepFor(MonoDelta::FromMilliseconds(100));
    for (int i = 0; i < kNumReplicas; i++) {
      int64_t num_rpcs = 0;
      ASSERT_OK(GetNumCreateTabletRPCs(cluster_->tablet_server(i)->bound_http_hostport(),
                                       &num_rpcs));
      ASSERT_EQ(num_create_attempts_arr[i], num_rpcs);
    }
  }
}

// Regression test for KUDU-1317. Ensure that, when a table is created,
// the tablets are well spread out across the machines in the cluster and
// that recovery from failures will be well parallelized.
TEST_F(CreateTableITest, TestSpreadReplicasEvenly) {
  const int kNumServers = 10;
  const int kNumTablets = 20;
  NO_FATALS(StartCluster({}, {}, kNumServers));

  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
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

// Regression test for KUDU-2823. Ensure that, after we add some new tablet servers
// to the cluster, the new tablets are evenly distributed in the cluster based on
// dimensions.
TEST_F(CreateTableITest, TestSpreadReplicasEvenlyWithDimension) {
  const int kNumServers = 10;
  const int kNumTablets = 20;
  vector<int32_t> num_new_replicas(kNumServers, 0);
  vector<string> master_flags = {
      "--tserver_last_replica_creations_halflife_ms=10",
  };
  // We have five tablet servers.
  NO_FATALS(StartCluster({}, master_flags, kNumServers / 2));

  Schema schema = Schema({ ColumnSchema("key1", INT32),
                           ColumnSchema("key2", INT32),
                           ColumnSchema("int_val", INT32),
                           ColumnSchema("string_val", STRING, true) }, 2);
  auto client_schema = KuduSchema::FromSchema(schema);

  auto create_table_func = [](KuduClient* client,
                              KuduSchema* client_schema,
                              const string& table_name,
                              int32_t range_lower_bound,
                              int32_t range_upper_bound,
                              const string& dimension_label) -> Status {
    unique_ptr<client::KuduTableCreator> table_creator(client->NewTableCreator());
    unique_ptr<KuduPartialRow> lower_bound(client_schema->NewRow());
    RETURN_NOT_OK(lower_bound->SetInt32("key2", range_lower_bound));
    unique_ptr<KuduPartialRow> upper_bound(client_schema->NewRow());
    RETURN_NOT_OK(upper_bound->SetInt32("key2", range_upper_bound));
    return table_creator->table_name(table_name)
        .schema(client_schema)
        .add_hash_partitions({ "key1" }, kNumTablets)
        .set_range_partition_columns({ "key2" })
        .add_range_partition(lower_bound.release(), upper_bound.release())
        .num_replicas(3)
        .dimension_label(dimension_label)
        .Create();
  };

  auto alter_table_func = [](KuduClient* client,
                             KuduSchema* client_schema,
                             const string& table_name,
                             int32_t range_lower_bound,
                             int32_t range_upper_bound,
                             const string& dimension_label) -> Status {
    unique_ptr<client::KuduTableAlterer> table_alterer(client->NewTableAlterer(table_name));
    unique_ptr<KuduPartialRow> lower_bound(client_schema->NewRow());
    RETURN_NOT_OK(lower_bound->SetInt32("key2", range_lower_bound));
    unique_ptr<KuduPartialRow> upper_bound(client_schema->NewRow());
    RETURN_NOT_OK(upper_bound->SetInt32("key2", range_upper_bound));
    return table_alterer->AddRangePartitionWithDimension(lower_bound.release(),
                                                         upper_bound.release(),
                                                         dimension_label)
                        ->Alter();
  };

  auto calc_stddev_func = [](const vector<int32_t>& num_replicas,
                             double mean_per_ts,
                             int32_t ts_idx_start,
                             int32_t ts_idx_end) {
    double sum_squared_deviation = 0;
    for (int ts_idx = ts_idx_start; ts_idx < ts_idx_end; ts_idx++) {
      int num_ts = num_replicas[ts_idx];
      LOG(INFO) << "TS " << ts_idx << " has " << num_ts << " tablets";
      double deviation = static_cast<double>(num_ts) - mean_per_ts;
      sum_squared_deviation += deviation * deviation;
    }
    return sqrt(sum_squared_deviation / (mean_per_ts - 1));
  };

  {
    for (int ts_idx = 0; ts_idx < kNumServers / 2; ts_idx++) {
      num_new_replicas[ts_idx] = inspect_->ListTabletsOnTS(ts_idx).size();
    }
    // create the 'test-table1' table with 'label1'.
    ASSERT_OK(create_table_func(client_.get(), &client_schema, "test-table1", 0, 100, "label1"));
    for (int ts_idx = 0; ts_idx < kNumServers / 2; ts_idx++) {
      int num_replicas = inspect_->ListTabletsOnTS(ts_idx).size();
      num_new_replicas[ts_idx] = num_replicas - num_new_replicas[ts_idx];
    }
    // Check that the replicas are fairly well spread by computing the standard
    // deviation of the number of replicas per alive server.
    const double kMeanPerServer = kNumTablets * 3.0 / kNumServers * 2;
    double stddev = calc_stddev_func(
        num_new_replicas, kMeanPerServer, 0, kNumServers / 2);
    LOG(INFO) << "stddev = " << stddev;
    ASSERT_LE(stddev, 3.0);
  }

  // Waiting for the recent creation replicas to decay to 0.
  SleepFor(MonoDelta::FromMilliseconds(1000));

  // Add five new tablet servers to cluster.
  for (int ts_idx = kNumServers / 2; ts_idx < kNumServers; ts_idx++) {
    ASSERT_OK(cluster_->AddTabletServer());
  }
  ASSERT_OK(cluster_->WaitForTabletServerCount(kNumServers, MonoDelta::FromSeconds(60)));

  {
    for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
      num_new_replicas[ts_idx] = inspect_->ListTabletsOnTS(ts_idx).size();
    }
    // create the 'test-table2' table with 'label2'.
    ASSERT_OK(create_table_func(client_.get(), &client_schema, "test-table2", 0, 100, "label2"));
    for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
      int num_replicas = inspect_->ListTabletsOnTS(ts_idx).size();
      num_new_replicas[ts_idx] = num_replicas - num_new_replicas[ts_idx];
    }
    // Check that the replicas are fairly well spread by computing the standard
    // deviation of the number of replicas per server.
    const double kMeanPerServer = kNumTablets * 3.0 / kNumServers;
    double stddev = calc_stddev_func(num_new_replicas, kMeanPerServer, 0, kNumServers);
    LOG(INFO) << "stddev = " << stddev;
    ASSERT_LE(stddev, 3.0);
  }

  // Waiting for the recent creation replicas to decay to 0.
  SleepFor(MonoDelta::FromMilliseconds(1000));

  {
    for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
      num_new_replicas[ts_idx] = inspect_->ListTabletsOnTS(ts_idx).size();
    }
    // Add partition with 'label3' to 'test-table1'
    ASSERT_OK(alter_table_func(client_.get(), &client_schema, "test-table1", 100, 200, "label3"));
    for (int ts_idx = 0; ts_idx < kNumServers; ts_idx++) {
      int num_replicas = inspect_->ListTabletsOnTS(ts_idx).size();
      num_new_replicas[ts_idx] = num_replicas - num_new_replicas[ts_idx];
    }
    // Check that the replicas are fairly well spread by computing the standard
    // deviation of the number of replicas per server.
    const double kMeanPerServer = kNumTablets * 3.0 / kNumServers;
    double stddev = calc_stddev_func(num_new_replicas, kMeanPerServer, 0, kNumServers);
    LOG(INFO) << "stddev = " << stddev;
    ASSERT_LE(stddev, 3.0);
  }
}

static void LookUpRandomKeysLoop(const std::shared_ptr<master::MasterServiceProxy>& master,
                                 const char* table_name,
                                 AtomicBool* quit) {
  Schema schema(GetSimpleTestSchema());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  unique_ptr<KuduPartialRow> r(client_schema.NewRow());

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
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());

  // Don't bother waiting for table creation to finish; it'll never happen
  // because all of the tservers are dead.
  CHECK_OK(table_creator->table_name(kTableName)
           .schema(&client_schema)
           .set_range_partition_columns({ "key" })
           .wait(false)
           .Create());

  // Spin off a bunch of threads that repeatedly look up random key ranges in the table.
  constexpr int kNumThreads = 16;
  AtomicBool quit(false);
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    auto proxy = cluster_->master_proxy();
    threads.emplace_back([proxy, kTableName, &quit]() {
      LookUpRandomKeysLoop(proxy, kTableName, &quit);
    });
  }
  SCOPED_CLEANUP({
    quit.Store(true);
    for (auto& t : threads) {
      t.join();
    }
  });

  // Give the lookup threads some time to crash the master.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(15);
  while (MonoTime::Now() < deadline) {
    ASSERT_TRUE(cluster_->master()->IsProcessAlive()) << "Master crashed!";
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
}

} // namespace kudu
