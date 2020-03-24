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
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(raft_prepare_replacement_before_eviction);

METRIC_DECLARE_counter(sys_catalog_oversized_write_requests);

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::ReplicaManagementInfoPB;
using kudu::itest::GetInt64Metric;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

class TSDescriptor;

const char * const kTableId1 = "testMasterReplication-1";
const char * const kTableId2 = "testMasterReplication-2";

const int kNumTabletServerReplicas = 3;

class MasterReplicationTest : public KuduTest {
 public:
  MasterReplicationTest() {
    opts_.num_masters = 3;
    opts_.num_tablet_servers = kNumTabletServerReplicas;
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, opts_));
    ASSERT_OK(cluster_->Start());
  }

  Status RestartCluster() {
    cluster_->Shutdown();
    RETURN_NOT_OK(cluster_->Start());
    return Status::OK();
  }

  // This method is meant to be run in a separate thread.
  void StartClusterDelayed(int64_t millis) {
    LOG(INFO) << "Sleeping for "  << millis << " ms...";
    SleepFor(MonoDelta::FromMilliseconds(millis));
    LOG(INFO) << "Attempting to start the cluster...";
    CHECK_OK(cluster_->Start());
  }

  void ListMasterServerAddrs(vector<string>* out) {
    for (const auto& hostport : cluster_->master_rpc_addrs()) {
      out->emplace_back(hostport.ToString());
    }
  }

  // Shut the cluster down, start initializing the client, and then
  // bring the cluster back up during the initialization (but before the
  // timeout can elapse).
  Status ConnectToClusterDuringStartup(const vector<string>& master_addrs) {
    // Shut the cluster down and ...
    cluster_->Shutdown();
    // ... start the cluster after a 1000 ms delay.
    thread start_thread([this]() { this->StartClusterDelayed(1000); });
    SCOPED_CLEANUP({
      start_thread.join();
    });

    // The timeouts for both RPCs and operations are increased to cope with slow
    // clusters (i.e. TSAN builds).
    shared_ptr<KuduClient> client;
    KuduClientBuilder builder;
    builder.master_server_addrs(master_addrs);
    builder.default_admin_operation_timeout(MonoDelta::FromSeconds(90));
    builder.default_rpc_timeout(MonoDelta::FromSeconds(15));
    return builder.Build(&client);
  }

  Status CreateClient(shared_ptr<KuduClient>* out) {
    KuduClientBuilder builder;
    for (int i = 0; i < cluster_->num_masters(); i++) {
      if (!cluster_->mini_master(i)->master()->IsShutdown()) {
        builder.add_master_server_addr(cluster_->mini_master(i)->bound_rpc_addr_str());
      }
    }
    return builder.Build(out);
  }


  Status CreateTable(const shared_ptr<KuduClient>& client,
                     const std::string& table_name) {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->NotNull();
    CHECK_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    return table_creator->table_name(table_name)
        .set_range_partition_columns({ "key" })
        .schema(&schema)
        .Create();
  }

 protected:
  InternalMiniClusterOptions opts_;
  unique_ptr<InternalMiniCluster> cluster_;
};

// Basic test. Verify that:
//
// 1) We can start multiple masters in a distributed configuration and
// that the clients and tablet servers can connect to the leader
// master.
//
// 2) We can create a table (using the standard client APIs) on the
// the leader and ensure that the appropriate table/tablet info is
// replicated to the newly elected leader.
TEST_F(MasterReplicationTest, TestSysTablesReplication) {
  shared_ptr<KuduClient> client;

  // Create the first table.
  ASSERT_OK(CreateClient(&client));
  ASSERT_OK(CreateTable(client, kTableId1));

  // Repeat the same for the second table.
  ASSERT_OK(CreateTable(client, kTableId2));

  // Verify that both tables exist. There can be a leader election at any time
  // so we need to loop and try all masters.
  while (true) {
    for (int i = 0; i < cluster_->num_masters(); i++) {
      Master* master = cluster_->mini_master(i)->master();
      CatalogManager* catalog = master->catalog_manager();
      CatalogManager::ScopedLeaderSharedLock l(catalog);
      if (l.first_failed_status().ok()) {
        ASSERT_EQ(1, master->num_raft_leaders()->value());
        bool exists;
        ASSERT_OK(catalog->TableNameExists(kTableId1, &exists));
        ASSERT_TRUE(exists);
        ASSERT_OK(catalog->TableNameExists(kTableId2, &exists));
        ASSERT_TRUE(exists);
        return;
      } else {
        ASSERT_EQ(0, master->num_raft_leaders()->value());
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
}

// When all masters are down, test that we can timeout the connection
// attempts after a specified deadline.
TEST_F(MasterReplicationTest, TestTimeoutWhenAllMastersAreDown) {
  vector<string> master_addrs;
  ListMasterServerAddrs(&master_addrs);

  cluster_->Shutdown();

  shared_ptr<KuduClient> client;
  KuduClientBuilder builder;
  builder.master_server_addrs(master_addrs);
  builder.default_rpc_timeout(MonoDelta::FromMilliseconds(100));
  Status s = builder.Build(&client);
  EXPECT_TRUE(!s.ok());
  EXPECT_TRUE(s.IsTimedOut());

  // We need to reset 'cluster_' so that TearDown() can run correctly.
  cluster_.reset();
}

TEST_F(MasterReplicationTest, TestCycleThroughAllMasters) {
  vector<string> master_addrs;
  ListMasterServerAddrs(&master_addrs);

  // Verify that the client doesn't give up even though the entire
  // cluster is down for a little while.
  EXPECT_OK(ConnectToClusterDuringStartup(master_addrs));

  // Verify that if the client was configure with more masters than actual masters
  // in the cluster, it would also keep retrying to connect to the cluster even though
  // it couldn't find a leader master for a little while.
  master_addrs.emplace_back("127.0.0.1:55555");
  EXPECT_OK(ConnectToClusterDuringStartup(master_addrs));
}

// Test that every master accepts heartbeats, and that a heartbeat to any
// master updates its TSDescriptor cache.
TEST_F(MasterReplicationTest, TestHeartbeatAcceptedByAnyMaster) {
  // Register a fake tserver with every master.
  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid("fake-ts-uuid");
  common.mutable_ts_instance()->set_instance_seqno(1);
  ServerRegistrationPB fake_reg;
  HostPortPB* pb = fake_reg.add_rpc_addresses();
  pb->set_host("localhost");
  pb->set_port(1000);
  pb = fake_reg.add_http_addresses();
  pb->set_host("localhost");
  pb->set_port(2000);
  std::shared_ptr<rpc::Messenger> messenger;
  rpc::MessengerBuilder bld("Client");
  ASSERT_OK(bld.Build(&messenger));

  // Information on replica management scheme.
  ReplicaManagementInfoPB rmi;
  rmi.set_replacement_scheme(FLAGS_raft_prepare_replacement_before_eviction
      ? ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
      : ReplicaManagementInfoPB::EVICT_FIRST);

  for (int i = 0; i < cluster_->num_masters(); i++) {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    rpc::RpcController rpc;

    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);

    const auto& addr = cluster_->mini_master(i)->bound_rpc_addr();
    MasterServiceProxy proxy(messenger, addr, addr.host());

    // All masters (including followers) should accept the heartbeat.
    ASSERT_OK(proxy.TSHeartbeat(req, &resp, &rpc));
    SCOPED_TRACE(pb_util::SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // Now each master should have four registered tservers.
  vector<std::shared_ptr<TSDescriptor>> descs;
  ASSERT_OK(cluster_->WaitForTabletServerCount(
      kNumTabletServerReplicas + 1,
      InternalMiniCluster::MatchMode::DO_NOT_MATCH_TSERVERS, &descs));
}

TEST_F(MasterReplicationTest, TestMasterPeerSetsDontMatch) {
  // Restart one master with an additional entry in --master_addresses. The
  // discrepancy with the on-disk list of masters should trigger a failure.
  vector<HostPort> master_rpc_addrs = cluster_->master_rpc_addrs();
  cluster_->mini_master(0)->Shutdown();
  master_rpc_addrs.emplace_back("127.0.0.1", 55555);
  cluster_->mini_master(0)->SetMasterAddresses(master_rpc_addrs);
  ASSERT_OK(cluster_->mini_master(0)->Start());
  Status s = cluster_->mini_master(0)->WaitForCatalogManagerInit();
  SCOPED_TRACE(s.ToString());
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "55555");
}

TEST_F(MasterReplicationTest, TestConnectToClusterReturnsAddresses) {
  for (int i = 0; i < cluster_->num_masters(); i++) {
    SCOPED_TRACE(Substitute("Connecting to master $0", i));
    auto proxy = cluster_->master_proxy(i);
    rpc::RpcController rpc;
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    ASSERT_OK(proxy->ConnectToMaster(req, &resp, &rpc));
    ASSERT_EQ(cluster_->num_masters(), resp.master_addrs_size());
    for (int j = 0; j < cluster_->num_masters(); j++) {
      const auto& addr = resp.master_addrs(j);
      ASSERT_EQ(cluster_->mini_master(j)->bound_rpc_addr().ToString(),
                Substitute("$0:$1", addr.host(), addr.port()));
    }
  }
}


// Test for KUDU-2200: if a user specifies just one of the masters, and that master is a
// follower, we should give a status message that explains their mistake.
TEST_F(MasterReplicationTest, TestConnectToFollowerMasterOnly) {
  int successes = 0;
  for (int i = 0; i < cluster_->num_masters(); i++) {
    SCOPED_TRACE(Substitute("Connecting to master $0", i));

    shared_ptr<KuduClient> client;
    KuduClientBuilder builder;
    builder.add_master_server_addr(cluster_->mini_master(i)->bound_rpc_addr_str());
    Status s = builder.Build(&client);
    if (s.ok()) {
      successes++;
    } else {
      ASSERT_STR_MATCHES(s.ToString(),
                         R"(Configuration error: .*Client configured with 1 master\(s\) \(.+\) )"
                         R"(but cluster indicates it expects 3.*)");
    }
  }
  // It's possible that we get either 0 or 1 success in the above loop:
  // - 0, in the case that no master had elected itself yet
  // - 1, in the case that one master had become leader by the time we connected.
  EXPECT_LE(successes, 1);
}

// In this test, a Kudu master receives RPC under the maximum size limit,
// however the corresponding update on the system tablet would be greater than.
class MasterReplicationAndRpcSizeLimitTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(Prepare());
  }

 protected:
  static constexpr const char* const kKeyColumnName = "key";
  static constexpr auto kNumMasters = 3;
  static constexpr auto kNumTabletServers = 3;
  static constexpr auto kReplicationFactor = 3;
  // Shorten the Raft election timeout intervals to speed up the test.
  static constexpr auto kHbIntervalMs = 500;
  static constexpr auto kMaxMissedHbs = 2;

  Status Prepare() {
    const vector<string> ts_extra_flags = {
      // Set custom timings for Raft heartbeats and heard-from-leader timeouts.
      Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),
      Substitute("--leader_failure_max_missed_heartbeat_periods=$0", kMaxMissedHbs),
      // This test scenario creates many replicas per tablet server and causes
      // multiple re-elections, so it's necessary to accommodate for spikes in
      // Raft heartbeat traffic coming from one tablet server to another,
      // especially in case of sanitizer builds.
      "--rpc_service_queue_length=200",
    };
    const vector<string> master_extra_flags = {
      // Set custom timings for Raft heartbeats and heard-from-leader timeouts.
      Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),
      Substitute("--leader_failure_max_missed_heartbeat_periods=$0", kMaxMissedHbs),
      // Turn off the validator for the --rpc_max_message_size flag since this
      // scenario uses non-conventional setting for the flag.
      "--rpc_max_message_size_enable_validation=false",
      // Set the maximum size for the master RPC to 64 KiByte.
      Substitute("--rpc_max_message_size=$0", 64 * 1024),
      // The updates on the system catalog tablet might be accumulated by Raft
      // in various scenarios due to connectivity, leadership changes, etc.
      // Substracting an extra 1K to account for extra fields while wrapping
      // messages to replicate into UpdateConsensus RPC.
      Substitute("--consensus_max_batch_size_bytes=$0", 63 * 1024),
      // The TabletReports scenario first verifies that master rejects tablet
      // reports which would lead to oversized updates on the system catalog
      // tablet, and then it toggles the flag in run time.
      "--catalog_manager_enable_chunked_tablet_reports=false",
    };

    ExternalMiniClusterOptions opts;
    opts.num_masters = kNumMasters;
    opts.num_tablet_servers = kNumTabletServers;
    opts.extra_master_flags = master_extra_flags;
    opts.extra_tserver_flags = ts_extra_flags;
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    RETURN_NOT_OK(cluster_->Start());
    return cluster_->CreateClient(nullptr, &client_);
  }

  // Create a table named 'table_name' with pre-defined structure.
  Status CreateTable(const string& table_name, int replication_factor) {
    // In this test scenario, long dimension labels are used to make
    // the corresponding update on the system tablet longer than the incoming
    // RPC to master (e.g. a tablet report or AlterTable request). In real life,
    // it's possible to achieve the same by other means, but it would be
    // necessary to create many more tablet replicas in the cluster.
    static const char* const kLabelSuffix =
        "_very_looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "ooooooooooooooooooooooooooooooooooooooooooooooooonooooog_label_suffix";
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("string_column")->Type(KuduColumnSchema::STRING);
    RETURN_NOT_OK(b.Build(&schema_));

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    const auto s = table_creator->table_name(table_name)
        .schema(&schema_)
        .set_range_partition_columns({ kKeyColumnName })
        .add_hash_partitions({ kKeyColumnName }, 10)
        .num_replicas(replication_factor)
        .dimension_label(table_name + kLabelSuffix)
        .Create();
    return s;
  }

  // Get sum of values for the specified metric across all masters in the
  // cluster.
  Status GetMetric(const MetricPrototype& metric_proto, int64_t* sum) {
    int64_t result = 0;
    for (auto idx = 0; idx < kNumMasters; ++idx) {
      int64_t val;
      RETURN_NOT_OK(GetInt64Metric(cluster_->master(idx)->bound_http_hostport(),
                                   &METRIC_ENTITY_server,
                                   nullptr,
                                   &metric_proto,
                                   "value",
                                   &val));
      CHECK_GE(val, 0);
      result += val;
    }
    *sum = result;
    return Status::OK();
  }

  unique_ptr<cluster::ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<client::KuduClient> client_;
  KuduSchema schema_;
};

// Make sure leader master rejects AlterTable requests which result in updates
// on the system tablet which it would not be able to push to its followers
// due to the limit set by the --rpc_max_message_size flag.
//
// This scenario simulates conditions described in KUDU-3036.
TEST_F(MasterReplicationAndRpcSizeLimitTest, AlterTable) {
  const string table_name = "table_to_alter";
  ASSERT_OK(CreateTable(table_name, kReplicationFactor));

  // After fresh start, there should be no rejected writes to the system catalog
  // tablet yet.
  {
    int64_t val;
    ASSERT_OK(GetMetric(METRIC_sys_catalog_oversized_write_requests, &val));
    ASSERT_EQ(0, val);
  }

  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(table_name));
  alterer->DropRangePartition(schema_.NewRow(), schema_.NewRow());
  for (auto i = 0; i < 50; ++i) {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(lower->SetInt64(kKeyColumnName, 10 * i));
    ASSERT_OK(upper->SetInt64(kKeyColumnName, 10 * (i + 1)));
    alterer->AddRangePartition(lower.release(), upper.release());
  }
  auto s = alterer->timeout(MonoDelta::FromSeconds(30))->Alter();

  // The DDL attempt above (i.e. the Alter() call) produces an oversized write
  // request to the system catalog tablet. The request should have been rejected
  // and the corresponding metric incremented.
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "too large for current setting of the "
                                    "--rpc_max_message_size flag");

  // Leader master can change after the ALTER TABLE request above and the time
  // when collecting the metric value below.
  {
    int64_t val;
    ASSERT_OK(GetMetric(METRIC_sys_catalog_oversized_write_requests, &val));
    ASSERT_EQ(1, val);
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

// In this scenario, Kudu tablet servers send Kudu master tablet reports which
// are under the maximum RPC size limit, however the corresponding update
// on the system tablet would be greater than that if lumping together updates
// for every tablet. If the --catalog_manager_enable_chunked_tablet_reports
// flag is set to 'false', Kudu masters should reject such reports. If the
// flag set to 'true', Kudu masters should chunk the result write request to
// the system catalog, so corresponding UpdateConsensus RPCs are not rejected
// by follower masters due to the limit on the maximum RPC size.
//
// This scenario simulates conditions described in KUDU-3016.
TEST_F(MasterReplicationAndRpcSizeLimitTest, TabletReports) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  for (auto idx = 0; idx < 10; ++idx) {
    ASSERT_OK(CreateTable(Substitute("table_$0", idx), kReplicationFactor));
  }

  // After fresh start, there should be no rejected writes to the system catalog
  // tablet yet.
  int64_t val;
  ASSERT_OK(GetMetric(METRIC_sys_catalog_oversized_write_requests, &val));
  ASSERT_EQ(0, val);

  // Stop all masters: they will be restarted later to receive tablet reports.
  for (auto idx = 0; idx < kNumMasters; ++idx) {
    cluster_->master(idx)->Shutdown();
  }

  // Pause and resume tablet servers to make the tablets re-elect their leaders,
  // so Raft configuration for every tablet is updated in the end of the cycle
  // because of the fresh Raft terms. The result distribution of leader replicas
  // makes them concentrated at two tablet servers out of three, which results
  // in two larger tablet reports.
  for (auto idx = 0; idx < kNumTabletServers; ++idx) {
    ASSERT_OK(cluster_->tablet_server(idx)->Pause());
    // Allow for leader re-election to happen.
    SleepFor(MonoDelta::FromMilliseconds(3 * kMaxMissedHbs * kHbIntervalMs));
    ASSERT_OK(cluster_->tablet_server(idx)->Resume());
    SleepFor(MonoDelta::FromMilliseconds(2 * kHbIntervalMs));
  }

  // Start all masters. The tablet servers should send full (non-incremental)
  // tablet reports to the leader master once hearing from it.
  for (auto idx = 0; idx < kNumMasters; ++idx) {
    ASSERT_OK(cluster_->master(idx)->Restart());
  }

  // Since the chunked updates on the system catalog tablet is disabled by
  // default, masters should reject tablet reports that would result in
  // oversized updates on the system catalog tablet.
  ASSERT_EVENTUALLY([&] {
    int64_t val;
    ASSERT_OK(GetMetric(METRIC_sys_catalog_oversized_write_requests, &val));
    ASSERT_GT(val, 0);
  });

  for (auto idx = 0; idx < kNumMasters; ++idx) {
    ASSERT_OK(cluster_->SetFlag(cluster_->master(idx),
                                "catalog_manager_enable_chunked_tablet_reports",
                                "true"));
  }

  ASSERT_OK(cluster_->WaitForTabletServerCount(
      kNumTabletServers, MonoDelta::FromSeconds(60)));

  // Run a test workload and make sure the system is operable. Prior to
  // KUDU-3016 fix, the scenario above would lead to a DoS situation.
  TestWorkload w(cluster_.get());
  w.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
  w.set_num_replicas(kReplicationFactor);
  w.set_num_write_threads(2);
  w.set_num_read_threads(2);
  w.Setup();
  w.Start();
  SleepFor(MonoDelta::FromSeconds(3));
  w.StopAndJoin();

  NO_FATALS(cluster_->AssertNoCrashes());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(
      w.table_name(), ClusterVerifier::EXACTLY, w.rows_inserted()));
}

} // namespace master
} // namespace kudu
