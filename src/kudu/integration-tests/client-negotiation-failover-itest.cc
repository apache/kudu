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

#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMaster;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::ScopedResumeExternalDaemon;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(rpc_reopen_outbound_connections);
DECLARE_int64(rpc_negotiation_timeout_ms);

namespace kudu {

// Series of tests to verify that client fails over to another available server
// if it experiences a timeout on connection negotiation with current server.
// The 'server' can be a master or a tablet server.
class ClientFailoverOnNegotiationTimeoutITest : public KuduTest {
 public:
  ClientFailoverOnNegotiationTimeoutITest() {
    // Since we want to catch timeout during connection negotiation phase,
    // let's make the client re-establishing connections on every RPC.
    FLAGS_rpc_reopen_outbound_connections = true;

    // Set the connection negotiation timeout shorter than its default value
    // to run the test faster. For sanitizer builds we don't want the timeout
    // to be too short: running the test concurrently with other activities
    // might lead client to fail even if the client retries again and again.
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
    FLAGS_rpc_negotiation_timeout_ms = 3000;
#else
    FLAGS_rpc_negotiation_timeout_ms = 1000;
#endif

    cluster_opts_.extra_tserver_flags = {
        // Speed up Raft elections.
        "--raft_heartbeat_interval_ms=25",
        "--leader_failure_exp_backoff_max_delta_ms=1000",
        // Decreasing TS->master heartbeat interval speeds up the test.
        "--heartbeat_interval_ms=25",
    };
    cluster_opts_.extra_master_flags = {
        // Speed up Raft elections.
        "--raft_heartbeat_interval_ms=25",
        "--leader_failure_exp_backoff_max_delta_ms=1000",
    };
  }

  Status CreateAndStartCluster() {
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    return cluster_->Start();
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }
 protected:
  ExternalMiniClusterOptions cluster_opts_;
  shared_ptr<ExternalMiniCluster> cluster_;
};

// Regression test for KUDU-1580: if a client times out on negotiating a connection
// to a tablet server, it should retry with other available tablet server.
TEST_F(ClientFailoverOnNegotiationTimeoutITest, Kudu1580ConnectToTServer) {
  static const int kNumTabletServers = 3;
  static const int kTimeoutMs = 5 * 60 * 1000;
  static const char* kTableName = "kudu1580";

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  cluster_opts_.num_tablet_servers = kNumTabletServers;
  ASSERT_OK(CreateAndStartCluster());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(
      &KuduClientBuilder()
          .default_admin_operation_timeout(MonoDelta::FromMilliseconds(kTimeoutMs))
          .default_rpc_timeout(MonoDelta::FromMilliseconds(kTimeoutMs)),
      &client));
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  KuduSchema schema(client::KuduSchemaFromSchema(CreateKeyValueTestSchema()));
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema)
      .add_hash_partitions({ "key" }, kNumTabletServers)
      .num_replicas(kNumTabletServers)
      .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));

  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(kTimeoutMs);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  // Running multiple iterations to cover possible variations of tablet leader
  // placement among tablet servers.
  for (int i = 0; i < 8 * kNumTabletServers; ++i) {
    vector<unique_ptr<ScopedResumeExternalDaemon>> resumers;
    for (int tsi = 0; tsi < kNumTabletServers; ++tsi) {
      ExternalTabletServer* ts(cluster_->tablet_server(tsi));
      ASSERT_NE(nullptr, ts);
      ASSERT_OK(ts->Pause());
      resumers.emplace_back(new ScopedResumeExternalDaemon(ts));
    }

    // Resume 2 out of 3 tablet servers (i.e. the majority), so the client
    // could eventially succeed with its write operations.
    thread resume_thread([&]() {
        const int idx0 = rand() % kNumTabletServers;
        unique_ptr<ScopedResumeExternalDaemon> r0(resumers[idx0].release());
        const int idx1 = (idx0 + 1) % kNumTabletServers;
        unique_ptr<ScopedResumeExternalDaemon> r1(resumers[idx1].release());
        SleepFor(MonoDelta::FromSeconds(1));
      });
    // An automatic clean-up to handle both success and failure cases
    // in the code below.
    SCOPED_CLEANUP({
        resume_thread.join();
      });

    // Since the table is hash-partitioned with kNumTabletServer partitions,
    // hopefully three sequential numbers would go into different partitions.
    for (int ii = 0; ii < kNumTabletServers; ++ii) {
      unique_ptr<KuduInsert> ins(table->NewInsert());
      ASSERT_OK(ins->mutable_row()->SetInt32(0, kNumTabletServers * i + ii));
      ASSERT_OK(ins->mutable_row()->SetInt32(1, 0));
      ASSERT_OK(session->Apply(ins.release()));
    }
  }
}

// Regression test for KUDU-2021: if client times out on establishing a
// connection to the leader master, it should retry with other master in case of
// a multi-master configuration.
TEST_F(ClientFailoverOnNegotiationTimeoutITest, Kudu2021ConnectToMaster) {
  static const int kNumMasters = 3;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);

  cluster_opts_.num_masters = kNumMasters;
  cluster_opts_.num_tablet_servers = 1;
  ASSERT_OK(CreateAndStartCluster());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(
      &KuduClientBuilder()
          .default_admin_operation_timeout(kTimeout)
          .default_rpc_timeout(kTimeout),
      &client));

  // Make a call to the master to populate the client's metadata cache.
  vector<string> tables;
  ASSERT_OK(client->ListTables(&tables));

  // Pause the leader master so next call client would time out on connection
  // negotiation. Do that few times.
  for (int i = 0; i < kNumMasters; ++i) {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    ASSERT_OK(cluster_->master(leader_idx)->Pause());
    ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

    ASSERT_OK(client->ListTables(&tables));
  }
}

// Regression test for KUDU-2021: if client times out on negotiating a
// connection with the master, it should retry with other master in case of
// a multi-master configuration.
TEST_F(ClientFailoverOnNegotiationTimeoutITest, Kudu2021NegotiateWithMaster) {
  static const int kNumMasters = 3;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);

  cluster_opts_.num_masters = kNumMasters;
  cluster_opts_.num_tablet_servers = 1;
  ASSERT_OK(CreateAndStartCluster());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(
      &KuduClientBuilder()
          .default_admin_operation_timeout(kTimeout)
          .default_rpc_timeout(kTimeout),
      &client));

  // Check client can successfully call ListTables().
  vector<string> tables;
  ASSERT_OK(client->ListTables(&tables));

  // The test sets the client-side RPC negotiation timeout via the flag
  // 'rpc_negotiation_timeout_ms'. We want the client to open a TCP connection
  // and start the negotiation process with the leader master and then time out
  // during the negotiation process. For the test to check the client's behavior
  // on timing out while establishing a TCP connection see the
  // Kudu2021ConnectToMaster test above.
  //
  // So, after the client times out on the negotiation process, it should re-resolve
  // the leader master and connect to the new leader. Since the former leader
  // has been paused in the middle of the negotiation, the client is supposed to
  // connect to the new leader and succeed with its ListTables() call.
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  ExternalMaster* m = cluster_->master(leader_idx);
  ASSERT_OK(
      cluster_->SetFlag(m, "rpc_negotiation_inject_delay_ms",
                        Substitute("$0", FLAGS_rpc_negotiation_timeout_ms * 2)));
  thread pause_thread([&]() {
      SleepFor(MonoDelta::FromMilliseconds(FLAGS_rpc_negotiation_timeout_ms / 2));
      CHECK_OK(m->Pause())
    });
  // An automatic clean-up to handle both success and failure cases.
  SCOPED_CLEANUP({
      pause_thread.join();
      CHECK_OK(m->Resume());
    });

  // After an attempt to negotiate with the former leader master, timing out,
  // and re-resolving the leader master, the client will eventually connect to
  // a new leader master elected after the former one is paused. The new leader
  // master doesn't impose any negotiation delay, so the client should succeed
  // with the ListTables() call.
  ASSERT_OK(client->ListTables(&tables));
}

} // namespace kudu
