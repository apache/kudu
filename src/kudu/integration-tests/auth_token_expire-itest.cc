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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(rpc_reopen_outbound_connections);

using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace client {

namespace {

// Create a table with the specified name (no replication) and specified
// number of hash partitions.
Status CreateTable(KuduClient* client,
                   const string& table_name,
                   const KuduSchema& schema,
                   int num_hash_partitions) {
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  return table_creator->table_name(table_name)
      .schema(&schema)
      .add_hash_partitions({ "key" }, num_hash_partitions)
      .num_replicas(1)
      .Create();
}

unique_ptr<KuduInsert> BuildTestRow(KuduTable* table, int index) {
  unique_ptr<KuduInsert> insert(table->NewInsert());
  KuduPartialRow* row = insert->mutable_row();
  CHECK_OK(row->SetInt32(0, index));
  CHECK_OK(row->SetInt32(1, index * 2));
  return insert;
}

// Insert given number of tests rows into the default test table in the context
// of a new session.
Status InsertTestRows(KuduClient* client, KuduTable* table,
                      int num_rows, int first_row = 0) {
  shared_ptr<KuduSession> session = client->NewSession();
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  session->SetTimeoutMillis(60000);
  for (int i = first_row; i < num_rows + first_row; ++i) {
    unique_ptr<KuduInsert> insert(BuildTestRow(table, i));
    RETURN_NOT_OK(session->Apply(insert.release()));
  }
  return session->Flush();
}

} // anonymous namespace

class AuthTokenExpireITestBase : public KuduTest {
 public:
  AuthTokenExpireITestBase(int64_t authn_token_validity_seconds,
                           int64_t authz_token_validity_seconds,
                           int num_masters,
                           int num_tablet_servers)
      : authn_token_validity_seconds_(authn_token_validity_seconds),
        authz_token_validity_seconds_(authz_token_validity_seconds),
        num_masters_(num_masters),
        num_tablet_servers_(num_tablet_servers),
        schema_(KuduSchema::FromSchema(CreateKeyValueTestSchema())) {
    cluster_opts_.num_tablet_servers = num_tablet_servers_;
    cluster_opts_.num_masters = num_masters_;
    cluster_opts_.enable_kerberos = true;
  }

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  const int64_t authn_token_validity_seconds_;
  const int64_t authz_token_validity_seconds_;
  const int num_masters_;
  const int num_tablet_servers_;
  KuduSchema schema_;
  ExternalMiniClusterOptions cluster_opts_;
  shared_ptr<ExternalMiniCluster> cluster_;
};


class AuthTokenExpireITest : public AuthTokenExpireITestBase {
 public:
  explicit AuthTokenExpireITest(int64_t authn_token_validity_seconds = 2,
                                int64_t authz_token_validity_seconds = 2)
      : AuthTokenExpireITestBase(authn_token_validity_seconds,
                                 authz_token_validity_seconds,
                                 /*num_masters=*/ 1,
                                 /*num_tablet_servers=*/ 3) {
    // Masters and tservers inject FATAL_INVALID_AUTHENTICATION_TOKEN errors.
    // The client should retry the operation again and eventually it should
    // succeed even with the high ratio of injected errors.
    cluster_opts_.extra_master_flags = {
      "--rpc_inject_invalid_authn_token_ratio=0.5",

      // In addition to very short authn token TTL, rotate token signing keys as
      // often as possible: we want to cover TSK propagation-related scenarios
      // as well (i.e. possible ERROR_UNAVAILABLE errors from tservers) upon
      // a new authn token re-acquisitions and retried RPCs.
      "--tsk_rotation_seconds=1",
      Substitute("--authn_token_validity_seconds=$0", authn_token_validity_seconds_),
      Substitute("--authz_token_validity_seconds=$0", authz_token_validity_seconds_),
    };

    cluster_opts_.extra_tserver_flags = {
      "--rpc_inject_invalid_authn_token_ratio=0.5",

      // Tservers inject ERROR_INVALID_AUTHORIZATION_TOKEN errors, which will
      // lead the client to retry the operation with after fetching a new authz
      // token from the master.
      "--tserver_inject_invalid_authz_token_ratio=0.5",

      "--tserver_enforce_access_control=true",

      // Decreasing TS->master heartbeat interval speeds up the test.
      "--heartbeat_interval_ms=10",
    };
  }

  void SetUp() override {
    AuthTokenExpireITestBase::SetUp();
    ASSERT_OK(cluster_->Start());
  }
};


// Make sure authn token is re-acquired on certain scenarios upon restarting
// tablet servers.
TEST_F(AuthTokenExpireITest, RestartTabletServers) {
  const string table_name = "authn-token-expire-restart-tablet-servers";

  // Create and open one table, keeping it open over the component restarts.
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  ASSERT_OK(CreateTable(client.get(), table_name, schema_, num_tablet_servers_));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name, &table));
  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 0));

  // Restart all tablet servers.
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    auto server = cluster_->tablet_server(i);
    ASSERT_NE(nullptr, server);
    server->Shutdown();
    ASSERT_OK(server->Restart());
  }
  SleepFor(MonoDelta::FromSeconds(authn_token_validity_seconds_ + 1));

  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 1));
  SleepFor(MonoDelta::FromSeconds(authn_token_validity_seconds_ + 1));
  // Make sure to insert a row into all tablets to make an RPC call to every
  // tablet server hosting the table.
  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 2));
}

// Make sure authn token is re-acquired on certain scenarios upon restarting
// both masters and tablet servers.
TEST_F(AuthTokenExpireITest, RestartCluster) {
  const string table_name = "authn-token-expire-restart-cluster";

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  ASSERT_OK(CreateTable(client.get(), table_name, schema_, num_tablet_servers_));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name, &table));
  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 0));

  // Restart all Kudu server-side components: masters and tablet servers.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());
  SleepFor(MonoDelta::FromSeconds(authn_token_validity_seconds_ + 1));

  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 1));
  SleepFor(MonoDelta::FromSeconds(authn_token_validity_seconds_ + 1));
  // Make sure to insert a row into all tablets to make an RPC call to every
  // tablet server hosting the table.
  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 2));
}

struct AuthTokenParams {
  int64_t authn_validity_secs;
  int64_t authz_validity_secs;
};

constexpr AuthTokenParams kEvenValidity = { 5, 5 };
constexpr AuthTokenParams kLongerAuthn = { 5, 3 };
constexpr AuthTokenParams kLongerAuthz = { 3, 5 };

class AuthTokenExpireDuringWorkloadITest : public AuthTokenExpireITest,
                                           public ::testing::WithParamInterface<AuthTokenParams> {
 public:
  AuthTokenExpireDuringWorkloadITest()
      : AuthTokenExpireITest(GetParam().authn_validity_secs, GetParam().authz_validity_secs) {
    // Close an already established idle connection to the server and open
    // a new one upon making another call to the same server. This is to force
    // authn token verification at every RPC.
    FLAGS_rpc_reopen_outbound_connections = true;
  }

  void SetUp() override {
    AuthTokenExpireITestBase::SetUp();
    // Do not start the cluster as a part of setup phase. Don't waste time on
    // on that because the scenario contains a test which is marked slow and
    // will be skipped if KUDU_ALLOW_SLOW_TESTS environment variable is not set.
  }
  const int64_t max_token_validity = std::max(GetParam().authn_validity_secs,
                                              GetParam().authz_validity_secs);
};

INSTANTIATE_TEST_CASE_P(ValidityIntervals, AuthTokenExpireDuringWorkloadITest,
    ::testing::Values(kEvenValidity, kLongerAuthn, kLongerAuthz));

// Run a mixed write/read test workload and check that client retries upon
// receiving the appropriate invalid token error, eventually succeeding with
// every issued RPC.
TEST_P(AuthTokenExpireDuringWorkloadITest, InvalidTokenDuringMixedWorkload) {
  static const int32_t kTimeoutMs = 10 * 60 * 1000;

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  ASSERT_OK(cluster_->Start());

  TestWorkload w(cluster_.get());
  w.set_client_default_admin_operation_timeout_millis(kTimeoutMs);
  w.set_client_default_rpc_timeout_millis(kTimeoutMs);
  w.set_num_replicas(num_tablet_servers_);
  w.set_num_read_threads(3);
  w.set_read_timeout_millis(kTimeoutMs);
  w.set_num_write_threads(3);
  w.set_write_batch_size(64);
  w.set_write_timeout_millis(kTimeoutMs);
  w.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);

  w.Setup();
  w.Start();
  SleepFor(MonoDelta::FromSeconds(8 * max_token_validity));
  w.StopAndJoin();

  ClusterVerifier v(cluster_.get());
  v.SetOperationsTimeout(MonoDelta::FromSeconds(5 * 60));
  NO_FATALS(v.CheckRowCount(
      w.table_name(), ClusterVerifier::EXACTLY, w.rows_inserted()));
  ASSERT_OK(w.Cleanup());

  NO_FATALS(cluster_->AssertNoCrashes());
}

// Run write-only and scan-only workloads and check that the client retries the
// appropriate invalid token error, eventually succeeding with its RPCs. There
// is also a test for the mixed workload (see above), but we are looking at the
// implementation as a black box: it's impossible to guarantee that the read
// paths are not affected by the write paths since the mixed workload uses the
// same shared client instance for both the read and the write paths.
TEST_P(AuthTokenExpireDuringWorkloadITest, InvalidTokenDuringSeparateWorkloads) {
  const string table_name = "authn-token-expire-separate-workloads";
  static const int32_t kTimeoutMs = 10 * 60 * 1000;

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  ASSERT_OK(cluster_->Start());

  // Close an already established idle connection to the server and open
  // a new one upon making another call to the same server. This is to force
  // authn token verification at every RPC.
  FLAGS_rpc_reopen_outbound_connections = true;

  // Run the write-only workload first.
  TestWorkload w(cluster_.get());
  w.set_table_name(table_name);
  w.set_num_replicas(num_tablet_servers_);
  w.set_client_default_admin_operation_timeout_millis(kTimeoutMs);
  w.set_client_default_rpc_timeout_millis(kTimeoutMs);
  w.set_num_replicas(num_tablet_servers_);
  w.set_num_read_threads(0);
  w.set_num_write_threads(8);
  w.set_write_batch_size(256);
  w.set_write_timeout_millis(kTimeoutMs);
  w.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
  w.Setup();
  w.Start();
  SleepFor(MonoDelta::FromSeconds(3 * max_token_validity));
  w.StopAndJoin();

  NO_FATALS(cluster_->AssertNoCrashes());
  const int64_t rows_inserted = w.rows_inserted();
  ASSERT_GE(rows_inserted, 0);

  // Run the read-only workload after the test table is populated.
  TestWorkload r(cluster_.get());
  r.set_table_name(table_name);
  r.set_num_replicas(num_tablet_servers_);
  r.set_client_default_admin_operation_timeout_millis(kTimeoutMs);
  r.set_client_default_rpc_timeout_millis(kTimeoutMs);
  r.set_num_read_threads(8);
  r.set_read_timeout_millis(kTimeoutMs);
  r.set_num_write_threads(0);
  r.Setup();
  r.Start();
  SleepFor(MonoDelta::FromSeconds(3 * max_token_validity));
  r.StopAndJoin();

  ClusterVerifier v(cluster_.get());
  v.SetOperationsTimeout(MonoDelta::FromSeconds(5 * 60));
  NO_FATALS(v.CheckRowCount(table_name, ClusterVerifier::EXACTLY, rows_inserted));
  ASSERT_OK(r.Cleanup());

  NO_FATALS(cluster_->AssertNoCrashes());
}

// Scenarios to verify that the client automatically re-acquires authn token
// when receiving ERROR_INVALID_AUTHENTICATION_TOKEN from the servers in case
// if the client has established a token-based connection to masters.
// Note: this test doesn't rely on authz tokens, but the TSK validity period is
// determined based all token validity intervals, so for simplicity, set the
// authz validity interval to be the same.
class TokenBasedConnectionITest : public AuthTokenExpireITestBase {
 public:
  TokenBasedConnectionITest()
      : AuthTokenExpireITestBase(
          /*authn_token_validity_seconds=*/ 2,
          /*authz_token_validity_seconds=*/ 2,
          /*num_masters=*/ 1,
          /*num_tablet_servers=*/ 3) {
    cluster_opts_.extra_master_flags = {
      Substitute("--authn_token_validity_seconds=$0", authn_token_validity_seconds_),
      Substitute("--authz_token_validity_seconds=$0", authz_token_validity_seconds_),
    };

    cluster_opts_.extra_tserver_flags = {
      // Decreasing TS->master heartbeat interval speeds up the test.
      "--heartbeat_interval_ms=10",
    };
  }

  void SetUp() override {
    AuthTokenExpireITestBase::SetUp();
    ASSERT_OK(cluster_->Start());
  }
};

// This test verifies that the token re-acquire logic behaves correctly in case
// if the connection to the master is established using previously acquired
// authn token. The master has particular constraint to prohibit re-issuing
// a new authn token over a connection established with authn token itself
// (otherwise, an authn token would never effectively expire).
TEST_F(TokenBasedConnectionITest, ReacquireAuthnToken) {
  const string table_name = "authn-token-reacquire";

  // Create a client and perform some basic operations to acquire authn token.
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(
      &KuduClientBuilder()
          .default_admin_operation_timeout(MonoDelta::FromSeconds(60))
          .default_rpc_timeout(MonoDelta::FromSeconds(60)),
      &client));
  ASSERT_OK(CreateTable(client.get(), table_name, schema_, num_tablet_servers_));

  // Restart the master and the tablet servers to make sure all connectons
  // between the client and the servers are closed.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());

  // Perform some operations using already existing token. The crux here is to
  // establish a connection to the master where client is authenticated
  // via the authn token, not Kerberos credentials.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name, &table));

  // Let the authn token to expire.
  SleepFor(MonoDelta::FromSeconds(authn_token_validity_seconds_ + 1));

  // Here a new authn token should be automatically acquired upon receiving
  // FATAL_INVALID_AUTHENTICATION_TOKEN error. To get a new token it's necessary
  // to establish a new connection to the master _not_ using the authn token
  // as client-side credentials.
  ASSERT_OK(InsertTestRows(client.get(), table.get(), num_tablet_servers_));
  NO_FATALS(cluster_->AssertNoCrashes());
}

// Test for scenarios involving multiple masters where
// client-to-non-leader-master connections are closed due to inactivity,
// but the connection to the former leader master is kept open.
class MultiMasterIdleConnectionsITest : public AuthTokenExpireITestBase {
 public:
  MultiMasterIdleConnectionsITest()
      : AuthTokenExpireITestBase(
          /*authn_token_validity_seconds=*/ 3,
          /*authz_token_validity_seconds=*/ 3,
          /*num_masters=*/ 3,
          /*num_tablet_servers=*/ 3) {

    cluster_opts_.extra_master_flags = {
      // Custom validity interval for authn tokens. The scenario involves
      // expiration of authn tokens, while the default authn expiration timeout
      // is 7 days. So, let's make the token validity interval really short.
      Substitute("--authn_token_validity_seconds=$0", authn_token_validity_seconds_),
      Substitute("--authz_token_validity_seconds=$0", authz_token_validity_seconds_),

      // The default for leader_failure_max_missed_heartbeat_periods 3.0, but
      // 2.0 is enough to have master leadership stable enough and makes it
      // run a bit faster.
      Substitute("--leader_failure_max_missed_heartbeat_periods=$0",
          master_leader_failure_max_missed_heartbeat_periods_),

      // Custom Raft heartbeat interval between replicas of the systable.
      // The default it 500ms, but custom setting keeps the test stable enough
      // while making it a bit faster to complete.
      Substitute("--raft_heartbeat_interval_ms=$0",
          master_raft_hb_interval_ms_),

      // The default is 65 seconds, but the test scenario need to run faster.
      // A multiple of (leader_failure_max_missed_heartbeat_periods *
      // raft_heartbeat_interval_ms) is good enough, but it's also necessary
      // it to be greater than token validity interval due to the scenario's
      // logic.
      Substitute("--rpc_default_keepalive_time_ms=$0",
          master_rpc_keepalive_time_ms_),
    };

    cluster_opts_.extra_tserver_flags = {
      // Decreasing TS->master heartbeat interval speeds up the test.
      "--heartbeat_interval_ms=100",
    };
  }

  void SetUp() override {
    AuthTokenExpireITestBase::SetUp();
    ASSERT_OK(cluster_->Start());
  }

 protected:
  const int master_rpc_keepalive_time_ms_ = 3 * authn_token_validity_seconds_ * 1000 / 2;
  const int master_raft_hb_interval_ms_ = 250;
  const double master_leader_failure_max_missed_heartbeat_periods_ = 2.0;
};

// Verify that Kudu C++ client reacquires authn token in the following scenario:
//
//   1. Client is running against a multi-master cluster.
//   2. Client successfully authenticates and gets an authn token by calling
//      ConnectToCluster.
//   3. Client keeps the connection to leader master open, but follower masters
//      close connections to the client due to inactivity.
//   4. After the authn token expires, a change in master leadership happens.
//   5. Client tries to open a table, first making a request to the former
//      leader master.  However, the former leader returns NOT_THE_LEADER error.
//
// In that situation, the client should reconnect to the cluster to get a new
// authn token. Prior to the KUDU-2580 fix, it didn't, and the test was failing
// when the client tried to open the test table after master leader re-election:
//   Timed out: GetTableSchema timed out after deadline expired
TEST_F(MultiMasterIdleConnectionsITest, ClientReacquiresAuthnToken) {
  const string kTableName = "keep-connection-to-former-master-leader";

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const auto time_start = MonoTime::Now();

  shared_ptr<KuduClient> client;
  {
    KuduClientBuilder builder;
    builder.default_rpc_timeout(MonoDelta::FromSeconds(5));
    ASSERT_OK(cluster_->CreateClient(&builder, &client));
  }
  ASSERT_OK(CreateTable(client.get(), kTableName, schema_, num_tablet_servers_));

  // Wait for the following events:
  //   1) authn token expires
  //   2) connections to non-leader masters close

  const auto time_right_before_token_expiration = time_start +
      MonoDelta::FromSeconds(authn_token_validity_seconds_);
  while (MonoTime::Now() < time_right_before_token_expiration) {
    // Keep the connection to leader master open, time to time making requests
    // that go to the leader master, but not to other masters in the cluster.
    //
    // The leader master might unexpectedly change in the middle of this cycle,
    // but that would not induce errors in this cycle. The only negative outcome
    // of that unexpected re-election would be missing the conditions of the
    // reference scenario, but due to the relative stability of the Raft
    // leadership role given current parameters for masters Raft consensus,
    // the reference condition is reproduced in the vast majority of runs.
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(kTableName, &table));
    SleepFor(MonoDelta::FromMilliseconds(250));
  }

  // Given the relation between the master_rpc_keepalive_time_ms_ and
  // authn_token_validity_seconds_ parameters, the original authn token should
  // expire and connections to follower masters should be torn down due to
  // inactivity, but the connection to the leader master should be kept open
  // after waiting for additional token expiration interval.
  SleepFor(MonoDelta::FromSeconds(authn_token_validity_seconds_));

  {
    int former_leader_master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&former_leader_master_idx));
    const int leader_idx = (former_leader_master_idx + 1) % num_masters_;
    ASSERT_EVENTUALLY([&] {
      consensus::ConsensusServiceProxy proxy(
          cluster_->messenger(), cluster_->master(leader_idx)->bound_rpc_addr(),
          cluster_->master(leader_idx)->bound_rpc_hostport().host());
      consensus::RunLeaderElectionRequestPB req;
      req.set_tablet_id(master::SysCatalogTable::kSysCatalogTabletId);
      req.set_dest_uuid(cluster_->master(leader_idx)->uuid());
      rpc::RpcController rpc;
      rpc.set_timeout(MonoDelta::FromSeconds(1));
      consensus::RunLeaderElectionResponsePB resp;
      ASSERT_OK(proxy.RunLeaderElection(req, &resp, &rpc));
      int idx;
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&idx));
      ASSERT_NE(former_leader_master_idx, idx);
    });
  }

  // Try to open the table after leader master re-election. The former leader
  // responds with NOT_THE_LEADER error even if the authn token has expired
  // (i.e. the client will not get FATAL_INVALID_AUTHENTICATION_TOKEN error).
  // That's because the connection between the client and the former leader
  // master was established in the beginning and kept open during the test.
  // However, the client should detect that condition and reconnect to the
  // cluster for a new authn token.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
}

} // namespace client
} // namespace kudu
