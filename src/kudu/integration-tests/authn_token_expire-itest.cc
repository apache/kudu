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

#include <memory>
#include <string>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

DECLARE_bool(rpc_reopen_outbound_connections);

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace client {

using sp::shared_ptr;

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

class AuthnTokenExpireITestBase : public KuduTest {
 public:
  AuthnTokenExpireITestBase(int num_tablet_servers,
                            int64_t token_validity_seconds)
      : num_tablet_servers_(num_tablet_servers),
        token_validity_seconds_(token_validity_seconds),
        schema_(client::KuduSchemaFromSchema(CreateKeyValueTestSchema())) {
    cluster_opts_.num_tablet_servers = num_tablet_servers_;
    cluster_opts_.enable_kerberos = true;
  }

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  const int num_tablet_servers_;
  const int64_t token_validity_seconds_;
  KuduSchema schema_;
  ExternalMiniClusterOptions cluster_opts_;
  shared_ptr<ExternalMiniCluster> cluster_;
};


class AuthnTokenExpireITest : public AuthnTokenExpireITestBase {
 public:
  AuthnTokenExpireITest()
      : AuthnTokenExpireITestBase(3, 2) {
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
      Substitute("--authn_token_validity_seconds=$0", token_validity_seconds_),
    };

    cluster_opts_.extra_tserver_flags = {
      "--rpc_inject_invalid_authn_token_ratio=0.5",

      // Decreasing TS->master heartbeat interval speeds up the test.
      "--heartbeat_interval_ms=10",
    };
  }

  void SetUp() override {
    AuthnTokenExpireITestBase::SetUp();
  }
};


// Make sure authn token is re-acquired on certain scenarios upon restarting
// tablet servers.
TEST_F(AuthnTokenExpireITest, RestartTabletServers) {
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
  SleepFor(MonoDelta::FromSeconds(token_validity_seconds_ + 1));

  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 1));
  SleepFor(MonoDelta::FromSeconds(token_validity_seconds_ + 1));
  // Make sure to insert a row into all tablets to make an RPC call to every
  // tablet server hosting the table.
  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 2));
}

// Make sure authn token is re-acquired on certain scenarios upon restarting
// both masters and tablet servers.
TEST_F(AuthnTokenExpireITest, RestartCluster) {
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
  SleepFor(MonoDelta::FromSeconds(token_validity_seconds_ + 1));

  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 1));
  SleepFor(MonoDelta::FromSeconds(token_validity_seconds_ + 1));
  // Make sure to insert a row into all tablets to make an RPC call to every
  // tablet server hosting the table.
  ASSERT_OK(InsertTestRows(client.get(), table.get(),
                           num_tablet_servers_, num_tablet_servers_ * 2));
}

// Run the workload and check that client retries INVALID_AUTH_TOKEN errors,
// eventually succeeding with its RPCs.
TEST_F(AuthnTokenExpireITest, InvalidTokenDuringWorkload) {
  static const int32_t kTimeoutMs = 10 * 60 * 1000;

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  // Close an already established idle connection to the server and open
  // a new one upon making another call to the same server. This is to force
  // authn token verification at every RPC.
  FLAGS_rpc_reopen_outbound_connections = true;

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
  SleepFor(MonoDelta::FromSeconds(8 * token_validity_seconds_));
  w.StopAndJoin();

  ClusterVerifier v(cluster_.get());
  v.SetOperationsTimeout(MonoDelta::FromSeconds(5 * 60));
  NO_FATALS(v.CheckRowCount(
      w.table_name(), ClusterVerifier::EXACTLY, w.rows_inserted()));
  ASSERT_OK(w.Cleanup());

  NO_FATALS(cluster_->AssertNoCrashes());
}

class AuthnTokenReacquireITest : public AuthnTokenExpireITestBase {
 public:
  AuthnTokenReacquireITest()
      : AuthnTokenExpireITestBase(3, 2) {

    cluster_opts_.extra_master_flags = {
      Substitute("--authn_token_validity_seconds=$0", token_validity_seconds_),
    };

    cluster_opts_.extra_tserver_flags = {
      // Decreasing TS->master heartbeat interval speeds up the test.
      "--heartbeat_interval_ms=10",
    };
  }
};

// This test verifies that the token re-acquire logic behaves correctly in case
// if the connection to the master is established using previously acquired
// authn token. The master has particular constraint to prohibit re-issuing
// a new authn token over a connection established with authn token itself
// (otherwise, a authn token would never effectively expire).
TEST_F(AuthnTokenReacquireITest, ConnectionToMasterViaAuthnToken) {
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
  // establish a connection to the master server where client is authenticated
  // via the authn token, not Kerberos credentials.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name, &table));

  // Let the authn token to expire.
  SleepFor(MonoDelta::FromSeconds(token_validity_seconds_ + 1));

  // Here a new authn token should be automatically acquired upon receiving
  // FATAL_INVALID_AUTHENTICATION_TOKEN error. To get a new token it's necessary
  // to establish a new connection to the master _not_ using the authn token
  // as client-side credentials.
  ASSERT_OK(InsertTestRows(client.get(), table.get(), num_tablet_servers_));
  NO_FATALS(cluster_->AssertNoCrashes());
}

} // namespace client
} // namespace kudu
