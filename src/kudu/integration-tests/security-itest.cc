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

#include <sys/stat.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/ranger-kms/mini_ranger_kms.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/cert.h"
#include "kudu/security/kinit_context.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/security/token.pb.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mini_oidc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(jwt_client_require_trusted_tls_cert);
DECLARE_string(local_ip_for_outbound_sockets);

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTransaction;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::rpc::Messenger;
using kudu::security::CreateTestSSLCertWithChainSignedByRoot;
using kudu::security::CreateTestSSLExpiredCertWithChainSignedByRoot;
using std::get;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

static const char* const kTableName = "test-table";
static const Schema kTestSchema = CreateKeyValueTestSchema();
static const KuduSchema kTestKuduSchema = client::KuduSchema::FromSchema(kTestSchema);

class SecurityITest : public KuduTest {
 public:
  SecurityITest() {
    cluster_opts_.enable_kerberos = true;
    cluster_opts_.enable_encryption = true;
    cluster_opts_.num_tablet_servers = 3;
    cluster_opts_.extra_master_flags.emplace_back("--rpc_trace_negotiation");
    cluster_opts_.extra_tserver_flags.emplace_back("--rpc_trace_negotiation");
  }
  Status StartCluster() {
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    return cluster_->Start();
  }

  Status TrySetFlagOnTS() {
    // Make a new messenger so that we don't reuse any cached connections from
    // the minicluster startup sequence.
    auto messenger = NewMessengerOrDie();
    const auto& addr = cluster_->tablet_server(0)->bound_rpc_addr();
    server::GenericServiceProxy proxy(messenger, addr, addr.host());

    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(30));
    server::SetFlagRequestPB req;
    server::SetFlagResponsePB resp;
    req.set_flag("non-existent");
    req.set_value("xx");
    return proxy.SetFlag(req, &resp, &controller);
  }

  static Status CreateTestTable(const shared_ptr<KuduClient>& client) {
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    return table_creator->table_name(kTableName)
        .set_range_partition_columns({ "key" })
        .schema(&kTestKuduSchema)
        .num_replicas(3)
        .Create();
  }

  // Create a table, insert a row, scan it back, and then drop the table.
  void SmokeTestCluster(const shared_ptr<KuduClient>& client = {},
                        bool transactional = false);

  Status TryRegisterAsTS() {
    // Make a new messenger so that we don't reuse any cached connections from
    // the minicluster startup sequence.
    auto messenger = NewMessengerOrDie();
    const auto& addr = cluster_->master(0)->bound_rpc_addr();
    master::MasterServiceProxy proxy(messenger, addr, addr.host());

    rpc::RpcController rpc;
    master::TSHeartbeatRequestPB req;
    master::TSHeartbeatResponsePB resp;
    req.mutable_common()->mutable_ts_instance()->set_permanent_uuid("x");
    req.mutable_common()->mutable_ts_instance()->set_instance_seqno(1);
    return proxy.TSHeartbeat(req, &resp, &rpc);
  }

  Status TryListTablets(vector<string>* tablet_ids = nullptr) {
    auto messenger = NewMessengerOrDie();
    const auto& addr = cluster_->tablet_server(0)->bound_rpc_addr();
    tserver::TabletServerServiceProxy proxy(messenger, addr, addr.host());

    rpc::RpcController rpc;
    tserver::ListTabletsRequestPB req;
    tserver::ListTabletsResponsePB resp;
    RETURN_NOT_OK(proxy.ListTablets(req, &resp, &rpc));
    if (tablet_ids) {
      for (int i = 0; i < resp.status_and_schema_size(); i++) {
        tablet_ids->emplace_back(resp.status_and_schema(i).tablet_status().tablet_id());
      }
    }
    return Status::OK();
  }

  // Sends a request to checksum the given tablet without an authz token.
  Status TryChecksumWithoutAuthzToken(const string& tablet_id) {
    auto messenger = NewMessengerOrDie();
    const auto& addr = cluster_->tablet_server(0)->bound_rpc_addr();
    tserver::TabletServerServiceProxy proxy(messenger, addr, addr.host());

    rpc::RpcController rpc;
    tserver::ChecksumRequestPB req;
    tserver::NewScanRequestPB* scan = req.mutable_new_request();
    scan->set_tablet_id(tablet_id);
    RETURN_NOT_OK(SchemaToColumnPBs(kTestSchema, scan->mutable_projected_columns()));
    tserver::ChecksumResponsePB resp;
    return proxy.Checksum(req, &resp, &rpc);
  }

  // Retrieve the cluster's IPKI certificate. Effectively, this waits until
  // the catalog manager is initialized and has the IPKI CA ready to process
  // requests to the /ipki-ca-cert HTTP endpoint. This also allows to have
  // the master's TLS certificate used for RPC signed with the IPKI CA,
  // so it will list the JWT authentication mechanism as available.
  Status FetchClusterCACert(string* ca_cert_pem) {
    int leader_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    const auto& http_hp = cluster_->master(leader_idx)->bound_http_hostport();
    string url = Substitute("http://$0/ipki-ca-cert", http_hp.ToString());
    EasyCurl curl;
    faststring dst;
    auto res = curl.FetchURL(url, &dst);
    *ca_cert_pem = dst.ToString();
    return res;
  }

 private:
  static std::shared_ptr<Messenger> NewMessengerOrDie() {
    std::shared_ptr<Messenger> messenger;
    CHECK_OK(rpc::MessengerBuilder("test-messenger")
             .set_num_reactors(1)
             .set_max_negotiation_threads(1)
             .Build(&messenger));
    return messenger;
  }

 protected:
  ExternalMiniClusterOptions cluster_opts_;
  unique_ptr<ExternalMiniCluster> cluster_;
};

void SecurityITest::SmokeTestCluster(const shared_ptr<KuduClient>& client,
                                     const bool transactional) {
  shared_ptr<KuduClient> new_client;
  const shared_ptr<KuduClient>& c = client ? client : new_client;
  if (!client) {
    ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));
  }

  // Create a table.
  ASSERT_OK(CreateTestTable(c));

  // Insert a row.
  shared_ptr<KuduTable> table;
  ASSERT_OK(c->OpenTable(kTableName, &table));
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> session;
  if (transactional) {
    ASSERT_OK(c->NewTransaction(&txn));
    ASSERT_OK(txn->CreateSession(&session));
  } else {
    session = c->NewSession();
  }
  session->SetTimeoutMillis(60000);
  unique_ptr<KuduInsert> ins(table->NewInsert());
  ASSERT_OK(ins->mutable_row()->SetInt32(0, 12345));
  ASSERT_OK(ins->mutable_row()->SetInt32(1, 54321));
  ASSERT_OK(session->Apply(ins.release()));
  FlushSessionOrDie(session);
  if (transactional) {
    ASSERT_OK(txn->Commit());
  }

  // Read the inserted row back.
  ASSERT_EQ(1, CountTableRows(table.get()));

  // Drop the table.
  ASSERT_OK(c->DeleteTable(kTableName));
}

// Test authorizing list tablets.
TEST_F(SecurityITest, TestAuthorizationOnListTablets) {
  // When enforcing access control, an operator of ListTablets must be
  // superuser.
  cluster_opts_.extra_tserver_flags.emplace_back("--tserver_enforce_access_control");
  ASSERT_OK(StartCluster());
  ASSERT_OK(cluster_->kdc()->Kinit("test-user"));
  Status s = TryListTablets();
  ASSERT_EQ("Remote error: Not authorized: unauthorized access to method: ListTablets",
            s.ToString());
  ASSERT_OK(cluster_->kdc()->Kinit("test-admin"));
  ASSERT_OK(TryListTablets());
}

TEST_F(SecurityITest, TestAuthorizationOnChecksum) {
  cluster_opts_.extra_tserver_flags.emplace_back("--tserver_enforce_access_control");
  ASSERT_OK(StartCluster());
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  ASSERT_OK(CreateTestTable(client));
  vector<string> tablet_ids;
  ASSERT_OK(TryListTablets(&tablet_ids));

  // As a regular user, we shouldn't be authorized if we didn't send an authz
  // token.
  ASSERT_OK(cluster_->kdc()->Kinit("test-user"));
  for (const auto& tablet_id : tablet_ids) {
    Status s = TryChecksumWithoutAuthzToken(tablet_id);
    ASSERT_STR_CONTAINS(s.ToString(), "Not authorized: no authorization token presented");
  }
  // As a super-user (e.g. if running the CLI as an admin), this should be
  // allowed.
  ASSERT_OK(cluster_->kdc()->Kinit("test-admin"));
  for (const auto& tablet_id : tablet_ids) {
    ASSERT_OK(TryChecksumWithoutAuthzToken(tablet_id));
  }
}

// Test creating a table, writing some data, reading data, and dropping
// the table.
TEST_F(SecurityITest, SmokeTestAsAuthorizedUser) {
  cluster_opts_.extra_master_flags.emplace_back("--txn_manager_enabled=true");
  cluster_opts_.extra_tserver_flags.emplace_back("--enable_txn_system_client_init=true");
  ASSERT_OK(StartCluster());

  ASSERT_OK(cluster_->kdc()->Kinit("test-user"));
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  NO_FATALS(SmokeTestCluster(client));
  NO_FATALS(SmokeTestCluster(client, /* transactional */ true));

  // Non-superuser clients should not be able to set flags.
  Status s = TrySetFlagOnTS();
  ASSERT_EQ("Remote error: Not authorized: unauthorized access to method: SetFlag",
            s.ToString());

  // Nor should they be able to send TS RPCs.
  s = TryRegisterAsTS();
  ASSERT_EQ("Remote error: Not authorized: unauthorized access to method: TSHeartbeat",
            s.ToString());
}

// Make sure that it's possible to call TxnManagerService's txn-related RPCs
// using credentials of a regular and a super user.
TEST_F(SecurityITest, TxnSmokeWithDifferentUserTypes) {
  constexpr const auto kTxnKeepaliveIntervalMs = 500;
  const auto usernames = {
    "test-admin",
    "test-user",
  };

  cluster_opts_.extra_master_flags.emplace_back(
      "--txn_manager_enabled=true");
  cluster_opts_.extra_tserver_flags.emplace_back(
      "--enable_txn_system_client_init=true");
  cluster_opts_.extra_tserver_flags.emplace_back(
      Substitute("--txn_keepalive_interval_ms=$0", kTxnKeepaliveIntervalMs));
  ASSERT_OK(StartCluster());

  for (const auto& username : usernames) {
    SCOPED_TRACE(Substitute("Running with credentials of user $0", username));
    ASSERT_OK(cluster_->kdc()->Kinit(username));

    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    NO_FATALS(SmokeTestCluster(client, /* transactional */ true));

    // Now, check for the rest of TxnManagerService RPCs.
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
              .set_range_partition_columns({ "key" })
              .schema(&kTestKuduSchema)
              .num_replicas(3)
              .Create());

    auto smoke_starter = [&](shared_ptr<KuduTable>* table,
                             shared_ptr<KuduTransaction>* txn) {
      RETURN_NOT_OK(client->OpenTable(kTableName, table));
      RETURN_NOT_OK(client->NewTransaction(txn));
      shared_ptr<KuduSession> session;
      RETURN_NOT_OK((*txn)->CreateSession(&session));
      RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
      {
        unique_ptr<KuduInsert> ins((*table)->NewInsert());
        RETURN_NOT_OK(ins->mutable_row()->SetInt32(0, 0));
        RETURN_NOT_OK(ins->mutable_row()->SetInt32(1, 1));
        RETURN_NOT_OK(session->Apply(ins.release()));
      }
      return session->Flush();
    };

    // Check TxnManagerService::AbortTransaction() RPC as well.
    {
      shared_ptr<KuduTable> table;
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(smoke_starter(&table, &txn));
      ASSERT_OK(txn->Rollback());

      // Wait for the transaction to complete the rollback phase. This is useful
      // because the next sub-scenario below starts a new transaction as well,
      // and in case of a race it may happen that the transaction below starts
      // before this one finalizes its abort phase. Both transactions attempt
      // to lock the same partition(s), so the latter is automatically aborted
      // by the deadlock prevention logic.
      ASSERT_EVENTUALLY([&]{
        bool is_complete = false;
        Status completion_status;
        ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
        ASSERT_TRUE(is_complete);
        ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
      });

      // Read the inserted row back.
      ASSERT_EQ(0, CountTableRows(table.get()));
    }

    // Check TxnManagerService::KeepTransactionAlive() -- wait for for over than
    // keepalive interval for a transaction and make sure it's still possible to
    // commit a transaction using its handle. Calling StartCommit() and then
    // calling IsCommitComplete() checks calling of the
    // TxnManagerService::GetTransactionState() RPC.
    {
      shared_ptr<KuduTable> table;
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(smoke_starter(&table, &txn));

      // Wait for a few keepalive intervals: the transaction would be aborted if
      // calls to TxnManagerService::KeepTransactionAlive() RPC do not come
      // through, and then KuduTransaction::StartCommit() below would fail.
      SleepFor(MonoDelta::FromMilliseconds(3 * kTxnKeepaliveIntervalMs));

      ASSERT_OK(txn->StartCommit());
      ASSERT_EVENTUALLY([&]{
        bool is_complete = false;
        Status completion_status;
        ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
        ASSERT_TRUE(is_complete);
        ASSERT_OK(completion_status);
      });

      // Read the inserted row back.
      ASSERT_EQ(1, CountTableRows(table.get()));
    }

    // Finally, drop the table.
    ASSERT_OK(client->DeleteTable(kTableName));
  }
}

// Test trying to access the cluster with no Kerberos credentials at all.
TEST_F(SecurityITest, TestNoKerberosCredentials) {
  ASSERT_OK(StartCluster());
  ASSERT_OK(cluster_->kdc()->Kdestroy());

  shared_ptr<KuduClient> client;
  Status s = cluster_->CreateClient(nullptr, &client);
  ASSERT_STR_MATCHES(s.ToString(),
                     "Not authorized: Could not connect to the cluster: "
                     "Client connection negotiation failed: client connection "
                     "to .*: server requires authentication, "
                     "but client does not have Kerberos credentials available");
}

// Regression test for KUDU-2121. Set up a Kerberized cluster with optional
// authentication. An un-Kerberized client should be able to connect with SASL
// PLAIN authentication.
TEST_F(SecurityITest, SaslPlainFallback) {
  cluster_opts_.num_masters = 1;
  cluster_opts_.num_tablet_servers = 0;
  cluster_opts_.extra_master_flags.emplace_back("--rpc-authentication=optional");
  cluster_opts_.extra_master_flags.emplace_back("--user-acl=*");
  ASSERT_OK(StartCluster());
  ASSERT_OK(cluster_->kdc()->Kdestroy());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));

  // Check client can successfully call ListTables().
  vector<string> tables;
  ASSERT_OK(client->ListTables(&tables));
}

// Test cluster access by a user who is not authorized as a client.
TEST_F(SecurityITest, TestUnauthorizedClientKerberosCredentials) {
  ASSERT_OK(StartCluster());
  ASSERT_OK(cluster_->kdc()->Kinit("joe-interloper"));
  shared_ptr<KuduClient> client;
  Status s = cluster_->CreateClient(nullptr, &client);
  ASSERT_EQ("Remote error: Could not connect to the cluster: "
            "Not authorized: unauthorized access to method: ConnectToMaster",
            s.ToString());
}

// Test superuser actions when authorized as a superuser.
TEST_F(SecurityITest, TestAuthorizedSuperuser) {
  ASSERT_OK(StartCluster());

  ASSERT_OK(cluster_->kdc()->Kinit("test-admin"));

  // Superuser can set flags.
  ASSERT_OK(TrySetFlagOnTS());

  // Even superusers can't pretend to be tablet servers.
  Status s = TryRegisterAsTS();

  ASSERT_EQ("Remote error: Not authorized: unauthorized access to method: TSHeartbeat",
            s.ToString());

}

// Test that the web UIs can be entirely disabled, for users who feel they
// are a security risk.
TEST_F(SecurityITest, TestDisableWebUI) {
  cluster_opts_.extra_master_flags.emplace_back("--webserver_enabled=0");
  cluster_opts_.extra_tserver_flags.emplace_back("--webserver_enabled=0");
  ASSERT_OK(StartCluster());
  NO_FATALS(SmokeTestCluster());
}

// Test disabling authentication and encryption.
TEST_F(SecurityITest, TestDisableAuthenticationEncryption) {
  cluster_opts_.extra_master_flags.emplace_back("--rpc_authentication=disabled");
  cluster_opts_.extra_tserver_flags.emplace_back("--rpc_authentication=disabled");
  cluster_opts_.extra_master_flags.emplace_back("--rpc_encryption=disabled");
  cluster_opts_.extra_tserver_flags.emplace_back("--rpc_encryption=disabled");
  cluster_opts_.enable_kerberos = false;
  ASSERT_OK(StartCluster());
  NO_FATALS(SmokeTestCluster());
}

void CreateWorldReadableFile(const string& name) {
  unique_ptr<RWFile> file;
  ASSERT_OK(Env::Default()->NewRWFile(name, &file));
  ASSERT_EQ(chmod(name.c_str(), 0444), 0);
}

void GetFullBinaryPath(string* binary) {
  string exe;
  ASSERT_OK(Env::Default()->GetExecutablePath(&exe));
  (*binary) = JoinPathSegments(DirName(exe), *binary);
}

TEST_F(SecurityITest, TestJwtMiniCluster) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  cluster_opts_.enable_kerberos = false;
  cluster_opts_.num_tablet_servers = 0;
  cluster_opts_.enable_client_jwt = true;

  MiniOidcOptions oidc_opts;
  const auto* const kValidAccount = "valid";
  const auto* const kInvalidAccount = "invalid";
  const uint64_t kLifetimeMs = 5000;
  oidc_opts.account_ids = {
    { kValidAccount, true },
    { kInvalidAccount, false },
  };
  oidc_opts.lifetime_ms = kLifetimeMs;

  // Set up certificates for the JWKS server
  string ca_certificate_file;
  string private_key_file;
  string certificate_file;
  ASSERT_OK(CreateTestSSLCertWithChainSignedByRoot(GetTestDataDirectory(),
                                                   &certificate_file,
                                                   &private_key_file,
                                                   &ca_certificate_file));
  // set the certs and private key for the jwks webserver
  oidc_opts.private_key_file = private_key_file;
  oidc_opts.server_certificate = certificate_file;
  // set the ca_cert (jwks certificate verification is enabled by default)
  cluster_opts_.extra_master_flags.push_back(Substitute("--trusted_certificate_file=$0",
                                                        ca_certificate_file));

  cluster_opts_.mini_oidc_options = std::move(oidc_opts);
  ASSERT_OK(StartCluster());

  string cluster_cert_pem;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FetchClusterCACert(&cluster_cert_pem));
  });
  ASSERT_FALSE(cluster_cert_pem.empty());

  const auto* const kSubject = "kudu-user";
  const auto configure_builder_for =
      [&] (const string& account_id, KuduClientBuilder* b, const uint64_t delay_ms, bool is_valid) {
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      b->add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    b->jwt(cluster_->oidc()->CreateJwt(account_id, kSubject, is_valid));
    b->require_authentication(true);
    b->trusted_certificate(cluster_cert_pem);
    SleepFor(MonoDelta::FromMilliseconds(delay_ms));
  };

  {
    SCOPED_TRACE("Valid JWT");
    KuduClientBuilder valid_builder;
    shared_ptr<KuduClient> client;
    configure_builder_for(kValidAccount, &valid_builder, 0, true);
    ASSERT_OK(valid_builder.Build(&client));
    vector<string> tables;
    ASSERT_OK(client->ListTables(&tables));
  }
  {
    SCOPED_TRACE("Invalid JWT");
    KuduClientBuilder invalid_builder;
    shared_ptr<KuduClient> client;
    configure_builder_for(kInvalidAccount, &invalid_builder, 0, false);
    Status s = invalid_builder.Build(&client);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "FATAL_INVALID_JWT");
  }
  {
    SCOPED_TRACE("Expired JWT");
    KuduClientBuilder timeout_builder;
    shared_ptr<KuduClient> client;
    configure_builder_for(kValidAccount, &timeout_builder, 3 * kLifetimeMs, true);
    Status s = timeout_builder.Build(&client);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "token expired");
  }
  {
    SCOPED_TRACE("No JWT provided");
    KuduClientBuilder no_jwt_builder;
    shared_ptr<KuduClient> client;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      no_jwt_builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    no_jwt_builder.require_authentication(true);
    Status s = no_jwt_builder.Build(&client);
    ASSERT_TRUE(s. IsNotAuthorized()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
  }
  {
    SCOPED_TRACE("Valid JWT but client does not trust master's TLS cert");
    KuduClientBuilder cb;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      cb.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    cb.jwt(cluster_->oidc()->CreateJwt(kValidAccount, kSubject, true));
    cb.require_authentication(true);

    shared_ptr<KuduClient> client;
    auto s = cb.Build(&client);
    ASSERT_TRUE(s. IsNotAuthorized()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
        "client requires authentication, but server does not have");
  }
  {
    SCOPED_TRACE("Valid JWT with relaxed requirements for server's TLS cert");
    KuduClientBuilder cb;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      cb.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    cb.jwt(cluster_->oidc()->CreateJwt(kValidAccount, kSubject, true));
    cb.require_authentication(true);

    // If not adding the CA certificate that Kudu RPC server certificates are
    // signed with, in simplified test scenarios it's possible to relax the
    // requirements at the client side of the Kudu RPC connection negotiation
    // protocol. With --jwt_client_require_trusted_tls_cert=false, the client
    // does not verify the server's TLS certificate before sending its JWT
    // to the server for authentication.
    FLAGS_jwt_client_require_trusted_tls_cert = false;

    shared_ptr<KuduClient> client;
    ASSERT_OK(cb.Build(&client));
    vector<string> tables;
    ASSERT_OK(client->ListTables(&tables));
  }
}

TEST_F(SecurityITest, TestJwtMiniClusterWithInvalidCert) {
  cluster_opts_.enable_kerberos = false;
  cluster_opts_.num_tablet_servers = 0;
  cluster_opts_.enable_client_jwt = true;
  MiniOidcOptions oidc_opts;
  const auto* const kValidAccount = "valid";
  const uint64_t kLifetimeMs = 5000;
  oidc_opts.account_ids = {
    { kValidAccount, true }
  };
  oidc_opts.lifetime_ms = kLifetimeMs;
  const auto* const kSubject = "kudu-user";

  // Set up certificates for the JWKS server
  string ca_certificate_file;
  string certificate_file;
  string private_key_file;
  ASSERT_OK(CreateTestSSLExpiredCertWithChainSignedByRoot(
      GetTestDataDirectory(),
      &certificate_file,
      &private_key_file,
      &ca_certificate_file));

  // set the certs and private key for the jwks webserver
  oidc_opts.private_key_file = private_key_file;
  oidc_opts.server_certificate = certificate_file;
  // set the ca_cert (jwks certificate verification is enabled by default)
  cluster_opts_.extra_master_flags.push_back(Substitute("--trusted_certificate_file=$0",
                                                        ca_certificate_file));

  cluster_opts_.mini_oidc_options = std::move(oidc_opts);

  ASSERT_OK(StartCluster());

  string cluster_cert_pem;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FetchClusterCACert(&cluster_cert_pem));
  });
  ASSERT_FALSE(cluster_cert_pem.empty());

  KuduClientBuilder client_builder;
  for (auto i = 0; i < cluster_->num_masters(); ++i) {
    client_builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
  }
  client_builder.jwt(cluster_->oidc()->CreateJwt(kValidAccount, kSubject, true));
  client_builder.trusted_certificate(cluster_cert_pem);
  client_builder.require_authentication(true);

  shared_ptr<KuduClient> client;
  auto s = client_builder.Build(&client);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized: Failed to load JWKS");
}

TEST_F(SecurityITest, TestJwtMiniClusterWithUntrustedCert) {
  cluster_opts_.enable_kerberos = false;
  cluster_opts_.num_tablet_servers = 0;
  cluster_opts_.enable_client_jwt = true;
  MiniOidcOptions oidc_opts;
  const auto* const kValidAccount = "valid";
  const uint64_t kLifetimeMs = 5000;
  oidc_opts.account_ids = {
    { kValidAccount, true }
  };
  oidc_opts.lifetime_ms = kLifetimeMs;
  const auto* const kSubject = "kudu-user";

  // Set up certificates for the JWKS server
  string ca_certificate_file;
  string private_key_file;
  string certificate_file;
  ASSERT_OK(CreateTestSSLCertWithChainSignedByRoot(GetTestDataDirectory(),
                                                   &certificate_file,
                                                   &private_key_file,
                                                   &ca_certificate_file));
  // set the certs and private key for the jwks webserver
  // jwks certificate verification is enabled by default, so we won't have to set it
  oidc_opts.private_key_file = private_key_file;
  oidc_opts.server_certificate = certificate_file;

  cluster_opts_.mini_oidc_options = std::move(oidc_opts);

  ASSERT_OK(StartCluster());

  string cluster_cert_pem;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FetchClusterCACert(&cluster_cert_pem));
  });
  ASSERT_FALSE(cluster_cert_pem.empty());

  KuduClientBuilder client_builder;
  for (auto i = 0; i < cluster_->num_masters(); ++i) {
    client_builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
  }
  client_builder.jwt(cluster_->oidc()->CreateJwt(kValidAccount, kSubject, true));
  client_builder.trusted_certificate(cluster_cert_pem);
  client_builder.require_authentication(true);

  shared_ptr<KuduClient> client;
  auto s = client_builder.Build(&client);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized: Failed to load JWKS");
}

TEST_F(SecurityITest, TestJwtMiniClusterWithNonWorkingJWKS) {
  cluster_opts_.enable_kerberos = false;
  cluster_opts_.num_tablet_servers = 0;
  cluster_opts_.start_jwks = false;
  cluster_opts_.enable_client_jwt = true;

  MiniOidcOptions oidc_opts;
  const auto* const kValidAccount = "valid";
  const auto* const kSubject = "kudu-user";
  oidc_opts.account_ids = {
    { kValidAccount, true }
  };

  // Set up certificates for the JWKS server
  string ca_certificate_file;
  string private_key_file;
  string certificate_file;
  ASSERT_OK(CreateTestSSLCertWithChainSignedByRoot(GetTestDataDirectory(),
                                                   &certificate_file,
                                                   &private_key_file,
                                                   &ca_certificate_file));
  // set the certs and private key for the jwks webserver
  oidc_opts.private_key_file = private_key_file;
  oidc_opts.server_certificate = certificate_file;
  // set the ca_cert (jwks certificate verification is enabled by default)
  cluster_opts_.extra_master_flags.push_back(Substitute("--trusted_certificate_file=$0",
                                                        ca_certificate_file));

  cluster_opts_.mini_oidc_options = std::move(oidc_opts);
  ASSERT_OK(StartCluster());

  string cluster_cert_pem;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FetchClusterCACert(&cluster_cert_pem));
  });
  ASSERT_FALSE(cluster_cert_pem.empty());

  KuduClientBuilder client_builder;
  for (auto i = 0; i < cluster_->num_masters(); ++i) {
    client_builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
  }
  client_builder.jwt(cluster_->oidc()->CreateJwt(kValidAccount, kSubject, true));
  client_builder.trusted_certificate(cluster_cert_pem);
  client_builder.require_authentication(true);

  shared_ptr<KuduClient> client;
  auto s = client_builder.Build(&client);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized: Failed to load JWKS");
}


TEST_F(SecurityITest, TestWorldReadableKeytab) {
  const string credentials_name = GetTestPath("insecure.keytab");
  NO_FATALS(CreateWorldReadableFile(credentials_name));
  string binary = "kudu";
  NO_FATALS(GetFullBinaryPath(&binary));
  const vector<string> argv = { binary,
                                "master",
                                "run",
                                Substitute("--keytab_file=$0", credentials_name) };
  string stderr;
  Status s = Subprocess::Call(argv, "", nullptr, &stderr);
  ASSERT_STR_CONTAINS(stderr, Substitute(
      "cannot use keytab file with world-readable permissions: $0",
      credentials_name));
}

TEST_F(SecurityITest, TestWorldReadablePrivateKey) {
  const string credentials_name = GetTestPath("insecure.key");
  NO_FATALS(CreateWorldReadableFile(credentials_name));
  string binary = "kudu";
  NO_FATALS(GetFullBinaryPath(&binary));
  const vector<string> argv = { binary,
                                "master",
                                "run",
                                "--unlock_experimental_flags",
                                Substitute("--rpc_private_key_file=$0", credentials_name),
                                "--rpc_certificate_file=fake_file",
                                "--rpc_ca_certificate_file=fake_file" };
  string stderr;
  Status s = Subprocess::Call(argv, "", nullptr, &stderr);
  ASSERT_STR_CONTAINS(stderr, Substitute(
      "cannot use private key file with world-readable permissions: $0",
      credentials_name));
}

// Test that our Kinit implementation can handle corrupted credential caches.
TEST_F(SecurityITest, TestCorruptKerberosCC) {
  ASSERT_OK(StartCluster());
  string admin_keytab = cluster_->kdc()->GetKeytabPathForPrincipal("test-admin");
  ASSERT_OK(cluster_->kdc()->CreateKeytabForExistingPrincipal("test-admin"));

  security::KinitContext kinit_ctx;
  ASSERT_OK(kinit_ctx.Kinit(admin_keytab, "test-admin"));

  // Truncate at different lengths to exercise different failure modes.
  Random rng(GetRandomSeed32());
  for (auto i = 0; i < 3; ++i) {
    const int32_t trunc_len = 10 + rng.Uniform(256);
    // Truncate the credential cache so that it no longer contains a valid
    // ticket for "test-admin".
    const char* cc_path = getenv("KRB5CCNAME");
    SCOPED_TRACE(Substitute("Truncating ccache at '$0' to $1", cc_path, trunc_len));
    {
      RWFileOptions opts;
      opts.mode = Env::MUST_EXIST;
      unique_ptr<RWFile> cc_file;
      ASSERT_OK(env_->NewRWFile(opts, cc_path, &cc_file));
      ASSERT_OK(cc_file->Truncate(trunc_len));
    }

    // With corrupt cache, we shouldn't be able to open connection.
    Status s = TrySetFlagOnTS();
    ASSERT_FALSE(s.ok());
    ASSERT_STR_CONTAINS(s.ToString(), "server requires authentication, but client does "
        "not have Kerberos credentials available");

    // Renewal should fix the corrupted credential cache and allow secure connections.
    ASSERT_OK(kinit_ctx.DoRenewal());
    ASSERT_OK(TrySetFlagOnTS());
  }
}

TEST_F(SecurityITest, TestNonDefaultPrincipal) {
  const string kPrincipal = "oryx";
  cluster_opts_.principal = kPrincipal;
  // Enable TxnManager in Kudu masters: it's necessary to test txn-related
  // operations along with others.
  cluster_opts_.extra_master_flags.emplace_back("--txn_manager_enabled=true");
  cluster_opts_.extra_tserver_flags.emplace_back("--enable_txn_system_client_init=true");
  ASSERT_OK(StartCluster());

  // A client with the default SASL proto shouldn't be able to connect to
  // a cluster using custom Kerberos principal for Kudu service user. Verify
  // that for both the regular and the super-user.
  for (const auto& username : {"test-user", "test-admin"}) {
    ASSERT_OK(cluster_->kdc()->Kinit(username));

    // Instantiate a KuduClientBuilder outside of this cluster's context, so
    // the custom service principals for this cluster don't affect the default
    // SASL proto name when creating this separate client instance.
    shared_ptr<KuduClient> client;
    KuduClientBuilder builder;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    const auto s = builder.Build(&client);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "not found in Kerberos database");

    // Create a client with the matching SASL proto name and verify it's able to
    // connect to the cluster and perform basic actions.
    {
      // Here we don't use out-of-this-cluster KuduClientBuilder instance,
      // so the client is created with matching SASL proto name.
      shared_ptr<KuduClient> client;
      ASSERT_OK(cluster_->CreateClient(nullptr, &client));
      SmokeTestCluster(client);
      SmokeTestCluster(client, /*transactional*/ true);
    }
  }
}

TEST_F(SecurityITest, TestNonExistentPrincipal) {
  cluster_opts_.extra_master_flags.emplace_back("--principal=oryx");
  cluster_opts_.extra_tserver_flags.emplace_back("--principal=oryx");
  auto s = StartCluster();
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to start masters");
}

TEST_F(SecurityITest, TestMismatchingPrincipals) {
  ASSERT_OK(StartCluster());
  string keytab_path;
  const string kPrincipalBase = "oryx";
  for (auto i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster::ExternalTabletServer* tserver = cluster_->tablet_server(i);
    const string kPrincipal = kPrincipalBase + "/" + tserver->bound_rpc_addr().host();
    ASSERT_OK(cluster_->kdc()->CreateServiceKeytab(kPrincipal, &keytab_path));
    tserver->mutable_flags()->emplace_back("--principal=" + kPrincipal);
    tserver->mutable_flags()->emplace_back("--keytab_file=" + keytab_path);
  }
  cluster_->Shutdown();
  Status s = cluster_->Restart();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
}

TEST_F(SecurityITest, TestRequireAuthenticationInsecureCluster) {
  cluster_opts_.enable_kerberos = false;
  ASSERT_OK(StartCluster());

  shared_ptr<KuduClient> client;
  KuduClientBuilder b;
  b.require_authentication(true);
  Status s = cluster_->CreateClient(&b, &client);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "client requires authentication, but server does not have Kerberos enabled");
}

TEST_F(SecurityITest, TestRequireEncryptionInsecureCluster) {
  cluster_opts_.enable_kerberos = false;
  cluster_opts_.extra_master_flags.emplace_back("--rpc_encryption=disabled");
  cluster_opts_.extra_tserver_flags.emplace_back("--rpc_encryption=disabled");
  cluster_opts_.extra_master_flags.emplace_back("--rpc_authentication=disabled");
  cluster_opts_.extra_tserver_flags.emplace_back("--rpc_authentication=disabled");
  ASSERT_OK(StartCluster());

  shared_ptr<KuduClient> client;
  KuduClientBuilder b;
  b.encryption_policy(client::KuduClientBuilder::REQUIRED);
  Status s = cluster_->CreateClient(&b, &client);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "server does not support required TLS encryption");
}

TEST_F(SecurityITest, TestRequireAuthenticationSecureCluster) {
  ASSERT_OK(StartCluster());

  shared_ptr<KuduClient> client;
  KuduClientBuilder b;
  b.require_authentication(true);
  ASSERT_OK(cluster_->CreateClient(&b, &client));
  SmokeTestCluster(client, /* transactional */ false);
}

TEST_F(SecurityITest, TestEncryptionWithKMSIntegration) {
  cluster_opts_.enable_ranger = true;
  cluster_opts_.enable_ranger_kms = true;
  ASSERT_OK(StartCluster());

  shared_ptr<KuduClient> client;
  KuduClientBuilder b;
  ASSERT_OK(cluster_->CreateClient(&b, &client));
  SmokeTestCluster(client, /* transactional */ false);
}

TEST_F(SecurityITest, TestEncryptionWithKMSIntegrationMultipleServers) {
  cluster_opts_.enable_ranger = true;
  cluster_opts_.enable_ranger_kms = true;
  ASSERT_OK(StartCluster());
  cluster_->Shutdown();

  const string& url = cluster_->ranger_kms()->url();
  for (int i = 0; i < cluster_->num_masters(); ++i) {
    cluster_->master(i)->mutable_flags()
        ->emplace_back("--ranger_kms_url=invalid.host:1234/kms," + url);
  }
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    cluster_->tablet_server(i)->mutable_flags()
        ->emplace_back("--ranger_kms_url=invalid.host:1234/kms," + url);
  }
  ASSERT_OK(cluster_->Restart());

  shared_ptr<KuduClient> client;
  KuduClientBuilder b;
  ASSERT_OK(cluster_->CreateClient(&b, &client));
  SmokeTestCluster(client, /*transactional=*/false);
}

TEST_F(SecurityITest, IPKICACert) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Need to test the functionality for both leader and follower masters.
  cluster_opts_.num_masters = 3;
  // No need to involve tablet servers in this scenario.
  cluster_opts_.num_tablet_servers = 0;

  ASSERT_OK(StartCluster());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));

  string authn_creds;
  ASSERT_OK(client->ExportAuthenticationCredentials(&authn_creds));
  client::AuthenticationCredentialsPB pb;
  ASSERT_TRUE(pb.ParseFromString(authn_creds));
  ASSERT_EQ(1, pb.ca_cert_ders_size());

  security::Cert ca_cert_client;
  ASSERT_OK(ca_cert_client.FromString(pb.ca_cert_ders(0),
                                      security::DataFormat::DER));
  string ca_cert_client_pem;
  ASSERT_OK(ca_cert_client.ToString(&ca_cert_client_pem,
                                    security::DataFormat::PEM));

  const auto fetch_ipki_ca = [c = cluster_.get()](int master_idx, string* out) {
    const auto& http_hp = c->master(master_idx)->bound_http_hostport();
    string url = Substitute("http://$0/ipki-ca-cert", http_hp.ToString());
    EasyCurl curl;
    faststring dst;
    auto res = curl.FetchURL(url, &dst);
    *out = dst.ToString();
    return res;
  };

  int leader_master_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
  string str;
  ASSERT_OK(fetch_ipki_ca(leader_master_idx, &str));
  security::Cert ca_cert;
  ASSERT_OK(ca_cert.FromString(str, security::DataFormat::PEM));

  // Using (string --> security::Cert --> string) conversion chain to compare
  // canonical representations of the CA certificates in PEM format.
  string ca_cert_str;
  ASSERT_OK(ca_cert.ToString(&ca_cert_str, security::DataFormat::PEM));
  ASSERT_EQ(ca_cert_client_pem, ca_cert_str);

  const auto count_valid_certs = [&](size_t* res) {
    size_t count = 0;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      string str;
      auto s = fetch_ipki_ca(i, &str);
      ASSERT_TRUE(s.ok() || s.IsRemoteError());
      if (s.IsRemoteError()) {
        // If there wasn't a CA cert in the output, there should had been
        // an error reported.
        ASSERT_NE(string::npos, str.find("ERROR: "));
        continue;
      }
      security::Cert ca_cert;
      ASSERT_OK(ca_cert.FromString(str, security::DataFormat::PEM));

      string ca_cert_str;
      ASSERT_OK(ca_cert.ToString(&ca_cert_str, security::DataFormat::PEM));
      ASSERT_EQ(ca_cert_client_pem, ca_cert_str);
      ++count;
    }
    *res = count;
  };

  // The IPKI has been certainly initialized at leader master since the client
  // was able to successfully connect to the cluster (see above).
  {
    size_t count = 0;
    NO_FATALS(count_valid_certs(&count));
    ASSERT_GE(count, 1);
  }

  // At some point, all the followers should have loaded the CA information
  // generated by the leader master upon the very first startup.
  ASSERT_EVENTUALLY([&] {
    size_t count = 0;
    NO_FATALS(count_valid_certs(&count));
    ASSERT_EQ(cluster_opts_.num_masters, count);
  });
}

class EncryptionPolicyTest :
    public SecurityITest,
    public ::testing::WithParamInterface<tuple<
        KuduClientBuilder::EncryptionPolicy, bool>> {
};

INSTANTIATE_TEST_SUITE_P(,
    EncryptionPolicyTest,
    ::testing::Combine(
        ::testing::Values(
            KuduClientBuilder::EncryptionPolicy::OPTIONAL,
            KuduClientBuilder::EncryptionPolicy::REQUIRED,
            KuduClientBuilder::EncryptionPolicy::REQUIRED_REMOTE),
        ::testing::Values(true, false)));

TEST_P(EncryptionPolicyTest, TestEncryptionPolicy) {
  const auto& params = GetParam();
  if (!get<1>(params)) {
    cluster_opts_.enable_kerberos = false;
    cluster_opts_.extra_master_flags.emplace_back("--rpc_authentication=disabled");
    cluster_opts_.extra_tserver_flags.emplace_back("--rpc_authentication=disabled");
  }
  ASSERT_OK(StartCluster());

  shared_ptr<KuduClient> client;
  KuduClientBuilder b;
  b.encryption_policy(get<0>(params));
  ASSERT_OK(cluster_->CreateClient(&b, &client));
  SmokeTestCluster(client, /* transactional */ false);
}

Status AssignIPToClient(bool external) {
  // If the test does not require an external IP address
  // assign loopback IP to FLAGS_local_ip_for_outbound_sockets.
  if (!external) {
    FLAGS_local_ip_for_outbound_sockets = "127.0.0.1";
    return Status::OK();
  }

  // Try finding an external IP address to assign to
  // FLAGS_local_ip_for_outbound_sockets.
  vector<Network> local_networks;
  RETURN_NOT_OK(GetLocalNetworks(&local_networks));

  for (const auto& network : local_networks) {
    if (!network.IsLoopback()) {
      FLAGS_local_ip_for_outbound_sockets = network.GetAddrAsString();
      // Found and assigned an external IP.
      return Status::OK();
    }
  }

  // Could not find an external IP.
  return Status::NotFound("Could not find an external IP.");
}

// Descriptive constants for test parameters.
const bool LOOPBACK_ENCRYPTED = true;
const bool LOOPBACK_PLAIN = false;
const bool TOKEN_PRESENT = true;
const bool TOKEN_MISSING = false;
const bool LOOPBACK_CLIENT_IP = false;
const bool EXTERNAL_CLIENT_IP = true;
const char* const AUTH_REQUIRED = "required";
const char* const AUTH_DISABLED = "disabled";
const char* const ENCRYPTION_REQUIRED = "required";
const char* const ENCRYPTION_DISABLED = "disabled";

struct AuthTokenIssuingTestParams {
  const BindMode bind_mode;
  const string rpc_authentication;
  const string rpc_encryption;
  const bool rpc_encrypt_loopback_connections;
  const bool force_external_client_ip;
  const bool authn_token_present;
};

class AuthTokenIssuingTest :
    public SecurityITest,
    public ::testing::WithParamInterface<AuthTokenIssuingTestParams> {
};

INSTANTIATE_TEST_SUITE_P(, AuthTokenIssuingTest, ::testing::ValuesIn(
    vector<AuthTokenIssuingTestParams>{
      // The following 3 test cases cover passing authn token over an
      // encrypted loopback connection.
      // Note: AUTH_REQUIRED with ENCRYPTION_DISABLED is not
      // an acceptable configuration.
      { BindMode::LOOPBACK, AUTH_REQUIRED, ENCRYPTION_REQUIRED,
        LOOPBACK_ENCRYPTED, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_REQUIRED,
        LOOPBACK_ENCRYPTED, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_DISABLED,
        LOOPBACK_ENCRYPTED, LOOPBACK_CLIENT_IP, TOKEN_MISSING, },

      // The following 3 test cases cover passing authn token over an
      // unencrypted loopback connection.
      { BindMode::LOOPBACK, AUTH_REQUIRED, ENCRYPTION_REQUIRED,
        LOOPBACK_PLAIN, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_REQUIRED,
        LOOPBACK_PLAIN, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_DISABLED,
        LOOPBACK_PLAIN, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      // The following 3 test cases cover passing authn token over an
      // external connection.
      { BindMode::LOOPBACK, AUTH_REQUIRED, ENCRYPTION_REQUIRED,
        LOOPBACK_PLAIN, EXTERNAL_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_REQUIRED,
        LOOPBACK_PLAIN, EXTERNAL_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_DISABLED,
        LOOPBACK_PLAIN, EXTERNAL_CLIENT_IP, TOKEN_MISSING, },

      // The following 3 test cases verify that enforcement of encryption
      // for loopback connections does not affect external connections.
      { BindMode::LOOPBACK, AUTH_REQUIRED, ENCRYPTION_REQUIRED,
        LOOPBACK_ENCRYPTED, EXTERNAL_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_REQUIRED,
        LOOPBACK_ENCRYPTED, EXTERNAL_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::LOOPBACK, AUTH_DISABLED, ENCRYPTION_DISABLED,
        LOOPBACK_ENCRYPTED, EXTERNAL_CLIENT_IP, TOKEN_MISSING, },
#if defined(__linux__)
      // The following 6 test cases verify that a connection from 127.0.0.1
      // to another loopback address is treated as a loopback connection.
      { BindMode::UNIQUE_LOOPBACK, AUTH_REQUIRED, ENCRYPTION_REQUIRED,
        LOOPBACK_ENCRYPTED, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::UNIQUE_LOOPBACK, AUTH_DISABLED, ENCRYPTION_REQUIRED,
        LOOPBACK_ENCRYPTED, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::UNIQUE_LOOPBACK, AUTH_DISABLED, ENCRYPTION_DISABLED,
        LOOPBACK_ENCRYPTED, LOOPBACK_CLIENT_IP, TOKEN_MISSING, },

      { BindMode::UNIQUE_LOOPBACK, AUTH_REQUIRED, ENCRYPTION_REQUIRED,
        LOOPBACK_PLAIN, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::UNIQUE_LOOPBACK, AUTH_DISABLED, ENCRYPTION_REQUIRED,
        LOOPBACK_PLAIN, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },

      { BindMode::UNIQUE_LOOPBACK, AUTH_DISABLED, ENCRYPTION_DISABLED,
        LOOPBACK_PLAIN, LOOPBACK_CLIENT_IP, TOKEN_PRESENT, },
#endif
    }
));

// Verify how master issues authn tokens to clients. Master sends authn tokens
// to clients upon call of the ConnectToMaster() RPC. The master's behavior
// must depend on whether the connection to the client is confidential or not.
TEST_P(AuthTokenIssuingTest, ChannelConfidentiality) {
  cluster_opts_.num_masters = 1;
  cluster_opts_.num_tablet_servers = 0;
  // --user-acl: just restoring back the default setting.
  cluster_opts_.extra_master_flags.emplace_back("--user_acl=*");

  const auto& params = GetParam();

  // When testing external connections, make sure the client connects from
  // an external IP, so that the connection is not considered to be local.
  // When testing local connections, make sure that the client
  // connects from standard loopback IP.
  // Skip tests that require an external connection
  // if the host does not have a non-loopback interface.
  Status s = AssignIPToClient(params.force_external_client_ip);
  if (s.IsNotFound()) {
    LOG(WARNING) << s.message().ToString() << "\nSkipping external connection test.";
    return;
  }
  // Fail if GetLocalNetworks failed to determine network interfaces.
  ASSERT_OK(s);

  // Set flags and start cluster.
  cluster_opts_.bind_mode = params.bind_mode;
  cluster_opts_.extra_master_flags.emplace_back(
      Substitute("--rpc_authentication=$0", params.rpc_authentication));
  cluster_opts_.extra_master_flags.emplace_back(
      Substitute("--rpc_encryption=$0", params.rpc_encryption));
  cluster_opts_.extra_master_flags.emplace_back(
      Substitute("--rpc_encrypt_loopback_connections=$0",
                 params.rpc_encrypt_loopback_connections));
  ASSERT_OK(StartCluster());

  // In current implementation, KuduClientBuilder calls ConnectToCluster() on
  // the newly created instance of the KuduClient.
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));

  string authn_creds;
  ASSERT_OK(client->ExportAuthenticationCredentials(&authn_creds));
  client::AuthenticationCredentialsPB pb;
  ASSERT_TRUE(pb.ParseFromString(authn_creds));
  ASSERT_EQ(params.authn_token_present, pb.has_authn_token());

  if (pb.has_authn_token()) {
    // If authn token is present, then check it for consistency.
    const auto& t = pb.authn_token();
    EXPECT_TRUE(t.has_token_data());
    EXPECT_TRUE(t.has_signature());
    EXPECT_TRUE(t.has_signing_key_seq_num());
  }
}

struct ConnectToFollowerMasterTestParams {
  const string rpc_authentication;
  const string rpc_encryption;
};
class ConnectToFollowerMasterTest :
    public SecurityITest,
    public ::testing::WithParamInterface<ConnectToFollowerMasterTestParams> {
};
INSTANTIATE_TEST_SUITE_P(, ConnectToFollowerMasterTest, ::testing::ValuesIn(
    vector<ConnectToFollowerMasterTestParams>{
      { "required", "optional", },
      { "required", "required", },
    }
));

// Test that a client can authenticate against a follower master using
// authn token. For that, the token verifier at follower masters should have
// appropriate keys for authn token signature verification.
TEST_P(ConnectToFollowerMasterTest, AuthnTokenVerifierHaveKeys) {
  // Want to have control over the master leadership.
  cluster_opts_.extra_master_flags.emplace_back(
      "--enable_leader_failure_detection=false");
  cluster_opts_.num_masters = 3;
  const auto& params = GetParam();
  cluster_opts_.extra_master_flags.emplace_back(
      Substitute("--rpc_authentication=$0", params.rpc_authentication));
  cluster_opts_.extra_tserver_flags.emplace_back(
      Substitute("--rpc_authentication=$0", params.rpc_authentication));
  cluster_opts_.extra_master_flags.emplace_back(
      Substitute("--rpc_encryption=$0", params.rpc_encryption));
  cluster_opts_.extra_tserver_flags.emplace_back(
      Substitute("--rpc_encryption=$0", params.rpc_encryption));
  ASSERT_OK(StartCluster());

  const auto& master_host = cluster_->master(0)->bound_rpc_addr().host();
  {
    consensus::ConsensusServiceProxy proxy(
        cluster_->messenger(), cluster_->master(0)->bound_rpc_addr(), master_host);
    consensus::RunLeaderElectionRequestPB req;
    consensus::RunLeaderElectionResponsePB resp;
    rpc::RpcController rpc;
    req.set_tablet_id(master::SysCatalogTable::kSysCatalogTabletId);
    req.set_dest_uuid(cluster_->master(0)->uuid());
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy.RunLeaderElection(req, &resp, &rpc));
  }

  // Get authentication credentials.
  string authn_creds;
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    ASSERT_OK(client->ExportAuthenticationCredentials(&authn_creds));
  }

  ASSERT_OK(cluster_->kdc()->Kdestroy());
  // At this point the primary credentials (i.e. Kerberos) are not available.

  // Make sure it's not possible to connect without authn token at this point:
  // the server side is configured to require authentication.
  {
    shared_ptr<KuduClient> client;
    KuduClientBuilder builder;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    const auto s = builder.Build(&client);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  // Try to connect to every available follower master, authenticating using
  // only the authn token. This scenario uses some sort of 'negative' criterion
  // based on the fact that it's not possible to receive 'configuration error'
  // status without being sucessfully authenticated first. The configuration
  // error is returned because the client uses only a single master in its list
  // of masters' endpoints while trying to connect to a multi-master Kudu cluster.
  ASSERT_EVENTUALLY([&] {
    for (auto i = 1; i < cluster_->num_masters(); ++i) {
      shared_ptr<KuduClient> client;
      const auto s = KuduClientBuilder()
          .add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString())
          .import_authentication_credentials(authn_creds)
          .Build(&client);
      ASSERT_TRUE(s.IsConfigurationError()) << s.ToString();
      ASSERT_STR_MATCHES(s.ToString(),
                         ".* Client configured with 1 master.* "
                         "but cluster indicates it expects 3 master.*");
    }
  });

  // Although it's not in the exact scope of this test, make sure it's possible
  // to connect and perform basic operations (like listing tables) when using
  // secondary credentials only (i.e. authn token).
  {
    shared_ptr<KuduClient> client;
    KuduClientBuilder builder;
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      builder.add_master_server_addr(cluster_->master(i)->bound_rpc_addr().ToString());
    }
    builder.import_authentication_credentials(authn_creds);
    ASSERT_OK(builder.Build(&client));

    vector<string> tables;
    ASSERT_OK(client->ListTables(&tables));
  }
}

}  // namespace kudu
