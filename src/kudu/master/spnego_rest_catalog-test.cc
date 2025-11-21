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

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/rest_catalog_test_base.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/regex.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(spnego_keytab_file);
DECLARE_bool(webserver_require_spnego);
DECLARE_bool(enable_rest_api);

using kudu::Status;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::set;
using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace master {

class SpnegoRestCatalogTest : public RestCatalogTestBase {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    kdc_.reset(new MiniKdc(MiniKdcOptions{}));
    ASSERT_OK(kdc_->Start());
    ASSERT_OK(kdc_->SetKrb5Environment());
    string kt_path;
    ASSERT_OK(kdc_->CreateServiceKeytabWithName("HTTP/127.0.0.1", "spnego.dedicated", &kt_path));
    ASSERT_OK(kdc_->CreateUserPrincipal(kDefaultPrincipal));
    FLAGS_spnego_keytab_file = kt_path;
    FLAGS_webserver_require_spnego = true;
    FLAGS_enable_rest_api = true;

    InternalMiniClusterOptions opts;
    opts.bind_mode = BindMode::LOOPBACK;

    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));

    ASSERT_OK(cluster_->Start());
    ASSERT_OK(KuduClientBuilder()
                  .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                  .Build(&client_));
  }

 protected:
  unique_ptr<MiniKdc> kdc_;
  unique_ptr<InternalMiniCluster> cluster_;
  const string kDefaultPrincipal = "alice";

  Status CreateTestTableAsAlice() { return CreateTestTable("alice@KRBTEST.COM"); }
};

TEST_F(SpnegoRestCatalogTest, TestGetTablesOneTable) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(CreateTestTableAsAlice());
  EasyCurl c;
  faststring buf;
  c.set_auth(CurlAuthType::SPNEGO);
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf));
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"tables\":[{\"table_id\":\"$0\",\"table_name\":\"test_table\"}]}", table_id));
}

TEST_F(SpnegoRestCatalogTest, TestGetTableWithUser) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(CreateTestTableAsAlice());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_auth(CurlAuthType::SPNEGO);
  ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                  cluster_->mini_master()->bound_http_addr().ToString(),
                                  table_id),
                       &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(buf.ToString(), Substitute("\"owner\":\"$0\"", table->owner()));
}

TEST_F(SpnegoRestCatalogTest, TestPostTableWithUser) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  c.set_auth(CurlAuthType::SPNEGO);
  ASSERT_OK(c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      R"({
        "name": "test_table",
        "schema": {
          "columns": [
            {"name": "key", "type": "INT32", "is_nullable": false, "is_key": true},
            {"name": "int_val", "type": "INT32", "is_nullable": false, "is_key": false}
          ]
        },
        "partition_schema": {
          "range_schema": {
            "columns": [{"name": "key"}]
          }
        },
        "num_replicas": 1
      })",
      &buf));
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->owner(), kDefaultPrincipal);
}

TEST_F(SpnegoRestCatalogTest, TestDeleteTableWithUser) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(CreateTestTableAsAlice());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  c.set_auth(CurlAuthType::SPNEGO);
  ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                  cluster_->mini_master()->bound_http_addr().ToString(),
                                  table_id),
                       &buf));
  shared_ptr<KuduTable> table;
  Status s = client_->OpenTable(kTableName, &table);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(SpnegoRestCatalogTest, TestAlterTableWithUser) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(CreateTestTableAsAlice());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  c.set_auth(CurlAuthType::SPNEGO);
  ASSERT_OK(c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "alter_schema_steps": [
                            {
                              "type": "ADD_COLUMN",
                              "add_column": {
                                "schema": {
                                  "name": "new_column",
                                  "type": "STRING",
                                  "is_nullable": true
                                }
                              }
                            }
                          ]
                        }
                        )",
                        &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_EQ(3, table->schema().num_columns());
  ASSERT_STR_CONTAINS(table->owner(), kDefaultPrincipal);
}

TEST_F(SpnegoRestCatalogTest, TestInvalidHeaders) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  Status s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf,
      {"Authorization: I am an invalid header"});
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 500");

  s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf,
      {"Authorization: Negotiate I am also an invalid header"});
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
}

// The following tests are skipped on macOS due to inconsistent behavior of SPNEGO.
// macOS heimdal kerberos caches the KDC port number, which can cause subsequent tests to fail.
// For more details, refer to KUDU-3533 (https://issues.apache.org/jira/browse/KUDU-3533)
#ifndef __APPLE__

TEST_F(SpnegoRestCatalogTest, TestNoKinitGetTables) {
  EasyCurl c;
  faststring buf;
  c.set_auth(CurlAuthType::SPNEGO);
  Status s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
  ASSERT_STR_CONTAINS(buf.ToString(), "Must authenticate with SPNEGO.");
}

TEST_F(SpnegoRestCatalogTest, TestNoKinitGetTable) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  faststring buf;
  EasyCurl c;
  c.set_auth(CurlAuthType::SPNEGO);
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
  ASSERT_STR_CONTAINS(buf.ToString(), "Must authenticate with SPNEGO.");
}

TEST_F(SpnegoRestCatalogTest, TestUnauthenticatedBadKeytab) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(kdc_->RandomizePrincipalKey("HTTP/127.0.0.1"));
  EasyCurl c;
  faststring buf;
  c.set_auth(CurlAuthType::SPNEGO);
  Status s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
  ASSERT_STR_CONTAINS(buf.ToString(),
                      "Not authorized: Unspecified GSS failure.  Minor code may provide more "
                      "information: Request ticket server HTTP/127.0.0.1@KRBTEST.COM kvno 3 not "
                      "found in keytab; keytab is likely out of date");
}

#endif

class MultiMasterSpnegoTest : public RestCatalogTestBase {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    kdc_.reset(new MiniKdc(MiniKdcOptions{}));
    ASSERT_OK(kdc_->Start());
    ASSERT_OK(kdc_->SetKrb5Environment());
    string kt_path;
    ASSERT_OK(kdc_->CreateServiceKeytabWithName("HTTP/127.0.0.1", "spnego.dedicated", &kt_path));
    ASSERT_OK(kdc_->CreateUserPrincipal(kDefaultPrincipal));
    FLAGS_spnego_keytab_file = kt_path;
    FLAGS_webserver_require_spnego = true;
    FLAGS_enable_rest_api = true;

    InternalMiniClusterOptions opts;

    // Note: The bind_mode is explicitly set to LOOPBACK to ensure the mini-cluster
    // binds to 127.0.0.1, which matches the hostname in the service keytab principal
    // (HTTP/127.0.0.1).
    opts.bind_mode = BindMode::LOOPBACK;
    opts.num_masters = 3;

    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder client_builder;
    ASSERT_OK(cluster_->CreateClient(&client_builder, &client_));
  }

 protected:
  unique_ptr<MiniKdc> kdc_;
  unique_ptr<InternalMiniCluster> cluster_;
  const string kDefaultPrincipal = "alice";
};

TEST_F(MultiMasterSpnegoTest, TestAuthenticatedLeaderAccess) {
  // Test authenticated access to leader endpoint across all masters
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));

  ASSERT_EVENTUALLY([&]() {
    set<string> leader_addresses;
    static KuduRegex re("\"leader\":\"([^\"]+)\"", 1);

    for (int i = 0; i < cluster_->num_masters(); i++) {
      EasyCurl c;
      c.set_auth(CurlAuthType::SPNEGO);
      faststring buf;
      ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/leader",
                                      cluster_->mini_master(i)->bound_http_addr().ToString()),
                           &buf));
      vector<string> matches;
      ASSERT_TRUE(re.Match(buf.ToString(), &matches));
      ASSERT_EQ(1, matches.size()) << "Expected 1 regex match group, got " << matches.size();
      leader_addresses.insert(matches[0]);
    }

    // All masters should report the same leader with authentication
    ASSERT_EQ(1, leader_addresses.size()) << "Authenticated requests yielded different leaders: "
                                          << JoinStrings(leader_addresses, ", ");
  });
}

TEST_F(MultiMasterSpnegoTest, TestUnauthenticatedRequestsRejected) {
  // Test that requests without authentication are properly rejected
  const vector<string> paths = { "/api/v1/tables", "/api/v1/leader", "/api/v1/invalid" };

  for (int i = 0; i < cluster_->num_masters(); i++) {
    string master_addr = cluster_->mini_master(i)->bound_http_addr().ToString();

    for (const auto& path : paths) {
      EasyCurl c;
      faststring buf;
      string url = Substitute("http://$0$1", master_addr, path);

      Status s = c.FetchURL(url, &buf);
      ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
      ASSERT_STR_CONTAINS(buf.ToString(), "Must authenticate with SPNEGO");
    }
  }
}

TEST_F(MultiMasterSpnegoTest, TestTableOperationsWithAuthentication) {
  // Test table operations with proper authentication on leader master
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(CreateTestTable(kDefaultPrincipal));
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));

  int leader_idx = -1;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  ASSERT_GE(leader_idx, 0) << "Failed to identify leader master";
  string leader_addr = cluster_->mini_master(leader_idx)->bound_http_addr().ToString();

  EasyCurl c;
  faststring buf;
  c.set_auth(CurlAuthType::SPNEGO);

  // Verify that the created table appears in the tables list endpoint
  ASSERT_OK(c.FetchURL(Substitute("$0/api/v1/tables", leader_addr), &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), table_id);

  // Verify that the table details endpoint contains the correct principal information
  ASSERT_OK(c.FetchURL(Substitute("$0/api/v1/tables/$1", leader_addr, table_id), &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), kDefaultPrincipal);
}

// Test behavior when client makes requests to former leader after leadership change
TEST_F(MultiMasterSpnegoTest, TestRequestsToFormerLeaderAfterElection) {
  ASSERT_OK(kdc_->Kinit(kDefaultPrincipal));
  ASSERT_OK(CreateTestTable(kDefaultPrincipal));

  int original_leader_idx = -1;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&original_leader_idx));
  ASSERT_GE(original_leader_idx, 0) << "Failed to identify original leader master";
  string original_leader_addr =
      cluster_->mini_master(original_leader_idx)->bound_http_addr().ToString();

  // Force a leadership change by shutting down the current leader
  cluster_->mini_master(original_leader_idx)->Shutdown();

  // Wait for a new leader to be elected
  ASSERT_EVENTUALLY([&]() {
    int new_leader_idx = -1;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&new_leader_idx));
    ASSERT_GE(new_leader_idx, 0);
    ASSERT_NE(new_leader_idx, original_leader_idx) << "Leadership should have changed";
  });

  // Restart the former leader, which now should be a follower
  ASSERT_OK(cluster_->mini_master(original_leader_idx)->Restart());

  // Wait for the former leader to rejoin and stabilize
  ASSERT_EVENTUALLY([&]() {
    EasyCurl test_c;
    test_c.set_auth(CurlAuthType::SPNEGO);
    faststring test_buf;
    // The former leader should respond to curl requests
    ASSERT_OK(
        test_c.FetchURL(Substitute("http://$0/api/v1/leader", original_leader_addr), &test_buf));
  });

  // Now client makes requests to the former leader's endpoints
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));

  // Test various paths to the former leader
  const vector<string> paths = {"/api/v1/tables", Substitute("/api/v1/tables/$0", table_id)};

  for (const auto& path : paths) {
    EasyCurl c;
    faststring buf;
    c.set_auth(CurlAuthType::SPNEGO);

    Status s = c.FetchURL(Substitute("http://$0$1", original_leader_addr, path), &buf);
    ASSERT_STR_CONTAINS(s.ToString(), "HTTP 503");
    ASSERT_STR_CONTAINS(buf.ToString(), "\"error\"");
    ASSERT_STR_CONTAINS(buf.ToString(), "Master is not the leader");
  }
}

} // namespace master
} // namespace kudu
