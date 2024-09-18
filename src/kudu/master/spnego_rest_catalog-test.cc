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
#include <type_traits>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/rest_catalog_test_base.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
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
using std::string;
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
    ASSERT_OK(kdc_->CreateUserPrincipal("alice"));
    FLAGS_spnego_keytab_file = kt_path;
    FLAGS_webserver_require_spnego = true;
    FLAGS_enable_rest_api = true;

    auto opts = InternalMiniClusterOptions();
    opts.bind_mode = BindMode::LOOPBACK;

    cluster_.reset(new InternalMiniCluster(env_, opts));

    ASSERT_OK(cluster_->Start());
    ASSERT_OK(KuduClientBuilder()
                  .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                  .Build(&client_));
  }

 protected:
  unique_ptr<MiniKdc> kdc_;
  unique_ptr<InternalMiniCluster> cluster_;

  Status CreateTestTableAsAlice() { return CreateTestTable("alice@KRBTEST.COM"); }
};

TEST_F(SpnegoRestCatalogTest, TestGetTablesOneTable) {
  ASSERT_OK(kdc_->Kinit("alice"));
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
  ASSERT_OK(kdc_->Kinit("alice"));
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
  ASSERT_OK(kdc_->Kinit("alice"));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  c.set_auth(CurlAuthType::SPNEGO);
  Status s = c.PostToURL(
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
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->owner(), "alice");
}

TEST_F(SpnegoRestCatalogTest, TestDeleteTableWithUser) {
  ASSERT_OK(kdc_->Kinit("alice"));
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
  ASSERT_OK(kdc_->Kinit("alice"));
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
  ASSERT_EQ(table->schema().num_columns(), 3);
  ASSERT_STR_CONTAINS(table->owner(), "alice");
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
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
  ASSERT_STR_CONTAINS(buf.ToString(), "Must authenticate with SPNEGO.");
}

TEST_F(SpnegoRestCatalogTest, TestUnauthenticatedBadKeytab) {
  ASSERT_OK(kdc_->Kinit("alice"));
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

}  // namespace master
}  // namespace kudu
