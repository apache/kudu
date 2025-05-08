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
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/rest_catalog_test_base.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/regex.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::Status;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using rapidjson::Value;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(enable_rest_api);
DECLARE_string(webserver_doc_root);

namespace kudu {
namespace master {

class RestCatalogTest : public RestCatalogTestBase {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // Set REST endpoint flag to true
    FLAGS_enable_rest_api = true;

    // Set webserver doc root to enable swagger UI and static file serving
    string bin_path;
    ASSERT_OK(Env::Default()->GetExecutablePath(&bin_path));
    FLAGS_webserver_doc_root = JoinPathSegments(DirName(bin_path), "testdata/www");

    // Configure the mini-cluster
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));

    // Start the cluster
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(KuduClientBuilder()
                  .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                  .Build(&client_));
  }

  static int FindColumnId(string const& schema_json) {
    vector<string> matches;
    static KuduRegex re("\\{\"id\":([0-9]+),\"name\":\"key\"", 1);
    if (!re.Match(schema_json, &matches)) {
      return -1;
    }
    return std::stoi(matches[0]);
  }

  static string ConstructTableSchema(int column_id, bool is_there_new_column = false) {
    string columns = Substitute(
        "{\"id\":$0,\"name\":\"key\",\"type\":\"INT32\",\"is_key\":"
        "true,\"is_nullable\":false,\"encoding\":\"AUTO_ENCODING\",\"compression\":"
        "\"DEFAULT_COMPRESSION\",\"cfile_block_size\":0,\"immutable\":false},{\"id\":"
        "$1,\"name\":\"int_val\",\"type\":\"INT32\",\"is_key\":false,\"is_nullable\":"
        "false,\"encoding\":\"AUTO_ENCODING\",\"compression\":\"DEFAULT_COMPRESSION\","
        "\"cfile_block_size\":0,\"immutable\":false}",
        column_id,
        column_id + 1);
    if (is_there_new_column) {
      string column_id_2_str = std::to_string(column_id + 2);
      string new_column = Substitute(
          ",{\"id\":$0,\"name\":\"new_column\",\"type\":\"STRING\",\"is_key\":false,"
          "\"is_nullable\":true,\"encoding\":\"AUTO_ENCODING\",\"compression\":"
          "\"DEFAULT_COMPRESSION\",\"cfile_block_size\":0,\"immutable\":false}",
          column_id_2_str);
      columns += new_column;
    }
    return Substitute("{\"columns\":[$0]}", columns);
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  const std::string kTablePartitionSchema = "{\"range_schema\":{\"columns\":[{\"id\":10}]}}";
  const std::string kTablePartitionSchemaColumnIdZero =
      "{\"range_schema\":{\"columns\":[{\"id\":0}]}}";
};

TEST_F(RestCatalogTest, TestInvalidMethods) {
  const std::vector<std::string> invalid_methods = {"PUT", "DELETE", "CONNECT"};

  for (const auto& method : invalid_methods) {
    EasyCurl c;
    c.set_custom_method(method);
    faststring buf;
    Status s;
    if (method == "PUT") {
      s = c.PostToURL(Substitute("http://$0/api/v1/tables",
                                 cluster_->mini_master()->bound_http_addr().ToString()),
                      "{\"name\":\"test_table\"}",
                      &buf);
    } else {
      s = c.FetchURL(Substitute("http://$0/api/v1/tables",
                                cluster_->mini_master()->bound_http_addr().ToString()),
                     &buf);
    }
    ASSERT_TRUE(!s.ok());
    ASSERT_STR_CONTAINS(s.ToString(), "HTTP 405") << "Failed for method: " << method;
    ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Method not allowed\"}")
        << "Failed for method: " << method;
  }
}

TEST_F(RestCatalogTest, TestInvalidMethodOnTableEndpoint) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  const std::vector<std::string> invalid_methods = {"POST", "CONNECT"};

  for (const auto& method : invalid_methods) {
    EasyCurl c;
    c.set_custom_method(method);
    faststring buf;
    Status s;
    if (method == "POST") {
      s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                 cluster_->mini_master()->bound_http_addr().ToString(),
                                 table_id),
                      "{\"name\":\"test_table\"}",
                      &buf);
    } else {
      s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                cluster_->mini_master()->bound_http_addr().ToString(),
                                table_id),
                     &buf);
    }
    ASSERT_TRUE(!s.ok());
    ASSERT_STR_CONTAINS(s.ToString(), "HTTP 405") << "Failed for method: " << method;
    ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Method not allowed\"}")
        << "Failed for method: " << method;
  }
}

TEST_F(RestCatalogTest, TestGetTablesZeroTables) {
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"tables\":[]}");
}

TEST_F(RestCatalogTest, TestGetTablesOneTable) {
  ASSERT_OK(CreateTestTable());
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf));
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"tables\":[{\"table_id\":\"$0\",\"table_name\":\"test_table\"}]}", table_id));
}

TEST_F(RestCatalogTest, TestGetTableNoId) {
  EasyCurl c;
  faststring buf;
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
}

TEST_F(RestCatalogTest, TestGetTableEndpoint) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                  cluster_->mini_master()->bound_http_addr().ToString(),
                                  table_id),
                       &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  int first_column_id = FindColumnId(buf.ToString());
  ASSERT_NE(first_column_id, -1) << "Column ID not found in the schema";

  // In DEBUG builds, column ID starts from 10 while in RELEASE builds it starts from 0.
  string partition_schema =
      (first_column_id == 0) ? kTablePartitionSchemaColumnIdZero : kTablePartitionSchema;
  ASSERT_STR_CONTAINS(buf.ToString(),
                      Substitute("{\"name\":\"test_table\",\"id\":\"$0\",\"schema\":$1,"
                                 "\"partition_schema\":$2,\"owner\":\"$3\","
                                 "\"comment\":\"\",\"extra_config\":{}}",
                                 table_id,
                                 ConstructTableSchema(first_column_id),
                                 partition_schema,
                                 table->owner()));
}

TEST_F(RestCatalogTest, TestGetTableNotFound) {
  EasyCurl c;
  faststring buf;
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/05755b4c0c7640cd9f6673c2530a4e78",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Table not found\"}");
}

TEST_F(RestCatalogTest, TestGetTableMalformedId) {
  EasyCurl c;
  faststring buf;
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/123",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(buf.ToString(),
                      "{\"error\":\"Invalid table ID: must be exactly 32 characters long.\"}");
}

TEST_F(RestCatalogTest, TestDeleteTableNonExistent) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/05755b4c0c7640cd9f6673c2530a4e78",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Table not found\"}");
}

TEST_F(RestCatalogTest, TestDeleteTableEndpoint) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                  cluster_->mini_master()->bound_http_addr().ToString(),
                                  table_id),
                       &buf));
  ASSERT_TRUE(buf.size() == 0);
  shared_ptr<KuduTable> table;
  Status s = client_->OpenTable(kTableName, &table);
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Not found: the table does not exist: table_name: \"test_table\"");
  ASSERT_TRUE(table == nullptr);
}

TEST_F(RestCatalogTest, TestDeleteTableMalformedId) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/123",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(buf.ToString(),
                      "{\"error\":\"Invalid table ID: must be exactly 32 characters long.\"}");
}

TEST_F(RestCatalogTest, TestPostTableNoData) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  Status s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 411");
}

TEST_F(RestCatalogTest, TestPostTableMalformedData) {
  EasyCurl c;
  faststring buf;
  Status s = c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      "{\"name\":\"test_table\"}",
      &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      "{\"error\":\"JSON table object is not correct: {\\\"name\\\":\\\"test_table\\\"}\"}");
}

TEST_F(RestCatalogTest, TestPostTableEndpoint) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
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
  int first_column_id = FindColumnId(buf.ToString());
  ASSERT_NE(first_column_id, -1) << "Column ID not found in the schema";
  string partition_schema =
      (first_column_id == 0) ? kTablePartitionSchemaColumnIdZero : kTablePartitionSchema;
  ASSERT_STR_CONTAINS(buf.ToString(),
                      Substitute("{\"name\":\"test_table\",\"id\":\"$0\",\"schema\":$1,\"partition_"
                                 "schema\":$2,"
                                 "\"owner\":\"$3\",\"comment\":\"\",\"extra_config\":{}}",
                                 table_id,
                                 ConstructTableSchema(first_column_id),
                                 partition_schema,
                                 table->owner()));
  ASSERT_TRUE(table != nullptr);
  ASSERT_EQ(kTableName, table->name());
  ASSERT_EQ(1, table->num_replicas());
}

TEST_F(RestCatalogTest, TestPutTableMalformedId) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/123",
                                    cluster_->mini_master()->bound_http_addr().ToString()),
                         "{\"name\":\"test_table\"}",
                         &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(buf.ToString(),
                      "{\"error\":\"Invalid table ID: must be exactly 32 characters long.\"}");
}

TEST_F(RestCatalogTest, TestPutTableNoData) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 411");
}

TEST_F(RestCatalogTest, TestPutTableMalformedData) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                    cluster_->mini_master()->bound_http_addr().ToString(),
                                    table_id),
                         "{\"name\":\"test_table\"}",
                         &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      "{\"error\":\"JSON table object is not correct: {\\\"name\\\":\\\"test_table\\\"}\"}");
}

TEST_F(RestCatalogTest, TestPutTableNonExistent) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/05755b4c0c7640cd9f6673c2530a4e78",
                                    cluster_->mini_master()->bound_http_addr().ToString()),
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
                         &buf);
  ASSERT_TRUE(!s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Table not found\"}");
}

TEST_F(RestCatalogTest, TestPutTableEndpointAddColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
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
  int first_column_id = FindColumnId(buf.ToString());
  ASSERT_NE(first_column_id, -1) << "Column ID not found in the schema";
  string partition_schema =
      (first_column_id == 0) ? kTablePartitionSchemaColumnIdZero : kTablePartitionSchema;
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"name\":\"test_table\",\"id\":\"$0\",\"schema\":$1,\"partition_schema\":$2,"
                 "\"owner\":\"$3\",\"comment\":\"\",\"extra_config\":{}}",
                 table_id,
                 ConstructTableSchema(first_column_id, true),
                 partition_schema,
                 table->owner()));
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(3, schema.num_columns());
  ASSERT_EQ("key", schema.Column(0).name());
  ASSERT_EQ("int_val", schema.Column(1).name());
  ASSERT_EQ("new_column", schema.Column(2).name());
}

TEST_F(RestCatalogTest, TestPutTableEndpointRenameColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  ASSERT_OK(c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "alter_schema_steps": [
                            {
                              "type": "RENAME_COLUMN",
                              "rename_column": {
                                "old_name": "int_val",
                                "new_name": "new_int_val"
                              }
                            }
                          ]
                        }
                        )",
                        &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(2, schema.num_columns());
  ASSERT_EQ("key", schema.Column(0).name());
  ASSERT_EQ("new_int_val", schema.Column(1).name());
}

TEST_F(RestCatalogTest, TestPutTableEndpointDropColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  ASSERT_OK(c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        R"({
                            "table": {
                              "table_name": "test_table"
                            },
                            "alter_schema_steps": [
                              {
                                "type": "DROP_COLUMN",
                                "drop_column": {
                                  "name": "int_val"
                                }
                              }
                            ]
                          }
                          )",
                        &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(1, schema.num_columns());
  ASSERT_EQ("key", schema.Column(0).name());
}

TEST_F(RestCatalogTest, TestPutTableEndpointChangeOwner) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  ASSERT_OK(c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "new_table_owner": "new_owner"
                        }
                        )",
                        &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_EQ("new_owner", table->owner());
}

TEST_F(RestCatalogTest, TestPostTableWithArrayColumns) {
  const string kArrayTableName = "array_table";
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  ASSERT_OK(c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      R"({
        "name": "array_table",
        "schema": {
          "columns": [
            {"name": "key", "type": "INT32", "is_nullable": false, "is_key": true},
            {"name": "int_val", "type": "INT32", "is_nullable": false, "is_key": false},
            {
              "name": "arr_int32",
              "type": "NESTED",
              "nested_type": {"array": {"type": "INT32"}},
              "is_nullable": true,
              "is_key": false
            },
            {
              "name": "arr_int64",
              "type": "NESTED",
              "nested_type": {"array": {"type": "INT64"}},
              "is_nullable": true,
              "is_key": false
            },
            {
              "name": "arr_string",
              "type": "NESTED",
              "nested_type": {"array": {"type": "STRING"}},
              "is_nullable": true,
              "is_key": false
            }
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
  ASSERT_OK(GetTableId(kArrayTableName, &table_id));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kArrayTableName, &table));
  ASSERT_TRUE(table != nullptr);
  ASSERT_EQ(kArrayTableName, table->name());

  const KuduSchema& schema = table->schema();
  ASSERT_EQ(5, schema.num_columns());
  ASSERT_EQ("key", schema.Column(0).name());
  ASSERT_EQ("int_val", schema.Column(1).name());
  ASSERT_EQ("arr_int32", schema.Column(2).name());
  ASSERT_EQ("arr_int64", schema.Column(3).name());
  ASSERT_EQ("arr_string", schema.Column(4).name());

  ASSERT_STR_CONTAINS(buf.ToString(), "\"type\":\"NESTED\"");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"nested_type\"");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"array\"");
}

TEST_F(RestCatalogTest, TestGetTableWithArrayColumns) {
  const string kArrayTableName = "array_table_get";
  client::KuduSchema schema;
  client::KuduSchemaBuilder b;
  b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(client::KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("arr_double")
      ->Type(client::KuduColumnSchema::NESTED)
      ->Nullable()
      ->NestedType(client::KuduColumnSchema::KuduNestedTypeDescriptor(
          client::KuduColumnSchema::KuduArrayTypeDescriptor(client::KuduColumnSchema::DOUBLE)));
  b.AddColumn("arr_bool")
      ->Type(client::KuduColumnSchema::NESTED)
      ->Nullable()
      ->NestedType(client::KuduColumnSchema::KuduNestedTypeDescriptor(
          client::KuduColumnSchema::KuduArrayTypeDescriptor(client::KuduColumnSchema::BOOL)));
  ASSERT_OK(b.Build(&schema));

  const vector<string> column_names{"key"};
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kArrayTableName)
                .schema(&schema)
                .set_range_partition_columns(column_names)
                .num_replicas(1)
                .Create());

  string table_id;
  ASSERT_OK(GetTableId(kArrayTableName, &table_id));

  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                  cluster_->mini_master()->bound_http_addr().ToString(),
                                  table_id),
                       &buf));

  const string response = buf.ToString();
  ASSERT_STR_CONTAINS(response, "\"name\":\"array_table_get\"");
  ASSERT_STR_CONTAINS(response, "\"arr_double\"");
  ASSERT_STR_CONTAINS(response, "\"arr_bool\"");
  ASSERT_STR_CONTAINS(response, "\"type\":\"NESTED\"");
  ASSERT_STR_CONTAINS(response, "\"nested_type\"");
  ASSERT_STR_CONTAINS(response, "\"array\"");
  ASSERT_STR_CONTAINS(response, "\"DOUBLE\"");
  ASSERT_STR_CONTAINS(response, "\"BOOL\"");
}

TEST_F(RestCatalogTest, TestPutTableAddArrayColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id;
  ASSERT_OK(GetTableId(kTableName, &table_id));
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
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
                                  "name": "arr_float",
                                  "type": "NESTED",
                                  "nested_type": {"array": {"type": "FLOAT"}},
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
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(3, schema.num_columns());
  ASSERT_EQ("key", schema.Column(0).name());
  ASSERT_EQ("int_val", schema.Column(1).name());
  ASSERT_EQ("arr_float", schema.Column(2).name());

  const string response = buf.ToString();
  ASSERT_STR_CONTAINS(response, "\"arr_float\"");
  ASSERT_STR_CONTAINS(response, "\"type\":\"NESTED\"");
  ASSERT_STR_CONTAINS(response, "\"nested_type\"");
  ASSERT_STR_CONTAINS(response, "\"array\"");
  ASSERT_STR_CONTAINS(response, "\"FLOAT\"");
}

TEST_F(RestCatalogTest, TestPostTableWithMultipleArrayTypes) {
  const string kArrayTableName = "multi_array_table";
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  ASSERT_OK(c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      R"({
        "name": "multi_array_table",
        "schema": {
          "columns": [
            {"name": "key", "type": "INT32", "is_nullable": false, "is_key": true},
            {
              "name": "arr_int8",
              "type": "NESTED",
              "nested_type": {"array": {"type": "INT8"}},
              "is_nullable": true,
              "is_key": false
            },
            {
              "name": "arr_int16",
              "type": "NESTED",
              "nested_type": {"array": {"type": "INT16"}},
              "is_nullable": true,
              "is_key": false
            },
            {
              "name": "arr_binary",
              "type": "NESTED",
              "nested_type": {"array": {"type": "BINARY"}},
              "is_nullable": true,
              "is_key": false
            },
            {
              "name": "arr_date",
              "type": "NESTED",
              "nested_type": {"array": {"type": "DATE"}},
              "is_nullable": false,
              "is_key": false
            }
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
  ASSERT_OK(GetTableId(kArrayTableName, &table_id));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kArrayTableName, &table));
  ASSERT_TRUE(table != nullptr);

  const KuduSchema& schema = table->schema();
  ASSERT_EQ(5, schema.num_columns());
  ASSERT_EQ("arr_int8", schema.Column(1).name());
  ASSERT_EQ("arr_int16", schema.Column(2).name());
  ASSERT_EQ("arr_binary", schema.Column(3).name());
  ASSERT_EQ("arr_date", schema.Column(4).name());

  const string response = buf.ToString();
  ASSERT_STR_CONTAINS(response, "\"INT8\"");
  ASSERT_STR_CONTAINS(response, "\"INT16\"");
  ASSERT_STR_CONTAINS(response, "\"BINARY\"");
  ASSERT_STR_CONTAINS(response, "\"DATE\"");
}

class MultiMasterTest : public RestCatalogTestBase {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_enable_rest_api = true;

    InternalMiniClusterOptions opts;
    opts.num_masters = 3;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder client_builder;
    ASSERT_OK(cluster_->CreateClient(&client_builder, &client_));
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
};

TEST_F(MultiMasterTest, TestGetLeaderEndpoint) {
  set<string> leader_addresses;

  static KuduRegex re("\"leader\":\"([^\"]+)\"", 1);

  for (int i = 0; i < cluster_->num_masters(); i++) {
    EasyCurl c;
    faststring buf;
    ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/leader",
                                    cluster_->mini_master(i)->bound_http_addr().ToString()),
                         &buf));
    vector<string> matches;
    ASSERT_TRUE(re.Match(buf.ToString(), &matches));
    ASSERT_FALSE(matches.empty()) << "No leader match in: " << buf.ToString();

    leader_addresses.insert(matches[0]);
  }

  // All masters should report the same leader
  ASSERT_EQ(1, leader_addresses.size())
      << JoinStrings(leader_addresses, ", ");

  string leader_addr = *leader_addresses.begin();
  ASSERT_FALSE(leader_addr.empty());
  EasyCurl leader_curl;
  faststring leader_buf;
  leader_curl.set_verbose(true);
  ASSERT_OK(leader_curl.FetchURL(Substitute("$0/api/v1/tables", leader_addr),
                                &leader_buf));
  ASSERT_STR_CONTAINS(leader_buf.ToString(), "{\"tables\":[]}");
}

TEST_F(RestCatalogTest, TestApiSpecEndpoint) {
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/spec", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf));

  string spec_json = buf.ToString();
  ASSERT_FALSE(spec_json.empty()) << "API spec should not be empty";

  JsonReader reader(spec_json);
  ASSERT_OK(reader.Init());

  // Helper lambda to verify an object exists in parent
  auto VerifyObjectExists = [&reader](const Value* parent, const char* field) {
    const Value* obj;
    Status s = reader.ExtractObject(parent, field, &obj);
    ASSERT_TRUE(s.ok()) << "Field '" << field << "' not found: " << s.ToString();
  };

  string openapi_version;
  ASSERT_OK(reader.ExtractString(reader.root(), "openapi", &openapi_version));
  ASSERT_FALSE(openapi_version.empty());

  const Value* paths;
  ASSERT_OK(reader.ExtractObject(reader.root(), "paths", &paths));

  const Value* tables_path;
  ASSERT_OK(reader.ExtractObject(paths, "/tables", &tables_path));
  NO_FATALS(VerifyObjectExists(tables_path, "get"));
  NO_FATALS(VerifyObjectExists(tables_path, "post"));

  const Value* table_by_id_path;
  ASSERT_OK(reader.ExtractObject(paths, "/tables/{table_id}", &table_by_id_path));
  NO_FATALS(VerifyObjectExists(table_by_id_path, "get"));
  NO_FATALS(VerifyObjectExists(table_by_id_path, "put"));
  NO_FATALS(VerifyObjectExists(table_by_id_path, "delete"));

  const Value* leader_path;
  ASSERT_OK(reader.ExtractObject(paths, "/leader", &leader_path));
  NO_FATALS(VerifyObjectExists(leader_path, "get"));

  const Value* components;
  ASSERT_OK(reader.ExtractObject(reader.root(), "components", &components));
  const Value* schemas;
  ASSERT_OK(reader.ExtractObject(components, "schemas", &schemas));

  const vector<string> expected_schemas = {"TablesResponse", "TableInfo", "TableResponse",
                                           "LeaderResponse", "ErrorResponse", "TableSchema",
                                           "PartitionSchema", "CreateTableRequest",
                                           "AlterTableRequest"};
  for (const auto& schema_name : expected_schemas) {
    NO_FATALS(VerifyObjectExists(schemas, schema_name.c_str()));
  }
}

TEST_F(RestCatalogTest, TestApiDocsEndpoint) {
  EasyCurl c;
  faststring buf;
  Status s = c.FetchURL(
      Substitute("http://$0/api/docs", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf);

  ASSERT_TRUE(s.ok()) << "API docs endpoint should return HTTP 200: " << s.ToString();

  string content = buf.ToString();
  ASSERT_FALSE(content.empty()) << "API docs should not be empty";

  ASSERT_STR_CONTAINS(content, "swagger-ui.css") << "Should include Swagger UI CSS";
  ASSERT_STR_CONTAINS(content, "swagger-ui-bundle.js") << "Should include Swagger UI JS bundle";
  ASSERT_STR_CONTAINS(content, "kudu-swagger-init.js") << "Should include Kudu Swagger init script";

  ASSERT_STR_CONTAINS(content, "id=\"swagger-ui\"") << "Should have Swagger UI container div";

  // Verify it has loading or ready state text
  bool has_swagger_content =
      content.find("swagger-ui") != string::npos || content.find("API") != string::npos;
  ASSERT_TRUE(has_swagger_content) << "Should contain Swagger UI or API-related content";
}

}  // namespace master
}  // namespace kudu
