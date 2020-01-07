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

#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::itest::MiniClusterFsInspector;
using kudu::itest::TServerDetails;
using std::map;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace tools {

class CreateTableToolTest : public KuduTest {
 public:
  ~CreateTableToolTest() {
    STLDeleteValues(&ts_map_);
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  void StartExternalMiniCluster(ExternalMiniClusterOptions opts = {});
  unique_ptr<ExternalMiniCluster> cluster_;
  unordered_map<string, TServerDetails*> ts_map_;
  unique_ptr<MiniClusterFsInspector> inspect_;
};

void CreateTableToolTest::StartExternalMiniCluster(ExternalMiniClusterOptions opts) {
  cluster_.reset(new ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  inspect_.reset(new MiniClusterFsInspector(cluster_.get()));
  ASSERT_OK(CreateTabletServerMap(cluster_->master_proxy(0),
                                  cluster_->messenger(), &ts_map_));
}

TEST_F(CreateTableToolTest, TestCreateTable) {
  constexpr auto kReplicationFactor = 4;
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kReplicationFactor;
  NO_FATALS(StartExternalMiniCluster(opts));
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder().add_master_server_addr(master_addr)
                               .Build(&client));

  // Test a few good cases.
  const auto check_good_input = [&](const string& json_str,
                                    const string& master,
                                    const string& table_name,
                                    const string& schema,
                                    const string& partition,
                                    const map<string, string>& extra_configs,
                                    KuduClient* client) {
    const vector<string> table_args = {
        "table", "create", master, json_str
    };
    bool table_exists = false;
    ASSERT_OK(RunKuduTool(table_args));
    ASSERT_EVENTUALLY([&] {
        ASSERT_OK(client->TableExists(table_name, &table_exists));
        ASSERT_TRUE(table_exists);
    });
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name, &table));
    ASSERT_EQ(table->name(), table_name);
    ASSERT_EQ(table->schema().ToString(), schema);
    ASSERT_EQ(table->partition_schema().DebugString(KuduSchema::ToSchema(
        table->schema())), partition);
    ASSERT_EQ(table->extra_configs(), extra_configs);
  };

  // Create a simple table.
  string simple_table = R"(
      {
          "table_name": "simple_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "encoding": 1,
                      "compression": 3
                  },
                  {
                      "column_name": "key",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "comment": "range key"
                  },
                  {
                      "column_name": "name",
                      "column_type": "BINARY",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id", "key"
              ]
          },
          "extra_configs" : {
              "configs" : {
                  "kudu.table.history_max_age_sec": "3600"
              }
          },
          "num_replicas": 3,
          "dimension_label": "test"
      }
  )";
  string schema = "(\n    id INT32 NOT NULL,\n    key STRING NOT NULL,\n    "
      "name BINARY NOT NULL,\n    PRIMARY KEY (id, key)\n)";
  string partition = "";
  map<string, string> extra_configs;
  extra_configs["kudu.table.history_max_age_sec"] = "3600";
  NO_FATALS(check_good_input(simple_table, master_addr, "simple_table",
      schema, partition, extra_configs, client.get()));

  // Create a hash table.
  string hash_table = R"(
      {
          "table_name": "hash_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "compression": 3
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2,
                      "seed": 100
                  }
              ]
          }
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    "
      "name STRING NOT NULL,\n    PRIMARY KEY (id)\n)";
  partition = "HASH (id) PARTITIONS 2 SEED 100";
  NO_FATALS(check_good_input(hash_table, master_addr, "hash_table",
      schema, partition, {}, client.get()));

  // Create a range table.
  string range_table = R"(
      {
          "table_name": "range_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  },
                  {
                      "column_name": "score",
                      "column_type": "DOUBLE",
                      "is_nullable": false,
                      "default_value": "0.0",
                      "comment": "user score"
                  }
              ],
              "key_column_names": [
                  "id", "name"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["id", "name"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["2", "zhangsan"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": 2,
                              "bound_values": ["2", "zhangsan"]
                          },
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["3", "lisi"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["3", "lisi"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    name STRING NOT NULL,\n"
      "    score DOUBLE NOT NULL,\n    PRIMARY KEY (id, name)\n)";
  partition = "RANGE (id, name)";
  NO_FATALS(check_good_input(range_table, master_addr, "range_table",
      schema, partition, {}, client.get()));

  // Create a hash+hash table.
  string hash_hash_table = R"(
      {
          "table_name": "hash_hash_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "key",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id", "key"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2
                  },
                  {
                      "columns": ["key"],
                      "num_buckets": 2
                  }
              ]
          },
          "extra_configs" : {
              "configs": {
                  "kudu.table.history_max_age_sec": "3600"
              }
          }
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    key STRING NOT NULL,\n"
      "    name STRING NOT NULL,\n    PRIMARY KEY (id, key)\n)";
  partition = "HASH (id) PARTITIONS 2, HASH (key) PARTITIONS 2";
  NO_FATALS(check_good_input(hash_hash_table, master_addr, "hash_hash_table",
      schema, partition, extra_configs, client.get()));

  // Create a hash+range table.
  string hash_range_table = R"(
      {
          "table_name": "hash_range_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "key",
                      "column_type": "INT64",
                      "is_nullable": false,
                      "comment": "range key"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id", "key"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2,
                      "seed": 100
                  }
              ],
              "range_partition": {
                  "columns": ["key"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": "EXCLUSIVE",
                              "bound_values": ["2"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["2"]
                          },
                          "upper_bound": {
                              "bound_type": "EXCLUSIVE",
                              "bound_values": ["3"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["3"]
                          }
                      }
                  ]
              }
          },
          "extra_configs" : {
              "configs" : {
                  "kudu.table.history_max_age_sec": "3600"
              }
          },
          "num_replicas": 3,
          "dimension_label": "test"
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    key INT64 NOT NULL,\n"
      "    name STRING NOT NULL,\n    PRIMARY KEY (id, key)\n)";
  partition = "HASH (id) PARTITIONS 2 SEED 100, RANGE (key)";
  NO_FATALS(check_good_input(hash_range_table, master_addr, "hash_range_table",
      schema, partition, extra_configs, client.get()));

  // Create a table with decimal, varchar, and date column types.
  string type_table = R"(
      {
          "table_name": "type_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT64",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "score",
                      "column_type": "DECIMAL",
                      "type_attributes": {
                          "precision": 10,
                          "scale": 10
                      },
                      "is_nullable": false,
                      "comment": "range key"
                  },
                  {
                      "column_name": "text",
                      "column_type": "VARCHAR",
                      "type_attributes": {
                          "length": 10
                      },
                      "is_nullable": false,
                      "default_value": "hello world"
                  },
                  {
                      "column_name": "create_date",
                      "column_type": "DATE",
                      "is_nullable": false
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "extra_configs" : {
              "configs" : {
                  "kudu.table.history_max_age_sec": "3600"
              }
          },
          "num_replicas": 3,
          "dimension_label": "test"
      }
  )";
  schema = "(\n    id INT64 NOT NULL,\n    score DECIMAL(10, 10) NOT NULL,\n"
           "    text VARCHAR(10) NOT NULL,\n    create_date DATE NOT NULL,\n"
           "    name STRING NOT NULL,\n    PRIMARY KEY (id)\n)";
  partition = "";
  extra_configs["kudu.table.history_max_age_sec"] = "3600";
  NO_FATALS(check_good_input(type_table, master_addr, "type_table",
      schema, partition, extra_configs, client.get()));

  // Create a table using string value instead of int for enum type,
  string enum_type_with_str = R"(
      {
          "table_name": "enum_type_with_str",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "encoding": "plain_encoding"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name",
                      "compression": "zlib"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["id"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": "EXCLUSIVE",
                              "bound_values": ["2"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["2"]
                          },
                          "upper_bound": {
                              "bound_type": "EXCLUSIVE",
                              "bound_values": ["3"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["3"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    name STRING NOT NULL,\n"
      "    PRIMARY KEY (id)\n)";
  partition = "RANGE (id)";
  NO_FATALS(check_good_input(enum_type_with_str, master_addr,
      "enum_type_with_str", schema, partition, {}, client.get()));

  // Create a table with invalid compression type, but it will be converted to default.
  string compression_type_unknown = R"(
      {
          "table_name": "compression_type_unknown",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "compression": 300,
                      "comment": "id"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          }
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    name STRING NOT NULL,\n"
      "    PRIMARY KEY (id)\n)";
  partition = "";
  NO_FATALS(check_good_input(compression_type_unknown, master_addr,
      "compression_type_unknown", schema, partition, {}, client.get()));

  // Create a table with invalid encoding type, but it will be converted to default.
  string encoding_type_unknown = R"(
      {
          "table_name": "encoding_type_unknown",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "encoding": 200
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          }
      }
  )";
  schema = "(\n    id INT32 NOT NULL,\n    name STRING NOT NULL,\n"
      "    PRIMARY KEY (id)\n)";
  partition = "";
  NO_FATALS(check_good_input(encoding_type_unknown, master_addr,
      "encoding_type_unknown", schema, partition, {}, client.get()));

  // Test a few error cases.
  const auto check_bad_input = [&](const string& json_str,
                                   const string& master,
                                   const string& err) {
    string stderr;
    string stdout;
    const vector<string> simple_table_args = {
        "table", "create", master, json_str
    };
    Status s = RunKuduTool(simple_table_args, &stdout, &stderr);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(stderr, err);
  };

  // JSON string is empty
  string empty_string = "";
  string err = "Unexpected end of string. Expected a value";
  NO_FATALS(check_bad_input(empty_string, master_addr, err));

  // JSON object is empty
  string empty_json = "{}";
  err = "Invalid argument: Missing table name";
  NO_FATALS(check_bad_input(empty_json, master_addr, err));

  // JSON object is invalid
  string invailed_json = "{\"table\": \"decimal_table\"}";
  err = "Cannot find field";
  NO_FATALS(check_bad_input(invailed_json, master_addr, err));

  // Create a table without primary key.
  string table_without_pk = R"(
      {
          "table_name": "table_without_pk",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "must specify at least one key column";
  NO_FATALS(check_bad_input(table_without_pk, master_addr, err));

  // Create a table without table name.
  string table_without_name = R"(
      {
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "Missing table name";
  NO_FATALS(check_bad_input(table_without_name, master_addr, err));

  // Create a table with primary key error.
  string primay_key_error = R"(
      {
          "table_name": "primay_key_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "ID"
              ]
          }
      }
  )";
  err = "primary key column not defined";
  NO_FATALS(check_bad_input(primay_key_error, master_addr, err));

  // Create a table with hash bucket error.
  string hash_bucket_error = R"(
      {
          "table_name": "hash_bucket_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 1
                  }
              ]
          }
      }
  )";
  err = "must have at least two hash buckets";
  NO_FATALS(check_bad_input(hash_bucket_error, master_addr, err));

  // Create a table with hash key error.
  string hash_key_error = R"(
      {
          "table_name": "hash_key_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["ID"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "unknown column: name: \"ID\"";
  NO_FATALS(check_bad_input(hash_key_error, master_addr, err));

  // Create a table with range key error.
  string range_key_error = R"(
      {
          "table_name": "range_key_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["key"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["2"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": 2,
                              "bound_values": ["2"]
                          },
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["3"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  err = "Invalid range value size, value size should be equal to number of range keys";
  NO_FATALS(check_bad_input(range_key_error, master_addr, err));

  // Create a table with range bound error.
  string range_bound_error = R"(
      {
          "table_name": "range_bound_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["id"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["3"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": 2,
                              "bound_values": ["2"]
                          },
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["4"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  err = "overlapping range partitions:";
  NO_FATALS(check_bad_input(range_bound_error, master_addr, err));

  // Create a table with range bound value error.
  string range_bound_value_error = R"(
      {
          "table_name": "range_bound_value_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["id"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["abc"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": 2,
                              "bound_values": ["2"]
                          },
                          "upper_bound": {
                              "bound_type": 1,
                              "bound_values": ["4"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  err = "JSON text is corrupt: Invalid value";
  NO_FATALS(check_bad_input(range_bound_value_error, master_addr, err));

  // Create a table with column type error.
  string column_type_error = R"(
      {
          "table_name": "column_type_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT31",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "data type INT31 is not supported";
  NO_FATALS(check_bad_input(column_type_error, master_addr, err));

  // Create a table with column default value error.
  string column_value_error = R"(
      {
          "table_name": "column_value_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "abc"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "JSON text is corrupt: Invalid value";
  NO_FATALS(check_bad_input(column_value_error, master_addr, err));

  // Create a table with pk not listed first.
  string pk_not_first = R"(
      {
          "table_name": "pk_not_first",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "name"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["name"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "primary key columns must be listed first in the schema";
  NO_FATALS(check_bad_input(pk_not_first, master_addr, err));

  // Create a table with column type and encoding conflict.
  string encoding_type_conflict = R"(
      {
          "table_name": "encoding_type_conflict",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "encoding": 4
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name",
                      "encoding": 4
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "hash_partitions": [
                  {
                      "columns": ["id"],
                      "num_buckets": 2
                  }
              ]
          }
      }
  )";
  err = "encoding DICT_ENCODING not supported for type INT32";
  NO_FATALS(check_bad_input(encoding_type_conflict, master_addr, err));

  // Create a table with encoding type errors,
  string encoding_type_error = R"(
      {
          "table_name": "encoding_type_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "encoding": "error_encoding"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name",
                      "compression": "zlib"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["id"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["2"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["2"]
                          },
                          "upper_bound": {
                              "bound_type": "EXCLUSIVE",
                              "bound_values": ["3"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["3"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  err = "unable to parse JSON";
  NO_FATALS(check_bad_input(encoding_type_error, master_addr, err));

  // Create a table with range partition bound type error,
  string bound_type_error = R"(
      {
          "table_name": "bound_type_error",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1",
                      "encoding": 1
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          },
          "partition": {
              "range_partition": {
                  "columns": ["id"],
                  "range_bounds": [
                      {
                          "upper_bound": {
                              "bound_type": 3,
                              "bound_values": ["2"]
                          }
                      },
                      {
                          "lower_bound": {
                              "bound_type": "INCLUSIVE",
                              "bound_values": ["3"]
                          }
                      }
                  ]
              }
          }
      }
  )";
  err = "Unexpected range partition bound type";
  NO_FATALS(check_bad_input(bound_type_error, master_addr, err));

  // Create a table with pk is nullable.
  string pk_is_nullable = R"(
      {
          "table_name": "pk_is_nullable",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": true,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          }
      }
  )";
  err = "Nullable key columns are not supported";
  NO_FATALS(check_bad_input(pk_is_nullable, master_addr, err));

  // Create a table that already exists.
  string existed_table = R"(
      {
          "table_name": "simple_table",
          "schema": {
              "columns": [
                  {
                      "column_name": "id",
                      "column_type": "INT32",
                      "is_nullable": false,
                      "default_value": "1"
                  },
                  {
                      "column_name": "name",
                      "column_type": "STRING",
                      "is_nullable": false,
                      "default_value": "zhangsan",
                      "comment": "user name"
                  }
              ],
              "key_column_names": [
                  "id"
              ]
          }
      }
  )";
  err = "table simple_table already exists with id";
  NO_FATALS(check_bad_input(existed_table, master_addr, err));
}

} // namespace tools
} // namespace kudu
