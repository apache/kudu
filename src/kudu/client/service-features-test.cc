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
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(master_support_1d_array_columns);
DECLARE_bool(tserver_support_1d_array_columns);

using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

class ServiceFeaturesITest : public KuduTest {
 public:
  void SetUp() override {
    // Set up the mini cluster
    cluster_.reset(new InternalMiniCluster(env_, {}));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// This test scenario verifies that Kudu C++ client of this version properly
// reports an error upon failed DDL requests involving tables with array type
// columns. If such DDL requests are run against Kudu master that doesn't
// support array type columns, and the behavior is as expected and the error
// message output by the client is actionable.
TEST_F(ServiceFeaturesITest, ArrayColumnSupportMaster) {
  constexpr const char* const kTableName = "array_columns_support_m";
  const Schema array_col_schema({
      ColumnSchema("key", INT32),
      ColumnSchemaBuilder().name("val").type(INT32).array(true).nullable(true)
  }, 1);

  // Make master not declaring its ARRAY_1D_COLUMN_TYPE feature.
  FLAGS_master_support_1d_array_columns = false;

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema(KuduSchema::FromSchema(array_col_schema));
  table_creator->table_name(kTableName)
      .schema(&schema)
      .add_hash_partitions({ "key" }, 2)
      .num_replicas(1);
  const auto s = table_creator->Create();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
      "Error creating table array_columns_support_m on the master: cluster "
      "does not support CreateTable with feature(s) ARRAY_1D_COLUMN_TYPE");

  // If master has ARRAY_1D_COLUMN_TYPE feature, it's possible to create tables
  // with array columns in the system catalog.
  FLAGS_master_support_1d_array_columns = true;
  ASSERT_OK(table_creator->Create());

  {
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->AddColumn("c0")->NestedType(
        KuduColumnSchema::KuduNestedTypeDescriptor(
            KuduColumnSchema::KuduArrayTypeDescriptor(KuduColumnSchema::INT8)));
    ASSERT_OK(alt->Alter());
  }

  // Below are unlikely sub-scenarios specific to disabling array type column
  // support in a Kudu master (i.e. --setting master_support_1d_array_columns=false)
  // after a table with array column(s) has been created during prior run of
  // the Kudu master while support for array type columns was enabled.
  FLAGS_master_support_1d_array_columns = false;

  // It's still possible to rename and drop the column, update the column's
  // comment, etc.  However, it's not possible to add a new array type column.
  {
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->AlterColumn("c0")->RenameTo("array0");
    ASSERT_OK(alt->Alter());
  }
  {
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->AlterColumn("array0")->Comment("comment");
    ASSERT_OK(alt->Alter());
  }
  {
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->DropColumn("array0");
    ASSERT_OK(alt->Alter());
  }
  {
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->AddColumn("c0")->NestedType(
        KuduColumnSchema::KuduNestedTypeDescriptor(
            KuduColumnSchema::KuduArrayTypeDescriptor(KuduColumnSchema::INT16)));
    const auto s = alt->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
        "cluster does not support AlterTable with feature(s) ARRAY_1D_COLUMN_TYPE");
  }
  {
    // It should be possible to open the table.
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
  }
  // It should be possible to drop the table.
  ASSERT_OK(client_->DeleteTable(kTableName));

  {
    vector<string> tables;
    ASSERT_OK(client_->ListTables(&tables));
    ASSERT_TRUE(tables.empty());
  }
}

TEST_F(ServiceFeaturesITest, ArrayColumnSupportTabletServer) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  constexpr const char* const kTableName = "array_columns_support_t";
  constexpr const char* const kErrMsgPattern =
      "Remote error: unsupported feature flags";

  const Schema array_schema({
      ColumnSchema("key", INT32),
      ColumnSchemaBuilder().name("val").type(INT32).array(true).nullable(true)
  }, 1);

  // Make tablet server not declaring its ARRAY_1D_COLUMN_TYPE feature.
  FLAGS_tserver_support_1d_array_columns = false;

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema(KuduSchema::FromSchema(array_schema));
  table_creator->table_name(kTableName)
      .schema(&schema)
      .add_hash_partitions({ "key" }, 2)
      .num_replicas(1)
      .timeout(MonoDelta::FromSeconds(1));

  {
    StringVectorSink capture_logs;
    {
      // Capture the log messages during attempts to create tablets for the new table.
      ScopedRegisterSink reg(&capture_logs);
      const auto s = table_creator->Create();
      // Since the only tablet server doesn't support creating tablets with schemas
      // containing array columns, waiting for tablets' creation eventually times out.
      ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "Timed out waiting for Table Creation");
    }

    bool pattern_found = false;
    for (const string& s : capture_logs.logged_msgs()) {
      if (string::npos != s.find("CreateTablet RPC failed for tablet") &&
          string::npos != s.find(kErrMsgPattern)) {
        pattern_found = true;
      }
    }
    ASSERT_TRUE(pattern_found);
  }

  // Since the system catalog retries adding tablet replicas, re-enable the
  // feature for the tablet server and wait until the system catalog succeeds
  // in creating all the tablet replicas.
  FLAGS_tserver_support_1d_array_columns = true;
  ASSERT_EVENTUALLY([&] {
    bool in_progress = true;
    ASSERT_OK(client_->IsCreateTableInProgress(kTableName, &in_progress));
    ASSERT_FALSE(in_progress);
  });

  // Disable the feature at the server side again.
  FLAGS_tserver_support_1d_array_columns = false;
  {
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->AddColumn("c0")->NestedType(
        KuduColumnSchema::KuduNestedTypeDescriptor(
            KuduColumnSchema::KuduArrayTypeDescriptor(KuduColumnSchema::INT8)));
    alt->timeout(MonoDelta::FromSeconds(1));

    StringVectorSink capture_logs;
    {
      // Capture the log messages during attempts to alter tablets for the new table.
      ScopedRegisterSink reg(&capture_logs);
      const auto s = alt->Alter();
      ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "Timed out waiting for AlterTable");
    }

    bool pattern_found = false;
    for (const string& s : capture_logs.logged_msgs()) {
      if (string::npos != s.find("AlterTable RPC failed for tablet") &&
          string::npos != s.find(kErrMsgPattern)) {
        pattern_found = true;
      }
    }
    ASSERT_TRUE(pattern_found);
  }

  // It should be possible to drop the table with array columns, even if
  // the tablet server doesn't support the ARRAY_1D_COLUMN_TYPE feature.
  ASSERT_OK(client_->DeleteTable(kTableName));

  {
    // The tablet server doesn't need to support the new ARRAY_1D_COLUMN_TYPE
    // feature if creating a new table without array columns.
    const Schema table_schema({
        ColumnSchema("key", INT32),
        ColumnSchema("val", INT64),
    }, 1);

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    KuduSchema schema(KuduSchema::FromSchema(table_schema));
    table_creator->table_name(kTableName)
        .schema(&schema)
        .add_hash_partitions({ "key" }, 2)
        .num_replicas(1);

    ASSERT_OK(table_creator->Create());

    // Adding a non-array column is possible as well.
    unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
    alt->AddColumn("c0")->Type(KuduColumnSchema::INT8)->Nullable();
    ASSERT_OK(alt->Alter());

    ASSERT_OK(client_->DeleteTable(kTableName));
  }
}

} // namespace client
} // namespace kudu
