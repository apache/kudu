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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using client::KuduColumnSchema;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::sp::shared_ptr;
using cluster::ExternalMiniClusterOptions;
using hms::HmsClient;
using hms::HmsClientOptions;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

// Test Master <-> HMS catalog synchronization.
class MasterHmsTest : public ExternalMiniClusterITestBase {
 public:

  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();

    ExternalMiniClusterOptions opts;
    opts.enable_hive_metastore = true;
    opts.num_masters = 1;
    opts.num_tablet_servers = 1;
    StartClusterWithOpts(std::move(opts));

    hms_client_.reset(new HmsClient(cluster_->hms()->address(), HmsClientOptions()));
    ASSERT_OK(hms_client_->Start());
  }

  void TearDown() override {
    ASSERT_OK(hms_client_->Stop());
    ExternalMiniClusterITestBase::TearDown();
  }

  Status StopHms() {
    RETURN_NOT_OK(hms_client_->Stop());
    RETURN_NOT_OK(cluster_->hms()->Stop());
    return Status::OK();
  }

  Status StartHms() {
    RETURN_NOT_OK(cluster_->hms()->Start());
    RETURN_NOT_OK(hms_client_->Start());
    return Status::OK();
  }

  Status CreateDatabase(const string& database_name) {
    hive::Database db;
    db.name = database_name;
    RETURN_NOT_OK(hms_client_->CreateDatabase(db));
    // Sanity check that the DB is created.
    RETURN_NOT_OK(hms_client_->GetDatabase(database_name, &db));
    return Status::OK();
  }

  Status CreateKuduTable(const string& database_name, const string& table_name) {
    // Get coverage of all column types.
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int8_val")->Type(KuduColumnSchema::INT8);
    b.AddColumn("int16_val")->Type(KuduColumnSchema::INT16);
    b.AddColumn("int32_val")->Type(KuduColumnSchema::INT32);
    b.AddColumn("int64_val")->Type(KuduColumnSchema::INT64);
    b.AddColumn("timestamp_val")->Type(KuduColumnSchema::UNIXTIME_MICROS);
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    b.AddColumn("bool_val")->Type(KuduColumnSchema::BOOL);
    b.AddColumn("float_val")->Type(KuduColumnSchema::FLOAT);
    b.AddColumn("double_val")->Type(KuduColumnSchema::DOUBLE);
    b.AddColumn("binary_val")->Type(KuduColumnSchema::BINARY);
    b.AddColumn("decimal32_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal32Precision);
    b.AddColumn("decimal64_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal64Precision);
    b.AddColumn("decimal128_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal128Precision);

    RETURN_NOT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    return table_creator->table_name(Substitute("$0.$1", database_name, table_name))
                         .schema(&schema)
                         .num_replicas(1)
                         .set_range_partition_columns({ "key" })
                         .Create();
  }

  // Checks that the Kudu table schema and the HMS table entry in their
  // respective catalogs are synchronized for a particular table.
  void CheckTable(const string& database_name, const string& table_name) {
    SCOPED_TRACE(Substitute("Checking table $0.$1", database_name, table_name));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(Substitute("$0.$1", database_name, table_name), &table));
    KuduSchema schema = table->schema();

    hive::Table hms_table;
    ASSERT_OK(hms_client_->GetTable(database_name, table_name, &hms_table));

    ASSERT_EQ(schema.num_columns(), hms_table.sd.cols.size());
    for (int idx = 0; idx < schema.num_columns(); idx++) {
      ASSERT_EQ(schema.Column(idx).name(), hms_table.sd.cols[idx].name);
    }
    ASSERT_EQ(table->id(), hms_table.parameters[hms::HmsClient::kKuduTableIdKey]);
    ASSERT_EQ(HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs()),
              hms_table.parameters[hms::HmsClient::kKuduMasterAddrsKey]);
    ASSERT_EQ(hms::HmsClient::kKuduStorageHandler,
              hms_table.parameters[hms::HmsClient::kStorageHandlerKey]);
  }

  // Checks that a table does not exist in the Kudu and HMS catalogs.
  void CheckTableDoesNotExist(const string& database_name, const string& table_name) {
    SCOPED_TRACE(Substitute("Checking table $0.$1 does not exist", database_name, table_name));

    shared_ptr<KuduTable> table;
    Status s = client_->OpenTable(Substitute("$0.$1", database_name, table_name), &table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();

    hive::Table hms_table;
    s = hms_client_->GetTable(database_name, table_name, &hms_table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

 protected:

  unique_ptr<HmsClient> hms_client_;
};

TEST_F(MasterHmsTest, TestCreateTable) {
  const char* hms_database_name = "create_db";
  const char* hms_table_name = "table";
  string table_name = Substitute("$0.$1", hms_database_name, hms_table_name);

  // Attempt to create the table before the database is created.
  Status s = CreateKuduTable(hms_database_name, hms_table_name);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  ASSERT_OK(CreateDatabase(hms_database_name));

  // Create a table entry with the name.
  hive::Table hms_table;
  hms_table.dbName = hms_database_name;
  hms_table.tableName = hms_table_name;
  ASSERT_OK(hms_client_->CreateTable(hms_table));

  // Attempt to create a Kudu table with the same name.
  s = CreateKuduTable(hms_database_name, hms_table_name);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "table already exists");

  // Attempt to create a Kudu table to an invalid table name.
  s = CreateKuduTable(hms_database_name, "☃");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "☃ is not a valid object name");

  // Drop the HMS entry and create the table through Kudu.
  ASSERT_OK(hms_client_->DropTableWithContext(hms_database_name, hms_table_name,
                                              hive::EnvironmentContext()));
  ASSERT_OK(CreateKuduTable(hms_database_name, hms_table_name));
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));

  // Shutdown the HMS and try to create a table.
  ASSERT_OK(StopHms());

  s = CreateKuduTable(hms_database_name, "foo");
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS and try again.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(CreateKuduTable(hms_database_name, "foo"));
  });
  NO_FATALS(CheckTable(hms_database_name, "foo"));
}

TEST_F(MasterHmsTest, TestRenameTable) {
  // Create the database and Kudu table.
  ASSERT_OK(CreateDatabase("db"));
  ASSERT_OK(CreateKuduTable("db", "a"));
  NO_FATALS(CheckTable("db", "a"));

  // Create a non-Kudu ('external') HMS table entry.
  hive::Table external_table;
  external_table.dbName = "db";
  external_table.tableName = "b";
  ASSERT_OK(hms_client_->CreateTable(external_table));

  // Attempt to rename the Kudu table to the external table name.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer("db.a"));
  Status s = table_alterer->RenameTo("db.b")->Alter();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "b already exists");

  // Attempt to rename the Kudu table to an invalid database/table name pair.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("foo")->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Kudu table names must be a period ('.') separated "
                                    "database and table name pair");

  // Attempt to rename the Kudu table to a non-existent database.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("non_existent_database.table")->Alter();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  // TODO(HIVE-18852): match on the error message.

  // Attempt to rename the Kudu table to an invalid table name.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("db.☃")->Alter();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "☃ is not a valid object name");

  // Drop the HMS table entry and rename the table. This tests that the
  // HmsCatalog will create a new entry when necessary.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("db.a", &table));
  ASSERT_OK(hms_client_->DropTableWithContext("db", "a", hive::EnvironmentContext()));
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  ASSERT_OK(table_alterer->RenameTo("db.c")->Alter());
  NO_FATALS(CheckTable("db", "c"));
  NO_FATALS(CheckTableDoesNotExist("db", "a"));

  // Shutdown the HMS and try to rename the table.
  ASSERT_OK(StopHms());
  table_alterer.reset(client_->NewTableAlterer("db.c")->RenameTo("db.a"));
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS and rename the table back to the original name.  This is the happy path.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(table_alterer->Alter());
  });
  NO_FATALS(CheckTable("db", "a"));
  NO_FATALS(CheckTableDoesNotExist("db", "c"));

  // Drop the HMS table entry, then create a non-Kudu table entry in it's place,
  // and attempt to rename the table.
  ASSERT_OK(hms_client_->DropTableWithContext("db", "a", hive::EnvironmentContext()));
  hive::Table external_table_2;
  external_table_2.dbName = "db";
  external_table_2.tableName = "a";
  ASSERT_OK(hms_client_->CreateTable(external_table_2));
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  ASSERT_OK(table_alterer->RenameTo("db.c")->Alter());
  NO_FATALS(CheckTable("db", "c"));

  // Check that all three tables still exist.
  vector<string> tables;
  ASSERT_OK(hms_client_->GetAllTables("db", &tables));
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(tables, vector<string>({ "a", "b", "c" })) << tables;
}

TEST_F(MasterHmsTest, TestAlterTable) {
  const char* hms_database_name = "alter_db";
  const char* hms_table_name = "table";
  string table_name = Substitute("$0.$1", hms_database_name, hms_table_name);

  ASSERT_OK(CreateDatabase(hms_database_name));

  // Create the Kudu table.
  ASSERT_OK(CreateKuduTable(hms_database_name, hms_table_name));
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));

  // Alter the HMS table entry in a destructive way (remove the columns).
  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable(hms_database_name, hms_table_name, &hms_table));
  hms_table.sd.cols.clear();
  ASSERT_OK(hms_client_->AlterTable(hms_database_name, hms_table_name, hms_table));
  hive::Table altered_table;
  ASSERT_OK(hms_client_->GetTable(hms_database_name, hms_table_name, &altered_table));
  ASSERT_TRUE(altered_table.sd.cols.empty());

  // Drop a column. This should correct the entire set of columns in the HMS.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_name));
  ASSERT_OK(table_alterer->DropColumn("int8_val")->Alter());
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));

  // Shutdown the HMS and try to alter the table.
  ASSERT_OK(StopHms());
  table_alterer.reset(client_->NewTableAlterer(table_name)->DropColumn("int16_val"));
  Status s = table_alterer->Alter();
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS and try again.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(table_alterer->Alter());
  });
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));

  // Drop the table from the HMS, and insert a non-Kudu table entry, then try
  // and alter the table.
  ASSERT_OK(hms_client_->DropTableWithContext(hms_database_name, hms_table_name,
                                              hive::EnvironmentContext()));
  hms_table = hive::Table();
  hms_table.dbName = hms_database_name;
  hms_table.tableName = hms_table_name;
  ASSERT_OK(hms_client_->CreateTable(hms_table));

  table_alterer.reset(client_->NewTableAlterer(table_name));
  s = table_alterer->DropColumn("int32_val")->Alter();
  EXPECT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "already exists in the HMS");
}

TEST_F(MasterHmsTest, TestDeleteTable) {
  const char* hms_database_name = "delete_db";
  const char* hms_table_name = "table";
  string table_name = Substitute("$0.$1", hms_database_name, hms_table_name);

  ASSERT_OK(CreateDatabase(hms_database_name));

  // Create the Kudu table, then drop it and ensure the HMS entry is removed.
  ASSERT_OK(CreateKuduTable(hms_database_name, hms_table_name));
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));
  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable(hms_database_name, hms_table_name, &hms_table));
  ASSERT_OK(client_->DeleteTable(table_name));
  NO_FATALS(CheckTableDoesNotExist(hms_database_name, hms_table_name));

  // Create the Kudu table, remove the HMS entry, and ensure the Kudu table can
  // still be dropped.
  ASSERT_OK(CreateKuduTable(hms_database_name, hms_table_name));
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));
  ASSERT_OK(hms_client_->DropTableWithContext(hms_database_name, hms_table_name,
                                              hive::EnvironmentContext()));
  Status s = hms_client_->GetTable(hms_database_name, hms_table_name, &hms_table);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(client_->DeleteTable(table_name));
  NO_FATALS(CheckTableDoesNotExist(hms_database_name, hms_table_name));

  // Ensure that dropping a table while the HMS is unreachable fails.
  ASSERT_OK(CreateKuduTable(hms_database_name, hms_table_name));
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));
  ASSERT_OK(StopHms());
  s = client_->DeleteTable(table_name);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  ASSERT_OK(StartHms());
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(client_->DeleteTable(table_name));
  });
  NO_FATALS(CheckTableDoesNotExist(hms_database_name, hms_table_name));
}
} // namespace kudu
