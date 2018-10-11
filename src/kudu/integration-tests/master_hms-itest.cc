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
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/table_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/user.h"

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
    opts.hms_mode = GetHmsMode();
    opts.num_masters = 1;
    opts.num_tablet_servers = 1;
    opts.enable_kerberos = EnableKerberos();
    // Tune down the notification log poll period in order to speed up catalog convergence.
    opts.extra_master_flags.emplace_back("--hive_metastore_notification_log_poll_period_seconds=1");
    StartClusterWithOpts(std::move(opts));

    thrift::ClientOptions hms_opts;
    hms_opts.enable_kerberos = EnableKerberos();
    hms_opts.service_principal = "hive";
    hms_client_.reset(new HmsClient(cluster_->hms()->address(), hms_opts));
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

  // Rename a table entry in the HMS catalog.
  Status RenameHmsTable(const string& database_name,
                        const string& old_table_name,
                        const string& new_table_name) {
    // The HMS doesn't have a rename table API. Instead it offers the more
    // general AlterTable API, which requires the entire set of table fields to be
    // set. Since we don't know these fields during a simple rename operation, we
    // have to look them up.
    hive::Table table;
    RETURN_NOT_OK(hms_client_->GetTable(database_name, old_table_name, &table));
    table.tableName = new_table_name;
    return hms_client_->AlterTable(database_name, old_table_name, table);
  }

  // Drop all columns from a Kudu HMS table entry.
  Status AlterHmsTableDropColumns(const string& database_name, const string& table_name) {
    hive::Table table;
    RETURN_NOT_OK(hms_client_->GetTable(database_name, table_name, &table));
    table.sd.cols.clear();

    // The KuduMetastorePlugin only allows the master to alter the columns in a
    // Kudu table, so we pretend to be the master.
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });
    RETURN_NOT_OK(hms_client_->AlterTable(database_name, table_name, table, env_ctx));
    return Status::OK();
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

    string username;
    ASSERT_OK(GetLoggedInUser(&username));

    ASSERT_EQ(hms_table.owner, username);
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

  static hive::EnvironmentContext MasterEnvCtx() {
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });
    return env_ctx;
  }

 protected:

  unique_ptr<HmsClient> hms_client_;

 private:

  virtual HmsMode GetHmsMode() {
    return HmsMode::ENABLE_METASTORE_INTEGRATION;
  }

  virtual bool EnableKerberos() {
    return false;
  }
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
  ASSERT_STR_CONTAINS(s.ToString(), "create_db.☃");

  // Drop the HMS entry and create the table through Kudu.
  ASSERT_OK(hms_client_->DropTable(hms_database_name, hms_table_name));
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

  // Attempt to rename the Kudu table to the same name.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer("db.a"));
  Status s = table_alterer->RenameTo("db.a")->Alter();
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "a already exists");

  // Attempt to rename the Kudu table to the external table name.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("db.b")->Alter();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "b already exists");

  // Attempt to rename the Kudu table to an invalid database/table name pair.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("foo")->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), kInvalidTableError);

  // Attempt to rename the Kudu table to a non-existent database.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("non_existent_database.table")->Alter();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  // TODO(HIVE-18852): match on the error message.

  // Attempt to rename the Kudu table to an invalid table name.
  table_alterer.reset(client_->NewTableAlterer("db.a"));
  s = table_alterer->RenameTo("db.☃")->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "db.☃");

  // Shutdown the HMS and try to rename the table.
  ASSERT_OK(StopHms());
  table_alterer.reset(client_->NewTableAlterer("db.a")->RenameTo("db.c"));
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS and rename the table through Kudu.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(table_alterer->Alter());
  });
  NO_FATALS(CheckTable("db", "c"));
  NO_FATALS(CheckTableDoesNotExist("db", "a"));

  // Rename the table through the HMS, and ensure the rename is handled in Kudu.
  ASSERT_OK(RenameHmsTable("db", "c", "d"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTable("db", "d"));
  });

  // Check that the two tables still exist.
  vector<string> tables;
  ASSERT_OK(hms_client_->GetTableNames("db", &tables));
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(tables, vector<string>({ "b", "d" })) << tables;

  // Regression test for HIVE-19569
  // If HIVE-19569 is in effect the rename across databases will result in DROP
  // TABLE and CREATE TABLE events, which will cause the notification log
  // listener to drop the table. The alter will succeed (see KUDU-2475), but the
  // subsequent checks will fail, since the table will be deleted.
  ASSERT_OK(CreateDatabase("db1"));
  ASSERT_OK(CreateDatabase("db2"));
  ASSERT_OK(CreateKuduTable("db1", "t1"));
  NO_FATALS(CheckTable("db1", "t1"));
  table_alterer.reset(client_->NewTableAlterer("db1.t1"));
  ASSERT_OK(table_alterer->RenameTo("db2.t2")->Alter());
  NO_FATALS(CheckTable("db2", "t2"));
  NO_FATALS(CheckTableDoesNotExist("db1", "t1"));
  NO_FATALS(CheckTableDoesNotExist("db1", "t2"));
}

TEST_F(MasterHmsTest, TestAlterTable) {
  // Create the Kudu table.
  ASSERT_OK(CreateKuduTable("default", "a"));
  NO_FATALS(CheckTable("default", "a"));

  // Alter the HMS table entry in a destructive way (remove all columns).
  ASSERT_OK(AlterHmsTableDropColumns("default", "a"));

  // Drop a column in Kudu. This should correct the entire set of columns in the HMS.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer("default.a"));
  ASSERT_OK(table_alterer->DropColumn("int8_val")->Alter());
  NO_FATALS(CheckTable("default", "a"));

  // Shutdown the HMS and try to alter the table.
  ASSERT_OK(StopHms());
  table_alterer.reset(client_->NewTableAlterer("default.a")->DropColumn("int16_val"));
  Status s = table_alterer->Alter();
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS and try again.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(table_alterer->Alter());
  });
  NO_FATALS(CheckTable("default", "a"));

  // Only alter the table in Kudu, the corresponding table in the HMS will not be altered.
  table_alterer.reset(client_->NewTableAlterer("default.a")->RenameTo("default.b")
                             ->modify_external_catalogs(false));
  ASSERT_OK(table_alterer->Alter());
  bool exists;
  ASSERT_OK(client_->TableExists("default.b", &exists));
  ASSERT_TRUE(exists);
  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable("default", "a", &hms_table));
}

TEST_F(MasterHmsTest, TestDeleteTable) {
  // Create a Kudu table, then drop it from Kudu and ensure the HMS entry is removed.
  ASSERT_OK(CreateKuduTable("default", "a"));
  NO_FATALS(CheckTable("default", "a"));
  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable("default", "a", &hms_table));

  ASSERT_OK(client_->DeleteTable("default.a"));
  NO_FATALS(CheckTableDoesNotExist("default", "a"));

  // Create the Kudu table, then drop it from the HMS, and ensure the Kudu table is deleted.
  ASSERT_OK(CreateKuduTable("default", "b"));
  NO_FATALS(CheckTable("default", "b"));
  hive::Table hms_table_b;
  ASSERT_OK(hms_client_->GetTable("default", "b", &hms_table_b));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("default.b", &table));
  ASSERT_OK(hms_client_->DropTable("default", "b"));
  ASSERT_EVENTUALLY([&] {
      NO_FATALS(CheckTableDoesNotExist("default", "b"));
  });

  // Ensure that dropping a table while the HMS is unreachable fails.
  ASSERT_OK(CreateKuduTable("default", "c"));
  NO_FATALS(CheckTable("default", "c"));
  ASSERT_OK(StopHms());
  Status s = client_->DeleteTable("default.c");
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  ASSERT_OK(StartHms());
  NO_FATALS(CheckTable("default", "c"));
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(client_->DeleteTable("default.c"));
  });
  NO_FATALS(CheckTableDoesNotExist("default", "c"));

  // Create a Kudu table, then only drop it from Kudu. Ensure the HMS
  // entry is not removed.
  ASSERT_OK(CreateKuduTable("default", "d"));
  NO_FATALS(CheckTable("default", "d"));
  hive::Table hms_table_d;
  ASSERT_OK(hms_client_->GetTable("default", "d", &hms_table_d));
  ASSERT_OK(client_->DeleteTableInCatalogs("default.d", false));
  s = client_->OpenTable(Substitute("$0.$1", "default", "d"), &table);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(hms_client_->GetTable("default", "d", &hms_table_d));
}

TEST_F(MasterHmsTest, TestNotificationLogListener) {
  // Create a Kudu table.
  ASSERT_OK(CreateKuduTable("default", "a"));
  NO_FATALS(CheckTable("default", "a"));

  // Rename the table in the HMS, and ensure that the notification log listener
  // detects the rename and updates the Kudu catalog accordingly.
  ASSERT_OK(RenameHmsTable("default", "a", "b"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS({
      CheckTable("default", "b");
      CheckTableDoesNotExist("default", "a");
    });
  });

  // Drop the table in the HMS, and ensure that the notification log listener
  // detects the drop and updates the Kudu catalog accordingly.
  ASSERT_OK(hms_client_->DropTable("default", "b"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTableDoesNotExist("default", "b"));
  });

  // Rename a table from A to B to A, and ensure that Kudu doesn't continue
  // applying notification log events in a self-perpetuating loop.
  ASSERT_OK(CreateKuduTable("default", "a"));
  unique_ptr<KuduTableAlterer> table_alterer;
  table_alterer.reset(client_->NewTableAlterer("default.a")->RenameTo("default.b"));
  ASSERT_OK(table_alterer->Alter());
  NO_FATALS(CheckTable("default", "b"));
  table_alterer.reset(client_->NewTableAlterer("default.b")->RenameTo("default.a"));
  ASSERT_OK(table_alterer->Alter());
  NO_FATALS(CheckTable("default", "a"));


  // Ensure that Kudu can rename a table just after it's been renamed through the HMS.
  RenameHmsTable("default", "a", "b");
  table_alterer.reset(client_->NewTableAlterer("default.b")->RenameTo("default.c"));
  ASSERT_OK(table_alterer->Alter());

  // Ensure that Kudu can drop a table just after it's been renamed through the HMS.
  RenameHmsTable("default", "c", "a");
  ASSERT_OK(client_->DeleteTable("default.a"));

  // Test concurrent drops from the HMS and Kudu.

  // Scenario 1: drop from the HMS first.
  ASSERT_OK(CreateKuduTable("default", "a"));
  ASSERT_OK(hms_client_->DropTable("default", "a"));
  Status s = client_->DeleteTable("default.a");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  NO_FATALS(CheckTableDoesNotExist("default", "a"));

  // Scenario 2: drop from Kudu first.
  ASSERT_OK(CreateKuduTable("default", "a"));
  ASSERT_OK(client_->DeleteTable("default.a"));
  s = hms_client_->DropTable("default", "a");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  NO_FATALS(CheckTableDoesNotExist("default", "a"));
}

TEST_F(MasterHmsTest, TestUppercaseIdentifiers) {
  ASSERT_OK(CreateKuduTable("default", "MyTable"));
  NO_FATALS(CheckTable("default", "MyTable"));
  NO_FATALS(CheckTable("default", "mytable"));
  NO_FATALS(CheckTable("default", "MYTABLE"));

  // Kudu table schema lookups should be case-insensitive.
  for (const auto& name : { "default.MyTable",
                            "default.mytable",
                            "DEFAULT.MYTABLE",
                            "default.mYtABLE" }) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(name, &table));
    // The client uses the requested name as the table object's name.
    ASSERT_EQ(name, table->name());
  }

  // Listing tables shows the normalized case.
  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(tables, vector<string>({ "default.mytable" }));

  // Rename the table to the same normalized name, but with a different case.
  unique_ptr<KuduTableAlterer> table_alterer;
  table_alterer.reset(client_->NewTableAlterer("default.mytable"));
  Status s = table_alterer->RenameTo("DEFAULT.MYTABLE")->Alter();
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();

  // Rename the table to something different.
  table_alterer.reset(client_->NewTableAlterer("DEFAULT.MYTABLE"));
  ASSERT_OK(table_alterer->RenameTo("default.T_1/1")->Alter());
  NO_FATALS(CheckTable("default", "T_1/1"));
  NO_FATALS(CheckTable("default", "t_1/1"));
  NO_FATALS(CheckTable("DEFAULT", "T_1/1"));

  // Rename the table through the HMS.
  ASSERT_OK(RenameHmsTable("default", "T_1/1", "AbC"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTable("default", "AbC"));
  });

  // Listing tables shows the normalized case.
  tables.clear();
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(tables, vector<string>({ "default.abc" }));

  // Drop the table.
  ASSERT_OK(client_->DeleteTable("DEFAULT.abc"));
  NO_FATALS(CheckTableDoesNotExist("default", "AbC"));
  NO_FATALS(CheckTableDoesNotExist("default", "abc"));
}

class MasterHmsUpgradeTest : public MasterHmsTest {
 public:
  HmsMode GetHmsMode() override {
    return HmsMode::ENABLE_HIVE_METASTORE;
  }
};

TEST_F(MasterHmsUpgradeTest, TestConflictingNormalizedNames) {
  ASSERT_OK(CreateKuduTable("default", "MyTable"));
  ASSERT_OK(CreateKuduTable("default", "mytable"));

  // Shutdown the masters and turn on the HMS integration. The masters should
  // fail to startup because of conflicting normalized table names.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->EnableMetastoreIntegration();
  // Typically, restarting the cluster will fail because the master will
  // immediately try to elect itself leader, and CHECK fail upon seeing the
  // conflicting table names. However, in TSAN or otherwise slow situations the
  // master may be able to register the tablet server before attempting to elect
  // itself leader, in which case ExternalMiniCluster::Restart() can succeed. In
  // this situation a fallback to a leader-only API will deterministically fail.
  Status s = cluster_->Restart();
  if (s.ok()) {
    vector<string> tables;
    s = client_->ListTables(&tables);
  }
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Disable the metastore integration and rename one of the conflicting tables.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->DisableMetastoreIntegration();
  ASSERT_OK(cluster_->Restart());
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer("default.mytable"));
  ASSERT_OK(table_alterer->RenameTo("default.mytable-renamed")->Alter());

  // Try again to enable the integration.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->EnableMetastoreIntegration();
  ASSERT_OK(cluster_->Restart());

  vector<string> tables;
  client_->ListTables(&tables);
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(tables, vector<string>({ "default.MyTable", "default.mytable-renamed" }));
}

// Checks that existing tables with HMS-incompatible names can be renamed post
// upgrade using a Kudu-catalog only alter.
TEST_F(MasterHmsUpgradeTest, TestRenameExistingTables) {
  ASSERT_OK(CreateKuduTable("default", "UPPERCASE"));
  ASSERT_OK(CreateKuduTable("default", "illegal-chars⁉"));

  // Shutdown the masters and turn on the HMS integration
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->EnableMetastoreIntegration();
  ASSERT_OK(cluster_->Restart());

  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(tables, vector<string>({ "default.UPPERCASE", "default.illegal-chars⁉" }));

  // Rename the tables using a Kudu catalog only rename.
  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer("default.UPPERCASE"));
  ASSERT_OK(alterer->RenameTo("default.uppercase")->modify_external_catalogs(false)->Alter());

  alterer.reset(client_->NewTableAlterer("default.illegal-chars⁉"));
  ASSERT_OK(alterer->RenameTo("default.illegal_chars")->modify_external_catalogs(false)->Alter());

  tables.clear();
  client_->ListTables(&tables);
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(tables, vector<string>({ "default.illegal_chars", "default.uppercase" }));
}

class MasterHmsKerberizedTest : public MasterHmsTest {
 public:
  bool EnableKerberos() override {
    return true;
  }
};

// Checks that table ownership in a Kerberized cluster is set to the user
// short-name (instead of the full Kerberos principal).
TEST_F(MasterHmsKerberizedTest, TestTableOwnership) {
  // Log in as the test user and reset the client to pick up the change in user.
  ASSERT_OK(cluster_->kdc()->Kinit("test-user"));
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  // Create a table as the user and ensure that the ownership is set correctly.
  ASSERT_OK(CreateKuduTable("default", "my_table"));
  hive::Table table;
  ASSERT_OK(hms_client_->GetTable("default", "my_table", &table));
  ASSERT_EQ("test-user", table.owner);
}
} // namespace kudu
