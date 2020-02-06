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
#include <initializer_list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
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
#include "kudu/integration-tests/hms_itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::none;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::hms::HmsClient;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

// Test Master <-> HMS catalog synchronization.
class MasterHmsTest : public HmsITestBase {
 public:

  void SetUp() override {
    HmsITestBase::SetUp();

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
    if (hms_client_) {
      ASSERT_OK(hms_client_->Stop());
    }
    HmsITestBase::TearDown();
  }

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
  ASSERT_OK(CreateHmsTable(hms_database_name, hms_table_name));

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
  NO_FATALS(CheckTable(hms_database_name, hms_table_name, /*user=*/none));

  // Create an external table in the HMS and validate it's not created Kudu side.
  const char* external_table_name = "externalTable";
  ASSERT_OK(CreateHmsTable(hms_database_name, external_table_name, HmsClient::kExternalTable));
  shared_ptr<KuduTable> table;
  s = client_->OpenTable(Substitute("$0.$1", hms_database_name, external_table_name), &table);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

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
  NO_FATALS(CheckTable(hms_database_name, "foo", /*user=*/none));
}

TEST_F(MasterHmsTest, TestRenameTable) {
  // Create the database and Kudu table.
  ASSERT_OK(CreateDatabase("db"));
  ASSERT_OK(CreateKuduTable("db", "a"));
  NO_FATALS(CheckTable("db", "a", /*user=*/none));

  // Create a non-Kudu ('external') HMS table entry.
  ASSERT_OK(CreateHmsTable("db", "b", HmsClient::kExternalTable));

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
  ASSERT_STR_CONTAINS(s.ToString(), kInvalidHiveTableError);

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
  NO_FATALS(CheckTable("db", "c", /*user=*/ none));
  NO_FATALS(CheckTableDoesNotExist("db", "a"));

  // Rename the table through the HMS, and ensure the rename is handled in Kudu.
  ASSERT_OK(RenameHmsTable("db", "c", "d"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTable("db", "d", /*user=*/ none));
  });

  // Check that the two tables still exist.
  vector<string> tables;
  ASSERT_OK(hms_client_->GetTableNames("db", &tables));
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(tables, vector<string>({ "b", "d" })) << tables;

  // Rename the external table through the HMS and ensure Kudu allows it.
  ASSERT_OK(RenameHmsTable("db", "b", "e"));

  // Regression test for HIVE-19569
  // If HIVE-19569 is in effect the rename across databases will result in DROP
  // TABLE and CREATE TABLE events, which will cause the notification log
  // listener to drop the table. The alter will succeed (see KUDU-2475), but the
  // subsequent checks will fail, since the table will be deleted.
  ASSERT_OK(CreateDatabase("db1"));
  ASSERT_OK(CreateDatabase("db2"));
  ASSERT_OK(CreateKuduTable("db1", "t1"));
  NO_FATALS(CheckTable("db1", "t1", /*user=*/ none));
  table_alterer.reset(client_->NewTableAlterer("db1.t1"));
  ASSERT_OK(table_alterer->RenameTo("db2.t2")->Alter());
  NO_FATALS(CheckTable("db2", "t2", /*user=*/ none));
  NO_FATALS(CheckTableDoesNotExist("db1", "t1"));
  NO_FATALS(CheckTableDoesNotExist("db1", "t2"));
}

TEST_F(MasterHmsTest, TestAlterTable) {
  // Create the Kudu table.
  ASSERT_OK(CreateKuduTable("default", "a"));
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));

  // Alter the HMS table entry in a destructive way (remove all columns).
  ASSERT_OK(AlterHmsTableDropColumns("default", "a"));

  // Drop a column in Kudu. This should correct the entire set of columns in the HMS.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer("default.a"));
  ASSERT_OK(table_alterer->DropColumn("int8_val")->Alter());
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));

  // Alter a column comment in Kudu. This should be reflected in the HMS.
  unique_ptr<KuduTableAlterer> comment_alterer(client_->NewTableAlterer("default.a"));
  comment_alterer->AlterColumn("key")->Comment("");
  comment_alterer->AlterColumn("int16_val")->Comment("A new comment");
  ASSERT_OK(comment_alterer->Alter());
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));

  // Create the Kudu table and alter the HMS entry to be an external table
  // with `external.table.purge = true`, then alter it in the HMS and ensure
  // the Kudu table is actually altered.
  ASSERT_OK(CreateKuduTable("default", "b"));
  ASSERT_OK(AlterHmsTableExternalPurge("default", "b"));
  NO_FATALS(CheckTable("default", "b", /*user=*/ none, hms::HmsClient::kExternalTable));
  unique_ptr<KuduTableAlterer> comment_alterer_b(client_->NewTableAlterer("default.b"));
  comment_alterer_b->AlterColumn("int16_val")->Comment("Another comment");
  ASSERT_OK(comment_alterer_b->Alter());
  NO_FATALS(CheckTable("default", "b", /*user=*/ none, hms::HmsClient::kExternalTable));

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
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));

  // Only alter the table in Kudu, the corresponding table in the HMS will not be altered.
  table_alterer.reset(client_->NewTableAlterer("default.a")->RenameTo("default.c")
                             ->modify_external_catalogs(false));
  ASSERT_OK(table_alterer->Alter());
  bool exists;
  ASSERT_OK(client_->TableExists("default.c", &exists));
  ASSERT_TRUE(exists);
  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable("default", "a", &hms_table));
}

TEST_F(MasterHmsTest, TestDeleteTable) {
  // Create a Kudu table, then drop it from Kudu and ensure the HMS entry is removed.
  ASSERT_OK(CreateKuduTable("default", "a"));
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));
  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable("default", "a", &hms_table));

  ASSERT_OK(client_->DeleteTable("default.a"));
  NO_FATALS(CheckTableDoesNotExist("default", "a"));

  // Create the Kudu table, then drop it from the HMS, and ensure the Kudu table is deleted.
  ASSERT_OK(CreateKuduTable("default", "b"));
  NO_FATALS(CheckTable("default", "b", /*user=*/ none));
  hive::Table hms_table_b;
  ASSERT_OK(hms_client_->GetTable("default", "b", &hms_table_b));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("default.b", &table));
  ASSERT_OK(hms_client_->DropTable("default", "b"));
  ASSERT_EVENTUALLY([&] {
      NO_FATALS(CheckTableDoesNotExist("default", "b"));
  });

  // Create the Kudu table and alter the HMS entry to be an external table
  // with `external.table.purge = true`, then drop it from the HMS, and ensure
  // the Kudu table is deleted.
  ASSERT_OK(CreateKuduTable("default", "b"));
  ASSERT_OK(AlterHmsTableExternalPurge("default", "b"));
  NO_FATALS(CheckTable("default", "b", /*user=*/ none, hms::HmsClient::kExternalTable));
  shared_ptr<KuduTable> table2;
  ASSERT_OK(client_->OpenTable("default.b", &table2));
  ASSERT_OK(hms_client_->DropTable("default", "b"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTableDoesNotExist("default", "b"));
  });

  // Ensure that dropping a table while the HMS is unreachable fails.
  ASSERT_OK(CreateKuduTable("default", "c"));
  NO_FATALS(CheckTable("default", "c", /*user=*/ none));
  ASSERT_OK(StopHms());
  Status s = client_->DeleteTable("default.c");
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  ASSERT_OK(StartHms());
  NO_FATALS(CheckTable("default", "c", /*user=*/ none));
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(client_->DeleteTable("default.c"));
  });
  NO_FATALS(CheckTableDoesNotExist("default", "c"));

  // Create a Kudu table, then only drop it from Kudu. Ensure the HMS
  // entry is not removed.
  ASSERT_OK(CreateKuduTable("default", "d"));
  NO_FATALS(CheckTable("default", "d", /*user=*/ none));
  hive::Table hms_table_d;
  ASSERT_OK(hms_client_->GetTable("default", "d", &hms_table_d));
  ASSERT_OK(client_->DeleteTableInCatalogs("default.d", false));
  s = client_->OpenTable(Substitute("$0.$1", "default", "d"), &table);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(hms_client_->GetTable("default", "d", &hms_table_d));

  // Create and drop a non-Kudu ('external') HMS table entry and ensure Kudu allows it.
  ASSERT_OK(CreateHmsTable("default", "externalTable", HmsClient::kExternalTable));
  ASSERT_OK(hms_client_->DropTable("default", "externalTable"));
}

TEST_F(MasterHmsTest, TestNotificationLogListener) {
  // Create a Kudu table.
  ASSERT_OK(CreateKuduTable("default", "a"));
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));

  // Rename the table in the HMS, and ensure that the notification log listener
  // detects the rename and updates the Kudu catalog accordingly.
  ASSERT_OK(RenameHmsTable("default", "a", "b"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS({
      CheckTable("default", "b", /*user=*/ none);
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
  NO_FATALS(CheckTable("default", "b", /*user=*/ none));
  table_alterer.reset(client_->NewTableAlterer("default.b")->RenameTo("default.a"));
  ASSERT_OK(table_alterer->Alter());
  NO_FATALS(CheckTable("default", "a", /*user=*/ none));


  // Ensure that Kudu can rename a table just after it's been renamed through the HMS.
  ASSERT_OK(RenameHmsTable("default", "a", "b"));
  table_alterer.reset(client_->NewTableAlterer("default.b")->RenameTo("default.c"));
  ASSERT_OK(table_alterer->Alter());

  // Ensure that Kudu can drop a table just after it's been renamed through the HMS.
  ASSERT_OK(RenameHmsTable("default", "c", "a"));
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

  // Create the Kudu table and alter the HMS entry to be an external table
  // with `external.table.purge = true`.
  ASSERT_OK(CreateKuduTable("default", "d"));
  ASSERT_OK(AlterHmsTableExternalPurge("default", "d"));
  NO_FATALS(CheckTable("default", "d", /*user=*/ none, hms::HmsClient::kExternalTable));

  // Rename the table in the HMS, and ensure that the notification log listener
  // detects the rename and updates the Kudu catalog accordingly.
  ASSERT_OK(RenameHmsTable("default", "d", "e"));
  ASSERT_EVENTUALLY([&] {
  NO_FATALS({
      CheckTable("default", "e", /*user=*/ none, hms::HmsClient::kExternalTable);
      CheckTableDoesNotExist("default", "d");
    });
  });

  // Drop the table in the HMS, and ensure that the notification log listener
  // detects the drop and updates the Kudu catalog accordingly.
  ASSERT_OK(hms_client_->DropTable("default", "e"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTableDoesNotExist("default", "e"));
  });
}

TEST_F(MasterHmsTest, TestIgnoreExternalTables) {
  // Set up a managed table, and a couple of external tables to point at it.
  ASSERT_OK(CreateKuduTable("default", "managed"));
  const string kManagedTableName = "default.managed";
  ASSERT_OK(CreateHmsTable("default", "ext1", HmsClient::kExternalTable, kManagedTableName));
  ASSERT_OK(CreateHmsTable("default", "ext2", HmsClient::kExternalTable, kManagedTableName));

  // Drop a table in the HMS and check that it didn't affect the underlying
  // Kudu table.
  ASSERT_OK(hms_client_->DropTable("default", "ext1"));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kManagedTableName, &table));

  // Do the same, but rename the HMS table.
  hive::Table ext;
  ASSERT_OK(hms_client_->GetTable("default", "ext2", &ext));
  ext.tableName = "ext3";
  ASSERT_OK(hms_client_->AlterTable("default", "ext2", ext));
  ASSERT_OK(client_->OpenTable(kManagedTableName, &table));
  Status s = hms_client_->GetTable("default", "ext2", &ext);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Alter the table in Kudu, the external tables in the HMS will not be
  // altered.
  unique_ptr<KuduTableAlterer> table_alterer(
      client_->NewTableAlterer(kManagedTableName)->RenameTo("default.other"));
  ASSERT_OK(table_alterer->Alter());
  ASSERT_OK(hms_client_->GetTable("default", "ext3", &ext));
  ASSERT_EQ(kManagedTableName, ext.parameters[HmsClient::kKuduTableNameKey]);
}

TEST_F(MasterHmsTest, TestUppercaseIdentifiers) {
  ASSERT_OK(CreateKuduTable("default", "MyTable"));
  NO_FATALS(CheckTable("default", "MyTable", /*user=*/none));
  NO_FATALS(CheckTable("default", "mytable", /*user=*/none));
  NO_FATALS(CheckTable("default", "MYTABLE", /*user=*/none));

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
  NO_FATALS(CheckTable("default", "T_1/1", /*user=*/none));
  NO_FATALS(CheckTable("default", "t_1/1", /*user=*/none));
  NO_FATALS(CheckTable("DEFAULT", "T_1/1", /*user=*/none));

  // Rename the table through the HMS.
  ASSERT_OK(RenameHmsTable("default", "T_1/1", "AbC"));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckTable("default", "AbC", /*user=*/none));
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
