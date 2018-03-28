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

#include "kudu/hms/hms_catalog.h"

#include <map> // IWYU pragma: keep
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h" // IWYU pragma: keep
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/test/mini_kdc.h" // IWYU pragma: keep
#include "kudu/util/net/net_util.h" // IWYU pragma: keep
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_string(hive_metastore_uris);
DECLARE_bool(hive_metastore_sasl_enabled);

using kudu::rpc::SaslProtection;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace hms {

TEST(HmsCatalogStaticTest, TestParseTableName) {
  string db;
  string tbl;

  EXPECT_OK(HmsCatalog::ParseTableName("foo.bar", &db, &tbl));
  EXPECT_EQ("foo", db);
  EXPECT_EQ("bar", tbl);

  EXPECT_OK(HmsCatalog::ParseTableName("99bottles.my_awesome/table/22", &db, &tbl));
  EXPECT_EQ("99bottles", db);
  EXPECT_EQ("my_awesome/table/22", tbl);

  EXPECT_OK(HmsCatalog::ParseTableName("_leading_underscore.trailing_underscore_", &db, &tbl));
  EXPECT_EQ("_leading_underscore", db);
  EXPECT_EQ("trailing_underscore_", tbl);

  EXPECT_OK(HmsCatalog::ParseTableName("unicode ☃ tables?.maybe/one_day", &db, &tbl));
  EXPECT_EQ("unicode ☃ tables?", db);
  EXPECT_EQ("maybe/one_day", tbl);

  EXPECT_OK(HmsCatalog::ParseTableName(".", &db, &tbl));
  EXPECT_EQ("", db);
  EXPECT_EQ("", tbl);

  EXPECT_OK(HmsCatalog::ParseTableName(string("\0.\0", 3), &db, &tbl));
  EXPECT_EQ(string("\0", 1), db);
  EXPECT_EQ(string("\0", 1), tbl);

  EXPECT_TRUE(HmsCatalog::ParseTableName("no-table", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(HmsCatalog::ParseTableName("lots.of.tables", &db, &tbl).IsInvalidArgument());
}

TEST(HmsCatalogStaticTest, TestParseUris) {
  vector<HostPort> hostports;

  EXPECT_OK(HmsCatalog::ParseUris("", &hostports));
  EXPECT_TRUE(hostports.empty());

  EXPECT_OK(HmsCatalog::ParseUris(",,,", &hostports));
  EXPECT_TRUE(hostports.empty());

  EXPECT_OK(HmsCatalog::ParseUris("thrift://foo-bar-baz:1234", &hostports));
  EXPECT_EQ(hostports, vector<HostPort>({ HostPort("foo-bar-baz", 1234) }));

  EXPECT_OK(HmsCatalog::ParseUris("thrift://hms-1:1234,thrift://hms-2", &hostports));
  EXPECT_EQ(hostports, vector<HostPort>(
            { HostPort("hms-1", 1234), HostPort("hms-2", HmsClient::kDefaultHmsPort) }));

  Status s = HmsCatalog::ParseUris("://illegal-scheme:12", &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "missing scheme");

  s = HmsCatalog::ParseUris("missing-scheme", &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "missing scheme");

  s = HmsCatalog::ParseUris("thrift://foo,missing-scheme:1234,thrift://baz", &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "missing scheme");
}

// Base class for HmsCatalog tests. Parameterized by whether
// SASL/GSSAPI/Kerberos should be enabled.
class HmsCatalogTest : public ::testing::Test {
 public:

  const char* const kMasterAddrs = "master-addrs";
  const char* const kTableId = "abc123";

  virtual bool EnableKerberos() {
    return false;
  }

  void SetUp() override {
    bool enable_kerberos = EnableKerberos();

    HmsClientOptions hms_client_opts;

    hms_.reset(new hms::MiniHms());
    if (enable_kerberos) {
      kdc_.reset(new MiniKdc(MiniKdcOptions()));
      ASSERT_OK(kdc_->Start());

      // Create a service principal for the HMS, and configure it to use it.
      string spn = "hive/127.0.0.1";
      string ktpath;
      ASSERT_OK(kdc_->CreateServiceKeytab(spn, &ktpath));
      hms_->EnableKerberos(kdc_->GetEnvVars()["KRB5_CONFIG"],
                           spn,
                           ktpath,
                           SaslProtection::kPrivacy);

      // Create a principal for the HmsCatalog, and configure it to use it.
      ASSERT_OK(rpc::SaslInit());
      ASSERT_OK(kdc_->CreateUserPrincipal("alice"));
      ASSERT_OK(kdc_->Kinit("alice"));
      ASSERT_OK(kdc_->SetKrb5Environment());
      hms_client_opts.enable_kerberos = true;

      // Configure the HmsCatalog flags.
      FLAGS_hive_metastore_sasl_enabled = true;
    }

    ASSERT_OK(hms_->Start());

    hms_client_.reset(new HmsClient(hms_->address(), hms_client_opts));
    ASSERT_OK(hms_client_->Start());

    FLAGS_hive_metastore_uris = hms_->uris();
    hms_catalog_.reset(new HmsCatalog(kMasterAddrs));
    ASSERT_OK(hms_catalog_->Start());
  }

  void TearDown() override {
    ASSERT_OK(hms_->Stop());
    ASSERT_OK(hms_client_->Stop());
  }

  Status StopHms() {
    RETURN_NOT_OK(hms_client_->Stop());
    RETURN_NOT_OK(hms_->Stop());
    return Status::OK();
  }

  Status StartHms() {
    RETURN_NOT_OK(hms_->Start());
    RETURN_NOT_OK(hms_client_->Start());
    return Status::OK();
  }

  Schema AllTypesSchema() {
    SchemaBuilder b;
    b.AddKeyColumn("key", DataType::INT32);
    b.AddColumn("int8_val", DataType::INT8);
    b.AddColumn("int16_val", DataType::INT16);
    b.AddColumn("int32_val", DataType::INT32);
    b.AddColumn("int64_val", DataType::INT64);
    b.AddColumn("timestamp_val", DataType::UNIXTIME_MICROS);
    b.AddColumn("string_val", DataType::STRING);
    b.AddColumn("bool_val", DataType::BOOL);
    b.AddColumn("float_val", DataType::FLOAT);
    b.AddColumn("double_val", DataType::DOUBLE);
    b.AddColumn("binary_val", DataType::BINARY);
    b.AddColumn("decimal32_val", DataType::DECIMAL32);
    b.AddColumn("decimal64_val", DataType::DECIMAL64);
    b.AddColumn("decimal128_val", DataType::DECIMAL128);
    return b.Build();
  }

  void CheckTable(const string& database_name,
                  const string& table_name,
                  const string& table_id,
                  const Schema& schema) {
    hive::Table table;
    ASSERT_OK(hms_client_->GetTable(database_name, table_name, &table));

    EXPECT_EQ(table.parameters[HmsClient::kKuduTableIdKey], table_id);
    EXPECT_EQ(table.parameters[HmsClient::kKuduMasterAddrsKey], kMasterAddrs);
    EXPECT_EQ(table.parameters[hive::g_hive_metastore_constants.META_TABLE_STORAGE],
              HmsClient::kKuduStorageHandler);

    for (int column_idx = 0; column_idx < schema.num_columns(); column_idx++) {
      EXPECT_EQ(table.sd.cols[column_idx].name, schema.columns()[column_idx].name());
    }
  }

  void CheckTableDoesNotExist(const string& database_name, const string& table_name) {
    hive::Table table;
    Status s = hms_client_->GetTable(database_name, table_name, &table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

 protected:
  unique_ptr<MiniKdc> kdc_;
  unique_ptr<MiniHms> hms_;
  unique_ptr<HmsClient> hms_client_;
  unique_ptr<HmsCatalog> hms_catalog_;
};

// Subclass of HmsCatalogTest that allows running individual test cases with
// Kerberos enabled and disabled. Most of the test cases are run only with
// Kerberos disabled, but to get coverage against a Kerberized HMS we run select
// cases in both modes.
class HmsCatalogTestParameterized : public HmsCatalogTest,
                                    public ::testing::WithParamInterface<bool> {
  bool EnableKerberos() override {
    return GetParam();
  }
};
INSTANTIATE_TEST_CASE_P(HmsCatalogTests, HmsCatalogTestParameterized,
                        ::testing::Values(false, true));

// Test creating, altering, and dropping a table with the HMS Catalog.
TEST_P(HmsCatalogTestParameterized, TestTableLifecycle) {
  const string kTableId = "table-id";
  const string kHmsDatabase = "default";
  const string kHmsTableName = "table_name";
  const string kTableName = Substitute("$0.$1", kHmsDatabase, kHmsTableName);
  const string kHmsAlteredTableName = "altered_table_name";
  const string kAlteredTableName = Substitute("$0.$1", kHmsDatabase, kHmsAlteredTableName);

  Schema schema = AllTypesSchema();

  // Create the table.
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, kTableName, schema));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsTableName, kTableId, schema));

  // Alter the table.
  SchemaBuilder b(schema);
  b.AddColumn("new_column", DataType::INT32);
  Schema altered_schema = b.Build();
  ASSERT_OK(hms_catalog_->AlterTable(kTableId, kTableName, kAlteredTableName, altered_schema));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsTableName));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsAlteredTableName, kTableId, altered_schema));

  // Drop the table.
  ASSERT_OK(hms_catalog_->DropTable(kTableId, kAlteredTableName));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsTableName));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsAlteredTableName));
}

// Checks that 'legacy' Kudu tables can be altered and dropped by the
// HmsCatalog. Altering a legacy table to be HMS compliant should result in a
// valid HMS table entry being created. Dropping a legacy table should do
// nothing, but return success.
TEST_F(HmsCatalogTest, TestLegacyTables) {
  const string kTableId = "table-id";
  const string kHmsDatabase = "default";

  Schema schema = AllTypesSchema();
  hive::Table table;

  // Alter a table containing a non Hive-compatible character, and ensure an
  // entry is created with the new (valid) name.
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "a"));
  ASSERT_OK(hms_catalog_->AlterTable(kTableId, "default.☃", "default.a", schema));
  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, schema));

  // Alter a table without a database and ensure an entry is created with the new (valid) name.
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "b"));
  ASSERT_OK(hms_catalog_->AlterTable(kTableId, "no_database", "default.b", schema));
  NO_FATALS(CheckTable(kHmsDatabase, "b", kTableId, schema));

  // Drop a table containing a Hive incompatible character, and ensure it doesn't fail.
  ASSERT_OK(hms_catalog_->DropTable(kTableId, "foo.☃"));

  // Drop a table without a database, and ensure it doesn't fail.
  ASSERT_OK(hms_catalog_->DropTable(kTableId, "no_database"));
}

// Checks that Kudu tables will not replace or modify existing HMS entries that
// belong to external tables from other systems.
TEST_F(HmsCatalogTest, TestExternalTable) {
  const string kTableId = "table-id";
  const string kHmsDatabase = "default";

  const string kHmsExternalTable = "external_table";
  const string kExternalTableName = Substitute("$0.$1", kHmsDatabase, kHmsExternalTable);

  const string kHmsKuduTable = "kudu_table";
  const string kKuduTableName = Substitute("$0.$1", kHmsDatabase, kHmsKuduTable);

  // Create the external table.
  hive::Table external_table;
  external_table.dbName = kHmsDatabase;
  external_table.tableName = kHmsExternalTable;
  external_table.tableType = HmsClient::kManagedTable;
  ASSERT_OK(hms_client_->CreateTable(external_table));
  // Retrieve the full HMS table object so it can be compared later (the HMS
  // fills in some fields during creation).
  ASSERT_OK(hms_client_->GetTable(kHmsDatabase, kHmsExternalTable, &external_table));

  auto CheckExternalTable = [&] {
    hive::Table current_table;
    ASSERT_OK(hms_client_->GetTable(kHmsDatabase, kHmsExternalTable, &current_table));
    ASSERT_EQ(current_table, external_table);
  };

  // Create the Kudu table.
  Schema schema = AllTypesSchema();
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, kKuduTableName, schema));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsKuduTable, kTableId, schema));

  // Try and create a Kudu table with the same name as the external table.
  Status s = hms_catalog_->CreateTable(kTableId, kKuduTableName, schema);
  EXPECT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckExternalTable());

  // Try and rename the Kudu table to the external table name.
  s = hms_catalog_->AlterTable(kTableId, kKuduTableName, kExternalTableName, schema);
  EXPECT_TRUE(s.IsIllegalState()) << s.ToString();
  NO_FATALS(CheckExternalTable());
  NO_FATALS(CheckTable(kHmsDatabase, kHmsKuduTable, kTableId, schema));

  // Try and rename a Kudu table from the external table name to a new name.
  // This depends on the Kudu table not actually existing in the HMS catalog.
  const string kHmsRenamedTable = "renamed_table";
  const string kRenamedTableName = Substitute("$0.$1", kHmsDatabase, kHmsRenamedTable);
  ASSERT_OK(hms_catalog_->AlterTable(kTableId, kExternalTableName, kRenamedTableName, schema));
  NO_FATALS(CheckExternalTable());
  // The 'renamed' table is really just created with the new name.
  NO_FATALS(CheckTable(kHmsDatabase, kHmsRenamedTable, kTableId, schema));

  // Try and alter a Kudu table with the same name as the external table.
  // This depends on the Kudu table not actually existing in the HMS catalog.
  s = hms_catalog_->AlterTable(kTableId, kExternalTableName, kExternalTableName, schema);
  EXPECT_TRUE(s.IsIllegalState()) << s.ToString();
  NO_FATALS(CheckExternalTable());

  // Try and drop the external table as if it were a Kudu table.  This should
  // return an OK status, but not actually modify the external table.
  ASSERT_OK(hms_catalog_->DropTable(kTableId, kExternalTableName));
  NO_FATALS(CheckExternalTable());

  // Drop a Kudu table with no corresponding HMS entry.
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "bogus_table_name"));
  ASSERT_OK(hms_catalog_->DropTable(kTableId, "default.bogus_table_name"));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "bogus_table_name"));
}

// Checks that the HmsCatalog handles reconnecting to the metastore after a connection failure.
TEST_F(HmsCatalogTest, TestReconnect) {
  // TODO(dan): Figure out a way to test failover among HA HMS instances. The
  // MiniHms does not support HA, since it relies on a single-process Derby database.

  const string kTableId = "table-id";
  const string kHmsDatabase = "default";
  Schema schema = AllTypesSchema();
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.a", schema));
  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, schema));

  // Shutdown the HMS and try a few operations.
  ASSERT_OK(StopHms());

  Status s = hms_catalog_->CreateTable(kTableId, "default.b", schema);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  s = hms_catalog_->AlterTable(kTableId, "default.a", "default.c", schema);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS back up and ensure that the same operations succeed.
  ASSERT_OK(StartHms());
  EXPECT_OK(hms_catalog_->CreateTable(kTableId, "default.d", schema));
  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, schema));
  NO_FATALS(CheckTable(kHmsDatabase, "d", kTableId, schema));

  EXPECT_OK(hms_catalog_->AlterTable(kTableId, "default.a", "default.c", schema));
  NO_FATALS(CheckTable(kHmsDatabase, "c", kTableId, schema));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "a"));
}

} // namespace hms
} // namespace kudu
