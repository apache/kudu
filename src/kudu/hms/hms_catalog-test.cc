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

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h" // IWYU pragma: keep
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(hive_metastore_uris);
DECLARE_bool(hive_metastore_sasl_enabled);

using kudu::rpc::SaslProtection;
using std::make_pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace hms {

TEST(HmsCatalogStaticTest, TestNormalizeTableName) {
  string table = "foo.bar";
  ASSERT_OK(HmsCatalog::NormalizeTableName(&table));
  ASSERT_EQ("foo.bar", table);

  table = "fOo.BaR";
  ASSERT_OK(HmsCatalog::NormalizeTableName(&table));
  EXPECT_EQ("foo.bar", table);

  table = "A.B";
  ASSERT_OK(HmsCatalog::NormalizeTableName(&table));
  EXPECT_EQ("a.b", table);

  table = "__/A__.buzz";
  ASSERT_OK(HmsCatalog::NormalizeTableName(&table));
  EXPECT_EQ("__/a__.buzz", table);

  table = "THE/QUICK/BROWN/FOX/JUMPS/OVER/THE/LAZY/DOG."
          "the_quick_brown_fox_jumps_over_the_lazy_dog";
  ASSERT_OK(HmsCatalog::NormalizeTableName(&table));
  EXPECT_EQ("the/quick/brown/fox/jumps/over/the/lazy/dog."
            "the_quick_brown_fox_jumps_over_the_lazy_dog", table);

  table = "default.MyTable";
  ASSERT_OK(HmsCatalog::NormalizeTableName(&table));
  EXPECT_EQ("default.mytable", table);
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
class HmsCatalogTest : public KuduTest {
 public:

  const char* const kMasterAddrs = "master-addrs";
  const char* const kTableId = "abc123";

  virtual bool EnableKerberos() {
    return false;
  }

  void SetUp() override {
    bool enable_kerberos = EnableKerberos();

    thrift::ClientOptions hms_client_opts;

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
      hms_client_opts.service_principal = "hive";

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
    ASSERT_OK(hms_client_->Stop());
    ASSERT_OK(hms_->Stop());
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
                  const boost::optional<const string&>& owner,
                  const Schema& schema) {
    hive::Table table;
    ASSERT_OK(hms_client_->GetTable(database_name, table_name, &table));

    if (owner) {
      EXPECT_EQ(table.owner, *owner);
    } else {
      EXPECT_TRUE(table.owner.empty());
    }
    EXPECT_EQ(table.parameters[HmsClient::kKuduTableIdKey], table_id);
    EXPECT_EQ(table.parameters[HmsClient::kKuduMasterAddrsKey], kMasterAddrs);
    EXPECT_EQ(table.parameters[HmsClient::kStorageHandlerKey], HmsClient::kKuduStorageHandler);

    for (int column_idx = 0; column_idx < schema.num_columns(); column_idx++) {
      EXPECT_EQ(table.sd.cols[column_idx].name, schema.columns()[column_idx].name());
    }
  }

  Status CreateLegacyTable(const string& database_name,
                           const string& table_name,
                           const string& table_type) {
    hive::Table table;
    string kudu_table_name(table_name);
    table.dbName = database_name;
    table.tableName = table_name;
    table.tableType = table_type;
    if (table_type == HmsClient::kManagedTable) {
      kudu_table_name = Substitute("$0$1.$2", HmsClient::kLegacyTablePrefix,
                                   database_name, table_name);
    }

    table.__set_parameters({
        make_pair(HmsClient::kStorageHandlerKey,
                  HmsClient::kLegacyKuduStorageHandler),
        make_pair(HmsClient::kLegacyKuduTableNameKey,
                  kudu_table_name),
        make_pair(HmsClient::kKuduMasterAddrsKey,
                  kMasterAddrs),
    });

    // TODO(Hao): Remove this once HIVE-19253 is fixed.
    if (table_type == HmsClient::kExternalTable) {
      table.parameters[HmsClient::kExternalTableKey] = "TRUE";
    }

    return hms_client_->CreateTable(table);
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
  const string kOwner = "alice";

  Schema schema = AllTypesSchema();

  // Create the table.
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, kTableName, kOwner, schema));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsTableName, kTableId, kOwner, schema));

  // Create the table again, and check that the expected failure occurs.
  Status s = hms_catalog_->CreateTable(kTableId, kTableName, kOwner, schema);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckTable(kHmsDatabase, kHmsTableName, kTableId, kOwner, schema));

  // Alter the table.
  SchemaBuilder b(schema);
  b.AddColumn("new_column", DataType::INT32);
  Schema altered_schema = b.Build();
  ASSERT_OK(hms_catalog_->AlterTable(kTableId, kTableName, kAlteredTableName, altered_schema));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsTableName));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsAlteredTableName, kTableId, kOwner, altered_schema));

  // Drop the table.
  ASSERT_OK(hms_catalog_->DropTable(kTableId, kAlteredTableName));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsTableName));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsAlteredTableName));
}

// Checks that Kudu tables will not replace or modify existing HMS entries that
// belong to external tables from other systems.
TEST_F(HmsCatalogTest, TestExternalTable) {
  const string kTableId = "table-id";

  // Create the external table (default.ext).
  hive::Table external_table;
  external_table.dbName = "default";
  external_table.tableName = "ext";
  external_table.tableType = HmsClient::kManagedTable;
  ASSERT_OK(hms_client_->CreateTable(external_table));
  // Retrieve the full HMS table object so it can be compared later (the HMS
  // fills in some fields during creation).
  ASSERT_OK(hms_client_->GetTable("default", "ext", &external_table));

  auto CheckExternalTable = [&] {
    hive::Table current_table;
    ASSERT_OK(hms_client_->GetTable("default", "ext", &current_table));
    ASSERT_EQ(current_table, external_table);
  };

  // Create the Kudu table (default.a).
  Schema schema = AllTypesSchema();
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.a", boost::none, schema));
  NO_FATALS(CheckTable("default", "a", kTableId, boost::none, schema));

  // Try and create a Kudu table with the same name as the external table.
  Status s = hms_catalog_->CreateTable(kTableId, "default.ext", boost::none, schema);
  EXPECT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckExternalTable());

  // Try and rename the Kudu table to the external table name.
  s = hms_catalog_->AlterTable(kTableId, "default.a", "default.ext", schema);
  EXPECT_TRUE(s.IsIllegalState()) << s.ToString();
  NO_FATALS(CheckExternalTable());
  NO_FATALS(CheckTable("default", "a", kTableId, boost::none, schema));

  // Try and rename the external table. This shouldn't succeed because the Table
  // ID doesn't match.
  s = hms_catalog_->AlterTable(kTableId, "default.ext", "default.b", schema);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  NO_FATALS(CheckExternalTable());
  NO_FATALS(CheckTable("default", "a", kTableId, boost::none, schema));
  NO_FATALS(CheckTableDoesNotExist("default", "b"));

  // Try and drop the external table as if it were a Kudu table.
  s = hms_catalog_->DropTable(kTableId, "default.ext");
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
  NO_FATALS(CheckExternalTable());

  // Drop a Kudu table with no corresponding HMS entry.
  NO_FATALS(CheckTableDoesNotExist("default", "bogus_table_name"));
  s = hms_catalog_->DropTable(kTableId, "default.bogus_table_name");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  NO_FATALS(CheckTableDoesNotExist("default", "bogus_table_name"));
}

TEST_F(HmsCatalogTest, TestGetKuduTables) {
  const string kHmsDatabase = "db";
  const string kManagedTableName = "managed_table";
  const string kExternalTableName = "external_table";
  const string kTableName = "external_table";
  const string kNonKuduTableName = "non_kudu_table";
  const string kOwner = "alice";

  // Create a legacy Impala managed table, a legacy Impala external table, a
  // Kudu table, and a non Kudu table.
  hive::Database db;
  db.name = "db";
  ASSERT_OK(hms_client_->CreateDatabase(db));
  ASSERT_OK(CreateLegacyTable("db", "managed_table", HmsClient::kManagedTable));
  hive::Table table;
  ASSERT_OK(hms_client_->GetTable(kHmsDatabase, "managed_table", &table));
  ASSERT_OK(CreateLegacyTable("db", "external_table", HmsClient::kExternalTable));
  ASSERT_OK(hms_client_->GetTable("db", "external_table", &table));

  ASSERT_OK(hms_catalog_->CreateTable("fake-id", "db.table", kOwner, Schema()));

  hive::Table non_kudu_table;
  non_kudu_table.dbName = "db";
  non_kudu_table.tableName = "non_kudu_table";
  ASSERT_OK(hms_client_->CreateTable(non_kudu_table));
  ASSERT_OK(hms_client_->GetTable("db", "non_kudu_table", &table));

  // Retrieve all tables and ensure all entries are found.
  vector<hive::Table> kudu_tables;
  ASSERT_OK(hms_catalog_->GetKuduTables(&kudu_tables));
  ASSERT_EQ(3, kudu_tables.size());
  for (const auto& kudu_table : kudu_tables) {
    ASSERT_FALSE(kudu_table.tableName == "non_kudu_table") << kudu_table;
  }
}

// Checks that the HmsCatalog handles reconnecting to the metastore after a connection failure.
TEST_F(HmsCatalogTest, TestReconnect) {
  // TODO(dan): Figure out a way to test failover among HA HMS instances. The
  // MiniHms does not support HA, since it relies on a single-process Derby database.

  const string kTableId = "table-id";
  const string kHmsDatabase = "default";
  const string kOwner = "alice";
  Schema schema = AllTypesSchema();
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.a", kOwner, schema));
  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, kOwner, schema));

  // Shutdown the HMS and try a few operations.
  ASSERT_OK(StopHms());

  // TODO(dan): once we have HMS catalog stats, assert that repeated attempts
  // while the HMS is unavailable results in a non-linear number of reconnect
  // attempts.

  Status s = hms_catalog_->CreateTable(kTableId, "default.b", kOwner, schema);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  s = hms_catalog_->AlterTable(kTableId, "default.a", "default.c", schema);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS back up and ensure that the same operations succeed.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.d", kOwner, schema));
  });

  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, kOwner, schema));
  NO_FATALS(CheckTable(kHmsDatabase, "d", kTableId, kOwner, schema));

  EXPECT_OK(hms_catalog_->AlterTable(kTableId, "default.a", "default.c", schema));
  NO_FATALS(CheckTable(kHmsDatabase, "c", kTableId, kOwner, schema));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "a"));
}

} // namespace hms
} // namespace kudu
