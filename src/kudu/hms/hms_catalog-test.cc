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

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
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
using std::nullopt;
using std::optional;
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
    KuduTest::SetUp();
    bool enable_kerberos = EnableKerberos();

    thrift::ClientOptions hms_client_opts;

    hms_.reset(new hms::MiniHms());

    // Set the `KUDU_HMS_SYNC_ENABLED` environment variable in the
    // HMS environment to manually enable HMS synchronization checks.
    // This means we don't need to stand up a Kudu Cluster for this test.
    hms_->AddEnvVar("KUDU_HMS_SYNC_ENABLED", "1");

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
    if (hms_client_) {
      ASSERT_OK(hms_client_->Stop());
    }
    ASSERT_OK(hms_->Stop());
    KuduTest::TearDown();
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
    CHECK_OK(b.AddKeyColumn("key", DataType::INT32));
    CHECK_OK(b.AddColumn("int8_val", DataType::INT8));
    CHECK_OK(b.AddColumn("int16_val", DataType::INT16));
    CHECK_OK(b.AddColumn("int32_val", DataType::INT32));
    CHECK_OK(b.AddColumn("int64_val", DataType::INT64));
    CHECK_OK(b.AddColumn("timestamp_val", DataType::UNIXTIME_MICROS));
    CHECK_OK(b.AddColumn("date_val", DataType::DATE));
    CHECK_OK(b.AddColumn("string_val", DataType::STRING));
    CHECK_OK(b.AddColumn("bool_val", DataType::BOOL));
    CHECK_OK(b.AddColumn("float_val", DataType::FLOAT));
    CHECK_OK(b.AddColumn("double_val", DataType::DOUBLE));
    CHECK_OK(b.AddColumn("binary_val", DataType::BINARY));
    CHECK_OK(b.AddColumn("decimal32_val", DataType::DECIMAL32));
    CHECK_OK(b.AddColumn("decimal64_val", DataType::DECIMAL64));
    CHECK_OK(b.AddColumn("decimal128_val", DataType::DECIMAL128));
    CHECK_OK(b.AddColumn("varchar_val", DataType::VARCHAR));
    return b.Build();
  }

  void CheckTable(const string& database_name,
                  const string& table_name,
                  const string& table_id,
                  const string& cluster_id,
                  const optional<string>& owner,
                  const Schema& schema,
                  const string& comment) {
    hive::Table table;
    ASSERT_OK(hms_client_->GetTable(database_name, table_name, &table));

    if (owner) {
      EXPECT_EQ(table.owner, *owner);
    } else {
      EXPECT_TRUE(table.owner.empty());
    }
    EXPECT_EQ(table.parameters[HmsClient::kKuduTableIdKey], table_id);
    EXPECT_EQ(table.parameters[HmsClient::kKuduClusterIdKey], cluster_id);
    EXPECT_EQ(table.parameters[HmsClient::kKuduMasterAddrsKey], kMasterAddrs);
    EXPECT_EQ(table.parameters[HmsClient::kStorageHandlerKey], HmsClient::kKuduStorageHandler);
    EXPECT_EQ(table.parameters[HmsClient::kTableCommentKey], comment);

    for (int column_idx = 0; column_idx < schema.num_columns(); column_idx++) {
      EXPECT_EQ(table.sd.cols[column_idx].name, schema.columns()[column_idx].name());
    }
    EXPECT_EQ(table.sd.inputFormat, HmsClient::kKuduInputFormat);
    EXPECT_EQ(table.sd.outputFormat, HmsClient::kKuduOutputFormat);
    EXPECT_EQ(table.sd.serdeInfo.serializationLib, HmsClient::kKuduSerDeLib);
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
        make_pair(HmsClient::kKuduTableNameKey,
                  kudu_table_name),
        make_pair(HmsClient::kKuduMasterAddrsKey,
                  kMasterAddrs),
    });

    // TODO(HIVE-19253): Used along with table type to indicate an external table.
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
INSTANTIATE_TEST_SUITE_P(HmsCatalogTests, HmsCatalogTestParameterized,
                         ::testing::Values(false, true));

// Test creating, altering, and dropping a table with the HMS Catalog.
TEST_P(HmsCatalogTestParameterized, TestTableLifecycle) {
  const string kTableId = "table-id";
  const string kClusterId = "cluster-id";
  const string kHmsDatabase = "default";
  const string kHmsTableName = "table_name";
  const string kTableName = Substitute("$0.$1", kHmsDatabase, kHmsTableName);
  const string kHmsAlteredTableName = "altered_table_name";
  const string kAlteredTableName = Substitute("$0.$1", kHmsDatabase, kHmsAlteredTableName);
  const string kOwner = "alice";
  const string kComment = "comment";

  Schema schema = AllTypesSchema();

  // Create the table.
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, kTableName,
      kClusterId, kOwner, schema, kComment));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsTableName, kTableId,
      kClusterId, kOwner, schema, kComment));

  // Create the table again, and check that the expected failure occurs.
  Status s = hms_catalog_->CreateTable(kTableId, kTableName,
      kClusterId, kOwner, schema, kComment);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckTable(kHmsDatabase, kHmsTableName, kTableId, kClusterId, kOwner,
      schema, kComment));

  // Alter the table.
  SchemaBuilder b(schema);
  ASSERT_OK(b.AddColumn("new_column", DataType::INT32));
  Schema altered_schema = b.Build();
  ASSERT_OK(hms_catalog_->AlterTable(kTableId, kTableName, kAlteredTableName,
      kClusterId, kOwner, altered_schema, kComment));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsTableName));
  NO_FATALS(CheckTable(kHmsDatabase, kHmsAlteredTableName, kTableId, kClusterId, kOwner,
      altered_schema, kComment));

  // Drop the table.
  ASSERT_OK(hms_catalog_->DropTable(kTableId, kAlteredTableName));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsTableName));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, kHmsAlteredTableName));
}

// Checks that Kudu tables will not replace or modify existing HMS entries that
// belong to external tables from other systems.
TEST_F(HmsCatalogTest, TestExternalTable) {
  const string kTableId = "table-id";
  const string kClusterId = "cluster-id";
  const string kComment = "comment";

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
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.a",
      kClusterId, nullopt, schema, kComment));
  NO_FATALS(CheckTable("default", "a", kTableId, kClusterId, nullopt, schema, kComment));

  // Try and create a Kudu table with the same name as the external table.
  Status s = hms_catalog_->CreateTable(kTableId, "default.ext",
      kClusterId, nullopt, schema, kComment);
  EXPECT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckExternalTable());

  // Try and rename the Kudu table to the external table name.
  s = hms_catalog_->AlterTable(kTableId, "default.a", "default.ext",
      kClusterId, nullopt, schema, kComment);
  EXPECT_TRUE(s.IsIllegalState()) << s.ToString();
  NO_FATALS(CheckExternalTable());
  NO_FATALS(CheckTable("default", "a", kTableId, kClusterId, nullopt, schema, kComment));

  // Try and rename the external table. This shouldn't succeed because the Table
  // ID doesn't match.
  s = hms_catalog_->AlterTable(kTableId, "default.ext", "default.b",
      kClusterId, nullopt, schema, kComment);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  NO_FATALS(CheckExternalTable());
  NO_FATALS(CheckTable("default", "a", kTableId, kClusterId, nullopt, schema, kComment));
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
  const string kClusterId = "cluster-id";
  const string kHmsDatabase = "db";
  const string kManagedTableName = "managed_table";
  const string kExternalTableName = "external_table";
  const string kTableName = "external_table";
  const string kNonKuduTableName = "non_kudu_table";
  const string kOwner = "alice";
  const string kComment = "comment";

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

  ASSERT_OK(hms_catalog_->CreateTable("fake-id", "db.table",
      kClusterId, kOwner, Schema(), kComment));

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
  const string kClusterId = "cluster-id";
  const string kHmsDatabase = "default";
  const string kOwner = "alice";
  const string kComment = "comment";

  Schema schema = AllTypesSchema();
  ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.a",
      kClusterId, kOwner, schema, kComment));
  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, kClusterId, kOwner, schema, kComment));
  // Shutdown the HMS and try a few operations.
  ASSERT_OK(StopHms());

  // TODO(dan): once we have HMS catalog stats, assert that repeated attempts
  // while the HMS is unavailable results in a non-linear number of reconnect
  // attempts.

  Status s = hms_catalog_->CreateTable(kTableId, "default.b",
      kClusterId, kOwner, schema, kComment);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  s = hms_catalog_->AlterTable(kTableId, "default.a", "default.c",
      kClusterId, kOwner, schema, kComment);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS back up and ensure that the same operations succeed.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(hms_catalog_->CreateTable(kTableId, "default.d",
        kClusterId, kOwner, schema, kComment));
  });

  NO_FATALS(CheckTable(kHmsDatabase, "a", kTableId, kClusterId, kOwner, schema, kComment));
  NO_FATALS(CheckTable(kHmsDatabase, "d", kTableId, kClusterId, kOwner, schema, kComment));

  EXPECT_OK(hms_catalog_->AlterTable(kTableId, "default.a", "default.c",
      kClusterId, kOwner, schema, kComment));
  NO_FATALS(CheckTable(kHmsDatabase, "c", kTableId, kClusterId, kOwner, schema, kComment));
  NO_FATALS(CheckTableDoesNotExist(kHmsDatabase, "a"));
}

TEST_F(HmsCatalogTest, TestMetastoreUuid) {
  // Test that if the HMS has a DB UUID, we can retrieve it.
  const auto get_and_verify_uuid = [&] (string* ret) {
    Status s = hms_catalog_->GetUuid(ret);
    if (s.ok()) {
      ASSERT_EQ(36, ret->size());
    } else {
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    }
  };
  string uuid;
  NO_FATALS(get_and_verify_uuid(&uuid));

  // After stopping the HMS:
  // 1. We should still be able to initialize the catalog.
  // 2. Attempts to fetch the DB UUID will fail.
  ASSERT_OK(hms_->Stop());
  hms_catalog_.reset(new HmsCatalog(kMasterAddrs));
  ASSERT_OK(hms_catalog_->Start());
  ASSERT_TRUE(hms_catalog_->GetUuid(nullptr).IsNotSupported());

  // But if we start the HMS back up, we should be able to eventually get the
  // DB UUID, though we need to account for the possibility that this HMS may
  // not support it.
  ASSERT_OK(hms_->Start());
  string uuid2;
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(get_and_verify_uuid(&uuid2));
    // Verify that if we got a UUID the first time, that it's the same as the
    // one we go this time.
    if (!uuid.empty()) {
      ASSERT_EQ(uuid, uuid2);
    }
  });
}

} // namespace hms
} // namespace kudu
