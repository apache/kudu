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

#include "kudu/integration-tests/hms_itest-base.h"

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/user.h"

using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::hms::HmsClient;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {

Status HmsITestBase::StopHms() {
  RETURN_NOT_OK(hms_client_->Stop());
  return cluster_->hms()->Stop();
}

Status HmsITestBase::StartHms() {
  RETURN_NOT_OK(cluster_->hms()->Start());
  return hms_client_->Start();
}

Status HmsITestBase::CreateDatabase(const string& database_name) {
  hive::Database db;
  db.name = database_name;
  RETURN_NOT_OK(hms_client_->CreateDatabase(db));
  // Sanity check that the DB is created.
  RETURN_NOT_OK(hms_client_->GetDatabase(database_name, &db));
  return Status::OK();
}

Status HmsITestBase::CreateKuduTable(const string& database_name,
                                     const string& table_name,
                                     MonoDelta timeout) {
  // Get coverage of all column types.
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()
                    ->PrimaryKey()->Comment("The Primary Key");
  b.AddColumn("int8_val")->Type(KuduColumnSchema::INT8);
  b.AddColumn("int16_val")->Type(KuduColumnSchema::INT16);
  b.AddColumn("int32_val")->Type(KuduColumnSchema::INT32);
  b.AddColumn("int64_val")->Type(KuduColumnSchema::INT64);
  b.AddColumn("timestamp_val")->Type(KuduColumnSchema::UNIXTIME_MICROS);
  b.AddColumn("date_val")->Type(KuduColumnSchema::DATE);
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
  KuduSchema schema;
  RETURN_NOT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  if (timeout.Initialized()) {
    // If specified, set the timeout for the operation.
    table_creator->timeout(timeout);
  }
  return table_creator->table_name(Substitute("$0.$1",
                                              database_name, table_name))
      .schema(&schema)
      .num_replicas(1)
      .set_range_partition_columns({ "key" })
      .Create();
}

Status HmsITestBase::CreateHmsTable(const string& database_name,
                                    const string& table_name,
                                    const string& table_type,
                                    boost::optional<const string&> kudu_table_name) {
  hive::Table hms_table;
  hms_table.dbName = database_name;
  hms_table.tableName = table_name;
  hms_table.tableType = table_type;
  // TODO(HIVE-19253): Used along with table type to indicate an external table.
  if (table_type == HmsClient::kExternalTable) {
    hms_table.parameters[HmsClient::kExternalTableKey] = "TRUE";
  }
  if (kudu_table_name) {
    hms_table.parameters[HmsClient::kKuduTableNameKey] = *kudu_table_name;
  } else {
    hms_table.parameters[HmsClient::kKuduTableNameKey] =
        Substitute("$0.$1", database_name, table_name);
  }
  return hms_client_->CreateTable(hms_table);
}

Status HmsITestBase::RenameHmsTable(const string& database_name,
                                    const string& old_table_name,
                                    const string& new_table_name) {
  // The HMS doesn't have a rename table API. Instead it offers the more
  // general AlterTable API, which requires the entire set of table fields to be
  // set. Since we don't know these fields during a simple rename operation, we
  // have to look them up.
  hive::Table table;
  RETURN_NOT_OK(hms_client_->GetTable(database_name, old_table_name, &table));
  table.tableName = new_table_name;
  table.parameters[hms::HmsClient::kKuduTableNameKey] =
      Substitute("$0.$1", database_name, new_table_name);
  return hms_client_->AlterTable(database_name, old_table_name, table);
}

Status HmsITestBase::AlterHmsTableDropColumns(const string& database_name,
                                              const string& table_name) {
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

Status HmsITestBase::AlterHmsTableExternalPurge(const string& database_name,
                                                const string& table_name) {
  hive::Table table;
  RETURN_NOT_OK(hms_client_->GetTable(database_name, table_name, &table));
  table.tableType = HmsClient::kExternalTable;
  table.parameters[HmsClient::kExternalTableKey] = "TRUE";
  table.parameters[HmsClient::kExternalPurgeKey] = "TRUE";

  // The KuduMetastorePlugin only allows the master to alter the table type
  // and purge property, so we pretend to be the master.
  hive::EnvironmentContext env_ctx;
  env_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });
  RETURN_NOT_OK(hms_client_->AlterTable(database_name, table_name, table, env_ctx));
  return Status::OK();
}

void HmsITestBase::CheckTable(const string& database_name,
                              const string& table_name,
                              boost::optional<const string&> user,
                              const string& table_type) {
  SCOPED_TRACE(Substitute("Checking table $0.$1", database_name, table_name));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(Substitute("$0.$1", database_name, table_name), &table));

  hive::Table hms_table;
  ASSERT_OK(hms_client_->GetTable(database_name, table_name, &hms_table));

  ASSERT_EQ(table_type, hms_table.tableType);

  string username;
  if (user) {
    username = *user;
  } else {
    ASSERT_OK(GetLoggedInUser(&username));
  }
  ASSERT_EQ(hms_table.owner, username);

  const auto& schema = table->schema();
  ASSERT_EQ(schema.num_columns(), hms_table.sd.cols.size());
  for (int idx = 0; idx < schema.num_columns(); idx++) {
    ASSERT_EQ(schema.Column(idx).name(), hms_table.sd.cols[idx].name);
    ASSERT_EQ(schema.Column(idx).comment(), hms_table.sd.cols[idx].comment);
  }
  ASSERT_EQ(table->id(), hms_table.parameters[hms::HmsClient::kKuduTableIdKey]);
  ASSERT_TRUE(boost::iequals(table->name(),
      hms_table.parameters[hms::HmsClient::kKuduTableNameKey]));
  ASSERT_EQ(HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs()),
            hms_table.parameters[hms::HmsClient::kKuduMasterAddrsKey]);
  ASSERT_EQ(hms::HmsClient::kKuduStorageHandler,
            hms_table.parameters[hms::HmsClient::kStorageHandlerKey]);
}

void HmsITestBase::CheckTableDoesNotExist(const string& database_name,
                                          const string& table_name) {
  SCOPED_TRACE(Substitute("Checking table $0.$1 does not exist", database_name, table_name));

  shared_ptr<KuduTable> table;
  Status s = client_->OpenTable(Substitute("$0.$1", database_name, table_name), &table);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  hive::Table hms_table;
  s = hms_client_->GetTable(database_name, table_name, &hms_table);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

} // namespace kudu
