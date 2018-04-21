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

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_catalog.h"
#include "kudu/hms/hms_client.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

DECLARE_int64(timeout_ms); // defined in tool_action_common
DECLARE_string(hive_metastore_uris);

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduTable;
using client::KuduTableAlterer;
using hms::HmsClient;
using hms::HmsCatalog;
using std::cin;
using std::cout;
using std::endl;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Split;
using strings::Substitute;

DEFINE_bool(enable_input, true,
            "Whether to enable user input for renaming tables that have hive"
            "incompatible names.");

const char* const kDefaultDatabaseArg = "default_database";
const char* const kDefaultDatabaseArgDesc = "The database that non-Impala Kudu "
                                            "table should reside in";
const char* const kInvalidNameError = "is not a valid object name";

// Returns a map that contains all Kudu tables in the HMS. Its key is
// the Kudu table name and its value is the corresponding HMS table.
unordered_map<string, hive::Table> RetrieveTablesMap(vector<hive::Table> hms_tables) {
  unordered_map<string, hive::Table> hms_tables_map;
  for (auto& hms_table : hms_tables) {
    if (hms_table.parameters[HmsClient::kStorageHandlerKey] ==
        HmsClient::kLegacyKuduStorageHandler) {
      hms_tables_map.emplace(hms_table.parameters[HmsClient::kLegacyKuduTableNameKey],
                              hms_table);
    } else if (hms_table.parameters[HmsClient::kStorageHandlerKey] ==
        HmsClient::kKuduStorageHandler) {
      hms_tables_map.emplace(Substitute("$0.$1", hms_table.dbName, hms_table.tableName),
                              hms_table);
    }
  }
  return hms_tables_map;
}

string RenameHiveIncompatibleTable(const string& name) {
  string table_name(name);
  cout << Substitute("Table $0 is not hive compatible.", table_name) << endl;
  cout << "Please input a new table name: ";
  getline(cin, table_name);

  return table_name;
}

// Alter legacy tables (which includes non-Impala tables, Impala managed/external
// tables) to follow the format 'database_name.table_name' in table naming in Kudu.
// Also, create HMS entries for non-Impala tables.
//
// Note that non-Impala tables name should conform to Hive naming standard.
// Otherwise, the upgrade process will fail.
Status AlterLegacyKuduTables(const client::sp::shared_ptr<KuduClient>& kudu_client,
                             const unique_ptr<HmsCatalog>& hms_catalog,
                             const string& default_database,
                             const vector<hive::Table>& hms_tables) {
  vector<string> table_names;
  RETURN_NOT_OK(kudu_client->ListTables(&table_names));
  unordered_map<string, hive::Table> hms_tables_map = RetrieveTablesMap(hms_tables);

  // Take care of the orphan tables in the HMS.
  for (const auto& hms_table : hms_tables_map) {
    bool exist;
    RETURN_NOT_OK(kudu_client->TableExists(hms_table.first, &exist));
    if (!exist) {
      LOG(WARNING) << Substitute("Found orphan table $0.$1 in the Hive Metastore",
                                 hms_table.second.dbName, hms_table.second.tableName);
    }
  }

  auto alter_kudu_table = [&](const string& name,
                              const string& new_name) -> Status {
    unique_ptr<KuduTableAlterer> alterer(kudu_client->NewTableAlterer(name));
    return alterer->RenameTo(new_name)
                  ->Alter();
  };

  unordered_map<string, Status> failures;
  for (const auto& table_name : table_names) {
    hive::Table* hms_table = FindOrNull(hms_tables_map, table_name);
    client::sp::shared_ptr<KuduTable> kudu_table;
    RETURN_NOT_OK(kudu_client->OpenTable(table_name, &kudu_table));
    Status s;
    if (hms_table) {
      // Rename legacy Impala managed tables and external tables to follow the
      // format 'database_name.table_name'. This is a no-op for non-legacy tables
      // stored in the HMS.
      if (hms_table->parameters[HmsClient::kStorageHandlerKey] ==
          HmsClient::kLegacyKuduStorageHandler) {
        string new_table_name = Substitute("$0.$1", hms_table->dbName, hms_table->tableName);
        bool exist;
        RETURN_NOT_OK(kudu_client->TableExists(new_table_name, &exist));
        if (!exist) {
          // TODO(Hao): Use notification listener to avoid race conditions.
          s = alter_kudu_table(table_name, new_table_name).AndThen([&] {
            return hms_catalog->UpgradeLegacyImpalaTable(kudu_table->id(),
                hms_table->dbName, hms_table->tableName,
                client::SchemaFromKuduSchema(kudu_table->schema()));
          });
        } else {
          LOG(WARNING) << Substitute("Failed to upgrade legacy Impala table '$0.$1' "
                                     "(Kudu table name: $2), because a Kudu table with "
                                     "name '$0.$1' already exists'.", hms_table->dbName,
                                      hms_table->tableName, table_name);
        }
      }
    } else {
      // Create the table in the HMS.
      string new_table_name = Substitute("$0.$1", default_database, table_name);
      Schema schema = client::SchemaFromKuduSchema(kudu_table->schema());
      s = hms_catalog->CreateTable(kudu_table->id(), new_table_name, schema);
      while (!s.ok() && FLAGS_enable_input &&
             (MatchPattern(s.ToString(), Substitute("*$0*", kInvalidNameError)) ||
              MatchPattern(s.ToString(), Substitute("*$0*", HmsCatalog::kInvalidTableError)))) {
        new_table_name = Substitute("$0.$1", default_database,
                                    RenameHiveIncompatibleTable(table_name));
        s = hms_catalog->CreateTable(kudu_table->id(), new_table_name, schema);
      }
      s = s.AndThen([&] {
        return alter_kudu_table(table_name, new_table_name);
      });
    }

    if (!s.ok()) {
      failures.emplace(table_name, s);
    }
  }

  // Returns the first failure that was seen, if any.
  if (!failures.empty()) {
    for (const auto& failure : failures) {
      LOG(WARNING) << Substitute("Failed to upgrade Kudu table $0, because: ",
                                 failure.first, failure.second.ToString());
    }

    return failures.begin()->second;
  } else {
    return Status::OK();
  }
}

// Upgrade the metadata format of legacy impala managed or external tables
// in HMS entries, as well as rename the existing tables in Kudu to adapt
// to the new naming rules.
//
// Sample Legacy Hms Entries
// Managed table
//  Table(
//      tableName=customer,
//      dbName=tpch_1000_kudu,
//      parameters={
//          kudu.master_addresses: <master-addr>:7051,
//          kudu.table_name: impala::tpch_1000_kudu.customer,
//          storage_handler: com.cloudera.kudu.hive.KuduStorageHandler,
//      },
//      tableType=MANAGED_TABLE,
//  )
//
//  External table
//  Table(
//      tableName=metrics,
//      dbName=default,
//      parameters={
//          EXTERNAL: TRUE,
//          kudu.master_addresses: <master-addr>,
//          kudu.table_name: metrics,
//          storage_handler: com.cloudera.kudu.hive.KuduStorageHandler,
//      },
//      tableType=EXTERNAL_TABLE,
//  )
Status HmsUpgrade(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  const string& default_database = FindOrDie(context.required_args,
                                             kDefaultDatabaseArg);

  if (!hms::HmsCatalog::IsEnabled()) {
    return Status::IllegalState("HMS URIs cannot be empty!");
  }

  // Start Hms Catalog.
  unique_ptr<HmsCatalog> hms_catalog(new hms::HmsCatalog(master_addresses_str));
  RETURN_NOT_OK(hms_catalog->Start());

  // Create a Kudu Client.
  client::sp::shared_ptr<KuduClient> kudu_client;
  RETURN_NOT_OK(KuduClientBuilder()
      .default_rpc_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms))
      .master_server_addrs(master_addresses)
      .Build(&kudu_client));

  // 1. Identify all Kudu tables in the HMS entries.
  vector<hive::Table> hms_tables;
  RETURN_NOT_OK(hms_catalog->RetrieveTables(&hms_tables));

  // 2. Rename all existing Kudu tables to have Hive-compatible table names.
  //    Also, correct all out of sync metadata in HMS entries.
  RETURN_NOT_OK(AlterLegacyKuduTables(kudu_client, hms_catalog,
                                      default_database, hms_tables));

  hms_catalog->Stop();
  return Status::OK();
}

unique_ptr<Mode> BuildHmsMode() {
  unique_ptr<Action> hms_upgrade =
      ActionBuilder("upgrade", &HmsUpgrade)
          .Description("Upgrade the legacy metadata for Kudu and Hive Metastores")
          .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
          .AddRequiredParameter({ kDefaultDatabaseArg, kDefaultDatabaseArgDesc })
          .AddOptionalParameter("hive_metastore_uris")
          .AddOptionalParameter("hive_metastore_sasl_enabled")
          .AddOptionalParameter("hive_metastore_retry_count")
          .AddOptionalParameter("hive_metastore_send_timeout")
          .AddOptionalParameter("hive_metastore_recv_timeout")
          .AddOptionalParameter("hive_metastore_conn_timeout")
          .AddOptionalParameter("enable_input")
          .Build();

  return ModeBuilder("hms").Description("Operate on remote Hive Metastores")
                           .AddAction(std::move(hms_upgrade))
                           .Build();
}

} // namespace tools
} // namespace kudu
