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
#include "kudu/gutil/strings/join.h"
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
using client::sp::shared_ptr;
using hms::HmsClient;
using hms::HmsCatalog;
using std::cin;
using std::cout;
using std::endl;
using std::ostream;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Split;
using strings::Substitute;

DEFINE_bool(enable_input, true,
            "Whether to enable user input for renaming tables that have Hive"
            "incompatible names.");

// The key is the table ID. The value is a pair of a table in Kudu and a
// vector of tables in the HMS that all share the same table ID.
typedef unordered_map<string, pair<shared_ptr<KuduTable>, vector<hive::Table>>> TablesMap;

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

// Only alter the table in Kudu but not in the Hive Metastore.
Status AlterKuduTableOnly(KuduClient* kudu_client,
                          const string& name,
                          const string& new_name) {
  unique_ptr<KuduTableAlterer> alterer(kudu_client->NewTableAlterer(name));
  return alterer->RenameTo(new_name)
                ->alter_external_catalogs(false)
                ->Alter();
}

// Alter legacy tables (which includes non-Impala tables, Impala managed/external
// tables) to follow the format 'database_name.table_name' in table naming in Kudu.
// Also, create HMS entries for non-Impala tables.
//
// Note that non-Impala tables name should conform to Hive naming standard.
// Otherwise, the upgrade process will fail.
Status AlterLegacyKuduTables(KuduClient* kudu_client,
                             HmsCatalog* hms_catalog,
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
      // Warn instead of dropping the table in the HMS to avoid breakage for
      // installations which have multiple Kudu clusters pointed at the same HMS.
      LOG(WARNING) << Substitute("Found orphan table $0.$1 in the Hive Metastore",
                                 hms_table.second.dbName, hms_table.second.tableName);
    }
  }

  unordered_map<string, Status> failures;
  for (const auto& table_name : table_names) {
    hive::Table* hms_table = FindOrNull(hms_tables_map, table_name);
    shared_ptr<KuduTable> kudu_table;
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
          s = AlterKuduTableOnly(kudu_client, table_name, new_table_name).AndThen([&] {
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
        return AlterKuduTableOnly(kudu_client, table_name, new_table_name);
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

Status Init(const RunnerContext& context,
            shared_ptr<KuduClient>* kudu_client,
            unique_ptr<HmsCatalog>* hms_catalog) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = Split(master_addresses_str, ",");

  if (!hms::HmsCatalog::IsEnabled()) {
    return Status::IllegalState("HMS URIs cannot be empty!");
  }

  // Create Hms Catalog.
  hms_catalog->reset(new hms::HmsCatalog(master_addresses_str));
  RETURN_NOT_OK((*hms_catalog)->Start());

  // Create a Kudu Client.
  return KuduClientBuilder()
      .default_rpc_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms))
      .master_server_addrs(master_addresses)
      .Build(kudu_client);
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
  const string& default_database = FindOrDie(context.required_args,
                                             kDefaultDatabaseArg);
  shared_ptr<KuduClient> kudu_client;
  unique_ptr<HmsCatalog> hms_catalog;
  Init(context, &kudu_client, &hms_catalog);

  // 1. Identify all Kudu tables in the HMS entries.
  vector<hive::Table> hms_tables;
  RETURN_NOT_OK(hms_catalog->RetrieveTables(&hms_tables));

  // 2. Rename all existing Kudu tables to have Hive-compatible table names.
  //    Also, correct all out of sync metadata in HMS entries.
  return AlterLegacyKuduTables(kudu_client.get(), hms_catalog.get(),
                               default_database, hms_tables);
}

// Given a kudu table and a hms table, checks if their metadata is in sync.
bool isSynced(const string& master_addresses,
              const shared_ptr<KuduTable>& kudu_table,
              const hive::Table& hms_table) {
  Schema schema(client::SchemaFromKuduSchema(kudu_table->schema()));
  hive::Table hms_table_copy(hms_table);
  Status s = HmsCatalog::PopulateTable(kudu_table->id(), kudu_table->name(),
                                       schema, master_addresses, &hms_table_copy);
  return hms_table_copy == hms_table && s.ok();
}

// Filter orphan tables from the unsynchronized tables map.
void FilterOrphanedTables(TablesMap* tables_map) {
  for (auto it = tables_map->cbegin(); it != tables_map->cend();) {
    // If the kudu table is empty, then these table in the HMS are
    // orphan tables. Filter it as we do not care about orphan tables.
    if (it->second.first == nullptr) {
      for (const auto &table : it->second.second) {
        LOG(WARNING) << Substitute("Found orphan table $0.$1 in the Hive Metastore",
                                   table.dbName, table.tableName);
      }
      it = tables_map->erase(it);
    } else {
      ++it;
    }
  }
}

// Sample of unsynchronized tables:
//
// TableID| KuduTableName| HmsDbName| HmsTableName| KuduMasterAddresses| HmsTableMasterAddresses
//--------+--------------+----------+-------------+--------------------+-------------------------
//    1   | a            |          |             | 127.0.0.1:50232    |
//    2   | default.c    | default  | c           | 127.0.0.1:50232    | 127.0.0.1:50232
//    2   |              | default  | d           | 127.0.0.1:50232    | 127.0.0.1:50232
//    3   | default.b    |          |             | 127.0.0.1:50232    |
Status PrintUnsyncedTables(const string& master_addresses,
                           const TablesMap& tables_map,
                           ostream& out) {
  out << "Metadata of unsynchronized tables:" << endl;
  DataTable table({ "TableID", "KuduTableName", "HmsDbName", "HmsTableName",
                    "KuduMasterAddresses", "HmsTableMasterAddresses"});
  for (const auto& entry : tables_map) {
    string table_id = entry.first;
    shared_ptr<KuduTable> kudu_table = entry.second.first;
    vector<hive::Table> hms_tables = entry.second.second;
    string kudu_table_name = kudu_table->name();
    if (hms_tables.empty()) {
      table.AddRow({ table_id, kudu_table_name, "", "", master_addresses, "" });
    } else {
      for (hive::Table hms_table : hms_tables) {
        table.AddRow({table_id, kudu_table_name, hms_table.dbName, hms_table.tableName,
                     master_addresses, hms_table.parameters[HmsClient::kKuduMasterAddrsKey]});
      }
    }
  }

  return table.PrintTo(out);
}

Status PrintLegacyTables(const vector<hive::Table>& tables,
                         ostream& out) {
  cout << "Found legacy tables in the Hive Metastore, "
       << "use metadata upgrade tool first: 'kudu hms upgrade'."
       << endl;
  DataTable table({ "HmsDbName", "HmsTableName", "KuduTableName",
                    "KuduMasterAddresses"});
  for (hive::Table t : tables) {
    string kudu_table_name = t.parameters[HmsClient::kLegacyKuduTableNameKey];
    string master_addresses = t.parameters[HmsClient::kKuduMasterAddrsKey];
    table.AddRow({ t.dbName, t.tableName, kudu_table_name, master_addresses });
  }
  return table.PrintTo(out);
}

Status RetrieveUnsyncedTables(const unique_ptr<HmsCatalog>& hms_catalog,
                              const shared_ptr<KuduClient>& kudu_client,
                              const string& master_addresses_str,
                              TablesMap* unsynced_tables_map,
                              vector<hive::Table>* legacy_tables) {
  // 1. Identify all Kudu table in the HMS entries.
  vector<hive::Table> hms_tables;
  RETURN_NOT_OK(hms_catalog->RetrieveTables(&hms_tables));

  // 2. Walk through all the Kudu tables in the HMS and identify any
  //    out of sync tables.
  for (auto& hms_table : hms_tables) {
    string hms_table_id = hms_table.parameters[HmsClient::kKuduTableIdKey];
    if (hms_table.parameters[HmsClient::kStorageHandlerKey] ==
        HmsClient::kKuduStorageHandler) {
      string table_name = Substitute("$0.$1", hms_table.dbName, hms_table.tableName);
      shared_ptr<KuduTable> kudu_table;
      Status s = kudu_client->OpenTable(table_name, &kudu_table);
      if (s.ok() && !isSynced(master_addresses_str, kudu_table, hms_table)) {
        (*unsynced_tables_map)[kudu_table->id()].first = kudu_table;
        (*unsynced_tables_map)[hms_table_id].second.emplace_back(hms_table);
      } else if (s.IsNotFound()) {
        // We cannot determine whether this table is an orphan table in the HMS now, since
        // there may be other tables in Kudu shares the same table ID but not the same name.
        // So do it in the filtering step below.
        (*unsynced_tables_map)[hms_table_id].second.emplace_back(hms_table);
      } else {
        RETURN_NOT_OK(s);
      }
    } else if (hms_table.parameters[HmsClient::kStorageHandlerKey] ==
               HmsClient::kLegacyKuduStorageHandler) {
      legacy_tables->push_back(hms_table);
    }
  }

  // 3. If any Kudu table is not present in the HMS, consider it as an out of sync
  //    table.
  vector<string> table_names;
  RETURN_NOT_OK(kudu_client->ListTables(&table_names));
  unordered_map<string, hive::Table> hms_tables_map = RetrieveTablesMap(std::move(hms_tables));
  for (const auto& table_name : table_names) {
    if (!ContainsKey(hms_tables_map, table_name)) {
      shared_ptr<KuduTable> kudu_table;
      RETURN_NOT_OK(kudu_client->OpenTable(table_name, &kudu_table));
      (*unsynced_tables_map)[kudu_table->id()].first = kudu_table;
    }
  }

  // 4. Filter orphan tables.
  FilterOrphanedTables(unsynced_tables_map);

  return Status::OK();
}

Status CheckHmsMetadata(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  shared_ptr<KuduClient> kudu_client;
  unique_ptr<HmsCatalog> hms_catalog;
  Init(context, &kudu_client, &hms_catalog);

  TablesMap unsynced_tables_map;
  std::vector<hive::Table> legacy_tables;
  RETURN_NOT_OK_PREPEND(RetrieveUnsyncedTables(hms_catalog, kudu_client,
                                               master_addresses_str, &unsynced_tables_map,
                                               &legacy_tables),
                        "error fetching unsynchronized tables");

  // All good.
  if (unsynced_tables_map.empty() && legacy_tables.empty()) {
    cout << "OK" << endl;
    return Status::OK();
  }

  // Something went wrong.
  cout << "FAILED" << endl;
  if (!unsynced_tables_map.empty()) {
    RETURN_NOT_OK_PREPEND(PrintUnsyncedTables(master_addresses_str,
                                              unsynced_tables_map, cout),
                          "error printing inconsistent data");
    cout << endl;
  }

  if (!legacy_tables.empty()) {
    RETURN_NOT_OK_PREPEND(PrintLegacyTables(legacy_tables, cout),
                          "error printing legacy tables");
  }

  return Status::RuntimeError("metadata check tool discovered inconsistent data");
}

Status FixUnsyncedTables(KuduClient* kudu_client,
                         HmsCatalog* hms_catalog,
                         const TablesMap& tables_map) {
  for (const auto& entry : tables_map) {
    const KuduTable& kudu_table = *entry.second.first;
    const vector<hive::Table>& hms_tables = entry.second.second;

    // 1. Create the table in the HMS if there is no corresponding table there.
    string table_id = entry.first;
    string kudu_table_name = kudu_table.name();
    Schema schema = client::SchemaFromKuduSchema(kudu_table.schema());
    cout << Substitute("Table (ID $0) is out of sync.", table_id) << endl;
    if (hms_tables.empty()) {
      RETURN_NOT_OK(hms_catalog->CreateTable(table_id, kudu_table_name,  schema));
      continue;
    }

    // 2. If more than one table shares the same table ID in the HMS, return an error
    //    as it is unsafe to do an automated fix.
    //
    if (hms_tables.size() > 1) {
      auto table_to_string = [] (const hive::Table& hms_table) {
          return strings::Substitute("$0.$1", hms_table.dbName, hms_table.tableName);
      };
      return Status::IllegalState(
          Substitute("Found more than one tables [$0] in the Hive Metastore, with the "
                     "same table ID: $1", JoinMapped(hms_tables, table_to_string, ", "),
                     table_id));
    }

    // 3. If the table name in Kudu is different from the one in the HMS, correct the
    //    table name in Kudu with the one in the HMS. Since we consider the HMS as the
    //    source of truth for table names.
    hive::Table hms_table = hms_tables[0];
    string hms_table_name = Substitute("$0.$1", hms_table.dbName, hms_table.tableName);
    if (kudu_table_name != hms_table_name) {
      string new_table_name;
      cout << Substitute("Renaming Kudu table $0 [id=$1] to $2 to match the Hive "
                         "Metastore catalog.", kudu_table_name, table_id, hms_table_name)
           << endl;
      RETURN_NOT_OK(AlterKuduTableOnly(kudu_client, kudu_table_name, hms_table_name));
      kudu_table_name = hms_table_name;
    }

    // 4. Correct the master addresses, and the column name and type based on the
    //    information in Kudu.
    cout << Substitute("Updating metadata of table $0 [id=$1] in Hive Metastore catalog.",
                       kudu_table_name, table_id) << endl;
    RETURN_NOT_OK(hms_catalog->AlterTable(table_id, hms_table_name,
                                          hms_table_name, schema));
  }

  return Status::OK();
}

Status FixHmsMetadata(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  shared_ptr<KuduClient> kudu_client;
  unique_ptr<HmsCatalog> hms_catalog;
  Init(context, &kudu_client, &hms_catalog);

  TablesMap unsynced_tables_map;
  std::vector<hive::Table> legacy_tables;
  RETURN_NOT_OK_PREPEND(RetrieveUnsyncedTables(hms_catalog, kudu_client,
                                               master_addresses_str,
                                               &unsynced_tables_map,
                                               &legacy_tables),
                        "error fetching unsynchronized tables");

  if (unsynced_tables_map.empty() && legacy_tables.empty()) {
    cout << "Metadata between Kudu and Hive Metastore is in sync." << endl;
    return Status::OK();
  }

  // print the legacy tables if any, and returns a runtime error.
  if (!legacy_tables.empty()) {
    RETURN_NOT_OK_PREPEND(PrintLegacyTables(legacy_tables, cout),
                          "error printing legacy tables");
    return Status::RuntimeError("metadata fix tool encountered fatal errors");
  }

  // Fix inconsistent metadata.
  RETURN_NOT_OK_PREPEND(FixUnsyncedTables(kudu_client.get(), hms_catalog.get(),
                                          unsynced_tables_map),
                        "error fixing inconsistent metadata");
  cout << "DONE" << endl;
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

  unique_ptr<Action> hms_check =
      ActionBuilder("check", &CheckHmsMetadata)
          .Description("Check the metadata consistency between Kudu and Hive Metastores")
          .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
          .AddOptionalParameter("hive_metastore_uris")
          .AddOptionalParameter("hive_metastore_sasl_enabled")
          .AddOptionalParameter("hive_metastore_retry_count")
          .AddOptionalParameter("hive_metastore_send_timeout")
          .AddOptionalParameter("hive_metastore_recv_timeout")
          .AddOptionalParameter("hive_metastore_conn_timeout")
          .Build();

  unique_ptr<Action> hms_fix =
    ActionBuilder("fix", &FixHmsMetadata)
        .Description("Fix the metadata inconsistency between Kudu and Hive Metastores")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter("hive_metastore_uris")
        .AddOptionalParameter("hive_metastore_sasl_enabled")
        .AddOptionalParameter("hive_metastore_retry_count")
        .AddOptionalParameter("hive_metastore_send_timeout")
        .AddOptionalParameter("hive_metastore_recv_timeout")
        .AddOptionalParameter("hive_metastore_conn_timeout")
        .Build();

  return ModeBuilder("hms").Description("Operate on remote Hive Metastores")
                           .AddAction(std::move(hms_upgrade))
                           .AddAction(std::move(hms_check))
                           .AddAction(std::move(hms_fix))
                           .Build();
}

} // namespace tools
} // namespace kudu
