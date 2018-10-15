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
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <rapidjson/document.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/value.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

DECLARE_string(tables);
DEFINE_bool(check_row_existence, false,
            "Also check for the existence of the row on the leader replica of "
            "the tablet. If found, the full row will be printed; if not found, "
            "an error message will be printed and the command will return a "
            "non-zero status.");
DEFINE_bool(modify_external_catalogs, true,
            "Whether to modify external catalogs, such as the Hive Metastore, "
            "when renaming or dropping a table.");
DEFINE_bool(list_tablets, false,
            "Include tablet and replica UUIDs in the output");

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduPredicate;
using client::KuduScanner;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduTable;
using client::KuduTableAlterer;
using client::internal::ReplicaController;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

// This class only exists so that ListTables() can easily be friended by
// KuduReplica, KuduReplica::Data, and KuduClientBuilder.
class TableLister {
 public:
  static Status ListTablets(const vector<string>& master_addresses) {
    KuduClientBuilder builder;
    ReplicaController::SetVisibility(&builder, ReplicaController::Visibility::ALL);
    client::sp::shared_ptr<KuduClient> client;
    RETURN_NOT_OK(builder
                  .master_server_addrs(master_addresses)
                  .Build(&client));
    vector<string> table_names;
    RETURN_NOT_OK(client->ListTables(&table_names));

    vector<string> table_filters = Split(FLAGS_tables, ",", strings::SkipEmpty());
    for (const auto& tname : table_names) {
      if (!MatchesAnyPattern(table_filters, tname)) continue;
      cout << tname << endl;
      if (!FLAGS_list_tablets) {
        continue;
      }
      client::sp::shared_ptr<KuduTable> client_table;
      RETURN_NOT_OK(client->OpenTable(tname, &client_table));
      vector<KuduScanToken*> tokens;
      ElementDeleter deleter(&tokens);
      KuduScanTokenBuilder builder(client_table.get());
      RETURN_NOT_OK(builder.Build(&tokens));

      for (const auto* token : tokens) {
        cout << "  T " << token->tablet().id() << endl;
        for (const auto* replica : token->tablet().replicas()) {
          const bool is_voter = ReplicaController::is_voter(*replica);
          const bool is_leader = replica->is_leader();
          cout << Substitute("    $0 $1 $2:$3",
              is_leader ? "L" : (is_voter ? "V" : "N"), replica->ts().uuid(),
              replica->ts().hostname(), replica->ts().port()) << endl;
        }
        cout << endl;
      }
      cout << endl;
    }
    return Status::OK();
  }
};

namespace {

const char* const kTableNameArg = "table_name";
const char* const kNewTableNameArg = "new_table_name";
const char* const kColumnNameArg = "column_name";
const char* const kNewColumnNameArg = "new_column_name";
const char* const kKeyArg = "primary_key";

Status CreateKuduClient(const RunnerContext& context,
                        client::sp::shared_ptr<KuduClient>* client) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  return KuduClientBuilder()
             .master_server_addrs(master_addresses)
             .Build(client);
}

Status DeleteTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  return client->DeleteTableInCatalogs(table_name, FLAGS_modify_external_catalogs);
}

Status DescribeTable(const RunnerContext& context) {
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  // The schema.
  const client::KuduSchema& schema = table->schema();
  cout << "TABLE " << table_name << " " << schema.ToString() << endl;

  // The partition schema with current range partitions.
  vector<Partition> partitions;
  RETURN_NOT_OK_PREPEND(ListPartitions(table, &partitions),
                        "failed to retrieve current partitions");
  const auto& schema_internal = client::SchemaFromKuduSchema(schema);
  const auto& partition_schema = table->partition_schema();
  vector<string> partition_strs;
  for (const auto& partition : partitions) {
    // Deduplicate by hash bucket to get a unique entry per range partition.
    const auto& hash_buckets = partition.hash_buckets();
    if (!std::all_of(hash_buckets.begin(),
                     hash_buckets.end(),
                     [](int32_t bucket) { return bucket == 0; })) {
      continue;
    }
    auto range_partition_str =
        partition_schema.RangePartitionDebugString(partition.range_key_start(),
                                                   partition.range_key_end(),
                                                   schema_internal);
    partition_strs.push_back(std::move(range_partition_str));
  }
  cout << partition_schema.DisplayString(schema_internal, partition_strs)
       << endl;

  // Finally, the replication factor.
  cout << "REPLICAS " << table->num_replicas() << endl;

  return Status::OK();
}

Status LocateRow(const RunnerContext& context) {
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  // Create an equality predicate for each primary key column.
  const string& row_str = FindOrDie(context.required_args, kKeyArg);
  JsonReader reader(row_str);
  RETURN_NOT_OK(reader.Init());
  vector<const rapidjson::Value*> values;
  RETURN_NOT_OK(reader.ExtractObjectArray(reader.root(), nullptr, &values));

  const auto& schema = table->schema();
  vector<int> key_indexes;
  schema.GetPrimaryKeyColumnIndexes(&key_indexes);
  if (values.size() != key_indexes.size()) {
    return Status::InvalidArgument(
        Substitute("wrong number of key columns specified: expected $0 but received $1",
                   key_indexes.size(),
                   values.size()));
  }

  vector<unique_ptr<KuduPredicate>> predicates;
  for (int i = 0; i < values.size(); i++) {
    const auto key_index = key_indexes[i];
    const auto& column = schema.Column(key_index);
    const auto& col_name = column.name();
    const auto type = column.type();
    switch (type) {
      case KuduColumnSchema::INT8:
      case KuduColumnSchema::INT16:
      case KuduColumnSchema::INT32:
      case KuduColumnSchema::INT64:
      case KuduColumnSchema::UNIXTIME_MICROS: {
        int64_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt64(values[i], nullptr, &value),
            Substitute("unable to parse value for column '$0' of type $1",
                       col_name,
                       KuduColumnSchema::DataTypeToString(type)));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name,
                                          client::KuduPredicate::EQUAL,
                                          client::KuduValue::FromInt(value)));
        break;
      }
      case KuduColumnSchema::BINARY:
      case KuduColumnSchema::STRING: {
        string value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractString(values[i], nullptr, &value),
            Substitute("unable to parse value for column '$0' of type $1",
                       col_name,
                       KuduColumnSchema::DataTypeToString(type)));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name,
                                          client::KuduPredicate::EQUAL,
                                          client::KuduValue::CopyString(value)));
        break;
      }
      case KuduColumnSchema::DECIMAL:
        return Status::NotSupported(
            Substitute("unsupported type $0 for key column '$1': "
                       "$0 key columns are not supported by this tool",
                       KuduColumnSchema::DataTypeToString(type),
                       col_name));
      default:
        return Status::NotSupported(
            Substitute("unsupported type $0 for key column '$1': "
                       "is this tool out of date?",
                       KuduColumnSchema::DataTypeToString(type),
                       col_name));
    }
  }

  // Find the tablet by constructing scan tokens for a scan with equality
  // predicates on all key columns. At most one tablet will match, so there
  // will be at most one token, and we can report the id of its tablet.
  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  KuduScanTokenBuilder builder(table.get());
  // In case we go on to check for existence of the row.
  RETURN_NOT_OK(builder.SetSelection(KuduClient::ReplicaSelection::LEADER_ONLY));
  for (auto& predicate : predicates) {
    RETURN_NOT_OK(builder.AddConjunctPredicate(predicate.release()));
  }
  RETURN_NOT_OK(builder.Build(&tokens));
  if (tokens.empty()) {
    // Must be in a non-covered range partition.
    return Status::NotFound("row does not belong to any currently existing tablet");
  }
  if (tokens.size() > 1) {
    // This should be impossible.
    return Status::IllegalState(
        "all primary key columns specified but more than one matching tablet?");
  }
  cout << tokens[0]->tablet().id() << endl;

  if (FLAGS_check_row_existence) {
    KuduScanner* scanner_ptr;
    RETURN_NOT_OK(tokens[0]->IntoKuduScanner(&scanner_ptr));
    unique_ptr<KuduScanner> scanner(scanner_ptr);
    vector<string> row_str;
    RETURN_NOT_OK(ScanToStrings(scanner.get(), &row_str));
    if (row_str.empty()) {
      return Status::NotFound("row does not exist");
    }
    // There should be exactly one result, but if somehow there are more, print
    // them all before returning an error.
    cout << JoinStrings(row_str, "\n") << endl;
    if (row_str.size() != 1) {
      // This should be impossible.
      return Status::IllegalState(
          Substitute("expected 1 row but received $0", row_str.size()));
    }
  }
  return Status::OK();
}

Status RenameTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& new_table_name = FindOrDie(context.required_args, kNewTableNameArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  return alterer->RenameTo(new_table_name)
                ->modify_external_catalogs(FLAGS_modify_external_catalogs)
                ->Alter();
}

Status RenameColumn(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& new_column_name = FindOrDie(context.required_args, kNewColumnNameArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->RenameTo(new_column_name);
  return alterer->Alter();
}

Status ListTables(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  return TableLister::ListTablets(Split(master_addresses_str, ","));
}

} // anonymous namespace

unique_ptr<Mode> BuildTableMode() {
  unique_ptr<Action> delete_table =
      ActionBuilder("delete", &DeleteTable)
      .Description("Delete a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to delete" })
      .AddOptionalParameter("modify_external_catalogs")
      .Build();

  unique_ptr<Action> describe_table =
      ActionBuilder("describe", &DescribeTable)
      .Description("Describe a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to describe" })
      .Build();

  unique_ptr<Action> list_tables =
      ActionBuilder("list", &ListTables)
      .Description("List tables")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddOptionalParameter("tables")
      .AddOptionalParameter("list_tablets")
      .Build();

  unique_ptr<Action> locate_row =
      ActionBuilder("locate_row", &LocateRow)
      .Description("Locate which tablet a row belongs to")
      .ExtraDescription("Provide the primary key as a JSON array of primary "
                        "key values, e.g. '[1, \"foo\", 2, \"bar\"]'. The "
                        "output will be the tablet id associated with the row "
                        "key. If there is no such tablet, an error message "
                        "will be printed and the command will return a "
                        "non-zero status")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to look up against" })
      .AddRequiredParameter({ kKeyArg,
                              "String representation of the row's primary key "
                              "as a JSON array" })
      .AddOptionalParameter("check_row_existence")
      .Build();

  unique_ptr<Action> rename_column =
      ActionBuilder("rename_column", &RenameColumn)
      .Description("Rename a column")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to rename" })
      .AddRequiredParameter({ kNewColumnNameArg, "New column name" })
      .Build();

  unique_ptr<Action> rename_table =
      ActionBuilder("rename_table", &RenameTable)
      .Description("Rename a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to rename" })
      .AddRequiredParameter({ kNewTableNameArg, "New table name" })
      .AddOptionalParameter("modify_external_catalogs")
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(delete_table))
      .AddAction(std::move(describe_table))
      .AddAction(std::move(list_tables))
      .AddAction(std::move(locate_row))
      .AddAction(std::move(rename_column))
      .AddAction(std::move(rename_table))
      .Build();
}

} // namespace tools
} // namespace kudu

