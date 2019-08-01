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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/value.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/table_scanner.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::internal::ReplicaController;
using std::cerr;
using std::cout;
using std::endl;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

DEFINE_bool(check_row_existence, false,
            "Also check for the existence of the row on the leader replica of "
            "the tablet. If found, the full row will be printed; if not found, "
            "an error message will be printed and the command will return a "
            "non-zero status.");
DEFINE_string(dst_table, "",
              "The name of the destination table the data will be copied to. "
              "If the empty string, use the same name as the source table.");
DEFINE_bool(list_tablets, false,
            "Include tablet and replica UUIDs in the output");
DEFINE_bool(modify_external_catalogs, true,
            "Whether to modify external catalogs, such as the Hive Metastore, "
            "when renaming or dropping a table.");
DEFINE_string(config_names, "",
              "Comma-separated list of configurations to display. "
              "An empty value displays all configs.");
DEFINE_string(lower_bound_type, "INCLUSIVE_BOUND",
              "The type of the lower bound, either inclusive or exclusive. "
              "Defaults to inclusive. This flag is case-insensitive.");
DEFINE_string(upper_bound_type, "EXCLUSIVE_BOUND",
              "The type of the upper bound, either inclusive or exclusive. "
              "Defaults to exclusive. This flag is case-insensitive.");
DECLARE_bool(show_values);
DECLARE_string(tables);

namespace kudu {
namespace tools {

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

const char* const kNewTableNameArg = "new_table_name";
const char* const kColumnNameArg = "column_name";
const char* const kNewColumnNameArg = "new_column_name";
const char* const kKeyArg = "primary_key";
const char* const kConfigNameArg = "config_name";
const char* const kConfigValueArg = "config_value";
const char* const kErrorMsgArg = "unable to parse value $0 for column $1 of type $2";
const char* const kTableRangeLowerBoundArg = "table_range_lower_bound";
const char* const kTableRangeUpperBoundArg = "table_range_upper_bound";

enum PartitionAction {
    ADD,
    DROP,
};

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
  const KuduSchema& schema = table->schema();
  cout << "TABLE " << table_name << " " << schema.ToString() << endl;

  // The partition schema with current range partitions.
  vector<Partition> partitions;
  RETURN_NOT_OK_PREPEND(table->ListPartitions(&partitions),
                        "failed to retrieve current partitions");
  const auto& schema_internal = KuduSchema::ToSchema(schema);
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
    partition_strs.emplace_back(std::move(range_partition_str));
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
  RETURN_NOT_OK(reader.ExtractObjectArray(reader.root(),
                                          /*field=*/nullptr,
                                          &values));

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
            reader.ExtractInt64(values[i], /*field=*/nullptr, &value),
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
            reader.ExtractString(values[i], /*field=*/nullptr, &value),
            Substitute("unable to parse value for column '$0' of type $1",
                       col_name,
                       KuduColumnSchema::DataTypeToString(type)));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name,
                                          client::KuduPredicate::EQUAL,
                                          client::KuduValue::CopyString(value)));
        break;
      }
      case KuduColumnSchema::BOOL: {
        // As of the writing of this tool, BOOL is not a supported key column
        // type, but just in case it becomes one, we pre-load support for it.
        bool value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractBool(values[i], /*field=*/nullptr, &value),
            Substitute("unable to parse value for column '$0' of type $1",
                       col_name,
                       KuduColumnSchema::DataTypeToString(type)));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name,
                                          client::KuduPredicate::EQUAL,
                                          client::KuduValue::FromBool(value)));
        break;
      }
      case KuduColumnSchema::FLOAT:
      case KuduColumnSchema::DOUBLE: {
        // Like BOOL, as of the writing of this tool, floating point types are
        // not supported for key columns, but we can pre-load support for them
        // in case they become supported.
        double value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractDouble(values[i], /*field=*/nullptr, &value),
            Substitute("unable to parse value for column '$0' of type $1",
                       col_name,
                       KuduColumnSchema::DataTypeToString(type)));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name,
                                          client::KuduPredicate::EQUAL,
                                          client::KuduValue::FromDouble(value)));
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
    // This should be impossible. But if it does happen, we'd like to know what
    // all the matching tablets were.
    for (const auto& token : tokens) {
      cerr << token->tablet().id() << endl;
    }
    return Status::IllegalState(Substitute(
          "all primary key columns specified but found $0 matching tablets!",
          tokens.size()));
  }
  cout << tokens[0]->tablet().id() << endl;

  if (FLAGS_check_row_existence) {
    KuduScanner* scanner_ptr;
    RETURN_NOT_OK(tokens[0]->IntoKuduScanner(&scanner_ptr));
    unique_ptr<KuduScanner> scanner(scanner_ptr);
    RETURN_NOT_OK(scanner->Open());
    vector<string> row_str;
    client::KuduScanBatch batch;
    while (scanner->HasMoreRows()) {
      RETURN_NOT_OK(scanner->NextBatch(&batch));
      for (const auto& row : batch) {
        row_str.emplace_back(row.ToString());
      }
    }
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
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  return TableLister::ListTablets(master_addresses);
}

Status ScanTable(const RunnerContext &context) {
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  const string& table_name = FindOrDie(context.required_args, kTableNameArg);

  FLAGS_show_values = true;
  TableScanner scanner(client, table_name);
  scanner.SetOutput(&cout);
  return scanner.StartScan();
}

Status CopyTable(const RunnerContext& context) {
  client::sp::shared_ptr<KuduClient> src_client;
  RETURN_NOT_OK(CreateKuduClient(context, &src_client));
  const string& src_table_name = FindOrDie(context.required_args, kTableNameArg);

  client::sp::shared_ptr<KuduClient> dst_client;
  if (FindOrDie(context.required_args, kMasterAddressesArg)
     == FindOrDie(context.required_args, kDestMasterAddressesArg)) {
    dst_client = src_client;
  } else {
    RETURN_NOT_OK(CreateKuduClient(context, kDestMasterAddressesArg, &dst_client));
  }

  const string& dst_table_name = FLAGS_dst_table.empty() ? src_table_name : FLAGS_dst_table;

  TableScanner scanner(src_client, src_table_name, dst_client, dst_table_name);
  scanner.SetOutput(&cout);
  return scanner.StartCopy();
}

Status SetExtraConfig(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& config_name = FindOrDie(context.required_args, kConfigNameArg);
  const string& config_value = FindOrDie(context.required_args, kConfigValueArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterExtraConfig({ { config_name, config_value} });
  return alterer->Alter();
}

Status GetExtraConfigs(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  set<string> config_names = strings::Split(FLAGS_config_names, ",", strings::SkipEmpty());

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  DataTable data_table({ "Configuration", "Value" });
  if (config_names.empty()) {
    for (const auto& extra_config : table->extra_configs()) {
      data_table.AddRow({ extra_config.first, extra_config.second });
    }
  } else {
    for (const auto& config_name : config_names) {
      const string* config_value = FindOrNull(table->extra_configs(), config_name);
      if (config_value) {
        data_table.AddRow({ config_name, *config_value });
      }
    }
  }
  return data_table.PrintTo(cout);
}

Status ConvertToKuduPartialRow(const client::sp::shared_ptr<KuduTable>& table,
                               const string& range_bound_str,
                               KuduPartialRow* range_bound_partial_row) {
  JsonReader reader(range_bound_str);
  RETURN_NOT_OK(reader.Init());
  vector<const rapidjson::Value *> values;
  RETURN_NOT_OK(reader.ExtractObjectArray(reader.root(),
                                          /*field=*/nullptr,
                                          &values));

  // If range_bound_str is empty, an empty row will be used.
  if (values.empty()) {
    return Status::OK();
  }
  const Schema& schema = KuduSchema::ToSchema(table->schema());
  const auto& partition_schema = table->partition_schema();
  vector<int32_t> key_indexes;
  RETURN_NOT_OK(partition_schema.GetRangeSchemaColumnIndexes(schema, &key_indexes));
  if (values.size() != key_indexes.size()) {
    return Status::InvalidArgument(
        Substitute("wrong number of range columns specified: expected $0 but received $1",
                   key_indexes.size(),
                   values.size()));
  }

  for (int i = 0; i < values.size(); i++) {
    const auto key_index = key_indexes[i];
    const auto& column = table->schema().Column(key_index);
    const auto& col_name = column.name();
    const auto type = column.type();
    const auto error_msg = Substitute(kErrorMsgArg, values[i], col_name,
        KuduColumnSchema::DataTypeToString(type));
    switch (type) {
      case KuduColumnSchema::INT8: {
        int64_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt64(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetInt8(col_name, static_cast<int8_t>(value)));
        break;
      }
      case KuduColumnSchema::INT16: {
        int64_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt64(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetInt16(col_name, static_cast<int16_t>(value)));
        break;
      }
      case KuduColumnSchema::INT32: {
        int32_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt32(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetInt32(col_name, value));
        break;
      }
      case KuduColumnSchema::INT64: {
        int64_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt64(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetInt64(col_name, value));
        break;
      }
      case KuduColumnSchema::UNIXTIME_MICROS: {
        int64_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt64(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetUnixTimeMicros(col_name, value));
        break;
      }
      case KuduColumnSchema::BINARY: {
        string value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractString(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetBinary(col_name, value));
        break;
      }
      case KuduColumnSchema::STRING: {
        string value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractString(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetString(col_name, value));
        break;
      }
      case KuduColumnSchema::BOOL:
      case KuduColumnSchema::FLOAT:
      case KuduColumnSchema::DOUBLE:
      case KuduColumnSchema::DECIMAL:
      default:
        return Status::NotSupported(
            Substitute("unsupported type $0 for key column '$1': "
                       "is this tool out of date?",
                       KuduColumnSchema::DataTypeToString(type),
                       col_name));
    }
  }
  return Status::OK();
}

Status ModifyRangePartition(const RunnerContext& context, PartitionAction action) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& table_range_lower_bound = FindOrDie(context.required_args,
                                                    kTableRangeLowerBoundArg);
  const string& table_range_upper_bound = FindOrDie(context.required_args,
                                                    kTableRangeUpperBoundArg);

  const auto convert_bounds_type = [&] (const string& range_bound,
                                        const string& flags_range_bound_type,
                                        KuduTableCreator::RangePartitionBound* range_bound_type)
      -> Status {
    string inclusive_bound = boost::iequals(flags_range_bound_type, "INCLUSIVE_BOUND") ?
        "INCLUSIVE_BOUND" : "";
    string exclusive_bound = boost::iequals(flags_range_bound_type, "EXCLUSIVE_BOUND") ?
        "EXCLUSIVE_BOUND" : "";

    if (inclusive_bound.empty() && exclusive_bound.empty()) {
      return Status::InvalidArgument(Substitute(
          "wrong type of range $0 : only 'INCLUSIVE_BOUND' or "
          "'EXCLUSIVE_BOUND' can be received", range_bound));

    }
    *range_bound_type = exclusive_bound.empty() ? KuduTableCreator::INCLUSIVE_BOUND :
        KuduTableCreator::EXCLUSIVE_BOUND;

    return Status::OK();
  };

  client::sp::shared_ptr<KuduClient> client;
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  RETURN_NOT_OK(client->OpenTable(table_name, &table));
  const auto& schema = table->schema();

  unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
  unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());

  RETURN_NOT_OK(ConvertToKuduPartialRow(table, table_range_lower_bound, lower_bound.get()));
  RETURN_NOT_OK(ConvertToKuduPartialRow(table, table_range_upper_bound, upper_bound.get()));

  KuduTableCreator::RangePartitionBound lower_bound_type;
  KuduTableCreator::RangePartitionBound upper_bound_type;

  RETURN_NOT_OK(convert_bounds_type("lower bound", FLAGS_lower_bound_type, &lower_bound_type));
  RETURN_NOT_OK(convert_bounds_type("upper bound", FLAGS_upper_bound_type, &upper_bound_type));

  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  if (action == PartitionAction::ADD) {
    return alterer->AddRangePartition(lower_bound.release(),
                                      upper_bound.release(),
                                      lower_bound_type,
                                      upper_bound_type)->Alter();
  }
  DCHECK_EQ(PartitionAction::DROP, action);
  return alterer->DropRangePartition(lower_bound.release(),
                                     upper_bound.release(),
                                     lower_bound_type,
                                     upper_bound_type)->Alter();
}

Status DropRangePartition(const RunnerContext& context) {
  return ModifyRangePartition(context, PartitionAction::DROP);
}

Status AddRangePartition(const RunnerContext& context) {
  return ModifyRangePartition(context, PartitionAction::ADD);
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
      .AddOptionalParameter("show_attributes")
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

  unique_ptr<Action> scan_table =
      ActionBuilder("scan", &ScanTable)
      .Description("Scan rows from a table")
      .ExtraDescription("Scan rows from an existing table. See the help "
                        "for the --predicates flag on how predicates can be specified.")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to scan"})
      .AddOptionalParameter("columns")
      .AddOptionalParameter("fill_cache")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("tablets")
      .Build();

  unique_ptr<Action> copy_table =
      ActionBuilder("copy", &CopyTable)
      .Description("Copy table data to another table")
      .ExtraDescription("Copy table data to another table; the two tables could be in the same "
                        "cluster or not. The two tables must have the same table schema, but "
                        "could have different partition schemas. Alternatively, the tool can "
                        "create the new table using the same table and partition schema as the "
                        "source table.")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the source table" })
      .AddRequiredParameter({ kDestMasterAddressesArg, kDestMasterAddressesArgDesc })
      .AddOptionalParameter("create_table")
      .AddOptionalParameter("dst_table")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("tablets")
      .AddOptionalParameter("write_type")
      .Build();

  unique_ptr<Action> set_extra_config =
      ActionBuilder("set_extra_config", &SetExtraConfig)
      .Description("Change a extra configuration value on a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kConfigNameArg, "Name of the configuration" })
      .AddRequiredParameter({ kConfigValueArg, "New value for the configuration" })
      .Build();

  unique_ptr<Action> get_extra_configs =
      ActionBuilder("get_extra_configs", &GetExtraConfigs)
      .Description("Get the extra configuration properties for a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg,
                              "Name of the table for which to get extra configurations" })
      .AddOptionalParameter("config_names")
      .Build();
  unique_ptr<Action> drop_range_partition =
      ActionBuilder("drop_range_partition", &DropRangePartition)
      .Description("Drop a range partition of table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table" })
      .AddRequiredParameter({ kTableRangeLowerBoundArg,
                              "String representation of lower bound of "
                              "the table range partition as a JSON array" })
      .AddRequiredParameter({ kTableRangeUpperBoundArg,
                              "String representation of upper bound of "
                              "the table range partition as a JSON array" })
      .AddOptionalParameter("lower_bound_type")
      .AddOptionalParameter("upper_bound_type")
      .Build();

  unique_ptr<Action> add_range_partition =
      ActionBuilder("add_range_partition", &AddRangePartition)
      .Description("Add a range partition for table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table" })
      .AddRequiredParameter({ kTableRangeLowerBoundArg,
                              "String representation of lower bound of "
                              "the table range partition as a JSON array."
                              "If the parameter is an empty array, "
                              "the lower range partition will be unbounded" })
      .AddRequiredParameter({ kTableRangeUpperBoundArg,
                              "String representation of upper bound of "
                              "the table range partition as a JSON array."
                              "If the parameter is an empty array, "
                              "the upper range partition will be unbounded" })
      .AddOptionalParameter("lower_bound_type")
      .AddOptionalParameter("upper_bound_type")
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(add_range_partition))
      .AddAction(std::move(delete_table))
      .AddAction(std::move(describe_table))
      .AddAction(std::move(drop_range_partition))
      .AddAction(std::move(list_tables))
      .AddAction(std::move(locate_row))
      .AddAction(std::move(rename_column))
      .AddAction(std::move(rename_table))
      .AddAction(std::move(scan_table))
      .AddAction(std::move(copy_table))
      .AddAction(std::move(set_extra_config))
      .AddAction(std::move(get_extra_configs))
      .Build();
}

} // namespace tools
} // namespace kudu

