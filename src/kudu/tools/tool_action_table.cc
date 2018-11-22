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
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/scan_batch.h"
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
#include "kudu/gutil/strtoint.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/atomic.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/string_case.h"

DECLARE_string(tables);
DEFINE_bool(check_row_existence, false,
            "Also check for the existence of the row on the leader replica of "
            "the tablet. If found, the full row will be printed; if not found, "
            "an error message will be printed and the command will return a "
            "non-zero status.");
DECLARE_string(columns);
DEFINE_bool(list_tablets, false,
            "Include tablet and replica UUIDs in the output");
DEFINE_bool(modify_external_catalogs, true,
            "Whether to modify external catalogs, such as the Hive Metastore, "
            "when renaming or dropping a table.");
DECLARE_int32(num_threads);
DEFINE_string(predicates, "",
              "Query predicates on columns, support three types of predicates, "
              "include 'Comparison', 'InList' and 'WhetherNull'."
              " *The 'Comparison' type support <=, <, ==, > and >=, "
              "    which can be represented by one character '[', '(', '=', ')' or ']'"
              " *The 'InList' type means values are in certain list, "
              "    which can be represented by one character '@'"
              " *The 'WhetherNull' type means whether the value is a NULL or not, "
              "    which can be represented by one character 'i'(is) or '!'(is not)"
              "One predicate entry can be represented as <column name>:<predicate type>:<value(s)>,"
              "  e.g. 'col1:[:lower;col1:]:upper;col2:@:v1,v2,v3;col3:!:NULL'");
DEFINE_int64(scan_count, 0,
             "Count limit for scan rows, less than or equal to 0 mean no limit.");
DEFINE_bool(show_value, false,
            "Whether to show values of scanned rows.");
DECLARE_string(tables);
DECLARE_string(tablets);

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduError;
using client::KuduPredicate;
using client::KuduScanBatch;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduValue;
using client::internal::ReplicaController;
using client::sp::shared_ptr;
using std::cerr;
using std::cout;
using std::endl;
using std::map;
using std::set;
using std::string;
using std::thread;
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
    shared_ptr<KuduClient> client;
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
      shared_ptr<KuduTable> client_table;
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

AtomicInt<uint64_t> total_count(0);
AtomicInt<int32_t> worker_count(0);

Status CreateKuduClient(const RunnerContext& context,
                        shared_ptr<KuduClient>* client) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  return KuduClientBuilder()
             .master_server_addrs(master_addresses)
             .Build(client);
}

Status DeleteTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  shared_ptr<KuduClient> client;
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

  shared_ptr<KuduClient> client;
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

  shared_ptr<KuduClient> client;
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

void MonitorThread() {
  MonoTime last_log_time = MonoTime::Now();
  while (worker_count.Load() > 0) {
    if (MonoTime::Now() - last_log_time >= MonoDelta::FromSeconds(5)) {
      LOG(INFO) << "Scanned count: " << total_count.Load() << endl;
      last_log_time = MonoTime::Now();
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
}

KuduValue* ParseValue(KuduColumnSchema::DataType type,
                      const string& str_value) {
  switch (type) {
    case KuduColumnSchema::DataType::INT8:
    case KuduColumnSchema::DataType::INT16:
    case KuduColumnSchema::DataType::INT32:
    case KuduColumnSchema::DataType::INT64:
      if (!str_value.empty()) {
        return KuduValue::FromInt(atoi64(str_value));
      }
      break;
    case KuduColumnSchema::DataType::STRING:
      if (!str_value.empty()) {
        return KuduValue::CopyString(str_value);
      }
      break;
    case KuduColumnSchema::DataType::FLOAT:
    case KuduColumnSchema::DataType::DOUBLE:
      if (!str_value.empty()) {
        return KuduValue::FromDouble(strtod(str_value.c_str(), nullptr));
      }
      break;
    default:
      CHECK(false) << Substitute("Unhandled type $0", type);
  }

  return nullptr;
}

Status NewComparisonPredicate(const shared_ptr<KuduTable>& table,
                              const string& name,
                              KuduColumnSchema::DataType type,
                              char op,
                              const string& value,
                              KuduPredicate** predicate) {
  KuduValue* lower = ParseValue(type, value);
  client::KuduPredicate::ComparisonOp cop;
  switch (op) {
    case '[':
      cop = client::KuduPredicate::ComparisonOp::GREATER_EQUAL;
      break;
    case '(':
      cop = client::KuduPredicate::ComparisonOp::GREATER;
      break;
    case '=':
      cop = client::KuduPredicate::ComparisonOp::EQUAL;
      break;
    case ')':
      cop = client::KuduPredicate::ComparisonOp::LESS;
      break;
    case ']':
      cop = client::KuduPredicate::ComparisonOp::LESS_EQUAL;
      break;
    default:
      return Status::InvalidArgument(Substitute("invalid op: $0", op));
  }
  *predicate = table->NewComparisonPredicate(name, cop, lower);

  return Status::OK();
}

Status NewInPredicate(const shared_ptr<KuduTable>& table,
                      const string& name,
                      KuduColumnSchema::DataType type,
                      char op,
                      const string& value,
                      KuduPredicate** predicate) {
  switch (op) {
    case '@': {
      std::vector<KuduValue *> values;
      vector<string> str_values = Split(value, ",", strings::SkipEmpty());
      for (const auto& str_value : str_values) {
        values.emplace_back(ParseValue(type, str_value));
      }
      *predicate = table->NewInListPredicate(name, &values);
      break;
    }
    default:
      return Status::InvalidArgument(Substitute("invalid op: $0", op));
  }

  return Status::OK();
}

Status NewNullPredicate(const shared_ptr<KuduTable>& table,
                        const string& name,
                        char op,
                        const string& value,
                        KuduPredicate** predicate) {
  std::string value_upper;
  ToUpperCase(value, &value_upper);
  if (value_upper != "NULL") {
    return Status::OK();
  }

  switch (op) {
    case 'i':
      *predicate = table->NewIsNullPredicate(name);
      break;
    case '!':
      *predicate = table->NewIsNotNullPredicate(name);
      break;
    default:
      return Status::InvalidArgument(Substitute("invalid op: $0", op));
  }

  return Status::OK();
}

enum class PredicateType {
  Invalid = 0,
  Comparison,
  InList,
  WhetherNull
};

PredicateType ParsePredicateType(const string& op) {
  if (op.size() != 1) {
    return PredicateType::Invalid;
  }

  switch (op[0]) {
    case '[':
    case '(':
    case '=':
    case ')':
    case ']':
      return PredicateType::Comparison;
    case '@':
      return PredicateType::InList;
    case 'i':
    case '!':
      return PredicateType::WhetherNull;
    default:
      return PredicateType::Invalid;
  }

  return PredicateType::Invalid;
}

Status AddPredicate(const shared_ptr<KuduTable>& table,
                    const string& name,
                    const string& op,
                    const string& value,
                    KuduScanTokenBuilder& builder) {
  if (name.empty() || op.empty()) {
    return Status::OK();
  }

  for (size_t i = 0; i < table->schema().num_columns(); ++i) {
    if (table->schema().Column(i).name() == name) {
      auto type = table->schema().Column(i).type();
      KuduPredicate* predicate = nullptr;
      PredicateType pt = ParsePredicateType(op);
      switch (pt) {
        case PredicateType::Comparison:
          RETURN_NOT_OK(NewComparisonPredicate(table, name, type, op[0], value, &predicate));
          break;
        case PredicateType::InList:
          RETURN_NOT_OK(NewInPredicate(table, name, type, op[0], value, &predicate));
          break;
        case PredicateType::WhetherNull:
          RETURN_NOT_OK(NewNullPredicate(table, name, op[0], value, &predicate));
          break;
        default:
          return Status::InvalidArgument("Invalid op: $1", op);
      }
      RETURN_NOT_OK(builder.AddConjunctPredicate(predicate));

      return Status::OK();
    }
  }

  return Status::OK();
}

Status AddPredicates(const shared_ptr<KuduTable>& table,
                     const string& predicates,
                     KuduScanTokenBuilder& builder) {
  vector<string> column_predicates = Split(predicates, ";", strings::SkipWhitespace());
  for (const auto& column_predicate : column_predicates) {
    vector<string> name_op_value = Split(column_predicate, ":", strings::SkipWhitespace());
    if (name_op_value.size() == 3) {
      RETURN_NOT_OK(AddPredicate(table, name_op_value[0], name_op_value[1], name_op_value[2], builder));
    }
  }

  return Status::OK();
}

void ScannerThread(const vector<KuduScanToken*>& tokens) {
  for (auto token : tokens) {
    Stopwatch sw(Stopwatch::THIS_THREAD);
    sw.start();

    KuduScanner *scanner_ptr;
    DCHECK_OK(token->IntoKuduScanner(&scanner_ptr));
    unique_ptr<KuduScanner> scanner(scanner_ptr);
    DCHECK_OK(scanner->Open());

    int count = 0;
    while (scanner->HasMoreRows()) {
      KuduScanBatch batch;
      DCHECK_OK(scanner->NextBatch(&batch));
      count += batch.NumRows();
      if (FLAGS_show_value) {
        for (auto it = batch.begin(); it != batch.end(); ++it) {
          KuduScanBatch::RowPtr row(*it);
          LOG(INFO) << row.ToString() << endl;
        }
      }
      total_count.IncrementBy(batch.NumRows());
      if (total_count.Load() >= FLAGS_scan_count && FLAGS_scan_count > 0) {   // TODO maybe larger than FLAGS_scan_count
        LOG(INFO) << "Scanned count(maybe not the total count in specified range): " << count << endl;
        return;
      }
    }
    sw.stop();
    LOG(INFO) << "T " << token->tablet().id() << " scanned count " << count
    << " cost " << sw.elapsed().wall_seconds() << " seconds" << endl;
  }
}

Status ScanRows(const shared_ptr<KuduTable>& table, const string& predicates, const string& columns) {
  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.SetCacheBlocks(false));
  RETURN_NOT_OK(builder.SetTimeoutMillis(30000));
  RETURN_NOT_OK(builder.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(builder.SetReadMode(KuduScanner::READ_LATEST));
  vector<string> projected_column_names = Split(columns, ",", strings::SkipWhitespace());
  if (!projected_column_names.empty()) {
    RETURN_NOT_OK(builder.SetProjectedColumnNames(projected_column_names));
  }
  RETURN_NOT_OK(AddPredicates(table, predicates, builder));
  const set<string>& tablet_id_filters = Split(FLAGS_tablets, ",", strings::SkipEmpty());

  vector<KuduScanToken*> tokens;
  ElementDeleter DeleteTable(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto token : tokens) {
    if (tablet_id_filters.empty() || ContainsKey(tablet_id_filters, token->tablet().id())) {
      thread_tokens[i++ % FLAGS_num_threads].push_back(token);
    }
  }

  worker_count.Store(FLAGS_num_threads);
  vector<thread> threads;
  Stopwatch sw(Stopwatch::THIS_THREAD);
  sw.start();
  for (i = 0; i < FLAGS_num_threads; ++i) {
      threads.emplace_back(&ScannerThread, thread_tokens[i]);
  }
  threads.emplace_back(&MonitorThread);

  for (auto& t : threads) {
    t.join();
    worker_count.IncrementBy(-1);
  }

  sw.stop();
  LOG(INFO) << "Total count " << total_count.Load() << " cost " << sw.elapsed().wall_seconds() << " seconds";

  return Status::OK();
}

Status ScanTable(const RunnerContext &context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  RETURN_NOT_OK(ScanRows(table, FLAGS_predicates, FLAGS_columns));

  return Status::OK();
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

  unique_ptr<Action> scan_table =
      ActionBuilder("scan", &ScanTable)
      .Description("Scan rows from a table")
      .ExtraDescription(
          "Scan rows from an exist table, you can specify "
          "one column's lower and upper bounds.")
      .AddRequiredParameter({ kMasterAddressesArg,
          "Comma-separated list of master addresses to run against. "
          "Addresses are in 'hostname:port' form where port may be omitted "
          "if a master server listens at the default port." })
      .AddRequiredParameter({ kTableNameArg,
          "Key column name of the existing table, which will be used "
          "to limit the lower and upper bounds when scan rows."})
      .AddOptionalParameter("tablets")
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("columns")
      .AddOptionalParameter("scan_count")
      .AddOptionalParameter("show_value")
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(delete_table))
      .AddAction(std::move(describe_table))
      .AddAction(std::move(list_tables))
      .AddAction(std::move(locate_row))
      .AddAction(std::move(rename_column))
      .AddAction(std::move(rename_table))
      .AddAction(std::move(scan_table))
      .Build();
}

} // namespace tools
} // namespace kudu

