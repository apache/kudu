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
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/status.h>
#include <google/protobuf/stubs/stringpiece.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/value.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/table_scanner.h"
#include "kudu/tools/tool.pb.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

using google::protobuf::util::JsonStringToMessage;
using google::protobuf::util::JsonParseOptions;
using google::protobuf::RepeatedPtrField;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduColumnSpec;
using kudu::client::KuduColumnStorageAttributes;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTableStatistics;
using kudu::client::KuduValue;
using kudu::client::internal::ReplicaController;
using std::cerr;
using std::cout;
using std::endl;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::unique_ptr;
using std::unordered_map;
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
DEFINE_bool(show_table_info, false,
            "Include extra information such as number of tablets, replicas, "
            "and live row count for a table in the output");
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
DEFINE_int32(scan_batch_size, -1,
             "The size for scan results batches, in bytes. A negative value "
             "means the server-side default is used, where the server-side "
             "default is controlled by the tablet server's "
             "--scanner_default_batch_size_bytes flag.");
DEFINE_bool(show_avro_format_schema, false,
            "Display the table schema in avro format. When enabled it only outputs the "
            "table schema in Avro format without any other information like "
            "partition/owner/comments. It cannot be used in conjunction with other flags");

DECLARE_bool(create_table);
DECLARE_int32(create_table_replication_factor);
DECLARE_bool(row_count_only);
DECLARE_bool(show_scanner_stats);

DEFINE_string(encoding_type, "AUTO_ENCODING",
              "Type of encoding for the column including AUTO_ENCODING, PLAIN_ENCODING, "
              "PREFIX_ENCODING, RLE, DICT_ENCODING, BIT_SHUFFLE, GROUP_VARINT");
DEFINE_string(compression_type, "DEFAULT_COMPRESSION",
              "Type of compression for the column including DEFAULT_COMPRESSION, "
              "NO_COMPRESSION, SNAPPY, LZ4, ZLIB");
DEFINE_string(default_value, "", "Default value for this column.");
DEFINE_string(comment, "", "Comment for this column.");

DECLARE_bool(show_values);
DECLARE_string(replica_selection);
DECLARE_string(tables);

bool ValidateCreateTable() {
  if (!FLAGS_create_table && FLAGS_create_table_replication_factor != -1) {
    LOG(ERROR) << Substitute("--create_table_replication_factor is meaningless "
                             "when --create_table=false");
    return false;
  }
  return true;
}

GROUP_FLAG_VALIDATOR(create_table, ValidateCreateTable);

namespace kudu {
namespace tools {

// This class only exists so that ListTables() can easily be friended by
// KuduReplica, KuduReplica::Data, and KuduClientBuilder.
class TableLister {
 public:
  static Status ListTablets(const vector<string>& master_addresses) {
    client::sp::shared_ptr<KuduClient> client;
    RETURN_NOT_OK(CreateKuduClient(master_addresses,
                                   &client,
                                   true /* can_see_all_replicas */));
    vector<kudu::client::KuduClient::Data::TableInfo> tables_info;
    RETURN_NOT_OK(client->data_->ListTablesWithInfo(
        client.get(), &tables_info, "" /* filter */));

    vector<string> table_filters = Split(FLAGS_tables, ",", strings::SkipEmpty());
    for (const auto& tinfo : tables_info) {
      const auto& tname = tinfo.table_name;
      if (!MatchesAnyPattern(table_filters, tname)) continue;
      if (FLAGS_show_table_info) {
        cout << tname << " " << "num_tablets:" << tinfo.num_tablets
             << " num_replicas:" << tinfo.num_replicas
             << " live_row_count:" << tinfo.live_row_count << endl;
      } else {
        cout << tname << endl;
      }
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

// This class only exists so that it can easily be friended by KuduTableAlterer.
class TableAlter {
 public:
  static Status SetReplicationFactor(const vector<string>& master_addresses,
                                     const string& table_name,
                                     int32_t replication_factor) {
    client::sp::shared_ptr<KuduClient> client;
    RETURN_NOT_OK(CreateKuduClient(master_addresses, &client));
    unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
    alterer->data_->set_replication_factor_to_ = replication_factor;
    return alterer->Alter();
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
const char* const kDefaultValueArg = "default_value";
const char* const kCompressionTypeArg = "compression_type";
const char* const kEncodingTypeArg = "encoding_type";
const char* const kBlockSizeArg = "block_size";
const char* const kColumnCommentArg = "column_comment";
const char* const kCreateTableJSONArg = "create_table_json";
const char* const kReplicationFactorArg = "replication_factor";
const char* const kDataTypeArg = "data_type";

enum PartitionAction {
  ADD,
  DROP,
};

Status AddLogicalType(JsonWriter* writer, const string& type, const string& logical_type,
                      const ColumnSchema& col_schema) {
  writer->StartArray();
  writer->StartObject();
  writer->String("type");
  writer->String(type);
  writer->String("logicalType");
  writer->String(logical_type);
  writer->EndObject();
  writer->EndArray();
  if (col_schema.has_read_default()) {
    writer->String("default");
    writer->String(col_schema.Stringify(col_schema.read_default_value()));
  }
  return Status::OK();
}

Status AddPrimitiveType(const ColumnSchema& col_schema, const string& type, JsonWriter* writer) {
  if (col_schema.is_nullable()) {
    writer->StartArray();
    writer->String("null");
    writer->String(type);
    writer->EndArray();
  } else {
    writer->String(type);
  }
  if (col_schema.has_read_default()) {
    writer->String("default");
    writer->String(col_schema.Stringify(col_schema.read_default_value()));
  }
  return Status::OK();
}

Status PopulateAvroSchema(const string& table_name,
                          const string& cluster_id,
                          const KuduSchema& kudu_schema) {
  std::ostringstream out;
  JsonWriter writer(&out, JsonWriter::Mode::PRETTY);
  // Start writing in Json format
  writer.StartObject();
  vector<string> json_attributes = {"type", "table", "name", table_name,
                                    "namespace", "kudu.cluster." + cluster_id, "fields"};
  for (const string& json: json_attributes) {
    writer.String(json);
  }
  writer.StartArray();
  const Schema schema = kudu::client::KuduSchema::ToSchema(kudu_schema);
  // Each column type is a nested field
  for (int i = 0; i < schema.num_columns(); i++) {
    writer.StartObject();
    writer.String("name");
    writer.String(kudu_schema.Column(i).name());
    writer.String("type");
    switch (kudu_schema.Column(i).type()) {
      case kudu::client::KuduColumnSchema::INT8:
      case kudu::client::KuduColumnSchema::INT16:
      case kudu::client::KuduColumnSchema::INT32:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "int", &writer));
        break;
      case kudu::client::KuduColumnSchema::INT64:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "long", &writer));
        break;
      case kudu::client::KuduColumnSchema::STRING:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "string", &writer));
        break;
      case kudu::client::KuduColumnSchema::BOOL:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "bool", &writer));
        break;
      case kudu::client::KuduColumnSchema::FLOAT:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "float", &writer));
        break;
      case kudu::client::KuduColumnSchema::DOUBLE:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "double", &writer));
        break;
      case kudu::client::KuduColumnSchema::BINARY:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "bytes", &writer));
        break;
      case kudu::client::KuduColumnSchema::VARCHAR:
        RETURN_NOT_OK(AddPrimitiveType(schema.column(i), "string", &writer));
        break;
      // Each logical type in avro schema has sub-nested fields
      case kudu::client::KuduColumnSchema::UNIXTIME_MICROS:
        RETURN_NOT_OK(AddLogicalType(&writer, "long", "time-micros", schema.column(i)));
        break;
      case kudu::client::KuduColumnSchema::DATE:
        RETURN_NOT_OK(AddLogicalType(&writer, "int", "date", schema.column(i)));
        break;
      case kudu::client::KuduColumnSchema::DECIMAL:
        RETURN_NOT_OK(AddLogicalType(&writer, "bytes", "decimal", schema.column(i)));
        break;
      default:
        LOG(DFATAL) << kudu_schema.Column(i).name() << ": Invalid column type";
    }
    writer.EndObject();
  }
  writer.EndArray();
  writer.EndObject();
  cout << out.str() << endl;
  return Status::OK();
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
  const KuduSchema& schema = table->schema();
  if (FLAGS_show_avro_format_schema) {
    return PopulateAvroSchema(FindOrDie(context.required_args, kTableNameArg),
                                         client->cluster_id(), schema);
  }
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
        partition_schema.RangePartitionDebugString(partition.begin().range_key(),
                                                   partition.end().range_key(),
                                                   schema_internal);
    partition_strs.emplace_back(std::move(range_partition_str));
  }
  cout << partition_schema.DisplayString(schema_internal, partition_strs)
       << endl;

  // The owner.
  cout << "OWNER " << table->owner() << endl;

  // Finally, the replication factor.
  cout << "REPLICAS " << table->num_replicas() << endl;

  // The comment.
  cout << "COMMENT " << table->comment() << endl;
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
      case KuduColumnSchema::DATE:
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
      case KuduColumnSchema::STRING:
      case KuduColumnSchema::VARCHAR: {
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

Status SetDiskSizeLimit(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& disk_size_limit_str = FindOrDie(context.required_args, "disk_size");
  int64_t disk_size_limit;
  if (iequals(disk_size_limit_str, "unlimited")) {
    disk_size_limit = -1;
  } else if (!safe_strto64(disk_size_limit_str, &disk_size_limit)) {
    return Status::InvalidArgument(Substitute(
        "Could not parse $0 as disk size limit", disk_size_limit_str));
  }
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->SetTableDiskSizeLimit(disk_size_limit);
  return alterer->Alter();
}

Status SetRowCountLimit(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& row_count_limit_str = FindOrDie(context.required_args, "row_count");
  int64_t row_count_limit;
  if (iequals(row_count_limit_str, "unlimited")) {
    row_count_limit = -1;
  } else if (!safe_strto64(row_count_limit_str, &row_count_limit)) {
    return Status::InvalidArgument(Substitute(
        "Could not parse $0 as row count limit", row_count_limit_str));
  }
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->SetTableRowCountLimit(row_count_limit);
  return alterer->Alter();
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

  if (!FLAGS_row_count_only) {
    FLAGS_show_values = true;
  }
  TableScanner scanner(client, table_name);
  scanner.SetOutput(&cout);
  scanner.SetScanBatchSize(FLAGS_scan_batch_size);
  const auto& replica_selection_str = FLAGS_replica_selection;
  if (!replica_selection_str.empty()) {
    RETURN_NOT_OK(scanner.SetReplicaSelection(replica_selection_str));
  }
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

Status ConvertToKuduPartialRow(
    const vector<pair<string, KuduColumnSchema::DataType>>& range_columns,
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
  if (values.size() != range_columns.size()) {
    return Status::InvalidArgument(
        Substitute("wrong number of range columns specified: expected $0 but received $1",
                   range_columns.size(),
                   values.size()));
  }

  for (int i = 0; i < values.size(); i++) {
    const auto& col_name = range_columns[i].first;
    const auto type = range_columns[i].second;
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
      case KuduColumnSchema::DATE: {
        int32_t value;
        RETURN_NOT_OK_PREPEND(
            reader.ExtractInt32(values[i], /*field=*/nullptr, &value),
            error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetDate(col_name, value));
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
      case KuduColumnSchema::VARCHAR: {
        string value;
        RETURN_NOT_OK_PREPEND(
          reader.ExtractString(values[i], /*field=*/nullptr, &value),
          error_msg);
        RETURN_NOT_OK(range_bound_partial_row->SetVarchar(col_name, value));
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
                                        KuduTableCreator::RangePartitionBound* range_bound_type) {
    string inclusive_bound = iequals(flags_range_bound_type, "INCLUSIVE_BOUND") ?
        "INCLUSIVE_BOUND" : "";
    string exclusive_bound = iequals(flags_range_bound_type, "EXCLUSIVE_BOUND") ?
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

  vector<pair<string, KuduColumnSchema::DataType>> range_col_names_and_types;
  const Schema& schema_tmp = KuduSchema::ToSchema(schema);
  const auto& partition_schema = table->partition_schema();
  vector<int32_t> key_indexes;
  RETURN_NOT_OK(partition_schema.GetRangeSchemaColumnIndexes(schema_tmp, &key_indexes));
  for (int i = 0; i < key_indexes.size(); i++) {
    const auto key_index = key_indexes[i];
    const auto& column = schema.Column(key_index);
    range_col_names_and_types.emplace_back(std::make_pair(column.name(),
                                           column.type()));
  }
  RETURN_NOT_OK(ConvertToKuduPartialRow(range_col_names_and_types,
                                        table_range_lower_bound,
                                        lower_bound.get()));
  RETURN_NOT_OK(ConvertToKuduPartialRow(range_col_names_and_types,
                                        table_range_upper_bound,
                                        upper_bound.get()));

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

Status ParseValueOfType(const string& default_value,
                        KuduColumnSchema::DataType type,
                        KuduValue** value) {
  JsonReader reader(default_value);
  RETURN_NOT_OK(reader.Init());
  vector<const rapidjson::Value*> values;
  RETURN_NOT_OK(reader.ExtractObjectArray(reader.root(),
                                          /*field=*/nullptr,
                                          &values));
  if (values.size() != 1) {
    return Status::InvalidArgument(Substitute(
      "We got $0 value(s), you should provide one default value.",
      std::to_string(values.size())));
  }

  string msg = Substitute("unable to parse value for column type $0",
                          KuduColumnSchema::DataTypeToString(type));
  switch (type) {
    case KuduColumnSchema::DataType::INT8:
    case KuduColumnSchema::DataType::INT16:
    case KuduColumnSchema::DataType::INT32:
    case KuduColumnSchema::DataType::INT64:
    case KuduColumnSchema::DataType::DATE:
    case KuduColumnSchema::DataType::UNIXTIME_MICROS: {
      int64_t int_value;
      RETURN_NOT_OK_PREPEND(
          reader.ExtractInt64(values[0], /*field=*/nullptr, &int_value), msg);
      *value = KuduValue::FromInt(int_value);
      break;
    }
    case KuduColumnSchema::DataType::BINARY:
    case KuduColumnSchema::DataType::STRING:
    case KuduColumnSchema::DataType::VARCHAR: {
      string str_value;
      RETURN_NOT_OK_PREPEND(
        reader.ExtractString(values[0], /*field=*/nullptr, &str_value), msg);
      *value = KuduValue::CopyString(str_value);
      break;
    }
    case KuduColumnSchema::DataType::BOOL: {
      bool bool_value;
      RETURN_NOT_OK_PREPEND(
        reader.ExtractBool(values[0], /*field=*/nullptr, &bool_value), msg);
      *value = KuduValue::FromBool(bool_value);
      break;
    }
    case KuduColumnSchema::DataType::FLOAT: {
      float float_value;
      RETURN_NOT_OK_PREPEND(
        reader.ExtractFloat(values[0], /*field=*/nullptr, &float_value), msg);
      *value = KuduValue::FromFloat(float_value);
      break;
    }
    case KuduColumnSchema::DataType::DOUBLE: {
      double double_value;
      RETURN_NOT_OK_PREPEND(
        reader.ExtractDouble(values[0], /*field=*/nullptr, &double_value), msg);
      *value = KuduValue::FromDouble(double_value);
      break;
    }
    case KuduColumnSchema::DataType::DECIMAL:
    default:
      return Status::NotSupported(Substitute(
        "$0 columns are not supported for setting default value by this tool, "
        "is this tool out of date?",
        KuduColumnSchema::DataTypeToString(type)));
  }
  return Status::OK();
}

Status ColumnSetDefault(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& default_value = FindOrDie(context.required_args, kDefaultValueArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  KuduSchema schema;
  RETURN_NOT_OK(client->GetTableSchema(table_name, &schema));

  // Here we use the first column to initialize an object of KuduColumnSchema
  // for there is no default constructor for it.
  KuduColumnSchema col_schema = schema.Column(0);
  if (!schema.HasColumn(column_name, &col_schema)) {
    return Status::NotFound(Substitute("Couldn't find column $0", column_name));
  }

  KuduValue* value = nullptr;
  RETURN_NOT_OK(ParseValueOfType(default_value, col_schema.type(), &value));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->Default(value);
  return alterer->Alter();
}

Status ColumnRemoveDefault(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->RemoveDefault();
  return alterer->Alter();
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
Status ColumnSetCompression(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& compression_type_arg = FindOrDie(context.required_args, kCompressionTypeArg);
  KuduColumnStorageAttributes::CompressionType compression_type;
  RETURN_NOT_OK(KuduColumnStorageAttributes::StringToCompressionType(
      compression_type_arg, &compression_type));
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->Compression(compression_type);
  return alterer->Alter();
}

Status ColumnSetEncoding(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& encoding_type_arg = FindOrDie(context.required_args, kEncodingTypeArg);
  KuduColumnStorageAttributes::EncodingType encoding_type;
  RETURN_NOT_OK(KuduColumnStorageAttributes::StringToEncodingType(
      encoding_type_arg, &encoding_type));
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->Encoding(encoding_type);
  return alterer->Alter();
}
#pragma GCC diagnostic pop

Status ColumnSetBlockSize(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& str_block_size = FindOrDie(context.required_args, kBlockSizeArg);

  int32_t block_size;
  if (!safe_strto32(str_block_size, &block_size)) {
    return Status::InvalidArgument(Substitute(
      "Unable to parse block_size value: $0.", str_block_size));
  }
  if (block_size <= 0) {
    return Status::InvalidArgument(Substitute(
      "Invalid block size: $0, it should be set higher than 0.", str_block_size));
  }

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->BlockSize(block_size);
  return alterer->Alter();
}

Status ClearComment(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->SetComment("");
  return alterer->Alter();
}

Status SetComment(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& table_comment = FindOrDie(context.required_args, kColumnCommentArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->SetComment(table_comment);
  return alterer->Alter();
}

Status ColumnSetComment(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& column_comment = FindOrDie(context.required_args, kColumnCommentArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->Comment(column_comment);
  return alterer->Alter();
}

Status AddColumn(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& data_type_name = FindOrDie(context.required_args, kDataTypeArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  KuduColumnSpec* column_spec = alterer->AddColumn(column_name);

  KuduColumnSchema::DataType data_type;
  RETURN_NOT_OK(KuduColumnSchema::StringToDataType(data_type_name, &data_type));
  column_spec->Type(data_type);

  if (!FLAGS_default_value.empty()) {
    KuduValue* value = nullptr;
    RETURN_NOT_OK(ParseValueOfType(FLAGS_default_value, data_type, &value));
    column_spec->Default(value);
  }

  if (!FLAGS_encoding_type.empty()) {
    KuduColumnStorageAttributes::EncodingType encoding_type;
    RETURN_NOT_OK(KuduColumnStorageAttributes::StringToEncodingType(FLAGS_encoding_type,
                                                                    &encoding_type));
    column_spec->Encoding(encoding_type);
  }

  if (!FLAGS_compression_type.empty()) {
    KuduColumnStorageAttributes::CompressionType compress_type;
    RETURN_NOT_OK(KuduColumnStorageAttributes::StringToCompressionType(FLAGS_compression_type,
                                                                       &compress_type));
    column_spec->Compression(compress_type);
  }

  if (!FLAGS_comment.empty()) {
    column_spec->Comment(FLAGS_comment);
  }

  return alterer->Alter();
}

Status DeleteColumn(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->DropColumn(column_name);
  return alterer->Alter();
}

Status SetReplicationFactor(const RunnerContext& context) {
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& str_replication_factor = FindOrDie(context.required_args, kReplicationFactorArg);

  int32_t replication_factor;
  if (!safe_strto32(str_replication_factor, &replication_factor)) {
    return Status::InvalidArgument(Substitute(
        "Unable to parse replication factor value: $0.", str_replication_factor));
  }

  return TableAlter::SetReplicationFactor(master_addresses, table_name, replication_factor);
}

Status GetTableStatistics(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  unique_ptr<KuduTableStatistics> statistics;
  KuduTableStatistics *table_statistics;
  RETURN_NOT_OK_PREPEND(client->GetTableStatistics(table_name, &table_statistics),
                        "failed to get table statistics.");
  statistics.reset(table_statistics);
  cout << "TABLE " << table_name << endl;
  cout << statistics->ToString() << endl;

  return Status::OK();
}

Status ToJsonPartialRow(const RepeatedPtrField<string>& values,
                      const vector<pair<string, KuduColumnSchema::DataType>>& range_columns,
                      string* json_value) {
  json_value->clear();
  if (values.empty() || values.size() != range_columns.size()) {
    return Status::InvalidArgument(Substitute(
        "Invalid range value size, value size should be equal to number of range keys."));
  }
  int i = 0;
  string joined = JoinMapped(values, [&](const string& v) {
    auto data_type = range_columns[i++].second;
    if (data_type == KuduColumnSchema::STRING ||
        data_type == KuduColumnSchema::VARCHAR ||
        data_type == KuduColumnSchema::BINARY) {
      return "\"" + v + "\"";
    }
    return v;
  }, ",");
  *json_value = "[" + joined + "]";
  return Status::OK();
}

Status ToClientEncodingType(
    ColumnPB::EncodingType type_pb,
    KuduColumnStorageAttributes::EncodingType* type) {
  Status s;
  switch (type_pb) {
    case ColumnPB::AUTO_ENCODING :
      *type = KuduColumnStorageAttributes::AUTO_ENCODING;
      break;
    case ColumnPB::PLAIN_ENCODING :
      *type = KuduColumnStorageAttributes::PLAIN_ENCODING;
      break;
    case ColumnPB::PREFIX_ENCODING :
      *type = KuduColumnStorageAttributes::PREFIX_ENCODING;
      break;
    case ColumnPB::DICT_ENCODING :
      *type = KuduColumnStorageAttributes::DICT_ENCODING;
      break;
    case ColumnPB::RLE :
      *type = KuduColumnStorageAttributes::RLE;
      break;
    case ColumnPB::BIT_SHUFFLE :
      *type = KuduColumnStorageAttributes::BIT_SHUFFLE;
      break;
    default :
      s = Status::InvalidArgument(Substitute("Unexpected encoding type: $0", type_pb));
  }
  return s;
}

Status ToClientCompressionType(
    ColumnPB::CompressionType type_pb,
    KuduColumnStorageAttributes::CompressionType* type) {
  Status s;
  switch (type_pb) {
    case ColumnPB::DEFAULT_COMPRESSION :
      *type = KuduColumnStorageAttributes::DEFAULT_COMPRESSION;
      break;
    case ColumnPB::NO_COMPRESSION :
      *type = KuduColumnStorageAttributes::NO_COMPRESSION;
      break;
    case ColumnPB::SNAPPY :
      *type = KuduColumnStorageAttributes::SNAPPY;
      break;
    case ColumnPB::LZ4 :
      *type = KuduColumnStorageAttributes::LZ4;
      break;
    case ColumnPB::ZLIB :
      *type = KuduColumnStorageAttributes::ZLIB;
      break;
    default :
      s = Status::InvalidArgument(Substitute("Unexpected compression type: $0", type_pb));
  }
  return s;
}

Status ToClientRangePartitionBound(
    PartitionPB_RangePartitionPB_BoundPB::Type type_pb,
    KuduTableCreator::RangePartitionBound* type) {
  Status s;
  switch (type_pb) {
    case PartitionPB_RangePartitionPB_BoundPB::EXCLUSIVE:
      *type = KuduTableCreator::EXCLUSIVE_BOUND;
      break;
    case PartitionPB_RangePartitionPB_BoundPB::INCLUSIVE:
      *type = KuduTableCreator::INCLUSIVE_BOUND;
      break;
    case PartitionPB_RangePartitionPB_BoundPB::UNKNOWN_BOUND :
    default:
      s = Status::InvalidArgument(Substitute("Unexpected range partition bound type: ", type_pb));
  }
  return s;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
Status ParseTableSchema(const SchemaPB& schema,
                        KuduSchema* kudu_schema) {
  KuduSchemaBuilder b;
  for (const auto& column : schema.columns()) {
    KuduColumnSpec* spec = b.AddColumn(column.column_name());
    KuduColumnSchema::DataType type;
    RETURN_NOT_OK(KuduColumnSchema::StringToDataType(
        column.column_type(), &type));
    spec->Type(type);
    if (column.has_type_attributes()) {
      if (type == KuduColumnSchema::DataType::DECIMAL) {
        spec->Precision(column.type_attributes().precision());
        spec->Scale(column.type_attributes().scale());
      } else if (type == KuduColumnSchema::DataType::VARCHAR) {
        spec->Length(column.type_attributes().length());
      }
    }
    if (!column.is_nullable()) {
      spec->NotNull();
    }
    if (column.has_default_value()) {
      KuduValue* value = nullptr;
      string default_v;
      if (column.column_type() == "STRING" ||
          column.column_type() == "BINARY" ||
          column.column_type() == "VARCHAR" ||
          column.column_type() == "DECIMAL") {
        default_v = "[\"" + column.default_value() + "\"]";
      } else {
        default_v = "[" + column.default_value() + "]";
      }
      RETURN_NOT_OK(ParseValueOfType(default_v, type, &value));
      spec->Default(value);
    }
    if (column.has_comment()) {
      spec->Comment(column.comment());
    }
    // If no valid encoding is provided, AUTO_ENCODING will be used by default.
    if (column.has_encoding()) {
      KuduColumnStorageAttributes::EncodingType type;
      RETURN_NOT_OK(ToClientEncodingType(column.encoding(), &type));
      spec->Encoding(type);
    }
    // If no valid compression is provided, DEFAULT_COMPRESSION will be used.
    if (column.has_compression()) {
      KuduColumnStorageAttributes::CompressionType type;
      RETURN_NOT_OK(ToClientCompressionType(column.compression(), &type));
      spec->Compression(type);
    }
  }

  b.SetPrimaryKey(vector<string>(schema.key_column_names().begin(),
                                 schema.key_column_names().end()));
  RETURN_NOT_OK(b.Build(kudu_schema));
  return Status::OK();
}
#pragma GCC diagnostic pop

Status ParseTablePartition(const PartitionPB& partition,
                           const KuduSchema& kudu_schema,
                           KuduTableCreator* table_creator) {
  for (const auto& hash_partition : partition.hash_partitions()) {
    vector<string> hash_keys;
    for (const auto& hk : hash_partition.columns()) {
      hash_keys.push_back(hk);
    }
    int32_t seed = 0;
    if (hash_partition.has_seed()) {
      seed = hash_partition.seed();
    }
    table_creator->add_hash_partitions(hash_keys, hash_partition.num_buckets(), seed);
  }
  // Generate and add the range partition splits for the table.
  if (!partition.has_range_partition()) {
    table_creator->set_range_partition_columns({});
    return Status::OK();
  }
  set<string> range_keys;
  vector<pair<string, KuduColumnSchema::DataType>> range_col_names_and_types;
  for (const auto& range_key : partition.range_partition().columns()) {
    range_keys.insert(range_key);
  }
  table_creator->set_range_partition_columns(
      vector<string>(range_keys.begin(), range_keys.end()));
  for (int idx = 0; idx < kudu_schema.num_columns(); ++idx) {
    // Find the range key type,
    KuduColumnSchema column = kudu_schema.Column(idx);
    if (ContainsKey(range_keys, column.name())) {
      range_col_names_and_types.emplace_back(
          std::make_pair(column.name(), column.type()));
    }
  }
  string bound_partial_row_json;
  for (const auto& bound : partition.range_partition().range_bounds()) {
    unique_ptr<KuduPartialRow> lower_bound(kudu_schema.NewRow());
    unique_ptr<KuduPartialRow> upper_bound(kudu_schema.NewRow());
    KuduTableCreator::RangePartitionBound lower_bound_type =
        KuduTableCreator::INCLUSIVE_BOUND;
    KuduTableCreator::RangePartitionBound upper_bound_type =
        KuduTableCreator::EXCLUSIVE_BOUND;
    if (bound.has_lower_bound()) {
      RETURN_NOT_OK(ToJsonPartialRow(bound.lower_bound().bound_values(),
                                     range_col_names_and_types,
                                     &bound_partial_row_json));
      RETURN_NOT_OK(ConvertToKuduPartialRow(range_col_names_and_types,
                                            bound_partial_row_json,
                                            lower_bound.get()));
      RETURN_NOT_OK(ToClientRangePartitionBound(bound.lower_bound().bound_type(),
                                                &lower_bound_type));
    }
    if (bound.has_upper_bound()) {
      RETURN_NOT_OK(ToJsonPartialRow(bound.upper_bound().bound_values(),
                                     range_col_names_and_types,
                                     &bound_partial_row_json));
      RETURN_NOT_OK(ConvertToKuduPartialRow(range_col_names_and_types,
                                            bound_partial_row_json,
                                            upper_bound.get()));
      RETURN_NOT_OK(ToClientRangePartitionBound(bound.upper_bound().bound_type(),
                                                &upper_bound_type));
    }
    table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
        lower_bound_type, upper_bound_type);
  }
  for (const auto& split_pb : partition.range_partition().range_splits()) {
    RETURN_NOT_OK(ToJsonPartialRow(split_pb.split_values(),
                                   range_col_names_and_types,
                                   &bound_partial_row_json));
    unique_ptr<KuduPartialRow> split(kudu_schema.NewRow());
    RETURN_NOT_OK(ConvertToKuduPartialRow(range_col_names_and_types,
                                          bound_partial_row_json,
                                          split.get()));
    table_creator->add_range_partition_split(split.release());
  }
  return Status::OK();
}

Status CreateTable(const RunnerContext& context) {
  const string& json_str = FindOrDie(context.required_args, kCreateTableJSONArg);
  CreateTablePB table_req;
  JsonParseOptions opts;
  opts.case_insensitive_enum_parsing = true;
  const auto& google_status = JsonStringToMessage(json_str, &table_req, opts);
  if (!google_status.ok()) {
    return Status::InvalidArgument(
        Substitute("unable to parse JSON: $0", json_str),
                   google_status.error_message().ToString());
  }

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  KuduSchema kudu_schema;
  RETURN_NOT_OK(ParseTableSchema(table_req.schema(), &kudu_schema));
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  table_creator->table_name(table_req.table_name())
                                     .schema(&kudu_schema);
  RETURN_NOT_OK(ParseTablePartition(table_req.partition(), kudu_schema, table_creator.get()));
  if (table_req.has_num_replicas()) {
    table_creator->num_replicas(table_req.num_replicas());
  }
  if (table_req.has_extra_configs()) {
    map<string, string> extra_configs(table_req.extra_configs().configs().begin(),
                                      table_req.extra_configs().configs().end());
    table_creator->extra_configs(extra_configs);
  }
  if (table_req.has_dimension_label()) {
    table_creator->dimension_label(table_req.dimension_label());
  }
  return table_creator->Create();
}


} // anonymous namespace

unique_ptr<Mode> BuildSetTableLimitMode() {
  unique_ptr<Action> set_disk_size_limit =
      ClusterActionBuilder("disk_size", &SetDiskSizeLimit)
      .Description("Set the disk size limit")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to set limit" })
      .AddRequiredParameter({ "disk_size",
                              "The disk size limit, 'unlimited' for no write limit" })
      .Build();
  unique_ptr<Action> set_row_count_limit =
      ClusterActionBuilder("row_count", &SetRowCountLimit)
      .Description("Set the row count limit")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to set limit" })
      .AddRequiredParameter({ "row_count",
                              "The row count limit, 'unlimited' for no write limit" })
      .Build();
  return ModeBuilder("set_limit")
      .Description("Set the write limit for a table")
      .AddAction(std::move(set_disk_size_limit))
      .AddAction(std::move(set_row_count_limit))
      .Build();
}

unique_ptr<Mode> BuildTableMode() {
  unique_ptr<Action> delete_table =
      ClusterActionBuilder("delete", &DeleteTable)
      .Description("Delete a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to delete" })
      .AddOptionalParameter("modify_external_catalogs")
      .Build();

  unique_ptr<Action> describe_table =
      ClusterActionBuilder("describe", &DescribeTable)
      .Description("Describe a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to describe" })
      .AddOptionalParameter("show_attributes")
      .AddOptionalParameter("show_avro_format_schema")
      .Build();

  unique_ptr<Action> list_tables =
      ClusterActionBuilder("list", &ListTables)
      .Description("List tables")
      .AddOptionalParameter("tables")
      .AddOptionalParameter("list_tablets")
      .AddOptionalParameter("show_table_info")
      .Build();

  unique_ptr<Action> locate_row =
      ClusterActionBuilder("locate_row", &LocateRow)
      .Description("Locate which tablet a row belongs to")
      .ExtraDescription("Provide the primary key as a JSON array of primary "
                        "key values, e.g. '[1, \"foo\", 2, \"bar\"]'. The "
                        "output will be the tablet id associated with the row "
                        "key. If there is no such tablet, an error message "
                        "will be printed and the command will return a "
                        "non-zero status")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to look up against" })
      .AddRequiredParameter({ kKeyArg,
                              "String representation of the row's primary key "
                              "as a JSON array" })
      .AddOptionalParameter("check_row_existence")
      .Build();

  unique_ptr<Action> rename_column =
      ClusterActionBuilder("rename_column", &RenameColumn)
      .Description("Rename a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to rename" })
      .AddRequiredParameter({ kNewColumnNameArg, "New column name" })
      .Build();

  unique_ptr<Action> rename_table =
      ClusterActionBuilder("rename_table", &RenameTable)
      .Description("Rename a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to rename" })
      .AddRequiredParameter({ kNewTableNameArg, "New table name" })
      .AddOptionalParameter("modify_external_catalogs")
      .Build();

  unique_ptr<Action> scan_table =
      ClusterActionBuilder("scan", &ScanTable)
      .Description("Scan rows from a table")
      .ExtraDescription("Scan rows from an existing table. See the help "
                        "for the --predicates flag on how predicates can be specified.")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to scan"})
      .AddOptionalParameter("columns")
      .AddOptionalParameter("row_count_only")
      .AddOptionalParameter("report_scanner_stats")
      .AddOptionalParameter("scan_batch_size")
      .AddOptionalParameter("fill_cache")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("tablets")
      .AddOptionalParameter("replica_selection")
      .Build();

  unique_ptr<Action> copy_table =
      ClusterActionBuilder("copy", &CopyTable)
      .Description("Copy table data to another table")
      .ExtraDescription("Copy table data to another table; the two tables could be in the same "
                        "cluster or not. The two tables must have the same table schema, but "
                        "could have different partition schemas. Alternatively, the tool can "
                        "create the new table using the same table and partition schema as the "
                        "source table.")
      .AddRequiredParameter({ kTableNameArg, "Name of the source table" })
      .AddRequiredParameter({ kDestMasterAddressesArg, kDestMasterAddressesArgDesc })
      .AddOptionalParameter("create_table")
      .AddOptionalParameter("create_table_hash_bucket_nums")
      .AddOptionalParameter("create_table_replication_factor")
      .AddOptionalParameter("dst_table")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("tablets")
      .AddOptionalParameter("write_type")
      .Build();

  unique_ptr<Action> set_extra_config =
      ClusterActionBuilder("set_extra_config", &SetExtraConfig)
      .Description("Change a extra configuration value on a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kConfigNameArg, "Name of the configuration" })
      .AddRequiredParameter({ kConfigValueArg, "New value for the configuration" })
      .Build();

  unique_ptr<Action> get_extra_configs =
      ClusterActionBuilder("get_extra_configs", &GetExtraConfigs)
      .Description("Get the extra configuration properties for a table")
      .AddRequiredParameter({ kTableNameArg,
                              "Name of the table for which to get extra configurations" })
      .AddOptionalParameter("config_names")
      .Build();

  unique_ptr<Action> drop_range_partition =
      ClusterActionBuilder("drop_range_partition", &DropRangePartition)
      .Description("Drop a range partition of table")
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
      ClusterActionBuilder("add_range_partition", &AddRangePartition)
      .Description("Add a range partition for table")
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

  unique_ptr<Action> column_set_default =
      ClusterActionBuilder("column_set_default", &ColumnSetDefault)
      .Description("Set write_default value for a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to alter" })
      .AddRequiredParameter({ kDefaultValueArg,
                              "Write default value of the column, should be provided as a "
                              "JSON array, e.g. [1] or [\"foo\"]" })
      .Build();

  unique_ptr<Action> column_remove_default =
      ClusterActionBuilder("column_remove_default", &ColumnRemoveDefault)
      .Description("Remove write_default value for a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to alter" })
      .Build();

  unique_ptr<Action> column_set_compression =
      ClusterActionBuilder("column_set_compression", &ColumnSetCompression)
      .Description("Set compression type for a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to alter" })
      .AddRequiredParameter({ kCompressionTypeArg, "Compression type of the column" })
      .Build();

  unique_ptr<Action> column_set_encoding =
      ClusterActionBuilder("column_set_encoding", &ColumnSetEncoding)
      .Description("Set encoding type for a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to alter" })
      .AddRequiredParameter({ kEncodingTypeArg, "Encoding type of the column" })
      .Build();

  unique_ptr<Action> column_set_block_size =
      ClusterActionBuilder("column_set_block_size", &ColumnSetBlockSize)
      .Description("Set block size for a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to alter" })
      .AddRequiredParameter({ kBlockSizeArg, "Block size of the column" })
      .Build();

  unique_ptr<Action> column_set_comment =
      ClusterActionBuilder("column_set_comment", &ColumnSetComment)
      .Description("Set comment for a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to alter" })
      .AddRequiredParameter({ kColumnCommentArg, "Comment of the column" })
      .Build();

  unique_ptr<Action> add_column =
      ActionBuilder("add_column", &AddColumn)
      .Description("Add a column")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to add" })
      .AddRequiredParameter({ kDataTypeArg, "Data Type, eg: INT8, INT16, INT32, INT64, STRING,"
                            " BOOL, FLOAT, DOUBLE, BINARY, UNIXTIME_MICROS, DECIMAL, VARCHAR,"
                            " TIMESTAMP, DATE"})
      .AddOptionalParameter(kEncodingTypeArg)
      .AddOptionalParameter(kCompressionTypeArg)
      .AddOptionalParameter(kDefaultValueArg)
      .AddOptionalParameter("comment")
      .Build();


  unique_ptr<Action> delete_column =
      ClusterActionBuilder("delete_column", &DeleteColumn)
      .Description("Delete a column")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnNameArg, "Name of the table column to delete" })
      .Build();

  unique_ptr<Action> set_comment =
      ClusterActionBuilder("set_comment", &SetComment)
      .Description("Set the comment for a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kColumnCommentArg, "Comment of the table" })
      .Build();

  unique_ptr<Action> clear_comment =
      ClusterActionBuilder("clear_comment", &ClearComment)
      .Description("Clear the comment for a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .Build();

  unique_ptr<Action> set_replication_factor =
      ClusterActionBuilder("set_replication_factor", &SetReplicationFactor)
      .Description("Change a table's replication factor")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
      .AddRequiredParameter({ kReplicationFactorArg, "New replication factor of the table" })
      .Build();

  unique_ptr<Action> statistics =
      ClusterActionBuilder("statistics", &GetTableStatistics)
      .Description("Get table statistics")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to get statistics" })
      .Build();

  unique_ptr<Action> create_table =
      ClusterActionBuilder("create", &CreateTable)
      .Description("Create a new table")
      .ExtraDescription("Provide the  table-build statements as a JSON object, e.g."
                        "'{\"table_name\":\"test\",\"schema\":{\"columns\":[{\"column_name"
                        "\":\"id\",\"column_type\":\"INT32\",\"default_value\":\"1\"},{"
                        "\"column_name\":\"key\",\"column_type\":\"INT64\",\"is_nullable\""
                        ":false,\"comment\":\"range key\"},{\"column_name\":\"name\",\""
                        "column_type\":\"STRING\",\"is_nullable\":false,\"comment\":\""
                        "user name\"}],\"key_column_names\":[\"id\", \"key\"]},\"partition\""
                        ":{\"hash_partitions\":[{\"columns\":[\"id\"],\"num_buckets\":2,\"seed"
                        "\":100}],\"range_partition\":{\"columns\":[\"key\"],\"range_bounds\":"
                        "[{\"upper_bound\":{\"bound_type\":\"inclusive\",\"bound_values\":[\"2"
                        "\"]}},{\"lower_bound\": {\"bound_type\":\"exclusive\",\"bound_values"
                        "\": [\"2\"]},\"upper_bound\":{\"bound_type\":\"inclusive\",\""
                        "bound_values\":[\"3\"]}}]}},\"extra_configs\":{\"configs\":{\""
                        "kudu.table.history_max_age_sec\":\"3600\"}},\"num_replicas\":3}'.")
      .AddRequiredParameter({ kCreateTableJSONArg, "JSON object for creating table" })
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddMode(BuildSetTableLimitMode())
      .AddAction(std::move(add_column))
      .AddAction(std::move(add_range_partition))
      .AddAction(std::move(clear_comment))
      .AddAction(std::move(column_remove_default))
      .AddAction(std::move(column_set_block_size))
      .AddAction(std::move(column_set_compression))
      .AddAction(std::move(column_set_default))
      .AddAction(std::move(column_set_encoding))
      .AddAction(std::move(column_set_comment))
      .AddAction(std::move(copy_table))
      .AddAction(std::move(create_table))
      .AddAction(std::move(delete_column))
      .AddAction(std::move(delete_table))
      .AddAction(std::move(describe_table))
      .AddAction(std::move(drop_range_partition))
      .AddAction(std::move(get_extra_configs))
      .AddAction(std::move(list_tables))
      .AddAction(std::move(locate_row))
      .AddAction(std::move(rename_column))
      .AddAction(std::move(rename_table))
      .AddAction(std::move(scan_table))
      .AddAction(std::move(set_comment))
      .AddAction(std::move(set_extra_config))
      .AddAction(std::move(set_replication_factor))
      .AddAction(std::move(statistics))
      .Build();
}

} // namespace tools
} // namespace kudu

