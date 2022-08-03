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

#include "kudu/tools/table_scanner.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <optional>
#include <map>
#include <memory>
#include <set>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/resource_metrics.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/string_case.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::KuduWriteOperation;
using kudu::iequals;
using std::endl;
using std::map;
using std::nullopt;
using std::optional;
using std::ostream;
using std::ostringstream;
using std::right;
using std::set;
using std::setw;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DEFINE_bool(create_table, true,
            "Whether to create the destination table if it doesn't exist.");
DEFINE_int32(create_table_replication_factor, -1,
             "The replication factor of the destination table if the table will be created. "
             "By default, the replication factor of source table will be used.");
DEFINE_string(create_table_hash_bucket_nums, "",
              "The number of hash buckets in each hash dimension seperated by comma");
DEFINE_bool(fill_cache, true,
            "Whether to fill block cache when scanning.");
DEFINE_bool(fault_tolerant, false,
            "Whether the results are returned in primary key order, make the scanner "
            "fault-tolerant. This also means the scanner can recover if the server it "
            "is scanning fails in the middle of a scan.");
DEFINE_string(predicates, "",
              "Query predicates on columns. Unlike traditional SQL syntax, "
              "the scan tool's simple query predicates are represented in a "
              "simple JSON syntax. Three types of predicates are supported, "
              "including 'Comparison', 'InList' and 'IsNull'.\n"
              " * The 'Comparison' type support <=, <, =, > and >=,\n"
              "   which can be represented as '[operator, column_name, value]',""\n"
              R"*(   e.g. '[">=", "col1", "value"]')*""\n"
              " * The 'InList' type can be represented as\n"
              R"*(   '["IN", column_name, [value1, value2, ...]]')*""\n"
              R"*(   e.g. '["IN", "col2", ["value1", "value2"]]')*""\n"
              " * The 'IsNull' type determine whether the value is NULL or not,\n"
              "   which can be represented as '[operator, column_name]'\n"
              R"*(   e.g. '["NULL", "col1"]', or '["NOTNULL", "col2"]')*""\n"
              "Predicates can be combined together with predicate operators using the syntax\n"
              "   [operator, predicate, predicate, ..., predicate].\n"
              "For example,\n"
              R"*(   ["AND", [">=", "col1", "value"], ["NOTNULL", "col2"]])*""\n"
              "The only supported predicate operator is `AND`.");
DEFINE_bool(report_scanner_stats, false,
            "Whether to report scanner statistics");
DEFINE_bool(show_values, false,
            "Whether to show values of scanned rows.");
DEFINE_string(write_type, "insert",
              "How data should be copied to the destination table. Valid values are 'insert', "
              "'upsert' or the empty string. If the empty string, data will not be copied "
              "(useful when --create_table=true).");
DEFINE_string(replica_selection, "CLOSEST",
              "Replica selection for scan operations. Acceptable values are: "
              "CLOSEST, LEADER (maps into KuduClient::CLOSEST_REPLICA and "
              "KuduClient::LEADER_ONLY correspondingly).");

DECLARE_bool(row_count_only);
DECLARE_int32(num_threads);
DECLARE_int64(timeout_ms);
DECLARE_string(columns);
DECLARE_string(tablets);

namespace {

bool IsFlagValueAcceptable(const char* flag_name,
                           const string& flag_value,
                           const vector<string>& acceptable_values) {
  if (std::find_if(acceptable_values.begin(), acceptable_values.end(),
                   [&](const string& value) {
                     return iequals(value, flag_value);
                   }) != acceptable_values.end()) {
    return true;
  }

  ostringstream ss;
  ss << "'" << flag_value << "': unsupported value for --" << flag_name
     << " flag; should be one of ";
  copy(acceptable_values.begin(), acceptable_values.end(),
       std::ostream_iterator<string>(ss, " "));
  LOG(ERROR) << ss.str();

  return false;
}

bool ValidateWriteType(const char* flag_name,
                       const string& flag_value) {
  static const vector<string> kWriteTypes = { "insert", "upsert", "" };
  return IsFlagValueAcceptable(flag_name, flag_value, kWriteTypes);
}

constexpr const char* const kReplicaSelectionClosest = "closest";
constexpr const char* const kReplicaSelectionLeader = "leader";
bool ValidateReplicaSelection(const char* flag_name,
                              const string& flag_value) {
  static const vector<string> kReplicaSelections = {
    kReplicaSelectionClosest,
    kReplicaSelectionLeader,
  };
  return IsFlagValueAcceptable(flag_name, flag_value, kReplicaSelections);
}

} // anonymous namespace

DEFINE_validator(write_type, &ValidateWriteType);
DEFINE_validator(replica_selection, &ValidateReplicaSelection);

namespace kudu {
namespace tools {

PredicateType ParsePredicateType(const string& predicate_type) {
  string predicate_type_uc;
  ToUpperCase(predicate_type, &predicate_type_uc);
  if (predicate_type_uc == "=") {
    return PredicateType::Equality;
  } else if (predicate_type_uc == "<" ||
      predicate_type_uc == "<=" ||
      predicate_type_uc == ">" ||
      predicate_type_uc == ">=") {
    return PredicateType::Range;
  } else if (predicate_type_uc == "NULL") {
    return PredicateType::IsNull;
  } else if (predicate_type_uc == "NOTNULL") {
    return PredicateType::IsNotNull;
  } else if (predicate_type_uc == "IN") {
    return PredicateType::InList;
  } else {
    LOG(FATAL) << Substitute("unhandled predicate type $0", predicate_type);
    return PredicateType::None;
  }
}

KuduValue* ParseValue(KuduColumnSchema::DataType type,
                      const rapidjson::Value* value) {
  CHECK(value != nullptr);
  switch (type) {
    case KuduColumnSchema::DataType::INT8:
    case KuduColumnSchema::DataType::INT16:
    case KuduColumnSchema::DataType::INT32:
      CHECK(value->IsInt());
      return KuduValue::FromInt(value->GetInt());
    case KuduColumnSchema::DataType::INT64:
      CHECK(value->IsInt64());
      return KuduValue::FromInt(value->GetInt64());
    case KuduColumnSchema::DataType::STRING:
      CHECK(value->IsString());
      return KuduValue::CopyString(value->GetString());
    case KuduColumnSchema::DataType::BOOL:
      CHECK(value->IsBool());
      return KuduValue::FromBool(value->GetBool());
    case KuduColumnSchema::DataType::FLOAT:
      CHECK(value->IsDouble());
      return KuduValue::FromFloat(static_cast<float>(value->GetDouble()));
    case KuduColumnSchema::DataType::DOUBLE:
      CHECK(value->IsDouble());
      return KuduValue::FromDouble(value->GetDouble());
    default:
      LOG(FATAL) << Substitute("unhandled data type $0", type);
  }

  return nullptr;
}

KuduPredicate* NewComparisonPredicate(const client::sp::shared_ptr<KuduTable>& table,
                                      KuduColumnSchema::DataType type,
                                      const string& predicate_type,
                                      const string& column_name,
                                      const rapidjson::Value* value) {
  KuduValue* kudu_value = ParseValue(type, value);
  CHECK(kudu_value != nullptr);
  KuduPredicate::ComparisonOp cop;
  if (predicate_type == "<") {
    cop = KuduPredicate::ComparisonOp::LESS;
  } else if (predicate_type == "<=") {
    cop = KuduPredicate::ComparisonOp::LESS_EQUAL;
  } else if (predicate_type == "=") {
    cop = KuduPredicate::ComparisonOp::EQUAL;
  } else if (predicate_type == ">") {
    cop = KuduPredicate::ComparisonOp::GREATER;
  } else if (predicate_type == ">=") {
    cop = KuduPredicate::ComparisonOp::GREATER_EQUAL;
  } else {
    return nullptr;
  }
  return table->NewComparisonPredicate(column_name, cop, kudu_value);
}

KuduPredicate* NewIsNullPredicate(const client::sp::shared_ptr<KuduTable>& table,
                                  const string& column_name,
                                  PredicateType pt) {
  switch (pt) {
    case PredicateType::IsNotNull:
      return table->NewIsNotNullPredicate(column_name);
    case PredicateType::IsNull:
      return table->NewIsNullPredicate(column_name);
    default:
      return nullptr;
  }
}

KuduPredicate* NewInListPredicate(const client::sp::shared_ptr<KuduTable>& table,
                                  KuduColumnSchema::DataType type,
                                  const string& name,
                                  const JsonReader& reader,
                                  const rapidjson::Value *object) {
  CHECK(object->IsArray());
  vector<const rapidjson::Value*> values;
  reader.ExtractObjectArray(object, nullptr, &values);
  vector<KuduValue *> kudu_values;
  for (const auto& value : values) {
    kudu_values.emplace_back(ParseValue(type, value));
  }
  return table->NewInListPredicate(name, &kudu_values);
}

Status AddPredicate(const client::sp::shared_ptr<KuduTable>& table,
                    const string& predicate_type,
                    const string& column_name,
                    const optional<const rapidjson::Value*>& value,
                    const JsonReader& reader,
                    KuduScanTokenBuilder* builder) {
  if (predicate_type.empty() || column_name.empty()) {
    return Status::OK();
  }

  Schema schema_internal = KuduSchema::ToSchema(table->schema());
  int idx = schema_internal.find_column(column_name);
  if (PREDICT_FALSE(idx == Schema::kColumnNotFound)) {
    return Status::NotFound("no such column", column_name);
  }
  auto type = table->schema().Column(static_cast<size_t>(idx)).type();
  KuduPredicate* predicate = nullptr;
  PredicateType pt = ParsePredicateType(predicate_type);
  switch (pt) {
    case PredicateType::Equality:
    case PredicateType::Range:
      CHECK(value);
      predicate = NewComparisonPredicate(table, type, predicate_type, column_name, *value);
      break;
    case PredicateType::IsNotNull:
    case PredicateType::IsNull:
      CHECK(!value);
      predicate = NewIsNullPredicate(table, column_name, pt);
      break;
    case PredicateType::InList: {
      CHECK(value);
      predicate = NewInListPredicate(table, type, column_name, reader, *value);
      break;
    }
    default:
      return Status::NotSupported(Substitute("not support predicate_type $0", predicate_type));
  }
  CHECK(predicate);
  RETURN_NOT_OK(builder->AddConjunctPredicate(predicate));

  return Status::OK();
}

Status AddPredicates(const client::sp::shared_ptr<KuduTable>& table,
                     KuduScanTokenBuilder* builder) {
  if (FLAGS_predicates.empty()) {
    return Status::OK();
  }
  JsonReader reader(FLAGS_predicates);
  RETURN_NOT_OK(reader.Init());
  vector<const rapidjson::Value*> predicate_objects;
  RETURN_NOT_OK(reader.ExtractObjectArray(reader.root(),
                                          nullptr,
                                          &predicate_objects));
  vector<unique_ptr<KuduPredicate>> predicates;
  for (int i = 0; i < predicate_objects.size(); ++i) {
    if (i == 0) {
      CHECK(predicate_objects[i]->IsString());
      string op;
      ToUpperCase(predicate_objects[i]->GetString(), &op);
      if (op != "AND") {
        return Status::InvalidArgument(Substitute("only 'AND' operator is supported now"));
      }
      continue;
    }

    CHECK(predicate_objects[i]->IsArray());
    vector<const rapidjson::Value*> elements;
    reader.ExtractObjectArray(predicate_objects[i], nullptr, &elements);
    if (elements.size() == 2 || elements.size() == 3) {
      CHECK(elements[0]->IsString());
      CHECK(elements[1]->IsString());
      RETURN_NOT_OK(AddPredicate(table,
          elements[0]->GetString(),
          elements[1]->GetString(),
          elements.size() == 2 ? nullopt
                               : optional<const rapidjson::Value*>(elements[2]),
          reader,
          builder));
    } else {
      return Status::InvalidArgument(
          Substitute("invalid predicate elements count $0", elements.size()));
    }
  }

  return Status::OK();
}

Status CreateDstTableIfNeeded(const client::sp::shared_ptr<KuduTable>& src_table,
                              const client::sp::shared_ptr<KuduClient>& dst_client,
                              const string& dst_table_name) {
  client::sp::shared_ptr<KuduTable> dst_table;
  Status s = dst_client->OpenTable(dst_table_name, &dst_table);
  if (!s.IsNotFound()) {
    return s;
  }

  // Destination table exists.
  const KuduSchema& src_table_schema = src_table->schema();
  if (s.ok()) {
    if (src_table->id() == dst_table->id()) {
      return Status::AlreadyPresent("Destination table is the same as the source table.");
    }

    const KuduSchema& dst_table_schema = dst_table->schema();
    if (src_table_schema != dst_table_schema) {
      return Status::NotSupported(Substitute(
          "destination table's schema differs from the source one ($0 vs $1)",
          dst_table_schema.ToString(), src_table_schema.ToString()));
    }

    return Status::OK();
  }

  // Destination table does NOT exist.
  if (!FLAGS_create_table) {
    return Status::NotFound(Substitute("Table $0 does not exist in the destination cluster.",
                                       dst_table_name));
  }

  Schema schema_internal = KuduSchema::ToSchema(src_table_schema);
  // Convert Schema to KuduSchema will drop internal ColumnIds.
  KuduSchema dst_table_schema = KuduSchema::FromSchema(schema_internal);
  const auto& partition_schema = src_table->partition_schema();

  auto convert_column_ids_to_names = [&schema_internal] (const vector<ColumnId>& column_ids) {
    vector<string> column_names;
    column_names.reserve(column_ids.size());
    for (const auto& column_id : column_ids) {
      column_names.emplace_back(schema_internal.column_by_id(column_id).name());
    }
    return column_names;
  };

  // Table schema and replica number.
  int num_replicas = FLAGS_create_table_replication_factor == -1 ?
      src_table->num_replicas() : FLAGS_create_table_replication_factor;
  unique_ptr<KuduTableCreator> table_creator(dst_client->NewTableCreator());
  table_creator->table_name(dst_table_name)
      .schema(&dst_table_schema)
      .num_replicas(num_replicas);

  // Add hash partition schema.
  vector<int> hash_bucket_nums;
  if (!partition_schema.hash_schema().empty()) {
    vector<string> hash_bucket_nums_str = Split(FLAGS_create_table_hash_bucket_nums,
                                                ",", strings::SkipEmpty());
    // FLAGS_create_table_hash_bucket_nums is not defined, set it to -1 defaultly.
    if (hash_bucket_nums_str.empty()) {
      for (int i = 0; i < partition_schema.hash_schema().size(); i++) {
        hash_bucket_nums.push_back(-1);
      }
    } else {
      // If the --create_table_hash_bucket_nums flag is set, the number
      // of comma-separated elements must be equal to the number of hash schema dimensions.
      if (partition_schema.hash_schema().size() != hash_bucket_nums_str.size()) {
        return Status::InvalidArgument("The count of hash bucket numbers must be equal to the "
                                       "number of hash schema dimensions.");
      }
      for (int i = 0; i < hash_bucket_nums_str.size(); i++) {
        int bucket_num = 0;
        bool is_number = safe_strto32(hash_bucket_nums_str[i], &bucket_num);
        if (!is_number) {
          return Status::InvalidArgument(Substitute("'$0': cannot parse the number "
                                                    "of hash buckets.",
                                                    hash_bucket_nums_str[i]));
        }
        if (bucket_num < 2) {
          return Status::InvalidArgument("The number of hash buckets must not be less than 2.");
        }
        hash_bucket_nums.push_back(bucket_num);
      }
    }
  }

  if (partition_schema.hash_schema().empty() &&
      !FLAGS_create_table_hash_bucket_nums.empty()) {
    return Status::InvalidArgument("There are no hash partitions defined in this table.");
  }

  int i = 0;
  for (const auto& hash_dimension : partition_schema.hash_schema()) {
    int num_buckets = hash_bucket_nums[i] != -1 ? hash_bucket_nums[i] :
                                                  hash_dimension.num_buckets;
    auto hash_columns = convert_column_ids_to_names(hash_dimension.column_ids);
    table_creator->add_hash_partitions(hash_columns,
                                       num_buckets,
                                       hash_dimension.seed);
    i++;
  }

  // Add range partition schema.
  if (!partition_schema.range_schema().column_ids.empty()) {
    auto range_columns
      = convert_column_ids_to_names(partition_schema.range_schema().column_ids);
    table_creator->set_range_partition_columns(range_columns);
  }

  // Add range bounds for each range partition.
  vector<Partition> partitions;
  RETURN_NOT_OK(src_table->ListPartitions(&partitions));
  for (const auto& partition : partitions) {
    // Deduplicate by hash bucket to get a unique entry per range partition.
    const auto& hash_buckets = partition.hash_buckets();
    if (!std::all_of(hash_buckets.begin(),
                     hash_buckets.end(),
                     [](int32_t bucket) { return bucket == 0; })) {
      continue;
    }

    // Partitions are considered metadata, so don't redact them.
    ScopedDisableRedaction no_redaction;

    Arena arena(256);
    std::unique_ptr<KuduPartialRow> lower(new KuduPartialRow(&schema_internal));
    std::unique_ptr<KuduPartialRow> upper(new KuduPartialRow(&schema_internal));
    Slice range_key_start(partition.begin().range_key());
    Slice range_key_end(partition.end().range_key());
    RETURN_NOT_OK(partition_schema.DecodeRangeKey(&range_key_start, lower.get(), &arena));
    RETURN_NOT_OK(partition_schema.DecodeRangeKey(&range_key_end, upper.get(), &arena));

    table_creator->add_range_partition(lower.release(), upper.release());
  }

  if (partition_schema.hash_schema().empty() &&
      partition_schema.range_schema().column_ids.empty()) {
    // This src table is unpartitioned, just create a table range partitioned on no columns.
    table_creator->set_range_partition_columns({});
  }

  // Create table.
  RETURN_NOT_OK(table_creator->Create());
  LOG(INFO) << "Table " << dst_table_name << " created successfully";

  return Status::OK();
}

void CheckPendingErrors(const client::sp::shared_ptr<KuduSession>& session) {
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  session->GetPendingErrors(&errors, nullptr);
  for (const auto& error : errors) {
    LOG(ERROR) << error->status().ToString();
  }
}

TableScanner::TableScanner(
    client::sp::shared_ptr<client::KuduClient> client,
    std::string table_name,
    optional<client::sp::shared_ptr<client::KuduClient>> dst_client,
    optional<std::string> dst_table_name)
    : total_count_(0),
      client_(std::move(client)),
      table_name_(std::move(table_name)),
      dst_client_(std::move(dst_client)),
      dst_table_name_(std::move(dst_table_name)),
      scan_batch_size_(-1),
      out_(nullptr) {
  CHECK_OK(SetReplicaSelection(FLAGS_replica_selection));
}

Status TableScanner::ScanData(const std::vector<kudu::client::KuduScanToken*>& tokens,
                              const std::function<void(const KuduScanBatch& batch)>& cb) {
  for (auto token : tokens) {
    Stopwatch sw(Stopwatch::THIS_THREAD);
    sw.start();

    KuduScanner* scanner_ptr;
    RETURN_NOT_OK(token->IntoKuduScanner(&scanner_ptr));

    unique_ptr<KuduScanner> scanner(scanner_ptr);
    RETURN_NOT_OK(scanner->Open());

    uint64_t count = 0;
    size_t next_batch_calls = 0;
    while (scanner->HasMoreRows()) {
      KuduScanBatch batch;
      RETURN_NOT_OK(scanner->NextBatch(&batch));
      count += batch.NumRows();
      total_count_ += batch.NumRows();
      ++next_batch_calls;
      cb(batch);
    }
    sw.stop();

    if (FLAGS_report_scanner_stats && out_) {
      auto& out = *out_;
      MutexLock l(output_lock_);
      out << Substitute("T $0 scanned $1 rows in $2 seconds\n",
                        token->tablet().id(), count, sw.elapsed().wall_seconds());
      const auto& metrics = scanner->GetResourceMetrics();
      out << setw(32) << right << "NextBatch() calls"
          << setw(16) << right << next_batch_calls << endl;
      for (const auto& [k, v] : metrics.Get()) {
        out << setw(32) << right << k << setw(16) << right << v << endl;
      }
    }
  }
  return Status::OK();
}

void TableScanner::ScanTask(const vector<KuduScanToken*>& tokens, Status* thread_status) {
  *thread_status = ScanData(tokens, [&](const KuduScanBatch& batch) {
    if (out_ && FLAGS_show_values) {
      MutexLock l(output_lock_);
      for (const auto& row : batch) {
        *out_ << row.ToString() << "\n";
      }
      out_->flush();
    }
  });
}

void TableScanner::CopyTask(const vector<KuduScanToken*>& tokens, Status* thread_status) {
  client::sp::shared_ptr<KuduTable> dst_table;
  CHECK_OK((*dst_client_)->OpenTable(*dst_table_name_, &dst_table));
  const KuduSchema& dst_table_schema = dst_table->schema();

  // One session per thread.
  client::sp::shared_ptr<KuduSession> session((*dst_client_)->NewSession());
  CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  CHECK_OK(session->SetErrorBufferSpace(1024));
  session->SetTimeoutMillis(FLAGS_timeout_ms);

  *thread_status = ScanData(tokens, [&](const KuduScanBatch& batch) {
    for (const auto& row : batch) {
      CHECK_OK(AddRow(dst_table, dst_table_schema, row, session));
    }
    CheckPendingErrors(session);
    // Flush here to make sure all write operations have been sent,
    // and all strings reference to batch are still valid.
    CHECK_OK(session->Flush());
  });
}

void TableScanner::SetOutput(ostream* out) {
  out_ = out;
}

void TableScanner::SetReadMode(KuduScanner::ReadMode mode) {
  mode_ = mode;
}

Status TableScanner::SetReplicaSelection(const string& selection_str) {
  KuduClient::ReplicaSelection selection;
  RETURN_NOT_OK(ParseReplicaSelection(selection_str, &selection));
  replica_selection_ = selection;
  return Status::OK();
}

void TableScanner::SetScanBatchSize(int32_t scan_batch_size) {
  scan_batch_size_ = scan_batch_size;
}

Status TableScanner::StartWork(WorkType type) {
  client::sp::shared_ptr<KuduTable> src_table;
  RETURN_NOT_OK(client_->OpenTable(table_name_, &src_table));

  // Create destination table if needed.
  if (type == WorkType::kCopy) {
    RETURN_NOT_OK(CreateDstTableIfNeeded(src_table, *dst_client_, *dst_table_name_));
    if (FLAGS_write_type.empty()) {
      // Create table only.
      return Status::OK();
    }
  }

  KuduScanTokenBuilder builder(src_table.get());
  RETURN_NOT_OK(builder.SetCacheBlocks(FLAGS_fill_cache));
  if (mode_) {
    RETURN_NOT_OK(builder.SetReadMode(*mode_));
  }
  if (scan_batch_size_ >= 0) {
    // Batch size of 0 is valid and has special semantics: the server sends
    // zero rows (i.e. no data) in the very first scan batch sent back to the
    // client. See {KuduScanner,KuduScanTokenBuilder}::SetBatchSizeBytes().
    RETURN_NOT_OK(builder.SetBatchSizeBytes(scan_batch_size_));
  }
  RETURN_NOT_OK(builder.SetSelection(replica_selection_));
  RETURN_NOT_OK(builder.SetTimeoutMillis(FLAGS_timeout_ms));
  if (FLAGS_fault_tolerant) {
    // TODO(yingchun): push down this judgement to ScanConfiguration::SetFaultTolerant
    if (mode_ && *mode_ != KuduScanner::READ_AT_SNAPSHOT) {
      return Status::InvalidArgument(Substitute("--fault_tolerant is conflict with "
          "the non-READ_AT_SNAPSHOT read mode"));
    }
    RETURN_NOT_OK(builder.SetFaultTolerant());
  }

  // Set projection if needed.
  if (type == WorkType::kScan) {
    const auto project_all = FLAGS_columns == "*" || FLAGS_columns.empty();
    if (!project_all || FLAGS_row_count_only) {
      vector<string> projected_column_names;
      if (!FLAGS_row_count_only && !FLAGS_columns.empty()) {
        projected_column_names = Split(FLAGS_columns, ",", strings::SkipEmpty());
      }
      RETURN_NOT_OK(builder.SetProjectedColumnNames(projected_column_names));
    }
  }

  // Set predicates.
  RETURN_NOT_OK(AddPredicates(src_table, &builder));

  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  // Set tablet filter.
  const set<string>& tablet_id_filters = Split(FLAGS_tablets, ",", strings::SkipWhitespace());
  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto* token : tokens) {
    if (tablet_id_filters.empty() || ContainsKey(tablet_id_filters, token->tablet().id())) {
      thread_tokens[i++ % FLAGS_num_threads].emplace_back(token);
    }
  }

  // Initialize statuses for each thread.
  vector<Status> thread_statuses(FLAGS_num_threads);

  RETURN_NOT_OK(ThreadPoolBuilder("table_scan_pool")
                  .set_max_threads(FLAGS_num_threads)
                  .set_idle_timeout(MonoDelta::FromMilliseconds(1))
                  .Build(&thread_pool_));

  Status end_status = Status::OK();
  Stopwatch sw(Stopwatch::THIS_THREAD);
  sw.start();
  for (i = 0; i < FLAGS_num_threads; ++i) {
    auto* t_tokens = &thread_tokens[i];
    auto* t_status = &thread_statuses[i];
    if (type == WorkType::kScan) {
      RETURN_NOT_OK(thread_pool_->Submit([this, t_tokens, t_status]()
                                         { this->ScanTask(*t_tokens, t_status); }));
    } else {
      CHECK(type == WorkType::kCopy);
      RETURN_NOT_OK(thread_pool_->Submit([this, t_tokens, t_status]()
                                         { this->CopyTask(*t_tokens, t_status); }));
    }
  }
  while (!thread_pool_->WaitFor(MonoDelta::FromSeconds(5))) {
    LOG(INFO) << "Scanned count: " << total_count_;
  }
  thread_pool_->Shutdown();

  sw.stop();
  if (out_) {
    *out_ << "Total count " << total_count_
        << " cost " << sw.elapsed().wall_seconds() << " seconds" << endl;
  }

  for (i = 0; i < FLAGS_num_threads; ++i) {
    if (!thread_statuses[i].ok()) {
      if (out_) {
        *out_ << "Scanning failed " << thread_statuses[i].ToString() << endl;
      }
      if (end_status.ok()) {
        end_status = thread_statuses[i];
      }
    }
  }

  return end_status;
}

Status TableScanner::StartScan() {
  return StartWork(WorkType::kScan);
}

Status TableScanner::StartCopy() {
  CHECK(dst_client_);
  CHECK(dst_table_name_);

  return StartWork(WorkType::kCopy);
}

Status TableScanner::AddRow(const client::sp::shared_ptr<KuduTable>& table,
                            const KuduSchema& table_schema,
                            const KuduScanBatch::RowPtr& src_row,
                            const client::sp::shared_ptr<KuduSession>& session) {
  unique_ptr<KuduWriteOperation> write_op;
  if (FLAGS_write_type == "insert") {
    write_op.reset(table->NewInsert());
  } else if (FLAGS_write_type == "upsert") {
    write_op.reset(table->NewUpsert());
  } else {
    LOG(FATAL) << Substitute("invalid write_type: $0", FLAGS_write_type);
  }

  KuduPartialRow* dst_row = write_op->mutable_row();
  size_t row_size = ContiguousRowHelper::row_size(*src_row.schema_);
  memcpy(dst_row->row_data_, src_row.row_data_, row_size);
  BitmapChangeBits(dst_row->isset_bitmap_, 0, table_schema.num_columns(), true);

  return session->Apply(write_op.release());
}

Status TableScanner::ParseReplicaSelection(
    const string& selection_str,
    KuduClient::ReplicaSelection* selection) {
  DCHECK(selection);
  if (iequals(kReplicaSelectionClosest, selection_str)) {
    *selection = KuduClient::ReplicaSelection::CLOSEST_REPLICA;
  } else if (iequals(kReplicaSelectionLeader, selection_str)) {
    *selection = KuduClient::ReplicaSelection::LEADER_ONLY;
  } else {
    return Status::InvalidArgument("invalid replica selection", selection_str);
  }
  return Status::OK();
}

} // namespace tools
} // namespace kudu
