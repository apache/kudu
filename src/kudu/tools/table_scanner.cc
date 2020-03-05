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
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/bind.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
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
using std::endl;
using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DEFINE_bool(create_table, true,
            "Whether to create the destination table if it doesn't exist.");
DECLARE_string(columns);
DEFINE_bool(fill_cache, true,
            "Whether to fill block cache when scanning.");
DECLARE_int32(num_threads);
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
DEFINE_bool(show_values, false,
            "Whether to show values of scanned rows.");
DECLARE_string(tablets);
DEFINE_string(write_type, "insert",
              "How data should be copied to the destination table. Valid values are 'insert', "
              "'upsert' or the empty string. If the empty string, data will not be copied "
              "(useful when create_table is 'true').");

static bool ValidateWriteType(const char* flag_name,
                              const string& flag_value) {
  const vector<string> allowed_values = { "insert", "upsert", "" };
  if (std::find_if(allowed_values.begin(), allowed_values.end(),
                   [&](const string& allowed_value) {
                     return boost::iequals(allowed_value, flag_value);
                   }) != allowed_values.end()) {
    return true;
  }

  std::ostringstream ss;
  ss << "'" << flag_value << "': unsupported value for --" << flag_name
     << " flag; should be one of ";
  copy(allowed_values.begin(), allowed_values.end(),
       std::ostream_iterator<string>(ss, " "));
  LOG(ERROR) << ss.str();

  return false;
}
DEFINE_validator(write_type, &ValidateWriteType);

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
                    const boost::optional<const rapidjson::Value*>& value,
                    const JsonReader& reader,
                    KuduScanTokenBuilder& builder) {
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
      predicate = NewComparisonPredicate(table, type, predicate_type, column_name, value.get());
      break;
    case PredicateType::IsNotNull:
    case PredicateType::IsNull:
      CHECK(!value);
      predicate = NewIsNullPredicate(table, column_name, pt);
      break;
    case PredicateType::InList: {
      CHECK(value);
      predicate = NewInListPredicate(table, type, column_name, reader, value.get());
      break;
    }
    default:
      return Status::NotSupported(Substitute("not support predicate_type $0", predicate_type));
  }
  CHECK(predicate);
  RETURN_NOT_OK(builder.AddConjunctPredicate(predicate));

  return Status::OK();
}

Status AddPredicates(const client::sp::shared_ptr<KuduTable>& table,
                     KuduScanTokenBuilder& builder) {
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
          elements.size() == 2 ?
            boost::none : boost::optional<const rapidjson::Value*>(elements[2]),
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
  if (!s.IsNotFound() && !s.ok()) {
    return s;
  }

  // Destination table exists.
  const KuduSchema& src_table_schema = src_table->schema();
  if (s.ok()) {
    if (src_table->id() == dst_table->id()) {
      return Status::AlreadyPresent("Destination table is the same as the source table.");
    }

    const KuduSchema& dst_table_schema = dst_table->schema();
    if (!src_table_schema.Equals(dst_table_schema)) {
      return Status::NotSupported(
          "Not support different schema of source table and destination table.");
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
  unique_ptr<KuduTableCreator> table_creator(dst_client->NewTableCreator());
  table_creator->table_name(dst_table_name)
      .schema(&dst_table_schema)
      .num_replicas(src_table->num_replicas());

  // Add hash partition schemas.
  for (const auto& hash_partition_schema : partition_schema.hash_partition_schemas()) {
    auto hash_columns = convert_column_ids_to_names(hash_partition_schema.column_ids);
    table_creator->add_hash_partitions(hash_columns,
                                       hash_partition_schema.num_buckets,
                                       hash_partition_schema.seed);
  }

  // Add range partition schema.
  if (!partition_schema.range_partition_schema().column_ids.empty()) {
    auto range_columns
      = convert_column_ids_to_names(partition_schema.range_partition_schema().column_ids);
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
    Slice range_key_start = partition.range_key_start();
    Slice range_key_end = partition.range_key_end();
    RETURN_NOT_OK(partition_schema.DecodeRangeKey(&range_key_start, lower.get(), &arena));
    RETURN_NOT_OK(partition_schema.DecodeRangeKey(&range_key_end, upper.get(), &arena));

    table_creator->add_range_partition(lower.release(), upper.release());
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
    while (scanner->HasMoreRows()) {
      KuduScanBatch batch;
      RETURN_NOT_OK(scanner->NextBatch(&batch));
      count += batch.NumRows();
      total_count_.IncrementBy(batch.NumRows());
      cb(batch);
    }

    sw.stop();
    if (out_) {
      MutexLock l(output_lock_);
      *out_ << "T " << token->tablet().id() << " scanned count " << count
           << " cost " << sw.elapsed().wall_seconds() << " seconds" << endl;
    }
  }

  return Status::OK();

}

void TableScanner::ScanTask(const vector<KuduScanToken *>& tokens, Status* thread_status) {
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
  CHECK_OK(dst_client_.get()->OpenTable(*dst_table_name_, &dst_table));
  const KuduSchema& dst_table_schema = dst_table->schema();

  // One session per thread.
  client::sp::shared_ptr<KuduSession> session(dst_client_.get()->NewSession());
  CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  CHECK_OK(session->SetErrorBufferSpace(1024));
  session->SetTimeoutMillis(30000);

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
    RETURN_NOT_OK(builder.SetReadMode(mode_.get()));
  }
  RETURN_NOT_OK(builder.SetTimeoutMillis(30000));

  // Set projection if needed.
  if (type == WorkType::kScan) {
    bool project_all = FLAGS_columns == "*" ||
                       (FLAGS_show_values && FLAGS_columns.empty());
    if (!project_all) {
      vector<string> projected_column_names = Split(FLAGS_columns, ",", strings::SkipEmpty());
      RETURN_NOT_OK(builder.SetProjectedColumnNames(projected_column_names));
    }
  }

  // Set predicates.
  RETURN_NOT_OK(AddPredicates(src_table, builder));

  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  // Set tablet filter.
  const set<string>& tablet_id_filters = Split(FLAGS_tablets, ",", strings::SkipWhitespace());
  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto token : tokens) {
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
    if (type == WorkType::kScan) {
      RETURN_NOT_OK(thread_pool_->SubmitFunc(
        boost::bind(&TableScanner::ScanTask, this, thread_tokens[i], &thread_statuses[i])));
    } else {
      CHECK(type == WorkType::kCopy);
      RETURN_NOT_OK(thread_pool_->SubmitFunc(
        boost::bind(&TableScanner::CopyTask, this, thread_tokens[i], &thread_statuses[i])));
    }
  }
  while (!thread_pool_->WaitFor(MonoDelta::FromSeconds(5))) {
    LOG(INFO) << "Scanned count: " << total_count_.Load();
  }
  thread_pool_->Shutdown();

  sw.stop();
  if (out_) {
    *out_ << "Total count " << total_count_.Load()
        << " cost " << sw.elapsed().wall_seconds() << " seconds" << endl;
  }

  for (i = 0; i < FLAGS_num_threads; ++i) {
    if (!thread_statuses[i].ok()) {
      if (out_) *out_ << "Scanning failed " << thread_statuses[i].ToString() << endl;
      if (end_status.ok()) end_status = thread_statuses[i];
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

} // namespace tools
} // namespace kudu
