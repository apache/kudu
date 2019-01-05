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

#include <stddef.h>

#include <iostream>
#include <map>
#include <memory>
#include <set>

#include <boost/bind.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/string_case.h"

using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using strings::Substitute;
using std::cout;
using std::endl;
using std::map;
using std::set;
using std::unique_ptr;

DECLARE_string(columns);
DEFINE_bool(fill_cache, true,
            "Whether to fill block cache when scanning.");
DECLARE_int32(num_threads);

DEFINE_string(predicates, "",
              "Query predicates on columns. Unlike traditional SQL syntax, "
              "the scan tool's simple query predicates are represented in a "
              "simple JSON syntax. Three types of predicates are supported, "
              "including 'Comparison', 'InList' and 'IsNull'.\n"
              " * The 'Comparison' type support <=, <, ==, > and >=,\n"
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
DEFINE_bool(show_value, false,
            "Whether to show values of scanned rows.");
DECLARE_string(tablets);

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
  client::KuduPredicate::ComparisonOp cop;
  if (predicate_type == "<") {
    cop = client::KuduPredicate::ComparisonOp::LESS;
  } else if (predicate_type == "<=") {
    cop = client::KuduPredicate::ComparisonOp::LESS_EQUAL;
  } else if (predicate_type == "=") {
    cop = client::KuduPredicate::ComparisonOp::EQUAL;
  } else if (predicate_type == ">") {
    cop = client::KuduPredicate::ComparisonOp::GREATER;
  } else if (predicate_type == ">=") {
    cop = client::KuduPredicate::ComparisonOp::GREATER_EQUAL;
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

KuduPredicate* NewInListPredicate(const client::sp::shared_ptr<KuduTable> &table,
                                  KuduColumnSchema::DataType type,
                                  const string &name,
                                  const JsonReader &reader,
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

void TableScanner::ScannerTask(const vector<KuduScanToken *>& tokens) {
  for (auto token : tokens) {
    Stopwatch sw(Stopwatch::THIS_THREAD);
    sw.start();

    KuduScanner* scanner;
    CHECK_OK(token->IntoKuduScanner(&scanner));
    CHECK_OK(scanner->Open());

    uint64_t count = 0;
    while (scanner->HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_OK(scanner->NextBatch(&batch));
      count += batch.NumRows();
      total_count_.IncrementBy(batch.NumRows());
      if (FLAGS_show_value) {
        for (const auto& row : batch) {
          cout << row.ToString() << endl;
        }
      }
    }
    delete scanner;

    sw.stop();
    cout << "T " << token->tablet().id() << " scanned count " << count
        << " cost " << sw.elapsed().wall_seconds() << " seconds" << endl;
  }
}

void TableScanner::MonitorTask() {
  MonoTime last_log_time = MonoTime::Now();
  while (thread_pool_->num_threads() > 1) {    // Some other table scan thread is running.
    if (MonoTime::Now() - last_log_time >= MonoDelta::FromSeconds(5)) {
      LOG(INFO) << "Scanned count: " << total_count_.Load() << endl;
      last_log_time = MonoTime::Now();
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
}

Status TableScanner::Run() {
 client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(table_name_, &table));

  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.SetCacheBlocks(FLAGS_fill_cache));
  RETURN_NOT_OK(builder.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(builder.SetReadMode(KuduScanner::READ_LATEST));
  RETURN_NOT_OK(builder.SetTimeoutMillis(30000));

  vector<string> projected_column_names = Split(FLAGS_columns, ",", strings::SkipEmpty());
  RETURN_NOT_OK(builder.SetProjectedColumnNames(projected_column_names));
  RETURN_NOT_OK(AddPredicates(table, builder));

  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  const set<string>& tablet_id_filters = Split(FLAGS_tablets, ",", strings::SkipWhitespace());
  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto token : tokens) {
    if (tablet_id_filters.empty() || ContainsKey(tablet_id_filters, token->tablet().id())) {
      thread_tokens[i++ % FLAGS_num_threads].push_back(token);
    }
  }

  RETURN_NOT_OK(ThreadPoolBuilder("table_scan_pool")
                  .set_max_threads(FLAGS_num_threads + 1)  // add extra 1 thread for MonitorTask
                  .set_idle_timeout(MonoDelta::FromMilliseconds(1))
                  .Build(&thread_pool_));

  Stopwatch sw(Stopwatch::THIS_THREAD);
  sw.start();
  for (i = 0; i < FLAGS_num_threads; ++i) {
    RETURN_NOT_OK(thread_pool_->SubmitFunc(
        boost::bind(&TableScanner::ScannerTask, this, thread_tokens[i])));
  }
  RETURN_NOT_OK(thread_pool_->SubmitFunc(boost::bind(&TableScanner::MonitorTask, this)));
  thread_pool_->Wait();
  thread_pool_->Shutdown();

  sw.stop();
  cout << "Total count " << total_count_.Load()
      << " cost " << sw.elapsed().wall_seconds() << " seconds" << endl;

  return Status::OK();
}

} // namespace tools
} // namespace kudu
