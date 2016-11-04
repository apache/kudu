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
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/util/test_util.h"

using std::count_if;
using std::numeric_limits;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

using sp::shared_ptr;

class PredicateTest : public KuduTest {

 protected:

  void SetUp() override {
    // Set up the mini cluster
    cluster_.reset(new MiniCluster(env_, MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  // Creates a key/value table schema with an int64 key and value of the
  // specified type.
  shared_ptr<KuduTable> CreateAndOpenTable(KuduColumnSchema::DataType value_type) {
    KuduSchema schema;
    {
      KuduSchemaBuilder builder;
      builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
      builder.AddColumn("value")->Type(value_type);
      CHECK_OK(builder.Build(&schema));
    }
    unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
    CHECK_OK(table_creator->table_name("table")
        .schema(&schema)
        .set_range_partition_columns({ "key" })
        .num_replicas(1)
        .Create());

    shared_ptr<KuduTable> table;
    CHECK_OK(client_->OpenTable("table", &table));
    return table;
  }

  // Creates a new session in automatic background flush mode.
  shared_ptr<KuduSession> CreateSession() {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(10000);
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    return session;
  }

  // Count the rows in a table which satisfy the specified predicates.
  int DoCountRows(const shared_ptr<KuduTable>& table,
                  const vector<KuduPredicate*>& predicates) {
    KuduScanner scanner(table.get());
    for (KuduPredicate* predicate : predicates) {
      CHECK_OK(scanner.AddConjunctPredicate(predicate));
    }
    CHECK_OK(scanner.Open());

    int rows = 0;
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_OK(scanner.NextBatch(&batch));
      rows += batch.NumRows();
    }
    return rows;
  }

  // Count the rows in a table which satisfy the specified predicates. This
  // also does a separate scan with cloned predicates, in order to test that
  // cloned predicates behave exactly as the original.
  int CountRows(const shared_ptr<KuduTable>& table,
                const vector<KuduPredicate*>& predicates) {

    vector<KuduPredicate*> cloned_predicates;
    for (KuduPredicate* pred : predicates) {
      cloned_predicates.push_back(pred->Clone());
    }

    int cloned_count = DoCountRows(table, cloned_predicates);
    int count = DoCountRows(table, predicates);
    CHECK_EQ(count, cloned_count);
    return count;
  }

  template <typename T>
  int CountMatchedRows(const vector<T>& values, const vector<T>& test_values) {

    int count = 0;
    for (const T& v : values) {
      if (std::any_of(test_values.begin(), test_values.end(),
                      [&] (const T& t) { return t == v; })) {
        count += 1;
      }
    }
    return count;
  }

  // Returns a vector of ints from -50 (inclusive) to 50 (exclusive), and
  // boundary values.
  template <typename T>
  vector<T> CreateIntValues() {
    vector<T> values;
    for (int i = -50; i < 50; i++) {
      values.push_back(i);
    }
    values.push_back(numeric_limits<T>::min());
    values.push_back(numeric_limits<T>::min() + 1);
    values.push_back(numeric_limits<T>::max() - 1);
    values.push_back(numeric_limits<T>::max());
    return values;
  }

  // Returns a vector of ints for testing as predicate boundaries.
  template <typename T>
  vector<T> CreateIntTestValues() {
    return {
      numeric_limits<T>::min(),
      numeric_limits<T>::min() + 1,
      -51,
      50,
      0,
      49,
      50,
      numeric_limits<T>::max() - 1,
      numeric_limits<T>::max(),
    };
  }

  // Returns a vector of floating point numbers from -50.50 (inclusive) to 49.49
  // (exclusive) (100 values), plus min, max, two normals around 0, two
  // subnormals around 0, positive and negatic infinity, and NaN.
  template <typename T>
  vector<T> CreateFloatingPointValues() {
    vector<T> values;
    for (int i = -50; i < 50; i++) {
      values.push_back(static_cast<T>(i) + static_cast<T>(i) / 100);
    }

    // Add special values (listed in ascending order)
    values.push_back(-numeric_limits<T>::infinity());
    values.push_back(numeric_limits<T>::lowest());
    values.push_back(-numeric_limits<T>::min());
    values.push_back(-numeric_limits<T>::denorm_min());
    values.push_back(-0.0);
    values.push_back(numeric_limits<T>::denorm_min());
    values.push_back(numeric_limits<T>::min());
    values.push_back(numeric_limits<T>::max());
    values.push_back(numeric_limits<T>::infinity());

    // Add special NaN value
    // TODO: uncomment after fixing KUDU-1386
    // values.push_back(numeric_limits<T>::quiet_NaN());

    return values;
  }

  /// Returns a vector of floating point numbers for creating test predicates.
  template <typename T>
  vector<T> CreateFloatingPointTestValues() {
    return {
      -numeric_limits<T>::infinity(),
      numeric_limits<T>::lowest(),
      -100.0,
      -1.1,
      -1.0,
      -numeric_limits<T>::min(),
      -numeric_limits<T>::denorm_min(),
      -0.0,
      0.0,
      numeric_limits<T>::denorm_min(),
      numeric_limits<T>::min(),
      1.0,
      1.1,
      100.0,
      numeric_limits<T>::max(),
      numeric_limits<T>::infinity(),

      // TODO: uncomment after fixing KUDU-1386
      // numeric_limits<T>::quiet_NaN();
    };
  }

  // Returns a vector of string values.
  vector<string> CreateStringValues() {
    return {
      string("", 0),
      string("\0", 1),
      string("\0\0", 2),
      string("a", 1),
      string("a\0", 2),
      string("a\0a", 3),
      string("aa\0", 3),
    };
  }

  // Check integer predicates against the specified table. The table must have
  // key/value rows with values from CreateIntValues, plus a null value.
  template <typename T>
  void CheckIntPredicates(shared_ptr<KuduTable> table) {
    vector<T> values = CreateIntValues<T>();
    vector<T> test_values = CreateIntTestValues<T>();
    ASSERT_EQ(values.size() + 1, CountRows(table, {}));

    for (T v : test_values) {
      SCOPED_TRACE(strings::Substitute("test value: $0", v));

      { // value = v
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value == v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::EQUAL,
                                            KuduValue::FromInt(v)),
        }));
      }

      { // value >= v
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value >= v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER_EQUAL,
                                            KuduValue::FromInt(v)),
        }));
      }

      { // value <= v
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value <= v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS_EQUAL,
                                            KuduValue::FromInt(v)),
        }));
      }

      { // value > v
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value > v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER,
                                            KuduValue::FromInt(v)),
        }));
      }

      { // value < v
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value < v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS,
                                            KuduValue::FromInt(v)),
        }));
      }

      { // value >= 0
        // value <= v
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value >= 0 && value <= v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER_EQUAL,
                                            KuduValue::FromInt(0)),
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS_EQUAL,
                                            KuduValue::FromInt(v)),
        }));
      }

      { // value >= v
        // value <= 0
        int count = count_if(values.begin(), values.end(),
                             [&] (T value) { return value >= v && value <= 0; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER_EQUAL,
                                            KuduValue::FromInt(v)),
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS_EQUAL,
                                            KuduValue::FromInt(0)),
        }));
      }
    }

    // IN list predicates
    std::random_shuffle(test_values.begin(), test_values.end());

    for (auto end = test_values.begin(); end <= test_values.end(); end++) {
      vector<KuduValue*> vals;

      for (auto itr = test_values.begin(); itr != end; itr++) {
        vals.push_back(KuduValue::FromInt(*itr));
      }

      int count = CountMatchedRows<T>(values, vector<T>(test_values.begin(), end));
      ASSERT_EQ(count, CountRows(table, { table->NewInListPredicate("value", &vals) }));
    }
  }

  // Check string predicates against the specified table.
  void CheckStringPredicates(shared_ptr<KuduTable> table) {

    vector<string> values = CreateStringValues();
    ASSERT_EQ(values.size() + 1, CountRows(table, {}));

    // Add some additional values to check against.
    vector<string> test_values = values;
    test_values.push_back("aa");
    test_values.push_back(string("\1", 1));
    test_values.push_back(string("a\1", 1));

    for (const string& v : test_values) {
      SCOPED_TRACE(strings::Substitute("test value: '$0'", strings::CHexEscape(v)));

      { // value = v
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value == v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::EQUAL,
                                            KuduValue::CopyString(v)),
        }));
      }

      { // value >= v
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value >= v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER_EQUAL,
                                            KuduValue::CopyString(v)),
        }));
      }

      { // value <= v
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value <= v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS_EQUAL,
                                            KuduValue::CopyString(v)),
        }));
      }

      { // value > v
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value > v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER,
                                            KuduValue::CopyString(v)),
        }));
      }

      { // value < v
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value < v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS,
                                            KuduValue::CopyString(v)),
        }));
      }

      { // value >= "a"
        // value <= v
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value >= "a" && value <= v; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER_EQUAL,
                                            KuduValue::CopyString("a")),
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS_EQUAL,
                                            KuduValue::CopyString(v)),
        }));
      }

      { // value >= v
        // value <= "a"
        int count = count_if(values.begin(), values.end(),
                             [&] (const string& value) { return value >= v && value <= "a"; });
        ASSERT_EQ(count, CountRows(table, {
              table->NewComparisonPredicate("value",
                                            KuduPredicate::GREATER_EQUAL,
                                            KuduValue::CopyString(v)),
              table->NewComparisonPredicate("value",
                                            KuduPredicate::LESS_EQUAL,
                                            KuduValue::CopyString("a")),
        }));
      }
    }

    // IN list predicates
    std::random_shuffle(test_values.begin(), test_values.end());

    for (auto end = test_values.begin(); end <= test_values.end(); end++) {
      vector<KuduValue*> vals;

      for (auto itr = test_values.begin(); itr != end; itr++) {
        vals.push_back(KuduValue::CopyString(*itr));
      }

      int count = CountMatchedRows<string>(values, vector<string>(test_values.begin(), end));
      ASSERT_EQ(count, CountRows(table, { table->NewInListPredicate("value", &vals) }));
    }
  }

  shared_ptr<KuduClient> client_;
  gscoped_ptr<MiniCluster> cluster_;
};

TEST_F(PredicateTest, TestBoolPredicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::BOOL);
  shared_ptr<KuduSession> session = CreateSession();

  int i = 0;
  for (bool b : { false, true }) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetBool("value", b));
      ASSERT_OK(session->Apply(insert.release()));
  }

  // Insert null value
  unique_ptr<KuduInsert> insert(table->NewInsert());
  ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(session->Apply(insert.release()));
  ASSERT_OK(session->Flush());

  ASSERT_EQ(3, CountRows(table, {}));

  { // value = false
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::EQUAL,
                                                        KuduValue::FromBool(false));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value = true
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::EQUAL,
                                                        KuduValue::FromBool(true));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value >= true
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::GREATER_EQUAL,
                                                        KuduValue::FromBool(true));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value >= false
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::GREATER_EQUAL,
                                                        KuduValue::FromBool(false));
    ASSERT_EQ(2, CountRows(table, { pred }));
  }

  { // value <= false
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::LESS_EQUAL,
                                                        KuduValue::FromBool(false));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value <= true
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::LESS_EQUAL,
                                                        KuduValue::FromBool(true));
    ASSERT_EQ(2, CountRows(table, { pred }));
  }

  { // value > true
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::GREATER,
                                                        KuduValue::FromBool(true));
    ASSERT_EQ(0, CountRows(table, { pred }));
  }

  { // value > false
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::GREATER,
                                                        KuduValue::FromBool(false));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value < false
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::LESS,
                                                        KuduValue::FromBool(false));
    ASSERT_EQ(0, CountRows(table, { pred }));
  }

  { // value < true
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::LESS,
                                                        KuduValue::FromBool(true));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value IN ()
    vector<KuduValue*> values = { };
    KuduPredicate* pred = table->NewInListPredicate("value", &values);
    ASSERT_EQ(0, CountRows(table, { pred }));
  }

  { // value IN (true)
    vector<KuduValue*> values = { KuduValue::FromBool(true) };
    KuduPredicate* pred = table->NewInListPredicate("value", &values);
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value IN (false)
    vector<KuduValue*> values = { KuduValue::FromBool(false) };
    KuduPredicate* pred = table->NewInListPredicate("value", &values);
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value IN (true, false)
    vector<KuduValue*> values = { KuduValue::FromBool(false), KuduValue::FromBool(true) };
    KuduPredicate* pred = table->NewInListPredicate("value", &values);
    ASSERT_EQ(2, CountRows(table, { pred }));
  }
}

TEST_F(PredicateTest, TestInt8Predicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::INT8);
  shared_ptr<KuduSession> session = CreateSession();

  int i = 0;
  for (int8_t value : CreateIntValues<int8_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt8("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int8_t>(table);
}

TEST_F(PredicateTest, TestInt16Predicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::INT16);
  shared_ptr<KuduSession> session = CreateSession();

  int i = 0;
  for (int16_t value : CreateIntValues<int16_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt16("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int16_t>(table);
}

TEST_F(PredicateTest, TestInt32Predicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::INT32);
  shared_ptr<KuduSession> session = CreateSession();

  int i = 0;
  for (int32_t value : CreateIntValues<int32_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt32("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int32_t>(table);
}

TEST_F(PredicateTest, TestInt64Predicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::INT64);
  shared_ptr<KuduSession> session = CreateSession();

  int i = 0;
  for (int64_t value : CreateIntValues<int64_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt64("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int64_t>(table);
}

TEST_F(PredicateTest, TestTimestampPredicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::UNIXTIME_MICROS);
  shared_ptr<KuduSession> session = CreateSession();

  int i = 0;
  for (int64_t value : CreateIntValues<int64_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetUnixTimeMicros("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int64_t>(table);
}

TEST_F(PredicateTest, TestFloatPredicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::FLOAT);
  shared_ptr<KuduSession> session = CreateSession();

  vector<float> values = CreateFloatingPointValues<float>();
  vector<float> test_values = CreateFloatingPointTestValues<float>();

  int i = 0;
  for (float value : values) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetFloat("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  ASSERT_EQ(values.size() + 1, CountRows(table, {}));

  for (float v : test_values) {
    SCOPED_TRACE(strings::Substitute("test value: $0", v));

    { // value = v
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value == v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value", KuduPredicate::EQUAL, KuduValue::FromFloat(v)),
      }));
    }

    { // value >= v
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value >= v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromFloat(v)),
      }));
    }

    { // value <= v
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value <= v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromFloat(v)),
      }));
    }

    { // value > v
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value > v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER,
                                          KuduValue::FromFloat(v)),
      }));
    }

    { // value < v
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value < v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS,
                                          KuduValue::FromFloat(v)),
      }));
    }

    { // value >= 0
      // value <= v
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value >= 0.0 && value <= v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromFloat(0.0)),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromFloat(v)),
      }));
    }

    { // value >= v
      // value <= 0.0
      int count = count_if(values.begin(), values.end(),
                           [&] (float value) { return value >= v && value <= 0.0; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromFloat(v)),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromFloat(0.0)),
      }));
    }
  }

  // IN list predicates
  std::random_shuffle(test_values.begin(), test_values.end());

  for (auto end = test_values.begin(); end <= test_values.end(); end++) {
    vector<KuduValue*> vals;

    for (auto itr = test_values.begin(); itr != end; itr++) {
      vals.push_back(KuduValue::FromFloat(*itr));
    }

    int count = CountMatchedRows<float>(values, vector<float>(test_values.begin(), end));
    ASSERT_EQ(count, CountRows(table, { table->NewInListPredicate("value", &vals) }));
  }
}

TEST_F(PredicateTest, TestDoublePredicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::DOUBLE);
  shared_ptr<KuduSession> session = CreateSession();

  vector<double> values = CreateFloatingPointValues<double>();
  vector<double> test_values = CreateFloatingPointTestValues<double>();

  int i = 0;
  for (double value : values) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetDouble("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  ASSERT_EQ(values.size() + 1, CountRows(table, {}));

  for (double v : test_values) {
    SCOPED_TRACE(strings::Substitute("test value: $0", v));

    { // value = v
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value == v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value", KuduPredicate::EQUAL, KuduValue::FromDouble(v)),
      }));
    }

    { // value >= v
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value >= v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromDouble(v)),
      }));
    }

    { // value <= v
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value <= v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromDouble(v)),
      }));
    }

    { // value > v
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value > v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER,
                                          KuduValue::FromDouble(v)),
      }));
    }

    { // value < v
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value < v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS,
                                          KuduValue::FromDouble(v)),
      }));
    }

    { // value >= 0.0
      // value <= v
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value >= 0.0 && value <= v; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromDouble(0.0)),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromDouble(v)),
      }));
    }

    { // value >= v
      // value <= 0.0
      int count = count_if(values.begin(), values.end(),
                           [&] (double value) { return value >= v && value <= 0.0; });
      ASSERT_EQ(count, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromDouble(v)),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromDouble(0.0)),
      }));
    }
  }

  // IN list predicates
  std::random_shuffle(test_values.begin(), test_values.end());

  for (auto end = test_values.begin(); end <= test_values.end(); end++) {
    vector<KuduValue*> vals;

    for (auto itr = test_values.begin(); itr != end; itr++) {
      vals.push_back(KuduValue::FromDouble(*itr));
    }

    int count = CountMatchedRows<double>(values, vector<double>(test_values.begin(), end));
    ASSERT_EQ(count, CountRows(table, { table->NewInListPredicate("value", &vals) }));
  }
}

TEST_F(PredicateTest, TestStringPredicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::STRING);
  shared_ptr<KuduSession> session = CreateSession();

  vector<string> values = CreateStringValues();
  int i = 0;
  for (const string& value : values) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetStringNoCopy("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckStringPredicates(table);
}

TEST_F(PredicateTest, TestBinaryPredicates) {
  shared_ptr<KuduTable> table = CreateAndOpenTable(KuduColumnSchema::BINARY);
  shared_ptr<KuduSession> session = CreateSession();

  vector<string> values = CreateStringValues();
  int i = 0;
  for (const string& value : values) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetBinaryNoCopy("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckStringPredicates(table);
}

} // namespace client
} // namespace kudu
