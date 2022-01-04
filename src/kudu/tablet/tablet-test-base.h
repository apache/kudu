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
#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/env.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_graph.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tablet {

// The base class takes as a template argument a "setup" class
// which can customize the schema for the tests. This way we can
// get coverage on various schemas without duplicating test code.
struct StringKeyTestSetup {
  static Schema CreateSchema() {
    return Schema({ ColumnSchema("key", STRING),
                    ColumnSchema("key_idx", INT32),
                    ColumnSchema("val", INT32) },
                  1);
  }

  void BuildRowKey(KuduPartialRow *row, int64_t key_idx) {
    // This is called from multiple threads, so can't move this buffer
    // to be a class member. However, it's likely to get inlined anyway
    // and loop-hosted.
    char buf[256];
    FormatKey(buf, sizeof(buf), key_idx);
    CHECK_OK(row->SetStringCopy(0, Slice(buf)));
  }

  // builds a row key from an existing row for updates
  void BuildRowKeyFromExistingRow(KuduPartialRow *row, const RowBlockRow& src_row) {
    CHECK_OK(row->SetStringCopy(0, *reinterpret_cast<const Slice*>(src_row.cell_ptr(0))));
  }

  void BuildRow(KuduPartialRow *row, int64_t key_idx, int32_t val = 0) {
    BuildRowKey(row, key_idx);
    CHECK_OK(row->SetInt32(1, key_idx));
    CHECK_OK(row->SetInt32(2, val));
  }

  static void FormatKey(char *buf, size_t buf_size, int64_t key_idx) {
    snprintf(buf, buf_size, "hello %" PRId64, key_idx);
  }

  std::string FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
    char buf[256];
    FormatKey(buf, sizeof(buf), key_idx);

    return strings::Substitute(
        R"((string key="$0", int32 key_idx=$1, int32 val=$2))",
        buf, key_idx, val);
  }

  // Slices can be arbitrarily large
  // but in practice tests won't overflow a uint64_t
  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint64_t>::max() - 1;
  }
};

// Setup for testing composite keys
struct CompositeKeyTestSetup {
  static Schema CreateSchema() {
    return Schema({ ColumnSchema("key1", STRING),
                    ColumnSchema("key2", INT32),
                    ColumnSchema("key_idx", INT32),
                    ColumnSchema("val", INT32) },
                  2);
  }

  // builds a row key from an existing row for updates
  void BuildRowKeyFromExistingRow(KuduPartialRow *row, const RowBlockRow& src_row) {
    CHECK_OK(row->SetStringCopy(0, *reinterpret_cast<const Slice*>(src_row.cell_ptr(0))));
    CHECK_OK(row->SetInt32(1, *reinterpret_cast<const int32_t*>(src_row.cell_ptr(1))));
  }

  static void FormatKey(char *buf, size_t buf_size, int64_t key_idx) {
    snprintf(buf, buf_size, "hello %" PRId64, key_idx);
  }

  std::string FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
    char buf[256];
    FormatKey(buf, sizeof(buf), key_idx);
    return strings::Substitute(
      "(string key1=$0, int32 key2=$1, int32 val=$2, int32 val=$3)",
      buf, key_idx, key_idx, val);
  }

  // Slices can be arbitrarily large
  // but in practice tests won't overflow a uint64_t
  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint64_t>::max() - 1;
  }
};

// Setup for testing integer keys
template<DataType Type>
struct IntKeyTestSetup {
  static Schema CreateSchema() {
    return Schema({ ColumnSchema("key", Type),
                    ColumnSchema("key_idx", INT32),
                    ColumnSchema("val", INT32) }, 1);
  }

  void BuildRowKey(KuduPartialRow *row, int64_t i) {
    CHECK(false) << "Unsupported type";
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(KuduPartialRow * /*row*/,
                                  const RowType& /*src_row*/) {
    CHECK(false) << "Unsupported type";
  }

  void BuildRow(KuduPartialRow *row, int64_t key_idx,
                int32_t val = 0) {
    BuildRowKey(row, key_idx);
    CHECK_OK(row->SetInt32(1, key_idx));
    CHECK_OK(row->SetInt32(2, val));
  }

  std::string FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
    CHECK(false) << "Unsupported type";
    return "";
  }

  uint64_t GetMaxRows() const {
    return std::numeric_limits<typename DataTypeTraits<Type>::cpp_type>::max() - 1;
  }
};

template<>
void IntKeyTestSetup<INT8>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt8(0, (int8_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<>
void IntKeyTestSetup<INT16>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt16(0, (int16_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<>
void IntKeyTestSetup<INT32>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt32(0, (int32_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<>
void IntKeyTestSetup<INT64>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt64(0, (int64_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<> template<class RowType>
void IntKeyTestSetup<INT8>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                       const RowType& src_row) {
  CHECK_OK(row->SetInt8(0, *reinterpret_cast<const int8_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<INT16>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetInt16(0, *reinterpret_cast<const int16_t*>(src_row.cell_ptr(0))));
}
template<> template<class RowType>
void IntKeyTestSetup<INT32>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetInt32(0, *reinterpret_cast<const int32_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<INT64>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetInt64(0, *reinterpret_cast<const int64_t*>(src_row.cell_ptr(0))));
}

template<>
std::string IntKeyTestSetup<INT8>::FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
  return strings::Substitute(
    "(int8 key=$0, int32 key_idx=$1, int32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

template<>
std::string IntKeyTestSetup<INT16>::FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
  return strings::Substitute(
    "(int16 key=$0, int32 key_idx=$1, int32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

template<>
std::string IntKeyTestSetup<INT32>::FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
  return strings::Substitute(
    "(int32 key=$0, int32 key_idx=$1, int32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

template<>
std::string IntKeyTestSetup<INT64>::FormatDebugRow(int64_t key_idx, int32_t val, bool updated) {
  return strings::Substitute(
    "(int64 key=$0, int32 key_idx=$1, int32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

// Setup for testing nullable columns
struct NullableValueTestSetup {
  static Schema CreateSchema() {
    return Schema({ ColumnSchema("key", INT32),
                    ColumnSchema("key_idx", INT32),
                    ColumnSchema("val", INT32, true) }, 1);
  }

  void BuildRowKey(KuduPartialRow *row, int64_t i) {
    CHECK_OK(row->SetInt32(0, (int32_t)i));
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(KuduPartialRow *row, const RowType& src_row) {
    CHECK_OK(row->SetInt32(0, *reinterpret_cast<const int32_t*>(src_row.cell_ptr(0))));
  }

  void BuildRow(KuduPartialRow *row, int64_t key_idx, int32_t val = 0) {
    BuildRowKey(row, key_idx);
    CHECK_OK(row->SetInt32(1, key_idx));
    if (ShouldInsertAsNull(key_idx)) {
      CHECK_OK(row->SetNull(2));
    } else {
      CHECK_OK(row->SetInt32(2, val));
    }
  }

  std::string FormatDebugRow(int64_t key_idx, int64_t val, bool updated) {
    if (!updated && ShouldInsertAsNull(key_idx)) {
      return strings::Substitute(
      "(int32 key=$0, int32 key_idx=$1, int32 val=NULL)",
        (int32_t)key_idx, key_idx);
    }

    return strings::Substitute(
      "(int32 key=$0, int32 key_idx=$1, int32 val=$2)",
      (int32_t)key_idx, key_idx, val);
  }

  static bool ShouldInsertAsNull(int64_t key_idx) {
    return (key_idx & 2) != 0;
  }

  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint32_t>::max() - 1;
  }
};

// Use this with TYPED_TEST_SUITE from gtest
typedef ::testing::Types<
                         StringKeyTestSetup,
                         IntKeyTestSetup<INT8>,
                         IntKeyTestSetup<INT16>,
                         IntKeyTestSetup<INT32>,
                         IntKeyTestSetup<INT64>,
                         NullableValueTestSetup
                         > TabletTestHelperTypes;

template<class TESTSETUP>
class TabletTestBase : public KuduTabletTest {
 public:
  typedef std::function<bool(int32_t, int32_t)> TestRowVerifier;

  TabletTestBase(TabletHarness::Options::ClockType clock_type =
                 TabletHarness::Options::ClockType::LOGICAL_CLOCK) :
    KuduTabletTest(TESTSETUP::CreateSchema(), clock_type),
    setup_(),
    max_rows_(setup_.GetMaxRows()) {
      client_schema_ptr_ = std::make_shared<Schema>(client_schema_);
  }

  // Inserts "count" rows.
  void InsertTestRows(int64_t first_row,
                      int64_t count,
                      int32_t val,
                      TimeSeries *ts = nullptr) {
    InsertOrUpsertTestRows(RowOperationsPB::INSERT, first_row, count, val, ts);
  }

  // Insert "count" rows, ignoring duplicate key errors.
  void InsertIgnoreTestRows(int64_t first_row,
                            int64_t count,
                            int32_t val,
                            TimeSeries *ts = nullptr) {
    InsertOrUpsertTestRows(RowOperationsPB::INSERT_IGNORE, first_row, count, val, ts);
  }

  // Upserts "count" rows.
  void UpsertTestRows(int64_t first_row,
                      int64_t count,
                      int32_t val,
                      TimeSeries *ts = nullptr) {
    InsertOrUpsertTestRows(RowOperationsPB::UPSERT, first_row, count, val, ts);
  }

  // Deletes 'count' rows, starting with 'first_row'.
  void DeleteTestRows(int64_t first_row, int64_t count) {
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    for (auto i = first_row; i < first_row + count; i++) {
      CHECK_OK(DeleteTestRow(&writer, i));
    }
  }

  void InsertOrUpsertTestRows(RowOperationsPB::Type type,
                              int64_t first_row,
                              int64_t count,
                              int32_t val,
                              TimeSeries *ts = nullptr) {
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);

    uint64_t inserted_since_last_report = 0;
    for (int64_t i = first_row; i < first_row + count; i++) {
      setup_.BuildRow(&row, i, val);
      if (type == RowOperationsPB::INSERT) {
        CHECK_OK(writer.Insert(row));
      } else if (type == RowOperationsPB::INSERT_IGNORE) {
        CHECK_OK(writer.InsertIgnore(row));
      } else if (type == RowOperationsPB::UPSERT) {
        CHECK_OK(writer.Upsert(row));
      } else {
        LOG(FATAL) << "bad type: " << type;
      }

      if ((inserted_since_last_report++ > 100) && ts) {
        ts->AddValue(static_cast<double>(inserted_since_last_report));
        inserted_since_last_report = 0;
      }
    }

    if (ts) {
      ts->AddValue(static_cast<double>(inserted_since_last_report));
    }
  }

  // Inserts a single test row.
  Status InsertTestRow(LocalTabletWriter* writer,
                       int64_t key_idx,
                       int32_t val) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRow(&row, key_idx, val);
    return writer->Insert(row);
  }

  Status UpdateTestRow(LocalTabletWriter* writer,
                       int64_t key_idx,
                       int32_t new_val,
                       int num_updates = 1) {
    std::vector<KuduPartialRow> rows;
    for (int i = 0; i < num_updates; i++) {
      KuduPartialRow row(&client_schema_);
      setup_.BuildRowKey(&row, key_idx);

      // Select the col to update (the third if there is only one key
      // or the fourth if there are two col keys).
      int col_idx = schema_.num_key_columns() == 1 ? 2 : 3;
      CHECK_OK(row.SetInt32(col_idx, new_val++));
      rows.emplace_back(std::move(row));
    }
    std::vector<LocalTabletWriter::RowOp> ops;
    ops.reserve(rows.size());
    for (const auto& row : rows) {
      ops.emplace_back(RowOperationsPB::UPDATE, &row);
    }
    return writer->WriteBatch(ops);
  }

  Status UpdateTestRowToNull(LocalTabletWriter* writer,
                             int64_t key_idx) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRowKey(&row, key_idx);

    // select the col to update (the third if there is only one key
    // or the fourth if there are two col keys).
    int col_idx = schema_.num_key_columns() == 1 ? 2 : 3;
    CHECK_OK(row.SetNull(col_idx));
    return writer->Update(row);
  }

  Status DeleteTestRow(LocalTabletWriter* writer, int64_t key_idx) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRowKey(&row, key_idx);
    return writer->Delete(row);
  }

  template <class RowType>
  void VerifyRow(const RowType& row, int32_t key_idx, int32_t val) {
    ASSERT_EQ(setup_.FormatDebugRow(key_idx, val, false), schema_.DebugRow(row));
  }

  void VerifyTestRows(int32_t first_row, uint64_t expected_count) {
    VerifyTestRowsWithVerifier(first_row, expected_count, boost::none);
  }

  void VerifyTestRowsWithVerifier(int32_t first_row, uint64_t expected_count,
                                  const boost::optional<TestRowVerifier>& verifier) {
    std::unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_ptr_, &iter));
    VerifyTestRowsWithRowIteratorAndVerifier(first_row, expected_count, std::move(iter), verifier);
  }

  void VerifyTestRowsWithTimestampAndVerifier(int32_t first_row, uint64_t expected_count,
                                              Timestamp timestamp,
                                              const boost::optional<TestRowVerifier>& verifier) {
    RowIteratorOptions opts;
    opts.projection = client_schema_ptr_;
    opts.snap_to_include = MvccSnapshot(timestamp);
    std::unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(std::move(opts), &iter));
    VerifyTestRowsWithRowIteratorAndVerifier(first_row, expected_count, std::move(iter), verifier);
  }

  void VerifyTestRowsWithRowIteratorAndVerifier(int32_t first_row, uint64_t expected_row_count,
                                                std::unique_ptr<RowwiseIterator> iter,
                                                const boost::optional<TestRowVerifier>& verifier) {
    ASSERT_OK(iter->Init(nullptr));
    int batch_size = std::max<size_t>(1, std::min<size_t>(expected_row_count / 10,
                                                          4L * 1024 * 1024 / schema_.byte_size()));
    RowBlockMemory mem(32 * 1024);
    RowBlock block(&schema_, batch_size, &mem);

    bool check_for_dups = true;
    if (expected_row_count > INT_MAX) {
      check_for_dups = false;
      LOG(WARNING) << "Not checking rows for duplicates -- duplicates expected since "
                   << "there were more than " << INT_MAX << " rows inserted.";
    }

    // Keep a bitmap of which rows have been seen from the requested
    // range.
    std::vector<bool> seen_rows;
    seen_rows.resize(expected_row_count);

    uint64_t actual_row_count = 0;
    while (iter->HasNext()) {
      mem.Reset();
      ASSERT_OK_FAST(iter->NextBlock(&block));

      RowBlockRow rb_row = block.row(0);
      VLOG(2) << Substitute("Fetched batch of $0\nFirst row: $1",
                            block.nrows(), schema_.DebugRow(rb_row));

      for (int i = 0; i < block.nrows(); i++) {
        rb_row.Reset(&block, i);
        int32_t key_idx = *schema_.ExtractColumnFromRow<INT32>(rb_row, 1);
        if (key_idx >= first_row && block.selection_vector()->IsRowSelected(i)) {
          actual_row_count++;
          if (key_idx < first_row + expected_row_count) {
            size_t rel_idx = key_idx - first_row;
            if (check_for_dups && seen_rows[rel_idx]) {
              FAIL() << "Saw row " << key_idx << " twice!\n"
                    << "Row: " << schema_.DebugRow(rb_row);
            }
            seen_rows[rel_idx] = true;
            if (verifier) {
              int32_t val = *schema_.ExtractColumnFromRow<INT32>(rb_row, 2);
              ASSERT_TRUE((*verifier)(key_idx, val))
                  << "Key index: " << key_idx << ", value: " << val;
            }
          }
        }
      }
    }

    // Verify that all the rows were seen.
    for (int i = 0; i < expected_row_count; i++) {
      ASSERT_EQ(true, seen_rows[i]) << "Never saw row " << (i + first_row);
    }
    ASSERT_EQ(expected_row_count, actual_row_count)
        << "Expected row count didn't match actual row count";
    LOG(INFO) << "Successfully verified " << expected_row_count << "rows";
  }

  // Iterate through the full table, stringifying the resulting rows
  // into the given vector. This is only useful in tests which insert
  // a very small number of rows.
  Status IterateToStringList(std::vector<std::string> *out) {
    std::unique_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(this->tablet()->NewRowIterator(this->client_schema_ptr_, &iter));
    RETURN_NOT_OK(iter->Init(nullptr));
    return kudu::tablet::IterateToStringList(iter.get(), out);
  }

  // Return the number of rows in the tablet.
  uint64_t TabletCount() const {
    uint64_t count;
    CHECK_OK(tablet()->CountRows(&count));
    return count;
  }

  // because some types are small we need to
  // make sure that we don't overflow the type on inserts
  // or else we get errors because the key already exists
  uint64_t ClampRowCount(uint64_t proposal) const {
    uint64_t num_rows = std::min(max_rows_, proposal);
    if (num_rows < proposal) {
      LOG(WARNING) << "Clamping max rows to " << num_rows << " to prevent overflow";
    }
    return num_rows;
  }

  TESTSETUP setup_;

  SchemaPtr client_schema_ptr_;
  const uint64_t max_rows_;
};


} // namespace tablet
} // namespace kudu
