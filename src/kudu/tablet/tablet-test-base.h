// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_BASE_H
#define KUDU_TABLET_TABLET_TEST_BASE_H

#include <boost/assign/list_of.hpp>
#include <boost/thread/thread.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_graph.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/gutil/strings/numbers.h"


namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

// The base class takes as a template argument a "setup" class
// which can customize the schema for the tests. This way we can
// get coverage on various schemas without duplicating test code.
struct StringKeyTestSetup {
 public:
  StringKeyTestSetup() :
    test_schema_(CreateSchema()),
    test_key_schema_(test_schema_.CreateKeyProjection())
  {}

  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                   (ColumnSchema("key", STRING))
                   (ColumnSchema("val", UINT32))
                   (ColumnSchema("update_count", UINT32)),
                   1);
  }

  void BuildRowKey(RowBuilder *rb, uint64_t row_idx) {
    // This is called from multiple threads, so can't move this buffer
    // to be a class member. However, it's likely to get inlined anyway
    // and loop-hosted.
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);
    rb->AddString(Slice(buf));
  }

  // builds a row key from an existing row for updates
  template <class RowType>
  void BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
    rb->AddString(*reinterpret_cast<const Slice*>(row.cell_ptr(0)));
  }

  void BuildRow(RowBuilder *rb, uint64_t row_idx, uint32_t update_count_val = 0) {
    BuildRowKey(rb, row_idx);
    rb->AddUint32(row_idx);
    rb->AddUint32(update_count_val);
  }

  const Schema &test_schema() const {
    return test_schema_;
  }

  static void FormatKey(char *buf, size_t buf_size, uint64_t row_idx) {
    snprintf(buf, buf_size, "hello %ld", row_idx);
  }

  string FormatDebugRow(uint64_t row_idx, uint32_t update_count) {
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);

    return StringPrintf(
      "(string key=%s, uint32 val=%ld, uint32 update_count=%d)",
      buf, row_idx, update_count);
  }

  Status DoUpdate(WriteTransactionState *tx_state,
                  Tablet *tablet,
                  uint64_t row_idx,
                  uint32_t *new_val) {
    RowBuilder rb(test_key_schema_);
    BuildRowKey(&rb, row_idx);
    *new_val = 10000 + row_idx;

    faststring ubuf;
    RowChangeListEncoder(&test_schema_, &ubuf).AddColumnUpdate(1, new_val);
    return tablet->MutateRowForTesting(tx_state, rb.row(), test_schema_, RowChangeList(ubuf));
  }

  template <class RowType>
  uint64_t GetRowIndex(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 1);
  }

  template <class RowType>
  uint64_t GetRowValueAfterUpdate(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 1);
  }

  bool ShouldUpdateRow(uint64_t row_idx) const {
    return (row_idx % 15) == 0;
  }

  uint64_t GetSizeOfKey() const {
    return sizeof(DataTypeTraits<STRING>::cpp_type);
  }

  // Slices can be arbitrarily large
  // but in practice tests won't overflow a uint64_t
  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint64_t>::max() - 1;
  }

  Schema test_schema_;
  Schema test_key_schema_;
};

// Setup for testing composite keys
struct CompositeKeyTestSetup {
 public:
  CompositeKeyTestSetup() :
    test_schema_(CreateSchema()),
    test_key_schema_(test_schema_.CreateKeyProjection())
  {}

  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key1", STRING))
                  (ColumnSchema("key2", UINT32))
                  (ColumnSchema("val", UINT32))
                  (ColumnSchema("update_count", UINT32)),
                  2);
  }

  void BuildRowKey(RowBuilder *rb, uint64_t row_idx) {
    // This is called from multiple threads, so can't move this buffer
    // to be a class member. However, it's likely to get inlined anyway
    // and loop-hosted.
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);
    rb->AddString(Slice(buf));
    rb->AddUint32(row_idx);
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
    rb->AddString(*reinterpret_cast<const Slice*>(row.cell_ptr(0)));
    rb->AddUint32(*reinterpret_cast<const uint32_t*>(row.cell_ptr(1)));
  }

  void BuildRow(RowBuilder *rb, uint64_t row_idx,
                uint32_t update_count_val = 0) {
    BuildRowKey(rb, row_idx);
    rb->AddUint32(row_idx);
    rb->AddUint32(update_count_val);
  }

  const Schema &test_schema() const {
    return test_schema_;
  }

  static void FormatKey(char *buf, size_t buf_size, uint64_t row_idx) {
    snprintf(buf, buf_size, "hello %ld", row_idx);
  }

  string FormatDebugRow(uint64_t row_idx, uint32_t update_count) {
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);
    return StringPrintf(
      "(string key1=%s, uint32 key2=%ld, uint32 val=%ld, uint32 update_count=%d)",
      buf, row_idx, row_idx, update_count);
  }

  Status DoUpdate(WriteTransactionState *tx_state,
                  Tablet *tablet,
                  uint64_t row_idx,
                  uint32_t *new_val) {
    RowBuilder rb(test_key_schema_);
    BuildRowKey(&rb, row_idx);
    *new_val = 10000 + row_idx;

    faststring ubuf;
    RowChangeListEncoder(&test_schema_, &ubuf).AddColumnUpdate(2, new_val);
    return tablet->MutateRowForTesting(tx_state, rb.row(), test_schema_, RowChangeList(ubuf));
  }

  template <class RowType>
  uint64_t GetRowIndex(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 1);
  }

  template <class RowType>
  uint64_t GetRowValueAfterUpdate(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 2);
  }

  bool ShouldUpdateRow(uint64_t row_idx) const {
    return (row_idx % 15) == 0;
  }

  uint64_t GetSizeOfKey() const {
    return sizeof(DataTypeTraits<STRING>::cpp_type)
        + sizeof(DataTypeTraits<UINT32>::cpp_type);
  }

  // Slices can be arbitrarily large
  // but in practice tests won't overflow a uint64_t
  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint64_t>::max() - 1;
  }

  Schema test_schema_;
  Schema test_key_schema_;
};

// Setup for testing integer keys
template<DataType Type>
struct IntKeyTestSetup {

 public:
  IntKeyTestSetup() :
    test_schema_(CreateSchema()),
    test_key_schema_(test_schema_.CreateKeyProjection()),
    type_info_(GetTypeInfo(Type)) {
  }

  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
              (ColumnSchema("key", Type))
              (ColumnSchema("val", UINT32))
              (ColumnSchema("update_count", UINT32)), 1);
  }

  void BuildRowKey(RowBuilder *rb, int64_t i) {
    CHECK(false) << "Unsupported type";
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
    CHECK(false) << "Unsupported type";
  }

  void BuildRow(RowBuilder *rb, int64_t row_idx,
                uint32_t update_count_val = 0) {
    BuildRowKey(rb, row_idx);
    rb->AddUint32((uint32_t) row_idx);
    rb->AddUint32(update_count_val);
  }

  const Schema &test_schema() const {
    return test_schema_;
  }

  string FormatDebugRow(int64_t row_idx, uint32_t update_count) {
    CHECK(false) << "Unsupported type";
    return "";
  }

  Status DoUpdate(WriteTransactionState *tx_state,
                  Tablet *tablet,
                  int64_t row_idx,
                  uint32_t *new_val) {
    RowBuilder rb(test_key_schema_);
    BuildRowKey(&rb, row_idx);
    faststring buf;
    *new_val = 10000 + row_idx;
    RowChangeListEncoder(&test_schema_, &buf).AddColumnUpdate(1, new_val);
    return tablet->MutateRowForTesting(tx_state, rb.row(), test_schema_, RowChangeList(buf));
  }

  template<class RowType>
  uint64_t GetRowIndex(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 1);
  }

  template<class RowType>
  uint64_t GetRowValueAfterUpdate(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 1);
  }

  bool ShouldUpdateRow(int64_t row_idx) const {
    return (row_idx % 15) == 0;
  }

  uint64_t GetSizeOfKey() const {
    return sizeof(typename DataTypeTraits<Type>::cpp_type);
  }

  uint64_t GetMaxRows() const {
    return std::numeric_limits<typename DataTypeTraits<Type>::cpp_type>::max() - 1;
  }

  Schema test_schema_;
  Schema test_key_schema_;
  const TypeInfo* type_info_;
};

template<>
void IntKeyTestSetup<UINT8>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddUint8((uint8_t) i);
}

template<>
void IntKeyTestSetup<INT8>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddInt8((int8_t) i * (i % 2 == 0 ? -1 : 1));
}

template<>
void IntKeyTestSetup<UINT16>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddUint16((uint16_t) i);
}

template<>
void IntKeyTestSetup<INT16>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddInt16((int16_t) i * (i % 2 == 0 ? -1 : 1));
}

template<>
void IntKeyTestSetup<UINT32>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddUint32((uint32_t) i);
}

template<>
void IntKeyTestSetup<INT32>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddInt32((int32_t) i * (i % 2 == 0 ? -1 : 1));
}

template<>
void IntKeyTestSetup<UINT64>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddUint64((uint64_t) i);
}

template<>
void IntKeyTestSetup<INT64>::BuildRowKey(RowBuilder *rb, int64_t i) {
  rb->AddInt64((int64_t) i * (i % 2 == 0 ? -1 : 1));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT8>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddUint8(*reinterpret_cast<const uint8_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<INT8>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddInt8(*reinterpret_cast<const int8_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT16>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddUint16(*reinterpret_cast<const uint16_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<INT16>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddInt16(*reinterpret_cast<const int16_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT32>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddUint32(*reinterpret_cast<const uint32_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<INT32>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddInt32(*reinterpret_cast<const int32_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT64>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddUint64(*reinterpret_cast<const uint64_t*>(row.cell_ptr(0)));
}

template<> template<class RowType>
void IntKeyTestSetup<INT64>::BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
  rb->AddInt64(*reinterpret_cast<const int64_t*>(row.cell_ptr(0)));
}

template<>
string IntKeyTestSetup<UINT8>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(uint8 key=%d, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const uint8_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<INT8>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(int8 key=%d, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const int8_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<UINT16>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(uint16 key=%d, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const uint16_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<INT16>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(int16 key=%d, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const int16_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<UINT32>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(uint32 key=%d, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const uint32_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<INT32>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(int32 key=%d, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const int32_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<UINT64>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(uint64 key=%ld, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const uint64_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

template<>
string IntKeyTestSetup<INT64>::FormatDebugRow(int64_t row_idx, uint32_t update_count) {
  RowBuilder rb(test_key_schema_);
  BuildRowKey(&rb, row_idx);
  return StringPrintf(
      "(int64 key=%ld, uint32 val=%d, uint32 update_count=%d)",
       *reinterpret_cast<const int64_t *>(rb.data().data()),
       (uint32_t)row_idx,
       update_count);
}

// Setup for testing nullable columns
struct NullableValueTestSetup {
 public:
  NullableValueTestSetup() :
    test_schema_(CreateSchema()),
    test_key_schema_(test_schema_.CreateKeyProjection())
  {}

  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                 (ColumnSchema("key", UINT32))
                 (ColumnSchema("val", UINT32, true))
                 (ColumnSchema("update_count", UINT32)), 1);
  }

  void BuildRowKey(RowBuilder *rb, uint64_t i) {
    rb->AddUint32((uint32_t)i);
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(RowBuilder *rb, const RowType& row) {
    rb->AddUint32(*reinterpret_cast<const uint32_t*>(row.cell_ptr(0)));
  }

  void BuildRow(RowBuilder *rb, uint64_t row_idx, uint32_t update_count_val = 0) {
    BuildRowKey(rb, row_idx);
    if (IsNullRow(row_idx)) {
      rb->AddNull();
    } else {
      rb->AddUint32((uint32_t)row_idx);
    }
    rb->AddUint32(update_count_val);
  }

  const Schema &test_schema() const { return test_schema_; }

  string FormatDebugRow(uint64_t row_idx, uint32_t update_count) {
    if (IsNullRow(row_idx)) {
      return StringPrintf(
      "(uint32 key=%d, uint32 val=NULL, uint32 update_count=%d)",
        (uint32_t)row_idx, update_count);
    }

    return StringPrintf(
      "(uint32 key=%d, uint32 val=%ld, uint32 update_count=%d)",
      (uint32_t)row_idx, row_idx, update_count);
  }

  Status DoUpdate(WriteTransactionState *tx_state,
                  Tablet *tablet,
                  uint64_t row_idx,
                  uint32_t *new_val) {
    RowBuilder rb(test_key_schema_);
    BuildRowKey(&rb, row_idx);
    faststring buf;
    *new_val = CalcUpdateValue(row_idx);
   RowChangeListEncoder(&test_schema_, &buf).AddColumnUpdate(1,
                                                             IsNullRow(row_idx) ?
                                                             new_val : NULL);
    return tablet->MutateRowForTesting(tx_state, rb.row(), test_schema_, RowChangeList(buf));
  }

  template <class RowType>
  uint64_t GetRowIndex(const RowType& row) const {
    return *test_schema_.ExtractColumnFromRow<UINT32>(row, 0);
  }

  template <class RowType>
  uint64_t GetRowValueAfterUpdate(const RowType& row) const {
    uint64_t row_idx = GetRowIndex(row);
    bool is_updated = ShouldUpdateRow(row_idx);
    bool is_null = IsNullRow(row_idx);

    uint64_t expected_val = is_updated ? CalcUpdateValue(row_idx) : row_idx;
    const uint32_t *val = test_schema_.ExtractColumnFromRow<UINT32>(row, 1);
    if (is_updated) {
      if (is_null) {
        DCHECK_EQ(expected_val, *val);
      } else {
        DCHECK(val == NULL) << "Expected NULL found: " << *val;
      }
    } else {
      if (is_null) {
        DCHECK(val == NULL) << "Expected NULL found: " << *val;
      } else {
        DCHECK_EQ(expected_val, *val);
      }
    }

    return expected_val;
  }

  bool IsNullRow(uint64_t row_idx) const {
    return !!(row_idx & 2);
  }

  bool ShouldUpdateRow(uint64_t row_idx) const {
    return (row_idx % 10) == 0;
  }

  uint32_t CalcUpdateValue(uint64_t row_idx) const {
    return 10000 + row_idx;
  }

  uint64_t GetSizeOfKey() const {
    return sizeof(DataTypeTraits<UINT32>::cpp_type);
  }

  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint32_t>::max() - 1;
  }

  Schema test_schema_;
  Schema test_key_schema_;
};


// Use this with TYPED_TEST_CASE from gtest
typedef ::testing::Types<
                         StringKeyTestSetup,
                         IntKeyTestSetup<UINT8>,
                         IntKeyTestSetup<INT8>,
                         IntKeyTestSetup<UINT16>,
                         IntKeyTestSetup<INT16>,
                         IntKeyTestSetup<UINT32>,
                         IntKeyTestSetup<INT32>,
                         IntKeyTestSetup<UINT64>,
                         IntKeyTestSetup<INT64>,
                         NullableValueTestSetup
                         > TabletTestHelperTypes;

template<class TESTSETUP>
class TabletTestBase : public KuduTabletTest {
 public:
  TabletTestBase() :
    KuduTabletTest(TESTSETUP::CreateSchema()),
    setup_(),
    max_rows_(setup_.GetMaxRows()),
    arena_(1024, 4*1024*1024)
  {}

  // Inserts "count" rows.
  void InsertTestRows(uint64_t first_row,
                      uint64_t count,
                      uint32_t update_count_val,
                      TimeSeries *ts = NULL) {
    WriteTransactionState tx_state;
    RowBuilder rb(schema_);

    uint64_t inserted_since_last_report = 0;
    for (uint64_t i = first_row; i < first_row + count; i++) {
      rb.Reset();
      tx_state.Reset();
      setup_.BuildRow(&rb, i, update_count_val);
      CHECK_OK(tablet()->InsertForTesting(&tx_state, rb.row()));

      if ((inserted_since_last_report++ > 100) && ts) {
        ts->AddValue(static_cast<double>(inserted_since_last_report));
        inserted_since_last_report = 0;
      }
    }

    if (ts) {
      ts->AddValue(static_cast<double>(inserted_since_last_report));
    }
  }

  // Inserts a single test row within a transaction.
  void InsertTestRow(WriteTransactionState *tx_state,
                     uint64_t row,
                     uint32_t update_count_val) {
    RowBuilder rb(schema_);
    rb.Reset();
    setup_.BuildRow(&rb, row, update_count_val);
    CHECK_OK(tablet()->InsertForTesting(tx_state, rb.row()));
  }

  Status UpdateTestRow(WriteTransactionState *tx_state,
                       uint64_t row_idx,
                       uint32_t new_val) {
    RowBuilder rb(schema_.CreateKeyProjection());
    setup_.BuildRowKey(&rb, row_idx);

    faststring buf;
    // select the col to update (the third if there is only one key
    // or the fourth if there are two col keys).
    int col_idx = schema_.num_key_columns() == 1 ? 2 : 3;
    RowChangeListEncoder(&schema_, &buf).AddColumnUpdate(col_idx, &new_val);
    return tablet()->MutateRowForTesting(tx_state, rb.row(), schema_, RowChangeList(buf));
  }

  Status DeleteTestRow(WriteTransactionState *tx_state, uint64_t row_idx) {
    RowBuilder rb(schema_.CreateKeyProjection());
    setup_.BuildRowKey(&rb, row_idx);
    faststring buf;
    RowChangeListEncoder(&schema_, &buf).SetToDelete();
    return tablet()->MutateRowForTesting(tx_state, rb.row(), schema_, RowChangeList(buf));
  }

  template <class RowType>
  void VerifyRow(const RowType& row, uint64_t row_idx, uint32_t update_count) {
    ASSERT_EQ(setup_.FormatDebugRow(row_idx, update_count), schema_.DebugRow(row));
  }

  void VerifyTestRows(uint64_t first_row, uint64_t expected_count) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet()->NewRowIterator(schema_, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    int batch_size = std::max(
      (size_t)1, std::min((size_t)(expected_count / 10),
                          4*1024*1024 / schema_.byte_size()));
    Arena arena(32*1024, 256*1024);
    RowBlock block(schema_, batch_size, &arena);

    if (expected_count > INT_MAX) {
      LOG(INFO) << "Not checking rows for duplicates -- duplicates expected since "
                << "there were more than " << INT_MAX << " rows inserted.";
      return;
    }

    // Keep a bitmap of which rows have been seen from the requested
    // range.
    std::vector<bool> seen_rows;
    seen_rows.resize(expected_count);

    while (iter->HasNext()) {
      ASSERT_STATUS_OK_FAST(iter->NextBlock(&block));

      RowBlockRow rb_row = block.row(0);
      if (VLOG_IS_ON(2)) {
        VLOG(2) << "Fetched batch of " << block.nrows() << "\n"
            << "First row: " << schema_.DebugRow(rb_row);
      }

      for (int i = 0; i < block.nrows(); i++) {
        rb_row.Reset(&block, i);
        uint64_t row = setup_.GetRowIndex(rb_row);
        if (row >= first_row && row < first_row + expected_count) {
          size_t idx = row - first_row;
          if (seen_rows[idx]) {
            FAIL() << "Saw row " << row << " twice!\n"
                   << "Row: " << schema_.DebugRow(rb_row);
          }
          seen_rows[idx] = true;
        }
      }
    }

    // Verify that all the rows were seen.
    for (int i = 0; i < expected_count; i++) {
      ASSERT_EQ(true, seen_rows[i]) << "Never saw row: " << (i + first_row);
    }
    LOG(INFO) << "Successfully verified " << expected_count << "rows";
  }

  // Iterate through the full table, stringifying the resulting rows
  // into the given vector. This is only useful in tests which insert
  // a very small number of rows.
  Status IterateToStringList(vector<string> *out) {
    gscoped_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(this->tablet()->NewRowIterator(this->schema_, &iter));
    RETURN_NOT_OK(iter->Init(NULL));
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
    uint64_t num_rows = min(max_rows_, proposal);
    if (num_rows < proposal) {
      LOG(WARNING) << "Clamping max rows to " << num_rows << " to prevent overflow";
    }
    return num_rows;
  }

  TESTSETUP setup_;

  const uint64_t max_rows_;

  Arena arena_;
};


} // namespace tablet
} // namespace kudu

#endif
