// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_BASE_H
#define KUDU_TABLET_TABLET_TEST_BASE_H

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/thread/thread.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <vector>

#include "common/row.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "gutil/strings/util.h"
#include "gutil/walltime.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/stopwatch.h"
#include "util/test_graph.h"
#include "util/test_macros.h"
#include "util/test_util.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-util.h"


namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

// The base class takes as a template argument a "setup" class
// which can customize the schema for the tests. This way we can
// get coverage on various schemas without duplicating test code.
struct StringKeyTestSetup {
public:
  StringKeyTestSetup() :
    test_schema_(boost::assign::list_of
                 (ColumnSchema("key", STRING))
                 (ColumnSchema("val", UINT32))
                 (ColumnSchema("update_count", UINT32)),
                 1)
  {}

  void BuildRowKey(RowBuilder *rb, uint64_t row_idx)
  {
    // This is called from multiple threads, so can't move this buffer
    // to be a class member. However, it's likely to get inlined anyway
    // and loop-hosted.
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);
    rb->AddString(Slice(buf));
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

  Status DoUpdate(Tablet *tablet, uint64_t row_idx, uint32_t *new_val) {
    char keybuf[256];
    FormatKey(keybuf, sizeof(keybuf), row_idx);
    Slice row_key(keybuf);
    *new_val = 10000 + row_idx;

    faststring ubuf;
    RowChangeListEncoder(test_schema_, &ubuf).AddColumnUpdate(1, new_val);
    return tablet->MutateRow(&row_key, RowChangeList(ubuf));
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

  Schema test_schema_;
};


// Setup for testing integer keys
struct IntKeyTestSetup {
public:
  IntKeyTestSetup() :
    test_schema_(boost::assign::list_of
                 (ColumnSchema("key", UINT32))
                 (ColumnSchema("val", UINT32))
                 (ColumnSchema("update_count", UINT32)), 1)
  {}

  void BuildRowKey(RowBuilder *rb, uint64_t i) {
    rb->AddUint32((uint32_t)i);
  }

  void BuildRow(RowBuilder *rb, uint64_t row_idx, uint32_t update_count_val = 0) {
    BuildRowKey(rb, row_idx);
    rb->AddUint32((uint32_t)row_idx);
    rb->AddUint32(update_count_val);
  }

  const Schema &test_schema() const { return test_schema_; }

  string FormatDebugRow(uint64_t row_idx, uint32_t update_count) {
    return StringPrintf(
      "(uint32 key=%d, uint32 val=%ld, uint32 update_count=%d)",
      (uint32_t)row_idx, row_idx, update_count);
  }

  Status DoUpdate(Tablet *tablet, uint64_t row_idx, uint32_t *new_val) {
    uint32_t row_key = row_idx;
    faststring buf;
    *new_val = 10000 + row_idx;
    RowChangeListEncoder(test_schema_, &buf).AddColumnUpdate(1, new_val);
    return tablet->MutateRow(&row_key, RowChangeList(buf));
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

  Schema test_schema_;
};

// Setup for testing nullable columns
struct NullableValueTestSetup {
 public:
  NullableValueTestSetup() :
    test_schema_(boost::assign::list_of
                 (ColumnSchema("key", UINT32))
                 (ColumnSchema("val", UINT32, true))
                 (ColumnSchema("update_count", UINT32)), 1)
  {}

  void BuildRowKey(RowBuilder *rb, uint64_t i) {
    rb->AddUint32((uint32_t)i);
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

  Status DoUpdate(Tablet *tablet, uint64_t row_idx, uint32_t *new_val) {
    uint32_t row_key = row_idx;
    faststring buf;
    *new_val = CalcUpdateValue(row_idx);
    RowChangeListEncoder(test_schema_, &buf).AddColumnUpdate(1, IsNullRow(row_idx) ? new_val : NULL);
    return tablet->MutateRow(&row_key, RowChangeList(buf));
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

  Schema test_schema_;
};


// Use this with TYPED_TEST_CASE from gtest
typedef ::testing::Types<StringKeyTestSetup, IntKeyTestSetup,
                         NullableValueTestSetup> TabletTestHelperTypes;

template<class TESTSETUP>
class TabletTestBase : public KuduTest {
public:
  TabletTestBase() :
    setup_(),
    schema_(setup_.test_schema()),
    arena_(1024, 4*1024*1024)
  {}

  virtual void SetUp() {
    KuduTest::SetUp();
    tablet_dir_ = env_->JoinPathSegments(test_dir_, "tablet");
    LOG(INFO) << "Creating tablet in: " << tablet_dir_;
    tablet_.reset(new Tablet(schema_, tablet_dir_));
    ASSERT_STATUS_OK(tablet_->CreateNew());
    ASSERT_STATUS_OK(tablet_->Open());
  }

  void InsertTestRows(uint64_t first_row, uint64_t count, uint32_t update_count_val,
                      TimeSeries *ts=NULL) {
    RowBuilder rb(schema_);

    uint64_t inserted_since_last_report = 0;
    for (uint64_t i = first_row; i < first_row + count; i++) {
      rb.Reset();
      setup_.BuildRow(&rb, i, update_count_val);
      CHECK_OK(tablet_->Insert(rb.data()));

      if ((inserted_since_last_report++ > 100) && ts) {
        ts->AddValue(static_cast<double>(inserted_since_last_report));
        inserted_since_last_report = 0;
      }
    }

    if (ts) {
      ts->AddValue(static_cast<double>(inserted_since_last_report));
    }
  }

  Status UpdateTestRow(uint64_t row_idx, uint32_t new_val) {
    RowBuilder rb(schema_.CreateKeyProjection());
    setup_.BuildRowKey(&rb, row_idx);

    faststring buf;
    RowChangeListEncoder(schema_, &buf).AddColumnUpdate(2, &new_val);
    return tablet_->MutateRow(rb.data().data(), RowChangeList(buf));
  }

  Status DeleteTestRow(uint64_t row_idx) {
    RowBuilder rb(schema_.CreateKeyProjection());
    setup_.BuildRowKey(&rb, row_idx);

    faststring buf;
    RowChangeListEncoder(schema_, &buf).SetToDelete();
    return tablet_->MutateRow(rb.data().data(), RowChangeList(buf));
  }

  template <class RowType>
  void VerifyRow(const RowType& row, uint64_t row_idx, uint32_t update_count) {
    ASSERT_EQ(setup_.FormatDebugRow(row_idx, update_count), schema_.DebugRow(row));
  }

  void VerifyTestRows(uint64_t first_row, uint64_t expected_count) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));
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
      ASSERT_STATUS_OK_FAST(RowwiseIterator::CopyBlock(iter.get(), &block));

      RowBlockRow rb_row = block.row(0);
      DLOG(INFO) << "Fetched batch of " << block.nrows() << "\n"
                 << "First row: " << schema_.DebugRow(rb_row);

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
  // The output is sorted by key.
  Status IterateToStringList(vector<string> *out) {
    gscoped_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(this->tablet_->NewRowIterator(this->schema_, &iter));
    RETURN_NOT_OK(iter->Init(NULL));
    return kudu::tablet::IterateToStringList(iter.get(), out);
  }

  // Return the number of rows in the tablet.
  uint64_t TabletCount() const {
    uint64_t count;
    CHECK_OK(tablet_->CountRows(&count));
    return count;
  }

  const Schema &schema() const {
    return schema_;
  }

  TESTSETUP setup_;

  const Schema schema_;
  gscoped_ptr<Tablet> tablet_;
  string tablet_dir_;

  Arena arena_;
};


} // namespace tablet
} // namespace kudu

#endif
