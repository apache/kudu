// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_BASE_H
#define KUDU_TABLET_TABLET_TEST_BASE_H

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/thread.hpp>
#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <vector>

#include "common/row.h"
#include "common/schema.h"
#include "gutil/strings/util.h"
#include "gutil/walltime.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"
#include "tablet/tablet.h"


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

  void BuildRow(RowBuilder *rb, uint64_t row_idx)
  {
    // This is called from multiple threads, so can't move this buffer
    // to be a class member. However, it's likely to get inlined anyway
    // and loop-hosted.
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);
    rb->AddString(Slice(buf));
    rb->AddUint32(row_idx);
    rb->AddUint32(0);
  }

  const Schema &test_schema() const {
    return test_schema_;
  }

  static void FormatKey(char *buf, size_t buf_size, uint64_t row_idx) {
    snprintf(buf, buf_size, "hello %ld", row_idx);
  }

  void VerifyRow(const uint8_t *row, uint64_t row_idx) {
    char buf[256];
    FormatKey(buf, sizeof(buf), row_idx);

    string expected = StringPrintf(
      "(string key=%s, uint32 val=%ld, uint32 update_count=0)",
      buf, row_idx);

    ASSERT_EQ(expected, test_schema_.DebugRow(row));
  }

  Status DoUpdate(Tablet *tablet, ScopedRowDelta *delta,
                  uint64_t row_idx, uint32_t new_val) {
    char keybuf[256];
    FormatKey(keybuf, sizeof(keybuf), row_idx);

    Slice key_slice(keybuf);
    delta->get().UpdateColumn(test_schema_, 1, &new_val);
    return tablet->UpdateRow(&key_slice, delta->get());
  }

  Schema test_schema_;
};


// Setup for testing integer keys
struct IntKeyTestSetup {
public:
  IntKeyTestSetup() :
    test_schema_(boost::assign::list_of
                 (ColumnSchema("k1", UINT32))
                 (ColumnSchema("k2", UINT32))
                 (ColumnSchema("k3", UINT32)), 1)
  {}

  void BuildRow(RowBuilder *rb, uint64_t i) {
    rb->AddUint32((uint32_t)i);
    rb->AddUint32((uint32_t)i);
    rb->AddUint32((uint32_t)0);
  }

  const Schema &test_schema() const { return test_schema_; }

  void VerifyRow(const uint8_t *row, uint64_t row_idx) {
    Slice row_slice(row, test_schema_.byte_size());
    ASSERT_EQ((uint32_t)row_idx,
              *test_schema_.ExtractColumnFromRow<UINT32>(row_slice, 0));
  }

  Status DoUpdate(Tablet *tablet, ScopedRowDelta *delta,
                  uint64_t row_idx, uint32_t new_val) {
    uint32_t row_key = row_idx;
    delta->get().UpdateColumn(test_schema_, 1, &new_val);
    return tablet->UpdateRow(&row_key, delta->get());
  }


  Schema test_schema_;

};

// Use this with TYPED_TEST_CASE from gtest
typedef ::testing::Types<StringKeyTestSetup, IntKeyTestSetup> TabletTestHelperTypes;

template<class TESTSETUP>
class TabletTestBase : public ::testing::Test {
public:
  TabletTestBase() :
    setup_(),
    env_(Env::Default()),
    schema_(setup_.test_schema()),
    arena_(1024, 4*1024*1024)
  {}

  virtual void SetUp() {
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

    ASSERT_STATUS_OK(env_->GetTestDirectory(&test_dir_));

    test_dir_ += StringPrintf(
      "/%s.%s.%ld",
      StringReplace(test_info->test_case_name(), "/", "_", true).c_str(),
      test_info->name(),
      time(NULL));

    LOG(INFO) << "Creating tablet in: " << test_dir_;
    tablet_.reset(new Tablet(schema_, test_dir_));
    ASSERT_STATUS_OK(tablet_->CreateNew());
    ASSERT_STATUS_OK(tablet_->Open());
  }

  void InsertTestRows(uint64_t first_row, uint64_t count) {
    RowBuilder rb(schema_);

    WallTime last_print_time = 0;
    uint64_t last_print_count = 0;

    for (uint64_t i = first_row; i < first_row + count; i++) {
      rb.Reset();
      setup_.BuildRow(&rb, i);
      ASSERT_STATUS_OK_FAST(tablet_->Insert(rb.data()));

      if (i % 100 == 0) {
        WallTime now = WallTime_Now();
        uint64_t insert_count = i - first_row;
        if (now > last_print_time + 0.1) {
          int rate = (insert_count - last_print_count) / (now - last_print_time);
          LOG(INFO) << "Insert thread " << boost::this_thread::get_id() << ":\t"
                    << insert_count << " rows (" << rate << "/s)";
          last_print_time = now;
          last_print_count = insert_count;
        }
      }
    }
  }

  void VerifyTestRows(uint64_t first_row, uint64_t expected_count) {
    scoped_ptr<Tablet::RowIterator> iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));
    int batch_size = std::min((size_t)(expected_count / 10),
                              4*1024*1024 / schema_.byte_size());
    scoped_array<uint8_t> buf(new uint8_t[schema_.byte_size() * batch_size]);
    RowBlock block(schema_, &buf[0], batch_size, &arena_);

    // Keep a bitmap of which rows have been seen from the requested
    // range.
    std::vector<bool> seen_rows;
    seen_rows.resize(expected_count);

    while (iter->HasNext()) {
      arena_.Reset();
      size_t n = batch_size;
      ASSERT_STATUS_OK(iter->CopyNextRows(&n, &block));
      LOG(INFO) << "Fetched batch of " << n << "\n"
                << "First row: " << schema_.DebugRow(&buf[0]);

      for (int i = 0; i < n; i++) {
        Slice s(reinterpret_cast<const char *>(&buf[i * schema_.byte_size()]),
                schema_.byte_size());
        int row = *schema_.ExtractColumnFromRow<UINT32>(s, 1);
        if (row >= first_row && row < first_row + expected_count) {
          size_t idx = row - first_row;
          if (seen_rows[idx]) {
            FAIL() << "Saw row " << row << " twice!\n"
                   << "Slice: " << s.ToDebugString() << "\n"
                   << "Row: " << schema_.DebugRow(s.data());
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

  // Return the number of rows in the tablet.
  size_t TabletCount() const {
    size_t count;
    CHECK_OK(tablet_->CountRows(&count));
    return count;
  }

  const Schema &schema() const {
    return schema_;
  }

  TESTSETUP setup_;

  Env *env_;
  const Schema schema_;
  string test_dir_;
  scoped_ptr<Tablet> tablet_;

  Arena arena_;
};


} // namespace tablet
} // namespace kudu

#endif
