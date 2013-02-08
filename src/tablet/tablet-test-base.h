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
#include "gutil/walltime.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"
#include "tablet/tablet.h"


namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

class TestTablet : public ::testing::Test {
public:
  TestTablet() :
    ::testing::Test(),
    env_(Env::Default()),
    schema_(boost::assign::list_of
            (ColumnSchema("key", STRING))
            (ColumnSchema("val", UINT32))
            (ColumnSchema("update_count", UINT32)),
            1),
    arena_(1024, 4*1024*1024)
  {}
protected:
  virtual void SetUp() {
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

    ASSERT_STATUS_OK(env_->GetTestDirectory(&test_dir_));

    test_dir_ += StringPrintf("/%s.%s.%ld",
                              test_info->test_case_name(),
                              test_info->name(),
                              time(NULL));

    LOG(INFO) << "Creating tablet in: " << test_dir_;
    tablet_.reset(new Tablet(schema_, test_dir_));
    ASSERT_STATUS_OK(tablet_->CreateNew());
    ASSERT_STATUS_OK(tablet_->Open());
  }

  void InsertTestRows(int first_row, int count) {
    char buf[256];
    RowBuilder rb(schema_);

    WallTime last_print_time = 0;
    uint64_t last_print_count = 0;

    for (int i = first_row; i < first_row + count; i++) {
      rb.Reset();
      snprintf(buf, sizeof(buf), "hello %d", i);
      rb.AddString(Slice(buf));
      rb.AddUint32(i);
      rb.AddUint32(0);
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

  void VerifyTestRows(int first_row, int expected_count) {
    scoped_ptr<Tablet::RowIterator> iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));
    int batch_size = std::max((size_t)(expected_count / 10),
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

  Env *env_;
  const Schema schema_;
  string test_dir_;
  scoped_ptr<Tablet> tablet_;

  Arena arena_;
};

} // namespace tablet
} // namespace kudu

#endif
