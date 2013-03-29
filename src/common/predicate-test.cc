// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>

#include "common/scan_predicate.h"
#include "common/rowblock.h"
#include "util/test_util.h"

namespace kudu {

class TestPredicate : public KuduTest {
public:
  TestPredicate() :
    arena_(1024, 4096),
    n_rows_(100),
    schema_(boost::assign::list_of
           (ColumnSchema("col0", UINT32))
           (ColumnSchema("col1", UINT32)),
            1),
    row_block_(schema_, n_rows_, &arena_)
  {}

  // Set up a block of data with two columns:
  // col0   col1
  // ----   ------
  // 0      0
  // 1      10
  // ...    ...
  // N      N * 10
  void SetUp() {
    ColumnBlock col0 = row_block_.column_block(0, n_rows_);
    ColumnBlock col1 = row_block_.column_block(1, n_rows_);

    for (size_t i = 0; i < n_rows_; i++) {
      *(reinterpret_cast<uint32_t *>(col0.cell_ptr(i))) = i;
      *(reinterpret_cast<uint32_t *>(col1.cell_ptr(i))) = i * 10;
    }
  }

protected:
  Arena arena_;
  const size_t n_rows_;
  Schema schema_;
  ScopedRowBlock row_block_;
};

TEST_F(TestPredicate, TestSelectionVector) {
  SelectionVector selvec(10);
  ASSERT_TRUE(selvec.IsRowSelected(0));
  ASSERT_TRUE(selvec.IsRowSelected(9));
  ASSERT_EQ(10, selvec.CountSelected());
  ASSERT_TRUE(selvec.AnySelected());

  for (int i = 0; i < 10; i++) {
    BitmapClear(selvec.mutable_bitmap(), i);
  }

  ASSERT_FALSE(selvec.AnySelected());
}

TEST_F(TestPredicate, TestColumnRange) {
  SelectionVector selvec(n_rows_);
  ASSERT_EQ(100, selvec.CountSelected());

  // Apply predicate 20 <= col0 <= 29
  uint32_t col0_lower = 20;
  uint32_t col0_upper = 29;
  ColumnRangePredicate pred1(schema_.column(0), &col0_lower, &col0_upper);
  ASSERT_EQ("(`col0` BETWEEN 20 AND 29)", pred1.ToString());
  pred1.Evaluate(&row_block_, &selvec);
  ASSERT_EQ(10, selvec.CountSelected()) << "Only 10 rows should be left (20-29)";

  // Apply predicate col1 >= 250
  uint32_t col1_lower = 250;
  ColumnRangePredicate pred2(schema_.column(1), &col1_lower, boost::none);
  ASSERT_EQ("(`col1` >= 250)", pred2.ToString());
  pred2.Evaluate(&row_block_, &selvec);
  ASSERT_EQ(5, selvec.CountSelected()) << "Only 5 rows should be left (25-29)";
}

}
