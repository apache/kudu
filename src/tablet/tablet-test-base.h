// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_BASE_H
#define KUDU_TABLET_TABLET_TEST_BASE_H

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <tr1/unordered_set>

#include "common/row.h"
#include "common/schema.h"
#include "util/env.h"
#include "util/memory/arena.h"
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
            (ColumnSchema("val", UINT32)),
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
    for (int i = first_row; i < first_row + count; i++) {
      rb.Reset();
      snprintf(buf, sizeof(buf), "hello %d", i);
      rb.AddString(Slice(buf));
      rb.AddUint32(i);
      ASSERT_STATUS_OK_FAST(tablet_->Insert(rb.data()));
    }
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
