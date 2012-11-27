// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <time.h>

#include "common/row.h"
#include "tablet/memstore.h"
#include "tablet/tablet.h"
#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {


class TestTablet : public ::testing::Test {
public:
  TestTablet() :
    ::testing::Test(),
    env_(Env::Default()),
    schema_(boost::assign::list_of
            (ColumnSchema("key", STRING))
            (ColumnSchema("val", UINT32)),
            1)
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

  Env *env_;
  const Schema schema_;
  string test_dir_;
  scoped_ptr<Tablet> tablet_;
};

TEST_F(TestTablet, TestFlush) {
  // Insert 1000 rows into memstore
  RowBuilder rb(schema_);
  char buf[256];
  for (int i = 0; i < 1000; i++) {
    rb.Reset();
    snprintf(buf, sizeof(buf), "hello %d", i);
    rb.AddString(Slice(buf));

    rb.AddUint32(i);
    ASSERT_STATUS_OK(tablet_->Insert(rb.data()));
  }

  // Flush it.
  ASSERT_STATUS_OK(tablet_->Flush());

  // TODO: assert that the data can still be read after the flush.
}

// Test that inserting a row which already exists causes an AlreadyPresent
// error
TEST_F(TestTablet, TestInsertDuplicateKey) {
  RowBuilder rb(schema_);
  rb.AddString(Slice("hello world"));
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(tablet_->Insert(rb.data()));

  // Insert again, should fail!
  Status s = tablet_->Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();

  // Flush, and make sure that inserting duplicate still fails
  ASSERT_STATUS_OK(tablet_->Flush());

  s = tablet_->Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();
}

}
}
