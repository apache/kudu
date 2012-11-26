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
  }

  Env *env_;
  const Schema schema_;
  string test_dir_;
};

TEST_F(TestTablet, TestMemStore) {
  MemStore ms(schema_);

  RowBuilder rb(schema_);
  rb.AddString(string("hello world"));
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(ms.Insert(rb.data()));

  rb.Reset();
  rb.AddString(string("goodbye world"));
  rb.AddUint32(54321);
  ASSERT_STATUS_OK(ms.Insert(rb.data()));

  ASSERT_EQ(2, ms.entry_count());

  scoped_ptr<MemStore::Iterator> iter(ms.NewIterator());

  // The first row returned from the iterator should
  // be "goodbye" because 'g' sorts before 'h'
  ASSERT_TRUE(iter->IsValid());
  Slice s = iter->GetCurrentRow();
  ASSERT_EQ(schema_.byte_size(), s.size());
  ASSERT_EQ(Slice("goodbye world"),
            *schema_.ExtractColumnFromRow<STRING>(s, 0));
  ASSERT_EQ(54321,
            *schema_.ExtractColumnFromRow<UINT32>(s, 1));

  // Next row should be 'hello world'
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->IsValid());
  s = iter->GetCurrentRow();
  ASSERT_EQ(schema_.byte_size(), s.size());
  ASSERT_EQ(Slice("hello world"),
            *schema_.ExtractColumnFromRow<STRING>(s, 0));
  ASSERT_EQ(12345,
            *schema_.ExtractColumnFromRow<UINT32>(s, 1));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->IsValid());
}


TEST_F(TestTablet, TestFlush) {
  LOG(INFO) << "Writing tablet in: " << test_dir_;

  Tablet tablet(schema_, test_dir_);
  ASSERT_STATUS_OK(tablet.CreateNew());
  ASSERT_STATUS_OK(tablet.Open());

  
  // Insert 1000 rows into memstore
  RowBuilder rb(schema_);
  char buf[256];
  for (int i = 0; i < 1000; i++) {
    rb.Reset();
    snprintf(buf, sizeof(buf), "hello %d", i);
    rb.AddString(Slice(buf));

    rb.AddUint32(i);
    ASSERT_STATUS_OK(tablet.Insert(rb.data()));
  }

  // Flush it.
  ASSERT_STATUS_OK(tablet.Flush());

  // TODO: assert that the data can still be read after the flush.
}

// Test that inserting a row which already exists causes an AlreadyPresent
// error
TEST_F(TestTablet, TestInsertDuplicateKey) {
  Tablet tablet(schema_, test_dir_);
  ASSERT_STATUS_OK(tablet.CreateNew());
  ASSERT_STATUS_OK(tablet.Open());

  RowBuilder rb(schema_);
  rb.AddString(Slice("hello world"));
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(tablet.Insert(rb.data()));

  // Insert again, should fail!
  Status s = tablet.Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();

  // Flush, and make sure that inserting duplicate still fails
  ASSERT_STATUS_OK(tablet.Flush());

  s = tablet.Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();
}

}
}
