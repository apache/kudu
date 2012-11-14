// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <time.h>

#include "tablet/memstore.h"
#include "tablet/row.h"
#include "tablet/tablet.h"
#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {


Schema CreateTestSchema() {
  ColumnSchema col1(kudu::cfile::STRING);
  ColumnSchema col2(kudu::cfile::UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  return Schema(cols, 1);
}

TEST(TestTablet, TestMemStore) {
  Schema schema = CreateTestSchema();
  MemStore ms(schema);

  RowBuilder rb(schema);
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
  ASSERT_EQ(schema.byte_size(), s.size());
  ASSERT_EQ(Slice("goodbye world"),
            *schema.ExtractColumnFromRow<cfile::STRING>(s, 0));
  ASSERT_EQ(54321,
            *schema.ExtractColumnFromRow<cfile::UINT32>(s, 1));

  // Next row should be 'hello world'
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->IsValid());
  s = iter->GetCurrentRow();
  ASSERT_EQ(schema.byte_size(), s.size());
  ASSERT_EQ(Slice("hello world"),
            *schema.ExtractColumnFromRow<cfile::STRING>(s, 0));
  ASSERT_EQ(12345,
            *schema.ExtractColumnFromRow<cfile::UINT32>(s, 1));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->IsValid());
}


TEST(TestTablet, TestFlush) {
  Env *env = Env::Default();

  string test_dir;
  ASSERT_STATUS_OK(env->GetTestDirectory(&test_dir));

  env->DeleteDir(test_dir);

  test_dir += "/TestTablet.TestFlush" +
    boost::lexical_cast<string>(time(NULL));

  LOG(INFO) << "Writing tablet in: " << test_dir;

  Schema schema = CreateTestSchema();
  Tablet tablet(schema, test_dir);
  ASSERT_STATUS_OK(tablet.CreateNew());
  ASSERT_STATUS_OK(tablet.Open());

  // Insert 1000 rows into memstore
  char buf[256];
  for (int i = 0; i < 1000; i++) {
    RowBuilder rb(schema);
    snprintf(buf, sizeof(buf), "hello %d", i);
    rb.AddString(Slice(buf));

    rb.AddUint32(i);
    ASSERT_STATUS_OK(tablet.Insert(rb.data()));
  }

  // Flush it.
  ASSERT_STATUS_OK(tablet.Flush());

  // TODO: assert that the data can still be read after the flush.
}

}
}
