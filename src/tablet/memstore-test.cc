// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>

#include "common/row.h"
#include "tablet/memstore.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {


class TestMemStore : public ::testing::Test {
public:
  TestMemStore() :
    ::testing::Test(),
    schema_(boost::assign::list_of
            (ColumnSchema("key", STRING))
            (ColumnSchema("val", UINT32)),
            1)
  {}

protected:
  const Schema schema_;
};


TEST_F(TestMemStore, TestInsertAndIterate) {
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


} // namespace tablet
} // namespace kudu
