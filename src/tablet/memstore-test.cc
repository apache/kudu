// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>

#include "common/row.h"
#include "tablet/memstore.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using boost::scoped_ptr;

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
  // Check that the given row in the memstore contains the given
  // integer value.
  void CheckValue(const MemStore &ms, string key, uint32_t expected_val) {
    scoped_ptr<MemStore::Iterator> iter(ms.NewIterator());
    Slice keystr_slice(key);
    Slice key_slice(reinterpret_cast<const char *>(&keystr_slice), sizeof(Slice));

    bool exact;
    ASSERT_STATUS_OK(iter->SeekAtOrAfter(key_slice, &exact));
    ASSERT_TRUE(exact) << "unable to seek to key " << key;
    ASSERT_TRUE(iter->IsValid());
    Slice s = iter->GetCurrentRow();
    ASSERT_EQ(schema_.byte_size(), s.size());

    ASSERT_EQ(expected_val, *schema_.ExtractColumnFromRow<UINT32>(s, 1));
  }

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

// Test that inserting duplicate key data fails with Status::AlreadyPresent
TEST_F(TestMemStore, TestInsertDuplicate) {
  MemStore ms(schema_);

  RowBuilder rb(schema_);
  rb.AddString(string("hello world"));
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(ms.Insert(rb.data()));

  Status s = ms.Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) << "bad status: " << s.ToString();
}

// Test for updating rows in memstore
TEST_F(TestMemStore, TestUpdate) {
  MemStore ms(schema_);

  RowBuilder rb(schema_);
  rb.AddString(string("hello world"));
  rb.AddUint32(1);
  ASSERT_STATUS_OK(ms.Insert(rb.data()));

  // Validate insertion
  CheckValue(ms, "hello world", 1);

  // Update a key which exists.
  ScopedRowDelta update(schema_);
  Slice key = Slice("hello world");
  uint32_t new_val = 2;
  update.get().UpdateColumn(schema_, 1, &new_val);
  ASSERT_STATUS_OK(ms.UpdateRow(&key, update.get()));

  // Validate the updated value
  CheckValue(ms, "hello world", 2);

  // Try to update a key which doesn't exist - should return NotFound
  key = Slice("does not exist");
  update.get().UpdateColumn(schema_, 1, &new_val);
  Status s = ms.UpdateRow(&key, update.get());
  ASSERT_TRUE(s.IsNotFound()) << "bad status: " << s.ToString();
}


} // namespace tablet
} // namespace kudu
