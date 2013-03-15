// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/row.h"
#include "tablet/memstore.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

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
  void CheckValue(const shared_ptr<MemStore> &ms, string key,
                  uint32_t expected_val) {
    gscoped_ptr<MemStore::Iterator> iter(ms->NewIterator());
    iter->Init();

    Slice keystr_slice(key);
    Slice key_slice(reinterpret_cast<const char *>(&keystr_slice), sizeof(Slice));

    bool exact;
    ASSERT_STATUS_OK(iter->SeekAtOrAfter(key_slice, &exact));
    ASSERT_TRUE(exact) << "unable to seek to key " << key;
    ASSERT_TRUE(iter->HasNext());
    Slice s = iter->GetCurrentRow();
    ASSERT_EQ(schema_.byte_size(), s.size());

    ASSERT_EQ(expected_val, *schema_.ExtractColumnFromRow<UINT32>(s, 1));
  }

  const Schema schema_;
};


TEST_F(TestMemStore, TestInsertAndIterate) {
  shared_ptr<MemStore> ms(new MemStore(schema_));

  RowBuilder rb(schema_);
  rb.AddString(string("hello world"));
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(ms->Insert(rb.data()));

  rb.Reset();
  rb.AddString(string("goodbye world"));
  rb.AddUint32(54321);
  ASSERT_STATUS_OK(ms->Insert(rb.data()));

  ASSERT_EQ(2, ms->entry_count());

  gscoped_ptr<MemStore::Iterator> iter(ms->NewIterator());

  // The first row returned from the iterator should
  // be "goodbye" because 'g' sorts before 'h'
  ASSERT_TRUE(iter->HasNext());
  Slice s = iter->GetCurrentRow();
  ASSERT_EQ(schema_.byte_size(), s.size());
  ASSERT_EQ(Slice("goodbye world"),
            *schema_.ExtractColumnFromRow<STRING>(s, 0));
  ASSERT_EQ(54321,
            *schema_.ExtractColumnFromRow<UINT32>(s, 1));

  // Next row should be 'hello world'
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  s = iter->GetCurrentRow();
  ASSERT_EQ(schema_.byte_size(), s.size());
  ASSERT_EQ(Slice("hello world"),
            *schema_.ExtractColumnFromRow<STRING>(s, 0));
  ASSERT_EQ(12345,
            *schema_.ExtractColumnFromRow<UINT32>(s, 1));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->HasNext());
}

// Test that inserting duplicate key data fails with Status::AlreadyPresent
TEST_F(TestMemStore, TestInsertDuplicate) {
  shared_ptr<MemStore> ms(new MemStore(schema_));

  RowBuilder rb(schema_);
  rb.AddString(string("hello world"));
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(ms->Insert(rb.data()));

  Status s = ms->Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) << "bad status: " << s.ToString();
}

// Test for updating rows in memstore
TEST_F(TestMemStore, TestUpdate) {
  shared_ptr<MemStore> ms(new MemStore(schema_));

  RowBuilder rb(schema_);
  rb.AddString(string("hello world"));
  rb.AddUint32(1);
  ASSERT_STATUS_OK(ms->Insert(rb.data()));

  // Validate insertion
  CheckValue(ms, "hello world", 1);

  // Update a key which exists.
  faststring update_buf;
  RowChangeListEncoder update(schema_, &update_buf);
  Slice key = Slice("hello world");
  uint32_t new_val = 2;
  update.AddColumnUpdate(1, &new_val);
  ASSERT_STATUS_OK(ms->UpdateRow(&key, RowChangeList(update_buf)));

  // Validate the updated value
  CheckValue(ms, "hello world", 2);

  // Try to update a key which doesn't exist - should return NotFound
  key = Slice("does not exist");
  Status s = ms->UpdateRow(&key, RowChangeList(update_buf));
  ASSERT_TRUE(s.IsNotFound()) << "bad status: " << s.ToString();
}

// Test which inserts many rows into memstore and checks for their
// existence
TEST_F(TestMemStore, TestInsertCopiesToArena) {
  shared_ptr<MemStore> ms(new MemStore(schema_));

  RowBuilder rb(schema_);
  char keybuf[256];
  for (uint32_t i = 0; i < 100; i++) {
    rb.Reset();
    snprintf(keybuf, sizeof(keybuf), "hello %d", i);
    rb.AddString(Slice(keybuf));
    rb.AddUint32(i);
    ASSERT_STATUS_OK(ms->Insert(rb.data()));
  }

  // Validate insertion
  for (uint32_t i = 0; i < 100; i++) {
    snprintf(keybuf, sizeof(keybuf), "hello %d", i);
    CheckValue(ms, keybuf, i);
  }

}


TEST_F(TestMemStore, TestMemStoreInsertAndScan) {
  shared_ptr<MemStore> ms(new MemStore(schema_));

  LOG_TIMING(INFO, "Inserting rows") {
    RowBuilder rb(schema_);
    char keybuf[256];
    for (uint32_t i = 0; i < FLAGS_roundtrip_num_rows; i++) {
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "hello %d", i);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      ASSERT_STATUS_OK_FAST(ms->Insert(rb.data()));
    }
  }

  LOG_TIMING(INFO, "Counting rows") {
    int count = ms->entry_count();
    ASSERT_EQ(FLAGS_roundtrip_num_rows, count);
  }
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
