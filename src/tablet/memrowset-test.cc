// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/row.h"
#include "common/scan_spec.h"
#include "tablet/memrowset.h"
#include "tablet/tablet-test-util.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

class TestMemRowSet : public ::testing::Test {
public:
  TestMemRowSet() :
    ::testing::Test(),
    schema_(boost::assign::list_of
            (ColumnSchema("key", STRING))
            (ColumnSchema("val", UINT32)),
            1)
  {}

protected:
  // Check that the given row in the memrowset contains the given data.
  void CheckValue(const shared_ptr<MemRowSet> &mrs, string key,
                  const string &expected_row) {
    gscoped_ptr<MemRowSet::Iterator> iter(mrs->NewIterator());
    iter->Init(NULL);

    Slice keystr_slice(key);
    Slice key_slice(reinterpret_cast<const char *>(&keystr_slice), sizeof(Slice));

    bool exact;
    ASSERT_STATUS_OK(iter->SeekAtOrAfter(key_slice, &exact));
    ASSERT_TRUE(exact) << "unable to seek to key " << key;
    ASSERT_TRUE(iter->HasNext());

    vector<string> out;
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), &out, 1));
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(expected_row, out[0]) << "bad result for key " << key;
  }

  Status InsertRows(MemRowSet *mrs, int num_rows) {
    RowBuilder rb(schema_);
    char keybuf[256];
    for (uint32_t i = 0; i < num_rows; i++) {
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "hello %d", i);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      RETURN_NOT_OK(mrs->Insert(txid_t(0), rb.data()));
    }

    return Status::OK();
  }

  Status InsertRow(MemRowSet *mrs, const string &key, uint32_t val) {
    ScopedTransaction tx(&mvcc_);
    RowBuilder rb(schema_);
    rb.AddString(key);
    rb.AddUint32(val);
    return mrs->Insert(tx.txid(), rb.data());
  }

  Status UpdateRow(MemRowSet *mrs, const string &key, uint32_t new_val) {
    ScopedTransaction tx(&mvcc_);
    update_buf_.clear();
    RowChangeListEncoder update(schema_, &update_buf_);
    Slice key_slice = Slice(key);
    update.AddColumnUpdate(1, &new_val);
    return mrs->UpdateRow(tx.txid(), &key_slice, RowChangeList(update_buf_));
  }

  MvccManager mvcc_;

  faststring update_buf_;
  const Schema schema_;
};


TEST_F(TestMemRowSet, TestInsertAndIterate) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  ASSERT_STATUS_OK(InsertRow(mrs.get(), "hello world", 12345));
  ASSERT_STATUS_OK(InsertRow(mrs.get(), "goodbye world", 54321));

  ASSERT_EQ(2, mrs->entry_count());

  gscoped_ptr<MemRowSet::Iterator> iter(mrs->NewIterator());

  // The first row returned from the iterator should
  // be "goodbye" because 'g' sorts before 'h'
  ASSERT_TRUE(iter->HasNext());
  MRSRow row = iter->GetCurrentRow();
  EXPECT_EQ("(string key=goodbye world, uint32 val=54321)", schema_.DebugRow(row));

  // Next row should be 'hello world'
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  row = iter->GetCurrentRow();
  EXPECT_EQ("(string key=hello world, uint32 val=12345)", schema_.DebugRow(row));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->HasNext());
}

// Test that inserting duplicate key data fails with Status::AlreadyPresent
TEST_F(TestMemRowSet, TestInsertDuplicate) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  ASSERT_STATUS_OK(InsertRow(mrs.get(), "hello world", 12345));
  Status s = InsertRow(mrs.get(), "hello world", 12345);
  ASSERT_TRUE(s.IsAlreadyPresent()) << "bad status: " << s.ToString();
}

// Test for updating rows in memrowset
TEST_F(TestMemRowSet, TestUpdate) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  ASSERT_STATUS_OK(InsertRow(mrs.get(), "hello world", 1));

  // Validate insertion
  CheckValue(mrs, "hello world", "(string key=hello world, uint32 val=1)");

  // Update a key which exists.
  UpdateRow(mrs.get(), "hello world", 2);

  // Validate the updated value
  CheckValue(mrs, "hello world", "(string key=hello world, uint32 val=2)");

  // Try to update a key which doesn't exist - should return NotFound
  Status s = UpdateRow(mrs.get(), "does not exist", 3);
  ASSERT_TRUE(s.IsNotFound()) << "bad status: " << s.ToString();
}

// Test which inserts many rows into memrowset and checks for their
// existence
TEST_F(TestMemRowSet, TestInsertCopiesToArena) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  InsertRows(mrs.get(), 100);
  // Validate insertion
  char keybuf[256];
  for (uint32_t i = 0; i < 100; i++) {
    snprintf(keybuf, sizeof(keybuf), "hello %d", i);
    CheckValue(mrs, keybuf,
               StringPrintf("(string key=%s, uint32 val=%d)", keybuf, i));
  }

}

TEST_F(TestMemRowSet, TestMemRowSetInsertAndScan) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  LOG_TIMING(INFO, "Inserting rows") {
    InsertRows(mrs.get(), FLAGS_roundtrip_num_rows);
  }

  LOG_TIMING(INFO, "Counting rows") {
    int count = mrs->entry_count();
    ASSERT_EQ(FLAGS_roundtrip_num_rows, count);
  }
}

// Test that scanning at past MVCC snapshots will hide rows which are
// not committed in that snapshot.
TEST_F(TestMemRowSet, TestInsertionMVCC) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));
  vector<MvccSnapshot> snapshots;

  // Insert 5 rows in tx 0 through 4
  for (uint32_t i = 0; i < 5; i++) {
    {
      ScopedTransaction tx(&mvcc_);
      RowBuilder rb(schema_);
      char keybuf[256];
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "tx%d", i);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      ASSERT_STATUS_OK_FAST(mrs->Insert(tx.txid(), rb.data()));
    }

    // Transaction is committed. Save the snapshot after this commit.
    snapshots.push_back(MvccSnapshot(mvcc_));
  }
  LOG(INFO) << "MemRowSet after inserts:";
  mrs->DebugDump();

  ASSERT_EQ(5, snapshots.size());
  for (int i = 0; i < 5; i++) {
    SCOPED_TRACE(i);
    // Each snapshot 'i' is taken after row 'i' was committed.
    vector<string> rows;
    ASSERT_STATUS_OK(kudu::tablet::DumpRowSet(*mrs, schema_, snapshots[i], &rows));
    ASSERT_EQ(1 + i, rows.size());
    string expected = StringPrintf("(string key=tx%d, uint32 val=%d)", i, i);
    ASSERT_EQ(expected, rows[i]);
  }
}

// Test that updates respect MVCC -- i.e. that scanning with a past MVCC snapshot
// will yield old versions of a row which has been updated.
TEST_F(TestMemRowSet, TestUpdateMVCC) {
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  // Insert a row ("myrow", 0)
  ASSERT_STATUS_OK(InsertRow(mrs.get(), "my row", 0));

  vector<MvccSnapshot> snapshots;
  // First snapshot is after insertion
  snapshots.push_back(MvccSnapshot(mvcc_));

  // Update the row 5 times (setting its int column to increasing ints 1-5)
  for (uint32_t i = 1; i <= 5; i++) {
    ASSERT_STATUS_OK(UpdateRow(mrs.get(), "my row", i));

    // Transaction is committed. Save the snapshot after this commit.
    snapshots.push_back(MvccSnapshot(mvcc_));
  }

  LOG(INFO) << "MemRowSet after updates:";
  mrs->DebugDump();

  // Validate that each snapshot returns the expected value
  ASSERT_EQ(6, snapshots.size());
  for (int i = 0; i <= 5; i++) {
    SCOPED_TRACE(i);
    vector<string> rows;
    ASSERT_STATUS_OK(kudu::tablet::DumpRowSet(*mrs, schema_, snapshots[i], &rows));
    ASSERT_EQ(1, rows.size());

    string expected = StringPrintf("(string key=my row, uint32 val=%d)", i);
    LOG(INFO) << "Reading with snapshot " << snapshots[i].ToString() << ": "
              << rows[0];
    EXPECT_EQ(expected, rows[0]);
  }
}

} // namespace tablet
} // namespace kudu
