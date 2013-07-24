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
            1),
    key_schema_(schema_.CreateKeyProjection())
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

  Status CheckRowPresent(const MemRowSet &mrs,
                         const string &key, bool *present) {
    RowBuilder rb(key_schema_);
    rb.AddString(Slice(key));
    RowSetKeyProbe probe(rb.row());

    return mrs.CheckRowPresent(probe, present);
  }

  Status InsertRows(MemRowSet *mrs, int num_rows) {
    RowBuilder rb(schema_);
    char keybuf[256];
    for (uint32_t i = 0; i < num_rows; i++) {
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "hello %d", i);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      RETURN_NOT_OK(mrs->Insert(txid_t(0), rb.row()));
    }

    return Status::OK();
  }

  Status InsertRow(MemRowSet *mrs, const string &key, uint32_t val) {
    ScopedTransaction tx(&mvcc_);
    RowBuilder rb(schema_);
    rb.AddString(key);
    rb.AddUint32(val);
    return mrs->Insert(tx.txid(), rb.row());
  }

  Status UpdateRow(MemRowSet *mrs, const string &key, uint32_t new_val) {
    ScopedTransaction tx(&mvcc_);
    mutation_buf_.clear();
    RowChangeListEncoder update(schema_, &mutation_buf_);
    update.AddColumnUpdate(1, &new_val);

    RowBuilder rb(key_schema_);
    rb.AddString(Slice(key));
    RowSetKeyProbe probe(rb.row());
    return mrs->MutateRow(tx.txid(), probe, RowChangeList(mutation_buf_));
  }

  Status DeleteRow(MemRowSet *mrs, const string &key) {
    ScopedTransaction tx(&mvcc_);
    mutation_buf_.clear();
    RowChangeListEncoder update(schema_, &mutation_buf_);
    update.SetToDelete();

    RowBuilder rb(key_schema_);
    rb.AddString(Slice(key));
    RowSetKeyProbe probe(rb.row());
    return mrs->MutateRow(tx.txid(), probe, RowChangeList(mutation_buf_));
  }

  MvccManager mvcc_;

  faststring mutation_buf_;
  const Schema schema_;
  const Schema key_schema_;
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

TEST_F(TestMemRowSet, TestInsertAndIterateCompoundKey) {

  Schema compound_key_schema(boost::assign::list_of
                              (ColumnSchema("key1", STRING))
                              (ColumnSchema("key2", INT32))
                              (ColumnSchema("val", UINT32)), 2);

  shared_ptr<MemRowSet> mrs(new MemRowSet(compound_key_schema));

  RowBuilder rb(compound_key_schema);
  {
    ScopedTransaction tx(&mvcc_);
    rb.AddString(string("hello world"));
    rb.AddInt32(1);
    rb.AddUint32(12345);
    Status row1 = mrs->Insert(tx.txid(), rb.row());
    ASSERT_STATUS_OK(row1);
  }

  {
    ScopedTransaction tx2(&mvcc_);
    rb.Reset();
    rb.AddString(string("goodbye world"));
    rb.AddInt32(2);
    rb.AddUint32(54321);
    Status row2 = mrs->Insert(tx2.txid(), rb.row());
    ASSERT_STATUS_OK(row2);
  }

  {
    ScopedTransaction tx3(&mvcc_);
    rb.Reset();
    rb.AddString(string("goodbye world"));
    rb.AddInt32(1);
    rb.AddUint32(12345);
    Status row3 = mrs->Insert(tx3.txid(), rb.row());
    ASSERT_STATUS_OK(row3);
  }

  ASSERT_EQ(3, mrs->entry_count());

  gscoped_ptr<MemRowSet::Iterator> iter(mrs->NewIterator());

  // The first row returned from the iterator should
  // be "goodbye" (row3) sorted on the second key
  ASSERT_TRUE(iter->HasNext());
  MRSRow row = iter->GetCurrentRow();
  EXPECT_EQ("(string key1=goodbye world, int32 key2=1, uint32 val=12345)",
            compound_key_schema.DebugRow(row));

  // Next row should be "goodbye" (row2)
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  row = iter->GetCurrentRow();
  EXPECT_EQ("(string key1=goodbye world, int32 key2=2, uint32 val=54321)",
            compound_key_schema.DebugRow(row));

  // Next row should be 'hello world' (row1)
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  row = iter->GetCurrentRow();
  EXPECT_EQ("(string key1=hello world, int32 key2=1, uint32 val=12345)",
            compound_key_schema.DebugRow(row));

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

TEST_F(TestMemRowSet, TestDelete) {
  const char kRowKey[] = "hello world";
  bool present;

  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

  // Insert row.
  ASSERT_STATUS_OK(InsertRow(mrs.get(), kRowKey, 1));
  MvccSnapshot snapshot_before_delete(mvcc_);

  // CheckRowPresent should return true
  ASSERT_STATUS_OK(CheckRowPresent(*mrs, kRowKey, &present));
  EXPECT_TRUE(present);

  // Delete it.
  ASSERT_STATUS_OK(DeleteRow(mrs.get(), kRowKey));
  MvccSnapshot snapshot_after_delete(mvcc_);

  // CheckRowPresent should return false
  ASSERT_STATUS_OK(CheckRowPresent(*mrs, kRowKey, &present));
  EXPECT_FALSE(present);

  // Trying to Delete again or Update should get an error.
  Status s = DeleteRow(mrs.get(), kRowKey);
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();

  s = UpdateRow(mrs.get(), kRowKey, 12345);
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();

  // Re-insert a new row with the same key.
  ASSERT_STATUS_OK(InsertRow(mrs.get(), kRowKey, 2));
  MvccSnapshot snapshot_after_reinsert(mvcc_);

  // CheckRowPresent should return now return true
  ASSERT_STATUS_OK(CheckRowPresent(*mrs, kRowKey, &present));
  EXPECT_TRUE(present);

  // Verify the MVCC contents of the memrowset.
  // NOTE: the REINSERT has txid 4 because of the two failed attempts
  // at mutating the deleted row above -- each of them grabs a txid even
  // though it doesn't actually make any successful mutations.
  vector<string> rows;
  mrs->DebugDump(&rows);
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ("@0: row (string key=hello world, uint32 val=1) mutations="
            "[@1(DELETE), "
            "@4(REINSERT (string key=hello world, uint32 val=2))]",
            rows[0]);

  // Verify that iterating the rowset at the first snapshot shows the row.
  ASSERT_STATUS_OK(DumpRowSet(*mrs, schema_, snapshot_before_delete, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ("(string key=hello world, uint32 val=1)", rows[0]);

  // Verify that iterating the rowset at the snapshot where it's deleted
  // doesn't show the row.
  ASSERT_STATUS_OK(DumpRowSet(*mrs, schema_, snapshot_after_delete, &rows));
  ASSERT_EQ(0, rows.size());

  // Verify that iterating the rowset after it's re-inserted shows the row.
  ASSERT_STATUS_OK(DumpRowSet(*mrs, schema_, snapshot_after_reinsert, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ("(string key=hello world, uint32 val=2)", rows[0]);
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
      ASSERT_STATUS_OK_FAST(mrs->Insert(tx.txid(), rb.row()));
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
