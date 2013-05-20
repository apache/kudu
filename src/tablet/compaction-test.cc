// Copyright (c) 2013, Cloudera, inc.
// All rights reserved

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "tablet/compaction.h"
#include "util/test_util.h"

namespace kudu {
namespace tablet {

class TestCompaction : public KuduTest {
 public:
  TestCompaction() :
    schema_(boost::assign::list_of
            (ColumnSchema("key", STRING))
            (ColumnSchema("val", UINT32)),
            1)
  {}

  virtual void SetUp() {
    KuduTest::SetUp();
    rowset_dir_ = GetTestPath("rowset");
  }

  // Insert n_rows rows of data.
  // Each row is the tuple: (string key=hello <n*10 + delta>, val=<n>)
  void InsertRows(MemStore *ms, int n_rows, int delta) {
    RowBuilder rb(schema_);
    char keybuf[256];
    for (uint32_t i = 0; i < n_rows; i++) {
      ScopedTransaction tx(&mvcc_);
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "hello %03d", i * 10 + delta);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      ASSERT_STATUS_OK(ms->Insert(tx.txid(), rb.data()));
    }
  }

  // Update n_rows rows of data.
  // Each row has the key (string key=hello <n*10 + delta>) and its 'val' column
  // is set to new_val.
  void UpdateRows(RowSetInterface *rowset, int n_rows, int delta, uint32_t new_val) {
    char keybuf[256];
    faststring update_buf;
    for (uint32_t i = 0; i < n_rows; i++) {
      SCOPED_TRACE(i);
      ScopedTransaction tx(&mvcc_);
      snprintf(keybuf, sizeof(keybuf), "hello %03d", i * 10 + delta);
      Slice key(keybuf);

      update_buf.clear();
      RowChangeListEncoder update(schema_, &update_buf);
      update.AddColumnUpdate(1, &new_val);
      ASSERT_STATUS_OK(rowset->UpdateRow(tx.txid(), &key, RowChangeList(update_buf)));
    }
  }

  // Iterate over the given compaction input, stringifying and dumping each
  // yielded row to *out
  void IterateInput(CompactionInput *input, vector<string> *out) {
    ASSERT_STATUS_OK(input->Init());
    vector<CompactionInputRow> rows;

    while (input->HasMoreBlocks()) {
      ASSERT_STATUS_OK(input->PrepareBlock(&rows));

      BOOST_FOREACH(const CompactionInputRow &row, rows) {
        string row_str = schema_.DebugRow(row.row_ptr) +
          " mutations: " + Mutation::StringifyMutationList(schema_, row.mutation_head);
        DVLOG(1) << "Iterating row: " << row_str;
        out->push_back(row_str);
      }

      ASSERT_STATUS_OK(input->FinishBlock());
    }
  }

  void DoFlush(CompactionInput *input, const string &out_dir) {
    RowSetWriter rsw(env_.get(), schema_, out_dir,
                   BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));
    ASSERT_STATUS_OK(rsw.Open());
    ASSERT_STATUS_OK(Flush(input, &rsw));
    ASSERT_STATUS_OK(rsw.Finish());
    ASSERT_FILE_EXISTS(env_, RowSet::GetBloomPath(out_dir));
  }

  void FlushAndReopen(const MemStore &ms, const string &out_dir, shared_ptr<RowSet> *rs) {
    gscoped_ptr<CompactionInput> input(CompactionInput::Create(ms, MvccSnapshot(mvcc_)));
    DoFlush(input.get(), out_dir);
    // Re-open it
    ASSERT_STATUS_OK(RowSet::Open(env_.get(), schema_, out_dir, rs));
  }

 protected:
  MvccManager mvcc_;
  Schema schema_;

  string rowset_dir_;
};

TEST_F(TestCompaction, TestMemstoreInput) {
  // Create a memstore with 10 rows and several updates.
  shared_ptr<MemStore> ms(new MemStore(schema_));
  InsertRows(ms.get(), 10, 0);
  UpdateRows(ms.get(), 10, 0, 1);
  UpdateRows(ms.get(), 10, 0, 2);

  // Ensure that the compaction input yields the expected rows
  // and mutations.
  vector<string> out;
  MvccSnapshot snap(mvcc_);
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*ms, snap));
  IterateInput(input.get(), &out);
  ASSERT_EQ(10, out.size());
  ASSERT_EQ("(string key=hello 000, uint32 val=0) mutations: [@10(SET val=1), @20(SET val=2)]",
            out[0]);
  ASSERT_EQ("(string key=hello 090, uint32 val=9) mutations: [@19(SET val=1), @29(SET val=2)]",
            out[9]);
}

TEST_F(TestCompaction, TestRowSetInput) {
  // Create a memstore with a bunch of rows, flush and reopen.
  shared_ptr<RowSet> rs;
  {
    shared_ptr<MemStore> ms(new MemStore(schema_));
    InsertRows(ms.get(), 10, 0);
    FlushAndReopen(*ms, rowset_dir_, &rs);
    ASSERT_NO_FATAL_FAILURE();
  }

  // Update the rows in the rowset.
  UpdateRows(rs.get(), 10, 0, 1);
  UpdateRows(rs.get(), 10, 0, 2);
  // Flush DMS, update some more.
  ASSERT_STATUS_OK(rs->FlushDeltas());
  UpdateRows(rs.get(), 10, 0, 3);
  UpdateRows(rs.get(), 10, 0, 4);

  // Check compaction input
  vector<string> out;
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*rs, MvccSnapshot(mvcc_)));
  IterateInput(input.get(), &out);
  ASSERT_EQ(10, out.size());
  ASSERT_EQ("(string key=hello 000, uint32 val=0) "
            "mutations: [@10(SET val=1), @20(SET val=2), @30(SET val=3), @40(SET val=4)]",
            out[0]);
  ASSERT_EQ("(string key=hello 090, uint32 val=9) "
            "mutations: [@19(SET val=1), @29(SET val=2), @39(SET val=3), @49(SET val=4)]",
            out[9]);
}

// Test case which doesn't do any merging -- just compacts
// a single input rowset (which may be the memstore) into a single
// output rowset (on disk).
TEST_F(TestCompaction, TestOneToOne) {
  // Create a memstore with a bunch of rows and updates.
  shared_ptr<MemStore> ms(new MemStore(schema_));
  InsertRows(ms.get(), 1000, 0);
  UpdateRows(ms.get(), 1000, 0, 1);
  MvccSnapshot snap(mvcc_);

  // Flush it to disk and re-open.
  shared_ptr<RowSet> rs;
  FlushAndReopen(*ms, rowset_dir_, &rs);
  ASSERT_NO_FATAL_FAILURE();

  // Update the rows with some updates that weren't in the snapshot.
  UpdateRows(ms.get(), 1000, 0, 2);

  // Catch the updates that came in after the snapshot flush was made.
  MvccSnapshot snap2(mvcc_);
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*ms, snap2));

  // Add some more updates which come into the new rowset while the "reupdate" is happening.
  UpdateRows(rs.get(), 1000, 0, 3);

  ASSERT_STATUS_OK(ReupdateMissedDeltas(input.get(), snap, snap2, rs->delta_tracker_.get()));

  // If we look at the contents of the RowSet now, we should see the "re-updated" data.
  vector<string> out;
  input.reset(CompactionInput::Create(*rs, MvccSnapshot(mvcc_)));
  IterateInput(input.get(), &out);
  ASSERT_EQ(1000, out.size());
  ASSERT_EQ("(string key=hello 000, uint32 val=1) mutations: [@2000(SET val=2), @3000(SET val=3)]",
            out[0]);

  // And compact (1 input to 1 output)
  MvccSnapshot snap3(mvcc_);  
  gscoped_ptr<CompactionInput> compact_input(CompactionInput::Create(*rs, snap3));
  string compact_dir = GetTestPath("rowset-compacted");
  DoFlush(compact_input.get(), compact_dir);
}

TEST_F(TestCompaction, TestMerge) {
  vector<shared_ptr<RowSet> > rowsets;

  // Create three input rowsets
  for (int delta = 0; delta < 3; delta++) {
    // Create a memstore with a bunch of rows and updates.
    shared_ptr<MemStore> ms(new MemStore(schema_));
    InsertRows(ms.get(), 1000, delta);
    UpdateRows(ms.get(), 1000, delta, 1);

    string dir = GetTestPath(StringPrintf("rowset-%d", delta));

    // Flush it to disk and re-open it.
    shared_ptr<RowSet> rs;
    FlushAndReopen(*ms, dir, &rs);
    ASSERT_NO_FATAL_FAILURE();
    rowsets.push_back(rs);

    // Perform some updates into DMS
    UpdateRows(rs.get(), 1000, delta, 2);
  }

  // Merge them.
  MvccSnapshot merge_snap(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets) {
    merge_inputs.push_back(shared_ptr<CompactionInput>(CompactionInput::Create(*rs, merge_snap)));
  }

  gscoped_ptr<CompactionInput> compact_input(CompactionInput::Merge(merge_inputs, schema_));
  string compact_dir = GetTestPath("rowset-compacted");
  DoFlush(compact_input.get(), compact_dir);

}

}
}
