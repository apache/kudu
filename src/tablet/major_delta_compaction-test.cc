// Copyright (c) 2013, Cloudera, inc.
// All rights reserved

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/generic_iterators.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "server/logical_clock.h"
#include "tablet/cfile_set.h"
#include "tablet/delta_compaction.h"
#include "tablet/tablet-test-util.h"
#include "tablet/diskrowset-test-base.h"
#include "util/test_util.h"
#include "gutil/algorithm.h"

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

using metadata::RowSetMetadata;
using strings::Substitute;
using util::gtl::is_sorted;

class TestMajorDeltaCompaction : public KuduRowSetTest {
 public:
  TestMajorDeltaCompaction() :
      KuduRowSetTest(Schema(boost::assign::list_of
                            (ColumnSchema("key", STRING))
                            (ColumnSchema("val1", UINT32))
                            (ColumnSchema("val2", STRING))
                            (ColumnSchema("val3", UINT32))
                            (ColumnSchema("val4", STRING)), 1)),
      mvcc_(scoped_refptr<server::Clock>(
          server::LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp))) {
  }

  struct ExpectedRow {
    string key;
    uint32_t val1;
    string val2;
    uint32_t val3;
    string val4;

    string Formatted() const {
      return strings::Substitute(
        "(string key=$0, uint32 val1=$1, string val2=$2, uint32 val3=$3, string val4=$4)",
        key, val1, val2, val3, val4);
    }
  };

  virtual void SetUp() OVERRIDE {
    KuduRowSetTest::SetUp();
  }

  // Insert data into tablet_, setting up equivalent state in
  // expected_state_.
  void WriteTestTablet(int nrows) {
    WriteTransactionState tx_state;
    RowBuilder rb(schema_);

    for (int i = 0; i < nrows; i++) {
      ExpectedRow row;
      row.key = StringPrintf("hello %08d", i);
      row.val1 = i * 2;
      row.val2 = StringPrintf("a %08d", i * 2);
      row.val3 = i * 10;
      row.val4 = StringPrintf("b %08d", i * 10);

      rb.Reset();
      tx_state.Reset();
      rb.AddString(row.key);
      rb.AddUint32(row.val1);
      rb.AddString(row.val2);
      rb.AddUint32(row.val3);
      rb.AddString(row.val4);
      ASSERT_STATUS_OK_FAST(tablet_->InsertForTesting(&tx_state, rb.row()));
      expected_state_.push_back(row);
    }
  }

  // Update the data, touching only odd or even rows based on the
  // value of 'even'.
  // Makes corresponding updates in expected_state_.
  void UpdateRows(int nrows, bool even) {
    WriteTransactionState tx_state;
    faststring update_buf;
    RowChangeListEncoder update(schema_, &update_buf);
    RowBuilder rb(schema_.CreateKeyProjection());
    for (int idx = 0; idx < nrows; idx++) {
      ExpectedRow* row = &expected_state_[idx];
      if ((idx % 2 == 0) == even) {
        tx_state.Reset();

        // Set key
        rb.Reset();
        rb.AddString(row->key);

        // Update the data
        row->val1 *= 2;
        row->val3 *= 2;
        row->val4.append("[U]");

        // Apply the updates.
        Slice val4_slice(row->val4);
        update.AddColumnUpdate(1, &row->val1);
        update.AddColumnUpdate(3, &row->val3);
        update.AddColumnUpdate(4, &val4_slice);
        ASSERT_STATUS_OK(tablet_->MutateRowForTesting(
            &tx_state, rb.row(), schema_, RowChangeList(update_buf)));
      }
    }
  }

  // Verify that the data seen by scanning the tablet matches the data in
  // expected_state_.
  void VerifyData() {
    gscoped_ptr<RowwiseIterator> row_iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &row_iter));
    ASSERT_STATUS_OK(row_iter->Init(NULL));
    vector<string> results;
    ASSERT_STATUS_OK(IterateToStringList(row_iter.get(), &results));
    VLOG(1) << "Results of iterating over the updated materialized rows:";
    ASSERT_EQ(expected_state_.size(), results.size());
    for (int i = 0; i < results.size(); i++) {
      SCOPED_TRACE(Substitute("row $0", i));
      const string& str = results[i];
      const ExpectedRow& expected = expected_state_[i];
      ASSERT_EQ(expected.Formatted(), str);
    }
  }

  MvccManager mvcc_;
  vector<ExpectedRow> expected_state_;
};

// Tests a major delta compaction run.
// Verifies that the output rowset accurately reflects the mutations, but keeps the
// unchanged columns intact.
TEST_F(TestMajorDeltaCompaction, TestCompact) {
  const int kNumRows = 100;
  ASSERT_NO_FATAL_FAILURE(WriteTestTablet(kNumRows));
  ASSERT_STATUS_OK(tablet_->Flush());

  vector<shared_ptr<RowSet> > all_rowsets;
  tablet_->GetRowSetsForTests(&all_rowsets);

  shared_ptr<RowSet> rs = all_rowsets.front();

  // We'll run a few rounds of update/compact to make sure
  // that we don't get into some funny state (regression test for
  // an earlier bug).
  for (int i = 0; i < 3; i++) {
    SCOPED_TRACE(Substitute("Update/compact round $0", i));
    // Update the even rows and verify.
    ASSERT_NO_FATAL_FAILURE(UpdateRows(kNumRows, false));
    ASSERT_NO_FATAL_FAILURE(VerifyData());

    // Flush the deltas, make sure data stays the same.
    ASSERT_STATUS_OK(tablet_->FlushBiggestDMS());
    ASSERT_NO_FATAL_FAILURE(VerifyData());

    // Update the odd rows and flush deltas
    ASSERT_NO_FATAL_FAILURE(UpdateRows(kNumRows, true));
    ASSERT_STATUS_OK(tablet_->FlushBiggestDMS());
    ASSERT_NO_FATAL_FAILURE(VerifyData());

    // Compact two columns, data should stay the same.
    vector<size_t> cols;
    cols.push_back(1);
    cols.push_back(3);
    ASSERT_STATUS_OK(tablet_->DoMajorDeltaCompaction(cols, rs));

    ASSERT_NO_FATAL_FAILURE(VerifyData());
  }
}

} // namespace tablet
} // namespace kudu
