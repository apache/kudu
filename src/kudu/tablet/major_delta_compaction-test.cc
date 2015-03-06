// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/generic_iterators.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/server/logical_clock.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/diskrowset-test-base.h"
#include "kudu/util/test_util.h"
#include "kudu/gutil/algorithm.h"

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

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
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow ins_row(&client_schema_);

    for (int i = 0; i < nrows; i++) {
      ExpectedRow row;
      row.key = StringPrintf("hello %08d", i);
      row.val1 = i * 2;
      row.val2 = StringPrintf("a %08d", i * 2);
      row.val3 = i * 10;
      row.val4 = StringPrintf("b %08d", i * 10);

      int col = 0;
      CHECK_OK(ins_row.SetString(col++, row.key));
      CHECK_OK(ins_row.SetUInt32(col++, row.val1));
      CHECK_OK(ins_row.SetString(col++, row.val2));
      CHECK_OK(ins_row.SetUInt32(col++, row.val3));
      CHECK_OK(ins_row.SetString(col++, row.val4));
      ASSERT_STATUS_OK_FAST(writer.Insert(ins_row));
      expected_state_.push_back(row);
    }
  }

  // Update the data, touching only odd or even rows based on the
  // value of 'even'.
  // Makes corresponding updates in expected_state_.
  void UpdateRows(int nrows, bool even) {
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow prow(&client_schema_);
    for (int idx = 0; idx < nrows; idx++) {
      ExpectedRow* row = &expected_state_[idx];
      if ((idx % 2 == 0) == even) {
        // Set key
        CHECK_OK(prow.SetString(0, row->key));

        // Update the data
        row->val1 *= 2;
        row->val3 *= 2;
        row->val4.append("[U]");

        // Apply the updates.
        CHECK_OK(prow.SetUInt32(1, row->val1));
        CHECK_OK(prow.SetUInt32(3, row->val3));
        CHECK_OK(prow.SetString(4, row->val4));
        ASSERT_STATUS_OK(writer.Update(prow));
      }
    }
  }

  // Verify that the data seen by scanning the tablet matches the data in
  // expected_state_.
  void VerifyData() {
    gscoped_ptr<RowwiseIterator> row_iter;
    ASSERT_STATUS_OK(tablet()->NewRowIterator(client_schema_, &row_iter));
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
  ASSERT_STATUS_OK(tablet()->Flush());

  vector<shared_ptr<RowSet> > all_rowsets;
  tablet()->GetRowSetsForTests(&all_rowsets);

  shared_ptr<RowSet> rs = all_rowsets.front();

  vector<size_t> cols_to_compact = boost::assign::list_of(1) (3) (4);

  // We'll run a few rounds of update/compact to make sure
  // that we don't get into some funny state (regression test for
  // an earlier bug).
  // We first compact all the columns, then for each other round we do one less,
  // so that we test a few combinations.
  for (int i = 0; i < 3; i++) {
    SCOPED_TRACE(Substitute("Update/compact round $0", i));
    // Update the even rows and verify.
    ASSERT_NO_FATAL_FAILURE(UpdateRows(kNumRows, false));
    ASSERT_NO_FATAL_FAILURE(VerifyData());

    // Flush the deltas, make sure data stays the same.
    ASSERT_STATUS_OK(tablet()->FlushBiggestDMS());
    ASSERT_NO_FATAL_FAILURE(VerifyData());

    // Update the odd rows and flush deltas
    ASSERT_NO_FATAL_FAILURE(UpdateRows(kNumRows, true));
    ASSERT_STATUS_OK(tablet()->FlushBiggestDMS());
    ASSERT_NO_FATAL_FAILURE(VerifyData());

    // Major compact some columns.
    vector<size_t> cols;
    for (int col_index = 0; col_index < cols_to_compact.size() - i; col_index++) {
      cols.push_back(cols_to_compact[col_index]);
    }
    ASSERT_STATUS_OK(tablet()->DoMajorDeltaCompaction(cols, rs));

    ASSERT_NO_FATAL_FAILURE(VerifyData());
  }
}

} // namespace tablet
} // namespace kudu
