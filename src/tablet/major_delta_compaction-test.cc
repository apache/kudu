// Copyright (c) 2013, Cloudera, inc.
// All rights reserved

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "gutil/strings/util.h"
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
using util::gtl::is_sorted;

class TestMajorDeltaCompaction : public KuduRowSetTest {
 public:
  TestMajorDeltaCompaction() :
      KuduRowSetTest(Schema(boost::assign::list_of
                            (ColumnSchema("key", STRING))
                            (ColumnSchema("val1", UINT32))
                            (ColumnSchema("val2", STRING))
                            (ColumnSchema("val3", UINT32))
                            (ColumnSchema("val4", STRING)), 1)) {
  }

  virtual void SetUp() {
    KuduRowSetTest::SetUp();
  }

  void WriteTestRowSet(int nrows) {
    DiskRowSetWriter rsw(rowset_meta_.get(),
                         BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));

    ASSERT_STATUS_OK(rsw.Open());

    RowBuilder rb(schema_);

    for (int i = 0; i < nrows; i++) {
      rb.Reset();
      rb.AddString(StringPrintf("hello %08d", i));
      rb.AddUint32(i * 2);
      rb.AddString(StringPrintf("a %08d", i * 2));
      rb.AddUint32(i * 10);
      rb.AddString(StringPrintf("b %08d", i * 10));
      ASSERT_STATUS_OK_FAST(WriteRow(rb.data(), &rsw));
    }
    ASSERT_STATUS_OK(rsw.Finish());
  }
};


// Tests modifying specified columns of an existing DiskRowSet
TEST_F(TestMajorDeltaCompaction, TestRowSetColumnUpdater) {
  const int kNumRows = 100;
  ASSERT_NO_FATAL_FAILURE(WriteTestRowSet(kNumRows));

  shared_ptr<CFileSet> fileset(new CFileSet(rowset_meta_));
  ASSERT_STATUS_OK(fileset->Open());

  vector<size_t> cols;
  cols.push_back(2);
  cols.push_back(4);

  shared_ptr<RowSetMetadata> rowset_meta_out;

  RowSetColumnUpdater col_updater(tablet_->metadata(),
                                  rowset_meta_.get(),
                                  cols);
  ASSERT_STATUS_OK(col_updater.Open(&rowset_meta_out));

  Schema projection;
  unordered_map<size_t, size_t> old_to_new;
  ASSERT_STATUS_OK(schema_.CreatePartialSchema(cols, &old_to_new, &projection));
  shared_ptr<CFileSet::Iterator> iter(fileset->NewIterator(projection));

  ASSERT_STATUS_OK(iter->Init(NULL));
  Arena arena(1024, 1024);
  RowBlock block(schema_, 100, &arena);

  while (iter->HasNext()) {
    size_t n = block.row_capacity();
    block.arena()->Reset();
    ASSERT_STATUS_OK(iter->PrepareBatch(&n));
    block.Resize(n);
    BOOST_FOREACH(size_t col_idx, cols) {
      size_t new_idx = old_to_new[col_idx];
      ColumnBlock col_block(block.column_block(col_idx));
      iter->MaterializeColumn(new_idx, &col_block);
      for (size_t row_idx = 0; row_idx < col_block.nrows(); row_idx++) {
        // Modify each cell with a deterministic marker: sets the first
        // character to ASCII representation of 'col_idx'. This is used
        // later to verify that updates to this column have been successful.
        Slice* data =
            reinterpret_cast<Slice*>(col_block.cell(row_idx).mutable_ptr());
        data->mutable_data()[0] = '0' + col_idx;
      }
    }
    ASSERT_STATUS_OK(col_updater.AppendColumnsFromRowBlock(block));
    ASSERT_STATUS_OK(iter->FinishBatch());
  }
  ASSERT_STATUS_OK(col_updater.Finish());

  fileset.reset(new CFileSet(rowset_meta_out));
  ASSERT_STATUS_OK(fileset->Open());

  iter.reset(fileset->NewIterator(schema_));
  gscoped_ptr<RowwiseIterator> row_iter(new MaterializingIterator(iter));
  ASSERT_STATUS_OK(row_iter->Init(NULL));

  vector<string> results;
  ASSERT_STATUS_OK(IterateToStringList(row_iter.get(), &results));
  VLOG(1) << "Results of iterating over the updated materialized rows:";
  BOOST_FOREACH(const string &str, results) {
      VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
  string expected_first =
      "(string key=hello 00000000, uint32 val1=0, string val2=2 00000000, uint32 val3=0, string val4=4 00000000)";
  ASSERT_EQ(expected_first, results.front());
  int last_row = kNumRows - 1;
  string expected_last = StringPrintf(
      "(string key=hello %08d, uint32 val1=%d, string val2=2 %08d, uint32 val3=%d, string val4=4 %08d)",
      last_row, last_row * 2, last_row * 2, last_row * 10, last_row * 10);
  ASSERT_EQ(expected_last, results.back());
}

} // namespace tablet
} // namespace kudu
