// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <tr1/memory>

#include "tablet/cfile_set.h"
#include "tablet/layer-test-base.h"
#include "util/test_util.h"

DECLARE_int32(cfile_default_block_size);

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

class TestCFileSet : public KuduTest {
public:
  TestCFileSet() :
    KuduTest(),
    schema_(boost::assign::list_of
            (ColumnSchema("c0", UINT32))
            (ColumnSchema("c1", UINT32))
            (ColumnSchema("c2", UINT32)), 1)
  {}

  void SetUp() {
    KuduTest::SetUp();
    layer_dir_ = GetTestPath("layer");
  }

  // Write out a test layer with two int columns.
  // The first column simply contains the row index.
  // The second contains the row index * 10.
  // The third column contains index * 100, but is never read.
  void WriteTestLayer(int nrows) {
    LayerWriter lw(env_.get(), schema_, layer_dir_,
                   BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));

    ASSERT_STATUS_OK(lw.Open());

    RowBuilder rb(schema_);
    for (int i = 0; i < nrows; i++) {
      rb.Reset();
      rb.AddUint32(i);
      rb.AddUint32(i * 10);
      rb.AddUint32(i * 100);
      ASSERT_STATUS_OK_FAST(lw.WriteRow(rb.data()));
    }
    ASSERT_STATUS_OK(lw.Finish());
  }

protected:
  Schema schema_;
  string layer_dir_;
};


TEST_F(TestCFileSet, TestPartiallyMaterialize) {
  google::FlagSaver saver;
  // Use a small cfile block size, so that when we skip materializing a given
  // column for 10,000 rows, it can actually skip over a number of blocks.
  FLAGS_cfile_default_block_size = 512;

  const int kCycleInterval = 10000;
  const int kNumRows = 100000;
  WriteTestLayer(kNumRows);

  shared_ptr<CFileSet> fileset(
    new CFileSet(env_.get(), layer_dir_, schema_));
  ASSERT_STATUS_OK(fileset->OpenAllColumns());

  gscoped_ptr<CFileSet::Iterator> iter(fileset->NewIterator(schema_));
  ASSERT_STATUS_OK(iter->Init(NULL));

  Arena arena(4096, 1024*1024);
  RowBlock block(schema_, 100, &arena);
  size_t row_idx = 0;
  while (iter->HasNext()) {
    arena.Reset();

    size_t n = block.nrows();
    ASSERT_STATUS_OK_FAST(iter->PrepareBatch(&n));

    // Cycle between:
    // 0: materializing just column 0
    // 1: materializing just column 1
    // 2: materializing both column 0 and 1
    // NOTE: column 2 ("c2") is never materialized, even though it was part of
    // the projection. It should thus do no IO.
    int cycle = (row_idx / kCycleInterval) % 3;
    if (cycle == 0 || cycle == 2) {
      ColumnBlock col(block.column_block(0));
      ASSERT_STATUS_OK_FAST(iter->MaterializeColumn(0, &col));

      // Verify
      for (int i = 0; i < n; i++) {
        uint32_t got = *reinterpret_cast<uint32_t *>(col.cell_ptr(i));
        if (got != row_idx + i) {
          FAIL() << "Failed at row index " << (row_idx + i) << ": expected "
                 << (row_idx + i) << " got " << got;
        }
      }
    }
    if (cycle == 1 || cycle == 2) {
      ColumnBlock col(block.column_block(1));
      ASSERT_STATUS_OK_FAST(iter->MaterializeColumn(1, &col));

      // Verify
      for (int i = 0; i < n; i++) {
        uint32_t got = *reinterpret_cast<uint32_t *>(col.cell_ptr(i));
        if (got != 10 * (row_idx + i)) {
          FAIL() << "Failed at row index " << (row_idx + i) << ": expected "
                 << 10 * (row_idx + i) << " got " << got;
        }
      }
    }

    ASSERT_STATUS_OK_FAST(iter->FinishBatch());
    row_idx += n;
  }

  // Verify through the iterator statistics that IO was saved by not materializing
  // all of the columns.
  vector<CFileIterator::IOStatistics> stats;
  iter->GetIOStatistics(&stats);
  ASSERT_EQ(3, stats.size());
  for (int i = 0; i < 3; i++) {
    LOG(INFO) << "Col " << i << " stats: " << stats[i].ToString();
  }

  // Since we pushed down the block size, we expect to have read 100+ blocks of column 0
  ASSERT_GT(stats[0].data_blocks_read, 100);

  // Since we didn't ever materialize column 2, we shouldn't have read any data blocks.
  // TODO: currently, the Seek() when we open the iterator is not lazy, so we always
  // read at least the very first data block here. Address this later.
  ASSERT_EQ(1, stats[2].data_blocks_read);

  // Column 0 and 1 skipped a lot of blocks, so should not have read all rows.
  ASSERT_LT(stats[0].rows_read, kNumRows * 3 / 4);
  ASSERT_LT(stats[1].rows_read, kNumRows * 3 / 4);
}

} // namespace tablet
} // namespace kudu

