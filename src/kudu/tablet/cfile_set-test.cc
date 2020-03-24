// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/cfile_set.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/generic_iterators.h"
#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/hash.pb.h"
#include "kudu/util/hash_util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_int32(cfile_default_block_size);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

class TestCFileSet : public KuduRowSetTest {
 public:
  TestCFileSet() :
    KuduRowSetTest(Schema({ ColumnSchema("c0", INT32),
                            ColumnSchema("c1", INT32, false, nullptr, nullptr, GetRLEStorage()),
                            ColumnSchema("c2", INT32, true) }, 1))
  {}

  virtual void SetUp() OVERRIDE {
    KuduRowSetTest::SetUp();

    // Use a small cfile block size, so that when we skip materializing a given
    // column for 10,000 rows, it can actually skip over a number of blocks.
    FLAGS_cfile_default_block_size = 512;
  }

  // Write out a test rowset with two int columns.
  // The first column contains the row index * 2.
  // The second contains the row index * 10.
  // The third column contains index * 100, but is never read.
  void WriteTestRowSet(int nrows) {
    DiskRowSetWriter rsw(rowset_meta_.get(), &schema_,
                         BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));

    ASSERT_OK(rsw.Open());

    RowBuilder rb(&schema_);
    for (int i = 0; i < nrows; i++) {
      rb.Reset();
      rb.AddInt32(i * kRatio[0]);
      rb.AddInt32(i * kRatio[1]);
      rb.AddInt32(i * kRatio[2]);
      ASSERT_OK_FAST(WriteRow(rb.data(), &rsw));
    }
    ASSERT_OK(rsw.Finish());
  }

  // Int32 type add probe to the bloom filter.
  // bf1_contain: 0 2 4 6 8 ... (2n)th key for column 1 to form bloom filter.
  // bf1_exclude: 1 3 5 7 9 ... (2n + 1)th key for column 1 to form bloom filter.
  // bf2_contain: 0 2 4 6 8 ... (2n)th key for column 2 to form bloom filter.
  // bf2_exclude: 1 3 5 7 9 ... (2n + 1)th key for column 2 to form bloom filter.
  static void FillBloomFilter(int nrows,
                              BlockBloomFilter* bf1_contain,
                              BlockBloomFilter* bf1_exclude,
                              BlockBloomFilter* bf2_contain,
                              BlockBloomFilter* bf2_exclude) {
    bool add = true;
    for (int i = 0; i < nrows; ++i) {
      int curr1 = i * kRatio[0];
      int curr2 = i * kRatio[1];
      Slice first(reinterpret_cast<const uint8_t*>(&curr1), sizeof(curr1));
      Slice second(reinterpret_cast<const uint8_t*>(&curr2), sizeof(curr2));
      uint32_t hash1 = HashUtil::ComputeHash32(first, FAST_HASH, 0);
      uint32_t hash2 = HashUtil::ComputeHash32(second, FAST_HASH, 0);

      if (add) {
        bf1_contain->Insert(hash1);
        bf2_contain->Insert(hash2);
      } else {
        bf1_exclude->Insert(hash1);
        bf2_exclude->Insert(hash2);
      }
      add = !add;
    }
  }

  // Int32 type add probe to the bloom filter.
  // ret1_contain: to get the key hits in bf1_contain for column 1.
  // ret1_exclude: to get the key hits in bf1_exclude for column 1.
  // ret2_contain: to get the key hits in bf2_contain for column 2.
  // ret2_exclude: to get the key hits in bf2_exclude for column 2.
  // In some case key may hit both contain and exclude bloom filter
  // so we get accurate item hits the bloom filter for test behind.
  static void GetBloomFilterResult(int nrows,
                                   BlockBloomFilter* bf1_contain,
                                   BlockBloomFilter* bf1_exclude,
                                   BlockBloomFilter* bf2_contain,
                                   BlockBloomFilter* bf2_exclude,
                                   vector<size_t>* ret1_contain,
                                   vector<size_t>* ret1_exclude,
                                   vector<size_t>* ret2_contain,
                                   vector<size_t>* ret2_exclude) {
    for (int i = 0; i < nrows; ++i) {
      int curr1 = i * kRatio[0];
      int curr2 = i * kRatio[1];
      Slice first(reinterpret_cast<const uint8_t*>(&curr1), sizeof(curr1));
      Slice second(reinterpret_cast<const uint8_t*>(&curr2), sizeof(curr2));
      uint32_t hash1 = HashUtil::ComputeHash32(first, FAST_HASH, 0);
      uint32_t hash2 = HashUtil::ComputeHash32(second, FAST_HASH, 0);

      if (bf1_contain->Find(hash1)) {
        ret1_contain->push_back(i);
      }
      if (bf1_exclude->Find(hash1)) {
        ret1_exclude->push_back(i);
      }
      if (bf2_contain->Find(hash2)) {
        ret2_contain->push_back(i);
      }
      if (bf2_exclude->Find(hash2)) {
        ret2_exclude->push_back(i);
      }
    }
  }

  // Issue a range scan between 'lower' and 'upper', and verify that all result
  // rows indeed fall inside that predicate.
  void DoTestRangeScan(const shared_ptr<CFileSet> &fileset,
                       int32_t lower,
                       int32_t upper) {
    // Create iterator.
    unique_ptr<CFileSet::Iterator> cfile_iter(fileset->NewIterator(&schema_, nullptr));
    unique_ptr<RowwiseIterator> iter(NewMaterializingIterator(std::move(cfile_iter)));

    // Create a scan with a range predicate on the key column.
    ScanSpec spec;
    auto pred1 = ColumnPredicate::Range(schema_.column(0),
                                        lower != kNoBound ? &lower : nullptr,
                                        upper != kNoBound ? &upper : nullptr);
    spec.AddPredicate(pred1);
    ASSERT_OK(iter->Init(&spec));

    // Check that the range was respected on all the results.
    Arena arena(1024);
    RowBlock block(&schema_, 100, &arena);
    while (iter->HasNext()) {
      ASSERT_OK_FAST(iter->NextBlock(&block));
      for (size_t i = 0; i < block.nrows(); i++) {
        if (block.selection_vector()->IsRowSelected(i)) {
          RowBlockRow row = block.row(i);
          if ((lower != kNoBound && *schema_.ExtractColumnFromRow<INT32>(row, 0) < lower) ||
              (upper != kNoBound && *schema_.ExtractColumnFromRow<INT32>(row, 0) >= upper)) {
            FAIL() << "Row " << schema_.DebugRow(row) << " should not have "
                   << "passed predicate " << pred1.ToString();
          }
        }
      }
    }
  }

  // Issue a BloomFilter scan and verify that all result
  // rows indeed fall inside that predicate.
  void DoTestBloomFilterScan(const shared_ptr<CFileSet>& fileset,
                             vector<ColumnPredicate> predicates,
                             vector<size_t> target) {
    LOG(INFO) << "predicates size: " << predicates.size();
    // Create iterator.
    unique_ptr<CFileSet::Iterator> cfile_iter(fileset->NewIterator(&schema_, nullptr));
    unique_ptr<RowwiseIterator> iter(NewMaterializingIterator(std::move(cfile_iter)));
    LOG(INFO) << "Target size: " << target.size();
    // Create a scan with a range predicate on the key column.
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    ASSERT_OK(iter->Init(&spec));
    // Check that the range was respected on all the results.
    Arena arena(1024);
    RowBlock block(&schema_, 100, &arena);
    while (iter->HasNext()) {
      ASSERT_OK_FAST(iter->NextBlock(&block));
      for (size_t i = 0; i < block.nrows(); i++) {
        if (block.selection_vector()->IsRowSelected(i)) {
          RowBlockRow row = block.row(i);
          size_t index = row.row_index();
          auto target_iter = std::find(target.begin(), target.end(), index);
          if (target_iter == target.end()) {
            FAIL() << "Row " << schema_.DebugRow(row) << " should not have "
                   << "passed predicate ";
          }
          target.erase(target_iter);
        }
      }
    }
    LOG(INFO) << "Selected size: " << block.selection_vector()->CountSelected();
    if (!target.empty()) {
      FAIL() << "Target size " << target.size() << " should have "
             << "passed predicate ";
    }
  }

  Status MaterializeColumn(CFileSet::Iterator *iter,
                           size_t col_idx,
                           ColumnBlock *cb) {
    SelectionVector sel(cb->nrows());
    ColumnMaterializationContext ctx(col_idx, nullptr, cb, &sel);
    return iter->MaterializeColumn(&ctx);
  }

 private:
  ColumnStorageAttributes GetRLEStorage() const {
    ColumnStorageAttributes attr;
    attr.encoding = RLE;
    return attr;
  }

  static const int kRatio[];

 protected:
  static const int32_t kNoBound;
  google::FlagSaver saver;
};

const int TestCFileSet::kRatio[] = {2, 10, 100};

const int32_t TestCFileSet::kNoBound = kuint32max;

TEST_F(TestCFileSet, TestPartiallyMaterialize) {
  const int kCycleInterval = 10000;
  const int kNumRows = 100000;
  WriteTestRowSet(kNumRows);

  shared_ptr<CFileSet> fileset;
  ASSERT_OK(CFileSet::Open(rowset_meta_, MemTracker::GetRootTracker(), MemTracker::GetRootTracker(),
                           nullptr, &fileset));

  unique_ptr<CFileSet::Iterator> iter(fileset->NewIterator(&schema_, nullptr));
  ASSERT_OK(iter->Init(nullptr));

  Arena arena(4096);
  RowBlock block(&schema_, 100, &arena);
  rowid_t row_idx = 0;
  while (iter->HasNext()) {
    arena.Reset();

    size_t n = block.nrows();
    ASSERT_OK_FAST(iter->PrepareBatch(&n));
    block.Resize(n);

    // Cycle between:
    // 0: materializing just column 0
    // 1: materializing just column 1
    // 2: materializing both column 0 and 1
    // NOTE: column 2 ("c2") is never materialized, even though it was part of
    // the projection. It should thus do no IO.
    int cycle = (row_idx / kCycleInterval) % 3;
    if (cycle == 0 || cycle == 2) {
      ColumnBlock col(block.column_block(0));
      ASSERT_OK_FAST(MaterializeColumn(iter.get(), 0, &col));

      // Verify
      for (int i = 0; i < n; i++) {
        int32_t got = *reinterpret_cast<const int32_t *>(col.cell_ptr(i));
        int32_t expected = (row_idx + i) * 2;
        if (got != expected) {
          FAIL() << "Failed at row index " << (row_idx + i) << ": expected "
                 << expected << " got " << got;
        }
      }
    }
    if (cycle == 1 || cycle == 2) {
      ColumnBlock col(block.column_block(1));
      ASSERT_OK_FAST(MaterializeColumn(iter.get(), 1, &col));

      // Verify
      for (int i = 0; i < n; i++) {
        int32_t got = *reinterpret_cast<const int32_t *>(col.cell_ptr(i));
        if (got != 10 * (row_idx + i)) {
          FAIL() << "Failed at row index " << (row_idx + i) << ": expected "
                 << 10 * (row_idx + i) << " got " << got;
        }
      }
    }

    ASSERT_OK_FAST(iter->FinishBatch());
    row_idx += n;
  }

  // Verify through the iterator statistics that IO was saved by not materializing
  // all of the columns.
  vector<IteratorStats> stats;
  iter->GetIteratorStats(&stats);
  ASSERT_EQ(3, stats.size());
  for (int i = 0; i < 3; i++) {
    LOG(INFO) << "Col " << i << " stats: " << stats[i].ToString();
  }

  // Since we pushed down the block size, we expect to have read 100+ blocks of column 0
  ASSERT_GT(stats[0].blocks_read, 100);

  // Since we didn't ever materialize column 2, we shouldn't have read any data blocks.
  ASSERT_EQ(0, stats[2].blocks_read);

  // Column 0 and 1 skipped a lot of blocks, so should not have read all of the cells
  // from either column.
  ASSERT_LT(stats[0].cells_read, kNumRows * 3 / 4);
  ASSERT_LT(stats[1].cells_read, kNumRows * 3 / 4);
}

TEST_F(TestCFileSet, TestIteratePartialSchema) {
  const int kNumRows = 100;
  WriteTestRowSet(kNumRows);

  shared_ptr<CFileSet> fileset;
  ASSERT_OK(CFileSet::Open(rowset_meta_, MemTracker::GetRootTracker(), MemTracker::GetRootTracker(),
                           nullptr, &fileset));

  Schema new_schema;
  ASSERT_OK(schema_.CreateProjectionByNames({ "c0", "c2" }, &new_schema));
  unique_ptr<CFileSet::Iterator> cfile_iter(fileset->NewIterator(&new_schema, nullptr));
  unique_ptr<RowwiseIterator> iter(NewMaterializingIterator(std::move(cfile_iter)));

  ASSERT_OK(iter->Init(nullptr));

  // Read all the results.
  vector<string> results;
  ASSERT_OK(IterateToStringList(iter.get(), &results));

  VLOG(1) << "Results of iterating over sparse partial schema: ";
  for (const string &str : results) {
    VLOG(1) << str;
  }

  // Ensure that we got the expected rows.
  ASSERT_EQ(results.size(), kNumRows);
  for (int i = 0; i < kNumRows; i++) {
    ASSERT_EQ(StringPrintf("(int32 c0=%d, int32 c2=%d)", i * 2, i * 100),
              results[i]);
  }
}

// Add a range predicate on the key column and ensure that only the relevant small number of rows
// are read off disk.
TEST_F(TestCFileSet, TestRangeScan) {
  const int kNumRows = 10000;
  WriteTestRowSet(kNumRows);

  shared_ptr<CFileSet> fileset;
  ASSERT_OK(CFileSet::Open(rowset_meta_, MemTracker::GetRootTracker(), MemTracker::GetRootTracker(),
                           nullptr, &fileset));

  // Create iterator.
  unique_ptr<CFileSet::Iterator> cfile_iter(fileset->NewIterator(&schema_, nullptr));
  CFileSet::Iterator* cfile_iter_raw = cfile_iter.get();
  unique_ptr<RowwiseIterator> iter(NewMaterializingIterator(std::move(cfile_iter)));
  Schema key_schema = schema_.CreateKeyProjection();
  Arena arena(1024);
  AutoReleasePool pool;

  // Create a scan with a range predicate on the key column.
  ScanSpec spec;
  int32_t lower = 2000;
  int32_t upper = 2010;
  auto pred1 = ColumnPredicate::Range(schema_.column(0), &lower, &upper);
  spec.AddPredicate(pred1);
  spec.OptimizeScan(schema_, &arena, &pool, true);
  ASSERT_OK(iter->Init(&spec));

  // Check that the bounds got pushed as index bounds.
  // Since the key column is the rowidx * 2, we need to divide the integer bounds
  // back down.
  EXPECT_EQ(lower / 2, cfile_iter_raw->lower_bound_idx_);
  EXPECT_EQ(upper / 2, cfile_iter_raw->upper_bound_idx_);

  // Read all the results.
  vector<string> results;
  ASSERT_OK(IterateToStringList(iter.get(), &results));

  // Ensure that we got the expected rows.
  for (const string &str : results) {
    LOG(INFO) << str;
  }
  ASSERT_EQ(5, results.size());
  EXPECT_EQ("(int32 c0=2000, int32 c1=10000, int32 c2=100000)", results[0]);
  EXPECT_EQ("(int32 c0=2008, int32 c1=10040, int32 c2=100400)", results[4]);

  // Ensure that we only read the relevant range from all of the columns.
  // Since it's a small range, it should be all in one data block in each column.
  vector<IteratorStats> stats;
  iter->GetIteratorStats(&stats);
  ASSERT_EQ(3, stats.size());
  EXPECT_EQ(1, stats[0].blocks_read);
  EXPECT_EQ(1, stats[1].blocks_read);
  EXPECT_EQ(1, stats[2].blocks_read);
}

// Several other black-box tests for range scans. These are similar to
// TestRangeScan above, except don't inspect internal state.
TEST_F(TestCFileSet, TestRangePredicates2) {
  const int kNumRows = 10000;
  WriteTestRowSet(kNumRows);

  shared_ptr<CFileSet> fileset;
  ASSERT_OK(CFileSet::Open(rowset_meta_, MemTracker::GetRootTracker(), MemTracker::GetRootTracker(),
                           nullptr, &fileset));

  // Range scan where rows match on both ends
  DoTestRangeScan(fileset, 2000, 2010);
  // Range scan which falls between rows on both ends
  DoTestRangeScan(fileset, 2001, 2009);
  // Range scan with open lower bound
  DoTestRangeScan(fileset, kNoBound, 2009);
  // Range scan with open upper bound
  DoTestRangeScan(fileset, 2001, kNoBound);
  // Range scan with upper bound coming at end of data
  DoTestRangeScan(fileset, 2001, kNumRows * 2);
  // Range scan with upper bound coming after end of data
  DoTestRangeScan(fileset, 2001, kNumRows * 10);
  // Range scan with lower bound coming at end of data
  DoTestRangeScan(fileset, kNumRows * 2, kNoBound);
  // Range scan with lower bound coming after end of data
  DoTestRangeScan(fileset, kNumRows * 10, kNoBound);
}

TEST_F(TestCFileSet, TestBloomFilterPredicates) {
  const int kNumRows = 100;
  Arena arena(1024);
  ArenaBlockBloomFilterBufferAllocator allocator(&arena);

  BlockBloomFilter bf1_contain(&allocator);
  int log_space_bytes1 = BlockBloomFilter::MinLogSpace(kNumRows, 0.01);
  ASSERT_OK(bf1_contain.Init(log_space_bytes1, FAST_HASH, 0));
  double expected_fp_rate1 = BlockBloomFilter::FalsePositiveProb(kNumRows, log_space_bytes1);
  ASSERT_LE(expected_fp_rate1, 0.01);

  BlockBloomFilter bf1_exclude(&allocator);
  int log_space_bytes11 = BlockBloomFilter::MinLogSpace(kNumRows, 0.01);
  ASSERT_OK(bf1_exclude.Init(log_space_bytes11, FAST_HASH, 0));
  double expected_fp_rate11 = BlockBloomFilter::FalsePositiveProb(kNumRows, log_space_bytes11);
  ASSERT_LE(expected_fp_rate11, 0.01);

  BlockBloomFilter bf2_contain(&allocator);
  int log_space_bytes2 = BlockBloomFilter::MinLogSpace(kNumRows, 0.01);
  ASSERT_OK(bf2_contain.Init(log_space_bytes2, FAST_HASH, 0));
  double expected_fp_rate2 = BlockBloomFilter::FalsePositiveProb(kNumRows, log_space_bytes2);
  ASSERT_LE(expected_fp_rate2, 0.01);

  BlockBloomFilter bf2_exclude(&allocator);
  int log_space_bytes22 = BlockBloomFilter::MinLogSpace(kNumRows, 0.01);
  ASSERT_OK(bf2_exclude.Init(log_space_bytes22, FAST_HASH, 0));
  double expected_fp_rate22 = BlockBloomFilter::FalsePositiveProb(kNumRows, log_space_bytes22);
  ASSERT_LE(expected_fp_rate22, 0.01);

  WriteTestRowSet(kNumRows);
  vector<size_t> ret1_contain;
  vector<size_t> ret1_exclude;
  vector<size_t> ret2_contain;
  vector<size_t> ret2_exclude;
  FillBloomFilter(kNumRows, &bf1_contain, &bf1_exclude, &bf2_contain, &bf2_exclude);
  GetBloomFilterResult(kNumRows, &bf1_contain, &bf1_exclude, &bf2_contain,
                       &bf2_exclude, &ret1_contain, &ret1_exclude, &ret2_contain,
                       &ret2_exclude);

  shared_ptr<CFileSet> fileset;
  ASSERT_OK(CFileSet::Open(rowset_meta_, MemTracker::GetRootTracker(), MemTracker::GetRootTracker(),
                           nullptr, &fileset));


  // BloomFilter of column 0 contain.
  auto pred1_contain = ColumnPredicate::InBloomFilter(schema_.column(0), {&bf1_contain},
                                                      nullptr, nullptr);
  DoTestBloomFilterScan(fileset, { pred1_contain }, ret1_contain);

  // BloomFilter of column 1 contain.
  auto pred2_contain = ColumnPredicate::InBloomFilter(schema_.column(1), {&bf2_contain},
                                                      nullptr, nullptr);
  DoTestBloomFilterScan(fileset, { pred2_contain }, ret2_contain);

  // BloomFilter of column 0 contain and exclude.
  vector<size_t> ret1_contain_exclude;
  auto pred1_contain_exclude = ColumnPredicate::InBloomFilter(
      schema_.column(0), {&bf1_contain, &bf1_exclude}, nullptr, nullptr);
  std::set_intersection(ret1_contain.begin(), ret1_contain.end(), ret1_exclude.begin(),
                        ret1_exclude.end(), std::back_inserter(ret1_contain_exclude));
  DoTestBloomFilterScan(fileset, { pred1_contain_exclude }, ret1_contain_exclude);
  // BloomFilter of column 0 contain and column 1 contain.
  vector<size_t> ret12_contain_contain;
  std::set_intersection(ret1_contain.begin(), ret1_contain.end(), ret2_contain.begin(),
                        ret2_contain.end(), std::back_inserter(ret12_contain_contain));
  DoTestBloomFilterScan(fileset, { pred1_contain, pred2_contain }, ret12_contain_contain);

  // BloomFilter of column 0 contain with lower and upper bound.
  int32_t lower = 8;
  int32_t upper = 58;
  int32_t lower_row_index = lower / 2;
  int32_t upper_row_index = upper / 2;
  vector<size_t> ret1_contain_range = ret1_contain;
  auto left = std::lower_bound(ret1_contain_range.begin(),
                               ret1_contain_range.end(), lower_row_index);
  ret1_contain_range.erase(ret1_contain_range.begin(), left); // don't erase left
  auto right = std::lower_bound(ret1_contain_range.begin(),
                                ret1_contain_range.end(), upper_row_index);
  ret1_contain_range.erase(right, ret1_contain_range.end()); // earse right
  auto range = ColumnPredicate::Range(schema_.column(0), &lower, &upper);
  DoTestBloomFilterScan(fileset, { pred1_contain, range }, ret1_contain_range);

  // BloomFilter of column 0 contain with Range with column.
  auto bf_with_range = ColumnPredicate::InBloomFilter(schema_.column(0), {&bf1_contain},
                                                      &lower, &upper);
  DoTestBloomFilterScan(fileset, { bf_with_range }, ret1_contain_range);
}

} // namespace tablet
} // namespace kudu
