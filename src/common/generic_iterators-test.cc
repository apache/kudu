// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "common/iterator.h"
#include "common/generic_iterators.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_int32(num_lists, 3, "Number of lists to merge");
DEFINE_int32(num_rows, 1000, "Number of entries per list");
DEFINE_int32(num_iters, 1, "Number of times to run merge");

namespace kudu {

using std::tr1::shared_ptr;

const static Schema kIntSchema(
  boost::assign::list_of(ColumnSchema("val", UINT32)), 1);


// Test iterator which just yields integer rows from a provided
// vector.
class VectorIterator : public RowwiseIterator {
public:
  VectorIterator(const vector<uint32_t> &ints) :
    ints_(ints),
    cur_idx_(0)
  {}

  Status Init() {
    return Status::OK();
  }

  virtual Status PrepareBatch(size_t *nrows) {
    int rem = ints_.size() - cur_idx_;
    if (rem < *nrows) {
      *nrows = rem;
    }
    prepared_ = rem;
    return Status::OK();
  }

  virtual Status MaterializeBlock(RowBlock *dst) {
    CHECK_EQ(dst->schema().byte_size(), sizeof(uint32_t));
    DCHECK_LE(prepared_, dst->nrows());

    for (size_t i = 0; i < prepared_; i++) {
      uint32_t *dst_cell = reinterpret_cast<uint32_t *>(dst->row_ptr(i));
      *dst_cell = ints_[cur_idx_++];
    }

    return Status::OK();
  }

  virtual Status FinishBatch() {
    prepared_ = 0;
    return Status::OK();
  }

  virtual bool HasNext() const {
    return cur_idx_ < ints_.size();
  }

  virtual string ToString() const {
    return string("VectorIterator");
  }

  virtual const Schema &schema() const {
    return kIntSchema;
  }

private:
  vector<uint32_t> ints_;
  int cur_idx_;
  size_t prepared_;
};

// Test that empty input to a merger behaves correctly.
TEST(TestMergeIterator, TestMergeEmpty) {
  vector<uint32_t> empty_vec;
  shared_ptr<VectorIterator> iter(new VectorIterator(empty_vec));

  vector<shared_ptr<RowwiseIterator> > to_merge;
  to_merge.push_back(iter);

  MergeIterator merger(kIntSchema, to_merge);
  ASSERT_STATUS_OK(merger.Init());
  ASSERT_FALSE(merger.HasNext());
}

TEST(TestMergeIterator, TestMerge) {
  vector<shared_ptr<RowwiseIterator> > to_merge;
  vector<uint32_t> ints;
  vector<uint32_t> all_ints;
  all_ints.reserve(FLAGS_num_rows * FLAGS_num_lists);

  for (int i = 0; i < FLAGS_num_lists; i++) {
    ints.clear();
    ints.reserve(FLAGS_num_rows);

    uint32_t entry = 0;
    for (int j = 0; j < FLAGS_num_rows; j++) {
      entry += rand() % 5;
      ints.push_back(entry);
      all_ints.push_back(entry);
    }

    shared_ptr<VectorIterator> iter(new VectorIterator(ints));
    to_merge.push_back(iter);
  }

  LOG_TIMING(INFO, "std::sort the expected results") {
    std::sort(all_ints.begin(), all_ints.end());
  }


  for (int trial = 0; trial < FLAGS_num_iters; trial++) {
    LOG_TIMING(INFO, "Iterate merged lists") {
      MergeIterator merger(kIntSchema, to_merge);
      ASSERT_STATUS_OK(merger.Init());

      ScopedRowBlock dst(kIntSchema, 100, NULL);
      size_t total_idx = 0;
      while (merger.HasNext()) {
        size_t n = dst.nrows();
        ASSERT_STATUS_OK_FAST(merger.PrepareBatch(&n));
        ASSERT_GT(n, 0) <<
          "if HasNext() returns true, must return some rows";
        ASSERT_STATUS_OK_FAST(merger.MaterializeBlock(&dst));
        ASSERT_STATUS_OK_FAST(merger.FinishBatch());

        for (int i = 0; i < n; i++) {
          uint32_t this_row = *(reinterpret_cast<const uint32_t *>(dst.row_ptr(i)));
          if (all_ints[total_idx] != this_row) { 
            ASSERT_EQ(all_ints[total_idx], this_row) <<
              "Yielded out of order at idx " << total_idx;
          }
          total_idx++;
        }
      }
    }
  }
}

} // namespace kudu
