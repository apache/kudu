// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "common/iterator.h"
#include "common/generic_iterators.h"
#include "common/rowblock.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "gutil/casts.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"
#include "util/test_util.h"

DEFINE_int32(num_lists, 3, "Number of lists to merge");
DEFINE_int32(num_rows, 1000, "Number of entries per list");
DEFINE_int32(num_iters, 1, "Number of times to run merge");

namespace kudu {

using std::tr1::shared_ptr;

const static Schema kIntSchema(
  boost::assign::list_of(ColumnSchema("val", UINT32)), 1);


// Test iterator which just yields integer rows from a provided
// vector.
class VectorIterator : public ColumnwiseIterator {
public:
  VectorIterator(const vector<uint32_t> &ints) :
    ints_(ints),
    cur_idx_(0)
  {}

  Status Init(ScanSpec *spec) {
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

  virtual Status MaterializeColumn(size_t col, ColumnBlock *dst) {
    CHECK_EQ(UINT32, dst->type_info().type());
    DCHECK_LE(prepared_, dst->nrows());

    for (size_t i = 0; i < prepared_; i++) {
      uint32_t *dst_cell = reinterpret_cast<uint32_t *>(dst->cell_ptr(i));
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
  shared_ptr<RowwiseIterator> iter(
    new MaterializingIterator(
      shared_ptr<ColumnwiseIterator>(new VectorIterator(empty_vec))));

  vector<shared_ptr<RowwiseIterator> > to_merge;
  to_merge.push_back(iter);

  MergeIterator merger(kIntSchema, to_merge);
  ASSERT_STATUS_OK(merger.Init(NULL));
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

    shared_ptr<RowwiseIterator> iter(
      new MaterializingIterator(
        shared_ptr<ColumnwiseIterator>(new VectorIterator(ints))));
    to_merge.push_back(iter);
  }

  LOG_TIMING(INFO, "std::sort the expected results") {
    std::sort(all_ints.begin(), all_ints.end());
  }


  for (int trial = 0; trial < FLAGS_num_iters; trial++) {
    LOG_TIMING(INFO, "Iterate merged lists") {
      MergeIterator merger(kIntSchema, to_merge);
      ASSERT_STATUS_OK(merger.Init(NULL));

      RowBlock dst(kIntSchema, 100, NULL);
      size_t total_idx = 0;
      while (merger.HasNext()) {
        size_t n = dst.nrows();
        ASSERT_STATUS_OK_FAST(merger.PrepareBatch(&n));
        ASSERT_GT(n, 0) <<
          "if HasNext() returns true, must return some rows";
        ASSERT_STATUS_OK_FAST(merger.MaterializeBlock(&dst));
        ASSERT_STATUS_OK_FAST(merger.FinishBatch());

        for (int i = 0; i < n; i++) {
          uint32_t this_row = *kIntSchema.ExtractColumnFromRow<UINT32>(dst.row(i), 0);
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

class TestIntRangePredicate {
public:
  TestIntRangePredicate(uint32_t lower, uint32_t upper) :
    lower_(lower),
    upper_(upper),
    pred_(kIntSchema.column(0), &lower_, &upper_)
  {}

  uint32_t lower_, upper_;
  ColumnRangePredicate pred_;
};

// Test that the MaterializingIterator properly evaluates predicates when they apply
// to single columns.
TEST(TestMaterializingIterator, TestMaterializingPredicatePushdown) {
  ScanSpec spec;
  TestIntRangePredicate pred1(20, 29);
  spec.AddPredicate(pred1.pred_);
  LOG(INFO) << "pred: " << pred1.pred_.ToString();

  vector<uint32> ints;
  for (int i = 0; i < 100; i++) {
    ints.push_back(i);
  }

  shared_ptr<VectorIterator> colwise(new VectorIterator(ints));
  MaterializingIterator materializing(colwise);
  ASSERT_STATUS_OK(materializing.Init(&spec));
  ASSERT_EQ(0, spec.predicates().size())
    << "Iterator should have pushed down predicate";

  Arena arena(1024, 1024);
  RowBlock dst(kIntSchema, 100, &arena);
  size_t n = 100;
  ASSERT_STATUS_OK(materializing.PrepareBatch(&n));
  ASSERT_EQ(n, 100);
  ASSERT_STATUS_OK(materializing.MaterializeBlock(&dst));
  ASSERT_STATUS_OK(materializing.FinishBatch());

  // Check that the resulting selection vector is correct (rows 20-29 selected)
  ASSERT_EQ(10, dst.selection_vector()->CountSelected());
  ASSERT_FALSE(dst.selection_vector()->IsRowSelected(0));
  ASSERT_TRUE(dst.selection_vector()->IsRowSelected(20));
  ASSERT_TRUE(dst.selection_vector()->IsRowSelected(29));
  ASSERT_FALSE(dst.selection_vector()->IsRowSelected(30));
}

// Test that PredicateEvaluatingIterator will properly evaluate predicates on its
// input.
TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluation) {
  ScanSpec spec;
  TestIntRangePredicate pred1(20, 29);
  spec.AddPredicate(pred1.pred_);
  LOG(INFO) << "pred: " << pred1.pred_.ToString();

  vector<uint32> ints;
  for (int i = 0; i < 100; i++) {
    ints.push_back(i);
  }

  // Set up a MaterializingIterator with pushdown disabled, so that the
  // PredicateEvaluatingIterator will wrap it and do evaluation.
  shared_ptr<VectorIterator> colwise(new VectorIterator(ints));
  MaterializingIterator *materializing = new MaterializingIterator(colwise);
  materializing->disallow_pushdown_for_tests_ = true;

  // Wrap it in another iterator to do the evaluation
  shared_ptr<RowwiseIterator> outer_iter(materializing);
  ASSERT_STATUS_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&outer_iter, &spec));

  ASSERT_NE(reinterpret_cast<uintptr_t>(outer_iter.get()),
            reinterpret_cast<uintptr_t>(materializing))
    << "Iterator pointer should differ after wrapping";

  PredicateEvaluatingIterator *pred_eval = down_cast<PredicateEvaluatingIterator *>(
    outer_iter.get());

  ASSERT_EQ(0, spec.predicates().size())
    << "Iterator tree should have accepted predicate";
  ASSERT_EQ(1, pred_eval->predicates_.size())
    << "Predicate should be evaluated by the outer iterator";

  Arena arena(1024, 1024);
  RowBlock dst(kIntSchema, 100, &arena);
  size_t n = 100;
  ASSERT_STATUS_OK(outer_iter->PrepareBatch(&n));
  ASSERT_EQ(n, 100);
  ASSERT_STATUS_OK(outer_iter->MaterializeBlock(&dst));
  ASSERT_STATUS_OK(outer_iter->FinishBatch());

  // Check that the resulting selection vector is correct (rows 20-29 selected)
  ASSERT_EQ(10, dst.selection_vector()->CountSelected());
  ASSERT_FALSE(dst.selection_vector()->IsRowSelected(0));
  ASSERT_TRUE(dst.selection_vector()->IsRowSelected(20));
  ASSERT_TRUE(dst.selection_vector()->IsRowSelected(29));
  ASSERT_FALSE(dst.selection_vector()->IsRowSelected(30));
}

// Test that PredicateEvaluatingIterator::InitAndMaybeWrap doesn't wrap an underlying
// iterator when there are no predicates left.
TEST(TestPredicateEvaluatingIterator, TestDontWrapWhenNoPredicates) {
  ScanSpec spec;

  vector<uint32> ints;
  shared_ptr<VectorIterator> colwise(new VectorIterator(ints));
  shared_ptr<RowwiseIterator> materializing(new MaterializingIterator(colwise));
  shared_ptr<RowwiseIterator> outer_iter(materializing);
  ASSERT_STATUS_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&outer_iter, &spec));
  ASSERT_EQ(outer_iter, materializing) << "InitAndMaybeWrap should not have wrapped iter";
}

} // namespace kudu
