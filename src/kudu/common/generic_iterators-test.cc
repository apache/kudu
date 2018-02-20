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

#include <algorithm>
#include <memory>
#include <cstdlib>
#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/generic_iterators.h"
#include "kudu/common/column_materialization_context.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

DEFINE_int32(num_lists, 3, "Number of lists to merge");
DEFINE_int32(num_rows, 1000, "Number of entries per list");
DEFINE_int32(num_iters, 1, "Number of times to run merge");

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;

namespace kudu {

struct IteratorStats;

static const Schema kIntSchema({ ColumnSchema("val", UINT32) }, 1);

// Test iterator which just yields integer rows from a provided
// vector.
class VectorIterator : public ColumnwiseIterator {
 public:
  explicit VectorIterator(vector<uint32_t> ints)
      : ints_(std::move(ints)),
        cur_idx_(0),
        block_size_(ints_.size()) {
  }

  // Set the number of rows that will be returned in each
  // call to PrepareBatch().
  void set_block_size(int block_size) {
    block_size_ = block_size;
  }

  Status Init(ScanSpec *spec) OVERRIDE {
    return Status::OK();
  }

  virtual Status PrepareBatch(size_t* nrows) OVERRIDE {
    prepared_ = std::min<int64_t>({
        static_cast<int64_t>(ints_.size()) - cur_idx_,
        block_size_,
        static_cast<int64_t>(*nrows) });
    *nrows = prepared_;
    return Status::OK();
  }

  virtual Status InitializeSelectionVector(SelectionVector *sel_vec) OVERRIDE {
    sel_vec->SetAllTrue();
    return Status::OK();
  }

  Status MaterializeColumn(ColumnMaterializationContext* ctx) override {
    ctx->SetDecoderEvalNotSupported();
    CHECK_EQ(UINT32, ctx->block()->type_info()->physical_type());
    DCHECK_LE(prepared_, ctx->block()->nrows());

    for (size_t i = 0; i < prepared_; i++) {
      ctx->block()->SetCellValue(i, &(ints_[cur_idx_++]));
    }

    return Status::OK();
  }

  virtual Status FinishBatch() OVERRIDE {
    prepared_ = 0;
    return Status::OK();
  }

  virtual bool HasNext() const OVERRIDE {
    return cur_idx_ < ints_.size();
  }

  virtual string ToString() const OVERRIDE {
    return string("VectorIterator");
  }

  virtual const Schema &schema() const OVERRIDE {
    return kIntSchema;
  }

  virtual void GetIteratorStats(vector<IteratorStats>* stats) const OVERRIDE {
    stats->resize(schema().num_columns());
  }

 private:
  vector<uint32_t> ints_;
  int cur_idx_;
  int block_size_;
  size_t prepared_;
};

// Test that empty input to a merger behaves correctly.
TEST(TestMergeIterator, TestMergeEmpty) {
  vector<uint32_t> empty_vec;
  shared_ptr<RowwiseIterator> iter(
    new MaterializingIterator(
      shared_ptr<ColumnwiseIterator>(new VectorIterator(empty_vec))));

  vector<shared_ptr<RowwiseIterator>> to_merge;
  to_merge.push_back(iter);

  MergeIterator merger(kIntSchema, to_merge);
  ASSERT_OK(merger.Init(nullptr));
  ASSERT_FALSE(merger.HasNext());
}


class TestIntRangePredicate {
 public:
  TestIntRangePredicate(uint32_t lower, uint32_t upper) :
    lower_(lower),
    upper_(upper),
    pred_(ColumnPredicate::Range(kIntSchema.column(0), &lower_, &upper_)) {}

  uint32_t lower_, upper_;
  ColumnPredicate pred_;
};

void TestMerge(const TestIntRangePredicate &predicate) {
  vector<shared_ptr<RowwiseIterator>> to_merge;
  vector<uint32_t> ints;
  vector<uint32_t> expected;
  expected.reserve(FLAGS_num_rows * FLAGS_num_lists);

  // Setup predicate exclusion
  ScanSpec spec;
  spec.AddPredicate(predicate.pred_);
  LOG(INFO) << "Predicate: " << predicate.pred_.ToString();

  for (int i = 0; i < FLAGS_num_lists; i++) {
    ints.clear();
    ints.reserve(FLAGS_num_rows);

    uint32_t entry = 0;
    for (int j = 0; j < FLAGS_num_rows; j++) {
      entry += rand() % 5;
      ints.push_back(entry);
      // Evaluate the predicate before pushing to 'expected'.
      if (entry >= predicate.lower_ && entry < predicate.upper_) {
        expected.push_back(entry);
      }
    }

    shared_ptr<VectorIterator> it(new VectorIterator(ints));
    it->set_block_size(10);
    shared_ptr<RowwiseIterator> iter(new MaterializingIterator(it));
    to_merge.emplace_back(new UnionIterator({ iter }));
  }

  VLOG(1) << "Predicate expects " << expected.size() << " results: " << expected;

  LOG_TIMING(INFO, "std::sort the expected results") {
    std::sort(expected.begin(), expected.end());
  }

  for (int trial = 0; trial < FLAGS_num_iters; trial++) {
    LOG_TIMING(INFO, "Iterate merged lists") {
      MergeIterator merger(kIntSchema, to_merge);
      ASSERT_OK(merger.Init(&spec));

      RowBlock dst(kIntSchema, 100, nullptr);
      size_t total_idx = 0;
      while (merger.HasNext()) {
        ASSERT_OK(merger.NextBlock(&dst));
        ASSERT_GT(dst.nrows(), 0) <<
          "if HasNext() returns true, must return some rows";

        for (int i = 0; i < dst.nrows(); i++) {
          uint32_t this_row = *kIntSchema.ExtractColumnFromRow<UINT32>(dst.row(i), 0);
          ASSERT_GE(this_row, predicate.lower_) << "Yielded integer excluded by predicate";
          ASSERT_LT(this_row, predicate.upper_) << "Yielded integer excluded by predicate";
          if (expected[total_idx] != this_row) {
            ASSERT_EQ(expected[total_idx], this_row) <<
              "Yielded out of order at idx " << total_idx;
          }
          total_idx++;
        }
      }
      ASSERT_EQ(total_idx, expected.size());
    }
  }
}

TEST(TestMergeIterator, TestMerge) {
  TestIntRangePredicate predicate(0, MathLimits<uint32_t>::kMax);
  TestMerge(predicate);
}

TEST(TestMergeIterator, TestMergePredicate) {
  TestIntRangePredicate predicate(0, FLAGS_num_rows / 5);
  TestMerge(predicate);
}

// Regression test for a bug in the merge which would incorrectly
// drop a merge input if it received an entirely non-selected block.
// This predicate excludes the first half of the rows but accepts the
// second half.
TEST(TestMergeIterator, TestMergePredicate2) {
  TestIntRangePredicate predicate(FLAGS_num_rows / 2, MathLimits<uint32_t>::kMax);
  TestMerge(predicate);
}

// Test that the MaterializingIterator properly evaluates predicates when they apply
// to single columns.
TEST(TestMaterializingIterator, TestMaterializingPredicatePushdown) {
  ScanSpec spec;
  TestIntRangePredicate pred1(20, 30);
  spec.AddPredicate(pred1.pred_);
  LOG(INFO) << "Predicate: " << pred1.pred_.ToString();

  vector<uint32_t> ints;
  for (int i = 0; i < 100; i++) {
    ints.push_back(i);
  }

  shared_ptr<VectorIterator> colwise(new VectorIterator(ints));
  MaterializingIterator materializing(colwise);
  ASSERT_OK(materializing.Init(&spec));
  ASSERT_EQ(0, spec.predicates().size()) << "Iterator should have pushed down predicate";

  Arena arena(1024);
  RowBlock dst(kIntSchema, 100, &arena);
  ASSERT_OK(materializing.NextBlock(&dst));
  ASSERT_EQ(dst.nrows(), 100);

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
  TestIntRangePredicate pred1(20, 30);
  spec.AddPredicate(pred1.pred_);
  LOG(INFO) << "Predicate: " << pred1.pred_.ToString();

  vector<uint32_t> ints;
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
  ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&outer_iter, &spec));

  ASSERT_NE(reinterpret_cast<uintptr_t>(outer_iter.get()),
            reinterpret_cast<uintptr_t>(materializing))
    << "Iterator pointer should differ after wrapping";

  PredicateEvaluatingIterator *pred_eval = down_cast<PredicateEvaluatingIterator *>(
    outer_iter.get());

  ASSERT_EQ(0, spec.predicates().size())
    << "Iterator tree should have accepted predicate";
  ASSERT_EQ(1, pred_eval->col_predicates_.size())
    << "Predicate should be evaluated by the outer iterator";

  Arena arena(1024);
  RowBlock dst(kIntSchema, 100, &arena);
  ASSERT_OK(outer_iter->NextBlock(&dst));
  ASSERT_EQ(dst.nrows(), 100);

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

  vector<uint32_t> ints;
  shared_ptr<VectorIterator> colwise(new VectorIterator(ints));
  shared_ptr<RowwiseIterator> materializing(new MaterializingIterator(colwise));
  shared_ptr<RowwiseIterator> outer_iter(materializing);
  ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&outer_iter, &spec));
  ASSERT_EQ(outer_iter, materializing) << "InitAndMaybeWrap should not have wrapped iter";
}

// Test row-wise iterator which does nothing.
class DummyIterator : public RowwiseIterator {
 public:

  explicit DummyIterator(Schema schema)
      : schema_(std::move(schema)) {
  }

  Status Init(ScanSpec* /*spec*/) override {
    return Status::OK();
  }

  virtual Status NextBlock(RowBlock* /*dst*/) override {
    LOG(FATAL) << "unimplemented!";
    return Status::OK();
  }

  virtual bool HasNext() const override {
    LOG(FATAL) << "unimplemented!";
    return false;
  }

  virtual string ToString() const override {
    return "DummyIterator";
  }

  virtual const Schema& schema() const override {
    return schema_;
  }

  virtual void GetIteratorStats(vector<IteratorStats>* stats) const override {
    stats->resize(schema().num_columns());
  }

 private:
  Schema schema_;
};

TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluationOrder) {
  Schema schema({ ColumnSchema("a_int64", INT64),
                  ColumnSchema("b_int64", INT64),
                  ColumnSchema("c_int32", INT32) }, 3);

  int64_t zero = 0;
  int64_t two = 2;
  auto a_equality = ColumnPredicate::Equality(schema.column(0), &zero);
  auto b_equality = ColumnPredicate::Equality(schema.column(1), &zero);
  auto c_equality = ColumnPredicate::Equality(schema.column(2), &zero);
  auto a_range = ColumnPredicate::Range(schema.column(0), &zero, &two);

  { // Test that more selective predicates come before others.
    ScanSpec spec;
    spec.AddPredicate(a_range);
    spec.AddPredicate(b_equality);
    spec.AddPredicate(c_equality);

    shared_ptr<RowwiseIterator> iter = make_shared<DummyIterator>(schema);
    ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&iter, &spec));

    PredicateEvaluatingIterator* pred_eval = down_cast<PredicateEvaluatingIterator*>(iter.get());
    ASSERT_TRUE(pred_eval->col_predicates_ ==
                vector<ColumnPredicate>({ c_equality, b_equality, a_range }));
  }

  { // Test that smaller columns come before larger ones, and ties are broken by idx.
    ScanSpec spec;
    spec.AddPredicate(b_equality);
    spec.AddPredicate(a_equality);
    spec.AddPredicate(c_equality);

    shared_ptr<RowwiseIterator> iter = make_shared<DummyIterator>(schema);
    ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&iter, &spec));

    PredicateEvaluatingIterator* pred_eval = down_cast<PredicateEvaluatingIterator*>(iter.get());
    ASSERT_TRUE(pred_eval->col_predicates_ ==
                vector<ColumnPredicate>({ c_equality, a_equality, b_equality }));
  }
}

} // namespace kudu
