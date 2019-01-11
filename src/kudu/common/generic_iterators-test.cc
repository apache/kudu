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

#include "kudu/common/generic_iterators.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_lists, 3, "Number of lists to merge");
DEFINE_int32(num_rows, 1000, "Number of entries per list");
DEFINE_int32(num_iters, 1, "Number of times to run merge");

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

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
        block_size_(ints_.size()),
        sel_vec_(nullptr) {
  }

  // Set the number of rows that will be returned in each
  // call to PrepareBatch().
  void set_block_size(int block_size) {
    block_size_ = block_size;
  }

  void set_selection_vector(SelectionVector* sv) {
    sel_vec_ = sv;
  }

  Status Init(ScanSpec* /*spec*/) override {
    return Status::OK();
  }

  Status PrepareBatch(size_t* nrows) override {
    prepared_ = std::min<int64_t>({
        static_cast<int64_t>(ints_.size()) - cur_idx_,
        block_size_,
        static_cast<int64_t>(*nrows) });
    *nrows = prepared_;
    return Status::OK();
  }

  Status InitializeSelectionVector(SelectionVector* sv) override {
    if (!sel_vec_) {
      sv->SetAllTrue();
      return Status::OK();
    }
    for (int i = 0; i < sv->nrows(); i++) {
      size_t row_idx = cur_idx_ + i;
      if (row_idx > sel_vec_->nrows() || !sel_vec_->IsRowSelected(row_idx)) {
        sv->SetRowUnselected(i);
      } else {
        DCHECK(sel_vec_->IsRowSelected(row_idx));
        sv->SetRowSelected(i);
      }
    }
    return Status::OK();
  }

  Status MaterializeColumn(ColumnMaterializationContext* ctx) override {
    ctx->SetDecoderEvalNotSupported();
    CHECK_EQ(UINT32, ctx->block()->type_info()->physical_type());
    DCHECK_LE(prepared_, ctx->block()->nrows());

    for (size_t i = 0; i < prepared_; i++) {
      ctx->block()->SetCellValue(i, &(ints_[cur_idx_ + i]));
    }

    return Status::OK();
  }

  Status FinishBatch() override {
    cur_idx_ += prepared_;
    prepared_ = 0;
    return Status::OK();
  }

  bool HasNext() const override {
    return cur_idx_ < ints_.size();
  }

  string ToString() const override {
    return Substitute("VectorIterator [$0,$1]", ints_[0], ints_[ints_.size() - 1]);
  }

  const Schema &schema() const override {
    return kIntSchema;
  }

  void GetIteratorStats(vector<IteratorStats>* stats) const override {
    stats->resize(schema().num_columns());
  }

 private:
  vector<uint32_t> ints_;
  int cur_idx_;
  int block_size_;
  size_t prepared_;
  SelectionVector* sel_vec_;
};

// Test that empty input to a merger behaves correctly.
TEST(TestMergeIterator, TestMergeEmpty) {
  unique_ptr<RowwiseIterator> iter(
      new MaterializingIterator(
          unique_ptr<ColumnwiseIterator>(new VectorIterator({}))));
  vector<unique_ptr<RowwiseIterator>> input;
  input.emplace_back(std::move(iter));
  MergeIterator merger(std::move(input));
  ASSERT_OK(merger.Init(nullptr));
  ASSERT_FALSE(merger.HasNext());
}

// Test that non-empty input to a merger with a zeroed selection vector
// behaves correctly.
TEST(TestMergeIterator, TestMergeEmptyViaSelectionVector) {
  SelectionVector sv(3);
  sv.SetAllFalse();
  unique_ptr<VectorIterator> vec(new VectorIterator({ 1, 2, 3 }));
  vec->set_selection_vector(&sv);
  unique_ptr<RowwiseIterator> iter(new MaterializingIterator(std::move(vec)));
  vector<unique_ptr<RowwiseIterator>> input;
  input.emplace_back(std::move(iter));
  MergeIterator merger(std::move(input));
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
  struct List {
    vector<uint32_t> ints;
    unique_ptr<SelectionVector> sv;
  };
  vector<List> all_ints;
  vector<uint32_t> expected;
  expected.reserve(FLAGS_num_rows * FLAGS_num_lists);
  Random prng(SeedRandom());

  for (int i = 0; i < FLAGS_num_lists; i++) {
    vector<uint32_t> ints;
    ints.reserve(FLAGS_num_rows);
    unique_ptr<SelectionVector> sv(new SelectionVector(FLAGS_num_rows));

    uint32_t entry = 0;
    for (int j = 0; j < FLAGS_num_rows; j++) {
      entry += prng.Uniform(5);
      ints.emplace_back(entry);

      // Some entries are randomly deselected in order to exercise the selection
      // vector logic in the MergeIterator. This is reflected both in the input
      // to the MeregIterator as well as the expected output (see below).
      bool row_selected = prng.Uniform(8) > 0;
      VLOG(2) << Substitute("Row $0 with value $1 selected? $2",
                            j, entry, row_selected);
      if (row_selected) {
        sv->SetRowSelected(j);
      } else {
        sv->SetRowUnselected(j);
      }

      // Consider the predicate and the selection vector before pushing to 'expected'.
      if (entry >= predicate.lower_ && entry < predicate.upper_ && row_selected) {
        expected.emplace_back(entry);
      }
    }

    all_ints.emplace_back(List{ std::move(ints), std::move(sv) });
  }

  LOG_TIMING(INFO, "std::sort the expected results") {
    std::sort(expected.begin(), expected.end());
  }

  VLOG(1) << "Predicate expects " << expected.size() << " results: " << expected;

  for (int trial = 0; trial < FLAGS_num_iters; trial++) {
    vector<unique_ptr<RowwiseIterator>> to_merge;
    for (const auto& e : all_ints) {
      unique_ptr<VectorIterator> vec_it(new VectorIterator(e.ints));
      vec_it->set_block_size(10);
      vec_it->set_selection_vector(e.sv.get());
      unique_ptr<RowwiseIterator> mat_it(new MaterializingIterator(std::move(vec_it)));
      vector<unique_ptr<RowwiseIterator>> to_union;
      to_union.emplace_back(std::move(mat_it));
      unique_ptr<RowwiseIterator> un_it(new UnionIterator(std::move(to_union)));
      to_merge.emplace_back(std::move(un_it));
    }

    // Setup predicate exclusion
    ScanSpec spec;
    spec.AddPredicate(predicate.pred_);
    LOG(INFO) << "Predicate: " << predicate.pred_.ToString();

    LOG_TIMING(INFO, "Iterate merged lists") {
      MergeIterator merger(std::move(to_merge));
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

  vector<uint32_t> ints(100);
  for (int i = 0; i < 100; i++) {
    ints[i] = i;
  }

  unique_ptr<VectorIterator> colwise(new VectorIterator(std::move(ints)));
  MaterializingIterator materializing(std::move(colwise));
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

  vector<uint32_t> ints(100);
  for (int i = 0; i < 100; i++) {
    ints[i] = i;
  }

  // Set up a MaterializingIterator with pushdown disabled, so that the
  // PredicateEvaluatingIterator will wrap it and do evaluation.
  unique_ptr<VectorIterator> colwise(new VectorIterator(std::move(ints)));
  unique_ptr<MaterializingIterator> materializing(
      new MaterializingIterator(std::move(colwise)));
  materializing->disallow_pushdown_for_tests_ = true;

  // Wrap it in another iterator to do the evaluation
  const MaterializingIterator* mat_iter_addr = materializing.get();
  unique_ptr<RowwiseIterator> outer_iter(std::move(materializing));
  ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&outer_iter, &spec));

  ASSERT_NE(reinterpret_cast<uintptr_t>(outer_iter.get()),
            reinterpret_cast<uintptr_t>(mat_iter_addr))
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
  unique_ptr<VectorIterator> colwise(new VectorIterator(std::move(ints)));
  unique_ptr<MaterializingIterator> materializing(
      new MaterializingIterator(std::move(colwise)));
  const MaterializingIterator* mat_iter_addr = materializing.get();
  unique_ptr<RowwiseIterator> outer_iter(std::move(materializing));
  ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&outer_iter, &spec));
  ASSERT_EQ(reinterpret_cast<uintptr_t>(outer_iter.get()),
            reinterpret_cast<uintptr_t>(mat_iter_addr))
      << "InitAndMaybeWrap should not have wrapped iter";
}

// Test row-wise iterator which does nothing.
class DummyIterator : public RowwiseIterator {
 public:

  explicit DummyIterator(const Schema& schema)
      : schema_(schema) {
  }

  Status Init(ScanSpec* /*spec*/) override {
    return Status::OK();
  }

  Status NextBlock(RowBlock* /*dst*/) override {
    LOG(FATAL) << "unimplemented!";
    return Status::OK();
  }

  bool HasNext() const override {
    LOG(FATAL) << "unimplemented!";
    return false;
  }

  string ToString() const override {
    return "DummyIterator";
  }

  const Schema& schema() const override {
    return schema_;
  }

  void GetIteratorStats(vector<IteratorStats>* stats) const override {
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

    unique_ptr<RowwiseIterator> iter(new DummyIterator(schema));
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

    unique_ptr<RowwiseIterator> iter(new DummyIterator(schema));
    ASSERT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&iter, &spec));

    PredicateEvaluatingIterator* pred_eval = down_cast<PredicateEvaluatingIterator*>(iter.get());
    ASSERT_TRUE(pred_eval->col_predicates_ ==
                vector<ColumnPredicate>({ c_equality, a_equality, b_equality }));
  }
}

} // namespace kudu
