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
#include <map>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
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
DECLARE_bool(materializing_iterator_do_pushdown);

using std::map;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

struct IteratorStats;

static const int kValColIdx = 0; // Index of 'val' column in these test schemas.
static const Schema kIntSchema({ ColumnSchema("val", INT64) },
                               /*key_columns=*/1);
static const bool kIsDeletedReadDefault = false;
static const Schema kIntSchemaWithVCol({ ColumnSchema("val", INT64),
                                         ColumnSchema("is_deleted", IS_DELETED,
                                                      /*is_nullable=*/false,
                                                      /*read_default=*/&kIsDeletedReadDefault) },
                                       /*key_columns=*/1);

// Test iterator which just yields integer rows from a provided
// vector.
class VectorIterator : public ColumnwiseIterator {
 public:
  VectorIterator(vector<int64_t> ints, vector<uint8_t> is_deleted, Schema schema)
      : ints_(std::move(ints)),
        is_deleted_(std::move(is_deleted)),
        schema_(std::move(schema)),
        cur_idx_(0),
        block_size_(ints_.size()),
        sel_vec_(nullptr) {
    CHECK_EQ(ints_.size(), is_deleted_.size());
  }

  explicit VectorIterator(const vector<int64_t>& ints)
      : VectorIterator(ints, vector<uint8_t>(ints.size()), kIntSchema) {
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
    DCHECK_LE(prepared_, ctx->block()->nrows());

    switch (ctx->block()->type_info()->physical_type()) {
      case INT64:
        for (size_t i = 0; i < prepared_; i++) {
          ctx->block()->SetCellValue(i, &(ints_[cur_idx_ + i]));
        }
        break;
      case BOOL:
        for (size_t i = 0; i < prepared_; i++) {
          ctx->block()->SetCellValue(i, &(is_deleted_[cur_idx_ + i]));
        }
        break;
      default:
        LOG(FATAL) << "unsupported column type in VectorIterator";
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

  const Schema& schema() const override {
    return schema_;
  }

  void GetIteratorStats(vector<IteratorStats>* stats) const override {
    stats->resize(schema().num_columns());
  }

 private:
  vector<int64_t> ints_;
  // We use vector<uint8_t> instead of vector<bool> to represent the IS_DELETED
  // column so we can call ColumnBlock::SetCellValue() in MaterializeColumn(),
  // whose API requires taking an address to a non-temporary for the value.
  vector<uint8_t> is_deleted_;
  const Schema schema_;
  int cur_idx_;
  int block_size_;
  size_t prepared_;
  SelectionVector* sel_vec_;
};

// Test that empty input to a merger behaves correctly.
TEST(TestMergeIterator, TestMergeEmpty) {
  unique_ptr<RowwiseIterator> iter(
    NewMaterializingIterator(
        unique_ptr<ColumnwiseIterator>(new VectorIterator({}))));
  vector<IterWithBounds> input;
  IterWithBounds iwb;
  iwb.iter = std::move(iter);
  input.emplace_back(std::move(iwb));
  unique_ptr<RowwiseIterator> merger(NewMergeIterator(
      MergeIteratorOptions(/*include_deleted_rows=*/false), std::move(input)));
  ASSERT_OK(merger->Init(nullptr));
  ASSERT_FALSE(merger->HasNext());
}

// Test that non-empty input to a merger with a zeroed selection vector
// behaves correctly.
TEST(TestMergeIterator, TestMergeEmptyViaSelectionVector) {
  SelectionVector sv(3);
  sv.SetAllFalse();
  unique_ptr<VectorIterator> vec(new VectorIterator({ 1, 2, 3 }));
  vec->set_selection_vector(&sv);
  unique_ptr<RowwiseIterator> iter(NewMaterializingIterator(std::move(vec)));
  vector<IterWithBounds> input;
  IterWithBounds iwb;
  iwb.iter = std::move(iter);
  input.emplace_back(std::move(iwb));
  unique_ptr<RowwiseIterator> merger(NewMergeIterator(
      MergeIteratorOptions(/*include_deleted_rows=*/false), std::move(input)));
  ASSERT_OK(merger->Init(nullptr));
  ASSERT_FALSE(merger->HasNext());
}

// Tests that if we stop using a MergeIterator with several elements remaining,
// it is cleaned up properly.
TEST(TestMergeIterator, TestNotConsumedCleanup) {
  unique_ptr<VectorIterator> vec1(new VectorIterator({ 1 }));
  unique_ptr<VectorIterator> vec2(new VectorIterator({ 2 }));
  unique_ptr<VectorIterator> vec3(new VectorIterator({ 3 }));

  vector<IterWithBounds> input;
  IterWithBounds iwb1;
  iwb1.iter = NewMaterializingIterator(std::move(vec1));
  input.emplace_back(std::move(iwb1));
  IterWithBounds iwb2;
  iwb2.iter = NewMaterializingIterator(std::move(vec2));
  input.emplace_back(std::move(iwb2));
  IterWithBounds iwb3;
  iwb3.iter = NewMaterializingIterator(std::move(vec3));
  input.emplace_back(std::move(iwb3));
  unique_ptr<RowwiseIterator> merger(NewMergeIterator(
      MergeIteratorOptions(/*include_deleted_rows=*/false), std::move(input)));
  ASSERT_OK(merger->Init(nullptr));

  ASSERT_TRUE(merger->HasNext());
  RowBlock dst(&kIntSchema, 1, nullptr);
  ASSERT_OK(merger->NextBlock(&dst));
  ASSERT_EQ(1, dst.nrows());
  ASSERT_TRUE(merger->HasNext());

  // Let the MergeIterator go out of scope with some remaining elements.
}

class TestIntRangePredicate {
 public:
  TestIntRangePredicate(int64_t lower, int64_t upper, const ColumnSchema& column)
      : lower_(lower),
        upper_(upper),
        pred_(ColumnPredicate::Range(column, &lower_, &upper_)) {
  }

  TestIntRangePredicate(int64_t lower, int64_t upper)
      : TestIntRangePredicate(lower, upper, kIntSchema.column(0)) {
  }

  int64_t lower_, upper_;
  ColumnPredicate pred_;
};

void TestMerge(const Schema& schema,
               const TestIntRangePredicate &predicate,
               bool overlapping_ranges = true,
               bool include_deleted_rows = false) {
  struct List {
    explicit List(int num_rows)
        : sv(new SelectionVector(num_rows)) {
      ints.reserve(num_rows);
      is_deleted.reserve(num_rows);
    }

    vector<int64_t> ints;
    vector<uint8_t> is_deleted;
    unique_ptr<SelectionVector> sv;
    boost::optional<pair<string, string>> encoded_bounds;
  };

  vector<List> all_ints;
  map<int64_t, bool> expected;
  unordered_set<int64_t> seen_live;
  const auto& encoder = GetKeyEncoder<string>(GetTypeInfo(INT64));
  Random prng(SeedRandom());

  int64_t entry = 0;
  for (int i = 0; i < FLAGS_num_lists; i++) {
    List list(FLAGS_num_rows);
    unordered_set<int64_t> seen_this_list;

    boost::optional<int64_t> min_entry;
    boost::optional<int64_t> max_entry;
    if (overlapping_ranges) {
      entry = 0;
    }
    for (int j = 0; j < FLAGS_num_rows; j++) {
      int64_t potential;
      bool is_deleted = false;
      // The merge iterator does not support duplicate non-deleted keys.
      while (true) {
        potential = entry + prng.Uniform(FLAGS_num_rows * FLAGS_num_lists * 10);
        // Only one live version of a row can exist across all lists.
        // Including several duplicate deleted keys is fine.
        if (ContainsKey(seen_live, potential)) continue;

        // No duplicate keys are allowed in the same list (same RowSet).
        if (ContainsKey(seen_this_list, potential)) continue;
        InsertOrDie(&seen_this_list, potential);

        // If we are including deleted rows, with some probability make this a
        // deleted row.
        if (include_deleted_rows) {
          is_deleted = prng.OneIn(4);
          if (is_deleted) {
            break;
          }
        }

        // This is a new live row. Un-mark it as deleted if necessary.
        if (!is_deleted) {
          InsertOrDie(&seen_live, potential);
        }
        break;
      }
      entry = potential;

      list.ints.emplace_back(entry);
      list.is_deleted.emplace_back(is_deleted);

      if (!max_entry || entry > max_entry) {
        max_entry = entry;
      }
      if (!min_entry || entry < min_entry) {
        min_entry = entry;
      }

      // Some entries are randomly deselected in order to exercise the selection
      // vector logic in the MergeIterator. This is reflected both in the input
      // to the MergeIterator as well as the expected output (see below).
      bool row_selected = prng.Uniform(8) > 0;
      VLOG(2) << Substitute("Row $0 with value $1 selected? $2",
                            j, entry, row_selected);
      if (row_selected) {
        list.sv->SetRowSelected(j);
      } else {
        list.sv->SetRowUnselected(j);
      }

      // Consider the predicate and the selection vector before inserting this
      // row into 'expected'.
      if (entry >= predicate.lower_ && entry < predicate.upper_ && row_selected) {
        auto result = expected.emplace(entry, is_deleted);
        if (!result.second) {
          // We should only be overwriting a deleted row.
          bool existing_is_deleted = result.first->second;
          CHECK_EQ(true, existing_is_deleted);
          result.first->second = is_deleted;
        }
      }
    }

    if (prng.Uniform(10) > 0) {
      // Use the smallest and largest entries as bounds most of the time. They
      // are randomly adjusted to reflect their inexactness in the real world.
      list.encoded_bounds.emplace();
      DCHECK(min_entry);
      DCHECK(max_entry);
      min_entry = *min_entry - prng.Uniform(5);
      max_entry = *max_entry + prng.Uniform(5);
      encoder.Encode(&(min_entry.get()), &list.encoded_bounds->first);
      encoder.Encode(&(max_entry.get()), &list.encoded_bounds->second);
    }
    all_ints.emplace_back(std::move(list));
  }

  LOG_TIMING(INFO, "shuffling the inputs") {
    std::random_device rdev;
    std::mt19937 gen(rdev());
    std::shuffle(all_ints.begin(), all_ints.end(), gen);
  }

  VLOG(1) << "Predicate expects " << expected.size() << " results: " << expected;

  const int kIsDeletedIndex = schema.find_first_is_deleted_virtual_column();

  for (int trial = 0; trial < FLAGS_num_iters; trial++) {
    vector<IterWithBounds> to_merge;
    for (const auto& list : all_ints) {
      unique_ptr<VectorIterator> vec_it(new VectorIterator(list.ints, list.is_deleted, schema));
      vec_it->set_block_size(10);
      vec_it->set_selection_vector(list.sv.get());
      unique_ptr<RowwiseIterator> mat_it(NewMaterializingIterator(std::move(vec_it)));
      IterWithBounds mat_iwb;
      mat_iwb.iter = std::move(mat_it);
      vector<IterWithBounds> un_input;
      un_input.emplace_back(std::move(mat_iwb));
      unique_ptr<RowwiseIterator> un_it(NewUnionIterator(std::move(un_input)));
      IterWithBounds un_iwb;
      un_iwb.iter = std::move(un_it);
      if (list.encoded_bounds) {
        un_iwb.encoded_bounds = list.encoded_bounds;
      }
      to_merge.emplace_back(std::move(un_iwb));
    }

    // Setup predicate exclusion
    ScanSpec spec;
    spec.AddPredicate(predicate.pred_);
    LOG(INFO) << "Predicate: " << predicate.pred_.ToString();

    LOG_TIMING(INFO, "iterating merged lists") {
      unique_ptr<RowwiseIterator> merger(NewMergeIterator(
          MergeIteratorOptions(include_deleted_rows), std::move(to_merge)));
      ASSERT_OK(merger->Init(&spec));

      RowBlock dst(&schema, 100, nullptr);
      size_t total_idx = 0;
      auto expected_iter = expected.cbegin();
      while (merger->HasNext()) {
        ASSERT_OK(merger->NextBlock(&dst));
        ASSERT_GT(dst.nrows(), 0) <<
          "if HasNext() returns true, must return some rows";

        for (int i = 0; i < dst.nrows(); i++) {
          ASSERT_NE(expected.end(), expected_iter);
          int64_t expected_key = expected_iter->first;
          bool expected_is_deleted = expected_iter->second;
          int64_t row_key = *schema.ExtractColumnFromRow<INT64>(dst.row(i), kValColIdx);
          ASSERT_GE(row_key, predicate.lower_) << "Yielded integer excluded by predicate";
          ASSERT_LT(row_key, predicate.upper_) << "Yielded integer excluded by predicate";
          EXPECT_EQ(expected_key, row_key) << "Yielded out of order at idx " << total_idx;
          bool is_deleted = false;
          if (include_deleted_rows) {
            CHECK_NE(Schema::kColumnNotFound, kIsDeletedIndex);
            is_deleted = *schema.ExtractColumnFromRow<IS_DELETED>(dst.row(i), kIsDeletedIndex);
            EXPECT_EQ(expected_is_deleted, is_deleted)
                << "Row " << row_key << " has unexpected IS_DELETED value at index " << total_idx;
          }
          VLOG(2) << "Observed: val=" << row_key << ", is_deleted=" << is_deleted;
          VLOG(2) << "Expected: val=" << expected_key << ", is_deleted=" << expected_is_deleted;
          ++expected_iter;
          ++total_idx;
        }
      }
      ASSERT_EQ(expected.size(), total_idx);
      ASSERT_EQ(expected.end(), expected_iter);
    }
  }
}

TEST(TestMergeIterator, TestMerge) {
  TestIntRangePredicate predicate(0, MathLimits<int64_t>::kMax);
  NO_FATALS(TestMerge(kIntSchema, predicate));
}

TEST(TestMergeIterator, TestMergeNonOverlapping) {
  TestIntRangePredicate predicate(0, MathLimits<int64_t>::kMax);
  NO_FATALS(TestMerge(kIntSchema, predicate, /*overlapping_ranges=*/false));
}

TEST(TestMergeIterator, TestMergePredicate) {
  TestIntRangePredicate predicate(0, FLAGS_num_rows / 5);
  NO_FATALS(TestMerge(kIntSchema, predicate));
}

// Regression test for a bug in the merge which would incorrectly
// drop a merge input if it received an entirely non-selected block.
// This predicate excludes the first half of the rows but accepts the
// second half.
TEST(TestMergeIterator, TestMergePredicate2) {
  TestIntRangePredicate predicate(FLAGS_num_rows / 2, MathLimits<int64_t>::kMax);
  NO_FATALS(TestMerge(kIntSchema, predicate));
}

TEST(TestMergeIterator, TestDeDupGhostRows) {
  TestIntRangePredicate match_all_pred(0, MathLimits<int64_t>::kMax);
  NO_FATALS(TestMerge(kIntSchemaWithVCol, match_all_pred,
                      /*overlapping_ranges=*/true,
                      /*include_deleted_rows=*/true));
}

// Test that the MaterializingIterator properly evaluates predicates when they apply
// to single columns.
TEST(TestMaterializingIterator, TestMaterializingPredicatePushdown) {
  ScanSpec spec;
  TestIntRangePredicate pred1(20, 30);
  spec.AddPredicate(pred1.pred_);
  LOG(INFO) << "Predicate: " << pred1.pred_.ToString();

  vector<int64_t> ints(100);
  for (int i = 0; i < 100; i++) {
    ints[i] = i;
  }

  unique_ptr<VectorIterator> colwise(new VectorIterator(ints));
  unique_ptr<RowwiseIterator> materializing(NewMaterializingIterator(std::move(colwise)));
  ASSERT_OK(materializing->Init(&spec));
  ASSERT_EQ(0, spec.predicates().size()) << "Iterator should have pushed down predicate";

  Arena arena(1024);
  RowBlock dst(&kIntSchema, 100, &arena);
  ASSERT_OK(materializing->NextBlock(&dst));
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

  vector<int64_t> ints(100);
  for (int i = 0; i < 100; i++) {
    ints[i] = i;
  }

  // Set up a MaterializingIterator with pushdown disabled, so that the
  // PredicateEvaluatingIterator will wrap it and do evaluation.
  unique_ptr<VectorIterator> colwise(new VectorIterator(ints));
  google::FlagSaver saver;
  FLAGS_materializing_iterator_do_pushdown = false;
  unique_ptr<RowwiseIterator> materializing(
      NewMaterializingIterator(std::move(colwise)));

  // Wrap it in another iterator to do the evaluation
  const RowwiseIterator* mat_iter_addr = materializing.get();
  unique_ptr<RowwiseIterator> outer_iter(std::move(materializing));
  ASSERT_OK(InitAndMaybeWrap(&outer_iter, &spec));

  ASSERT_NE(reinterpret_cast<uintptr_t>(outer_iter.get()),
            reinterpret_cast<uintptr_t>(mat_iter_addr))
    << "Iterator pointer should differ after wrapping";

  ASSERT_EQ(0, spec.predicates().size())
    << "Iterator tree should have accepted predicate";
  ASSERT_EQ(1, GetIteratorPredicatesForTests(outer_iter).size())
    << "Predicate should be evaluated by the outer iterator";

  Arena arena(1024);
  RowBlock dst(&kIntSchema, 100, &arena);
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

  unique_ptr<VectorIterator> colwise(new VectorIterator({}));
  unique_ptr<RowwiseIterator> materializing(
      NewMaterializingIterator(std::move(colwise)));
  const RowwiseIterator* mat_iter_addr = materializing.get();
  unique_ptr<RowwiseIterator> outer_iter(std::move(materializing));
  ASSERT_OK(InitAndMaybeWrap(&outer_iter, &spec));
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
    ASSERT_OK(InitAndMaybeWrap(&iter, &spec));
    ASSERT_EQ(GetIteratorPredicatesForTests(iter),
              vector<ColumnPredicate>({ c_equality, b_equality, a_range }));
  }

  { // Test that smaller columns come before larger ones, and ties are broken by idx.
    ScanSpec spec;
    spec.AddPredicate(b_equality);
    spec.AddPredicate(a_equality);
    spec.AddPredicate(c_equality);

    unique_ptr<RowwiseIterator> iter(new DummyIterator(schema));
    ASSERT_OK(InitAndMaybeWrap(&iter, &spec));

    ASSERT_EQ(GetIteratorPredicatesForTests(iter),
              vector<ColumnPredicate>({ c_equality, a_equality, b_equality }));
  }
}

} // namespace kudu
