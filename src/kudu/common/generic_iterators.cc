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

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/memory/arena.h"

using std::get;
using std::move;
using std::pair;
using std::remove_if;
using std::shared_ptr;
using std::sort;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DEFINE_bool(materializing_iterator_do_pushdown, true,
            "Should MaterializingIterator do predicate pushdown");
TAG_FLAG(materializing_iterator_do_pushdown, hidden);
DEFINE_bool(materializing_iterator_decoder_eval, true,
            "Should MaterializingIterator do decoder-level evaluation");
TAG_FLAG(materializing_iterator_decoder_eval, hidden);
TAG_FLAG(materializing_iterator_decoder_eval, runtime);

namespace kudu {
namespace {
void AddIterStats(const RowwiseIterator& iter,
                  vector<IteratorStats>* stats) {
  vector<IteratorStats> iter_stats;
  iter.GetIteratorStats(&iter_stats);
  DCHECK_EQ(stats->size(), iter_stats.size());
  for (int i = 0; i < iter_stats.size(); i++) {
    (*stats)[i] += iter_stats[i];
  }
}
} // anonymous namespace

////////////////////////////////////////////////////////////
// Merge iterator
////////////////////////////////////////////////////////////

// TODO(todd): this should be sized by # bytes, not # rows.
static const int kMergeRowBuffer = 1000;

// MergeIterState wraps a RowwiseIterator for use by the MergeIterator.
// Importantly, it also filters out unselected rows from the wrapped RowwiseIterator,
// such that all returned rows are valid.
class MergeIterState {
 public:
  explicit MergeIterState(unique_ptr<RowwiseIterator> iter) :
      iter_(std::move(iter)),
      arena_(1024),
      read_block_(iter_->schema(), kMergeRowBuffer, &arena_),
      next_row_idx_(0),
      rows_advanced_(0),
      rows_valid_(0)
  {}

  // Fetch the next row from the iterator. Does not advance the iterator.
  // IsFullyExhausted() must return false at the time this method is invoked.
  const RowBlockRow& next_row() const {
    DCHECK_LT(rows_advanced_, rows_valid_);
    return next_row_;
  }

  // Initialize the underlying iterator to point to the first valid row, if
  // any. This method should be called before calling any other methods.
  Status Init() {
    CHECK_EQ(0, rows_valid_);
    return PullNextBlock();
  }

  // Returns true if the underlying iterator is fully exhausted.
  bool IsFullyExhausted() const {
    return rows_valid_ == 0 && !iter_->HasNext();
  }

  // Advance to the next row in the underlying iterator.
  Status Advance();

  // Add statistics about the underlying iterator to the given vector.
  void AddStats(vector<IteratorStats>* stats) const {
    AddIterStats(*iter_, stats);
  }

  // Returns the number of valid rows remaining in the current block.
  size_t remaining_in_block() const {
    return rows_valid_ - rows_advanced_;
  }

  // Returns the schema from the underlying iterator.
  const Schema& schema() const {
    return iter_->schema();
  }

 private:
  // Pull the next block from the underlying iterator.
  Status PullNextBlock();

  // Returns true if the current block in the underlying iterator is exhausted.
  bool IsBlockExhausted() const {
    return rows_advanced_ == rows_valid_;
  }

  unique_ptr<RowwiseIterator> iter_;
  Arena arena_;
  RowBlock read_block_;

  // The row currently pointed to by the iterator.
  RowBlockRow next_row_;

  // Row index of next_row_ in read_block_.
  size_t next_row_idx_;

  // Number of rows we've advanced past in the current RowBlock.
  size_t rows_advanced_;

  // Number of valid (selected) rows in the current RowBlock.
  size_t rows_valid_;

  DISALLOW_COPY_AND_ASSIGN(MergeIterState);
};

Status MergeIterState::Advance() {
  rows_advanced_++;
  if (IsBlockExhausted()) {
    arena_.Reset();
    return PullNextBlock();
  }

  // Seek to the next selected row.
  SelectionVector *selection = read_block_.selection_vector();
  for (++next_row_idx_; next_row_idx_ < read_block_.nrows(); next_row_idx_++) {
    if (selection->IsRowSelected(next_row_idx_)) {
      next_row_.Reset(&read_block_, next_row_idx_);
      break;
    }
  }
  DCHECK_NE(next_row_idx_, read_block_.nrows()+1) << "No selected rows found!";
  return Status::OK();
}

Status MergeIterState::PullNextBlock() {
  CHECK_EQ(rows_advanced_, rows_valid_)
      << "should not pull next block until current block is exhausted";

  while (iter_->HasNext()) {
    RETURN_NOT_OK(iter_->NextBlock(&read_block_));
    rows_advanced_ = 0;
    // Honor the selection vector of the read_block_, since not all rows are necessarily selected.
    SelectionVector *selection = read_block_.selection_vector();
    DCHECK_EQ(selection->nrows(), read_block_.nrows());
    DCHECK_LE(selection->CountSelected(), read_block_.nrows());
    rows_valid_ = selection->CountSelected();
    VLOG(2) << Substitute("$0/$1 rows selected", rows_valid_, read_block_.nrows());
    if (rows_valid_ == 0) {
      // Short-circuit: this block is entirely unselected and can be skipped.
      continue;
    }

    // Seek next_row_ to the first selected row.
    CHECK(selection->FindFirstRowSelected(&next_row_idx_));
    next_row_.Reset(&read_block_, next_row_idx_);
    return Status::OK();
  }

  // The underlying iterator is fully exhausted.
  rows_advanced_ = 0;
  rows_valid_ = 0;
  return Status::OK();
}

MergeIterator::MergeIterator(vector<unique_ptr<RowwiseIterator>> iters)
    : initted_(false),
      orig_iters_(move(iters)),
      num_orig_iters_(orig_iters_.size()) {
  CHECK_GT(orig_iters_.size(), 0);
}

MergeIterator::~MergeIterator() {}

Status MergeIterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  // Initialize the iterators and construct the per-iterator merge states.
  //
  // When this method finishes, orig_iters_ has been cleared and states_ has
  // been populated.
  RETURN_NOT_OK(InitSubIterators(spec));

  // Retrieve every iterator's first block.
  //
  // TODO(adar): establish dominance between iterators and only initialize
  // non-dominated iterators.
  for (auto& s : states_) {
    RETURN_NOT_OK(s->Init());
  }

  // Verify that the schemas match in debug builds.
  //
  // It's important to do the verification after initializing the iterators, as
  // they may not know their own schemas until they've been initialized (in the
  // case of a union of unions).
  schema_.reset(new Schema(states_.front()->schema()));
  CHECK_GT(schema_->num_key_columns(), 0);
  finished_iter_stats_by_col_.resize(schema_->num_columns());
#ifndef NDEBUG
  for (const auto& s : states_) {
    if (!s->schema().Equals(*schema_)) {
      return Status::InvalidArgument(
          Substitute("Schemas do not match: $0 vs. $1",
                     schema_->ToString(), s->schema().ToString()));
    }
  }
#endif

  // Before we copy any rows, clean up any iterators which were empty
  // to start with. Otherwise, HasNext() won't properly return false
  // if we were passed only empty iterators.
  states_.erase(
      remove_if(states_.begin(), states_.end(), [] (const unique_ptr<MergeIterState>& iter) {
        return PREDICT_FALSE(iter->IsFullyExhausted());
      }),
      states_.end());

  initted_ = true;
  return Status::OK();
}

bool MergeIterator::HasNext() const {
  CHECK(initted_);
  return !states_.empty();
}

Status MergeIterator::InitSubIterators(ScanSpec *spec) {
  // Initialize all the sub iterators.
  for (auto& i : orig_iters_) {
    ScanSpec *spec_copy = spec != nullptr ? scan_spec_copies_.Construct(*spec) : nullptr;
    RETURN_NOT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&i, spec_copy));
    states_.push_back(unique_ptr<MergeIterState>(new MergeIterState(std::move(i))));
  }
  orig_iters_.clear();

  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != nullptr) {
    spec->RemovePredicates();
  }
  return Status::OK();
}

Status MergeIterator::NextBlock(RowBlock* dst) {
  CHECK(initted_);
  DCHECK_SCHEMA_EQ(dst->schema(), schema());

  PrepareBatch(dst);
  RETURN_NOT_OK(MaterializeBlock(dst));

  return Status::OK();
}

void MergeIterator::PrepareBatch(RowBlock* dst) {
  if (dst->arena()) {
    dst->arena()->Reset();
  }

  // We can always provide at least as many rows as are remaining
  // in the currently queued up blocks.
  size_t available = 0;
  for (const auto& s : states_) {
    available += s->remaining_in_block();
  }

  dst->Resize(std::min(dst->row_capacity(), available));
}

// TODO(todd): this is an obvious spot to add codegen - there's a ton of branching
// and such around the comparisons. A simple experiment indicated there's some
// 2x to be gained.
Status MergeIterator::MaterializeBlock(RowBlock *dst) {
  // Initialize the selection vector.
  // MergeIterState only returns selected rows.
  dst->selection_vector()->SetAllTrue();
  for (size_t dst_row_idx = 0; dst_row_idx < dst->nrows(); dst_row_idx++) {
    RowBlockRow dst_row = dst->row(dst_row_idx);

    // Find the sub-iterator which is currently smallest
    MergeIterState *smallest = nullptr;
    ssize_t smallest_idx = -1;

    // Typically the number of states_ is not that large, so using a priority
    // queue is not worth it
    for (size_t i = 0; i < states_.size(); i++) {
      unique_ptr<MergeIterState> &state = states_[i];

      if (smallest == nullptr ||
          schema_->Compare(state->next_row(), smallest->next_row()) < 0) {
        smallest = state.get();
        smallest_idx = i;
      }
    }

    // If no iterators had any row left, then we're done iterating.
    if (PREDICT_FALSE(smallest == nullptr)) break;

    // Otherwise, copy the row from the smallest one, and advance it
    RETURN_NOT_OK(CopyRow(smallest->next_row(), &dst_row, dst->arena()));
    RETURN_NOT_OK(smallest->Advance());

    if (smallest->IsFullyExhausted()) {
      std::lock_guard<rw_spinlock> l(states_lock_);
      smallest->AddStats(&finished_iter_stats_by_col_);
      states_.erase(states_.begin() + smallest_idx);
    }
  }

  return Status::OK();
}

string MergeIterator::ToString() const {
  return Substitute("Merge($0 iters)", num_orig_iters_);
}

const Schema& MergeIterator::schema() const {
  CHECK(initted_);
  return *schema_;
}

void MergeIterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  shared_lock<rw_spinlock> l(states_lock_);
  CHECK(initted_);
  *stats = finished_iter_stats_by_col_;

  for (const auto& s : states_) {
    s->AddStats(stats);
  }
}


////////////////////////////////////////////////////////////
// Union iterator
////////////////////////////////////////////////////////////

UnionIterator::UnionIterator(vector<unique_ptr<RowwiseIterator>> iters)
  : initted_(false),
    iters_(std::make_move_iterator(iters.begin()),
           std::make_move_iterator(iters.end())) {
  CHECK_GT(iters_.size(), 0);
}

Status UnionIterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  // Initialize the underlying iterators
  RETURN_NOT_OK(InitSubIterators(spec));

  // Verify that the schemas match in debug builds.
  //
  // It's important to do the verification after initializing the iterators, as
  // they may not know their own schemas until they've been initialized (in the
  // case of a union of unions).
  schema_.reset(new Schema(iters_.front()->schema()));
  finished_iter_stats_by_col_.resize(schema_->num_columns());
#ifndef NDEBUG
  for (const auto& i : iters_) {
    if (!i->schema().Equals(*schema_)) {
      return Status::InvalidArgument(
          Substitute("Schemas do not match: $0 vs. $1",
                     schema_->ToString(), i->schema().ToString()));
    }
  }
#endif

  initted_ = true;
  return Status::OK();
}


Status UnionIterator::InitSubIterators(ScanSpec *spec) {
  for (auto& i : iters_) {
    ScanSpec *spec_copy = spec != nullptr ? scan_spec_copies_.Construct(*spec) : nullptr;
    RETURN_NOT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&i, spec_copy));
  }
  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != nullptr) {
    spec->RemovePredicates();
  }
  return Status::OK();
}

bool UnionIterator::HasNext() const {
  CHECK(initted_);
  for (const auto& i : iters_) {
    if (i->HasNext()) return true;
  }

  return false;
}

Status UnionIterator::NextBlock(RowBlock* dst) {
  CHECK(initted_);
  PrepareBatch();
  RETURN_NOT_OK(MaterializeBlock(dst));
  FinishBatch();
  return Status::OK();
}

void UnionIterator::PrepareBatch() {
  CHECK(initted_);

  while (!iters_.empty() &&
         !iters_.front()->HasNext()) {
    PopFront();
  }
}

Status UnionIterator::MaterializeBlock(RowBlock *dst) {
  return iters_.front()->NextBlock(dst);
}

void UnionIterator::FinishBatch() {
  if (!iters_.front()->HasNext()) {
    // Iterator exhausted, remove it.
    PopFront();
  }
}

void UnionIterator::PopFront() {
  std::lock_guard<rw_spinlock> l(iters_lock_);
  AddIterStats(*iters_.front(), &finished_iter_stats_by_col_);
  iters_.pop_front();
}

string UnionIterator::ToString() const {
  string s;
  s.append("Union(");
  s += JoinMapped(iters_, [](const unique_ptr<RowwiseIterator>& it) {
      return it->ToString();
    }, ",");
  s.append(")");
  return s;
}

void UnionIterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  CHECK(initted_);
  shared_lock<rw_spinlock> l(iters_lock_);
  *stats = finished_iter_stats_by_col_;
  if (!iters_.empty()) {
    AddIterStats(*iters_.front(), stats);
  }
}

////////////////////////////////////////////////////////////
// Materializing iterator
////////////////////////////////////////////////////////////

MaterializingIterator::MaterializingIterator(unique_ptr<ColumnwiseIterator> iter)
    : iter_(move(iter)),
      disallow_pushdown_for_tests_(!FLAGS_materializing_iterator_do_pushdown),
      disallow_decoder_eval_(!FLAGS_materializing_iterator_decoder_eval) {
}

Status MaterializingIterator::Init(ScanSpec *spec) {
  RETURN_NOT_OK(iter_->Init(spec));

  int32_t num_columns = schema().num_columns();
  col_idx_predicates_.clear();
  non_predicate_column_indexes_.clear();

  if (spec != nullptr && !disallow_pushdown_for_tests_) {
    col_idx_predicates_.reserve(spec->predicates().size());
    non_predicate_column_indexes_.reserve(num_columns - spec->predicates().size());

    for (const auto& col_pred : spec->predicates()) {
      const ColumnPredicate& pred = col_pred.second;
      int col_idx = schema().find_column(pred.column().name());
      if (col_idx == Schema::kColumnNotFound) {
        return Status::InvalidArgument("No such column", col_pred.first);
      }
      VLOG(1) << "Pushing down predicate " << pred.ToString();
      col_idx_predicates_.emplace_back(col_idx, col_pred.second);
    }

    for (int32_t col_idx = 0; col_idx < schema().num_columns(); col_idx++) {
      if (!ContainsKey(spec->predicates(), schema().column(col_idx).name())) {
        non_predicate_column_indexes_.emplace_back(col_idx);
      }
    }

    // Since we'll evaluate these predicates ourselves, remove them from the
    // scan spec so higher layers don't repeat our work.
    spec->RemovePredicates();
  } else {
    non_predicate_column_indexes_.reserve(num_columns);
    for (int32_t col_idx = 0; col_idx < num_columns; col_idx++) {
      non_predicate_column_indexes_.emplace_back(col_idx);
    }
  }

  // Sort the predicates by selectivity so that the most selective are evaluated
  // earlier, with ties broken by the column index.
  sort(col_idx_predicates_.begin(), col_idx_predicates_.end(),
       [] (const pair<int32_t, ColumnPredicate>& left,
           const pair<int32_t, ColumnPredicate>& right) {
          int comp = SelectivityComparator(left.second, right.second);
          return comp ? comp < 0 : left.first < right.first;
       });

  return Status::OK();
}

bool MaterializingIterator::HasNext() const {
  return iter_->HasNext();
}

Status MaterializingIterator::NextBlock(RowBlock* dst) {
  size_t n = dst->row_capacity();
  if (dst->arena()) {
    dst->arena()->Reset();
  }

  RETURN_NOT_OK(iter_->PrepareBatch(&n));
  dst->Resize(n);
  RETURN_NOT_OK(MaterializeBlock(dst));
  RETURN_NOT_OK(iter_->FinishBatch());

  return Status::OK();
}

Status MaterializingIterator::MaterializeBlock(RowBlock *dst) {
  // Initialize the selection vector indicating which rows have been
  // been deleted.
  RETURN_NOT_OK(iter_->InitializeSelectionVector(dst->selection_vector()));

  // It's relatively common to delete large sequential chunks of rows.
  // We can fast-path that case and avoid reading any column data.
  if (!dst->selection_vector()->AnySelected()) {
    DVLOG(1) << "Fast path over " << dst->nrows() << " deleted rows";
    return Status::OK();
  }

  for (const auto& col_pred : col_idx_predicates_) {
    // Materialize the column itself into the row block.
    ColumnBlock dst_col(dst->column_block(get<0>(col_pred)));
    ColumnMaterializationContext ctx(get<0>(col_pred),
                                     &get<1>(col_pred),
                                     &dst_col,
                                     dst->selection_vector());
    // None predicates should be short-circuited in scan spec.
    DCHECK(ctx.pred()->predicate_type() != PredicateType::None);
    if (disallow_decoder_eval_) {
      ctx.SetDecoderEvalNotSupported();
    }
    RETURN_NOT_OK(iter_->MaterializeColumn(&ctx));
    if (ctx.DecoderEvalNotSupported()) {
      get<1>(col_pred).Evaluate(dst_col, dst->selection_vector());
    }

    // If after evaluating this predicate the entire row block has been filtered
    // out, we don't need to materialize other columns at all.
    if (!dst->selection_vector()->AnySelected()) {
      DVLOG(1) << "0/" << dst->nrows() << " passed predicate";
      return Status::OK();
    }
  }

  for (size_t col_idx : non_predicate_column_indexes_) {
    // Materialize the column itself into the row block.
    ColumnBlock dst_col(dst->column_block(col_idx));
    ColumnMaterializationContext ctx(col_idx,
                                     nullptr,
                                     &dst_col,
                                     dst->selection_vector());
    RETURN_NOT_OK(iter_->MaterializeColumn(&ctx));
  }

  DVLOG(1) << dst->selection_vector()->CountSelected() << "/"
           << dst->nrows() << " passed predicate";
  return Status::OK();
}

string MaterializingIterator::ToString() const {
  string s;
  s.append("Materializing(").append(iter_->ToString()).append(")");
  return s;
}

////////////////////////////////////////////////////////////
// PredicateEvaluatingIterator
////////////////////////////////////////////////////////////

PredicateEvaluatingIterator::PredicateEvaluatingIterator(unique_ptr<RowwiseIterator> base_iter)
    : base_iter_(move(base_iter)) {
}

Status PredicateEvaluatingIterator::InitAndMaybeWrap(
  unique_ptr<RowwiseIterator> *base_iter, ScanSpec *spec) {
  RETURN_NOT_OK((*base_iter)->Init(spec));

  if (spec != nullptr && !spec->predicates().empty()) {
    // Underlying iterator did not accept all predicates. Wrap it.
    unique_ptr<RowwiseIterator> wrapper(
        new PredicateEvaluatingIterator(std::move(*base_iter)));
    CHECK_OK(wrapper->Init(spec));
    *base_iter = std::move(wrapper);
  }
  return Status::OK();
}

Status PredicateEvaluatingIterator::Init(ScanSpec *spec) {
  // base_iter_ already Init()ed before this is constructed.
  CHECK_NOTNULL(spec);

  // Gather any predicates that the base iterator did not pushdown, and remove
  // the predicates from the spec.
  col_predicates_.clear();
  col_predicates_.reserve(spec->predicates().size());
  for (auto& predicate : spec->predicates()) {
    col_predicates_.emplace_back(predicate.second);
  }
  spec->RemovePredicates();

  // Sort the predicates by selectivity so that the most selective are evaluated
  // earlier, with ties broken by the column index.
  sort(col_predicates_.begin(), col_predicates_.end(),
       [&] (const ColumnPredicate& left, const ColumnPredicate& right) {
          int comp = SelectivityComparator(left, right);
          if (comp != 0) return comp < 0;
          return schema().find_column(left.column().name())
               < schema().find_column(right.column().name());
       });

  return Status::OK();
}

bool PredicateEvaluatingIterator::HasNext() const {
  return base_iter_->HasNext();
}

Status PredicateEvaluatingIterator::NextBlock(RowBlock *dst) {
  RETURN_NOT_OK(base_iter_->NextBlock(dst));

  for (const auto& predicate : col_predicates_) {
    int32_t col_idx = dst->schema().find_column(predicate.column().name());
    if (col_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument("Unknown column in predicate", predicate.ToString());
    }
    predicate.Evaluate(dst->column_block(col_idx), dst->selection_vector());

    // If after evaluating this predicate, the entire row block has now been
    // filtered out, we don't need to evaluate any further predicates.
    if (!dst->selection_vector()->AnySelected()) {
      break;
    }
  }

  return Status::OK();
}

string PredicateEvaluatingIterator::ToString() const {
  return Substitute("PredicateEvaluating($0)", base_iter_->ToString());
}

} // namespace kudu
