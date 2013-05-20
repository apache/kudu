// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <string>

#include "common/generic_iterators.h"
#include "common/row.h"
#include "common/rowblock.h"
#include "gutil/gscoped_ptr.h"
#include "util/memory/arena.h"

DEFINE_bool(materializing_iterator_do_pushdown, true,
            "Should MaterializingIterator do predicate pushdown"
            " (advanced option, only for debugging)");

namespace kudu {

using std::string;
using std::tr1::shared_ptr;

////////////////////////////////////////////////////////////
// Merge iterator
////////////////////////////////////////////////////////////

// TODO: size by bytes, not # rows
static const int kMergeRowBuffer = 1000;

class MergeIterState {
public:
  MergeIterState(const shared_ptr<RowwiseIterator> &iter) :
    iter_(iter),
    arena_(1024, 256*1024),
    read_block_(iter->schema(), kMergeRowBuffer, &arena_),
    cur_row_(0),
    valid_rows_(0)
  {}

  const RowBlockRow& next_row() {
    DCHECK_LT(cur_row_, valid_rows_);
    return next_row_;
  }

  Status Advance() {
    cur_row_++;
    if (IsBlockExhausted()) {
      arena_.Reset();
      return PullNextBlock();
    } else {
      // TODO: Manually advancing next_row_ptr_ is some 20% faster
      // than calling row_ptr(cur_row_), since it avoids an expensive multiplication
      // in the inner loop.
      next_row_.Reset(&read_block_, cur_row_);

      return Status::OK();
    }
  }

  bool IsBlockExhausted() const {
    return cur_row_ == valid_rows_;
  }

  bool IsFullyExhausted() const {
    return valid_rows_ == 0;
  }

  Status PullNextBlock() {
    CHECK_EQ(cur_row_, valid_rows_)
      << "should not pull next block until current block is exhausted";

    if (!iter_->HasNext()) {
      // Fully exhausted
      cur_row_ = 0;
      valid_rows_ = 0;
      return Status::OK();
    }

    RETURN_NOT_OK(RowwiseIterator::CopyBlock(iter_.get(), &read_block_));
    cur_row_ = 0;
    // TODO: do we need valid_rows_ or can we just use read_block_.nrows()?
    valid_rows_ = read_block_.nrows();
    next_row_.Reset(&read_block_, 0);
    return Status::OK();
  }

  size_t remaining_in_block() const {
    return valid_rows_ - cur_row_;
  }

  shared_ptr<RowwiseIterator> iter_;
  Arena arena_;
  RowBlock read_block_;
  RowBlockRow next_row_;
  size_t cur_row_;
  size_t valid_rows_;
};


MergeIterator::MergeIterator(
  const Schema &schema,
  const vector<shared_ptr<RowwiseIterator> > &iters)
  : schema_(schema),
    initted_(false),
    prepared_count_(0)
{
  CHECK_GT(iters.size(), 0);
  BOOST_FOREACH(const shared_ptr<RowwiseIterator> &iter, iters) {
    iters_.push_back(shared_ptr<MergeIterState>(new MergeIterState(iter)));
  }
}


Status MergeIterator::Init(ScanSpec *spec) {
  CHECK(!initted_);
  // TODO: check that schemas match up!

  RETURN_NOT_OK(InitSubIterators(spec));

  BOOST_FOREACH(shared_ptr<MergeIterState> &state, iters_) {
    RETURN_NOT_OK(state->PullNextBlock());
  }

  // Before we copy any rows, clean up any iterators which were empty
  // to start with. Otherwise, HasNext() won't properly return false
  // if we were passed only empty iterators.
  for (size_t i = 0; i < iters_.size(); i++) {
    if (PREDICT_FALSE(iters_[i]->IsFullyExhausted())) {
      iters_.erase(iters_.begin() + i);
      i--;
      continue;
    }
  }

  initted_ = true;
  return Status::OK();
}

Status MergeIterator::InitSubIterators(ScanSpec *spec) {
  BOOST_FOREACH(shared_ptr<MergeIterState> &state, iters_) {
    ScanSpec *spec_copy = spec != NULL ? scan_spec_copies_.Construct(*spec) : NULL;
    RETURN_NOT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&state->iter_, spec_copy));
  }
  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != NULL) {
    spec->mutable_predicates()->clear();
  }
  return Status::OK();
}


Status MergeIterator::PrepareBatch(size_t *nrows) {
  // We can always provide at least as many rows as are remaining
  // in the currently queued up blocks.
  size_t available = 0;
  BOOST_FOREACH(shared_ptr<MergeIterState> &iter, iters_) {
    available += iter->remaining_in_block();
  }

  if (available < *nrows) {
    *nrows = available;
  }

  prepared_count_ = *nrows;

  return Status::OK();
}

// TODO: this is an obvious spot to add codegen - there's a ton of branching
// and such around the comparisons. A simple experiment indicated there's some
// 2x to be gained.
Status MergeIterator::MaterializeBlock(RowBlock *dst) {
  CHECK(initted_);

  DCHECK_SCHEMA_EQ(dst->schema(), schema());
  DCHECK_LE(prepared_count_, dst->nrows());

  for (size_t dst_row_idx = 0; dst_row_idx < prepared_count_; dst_row_idx++) {
    RowBlockRow dst_row = dst->row(dst_row_idx);

    // Find the sub-iterator which is currently smallest
    MergeIterState *smallest = NULL;
    size_t smallest_idx;

    for (size_t i = 0; i < iters_.size(); i++) {
      shared_ptr<MergeIterState> &state = iters_[i];

      if (smallest == NULL ||
          schema_.Compare(state->next_row(), smallest->next_row()) < 0) {
        smallest = state.get();
        smallest_idx = i;
      }
    }

    // If no iterators had any row left, then we're done iterating.
    if (PREDICT_FALSE(smallest == NULL)) break;

    // Otherwise, copy the row from the smallest one, and advance it
    dst_row.CopyCellsFrom(schema_, smallest->next_row());
    if (dst->arena() != NULL) {
      RETURN_NOT_OK(dst_row.CopyIndirectDataToArena(dst->arena()));
    }

    RETURN_NOT_OK(smallest->Advance());

    if (smallest->IsFullyExhausted()) {
      iters_.erase(iters_.begin() + smallest_idx);
    }
  }

  return Status::OK();
}

Status MergeIterator::FinishBatch() {
  prepared_count_ = 0;
  return Status::OK();
}

string MergeIterator::ToString() const {
  string s;
  s.append("Merge(");
  bool first = true;
  BOOST_FOREACH(const shared_ptr<MergeIterState> &iter, iters_) {
    s.append(iter->iter_->ToString());
    if (!first) {
      s.append(", ");
    }
    first = false;
  }
  s.append(")");
  return s;
}


////////////////////////////////////////////////////////////
// Union iterator
////////////////////////////////////////////////////////////

UnionIterator::UnionIterator(const vector<shared_ptr<RowwiseIterator> > &iters) :
  initted_(false),
  iters_(iters.size())
{
  CHECK_GT(iters.size(), 0);
  iters_.assign(iters.begin(), iters.end());
}

Status UnionIterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  // Initialize the underlying iterators
  RETURN_NOT_OK(InitSubIterators(spec));

  // Verify schemas match.
  // Important to do the verification after initializing the
  // sub-iterators, since they may not know their own schemas
  // until they've been initialized (in the case of a union of unions)
  schema_.reset(new Schema(iters_.front()->schema()));
  BOOST_FOREACH(const shared_ptr<RowwiseIterator> &iter, iters_) {
    if (!iter->schema().Equals(*schema_)) {
      return Status::InvalidArgument(
        string("Schemas do not match: ") + schema_->ToString()
        + " vs " + iter->schema().ToString());
    }
  }

  initted_ = true;
  return Status::OK();
}


Status UnionIterator::InitSubIterators(ScanSpec *spec) {
  BOOST_FOREACH(shared_ptr<RowwiseIterator> &iter, iters_) {
    ScanSpec *spec_copy = spec != NULL ? scan_spec_copies_.Construct(*spec) : NULL;
    RETURN_NOT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&iter, spec_copy));
  }
  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != NULL) {
    spec->mutable_predicates()->clear();
  }
  return Status::OK();
}

bool UnionIterator::HasNext() const {
  BOOST_FOREACH(const shared_ptr<RowwiseIterator> &iter, iters_) {
    if (iter->HasNext()) return true;
  }

  return false;
}

Status UnionIterator::PrepareBatch(size_t *nrows) {
  CHECK(initted_);

  while (!iters_.empty() &&
         !iters_.front()->HasNext()) {
    iters_.pop_front();
  }
  if (iters_.empty()) {
    *nrows = 0;
    return Status::OK();
  }

  return iters_.front()->PrepareBatch(nrows);
}

Status UnionIterator::MaterializeBlock(RowBlock *dst) {
  return iters_.front()->MaterializeBlock(dst);
}

Status UnionIterator::FinishBatch() {
  RETURN_NOT_OK(iters_.front()->FinishBatch());
  if (!iters_.front()->HasNext()) {
    // Iterator exhausted, remove it.
    iters_.pop_front();
  }
  return Status::OK();
}


string UnionIterator::ToString() const {
  string s;
  s.append("Union(");
  bool first = true;
  BOOST_FOREACH(const shared_ptr<RowwiseIterator> &iter, iters_) {
    if (!first) {
      s.append(", ");
    }
    first = false;
    s.append(iter->ToString());
  }
  s.append(")");
  return s;
}

////////////////////////////////////////////////////////////
// Materializing iterator
////////////////////////////////////////////////////////////

MaterializingIterator::MaterializingIterator(const shared_ptr<ColumnwiseIterator> &iter) :
  iter_(iter),
  prepared_count_(0),
  disallow_pushdown_for_tests_(!FLAGS_materializing_iterator_do_pushdown)
{
}

Status MaterializingIterator::Init(ScanSpec *spec) {
  RETURN_NOT_OK(iter_->Init(spec));

  if (spec != NULL && !disallow_pushdown_for_tests_) {
    // Gather any single-column predicates.
    ScanSpec::PredicateList *preds = spec->mutable_predicates();
    for (ScanSpec::PredicateList::iterator iter = preds->begin();
         iter != preds->end();) {
      const ColumnRangePredicate &pred = *iter;
      const string &col_name = pred.column().name();
      int idx = schema().find_column(col_name);
      if (idx == -1) {
        return Status::InvalidArgument("No such column", col_name);
      }

      VLOG(1) << "Pushing down predicate " << pred.ToString();
      preds_by_column_.insert(std::make_pair(idx, pred));

      // Since we'll evaluate this predicate ourselves, remove it from the scan spec
      // so higher layers don't repeat our work.
      iter = preds->erase(iter);
    }
  }

  // Determine a materialization order such that columns with predicates
  // are materialized first.
  //
  // TODO: we can be a little smarter about this, by trying to estimate
  // predicate selectivity, involve the materialization cost of types, etc.
  vector<size_t> with_preds, without_preds;

  for (size_t i = 0; i < schema().num_columns(); i++) {
    int num_preds = preds_by_column_.count(i);
    if (num_preds > 0) {
      with_preds.push_back(i);
    } else {
      without_preds.push_back(i);
    }
  }

  materialization_order_.swap(with_preds);
  materialization_order_.insert(materialization_order_.end(),
                                without_preds.begin(), without_preds.end());
  DCHECK_EQ(materialization_order_.size(), schema().num_columns());

  return Status::OK();
}

bool MaterializingIterator::HasNext() const {
  return iter_->HasNext();
}

Status MaterializingIterator::PrepareBatch(size_t *nrows) {
  RETURN_NOT_OK( iter_->PrepareBatch(nrows) );
  prepared_count_ = *nrows;
  return Status::OK();
}

Status MaterializingIterator::MaterializeBlock(RowBlock *dst) {
  DCHECK_EQ(dst->nrows(), prepared_count_);
  DCHECK_EQ(dst->selection_vector()->nrows(), prepared_count_);

  bool short_circuit = false;

  BOOST_FOREACH(size_t col_idx, materialization_order_) {
    // Materialize the column itself into the row block.
    ColumnBlock dst_col(dst->column_block(col_idx));
    RETURN_NOT_OK(iter_->MaterializeColumn(col_idx, &dst_col));

    // Evaluate any predicates that apply to this column.
    typedef std::pair<size_t, ColumnRangePredicate> MapEntry;
    BOOST_FOREACH(const MapEntry &entry, preds_by_column_.equal_range(col_idx)) {
      const ColumnRangePredicate &pred = entry.second;

      pred.Evaluate(dst, dst->selection_vector());

      // If after evaluating this predicate, the entire row block has now been
      // filtered out, we don't need to materialize other columns at all.
      if (!dst->selection_vector()->AnySelected()) {
        short_circuit = true;
        break;
      }
    }
    if (short_circuit) {
      break;
    }
  }
  DVLOG(1) << dst->selection_vector()->CountSelected() << "/" << prepared_count_ << " passed predicate";
  return Status::OK();
}

Status MaterializingIterator::FinishBatch() {
  return iter_->FinishBatch();
}

string MaterializingIterator::ToString() const {
  string s;
  s.append("Materializing(").append(iter_->ToString()).append(")");
  return s;
}

////////////////////////////////////////////////////////////
// PredicateEvaluatingIterator
////////////////////////////////////////////////////////////


PredicateEvaluatingIterator::PredicateEvaluatingIterator(
  const shared_ptr<RowwiseIterator> &base_iter) :
  base_iter_(base_iter)
{
}

Status PredicateEvaluatingIterator::InitAndMaybeWrap(
  shared_ptr<RowwiseIterator> *base_iter, ScanSpec *spec)
{
  RETURN_NOT_OK((*base_iter)->Init(spec));
  if (spec != NULL &&
      !spec->predicates().empty()) {
    // Underlying iterator did not accept all predicates. Wrap it.
    shared_ptr<RowwiseIterator> wrapper(
      new PredicateEvaluatingIterator(*base_iter));
    CHECK_OK(wrapper->Init(spec));
    base_iter->swap(wrapper);
  }
  return Status::OK();
}

Status PredicateEvaluatingIterator::Init(ScanSpec *spec) {
  // base_iter_ already Init()ed before this is constructed.

  CHECK_NOTNULL(spec);
  // Gather any predicates that the base iterator did not pushdown.
  // This also clears the predicates from the spec.
  predicates_.swap(*(spec->mutable_predicates()));
  return Status::OK();
}

bool PredicateEvaluatingIterator::HasNext() const {
  return base_iter_->HasNext();
}

Status PredicateEvaluatingIterator::PrepareBatch(size_t *nrows) {
  RETURN_NOT_OK( base_iter_->PrepareBatch(nrows) );
  return Status::OK();
}

Status PredicateEvaluatingIterator::MaterializeBlock(RowBlock *dst) {
  RETURN_NOT_OK(base_iter_->MaterializeBlock(dst));

  BOOST_FOREACH(ColumnRangePredicate &pred, predicates_) {
    pred.Evaluate(dst, dst->selection_vector());

    // If after evaluating this predicate, the entire row block has now been
    // filtered out, we don't need to evaluate any further predicates.
    if (!dst->selection_vector()->AnySelected()) {
      break;
    }
  }

  return Status::OK();
}

Status PredicateEvaluatingIterator::FinishBatch() {
  return base_iter_->FinishBatch();
}

string PredicateEvaluatingIterator::ToString() const {
  string s;
  s.append("PredicateEvaluating(").append(base_iter_->ToString()).append(")");
  return s;
}


} // namespace kudu
