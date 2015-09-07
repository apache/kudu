// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>

#include <algorithm>
#include <string>
#include <utility>

#include "kudu/common/generic_iterators.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/memory/arena.h"

DEFINE_bool(materializing_iterator_do_pushdown, true,
            "Should MaterializingIterator do predicate pushdown");
TAG_FLAG(materializing_iterator_do_pushdown, hidden);

namespace kudu {

using std::string;
using std::tr1::shared_ptr;

////////////////////////////////////////////////////////////
// Merge iterator
////////////////////////////////////////////////////////////

// TODO: size by bytes, not # rows
static const int kMergeRowBuffer = 1000;

// MergeIterState wraps a RowwiseIterator for use by the MergeIterator.
// Importantly, it also filters out unselected rows from the wrapped RowwiseIterator,
// such that all returned rows are valid.
class MergeIterState {
 public:
  explicit MergeIterState(const shared_ptr<RowwiseIterator> &iter) :
    iter_(iter),
    arena_(1024, 256*1024),
    read_block_(iter->schema(), kMergeRowBuffer, &arena_),
    next_row_idx_(0),
    num_advanced_(0),
    num_valid_(0)
  {}

  const RowBlockRow& next_row() {
    DCHECK_LT(num_advanced_, num_valid_);
    return next_row_;
  }

  Status Advance() {
    num_advanced_++;
    if (IsBlockExhausted()) {
      arena_.Reset();
      return PullNextBlock();
    } else {
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
  }

  bool IsBlockExhausted() const {
    return num_advanced_ == num_valid_;
  }

  bool IsFullyExhausted() const {
    return num_valid_ == 0;
  }

  Status PullNextBlock() {
    CHECK_EQ(num_advanced_, num_valid_)
      << "should not pull next block until current block is exhausted";

    if (!iter_->HasNext()) {
      // Fully exhausted
      num_advanced_ = 0;
      num_valid_ = 0;
      return Status::OK();
    }

    RETURN_NOT_OK(iter_->NextBlock(&read_block_));
    num_advanced_ = 0;
    // Honor the selection vector of the read_block_, since not all rows are necessarily selected.
    SelectionVector *selection = read_block_.selection_vector();
    DCHECK_EQ(selection->nrows(), read_block_.nrows());
    DCHECK_LE(selection->CountSelected(), read_block_.nrows());
    num_valid_ = selection->CountSelected();
    VLOG(2) << selection->CountSelected() << "/" << read_block_.nrows() << " rows selected";
    // Seek next_row_ to the first selected row.
    for (next_row_idx_ = 0; next_row_idx_ < read_block_.nrows(); next_row_idx_++) {
      if (selection->IsRowSelected(next_row_idx_)) {
        next_row_.Reset(&read_block_, next_row_idx_);
        break;
      }
    }
    DCHECK_NE(next_row_idx_, read_block_.nrows()+1) << "No selected rows found!";
    return Status::OK();
  }

  size_t remaining_in_block() const {
    return num_valid_ - num_advanced_;
  }

  const shared_ptr<RowwiseIterator>& iter() const {
    return iter_;
  }

  shared_ptr<RowwiseIterator> iter_;
  Arena arena_;
  RowBlock read_block_;
  // The row currently pointed to by the iterator.
  RowBlockRow next_row_;
  // Row index of next_row_ in read_block_.
  size_t next_row_idx_;
  // Number of rows we've advanced past in the current RowBlock.
  size_t num_advanced_;
  // Number of valid (selected) rows in the current RowBlock.
  size_t num_valid_;
};


MergeIterator::MergeIterator(
  const Schema &schema,
  const vector<shared_ptr<RowwiseIterator> > &iters)
  : schema_(schema),
    initted_(false) {
  CHECK_GT(iters.size(), 0);
  CHECK_GT(schema.num_key_columns(), 0);
  orig_iters_.assign(iters.begin(), iters.end());
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

bool MergeIterator::HasNext() const {
  CHECK(initted_);
  return !iters_.empty();
}

Status MergeIterator::InitSubIterators(ScanSpec *spec) {
  // Initialize all the sub iterators.
  BOOST_FOREACH(shared_ptr<RowwiseIterator> &iter, orig_iters_) {
    ScanSpec *spec_copy = spec != NULL ? scan_spec_copies_.Construct(*spec) : NULL;
    RETURN_NOT_OK(PredicateEvaluatingIterator::InitAndMaybeWrap(&iter, spec_copy));
    iters_.push_back(shared_ptr<MergeIterState>(new MergeIterState(iter)));
  }

  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != NULL) {
    spec->mutable_predicates()->clear();
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
  BOOST_FOREACH(shared_ptr<MergeIterState> &iter, iters_) {
    available += iter->remaining_in_block();
  }

  dst->Resize(std::min(dst->row_capacity(), available));
}

// TODO: this is an obvious spot to add codegen - there's a ton of branching
// and such around the comparisons. A simple experiment indicated there's some
// 2x to be gained.
Status MergeIterator::MaterializeBlock(RowBlock *dst) {
  // Initialize the selection vector.
  // MergeIterState only returns selected rows.
  dst->selection_vector()->SetAllTrue();
  for (size_t dst_row_idx = 0; dst_row_idx < dst->nrows(); dst_row_idx++) {
    RowBlockRow dst_row = dst->row(dst_row_idx);

    // Find the sub-iterator which is currently smallest
    MergeIterState *smallest = NULL;
    ssize_t smallest_idx = -1;

    // Typically the number of iters_ is not that large, so using a priority
    // queue is not worth it
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
    RETURN_NOT_OK(CopyRow(smallest->next_row(), &dst_row, dst->arena()));
    RETURN_NOT_OK(smallest->Advance());

    if (smallest->IsFullyExhausted()) {
      iters_.erase(iters_.begin() + smallest_idx);
    }
  }

  return Status::OK();
}

string MergeIterator::ToString() const {
  string s;
  s.append("Merge(");
  bool first = true;
  BOOST_FOREACH(const shared_ptr<RowwiseIterator> &iter, orig_iters_) {
    s.append(iter->ToString());
    if (!first) {
      s.append(", ");
    }
    first = false;
  }
  s.append(")");
  return s;
}

const Schema& MergeIterator::schema() const {
  CHECK(initted_);
  return schema_;
}

void MergeIterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  CHECK(initted_);
  vector<vector<IteratorStats> > stats_by_iter;
  BOOST_FOREACH(const shared_ptr<RowwiseIterator>& iter, orig_iters_) {
    vector<IteratorStats> stats_for_iter;
    iter->GetIteratorStats(&stats_for_iter);
    stats_by_iter.push_back(stats_for_iter);
  }
  for (size_t idx = 0; idx < schema_.num_columns(); ++idx) {
    IteratorStats stats_for_col;
    BOOST_FOREACH(const vector<IteratorStats>& stats_for_iter, stats_by_iter) {
      stats_for_col.AddStats(stats_for_iter[idx]);
    }
    stats->push_back(stats_for_col);
  }
}


////////////////////////////////////////////////////////////
// Union iterator
////////////////////////////////////////////////////////////

UnionIterator::UnionIterator(const vector<shared_ptr<RowwiseIterator> > &iters)
  : initted_(false),
    iters_(iters.size()) {
  CHECK_GT(iters.size(), 0);
  iters_.assign(iters.begin(), iters.end());
  all_iters_.assign(iters.begin(), iters.end());
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
  CHECK(initted_);
  BOOST_FOREACH(const shared_ptr<RowwiseIterator> &iter, iters_) {
    if (iter->HasNext()) return true;
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
    iters_.pop_front();
  }
}

Status UnionIterator::MaterializeBlock(RowBlock *dst) {
  return iters_.front()->NextBlock(dst);
}

void UnionIterator::FinishBatch() {
  if (!iters_.front()->HasNext()) {
    // Iterator exhausted, remove it.
    iters_.pop_front();
  }
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

void UnionIterator::GetIteratorStats(std::vector<IteratorStats>* stats) const {
  CHECK(initted_);
  vector<vector<IteratorStats> > stats_by_iter;
  BOOST_FOREACH(const shared_ptr<RowwiseIterator>& iter, all_iters_) {
    vector<IteratorStats> stats_for_iter;
    iter->GetIteratorStats(&stats_for_iter);
    stats_by_iter.push_back(stats_for_iter);
  }
  for (size_t idx = 0; idx < schema_->num_columns(); ++idx) {
    IteratorStats stats_for_col;
    BOOST_FOREACH(const vector<IteratorStats>& stats_for_iter, stats_by_iter) {
      stats_for_col.AddStats(stats_for_iter[idx]);
    }
    stats->push_back(stats_for_col);
  }
}

////////////////////////////////////////////////////////////
// Materializing iterator
////////////////////////////////////////////////////////////

MaterializingIterator::MaterializingIterator(const shared_ptr<ColumnwiseIterator> &iter)
  : iter_(iter),
    disallow_pushdown_for_tests_(!FLAGS_materializing_iterator_do_pushdown) {
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


PredicateEvaluatingIterator::PredicateEvaluatingIterator(
  const shared_ptr<RowwiseIterator> &base_iter) :
  base_iter_(base_iter) {
}

Status PredicateEvaluatingIterator::InitAndMaybeWrap(
  shared_ptr<RowwiseIterator> *base_iter, ScanSpec *spec) {
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

Status PredicateEvaluatingIterator::NextBlock(RowBlock *dst) {
  RETURN_NOT_OK(base_iter_->NextBlock(dst));

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

string PredicateEvaluatingIterator::ToString() const {
  string s;
  s.append("PredicateEvaluating(").append(base_iter_->ToString()).append(")");
  return s;
}


} // namespace kudu
