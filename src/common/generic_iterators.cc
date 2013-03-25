// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <string>

#include "common/generic_iterators.h"
#include "common/row.h"
#include "common/rowblock.h"
#include "gutil/gscoped_ptr.h"
#include "util/memory/arena.h"

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

  const void *next_row_ptr() {
    DCHECK_LT(cur_row_, valid_rows_);
    return next_row_ptr_;
  }

  Status Advance() {
    cur_row_++;
    if (IsBlockExhausted()) {
      arena_.Reset();
      return PullNextBlock();
    } else {
      // Manually advancing next_row_ptr_ is some 20% faster
      // than calling row_ptr(cur_row_), since it avoids an expensive multiplication
      // in the inner loop.
      next_row_ptr_ += read_block_.schema().byte_size();

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

    size_t n = read_block_.nrows();
    RETURN_NOT_OK(RowwiseIterator::CopyBlock(iter_.get(), &n, &read_block_));
    cur_row_ = 0;
    valid_rows_ = n;
    next_row_ptr_ = read_block_.row_ptr(0);
    return Status::OK();
  }

  size_t remaining_in_block() const {
    return valid_rows_ - cur_row_;
  }

  shared_ptr<RowwiseIterator> iter_;
  Arena arena_;
  ScopedRowBlock read_block_;
  uint8_t *next_row_ptr_;
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


Status MergeIterator::Init() {
  CHECK(!initted_);

  // TODO: check that schemas match up!

  BOOST_FOREACH(shared_ptr<MergeIterState> &state, iters_) {
    RETURN_NOT_OK(state->iter_->Init());
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

  size_t dst_row_idx = 0;
  size_t row_size = schema_.byte_size();
  uint8_t *dst_ptr = dst->row_ptr(0);

  while (dst_row_idx < prepared_count_) {

    // Find the sub-iterator which is currently smallest
    MergeIterState *smallest = NULL;
    size_t smallest_idx;

    for (size_t i = 0; i < iters_.size(); i++) {
      shared_ptr<MergeIterState> &state = iters_[i];

      if (smallest == NULL ||
          schema_.Compare(state->next_row_ptr(), smallest->next_row_ptr()) < 0) {
        smallest = state.get();
        smallest_idx = i;
      }
    }

    // If no iterators had any row left, then we're done iterating.
    if (PREDICT_FALSE(smallest == NULL)) break;

    // Otherwise, copy the row from the smallest one, and advance it
    strings::memcpy_inlined(dst_ptr, smallest->next_row_ptr(), row_size);
    if (dst->arena() != NULL) {
      RETURN_NOT_OK(kudu::CopyRowIndirectDataToArena(dst_ptr, schema_, dst->arena()));
    }

    dst_ptr += row_size;

    RETURN_NOT_OK(smallest->Advance());

    if (smallest->IsFullyExhausted()) {
      iters_.erase(iters_.begin() + smallest_idx);
    }

    dst_row_idx++;
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

Status UnionIterator::Init() {
  CHECK(!initted_);

  // Verify schemas match.
  schema_.reset(new Schema(iters_.front()->schema()));
  BOOST_FOREACH(shared_ptr<RowwiseIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->Init());
    if (!iter->schema().Equals(*schema_)) {
      return Status::InvalidArgument(
        string("Schemas do not match: ") + schema_->ToString()
        + " vs " + iter->schema().ToString());
    }
  }
  initted_ = true;
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
  iter_(iter)
{
}

Status MaterializingIterator::Init() {
  return iter_->Init();
}

bool MaterializingIterator::HasNext() const {
  return iter_->HasNext();
}

Status MaterializingIterator::PrepareBatch(size_t *nrows) {
  return iter_->PrepareBatch(nrows);
}

Status MaterializingIterator::MaterializeBlock(RowBlock *dst) {
  for (size_t i = 0; i < schema().num_columns(); i++) {
    ColumnBlock dst_col(dst->column_block(i));
    RETURN_NOT_OK(iter_->MaterializeColumn(i, &dst_col));
  }
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

} // namespace kudu
