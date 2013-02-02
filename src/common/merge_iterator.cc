// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <string>

#include "common/merge_iterator.h"
#include "common/row.h"
#include "common/rowblock.h"
#include "util/memory/arena.h"
#include <boost/scoped_array.hpp>

namespace kudu {

using std::string;
using std::tr1::shared_ptr;

// TODO: size by bytes, not # rows
static const int kMergeRowBuffer = 1000;

class MergeIterState {
public:
  MergeIterState(const shared_ptr<RowIteratorInterface> &iter) :
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
      next_row_ptr_ += iter_->schema().byte_size();

      return Status::OK();
    }
  }

  bool IsBlockExhausted() const {
    return cur_row_ == valid_rows_;
  }

  bool IsFullyExhausted() const {
    return IsBlockExhausted() && !iter_->HasNext();
  }

  Status PullNextBlock() {
    CHECK_EQ(cur_row_, valid_rows_)
      << "should not pull next block until current block is exhausted";
    if (IsFullyExhausted()) {
      return Status::OK();
    }

    size_t n = read_block_.nrows();
    RETURN_NOT_OK( iter_->CopyNextRows(&n, &read_block_) );
    cur_row_ = 0;
    valid_rows_ = n;
    next_row_ptr_ = read_block_.row_ptr(0);
    return Status::OK();
  }

  shared_ptr<RowIteratorInterface> iter_;
  Arena arena_;
  ScopedRowBlock read_block_;
  uint8_t *next_row_ptr_;
  size_t cur_row_;
  size_t valid_rows_;

};


MergeIterator::MergeIterator(
  const Schema &schema,
  const vector<shared_ptr<RowIteratorInterface> > &iters)
  : schema_(schema),
    initted_(false)
{
  CHECK_GT(iters.size(), 0);
  BOOST_FOREACH(const shared_ptr<RowIteratorInterface> &iter, iters) {
    iters_.push_back(shared_ptr<MergeIterState>(new MergeIterState(iter)));
  }
}


Status MergeIterator::Init() {
  CHECK(!initted_);

  // TODO: check that schemas match up!

  BOOST_FOREACH(shared_ptr<MergeIterState> &state, iters_) {
    RETURN_NOT_OK(state->iter_->Init());
    RETURN_NOT_OK(state->iter_->SeekToStart());
    RETURN_NOT_OK(state->PullNextBlock());
  }

  initted_ = true;
  return Status::OK();
}

Status MergeIterator::SeekAtOrAfter(const Slice &key, bool *exact) {
  return Status::NotSupported("Merge iterator doesn't currently support seek");
}

// TODO: this is an obvious spot to add codegen - there's a ton of branching
// and such around the comparisons. A simple experiment indicated there's some
// 2x to be gained.
Status MergeIterator::CopyNextRows(size_t *nrows, RowBlock *dst) {
  CHECK(initted_);

  // TODO: check that dst has the same schema


  *nrows = 0;
  size_t dst_row_idx = 0;
  size_t row_size = schema_.byte_size();
  char *dst_ptr = reinterpret_cast<char *>(dst->row_ptr(0));


  while (dst_row_idx < dst->nrows()) {

    // Find the sub-iterator which is currently smallest
    MergeIterState *smallest = NULL;

    for (size_t i = 0; i < iters_.size(); i++) {
      shared_ptr<MergeIterState> &state = iters_[i];

      if (PREDICT_FALSE(state->IsFullyExhausted())) {
        iters_.erase(iters_.begin() + i);
        i--;
        continue;
      }

      if (smallest == NULL ||
          schema_.Compare(state->next_row_ptr(), smallest->next_row_ptr()) < 0) {
        smallest = state.get();
      }
    }

    // If no iterators had any row left, then we're done iterating.
    if (PREDICT_FALSE(smallest == NULL)) break;

    // Otherwise, copy the row from the smallest one, and advance it
    strings::memcpy_inlined(dst_ptr,
                            reinterpret_cast<const char *>(smallest->next_row_ptr()),
                            row_size);
    if (dst->arena() != NULL) {
      RETURN_NOT_OK(kudu::CopyRowIndirectDataToArena(dst_ptr, schema_, dst->arena()));
    }

    dst_ptr += row_size;

    RETURN_NOT_OK(smallest->Advance());
    dst_row_idx++;
  }

  *nrows = dst_row_idx;
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



} // namespace kudu
