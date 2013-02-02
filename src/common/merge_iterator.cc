// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <string>

#include "common/merge_iterator.h"
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
    read_block_(iter->schema(), kMergeRowBuffer, NULL),
    cur_row_(0),
    valid_rows_(0)
  {}

  const void *next_row_ptr() {
    return read_block_.row_ptr(cur_row_);
  }

  Status Advance() {
    cur_row_++;
    if (PREDICT_TRUE(cur_row_ < valid_rows_)) {
      return Status::OK();
    } else {
      return PullNextBlock();
    }
  }

  Status PullNextBlock() {
    CHECK_EQ(cur_row_, valid_rows_)
      << "should not pull next block until current block is used up";
    size_t n = read_block_.nrows();
    RETURN_NOT_OK( iter_->CopyNextRows(&n, &read_block_) );
    cur_row_ = 0;
    valid_rows_ = n;
    return Status::OK();
  }

  bool IsExhausted() const {
    if (PREDICT_FALSE(cur_row_ == valid_rows_)) {
      CHECK(!iter_->HasNext())
        << "Should not be considered exhausted if underlying iter has more "
        << "rows";
      return true;
    } else {
      return false;
    }
  }

  shared_ptr<RowIteratorInterface> iter_;
  ScopedRowBlock read_block_;
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
    RETURN_NOT_OK(state->PullNextBlock());
  }

  initted_ = true;
  return Status::OK();
}

Status MergeIterator::SeekAtOrAfter(const Slice &key, bool *exact) {
  return Status::NotSupported("Merge iterator doesn't currently support seek");
}

Status MergeIterator::CopyNextRows(size_t *nrows, RowBlock *dst) {
  CHECK(initted_);

  *nrows = 0;
  size_t dst_row_idx = 0;

  while (dst_row_idx < dst->nrows()) {

    // Find the sub-iterator which is currently smallest
    MergeIterState *smallest = NULL;

    for (size_t i = 0; i < iters_.size(); i++) {
      shared_ptr<MergeIterState> &state = iters_[i];

      if (state->IsExhausted()) {
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
    strings::memcpy_inlined(reinterpret_cast<char *>(dst->row_ptr(dst_row_idx)),
                            reinterpret_cast<const char *>(smallest->next_row_ptr()),
                            schema_.byte_size());
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
