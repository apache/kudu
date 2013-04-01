// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_MERGE_ITERATOR_H
#define KUDU_COMMON_MERGE_ITERATOR_H

#include "common/iterator.h"
#include "common/scan_spec.h"

#include <gtest/gtest.h>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <deque>
#include <vector>

namespace kudu {

class Arena;
class MergeIterState;

using std::deque;
using std::tr1::shared_ptr;
using std::vector;
using std::tr1::unordered_multimap;

// An iterator which merges the results of other iterators, comparing
// based on keys.
class MergeIterator : public RowwiseIterator {
public:

  // TODO: clarify whether schema is just the projection, or must include the merge
  // key columns. It should probably just be the required projection, which must be
  // a subset of the columns in 'iters'.
  MergeIterator(const Schema &schema,
                const vector<shared_ptr<RowwiseIterator> > &iters);

  // The passed-in iterators should be already initialized.
  Status Init(ScanSpec *spec);

  virtual Status PrepareBatch(size_t *nrows);

  virtual Status MaterializeBlock(RowBlock *dst);

  virtual Status FinishBatch();

  virtual bool HasNext() const {
    return !iters_.empty();
  }

  virtual string ToString() const;

  virtual const Schema &schema() const { return schema_; }

private:
  const Schema schema_;

  bool initted_;

  vector<shared_ptr<MergeIterState> > iters_;

  size_t prepared_count_;
};


// An iterator which unions the results of other iterators.
// This is different from MergeIterator in that it lays the results out end-to-end
// rather than merging them based on keys. Hence it is more efficient since there is
// no comparison needed, and the key column does not need to be read if it is not
// part of the projection.
class UnionIterator : public RowwiseIterator {
public:

  // Construct a union iterator of the given iterators.
  // The iterators must have matching schemas.
  // The passed-in iterators should be already initialized.
  UnionIterator(const vector<shared_ptr<RowwiseIterator> > &iters);

  Status Init(ScanSpec *spec);

  Status PrepareBatch(size_t *nrows);
  Status MaterializeBlock(RowBlock *dst);
  Status FinishBatch();

  bool HasNext() const;

  string ToString() const;

  const Schema &schema() const {
    return *CHECK_NOTNULL(schema_.get());
  }

private:
  // Schema: initialized during Init()
  gscoped_ptr<Schema> schema_;
  bool initted_;
  deque<shared_ptr<RowwiseIterator> > iters_;
};

// An iterator which wraps a ColumnwiseIterator, materializing it into full rows.
//
// Predicates which only apply to a single column are pushed down into this iterator.
// While materializing a block, columns with associated predicates are materialized
// first, and the predicates evaluated. If the predicates succeed in filtering out
// an entire batch, then other columns may avoid doing any IO.
class MaterializingIterator : public RowwiseIterator {
public:
  explicit MaterializingIterator(const shared_ptr<ColumnwiseIterator> &iter);

  // Initialize the iterator, performing predicate pushdown as described above.
  Status Init(ScanSpec *spec);

  Status PrepareBatch(size_t *nrows);
  Status MaterializeBlock(RowBlock *dst);
  Status FinishBatch();

  bool HasNext() const;

  string ToString() const;

  const Schema &schema() const {
    return iter_->schema();
  }

private:
  FRIEND_TEST(TestMaterialization, TestPredicatePushdown);

  shared_ptr<ColumnwiseIterator> iter_;
  size_t prepared_count_;

  unordered_multimap<size_t, ColumnRangePredicate> preds_by_column_;

  // The order in which the columns will be materialized.
  vector<size_t> materialization_order_;
};


} // namespace kudu
#endif
