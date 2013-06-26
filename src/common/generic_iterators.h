// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_MERGE_ITERATOR_H
#define KUDU_COMMON_MERGE_ITERATOR_H

#include <gtest/gtest.h>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <deque>
#include <string>
#include <vector>

#include "common/iterator.h"
#include "common/scan_spec.h"
#include "util/object_pool.h"

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
  Status InitSubIterators(ScanSpec *spec);

  const Schema schema_;

  bool initted_;

  vector<shared_ptr<MergeIterState> > iters_;

  // When the underlying iterators are initialized, each needs its own
  // copy of the scan spec in order to do its own pushdown calculations, etc.
  // The copies are allocated from this pool so they can be automatically freed
  // when the UnionIterator goes out of scope.
  ObjectPool<ScanSpec> scan_spec_copies_;


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
  // The passed-in iterators should not yet be initialized.
  //
  // All passed-in iterators must be fully able to evaluate all predicates - i.e.
  // calling iter->Init(spec) should remove all predicates from the spec.
  explicit UnionIterator(const vector<shared_ptr<RowwiseIterator> > &iters);

  Status Init(ScanSpec *spec);

  Status PrepareBatch(size_t *nrows);
  Status MaterializeBlock(RowBlock *dst);
  Status FinishBatch();

  bool HasNext() const;

  string ToString() const;

  const Schema &schema() const {
    CHECK(schema_.get() != NULL) << "Bad schema in " << ToString();
    return *CHECK_NOTNULL(schema_.get());
  }

 private:
  Status InitSubIterators(ScanSpec *spec);

  // Schema: initialized during Init()
  gscoped_ptr<Schema> schema_;
  bool initted_;
  deque<shared_ptr<RowwiseIterator> > iters_;

  // When the underlying iterators are initialized, each needs its own
  // copy of the scan spec in order to do its own pushdown calculations, etc.
  // The copies are allocated from this pool so they can be automatically freed
  // when the UnionIterator goes out of scope.
  ObjectPool<ScanSpec> scan_spec_copies_;
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
  FRIEND_TEST(TestMaterializingIterator, TestPredicatePushdown);
  FRIEND_TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluation);

  shared_ptr<ColumnwiseIterator> iter_;
  size_t prepared_count_;

  unordered_multimap<size_t, ColumnRangePredicate> preds_by_column_;

  // The order in which the columns will be materialized.
  vector<size_t> materialization_order_;

  // Set only by test code to disallow pushdown.
  bool disallow_pushdown_for_tests_;
};


// An iterator which wraps another iterator and evaluates any predicates that the
// wrapped iterator did not itself handle during push down.
class PredicateEvaluatingIterator : public RowwiseIterator {
 public:
  // Initialize the given '*base_iter' with the given 'spec'.
  //
  // If the base_iter accepts all predicates, then simply returns.
  // Otherwise, swaps out *base_iter for a PredicateEvaluatingIterator which wraps
  // the original iterator and accepts all predicates on its behalf.
  //
  // POSTCONDITION: spec->predicates().empty()
  // POSTCONDITION: base_iter and its wrapper are initialized
  static Status InitAndMaybeWrap(shared_ptr<RowwiseIterator> *base_iter,
                                 ScanSpec *spec);

  // Initialize the iterator.
  // POSTCONDITION: spec->predicates().empty()
  Status Init(ScanSpec *spec);

  Status PrepareBatch(size_t *nrows);
  Status MaterializeBlock(RowBlock *dst);
  Status FinishBatch();

  bool HasNext() const;

  string ToString() const;

  const Schema &schema() const {
    return base_iter_->schema();
  }

 private:
  // Construct the evaluating iterator.
  // This is only called from ::InitAndMaybeWrap()
  // REQUIRES: base_iter is already Init()ed.
  explicit PredicateEvaluatingIterator(const shared_ptr<RowwiseIterator> &base_iter);


  FRIEND_TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluation);

  shared_ptr<RowwiseIterator> base_iter_;
  vector<ColumnRangePredicate> predicates_;
};

} // namespace kudu
#endif
