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
#ifndef KUDU_COMMON_MERGE_ITERATOR_H
#define KUDU_COMMON_MERGE_ITERATOR_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/util/locks.h"
#include "kudu/util/object_pool.h"
#include "kudu/util/status.h"

namespace kudu {

class MergeIterState;
class RowBlock;

// An iterator which merges the results of other iterators, comparing
// based on keys.
class MergeIterator : public RowwiseIterator {
 public:
  // TODO: clarify whether schema is just the projection, or must include the merge
  // key columns. It should probably just be the required projection, which must be
  // a subset of the columns in 'iters'.
  MergeIterator(const Schema& schema,
                std::vector<std::shared_ptr<RowwiseIterator>> iters);
  virtual ~MergeIterator();

  // The passed-in iterators should be already initialized.
  Status Init(ScanSpec *spec) OVERRIDE;

  virtual bool HasNext() const OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

  virtual const Schema& schema() const OVERRIDE;

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE;

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  void PrepareBatch(RowBlock* dst);
  Status MaterializeBlock(RowBlock* dst);
  Status InitSubIterators(ScanSpec *spec);

  const Schema schema_;

  bool initted_;

  // Holds the subiterators until Init is called, at which point this is cleared.
  // This is required because we can't create a MergeIterState of an uninitialized iterator.
  std::vector<std::shared_ptr<RowwiseIterator>> orig_iters_;

  // See UnionIterator::iters_lock_ for details on locking. This follows the same
  // pattern.
  mutable rw_spinlock iters_lock_;
  std::vector<std::unique_ptr<MergeIterState>> iters_;

  // Statistics (keyed by projection column index) accumulated so far by any
  // fully-consumed sub-iterators.
  std::vector<IteratorStats> finished_iter_stats_by_col_;

  // The number of iterators, used by ToString().
  const int num_orig_iters_;

  // When the underlying iterators are initialized, each needs its own
  // copy of the scan spec in order to do its own pushdown calculations, etc.
  // The copies are allocated from this pool so they can be automatically freed
  // when the UnionIterator goes out of scope.
  ObjectPool<ScanSpec> scan_spec_copies_;
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
  explicit UnionIterator(std::vector<std::shared_ptr<RowwiseIterator>> iters);

  Status Init(ScanSpec *spec) OVERRIDE;

  bool HasNext() const OVERRIDE;

  std::string ToString() const OVERRIDE;

  const Schema &schema() const OVERRIDE {
    CHECK(initted_);
    CHECK(schema_.get() != NULL) << "Bad schema in " << ToString();
    return *CHECK_NOTNULL(schema_.get());
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE;

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  void PrepareBatch();
  Status MaterializeBlock(RowBlock* dst);
  void FinishBatch();
  Status InitSubIterators(ScanSpec *spec);

  // Pop the front iterator from iters_ and accumulate its statistics into
  // finished_iter_stats_by_col_.
  void PopFront();

  // Schema: initialized during Init()
  gscoped_ptr<Schema> schema_;
  bool initted_;

  // Lock protecting 'iters_' and 'finished_iter_stats_by_col_'.
  //
  // Scanners are mostly accessed by the thread doing the scanning, but the HTTP endpoint
  // which lists running scans may occasionally need to read as well.
  //
  // The "owner" thread of the scanner doesn't need to acquire this in read mode, since
  // it's the only thread which might write. However, it does need to acquire in write
  // mode when changing 'iters_'.
  mutable rw_spinlock iters_lock_;
  std::deque<std::shared_ptr<RowwiseIterator>> iters_;

  // Statistics (keyed by projection column index) accumulated so far by any
  // fully-consumed sub-iterators.
  std::vector<IteratorStats> finished_iter_stats_by_col_;

  // When the underlying iterators are initialized, each needs its own
  // copy of the scan spec in order to do its own pushdown calculations, etc.
  // The copies are allocated from this pool so they can be automatically freed
  // when the UnionIterator goes out of scope.
  ObjectPool<ScanSpec> scan_spec_copies_;
};

// An iterator which wraps a ColumnwiseIterator, materializing it into full rows.
//
// Column predicates are pushed down into this iterator. While materializing a
// block, columns with associated predicates are materialized first, and the
// predicates evaluated. If the predicates succeed in filtering out an entire
// batch, then other columns may avoid doing any IO.
class MaterializingIterator : public RowwiseIterator {
 public:
  explicit MaterializingIterator(std::shared_ptr<ColumnwiseIterator> iter);

  // Initialize the iterator, performing predicate pushdown as described above.
  Status Init(ScanSpec *spec) OVERRIDE;

  bool HasNext() const OVERRIDE;

  std::string ToString() const OVERRIDE;

  const Schema &schema() const OVERRIDE {
    return iter_->schema();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    iter_->GetIteratorStats(stats);
  }

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  FRIEND_TEST(TestMaterializingIterator, TestPredicatePushdown);
  FRIEND_TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluation);

  Status MaterializeBlock(RowBlock *dst);

  std::shared_ptr<ColumnwiseIterator> iter_;

  // List of (column index, predicate) in order of most to least selective, with
  // ties broken by the index.
  std::vector<std::pair<int32_t, ColumnPredicate>> col_idx_predicates_;

  // List of column indexes without predicates to materialize.
  std::vector<int32_t> non_predicate_column_indexes_;

  // Set only by test code to disallow pushdown.
  bool disallow_pushdown_for_tests_;
  bool disallow_decoder_eval_;
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
  static Status InitAndMaybeWrap(std::shared_ptr<RowwiseIterator> *base_iter,
                                 ScanSpec *spec);

  // Initialize the iterator.
  // POSTCONDITION: spec->predicates().empty()
  Status Init(ScanSpec *spec) OVERRIDE;

  virtual Status NextBlock(RowBlock *dst) OVERRIDE;

  bool HasNext() const OVERRIDE;

  std::string ToString() const OVERRIDE;

  const Schema &schema() const OVERRIDE {
    return base_iter_->schema();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    base_iter_->GetIteratorStats(stats);
  }

 private:

  // Construct the evaluating iterator.
  // This is only called from ::InitAndMaybeWrap()
  // REQUIRES: base_iter is already Init()ed.
  explicit PredicateEvaluatingIterator(std::shared_ptr<RowwiseIterator> base_iter);

  FRIEND_TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluation);
  FRIEND_TEST(TestPredicateEvaluatingIterator, TestPredicateEvaluationOrder);

  std::shared_ptr<RowwiseIterator> base_iter_;

  // List of predicates in order of most to least selective, with
  // ties broken by the column index.
  std::vector<ColumnPredicate> col_predicates_;
};

} // namespace kudu
#endif
