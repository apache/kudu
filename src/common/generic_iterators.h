// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_MERGE_ITERATOR_H
#define KUDU_COMMON_MERGE_ITERATOR_H

#include "common/iterator.h"

#include <tr1/memory>
#include <deque>
#include <vector>

namespace kudu {

class Arena;
class MergeIterState;

using std::deque;
using std::tr1::shared_ptr;
using std::vector;


// An iterator which merges the results of other iterators, comparing
// based on keys.
class MergeIterator : public RowIteratorInterface {
public:

  // TODO: clarify whether schema is just the projection, or must include the merge
  // key columns. It should probably just be the required projection, which must be
  // a subset of the columns in 'iters'.
  MergeIterator(const Schema &schema,
                const vector<shared_ptr<RowIteratorInterface> > &iters);

  // The passed-in iterators should be already initialized.
  Status Init();

  virtual Status SeekAtOrAfter(const Slice &key, bool *exact);

  virtual Status CopyNextRows(size_t *nrows, RowBlock *dst);

  virtual bool HasNext() const {
    return !iters_.empty();
  }

  virtual string ToString() const;

  virtual const Schema &schema() const { return schema_; }

private:
  const Schema schema_;

  bool initted_;

  vector<shared_ptr<MergeIterState> > iters_;
};


// An iterator which unions the results of other iterators.
// This is different from MergeIterator in that it lays the results out end-to-end
// rather than merging them based on keys. Hence it is more efficient since there is
// no comparison needed, and the key column does not need to be read if it is not
// part of the projection.
class UnionIterator : public RowIteratorInterface {
public:

  // Construct a union iterator of the given iterators.
  // The iterators must have matching schemas.
  // The passed-in iterators should be already initialized.
  UnionIterator(const vector<shared_ptr<RowIteratorInterface> > &iters);

  Status Init();

  Status SeekAtOrAfter(const Slice &key, bool *exact);

  Status CopyNextRows(size_t *nrows, RowBlock *dst);

  bool HasNext() const;

  string ToString() const;

  const Schema &schema() const {
    return *CHECK_NOTNULL(schema_.get());
  }

private:
  // Schema: initialized during Init()
  gscoped_ptr<Schema> schema_;
  bool initted_;
  deque<shared_ptr<RowIteratorInterface> > iters_;
};


} // namespace kudu
#endif
