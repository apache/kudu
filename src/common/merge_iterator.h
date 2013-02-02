// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_MERGE_ITERATOR_H
#define KUDU_COMMON_MERGE_ITERATOR_H

#include "common/iterator.h"

#include <tr1/memory>
#include <vector>

namespace kudu {

class Arena;
class MergeIterState;

using std::tr1::shared_ptr;
using std::vector;


// An iterator which merges the results of other iterators, comparing
// based on keys.
class MergeIterator : public RowIteratorInterface {
public:
  MergeIterator(const Schema &schema,
                const vector<shared_ptr<RowIteratorInterface> > &iters);

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

} // namespace kudu
#endif
