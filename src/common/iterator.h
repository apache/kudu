// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_ITERATOR_H
#define KUDU_COMMON_ITERATOR_H

#include <string>
#include <vector>

#include "common/columnblock.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "common/iterator_stats.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class Arena;
class RowBlock;
class ScanSpec;

class BatchedIterator {
 public:
  // Initialize the iterator with the given scan spec.
  //
  // The scan spec may be transformed by this call to remove predicates
  // which will be fully pushed down into the iterator.
  //
  // The scan spec pointer must remain valid for the lifetime of the
  // iterator -- the iterator does not take ownership of the object.
  //
  // This may be NULL if there are no predicates, etc.
  // TODO: passing NULL is just convenience for unit tests, etc.
  // Should probably simplify the API by not allowing NULL.
  virtual Status Init(ScanSpec *spec) = 0;

  // Return true if the underlying storage is a column store.
  //
  // If true, then this class also must implement RowStoreBaseDataInterface.
  // Otherwise, this class must implement ColumnStoreBaseDataInterface.
  virtual bool is_column_store() const = 0;

  // Prepare to read the next nrows from the underlying base data.
  // Sets *nrows back to the number of rows available to be read,
  // which may be less than the requested number in the case that the iterator
  // is at the end of the available data.
  virtual Status PrepareBatch(size_t *nrows) = 0;

  // Finish the current batch.
  virtual Status FinishBatch() = 0;

  // Return true if the next call to PrepareBatch is expected to return at least
  // one row.
  virtual bool HasNext() const = 0;

  // Return a string representation of this iterator, suitable for debug output.
  virtual string ToString() const = 0;

  // Return the schema for the rows which this iterator produces.
  virtual const Schema &schema() const = 0;

  virtual ~BatchedIterator() {}
};

class RowwiseIterator : public virtual BatchedIterator {
 public:
  virtual bool is_column_store() const OVERRIDE { return false; }

  // Materialize all columns in the destination block.
  //
  // Any indirect data (eg strings) are copied into the destination block's
  // arena, if non-null.
  //
  // The destination row block's selection vector is set to indicate whether
  // each row in the result has passed scan predicates and is still live in
  // the current MVCC snapshot. The iterator implementation should not assume
  // that the selection vector has been initialized prior to this call.
  virtual Status MaterializeBlock(RowBlock *dst) = 0;

  // One-shot function to prepare, materialize, and copy a block of data
  // from 'iter' into the provided row block. The 'dst' block is resized
  // to the number of rows successfully copied.
  //
  // It is not necessary to initialize the selection vector of the RowBlock
  // prior to using this call.
  static Status CopyBlock(RowwiseIterator *iter, RowBlock *dst);

  // Get IteratorStats for each column in the row, including
  // (potentially) columns that are iterated over but not projected;
  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const = 0;
};

class ColumnwiseIterator : public virtual BatchedIterator {
 public:
  virtual bool is_column_store() const OVERRIDE { return true; }

  // Initialize the given SelectionVector to indicate which rows in the currently
  // prepared batch are live vs deleted.
  //
  // The SelectionVector passed in is uninitialized -- i.e its bits are in
  // an undefined state and need to be explicitly set to 1 if the row is live.
  virtual Status InitializeSelectionVector(SelectionVector *sel_vec) = 0;

  // Materialize the given column into the given column block.
  // col_idx is within the projection schema, not the underlying schema.
  //
  // Any indirect data (eg strings) are copied into the destination block's
  // arena, if non-null.
  virtual Status MaterializeColumn(size_t col_idx, ColumnBlock *dst) = 0;

  // Get IteratorStats for each column.
  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const = 0;
};


inline Status RowwiseIterator::CopyBlock(RowwiseIterator *iter, RowBlock *dst) {
  size_t n = dst->row_capacity();
  if (dst->arena()) {
    dst->arena()->Reset();
  }
  RETURN_NOT_OK(iter->PrepareBatch(&n));
  dst->Resize(n);
  RETURN_NOT_OK(iter->MaterializeBlock(dst));
  RETURN_NOT_OK(iter->FinishBatch());
  return Status::OK();
}


} // namespace kudu
#endif
