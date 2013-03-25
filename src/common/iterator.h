// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_ITERATOR_H
#define KUDU_COMMON_ITERATOR_H

#include <string>

#include "common/columnblock.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class Arena;
class RowBlock;

class BatchedIterator {
public:
  virtual Status Init() = 0;

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
  virtual bool is_column_store() const { return false; }

  // Materialize all columns in the destination block.
  //
  // Any indirect data (eg strings) are copied into the destination block's
  // arena, if non-null.
  virtual Status MaterializeBlock(RowBlock *dst) = 0;

  // One-shot function to prepare, materialize, and copy a block of data
  // from 'iter' into the provided row block. *n is set to the number of rows
  // copied.
  static Status CopyBlock(RowwiseIterator *iter, size_t *n, RowBlock *dst);

};

class ColumnwiseIterator : public virtual BatchedIterator {
public:
  virtual bool is_column_store() const { return true; }

  // Materialize the given column into the given column block.
  // col_idx is within the projection schema, not the underlying schema.
  //
  // Any indirect data (eg strings) are copied into the destination block's
  // arena, if non-null.
  virtual Status MaterializeColumn(size_t col_idx, ColumnBlock *dst) = 0;
};


inline Status RowwiseIterator::CopyBlock(RowwiseIterator *iter, size_t *n, RowBlock *dst) {
  RETURN_NOT_OK(iter->PrepareBatch(n));
  RETURN_NOT_OK(iter->MaterializeBlock(dst));
  RETURN_NOT_OK(iter->FinishBatch());
  return Status::OK();
}


} // namespace kudu
#endif
