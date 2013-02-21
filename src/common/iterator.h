// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_ITERATOR_H
#define KUDU_COMMON_ITERATOR_H

#include <string>

#include "common/schema.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class Arena;
class RowBlock;

// Interface which various stores of data implement (eg tablet layers, memstores, etc).
//
// An iterator has an associated schema and can produce rows into a buffer which fits that
// schema.
class RowIteratorInterface {
public:
  virtual Status Init() = 0;

  // Seek to a given key in the underlying data.
  // Note that the 'key' must correspond to the key in the
  // Layer's schema, not the projection schema.
  virtual Status SeekAtOrAfter(const Slice &key, bool *exact) = 0;

  Status SeekToStart() {
    bool exact_unused;
    return SeekAtOrAfter(Slice(""), &exact_unused);
  }

  // Get the next batch of rows from the iterator.
  //
  // Retrieves up to *nrows rows into the given row block.
  // On return, if successfull, sets *nrows to the number of rows actually fetched.
  //
  // Any indirect data (eg strings) are copied into the destination row block's
  // arena, if non-null.
  virtual Status CopyNextRows(size_t *nrows, RowBlock *dst) = 0;

  // Return true if the next call to CopyNextRows will return at least one row.
  virtual bool HasNext() const = 0;

  // Return a string representation of this iterator, suitable for debug output.
  virtual string ToString() const = 0;

  // Return the schema for the rows which this iterator produces.
  virtual const Schema &schema() const = 0;

  virtual ~RowIteratorInterface() {}
};

} // namespace kudu
#endif
