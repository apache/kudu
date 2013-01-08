// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_ITERATOR_H
#define KUDU_COMMON_ITERATOR_H

#include <string>

#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class Arena;

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
  // Retrieves up to 'nrows' rows, and writes back the number
  // of rows actually fetched into the same variable.
  // Any indirect data (eg strings) are allocated out of
  // 'dst_arena'
  //
  // TODO: this probably needs something like 'class RowBlock' to correspond to
  // class ColumnBlock
  virtual Status CopyNextRows(size_t *nrows,
                              uint8_t *dst,
                              Arena *dst_arena) = 0;

  virtual bool HasNext() const = 0;

  virtual string ToString() const = 0;

  virtual ~RowIteratorInterface() {}
};

} // namespace kudu
#endif
