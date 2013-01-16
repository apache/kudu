// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_INTERFACES_H
#define KUDU_TABLET_LAYER_INTERFACES_H

#include "common/iterator.h"
#include "common/row.h"
#include "util/status.h"

namespace kudu {

class ColumnBlock;

namespace tablet {

class LayerInterface {
public:
  // Check if a given row key is present in this layer.
  // Sets *present and returns Status::OK, unless an error
  // occurs.
  virtual Status CheckRowPresent(const void *key, bool *present) const = 0;

  // Update a row in this layer.
  //
  // If the row does not exist in this layer, returns
  // Status::NotFound().
  virtual Status UpdateRow(const void *key,
                           const RowDelta &update) = 0;

  // Return a new RowIterator for this layer, with the given projection.
  // NB: the returned iterator is not yet Initted.
  // TODO: make this consistent with above.
  virtual RowIteratorInterface *NewRowIterator(const Schema &projection) const = 0;

  // Count the number of rows in this layer.
  virtual Status CountRows(size_t *count) const = 0;

  // Return a displayable string for this layer.
  virtual string ToString() const = 0;


  virtual ~LayerInterface() {}
};

class DeltaTrackerInterface {
public:

  // Apply updates for a given column to a batch of rows.
  // TODO: would be better to take in a projection schema here, maybe?
  // Or provide functions for each (column-wise scanning vs early materialization?)
  //
  // The target buffer 'dst' is assumed to have a length at least
  // as large as row_stride * nrows.
  virtual Status ApplyUpdates(size_t col_idx, uint32_t start_row,
                              ColumnBlock *dst) const = 0;

  virtual ~DeltaTrackerInterface() {}
};


} // namespace tablet
} // namespace kudu

#endif
