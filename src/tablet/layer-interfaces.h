// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_INTERFACES_H
#define KUDU_TABLET_LAYER_INTERFACES_H

namespace kudu { namespace tablet {

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
  
};


} // namespace tablet
} // namespace kudu

#endif
