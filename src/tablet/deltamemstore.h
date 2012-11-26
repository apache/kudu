// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAMEMSTORE_H
#define KUDU_TABLET_DELTAMEMSTORE_H

#include <boost/noncopyable.hpp>
#include <map>

#include "common/columnblock.h"
#include "common/schema.h"
#include "tablet/rowdelta.h"
#include "util/memory/arena.h"

namespace kudu {
namespace tablet {

using std::map;

class DeltaFileWriter;

// In-memory storage for data which has been recently updated.
// This essentially tracks a 'diff' per row, which contains the
// modified columns.

class DeltaMemStore : boost::noncopyable {
public:
  explicit DeltaMemStore(const Schema &schema);

  // Update the given row in the database.
  // Copies the data, as well as any referenced
  // values into this DMS's local arena.
  void Update(uint32_t row_idx, const RowDelta &update);

  // Apply updates for a given column to a batch of rows.
  // TODO: would be better to take in a projection schema here, maybe?
  // Or provide functions for each (column-wise scanning vs early materialization?)
  //
  // The target buffer 'dst' is assumed to have a length at least
  // as large as row_stride * nrows.
  void ApplyUpdates(size_t col_idx, uint32_t start_row,
                    ColumnBlock *dst) const;

  size_t Count() const {
    return map_.size();
  }

  Status FlushToFile(DeltaFileWriter *dfw) const;

private:
  friend class RowDelta;

  const Schema &schema() const {
    return schema_;
  }

  const Schema schema_;

  typedef map<uint32_t, RowDelta> DMSMap;

  DMSMap map_;

  Arena arena_;
};



} // namespace tablet
} // namespace kudu

#endif
