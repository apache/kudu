// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAMEMSTORE_H
#define KUDU_TABLET_DELTAMEMSTORE_H

#include <boost/noncopyable.hpp>
#include <boost/scoped_array.hpp>
#include <map>

#include "tablet/schema.h"
#include "util/bitmap.h"
#include "util/memory/arena.h"

namespace kudu {
namespace tablet {

using std::map;

class RowDelta;

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
                    void *dst, size_t row_stride, size_t nrows) const;

  size_t Count() const {
    return map_.size();
  }

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


struct RowDelta {
public:
  RowDelta(const Schema &schema,
           uint8_t *data);

  // Return the number of bytes of storage needed for a row delta.
  static size_t SizeForSchema(const Schema &schema) {
    size_t bm_size = BitmapSize(schema.num_columns());
    return bm_size + schema.byte_size();
  }

  // Copy the delta data itself to a new arena, and return a new object
  // which references storage in the destination arena.
  // TODO: this doesn't copy Slices.
  RowDelta CopyToArena(const Schema &schema, Arena &arena) const;

  // Clear any updated columns
  void Clear(const Schema &schema) {
    memset(bitmap(), 0, BitmapSize(schema.num_columns()));
  }


  // Marks the given column as updated with the given new value.
  //
  // NOTE: this does _not_ copy the indirected data into the DMS's
  // arena. That is the responsibility of a higher layer (eg
  // DeltaMemStore::Update)
  void UpdateColumn(const Schema &schema, size_t col_idx,
                    const void *new_val);

  // Return true if the given column has been updated in this row delta.
  bool IsUpdated(size_t col_idx) const;

  // If this row delta contains an update for the given column, copy
  // the new value to 'dst'.
  //
  // NOTE: This does not copy any referenced data.
  // TODO: it should probably copy referenced data to an arena.
  bool ApplyColumnUpdate(const Schema &schema,
                         size_t col_idx,
                         void *dst) const;

  // Merge updates from another delta. The two RowDelta objects
  // must correspond to the same schema.
  // If 'from' has references to external data (eg slices) it is not
  // copied. TODO: need to deal with slice handling here.
  void MergeUpdatesFrom(const Schema &schema,
                        const RowDelta &from);

private:
  const uint8_t *bitmap() const { return data_; }
  uint8_t *bitmap() { return data_; }


  // This conceptually would have a reference back to an associated
  // DeltaMemStore or Schema, but in order to save space it is instead passed
  // to all of the call sites.

  // The underlying data. Format
  // [bitset of updated columns]  (width: 1 bit per column in the schema)
  // a row (with all columns present)
  uint8_t *data_;
};


// A RowDelta which manages its own dynamically-allocated
// storage, freeing it when it goes out of scope.
class ScopedRowDelta {
public:
  ScopedRowDelta(const Schema &schema) :
    data_(new uint8_t[RowDelta::SizeForSchema(schema)]),
    delta_(schema, data_.get()) {
    delta_.Clear(schema);
  }

  RowDelta &get() { return delta_; }
  const RowDelta &get() const { return delta_; }

private:
  scoped_array<uint8_t> data_;
  RowDelta delta_;
};



} // namespace tablet
} // namespace kudu

#endif
