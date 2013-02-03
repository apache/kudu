// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_ROWDELTA_H
#define KUDU_TABLET_ROWDELTA_H

#include <boost/scoped_array.hpp>

#include "common/schema.h"
#include "util/faststring.h"
#include "util/bitmap.h"

namespace kudu {

class Arena;

namespace tablet {

struct RowDelta {
public:
  RowDelta(const Schema &schema,
           uint8_t *data)  :
    data_(data) {
    DCHECK(data != NULL);
  }

  // Return the number of bytes of storage needed for a row delta.
  static size_t SizeForSchema(const Schema &schema) {
    size_t bm_size = BitmapSize(schema.num_columns());
    return bm_size + schema.byte_size();
  }

  // Copy the delta data itself to a new arena, and return a new object
  // which references storage in the destination arena.
  //
  // This copies both the updated row itself as well as any updated
  // STRING data.
  template<class ArenaType>
  RowDelta CopyToArena(const Schema &schema, ArenaType *arena) const;

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
  //
  // Return true if any updates were made.
  bool ApplyColumnUpdate(const Schema &schema,
                         size_t col_idx,
                         void *dst) const;

  // Apply all updated columns to the given row.
  // dst must contain an entire row's worth of data.
  //
  // Updated slices are copied into the dst arena
  template<class ArenaType>
  void ApplyRowUpdate(const Schema &schema,
                      void *dst,
                      ArenaType *dst_arena) const;

  // Merge updates from another delta. The two RowDelta objects
  // must correspond to the same schema.
  // If 'from' has references to external data (eg slices), then that
  // data is copied into the provided destination arena.
  template<class ArenaType>
  void MergeUpdatesFrom(const Schema &schema,
                        const RowDelta &from,
                        ArenaType *arena);

  // Serialize the delta into a compact form in the destination buffer.
  // The result is entirely self-contained, suitable for storing
  // on disk (ie no pointers).
  //
  // The format here is simply the same bitmap stored in-memory in this
  // class (1-bit set for any updated column), followed by the data for
  // only the updated columns.
  //
  // In the case of Slice columns, the data is represented as
  // <vint32 length> <data>
  // Other columns are copied exactly from their in-memory form.
  void SerializeToBuffer(const Schema &schema,
                         faststring *dst) const;

private:
  const uint8_t *bitmap() const { return data_; }
  uint8_t *bitmap() { return data_; }

  uint8_t *col_ptr(const Schema &schema, size_t idx) {
    size_t bm_size = BitmapSize(schema.num_columns());
    size_t off = schema.column_offset(idx);
    return &data_[bm_size + off];
  }

  const uint8_t *col_ptr(const Schema &schema, size_t idx) const {
    return const_cast<RowDelta *>(this)->col_ptr(schema, idx);
  }

  // This conceptually would have a reference back to an associated
  // DeltaMemStore or Schema, but in order to save space it is instead passed
  // to all of the call sites.

  // The underlying data. Format
  // [bitset of updated columns]  (width: 1 bit per column in the schema)
  // a row (with all columns present)
  //
  // TODO: Given that CBTree stores raw slices, now, do we ever persist these
  // things? Can probably simplify this class significantly.
  uint8_t *data_;
};


// A RowDelta which manages its own dynamically-allocated
// storage, freeing it when it goes out of scope.
class ScopedRowDelta {
public:
  ScopedRowDelta(const Schema &schema) :
    data_(new uint8_t[RowDelta::SizeForSchema(schema)]),
    delta_(schema, data_.get()) {
#ifndef NDEBUG
    OverwriteWithPattern(reinterpret_cast<char *>(&data_[0]),
                         RowDelta::SizeForSchema(schema),
                         "NEW");
#endif
    delta_.Clear(schema);
  }

  RowDelta &get() { return delta_; }
  const RowDelta &get() const { return delta_; }

private:
  scoped_array<uint8_t> data_;
  RowDelta delta_;
};

template<class ArenaType>
inline RowDelta RowDelta::CopyToArena(const Schema &schema, ArenaType *arena) const {
  void *copied_data = arena->AddBytes(data_, SizeForSchema(schema));
  CHECK(copied_data) << "failed to allocate";

  RowDelta ret(schema, reinterpret_cast<uint8_t *>(copied_data));

  // Iterate over the valid columns, copying any STRING data into
  // the target arena.
  for (TrueBitIterator it(ret.bitmap(), schema.num_columns());
       !it.done();
       ++it) {
    int i = *it;
    if (schema.column(i).type_info().type() == STRING) {
      Slice *s = reinterpret_cast<Slice *>(ret.col_ptr(schema, i));
      CHECK(arena->RelocateSlice(*s, s))
        << "Unable to relocate slice " << s->ToString()
        << " (col " << i << " in schema " << schema.ToString() << ")";
    }
  }
  return ret;
}


template<class ArenaType>
inline void RowDelta::ApplyRowUpdate(
  const Schema &schema, void *dst_v, ArenaType *dst_arena) const
{
  uint8_t *dst = reinterpret_cast<uint8_t *>(dst_v);

  // Iterate over the valid columns, copying any STRING data into
  // the target arena.
  for (TrueBitIterator it(bitmap(), schema.num_columns());
       !it.done();
       ++it) {
    size_t i = *it;
    size_t off = schema.column_offset(i);

    CHECK_OK(schema.column(i).CopyCell(dst + off, col_ptr(schema, i), dst_arena));
  }

}

template<class ArenaType>
inline void RowDelta::MergeUpdatesFrom(
  const Schema &schema, const RowDelta &from, ArenaType *arena)
{
  // Copy the data from the other row, where the other row
  // has its bitfield set.
  from.ApplyRowUpdate(schema, col_ptr(schema, 0), arena);

  // Merge the set of updated fields
  BitmapMergeOr(bitmap(), from.bitmap(), schema.num_columns());
}


} // namespace tablet
} // namespace kudu

#endif
