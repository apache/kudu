// Copyright (c) 2012, Cloudera, inc.

#include "common/schema.h"
#include "tablet/rowdelta.h"
#include "util/bitmap.h"
#include "util/coding-inl.h"
#include "util/memory/arena.h"
#include "util/slice.h"

namespace kudu { namespace tablet {

RowDelta::RowDelta(const Schema &schema,
                   uint8_t *data) :
  data_(data) {
  DCHECK(data != NULL);
}

RowDelta RowDelta::CopyToArena(const Schema &schema, Arena *arena) const {
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

void RowDelta::UpdateColumn(const Schema &schema,
                            size_t col_idx,
                            const void *new_val) {
  DCHECK_LT(col_idx, schema.num_columns());
  memcpy(col_ptr(schema, col_idx), new_val,
         schema.column(col_idx).type_info().size());
  BitmapSet(bitmap(), col_idx);
}

bool RowDelta::IsUpdated(size_t col_idx) const {
  return BitmapTest(bitmap(), col_idx);
}

bool RowDelta::ApplyColumnUpdate(const Schema &schema,
                                 size_t col_idx,
                                 void *dst) const {
  DCHECK_LT(col_idx, schema.num_columns());

  if (IsUpdated(col_idx)) {
    size_t bm_size = BitmapSize(schema.num_columns());
    size_t off = schema.column_offset(col_idx);
    memcpy(dst, &data_[bm_size + off],
           schema.column(col_idx).type_info().size());
    return true;
  }
  return false;
}

void RowDelta::ApplyRowUpdate(const Schema &schema,
                              void *dst_v,
                              Arena *dst_arena) const {
  uint8_t *dst = reinterpret_cast<uint8_t *>(dst_v);

  // Iterate over the valid columns, copying any STRING data into
  // the target arena.
  for (TrueBitIterator it(bitmap(), schema.num_columns());
       !it.done();
       ++it) {
    size_t i = *it;
    size_t off = schema.column_offset(i);

    if (schema.column(i).type_info().type() == STRING) {
      // If it's a Slice column, need to relocate the referred-to data
      // as well as the slice itself.
      // TODO: potential optimization here: if the new value is smaller than
      // the old value, we could potentially just overwrite.
      const Slice *src = reinterpret_cast<const Slice *>(col_ptr(schema, i));
      Slice *dst_slice = reinterpret_cast<Slice *>(dst + off);
      CHECK(dst_arena->RelocateSlice(*src, dst_slice))
        << "Unable to relocate slice " << src->ToString()
        << " (col " << i << " in schema " << schema.ToString() << ")";
    } else {
      size_t size = schema.column(i).type_info().size();
      memcpy(&dst[off], col_ptr(schema, i), size);
    }
  }

}

void RowDelta::MergeUpdatesFrom(const Schema &schema,
                                const RowDelta &from,
                                Arena *arena) {
  // Copy the data from the other row, where the other row
  // has its bitfield set.
  from.ApplyRowUpdate(schema, col_ptr(schema, 0), arena);

  // Merge the set of updated fields
  BitmapMergeOr(bitmap(), from.bitmap(), schema.num_columns());
}

void RowDelta::SerializeToBuffer(
  const Schema &schema, faststring *dst) const {

  // First append the bitmap.
  dst->append(bitmap(), BitmapSize(schema.num_columns()));

  // Iterate over the valid columns, copying their data into the buffer.
  for (TrueBitIterator it(bitmap(), schema.num_columns());
       !it.done();
       ++it) {
    size_t i = *it;
    const TypeInfo &ti = schema.column(i).type_info();
    if (ti.type() == STRING) {
      const Slice *src = reinterpret_cast<const Slice *>(col_ptr(schema, i));

      // If it's a Slice column, copy the length followed by the data.
      InlinePutVarint32(dst, src->size());
      dst->append(src->data(), src->size());
    } else {
      // Otherwise, just copy the data itself.
      dst->append(col_ptr(schema, i), ti.size());
    }
  }
}

} // namespace tablet
} // namespace kudu
