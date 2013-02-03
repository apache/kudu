// Copyright (c) 2012, Cloudera, inc.

#include "common/schema.h"
#include "tablet/rowdelta.h"
#include "util/bitmap.h"
#include "util/coding-inl.h"
#include "util/memory/arena.h"
#include "util/slice.h"

namespace kudu { namespace tablet {

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
