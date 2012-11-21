// Copyright (c) 2012, Cloudera, inc.

#include <utility>

#include "tablet/deltamemstore.h"
#include "util/bitmap.h"

namespace kudu { namespace tablet {

////////////////////////////////////////////////////////////
// DeltaMemStore implementation
////////////////////////////////////////////////////////////

DeltaMemStore::DeltaMemStore(const Schema &schema) :
  schema_(schema),
  arena_(8*1024, 1*1024*1024)
{
}


void DeltaMemStore::Update(uint32_t row_idx,
                           const RowDelta &update) {
  // TODO: copy any STRING type data into local arena

  DMSMap::iterator it = map_.lower_bound(row_idx);
  // returns the first index >= row_idx

  if ((it != map_.end()) &&
      ((*it).first == row_idx)) {
    // Found an exact match, just merge the update in, no need to copy the
    // update into arena
    (*it).second.MergeUpdatesFrom(schema_, update);
    return;
  } else if (PREDICT_TRUE(it != map_.begin())) {
    // Rewind the iterator to the previous entry, to act as an insertion
    // hint
    --it;
  }

  // Otherwise, copy the update into the arena.
  RowDelta copied = update.CopyToArena(schema_, arena_);


  DMSMap::value_type pair(row_idx, copied);
  it = map_.insert(it, pair);
}


void DeltaMemStore::ApplyUpdates(
  size_t col_idx, uint32_t start_row,
  ColumnBlock *dst) const
{
  DCHECK_EQ(schema_.column(col_idx).type_info().type(),
            dst->type_info().type());

  DMSMap::const_iterator it = map_.lower_bound(start_row);
  while (it != map_.end() &&
         (*it).first < start_row + dst->size()) {
    uint32_t rel_idx = (*it).first - start_row;
    (*it).second.ApplyColumnUpdate(schema_, col_idx,
                                   dst->cell_ptr(rel_idx));
    ++it;
  }
}

////////////////////////////////////////////////////////////
// RowDelta implementation
////////////////////////////////////////////////////////////


RowDelta::RowDelta(const Schema &schema,
                   uint8_t *data) :
  data_(data) {
  DCHECK(data != NULL);
}

RowDelta RowDelta::CopyToArena(const Schema &schema, Arena &arena) const {
  void *copied_data = arena.AddBytes(data_, SizeForSchema(schema));
  CHECK(copied_data) << "failed to allocate";

  return RowDelta(schema, reinterpret_cast<uint8_t *>(copied_data));
}


void RowDelta::UpdateColumn(const Schema &schema,
                            size_t col_idx,
                            const void *new_val) {
  DCHECK_LT(col_idx, schema.num_columns());
  size_t bm_size = BitmapSize(schema.num_columns());
  size_t off = schema.column_offset(col_idx);
  memcpy(&data_[off + bm_size], new_val,
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

void RowDelta::MergeUpdatesFrom(const Schema &schema,
                                const RowDelta &from) {
  size_t bm_size = BitmapSize(schema.num_columns());

  // Copy the data from the other row, where the other row
  // has its bitfield set.
  for (size_t i = 0; i < schema.num_columns(); i++) {
    if (from.IsUpdated(i)) {
      size_t off = schema.column_offset(i) + bm_size;
      size_t size = schema.column(i).type_info().size();
      memcpy(&data_[off], &from.data_[off], size);
    }
  }

  // Merge the set of updated fields
  BitmapMergeOr(bitmap(), from.bitmap(), schema.num_columns());
}


} // namespace tablet
} // namespace kudu
