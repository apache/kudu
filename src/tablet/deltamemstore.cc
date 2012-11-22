// Copyright (c) 2012, Cloudera, inc.

#include <utility>

#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "util/bitmap.h"
#include "util/coding-inl.h"
#include "util/status.h"

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
  DMSMap::iterator it = map_.lower_bound(row_idx);
  // returns the first index >= row_idx

  if ((it != map_.end()) &&
      ((*it).first == row_idx)) {
    // Found an exact match, just merge the update in, no need to copy the
    // update into arena
    (*it).second.MergeUpdatesFrom(schema_, update, &arena_);
    return;
  } else if (PREDICT_TRUE(it != map_.begin())) {
    // Rewind the iterator to the previous entry, to act as an insertion
    // hint
    --it;
  }

  // Otherwise, copy the update into the arena.
  RowDelta copied = update.CopyToArena(schema_, &arena_);


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

Status DeltaMemStore::FlushToFile(DeltaFileWriter *dfw) const {
  BOOST_FOREACH(DMSMap::value_type entry, map_) {
    dfw->AppendDelta(entry.first, entry.second);
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////
// RowDelta implementation
////////////////////////////////////////////////////////////


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

void RowDelta::MergeUpdatesFrom(const Schema &schema,
                                const RowDelta &from,
                                Arena *arena) {
  size_t bm_size = BitmapSize(schema.num_columns());

  // Copy the data from the other row, where the other row
  // has its bitfield set.
  
  // Iterate over the valid columns, copying any STRING data into
  // the target arena.
  for (TrueBitIterator it(from.bitmap(), schema.num_columns());
       !it.done();
       ++it) {
    size_t i = *it;
    if (schema.column(i).type_info().type() == STRING) {
      // If it's a Slice column, need to relocate the referred-to data
      // as well as the slice itself.
      // TODO: potential optimization here: if the new value is smaller than
      // the old value, we could potentially just overwrite.
      const Slice *src = reinterpret_cast<const Slice *>(from.col_ptr(schema, i));
      Slice *dst = reinterpret_cast<Slice *>(col_ptr(schema, i));
      CHECK(arena->RelocateSlice(*src, dst))
        << "Unable to relocate slice " << src->ToString()
        << " (col " << i << " in schema " << schema.ToString() << ")";
    } else {
      size_t off = schema.column_offset(i) + bm_size;
      size_t size = schema.column(i).type_info().size();
      memcpy(&data_[off], &from.data_[off], size);
    }
  }

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
