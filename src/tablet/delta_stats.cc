// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#include "tablet/delta_stats.h"

#include "util/bitmap.h"

namespace kudu {

using std::vector;

namespace tablet {

DeltaStats::DeltaStats(size_t ncols)
    : delete_count_(0) {
  Resize(ncols);
}

void DeltaStats::Resize(size_t ncols) {
  update_counts_.resize(ncols, 0);
}

void DeltaStats::IncrUpdateCount(size_t col_idx, int64_t update_count) {
  update_counts_[col_idx] += update_count;
}

void DeltaStats::IncrDeleteCount(int64_t delete_count) {
  delete_count_ += delete_count;
}

Status DeltaStats::UpdateStats(const Schema& schema, const RowChangeList& update) {
  // We'd like to maintain per column statistics of updates and deletes.
  // Problem is that with updates, the column ids are encoded in the RowChangeList
  // itself. In the long term, we should use bitmaps in RowChangeList to represent
  // the columns as opposed to the existing [(id, change)] format -- this will be
  // substantial change and useful elsewhere in the code. However, for now we're
  // using the hacky approach of decoding the changelist and extracting the column ids.
  RowChangeListDecoder update_decoder(schema, update);
  RETURN_NOT_OK(update_decoder.Init());
  if (PREDICT_FALSE(update_decoder.is_delete())) {
    IncrDeleteCount(1);
  } else if (PREDICT_TRUE(update_decoder.is_update())) {
    // VLAs aren't officially part of any C++ standard, but they're supported by
    // both gcc and clang.
    size_t bitmap_size = BitmapSize(schema.num_columns());
    uint8_t bitmap[bitmap_size];
    memset(bitmap, 0, bitmap_size);
    RETURN_NOT_OK(update_decoder.GetIncludedColumns(bitmap));
    for (TrueBitIterator iter(bitmap, schema.num_columns());
         !iter.done();
         ++iter) {
      size_t col_idx = *iter;
      IncrUpdateCount(col_idx, 1);
    }
  } // Don't handle re-inserts
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
