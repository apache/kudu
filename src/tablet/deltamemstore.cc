// Copyright (c) 2012, Cloudera, inc.

#include <utility>

#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
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



} // namespace tablet
} // namespace kudu
