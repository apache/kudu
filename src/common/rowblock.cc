// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <glog/logging.h>
#include "common/rowblock.h"
#include "util/bitmap.h"

namespace kudu {

SelectionVector::SelectionVector(size_t row_capacity) :
  bytes_capacity_(BitmapSize(row_capacity)),
  n_rows_(row_capacity),
  n_bytes_(bytes_capacity_),
  bitmap_(new uint8_t[n_bytes_])
{
  CHECK_GT(n_bytes_, 0);

  SetAllTrue();
}

void SelectionVector::Resize(size_t n_rows) {
  size_t new_bytes = BitmapSize(n_rows);
  CHECK_LE(new_bytes, bytes_capacity_);
  n_rows_ = n_rows;
  n_bytes_ = new_bytes;
}

size_t SelectionVector::CountSelected() const {
  return Bits::Count(&bitmap_[0], n_bytes_);
}

bool SelectionVector::AnySelected() const {
  size_t rem = n_bytes_;
  const uint32_t *p32 = reinterpret_cast<const uint32_t *>(
    &bitmap_[0]);
  while (rem >= 4) {
    if (*p32 != 0) {
      return true;
    }
    p32++;
    rem -= 4;
  }

  const uint8_t *p8 = reinterpret_cast<const uint8_t *>(p32);
  while (rem > 0) {
    if (*p8 != 0) {
      return true;
    }
    p8++;
    rem--;
  }

  return false;
}

//////////////////////////////
// RowBlock
//////////////////////////////

RowBlock::RowBlock(const Schema &schema,
                   size_t nrows,
                   Arena *arena) :
  schema_(schema),
  data_(new uint8_t[nrows * schema.byte_size()]),
  row_capacity_(nrows),
  nrows_(nrows),
  arena_(arena),
  sel_vec_(nrows)
{
  CHECK_GT(row_capacity_, 0);
}


void RowBlock::Resize(size_t new_size) {
  CHECK_LE(new_size, row_capacity_);
  nrows_ = new_size;
  sel_vec_.Resize(new_size);
}

}
