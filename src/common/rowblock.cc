// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <glog/logging.h>
#include "common/rowblock.h"
#include "util/bitmap.h"

namespace kudu {

SelectionVector::SelectionVector(size_t n_rows) :
  n_rows_(n_rows),
  n_bytes_(BitmapSize(n_rows)),
  bitmap_(new uint8_t[n_bytes_])
{
  CHECK_GT(n_bytes_, 0);

  SetAllTrue();
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
  nrows_(nrows),
  arena_(arena),
  sel_vec_(nrows)
{}


}
