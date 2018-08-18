// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "kudu/common/rowblock.h"

#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/util/bitmap.h"

namespace kudu {

SelectionVector::SelectionVector(size_t row_capacity)
  : bytes_capacity_(BitmapSize(row_capacity)),
    n_rows_(row_capacity),
    n_bytes_(bytes_capacity_),
    bitmap_(new uint8_t[n_bytes_]) {
  CHECK_GT(n_bytes_, 0);
}

void SelectionVector::Resize(size_t n_rows) {
  size_t new_bytes = BitmapSize(n_rows);
  CHECK_LE(new_bytes, bytes_capacity_);
  n_rows_ = n_rows;
  n_bytes_ = new_bytes;
  // Pad with zeroes up to the next byte in order to give CountSelected()
  // and AnySelected() the assumption that the size is an even byte
  size_t bits_in_last_byte = n_rows & 7;
  if (bits_in_last_byte > 0) {
    BitmapChangeBits(&bitmap_[0], n_rows_, 8 - bits_in_last_byte, 0);
  }
}

void SelectionVector::ClearToSelectAtMost(size_t max_rows) {
  if (max_rows < n_rows_) {
    BitmapIterator iter(&bitmap_[0], n_rows_);
    bool selected;
    size_t run_size;
    size_t end_idx = 0;
    // Adjust the end index until we have selected 'max_rows' rows.
    while ((run_size = iter.Next(&selected)) && max_rows > 0) {
      if (selected) {
        if (run_size >= max_rows) {
          end_idx += max_rows;
          break;
        }
        max_rows -= run_size;
      }
      end_idx += run_size;
    }
    // If the limit is reached, zero out the rest of the selection vector.
    if (n_rows_ > end_idx) {
      BitmapChangeBits(&bitmap_[0], end_idx, n_rows_ - end_idx, false);
    }
  }
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

bool operator==(const SelectionVector& a, const SelectionVector& b) {
  if (a.nrows() != b.nrows()) {
    return false;
  }
  return BitmapEquals(a.bitmap(), b.bitmap(), a.nrows());
}

bool operator!=(const SelectionVector& a, const SelectionVector& b) {
  return !(a == b);
}

//////////////////////////////
// RowBlock
//////////////////////////////
RowBlock::RowBlock(const Schema &schema,
                   size_t nrows,
                   Arena *arena)
  : schema_(schema),
    columns_data_(schema.num_columns()),
    column_null_bitmaps_(schema.num_columns()),
    row_capacity_(nrows),
    nrows_(nrows),
    arena_(arena),
    sel_vec_(nrows) {
  CHECK_GT(row_capacity_, 0);

  size_t bitmap_size = BitmapSize(row_capacity_);
  for (size_t i = 0; i < schema.num_columns(); ++i) {
    const ColumnSchema& col_schema = schema.column(i);
    size_t col_size = row_capacity_ * col_schema.type_info()->size();
    columns_data_[i] = new uint8_t[col_size];

    if (col_schema.is_nullable()) {
      column_null_bitmaps_[i] = new uint8_t[bitmap_size];
    }
  }
}

RowBlock::~RowBlock() {
  for (uint8_t *column_data : columns_data_) {
    delete[] column_data;
  }
  for (uint8_t *bitmap_data : column_null_bitmaps_) {
    delete[] bitmap_data;
  }
}

void RowBlock::Resize(size_t new_size) {
  CHECK_LE(new_size, row_capacity_);
  nrows_ = new_size;
  sel_vec_.Resize(new_size);
}

} // namespace kudu
