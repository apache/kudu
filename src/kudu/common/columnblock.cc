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

#include "kudu/common/columnblock.h"

#include <cstring>

#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"

namespace kudu {

Status ColumnBlock::CopyTo(const SelectionVector& sel_vec,
                           ColumnBlock* dst, size_t src_cell_off,
                           size_t dst_cell_off, size_t num_cells) const {
  DCHECK_EQ(type_, dst->type_);
  DCHECK_EQ(is_nullable(), dst->is_nullable());
  DCHECK_GE(nrows_, src_cell_off + num_cells);
  DCHECK_GE(dst->nrows_, dst_cell_off + num_cells);

  // Columns with indirect data need to be copied cell-by-cell in order to
  // perform arena relocation. Deselected cells must be skipped; the source
  // content could be garbage so it'd be unsafe to access it as indirect data.
  if (type_->physical_type() == BINARY) {
    for (size_t cell_idx = 0; cell_idx < num_cells; cell_idx++) {
      if (sel_vec.IsRowSelected(src_cell_off + cell_idx)) {
        Cell s(cell(src_cell_off + cell_idx));
        Cell d(dst->cell(dst_cell_off + cell_idx));
        RETURN_NOT_OK(CopyCell(s, &d, dst->arena())); // Also copies nullability.
      }
    }
  } else {
    memcpy(dst->data_ + (dst_cell_off * type_->size()),
           data_ + (src_cell_off * type_->size()),
           num_cells * type_->size());
    if (null_bitmap_) {
      BitmapCopy(dst->null_bitmap_, dst_cell_off,
                 null_bitmap_, src_cell_off,
                 num_cells);
  }
}

  return Status::OK();
}

} // namespace kudu
