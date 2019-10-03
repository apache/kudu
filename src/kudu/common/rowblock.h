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

#ifndef KUDU_COMMON_ROWBLOCK_H
#define KUDU_COMMON_ROWBLOCK_H

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/columnblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class RowBlockRow;

// Bit-vector representing the selection status of each row in a row block.
//
// When scanning through data, a 1 bit in the selection vector indicates that
// the given row is live and has passed all predicates.
class SelectionVector {
 public:
  // Construct a new vector. The bits are initially in an indeterminate state.
  // Call SetAllTrue() if you require all rows to be initially selected.
  explicit SelectionVector(size_t row_capacity);

  // Construct a vector which shares the underlying memory of another vector,
  // but only exposes up to a given prefix of the number of rows.
  //
  // Note that mutating the resulting bitmap may arbitrarily mutate the "extra"
  // bits of the 'other' bitmap.
  //
  // The underlying bytes must not be deallocated or else this object will become
  // invalid.
  SelectionVector(SelectionVector *other, size_t prefix_rows);

  // Resize the selection vector to the given number of rows.
  // This size must be <= the allocated capacity.
  //
  // Ensures that all rows for indices < n_rows are unmodified.
  void Resize(size_t n_rows);

  // Zeroes out the end of the selection vector such that it will be left with
  // at most 'max_rows' selected.
  //
  // If 'max_rows' is greater than the allocated capacity, this does nothing.
  void ClearToSelectAtMost(size_t max_rows);

  // Return the number of selected rows.
  size_t CountSelected() const;

  // Return true if any rows are selected, false if size 0.
  // This is equivalent to (CountSelected() > 0), but faster.
  bool AnySelected() const;

  bool IsRowSelected(size_t row) const {
    DCHECK_LT(row, n_rows_);
    return BitmapTest(&bitmap_[0], row);
  }

  void SetRowSelected(size_t row) {
    DCHECK_LT(row, n_rows_);
    BitmapSet(&bitmap_[0], row);
  }

  void SetRowUnselected(size_t row) {
    DCHECK_LT(row, n_rows_);
    BitmapClear(&bitmap_[0], row);
  }

  // Finds the first selected row beginning at 'row_offset'.
  //
  // Returns true if at least one row is selected and writes its index to 'row';
  // returns false otherwise.
  bool FindFirstRowSelected(size_t row_offset, size_t* row) {
    DCHECK_LT(row_offset, n_rows_);
    DCHECK(row);
    return BitmapFindFirstSet(&bitmap_[0], row_offset, n_rows_, row);
  }

  // Sets '*selected' to the indices of all rows marked as selected
  // in this selection vector.
  void GetSelectedRows(std::vector<int>* selected) const;

  uint8_t *mutable_bitmap() {
    return &bitmap_[0];
  }

  const uint8_t *bitmap() const {
    return &bitmap_[0];
  }

  // Set all bits in the bitmap to 1
  void SetAllTrue() {
    memset(&bitmap_[0], 0xff, n_bytes_);
    PadExtraBitsWithZeroes();
  }

  // Set all bits in the bitmap to 0
  void SetAllFalse() {
    memset(&bitmap_[0], 0, n_bytes_);
  }

  size_t nrows() const { return n_rows_; }

  // Copies a range of bits between two SelectionVectors.
  //
  // The extent of the range is designated by 'src_row_off' and 'num_rows'. It
  // is copied to 'dst' at 'dst_row_off'.
  //
  // Note: 'dst' will be resized if the copy causes it to grow (though this is
  // just a "logical" resize; no reallocation takes place).
  void CopyTo(SelectionVector* dst, size_t src_row_off,
              size_t dst_row_off, size_t num_rows) const {
    DCHECK_GE(n_rows_, src_row_off + num_rows);

    size_t new_num_rows = dst_row_off + num_rows;
    if (new_num_rows > dst->nrows()) {
      // This will crash if 'dst' lacks adequate capacity.
      dst->Resize(new_num_rows);
    }

    BitmapCopy(dst->mutable_bitmap(), dst_row_off,
               bitmap_.get(), src_row_off, num_rows);
  }

  std::string ToString() const {
    return BitmapToString(&bitmap_[0], n_rows_);
  }

 private:

  // Pads any non-byte-aligned bits at the end of the SelectionVector with zeroes.
  //
  // To improve performance, CountSelected() and AnySelected() evaluate the
  // SelectionVector's bitmap in terms of bytes. As such, they consider all of
  // the trailing bits, even if the bitmap's bit length is not byte-aligned and
  // some trailing bits aren't part of the bitmap.
  //
  // To address this without sacrificing performance, we need to zero out all
  // trailing bits at construction time, or after any operation that sets all
  // bytes in bulk.
  void PadExtraBitsWithZeroes() {
    size_t bits_in_last_byte = n_rows_ & 7;
    if (bits_in_last_byte > 0) {
      BitmapChangeBits(&bitmap_[0], n_rows_, 8 - bits_in_last_byte, false);
    }
  }

  // The number of allocated bytes in bitmap_
  size_t bytes_capacity_;

  size_t n_rows_;
  size_t n_bytes_;

  gscoped_array<uint8_t> bitmap_;

  DISALLOW_COPY_AND_ASSIGN(SelectionVector);
};

bool operator==(const SelectionVector& a, const SelectionVector& b);
bool operator!=(const SelectionVector& a, const SelectionVector& b);

// A SelectionVectorView keeps track of where in the selection vector a given
// batch will start from. After processing a batch, Advance() should be called
// and the view will move forward by the appropriate amount. In this way, the
// underlying selection vector can easily be updated batch-by-batch.
class SelectionVectorView {
 public:
  // Constructs a new SelectionVectorView.
  //
  // The 'sel_vec' object must outlive this SelectionVectorView.
  explicit SelectionVectorView(SelectionVector *sel_vec)
    : sel_vec_(sel_vec), row_offset_(0) {
  }
  void Advance(size_t skip) {
    DCHECK_LE(skip, sel_vec_->nrows() - row_offset_);
    row_offset_ += skip;
  }
  void SetBit(size_t row_idx) {
    DCHECK_LE(row_idx, sel_vec_->nrows() - row_offset_);
    BitmapSet(sel_vec_->mutable_bitmap(), row_offset_ + row_idx);
  }
  void ClearBit(size_t row_idx) {
    DCHECK_LE(row_idx, sel_vec_->nrows() - row_offset_);
    BitmapClear(sel_vec_->mutable_bitmap(), row_offset_ + row_idx);
  }
  bool TestBit(size_t row_idx) {
    DCHECK_LE(row_idx, sel_vec_->nrows() - row_offset_);
    return BitmapTest(sel_vec_->bitmap(), row_offset_ + row_idx);
  }
  // Clear "nrows" bits from the supplied "offset" in the current view.
  void ClearBits(size_t nrows, size_t offset = 0) {
    DCHECK_LE(offset + nrows, sel_vec_->nrows() - row_offset_);
    BitmapChangeBits(sel_vec_->mutable_bitmap(), row_offset_ + offset, nrows, false);
  }
 private:
  SelectionVector* sel_vec_;
  size_t row_offset_;
};

// A block of decoded rows.
// Wrapper around a buffer, which keeps the buffer's size, associated arena,
// and schema. Provides convenience accessors for indexing by row, column, etc.
//
// NOTE: TODO: We don't have any separate class for ConstRowBlock and ConstColumnBlock
// vs RowBlock and ColumnBlock. So, we use "const" in various places to either
// mean const-ness of the wrapper structure vs const-ness of the referred-to data.
// Typically in C++ this is done with separate wrapper classes for const vs non-const
// referred-to data, but that would require a lot of duplication elsewhere in the code,
// so for now, we just use convention: if you have a RowBlock or ColumnBlock parameter
// that you expect to be modifying, use a "RowBlock *param". Otherwise, use a
// "const RowBlock& param". Just because you _could_ modify the referred-to contents
// of the latter doesn't mean you _should_.
class RowBlock {
 public:
  // Constructs a new RowBlock.
  //
  // The 'schema' and 'arena' objects must outlive this RowBlock.
  RowBlock(const Schema* schema,
           size_t nrows,
           Arena* arena);
  ~RowBlock();

  // Resize the block to the given number of rows.
  // This size must be <= the the allocated capacity row_capacity().
  //
  // Ensures that all rows for indices < n_rows are unmodified.
  void Resize(size_t n_rows);

  size_t row_capacity() const {
    return row_capacity_;
  }

  RowBlockRow row(size_t idx) const;

  const Schema* schema() const { return schema_; }
  Arena* arena() const { return arena_; }

  ColumnBlock column_block(size_t col_idx) const {
    return column_block(col_idx, nrows_);
  }

  ColumnBlock column_block(size_t col_idx, size_t nrows) const {
    DCHECK_LE(nrows, nrows_);

    const ColumnSchema& col_schema = schema_->column(col_idx);
    uint8_t* col_data = columns_data_[col_idx];
    uint8_t* nulls_bitmap = column_null_bitmaps_[col_idx];

    return ColumnBlock(col_schema.type_info(), nulls_bitmap, col_data, nrows, arena_);
  }

  // Return the base pointer for the given column's data.
  //
  // This is used by the codegen code in preference to the "nicer" column_block APIs,
  // because the codegenned code knows the column's type sizes statically, and thus
  // computing the pointers by itself ends up avoiding a multiply instruction.
  uint8_t* column_data_base_ptr(size_t col_idx) const {
    DCHECK_LT(col_idx, columns_data_.size());
    return columns_data_[col_idx];
  }

  // Return the number of rows in the row block. Note that this includes
  // rows which were filtered out by the selection vector.
  size_t nrows() const { return nrows_; }

  // Zero the memory pointed to by this row block.
  // This physically zeros the memory, so is not efficient - mostly useful
  // from unit tests.
  void ZeroMemory() {
    size_t bitmap_size = BitmapSize(row_capacity_);
    for (size_t i = 0; i < schema_->num_columns(); ++i) {
      const ColumnSchema& col_schema = schema_->column(i);
      size_t col_size = col_schema.type_info()->size() * row_capacity_;
      memset(columns_data_[i], '\0', col_size);

      if (column_null_bitmaps_[i] != NULL) {
        memset(column_null_bitmaps_[i], '\0', bitmap_size);
      }
    }
  }

  // Return the selection vector which indicates which rows have passed
  // predicates so far during evaluation of this block of rows.
  //
  // At the beginning of each batch, the vector is set to all 1s, and
  // as predicates or deletions make rows invalid, they are set to 0s.
  // After a batch has completed, only those rows with associated true
  // bits in the selection vector are valid results for the scan.
  SelectionVector* selection_vector() {
    return &sel_vec_;
  }

  const SelectionVector* selection_vector() const {
    return &sel_vec_;
  }

  // Copies a range of rows between two RowBlocks.
  //
  // The extent of the range is designated by 'src_row_off' and 'num_rows'. It
  // is copied to 'dst' at 'dst_row_off'.
  //
  // Note: 'dst' will be resized if the copy causes it to grow (though this is
  // just a "logical" resize; no reallocation takes place).
  Status CopyTo(RowBlock* dst, size_t src_row_off,
                size_t dst_row_off, size_t num_rows) const {
    DCHECK_SCHEMA_EQ(*schema_, *dst->schema());
    DCHECK_GE(nrows_, src_row_off + num_rows);

    size_t new_num_rows = dst_row_off + num_rows;
    if (new_num_rows > dst->nrows()) {
      // This will crash if 'dst' lacks adequate capacity.
      dst->Resize(new_num_rows);
    }

    for (size_t col_idx = 0; col_idx < schema_->num_columns(); col_idx++) {
      ColumnBlock src_cb(column_block(col_idx));
      ColumnBlock dst_cb(dst->column_block(col_idx));
      RETURN_NOT_OK(src_cb.CopyTo(sel_vec_, &dst_cb,
                                  src_row_off, dst_row_off, num_rows));
    }

    sel_vec_.CopyTo(&dst->sel_vec_, src_row_off, dst_row_off, num_rows);
    return Status::OK();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(RowBlock);

  static size_t RowBlockSize(const Schema& schema, size_t nrows) {
    size_t block_size = schema.num_columns() * sizeof(size_t);
    size_t bitmap_size = BitmapSize(nrows);
    for (size_t col = 0; col < schema.num_columns(); col++) {
      const ColumnSchema& col_schema = schema.column(col);
      block_size += nrows * col_schema.type_info()->size();
      if (col_schema.is_nullable())
        block_size += bitmap_size;
    }
    return block_size;
  }

  const Schema* schema_;
  std::vector<uint8_t*> columns_data_;
  std::vector<uint8_t*> column_null_bitmaps_;

  // The maximum number of rows that can be stored in our allocated buffer.
  size_t row_capacity_;

  // The number of rows currently being processed in this block.
  // nrows_ <= row_capacity_
  size_t nrows_;

  Arena* arena_;

  // The bitmap indicating which rows are valid in this block.
  // Deleted rows or rows which have failed to pass predicates will be zeroed
  // in the bitmap, and thus not returned to the end user.
  SelectionVector sel_vec_;
};

// Provides an abstraction to interact with a RowBlock row.
//  Example usage:
//    RowBlock row_block(schema, 10, NULL);
//    RowBlockRow row = row_block.row(5);       // Get row 5
//    void *cell_data = row.cell_ptr(3);        // Get column 3 of row 5
//    ...
class RowBlockRow {
 public:
  typedef ColumnBlock::Cell Cell;

  // Constructs a new RowBlockRow.
  //
  // The 'row_block' object must outlive this RowBlockRow.
  explicit RowBlockRow(const RowBlock* row_block = nullptr,
                       size_t row_index = 0)
    : row_block_(row_block), row_index_(row_index) {
  }

  RowBlockRow* Reset(const RowBlock* row_block, size_t row_index) {
    row_block_ = row_block;
    row_index_ = row_index;
    return this;
  }

  const RowBlock* row_block() const {
    return row_block_;
  }

  size_t row_index() const {
    return row_index_;
  }

  const Schema* schema() const {
    return row_block_->schema();
  }

  bool is_null(size_t col_idx) const {
    return column_block(col_idx).is_null(row_index_);
  }

  uint8_t* mutable_cell_ptr(size_t col_idx) const {
    return const_cast<uint8_t*>(cell_ptr(col_idx));
  }

  const uint8_t* cell_ptr(size_t col_idx) const {
    return column_block(col_idx).cell_ptr(row_index_);
  }

  const uint8_t* nullable_cell_ptr(size_t col_idx) const {
    return column_block(col_idx).nullable_cell_ptr(row_index_);
  }

  Cell cell(size_t col_idx) const {
    return row_block_->column_block(col_idx).cell(row_index_);
  }

  ColumnBlock column_block(size_t col_idx) const {
    return row_block_->column_block(col_idx);
  }

  // Mark this row as unselected in the selection vector.
  void SetRowUnselected() {
    // TODO(todd): const-ness issues since this class holds a const RowBlock*.
    // hack around this for now
    SelectionVector* vec = const_cast<SelectionVector*>(row_block_->selection_vector());
    vec->SetRowUnselected(row_index_);
  }

#ifndef NDEBUG
  void OverwriteWithPattern(StringPiece pattern) {
    const Schema* schema = row_block_->schema();
    for (size_t col = 0; col < schema->num_columns(); col++) {
      row_block_->column_block(col).OverwriteWithPattern(row_index_, pattern);
    }
  }
#endif

 private:
  const RowBlock* row_block_;
  size_t row_index_;
};

inline RowBlockRow RowBlock::row(size_t idx) const {
  return RowBlockRow(this, idx);
}

} // namespace kudu

#endif
