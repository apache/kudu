// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_COMMON_ROWBLOCK_H
#define KUDU_COMMON_ROWBLOCK_H

#include <boost/noncopyable.hpp>

#include "common/columnblock.h"
#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "util/memory/arena.h"
#include "util/bitmap.h"

namespace kudu {


// Bit-vector representing the selection status of each row in a row block.
// Initially, this vector will be set to 1 for every row, and as predicates
// are applied, the bits may be changed to 0 for any row which does not match
// a predicate.
class SelectionVector : boost::noncopyable {
public:
  SelectionVector(size_t row_capacity);

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
  // After this call, the state of the values in the bitmap is indeterminate.
  void Resize(size_t n_rows);

  // Return the number of selected rows.
  size_t CountSelected() const;

  // Return true if any rows are selected
  // This is equivalent to (CountSelected() > 0), but faster.
  bool AnySelected() const;

  bool IsRowSelected(size_t row) const {
    DCHECK_LT(row, n_rows_);
    return BitmapTest(&bitmap_[0], row);
  }

  uint8_t *mutable_bitmap() {
    return &bitmap_[0];
  }

  // Set all bits in the bitmap to 1
  void SetAllTrue() {
    // Initially all rows should be selected.
    memset(&bitmap_[0], 0xff, n_bytes_);
    // the last byte in the bitmap may have a few extra bits - need to
    // clear those

    int trailer_bits = 8 - (n_rows_ % 8);
    if (trailer_bits != 8) {
      bitmap_[n_bytes_ - 1] >>= trailer_bits;
    }
  }

  size_t nrows() const { return n_rows_; }

private:
  // The number of allocated bytes in bitmap_
  size_t bytes_capacity_;

  size_t n_rows_;
  size_t n_bytes_;

  gscoped_array<uint8_t> bitmap_;
};


// Wrapper around a buffer, which keeps the buffer's size, associated arena,
// and schema. Provides convenience accessors for indexing by row, column, etc.
class RowBlock : boost::noncopyable {
public:
  RowBlock(const Schema &schema,
           size_t nrows,
           Arena *arena);

  // Resize the block to the given number of rows.
  // This size must be <= the the allocated capacity row_capacity().
  //
  // After this call, the state of the underlying data is indeterminate.
  void Resize(size_t n_rows);

  size_t row_capacity() const {
    return row_capacity_;
  }

  uint8_t *row_ptr(size_t idx) {
    return &data_[schema_.byte_size() * idx];
  }

  const uint8_t *row_ptr(size_t idx) const {
    return &data_[schema_.byte_size() * idx];
  }

  const Slice row_slice(size_t idx) const {
    return Slice(row_ptr(idx), schema_.byte_size());
  }

  const Schema &schema() const { return schema_; }
  Arena *arena() const { return arena_; }

  ColumnBlock column_block(size_t col_idx) {
    return column_block(col_idx, nrows_);
  }

  ColumnBlock column_block(size_t col_idx, size_t nrows) {
    DCHECK_LE(nrows, nrows_);
    return ColumnBlock(schema_.column(col_idx).type_info(),
                       row_ptr(0) + schema_.column_offset(col_idx),
                       schema_.byte_size(),
                       nrows,
                       arena_);
  }

  size_t nrows() const { return nrows_; }

  // Zero the memory pointed to by this row block.
  // This physically zeros the memory, so is not efficient - mostly useful
  // from unit tests.
  void ZeroMemory() {
    memset(data_.get(), '\0', schema_.byte_size() * nrows_);
  }

  // Return the selection vector which indicates which rows have passed
  // predicates so far during evaluation of this block of rows.
  //
  // At the beginning of each batch, the vector is set to all 1s, and
  // as predicates or deletions make rows invalid, they are set to 0s.
  // After a batch has completed, only those rows with associated true
  // bits in the selection vector are valid results for the scan.
  SelectionVector *selection_vector() {
    return &sel_vec_;
  }

  const SelectionVector *selection_vector() const {
    return &sel_vec_;
  }

private:
  Schema schema_;
  gscoped_array<uint8_t> data_;

  // The maximum number of rows that can be stored in our allocated buffer.
  size_t row_capacity_;

  // The number of rows currently being processed in this block.
  // nrows_ <= row_capacity_
  size_t nrows_;

  Arena *arena_;

  // The bitmap indicating which rows are valid in this block.
  // Deleted rows or rows which have failed to pass predicates will be zeroed
  // in the bitmap, and thus not returned to the end user.
  SelectionVector sel_vec_;
};


} // namespace kudu

#endif
