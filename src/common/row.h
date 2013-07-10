// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_ROW_H
#define KUDU_COMMON_ROW_H

#include <glog/logging.h>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/schema.h"
#include "gutil/macros.h"
#include "util/memory/arena.h"
#include "util/bitmap.h"

namespace kudu {

// Copy any indirect (eg STRING) data referenced by the given row into the
// provided arena.
//
// The row itself is mutated so that the indirect data points to the relocated
// storage.
template <class RowType, class ArenaType>
inline Status CopyRowIndirectDataToArena(RowType *row, ArenaType *dst_arena) {
  const Schema &schema = row->schema();
  // For any Slice columns, copy the sliced data into the arena
  // and update the pointers
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col_schema = schema.column(i);

    if (col_schema.type_info().type() == STRING) {
      if (col_schema.is_nullable() && row->is_null(schema, i)) {
        continue;
      }

      const Slice *slice = reinterpret_cast<const Slice *>(row->cell_ptr(schema, i));
      Slice copied_slice;
      if (!dst_arena->RelocateSlice(*slice, &copied_slice)) {
        return Status::IOError("Unable to relocate slice");
      }

      row->SetCellValue(schema, i, &copied_slice);
    }
  }
  return Status::OK();
}

// Project a row from one schema into another, using the given
// projection mapping:
//   projection_mapping[dst_row_column] = src_row_column
// i.e. projection_mapping should have the same number of entries
// as dst_row->schema().num_columns()
//
// The projection_mapping can be built using Schema::GetProjectionFrom(...).
//
// Indirected data is copied into the provided dst arena.
template<class RowType1, class RowType2, class ArenaType>
inline Status ProjectRow(const RowType1 &src_row, const vector<size_t> &projection_mapping,
                         RowType2 *dst_row, ArenaType *dst_arena) {
  DCHECK_EQ(projection_mapping.size(), dst_row->schema().num_columns());

  for (size_t proj_col_idx = 0; proj_col_idx < projection_mapping.size(); proj_col_idx++) {
    size_t src_col_idx = projection_mapping[proj_col_idx];

    const void *src_cell;
    if (src_row.schema().column(src_col_idx).is_nullable()) {
      src_cell = src_row.nullable_cell_ptr(src_row.schema(), src_col_idx);
    } else {
      src_cell = src_row.cell_ptr(src_row.schema(), src_col_idx);
    }

    if (dst_row->schema().column(proj_col_idx).is_nullable()) {
      RETURN_NOT_OK(dst_row->CopyNullableCell(dst_row->schema(), proj_col_idx,src_cell, dst_arena));
    } else {
      RETURN_NOT_OK(dst_row->CopyCell(dst_row->schema(), proj_col_idx, src_cell, dst_arena));
    }
  }

  return Status::OK();
}

class ContiguousRowHelper {
 public:
  static size_t null_bitmap_size(const Schema& schema) {
    return schema.has_nullables() ? BitmapSize(schema.num_columns()) : 0;
  }

  static size_t row_size(const Schema& schema) {
    return schema.byte_size() + null_bitmap_size(schema);
  }

  static void InitNullsBitmap(const Schema& schema, Slice& row_data) {
    InitNullsBitmap(schema, row_data.mutable_data(), row_data.size() - schema.byte_size());
  }

  static void InitNullsBitmap(const Schema& schema, uint8_t *row_data, size_t bitmap_size) {
    uint8_t *null_bitmap = row_data + schema.byte_size();
    for (size_t i = 0; i < bitmap_size; ++i) {
      null_bitmap[i] = 0xff;
    }
  }

  static void SetCellValue(const Schema& schema, uint8_t *row_data, size_t col_idx, const void *value) {
    const ColumnSchema& col_schema = schema.column(col_idx);
    if (col_schema.is_nullable()) {
      uint8_t *null_bitmap = row_data + schema.byte_size();
      BitmapChange(null_bitmap, col_idx, value != NULL);
      if (value == NULL) return;
    }
    uint8_t *dst_ptr = row_data + schema.column_offset(col_idx);
    strings::memcpy_inlined(dst_ptr, value, col_schema.type_info().size());
  }

  static bool is_null(const Schema& schema, const uint8_t *row_data, size_t col_idx) {
    DCHECK(schema.column(col_idx).is_nullable());
    return !BitmapTest(row_data + schema.byte_size(), col_idx);
  }

  static const uint8_t *cell_ptr(const Schema& schema, const uint8_t *row_data, size_t col_idx) {
    return row_data + schema.column_offset(col_idx);
  }

  static const uint8_t *nullable_cell_ptr(const Schema& schema, const uint8_t *row_data, size_t col_idx) {
    return is_null(schema, row_data, col_idx) ? NULL : cell_ptr(schema, row_data, col_idx);
  }
};

// Utility class for building rows corresponding to a given schema.
// This is used when inserting data into the MemStore or a new Layer.
class RowBuilder {
 public:
  explicit RowBuilder(const Schema &schema)
    : schema_(schema),
      arena_(1024, 1024*1024),
      bitmap_size_(ContiguousRowHelper::null_bitmap_size(schema)) {
    Reset();
  }

  // Reset the RowBuilder so that it is ready to build
  // the next row.
  // NOTE: The previous row's data is invalidated. Even
  // if the previous row's data has been copied, indirected
  // entries such as strings may end up shared or deallocated
  // after Reset. So, the previous row must be fully copied
  // (eg using CopyRowToArena()).
  void Reset() {
    arena_.Reset();
    size_t row_size = schema_.byte_size() + bitmap_size_;
    buf_ = reinterpret_cast<uint8_t *>(arena_.AllocateBytes(row_size));
    CHECK(buf_) << "could not allocate " << row_size << " bytes for row builder";
    col_idx_ = 0;
    byte_idx_ = 0;
    ContiguousRowHelper::InitNullsBitmap(schema_, buf_, bitmap_size_);
  }

  void AddString(const Slice &slice) {
    CheckNextType(STRING);

    Slice *ptr = reinterpret_cast<Slice *>(buf_ + byte_idx_);
    CHECK(arena_.RelocateSlice(slice, ptr)) << "could not allocate space in arena";

    Advance();
  }

  void AddString(const string &str) {
    CheckNextType(STRING);

    uint8_t *in_arena = arena_.AddSlice(str);
    CHECK(in_arena) << "could not allocate space in arena";

    Slice *ptr = reinterpret_cast<Slice *>(buf_ + byte_idx_);
    *ptr = Slice(in_arena, str.size());

    Advance();
  }

  void AddInt8(int8_t val) {
    CheckNextType(INT8);
    *reinterpret_cast<int8_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddUint8(uint8_t val) {
    CheckNextType(UINT8);
    *reinterpret_cast<uint8_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddInt16(int16_t val) {
    CheckNextType(INT16);
    *reinterpret_cast<int16_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddUint16(uint16_t val) {
    CheckNextType(UINT16);
    *reinterpret_cast<uint16_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddInt32(int32_t val) {
    CheckNextType(INT32);
    *reinterpret_cast<int32_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddUint32(uint32_t val) {
    CheckNextType(UINT32);
    *reinterpret_cast<uint32_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddInt64(int64_t val) {
    CheckNextType(INT64);
    *reinterpret_cast<int64_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddUint64(uint64_t val) {
    CheckNextType(UINT64);
    *reinterpret_cast<uint64_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  void AddNull() {
    CHECK(schema_.column(col_idx_).is_nullable());
    BitmapClear(buf_ + schema_.byte_size(), col_idx_);
    Advance();
  }

  // Retrieve the data slice from the current row.
  // The Add*() functions must have been called an appropriate
  // number of times such that all columns are filled in, or else
  // a crash will occur.
  //
  // The data slice returned by this is only valid until the next
  // call to Reset().
  // Note that the Slice may also contain pointers which refer to
  // other parts of the internal Arena, so even if the returned
  // data is copied, it is not safe to Reset() before also calling
  // CopyRowIndirectDataToArena.
  const Slice data() const {
    CHECK_EQ(byte_idx_, schema_.byte_size());
    return Slice(buf_, byte_idx_ + bitmap_size_);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(RowBuilder);

  void CheckNextType(DataType type) {
    CHECK_EQ(schema_.column(col_idx_).type_info().type(),
             type);
  }

  void Advance() {
    int size = schema_.column(col_idx_).type_info().size();
    byte_idx_ += size;
    col_idx_++;
  }

  const Schema schema_;
  Arena arena_;
  uint8_t *buf_;

  size_t col_idx_;
  size_t byte_idx_;
  size_t bitmap_size_;
};


// TODO: Currently used only for testing and as memstore row wrapper

// The row has all columns layed out in memory based on the schema.column_offset()
class ContiguousRow {
 public:
  ContiguousRow(const Schema& schema, void *row_data = NULL)
    : schema_(schema), row_data_(reinterpret_cast<uint8_t *>(row_data)) {
  }

  const Schema& schema() const {
    return schema_;
  }

  void Reset(void *row_data) {
    row_data_ = reinterpret_cast<uint8_t *>(row_data);
  }

  void SetCellValue(const Schema& schema, size_t col_idx, const void *value) {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    ContiguousRowHelper::SetCellValue(schema, row_data_, col_idx, value);
  }

  bool is_null(const Schema& schema, size_t col_idx) const {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    return ContiguousRowHelper::is_null(schema, row_data_, col_idx);
  }

  const uint8_t *cell_ptr(const Schema& schema, size_t col_idx) const {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    return ContiguousRowHelper::cell_ptr(schema, row_data_, col_idx);
  }

  const uint8_t *nullable_cell_ptr(const Schema& schema, size_t col_idx) const {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    return ContiguousRowHelper::nullable_cell_ptr(schema, row_data_, col_idx);
  }

 private:
  friend class ConstContiguousRow;

  const Schema& schema_;
  uint8_t *row_data_;
};

// This is the same as ContiguousRow except it refers to a const area of memory that
// should not be mutated.
class ConstContiguousRow {
 public:
  explicit ConstContiguousRow(const ContiguousRow &row)
    : schema_(row.schema_),
      row_data_(row.row_data_) {
  }

  ConstContiguousRow(const Schema& schema, const void *row_data = NULL)
    : schema_(schema), row_data_(reinterpret_cast<const uint8_t *>(row_data)) {
  }

  const Schema& schema() const {
    return schema_;
  }

  const uint8_t *row_data() const {
    return row_data_;
  }

  bool is_null(const Schema& schema, size_t col_idx) const {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    return ContiguousRowHelper::is_null(schema, row_data_, col_idx);
  }

  const uint8_t *cell_ptr(const Schema& schema, size_t col_idx) const {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    return ContiguousRowHelper::cell_ptr(schema, row_data_, col_idx);
  }

  const uint8_t *nullable_cell_ptr(const Schema& schema, size_t col_idx) const {
    // TODO: Handle different schema
    DCHECK(schema.Equals(schema_));
    return ContiguousRowHelper::nullable_cell_ptr(schema, row_data_, col_idx);
  }

 private:
  const Schema& schema_;
  const uint8_t *row_data_;
};

} // namespace kudu

#endif
