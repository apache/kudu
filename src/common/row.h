// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_ROW_H
#define KUDU_COMMON_ROW_H

#include <boost/noncopyable.hpp>
#include <glog/logging.h>

#include "common/types.h"
#include "common/schema.h"
#include "util/memory/arena.h"

namespace kudu {

// Copy any indirect (eg STRING) data referenced by the given row into the
// provided arena.
//
// The row itself is mutated so that the indirect data points to the relocated
// storage.
template <class ArenaType, class RowType>
inline Status CopyRowIndirectDataToArena(RowType *row,
                                         ArenaType *dst_arena) {
  const Schema &schema = row->schema();
  // For any Slice columns, copy the sliced data into the arena
  // and update the pointers
  for (int i = 0; i < schema.num_columns(); i++) {
    uint8_t *ptr = row->cell_ptr(schema, i);

    if (schema.column(i).type_info().type() == STRING) {
      Slice *slice = reinterpret_cast<Slice *>(ptr);
      Slice copied_slice;
      if (!dst_arena->RelocateSlice(*slice, &copied_slice)) {
        return Status::IOError("Unable to relocate slice");
      }

      *slice = copied_slice;
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
    uint8_t *dst_cell = dst_row->cell_ptr(dst_row->schema(), proj_col_idx);
    const void *src_cell = src_row.cell_ptr(src_row.schema(), src_col_idx);

    const ColumnSchema &dst_col = dst_row->schema().column(proj_col_idx);
    RETURN_NOT_OK(dst_col.CopyCell(dst_cell, src_cell, dst_arena));
  }
  return Status::OK();
}

// Utility class for building rows corresponding to a given schema.
// This is used when inserting data into the MemStore or a new Layer.
class RowBuilder : boost::noncopyable {
public:
  explicit RowBuilder(const Schema &schema) :
    schema_(schema),
    arena_(1024, 1024*1024)
  {
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
    buf_ = reinterpret_cast<uint8_t *>(
      arena_.AllocateBytes(schema_.byte_size()));
    CHECK(buf_) <<
      "could not allocate " << schema_.byte_size() << " bytes for row builder";
    col_idx_ = 0;
    byte_idx_ = 0;
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

  void AddUint32(uint32_t val) {
    CheckNextType(UINT32);
    *reinterpret_cast<uint32_t *>(&buf_[byte_idx_]) = val;
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
    return Slice(buf_, byte_idx_);
  }

private:
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
};

} // namespace kudu

#endif
