// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_ROW_H
#define KUDU_COMMON_ROW_H

#include <boost/noncopyable.hpp>
#include <glog/logging.h>

#include "common/types.h"
#include "common/schema.h"
#include "util/memory/arena.h"

namespace kudu {

inline Status CopyRowIndirectDataToArena(char *row,
                                         const Schema &schema,
                                         Arena *dst_arena) {
  // For any Slice columns, copy the sliced data into the arena
  // and update the pointers
  char *ptr = reinterpret_cast<char *>(row);
  for (int i = 0; i < schema.num_columns(); i++) {
    if (schema.column(i).type_info().type() == STRING) {
      Slice *slice = reinterpret_cast<Slice *>(ptr);
      Slice copied_slice;
      if (!dst_arena->RelocateSlice(*slice, &copied_slice)) {
        return Status::IOError("Unable to relocate slice");
      }

      *slice = copied_slice;
    }
    ptr += schema.column(i).type_info().size();
  }
  DCHECK_EQ(ptr, row + schema.byte_size());
  return Status::OK();
}

inline Status CopyRowToArena(const Slice &row,
                             const Schema &schema,
                             Arena *dst_arena,
                             Slice *copied) {
  // Copy the direct row data to arena
  if (!dst_arena->RelocateSlice(row, copied)) {
    return Status::IOError("no space for row data in arena");
  }

  RETURN_NOT_OK(CopyRowIndirectDataToArena(
                  copied->mutable_data(), schema, dst_arena));
  return Status::OK();
}

// Utility class for building rows corresponding to a given schema.
// This is used when inserting data into the MemStore or a new Layer.
// TODO: maybe this should not have an internal Arena, but instead
// should just take a destination arena.
class RowBuilder : boost::noncopyable {
public:
  RowBuilder(const Schema &schema) :
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

    char *in_arena = arena_.AddStringPieceContent(str);
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

  // Copy the currently built row into the destination arena.
  // This copies all referenced data as well, such as strings.
  // After using this method, the RowBuilder may be Reset.
  Status CopyRowToArena(Arena *dst_arena,
                        Slice *copied) const {
    return kudu::CopyRowToArena(data(), schema_, dst_arena, copied);
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
  // data is copied, it is not safe to Reset(). Use CopyToArena()
  // to make a deep copy of the current row.
  const Slice data() const {
    CHECK_EQ(byte_idx_, schema_.byte_size());
    return Slice(reinterpret_cast<const char *>(buf_), byte_idx_);
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
