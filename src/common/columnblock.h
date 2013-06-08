// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_COLUMNBLOCK_H
#define KUDU_COMMON_COLUMNBLOCK_H

#include "common/types.h"
#include "gutil/gscoped_ptr.h"
#include "util/bitmap.h"
#include "util/memory/arena.h"
#include "util/status.h"

namespace kudu {

class Arena;

template <class ArenaType>
Status CopyCellData(const TypeInfo& type_info, void *dst, const void *src, ArenaType *dst_arena) {
  if (type_info.type() == STRING) {
    // If it's a Slice column, need to relocate the referred-to data
    // as well as the slice itself.
    // TODO: potential optimization here: if the new value is smaller than
    // the old value, we could potentially just overwrite in some cases.
    const Slice *src_slice = reinterpret_cast<const Slice *>(src);
    Slice *dst_slice = reinterpret_cast<Slice *>(dst);
    if (dst_arena != NULL) {
      if (PREDICT_FALSE(!dst_arena->RelocateSlice(*src_slice, dst_slice))) {
        return Status::IOError("out of memory copying slice", src_slice->ToString());
      }
    } else {
      // Just copy the slice without relocating.
      // This is used by callers who know that the source row's data is going
      // to stick around for the scope of the destination.
      *dst_slice = *src_slice;
    }
  } else {
    size_t size = type_info.size();
    memcpy(dst, src, size); // TODO: inline?
  }
  return Status::OK();
}

// A block of data all belonging to a single column.  The column data
// This is simply a view into a buffer - it does not have any associated
// storage in and of itself. It does, however, maintain its type
// information, which can be used for extra type safety in debug mode.
class ColumnBlock {
public:
  ColumnBlock(const TypeInfo &type,
              uint8_t *null_bitmap,
              void *data,
              size_t nrows,
              Arena *arena) :
    type_(type),
    null_bitmap_(null_bitmap),
    data_(reinterpret_cast<uint8_t *>(data)),
    nrows_(nrows),
    arena_(arena)
  {
    DCHECK(data_) << "null data";
  }

  void SetNullableCellValue(size_t idx, const void *new_val) {
    DCHECK(is_nullable());
    BitmapChange(null_bitmap_, idx, new_val != NULL);
    if (new_val != NULL) SetCellValue(idx, new_val);
  }

  void SetCellValue(size_t idx, const void *new_val) {
    strings::memcpy_inlined(mutable_cell_ptr(idx), new_val, type_.size());
  }

  template <class ArenaType>
  Status CopyNullableCell(size_t idx, const void *new_val, ArenaType *arena) {
    BitmapChange(null_bitmap_, idx, new_val != NULL);
    return (new_val != NULL) ? CopyCell(idx, new_val, arena) : Status::OK();
  }

  template <class ArenaType>
  Status CopyCell(size_t idx, const void *new_val, ArenaType *arena) {
    return CopyCellData(type_, mutable_cell_ptr(idx), new_val, arena);
  }

#ifndef NDEBUG
  void OverwriteWithPattern(size_t idx, StringPiece pattern) {
    char *col_data = reinterpret_cast<char *>(mutable_cell_ptr(idx));
    kudu::OverwriteWithPattern(col_data, type_.size(), pattern);
  }
#endif

  // Return a pointer to the given cell.
  const uint8_t *cell_ptr(size_t idx) const {
    DCHECK_LT(idx, nrows_);
    return data_ + type_.size() * idx;
  }

  // Returns a pointer to the given cell or NULL.
  const uint8_t *nullable_cell_ptr(size_t idx) const {
    return is_null(idx) ? NULL : cell_ptr(idx);
  }

  uint8_t *null_bitmap() const {
    return null_bitmap_;
  }

  bool is_nullable() const {
    return null_bitmap_ != NULL;
  }

  bool is_null(size_t idx) const {
    DCHECK(is_nullable());
    DCHECK_LT(idx, nrows_);
    return !BitmapTest(null_bitmap_, idx);
  }

  const size_t stride() const { return type_.size(); }
  const uint8_t * data() const { return data_; }
  uint8_t *data() { return data_; }
  const size_t nrows() const { return nrows_; }

  Arena *arena() { return arena_; }

  const TypeInfo& type_info() const {
    return type_;
  }

private:
  // Return a pointer to the given cell.
  uint8_t *mutable_cell_ptr(size_t idx) {
    DCHECK_LT(idx, nrows_);
    return data_ + type_.size() * idx;
  }

  const TypeInfo &type_;
  uint8_t *null_bitmap_;

  uint8_t *data_;
  size_t nrows_;

  Arena *arena_;

  friend class ColumnDataView;
};

// Wrap the ColumnBlock to expose a directly raw block at the specified offset.
// Used by the reader and block encoders to read/write raw data.
class ColumnDataView {
 public:
  ColumnDataView(ColumnBlock *column_block, size_t first_row_idx = 0)
    : column_block_(column_block), row_offset_(0)
  {
    Advance(first_row_idx);
  }

  void Advance(size_t skip) {
    // Check <= here, not <, since you can skip to
    // the very end of the data (leaving an empty block)
    DCHECK_LE(skip, column_block_->nrows());
    row_offset_ += skip;
  }

  size_t first_row_index() const {
    return row_offset_;
  }

  void SetNullBits(size_t nrows, bool value) {
    BitmapChangeBits(column_block_->null_bitmap(), row_offset_, nrows, value);
  }

  uint8_t *data() {
    return column_block_->mutable_cell_ptr(row_offset_);
  }

  const uint8_t *data() const {
    return column_block_->cell_ptr(row_offset_);
  }

  Arena *arena() { return column_block_->arena(); }

  size_t nrows() const {
    return column_block_->nrows() - row_offset_;
  }

  const size_t stride() const {
    return column_block_->stride();
  }

  const TypeInfo& type_info() const {
    return column_block_->type_info();
  }

 private:
  ColumnBlock *column_block_;
  size_t row_offset_;
};

// Utility class which allocates temporary storage for a
// dense block of column data, freeing it when it goes
// out of scope.
//
// This is more useful in test code than production code,
// since it doesn't allocate from an arena, etc.
template<DataType type>
class ScopedColumnBlock : public ColumnBlock {
public:
  typedef typename TypeTraits<type>::cpp_type cpp_type;

  explicit ScopedColumnBlock(size_t n_rows) :
    ColumnBlock(GetTypeInfo(type),
                new uint8_t[BitmapSize(n_rows)],
                new cpp_type[n_rows],
                n_rows,
                new Arena(1024, 1*1024*1024)),
    null_bitmap_(null_bitmap()),
    data_(reinterpret_cast<cpp_type *>(data())),
    arena_(arena())
  {
  }

  const cpp_type &operator[](size_t idx) const {
    return data_[idx];
  }

  cpp_type &operator[](size_t idx) {
    return data_[idx];
  }

private:
  gscoped_array<uint8_t> null_bitmap_;
  gscoped_array<cpp_type> data_;
  gscoped_ptr<Arena> arena_;

};

}
#endif
