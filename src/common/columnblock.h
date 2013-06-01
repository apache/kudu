// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_COLUMNBLOCK_H
#define KUDU_COMMON_COLUMNBLOCK_H

#include "common/types.h"
#include "gutil/gscoped_ptr.h"
#include "util/memory/arena.h"

namespace kudu {

class Arena;

// A block of data all belonging to a single column.
//
// This is simply a view into a buffer - it does not have any associated
// storage in and of itself. It does, however, maintain its type
// information, which can be used for extra type safety in debug mode.
class ColumnBlock {
public:
  ColumnBlock(const TypeInfo &type,
              void *data,
              size_t nrows,
              Arena *arena) :
    type_(type),
    data_(reinterpret_cast<uint8_t *>(data)),
    nrows_(nrows),
    arena_(arena)
  {
    DCHECK(data_) << "null data";
  }

  ColumnBlock SliceRange(size_t start_row,
                         size_t nrows) {
    return ColumnBlock(type_,
                       cell_ptr(start_row),
                       nrows_ - start_row,
                       arena_);
  }

  // Advance the column block forward so that it points midway
  // into its data. This is mostly useful after cloning a ColumnBlock.
  void Advance(size_t skip) {
    // Check <= here, not <, since you can skip to
    // the very end of the data (leaving an empty block)
    DCHECK_LE(skip, nrows_);

    data_ = data_ + type_.size() * skip;
    nrows_ -= skip;
  }

  // Return a pointer to the given cell.
  void *cell_ptr(size_t idx) {
    DCHECK_LT(idx, nrows_);
    return data_ + type_.size() * idx;
  }

  size_t stride() const { return type_.size(); }
  const void * data() const { return data_; }
  void *data() { return data_; }
  const size_t size() const { return nrows_; }
  const size_t nrows() const { return nrows_; }

  Arena *arena() { return arena_; }

  const TypeInfo& type_info() const {
    return type_;
  }

private:

  const TypeInfo &type_;

  uint8_t *data_;
  size_t nrows_;

  Arena *arena_;
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
                new cpp_type[n_rows],
                n_rows,
                new Arena(1024, 1*1024*1024)),
    data_(reinterpret_cast<cpp_type *>(data())),
    arena_(arena())
  {}

  const cpp_type &operator[](size_t idx) const {
    return data_[idx];
  }

  cpp_type &operator[](size_t idx) {
    return data_[idx];
  }

private:

  gscoped_array<cpp_type> data_;
  gscoped_ptr<Arena> arena_;

};

}
#endif
