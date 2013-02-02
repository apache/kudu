// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_COMMON_ROWBLOCK_H
#define KUDU_COMMON_ROWBLOCK_H

#include <boost/noncopyable.hpp>

#include "common/columnblock.h"
#include "common/schema.h"
#include "util/memory/arena.h"

namespace kudu {

class RowBlock : boost::noncopyable {
public:
  RowBlock(const Schema &schema,
           uint8_t *data,
           size_t nrows,
           Arena *arena) :
    schema_(schema),
    data_(data),
    nrows_(nrows),
    arena_(arena)
  {
  }

  uint8_t *row_ptr(size_t idx) {
    return data_ + schema_.byte_size() * idx;
  }
  const uint8_t *row_ptr(size_t idx) const {
    return data_ + schema_.byte_size() * idx;
  }

  const Schema &schema() const { return schema_; }
  Arena *arena() const { return arena_; }

  ColumnBlock column_block(size_t col_idx, size_t nrows) {
    DCHECK_LE(nrows, nrows_);
    return ColumnBlock(schema_.column(col_idx).type_info(),
                       row_ptr(0) + schema_.column_offset(col_idx),
                       schema_.byte_size(),
                       nrows,
                       arena_);
  }

  size_t nrows() const { return nrows_; }

private:
  Schema schema_;
  uint8_t *data_;
  size_t nrows_;
  Arena *arena_;
};



class ScopedRowBlock : public RowBlock {
public:
  ScopedRowBlock(const Schema &schema,
                 size_t nrows,
                 Arena *arena) :
    RowBlock(schema,
             new uint8_t[nrows * schema.byte_size()],
             nrows,
             arena)
  {}

  ~ScopedRowBlock() {
    delete [] row_ptr(0);
  }
};

} // namespace kudu

#endif
