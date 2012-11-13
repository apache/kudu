// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_ROW_H
#define KUDU_TABLET_ROW_H

#include <boost/noncopyable.hpp>
#include <glog/logging.h>

#include "cfile/types.h"
#include "tablet/schema.h"
#include "util/memory/arena.h"

namespace kudu {
namespace tablet {

// Utility class for building rows corresponding to a given schema.
// This is used when inserting data into the MemStore or a new Layer.
class RowBuilder : boost::noncopyable {
public:
  RowBuilder(const Schema &schema) :
    schema_(schema),
    arena_(1024, 1024*1024)
  {
    Reset();
  }

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
    CheckNextType(cfile::STRING);

    Slice *ptr = reinterpret_cast<Slice *>(buf_ + byte_idx_);
    CHECK(arena_.RelocateSlice(slice, ptr)) << "could not allocate space in arena";

    Advance();
  }

  void AddString(const string &str) {
    CheckNextType(cfile::STRING);

    char *in_arena = arena_.AddStringPieceContent(str);
    CHECK(in_arena) << "could not allocate space in arena";

    Slice *ptr = reinterpret_cast<Slice *>(buf_ + byte_idx_);
    *ptr = Slice(in_arena, str.size());

    Advance();
  }

  void AddUint32(uint32_t val) {
    CheckNextType(cfile::UINT32);
    *reinterpret_cast<uint32_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

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

} // namespace tablet
} // namespace kudu

#endif
