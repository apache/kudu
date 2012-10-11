// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_CFILE_BLOCK_POINTER_H
#define KUDU_CFILE_BLOCK_POINTER_H

#include "util/coding.h"

namespace kudu { namespace cfile {

using std::string;

class BlockPointer {
public:
  BlockPointer() {}
  BlockPointer(const BlockPointer &from) :
    offset_(from.offset_),
    size_(from.size_) {}

  BlockPointer(uint64_t offset, uint64_t size) :
    offset_(offset),
    size_(size) {}

  string ToString() const {
    char tmp[100];
    snprintf(tmp, sizeof(tmp), "offset=%ld size=%d",
             offset_, size_);
    return string(tmp);
  }

  void EncodeTo(string *s) const {
    PutVarint64(s, offset_);
    PutVarint32(s, size_);
  }

  Status DecodeFrom(const char *data, const char *limit) {
    data = GetVarint64Ptr(data, limit, &offset_);
    if (!data || offset_ < 0) {
      return Status::Corruption("bad block pointer");
    }

    data = GetVarint32Ptr(data, limit, &size_);
    if (!data || size_ < 0) {
      return Status::Corruption("bad block pointer");
    }

    return Status::OK();
  }

  void CopyToPB(BlockPointerPB *pb) const {
    pb->set_offset(offset_);
    pb->set_size(size_);
  }

  uint64_t offset() {
    return offset_;
  }

  uint32_t size() {
    return size_;
  }

private:
  uint64_t offset_;
  uint32_t size_;
};


} // namespace cfile
} // namespace kudu
#endif
