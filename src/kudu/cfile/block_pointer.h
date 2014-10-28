// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CFILE_BLOCK_POINTER_H
#define KUDU_CFILE_BLOCK_POINTER_H

#include <stdio.h>
#include <string>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/coding.h"
#include "kudu/util/status.h"

namespace kudu { namespace cfile {

using std::string;

class BlockPointer {
 public:
  BlockPointer() {}
  BlockPointer(const BlockPointer &from) :
    offset_(from.offset_),
    size_(from.size_) {}

  explicit BlockPointer(const BlockPointerPB &from) :
    offset_(from.offset()),
    size_(from.size()) {
  }

  BlockPointer(uint64_t offset, uint64_t size) :
    offset_(offset),
    size_(size) {}

  string ToString() const {
    char tmp[100];
    snprintf(tmp, sizeof(tmp), "offset=%ld size=%d",
             offset_, size_);
    return string(tmp);
  }

  template<class StrType>
  void EncodeTo(StrType *s) const {
    PutVarint64(s, offset_);
    InlinePutVarint32(s, size_);
  }

  Status DecodeFrom(const uint8_t *data, const uint8_t *limit) {
    data = GetVarint64Ptr(data, limit, &offset_);
    if (!data) {
      return Status::Corruption("bad block pointer");
    }

    data = GetVarint32Ptr(data, limit, &size_);
    if (!data) {
      return Status::Corruption("bad block pointer");
    }

    return Status::OK();
  }

  void CopyToPB(BlockPointerPB *pb) const {
    pb->set_offset(offset_);
    pb->set_size(size_);
  }

  uint64_t offset() const {
    return offset_;
  }

  uint32_t size() const {
    return size_;
  }

 private:
  uint64_t offset_;
  uint32_t size_;
};


} // namespace cfile
} // namespace kudu
#endif
