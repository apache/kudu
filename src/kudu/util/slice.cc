// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/slice.h"

#include "kudu/gutil/stringprintf.h"
#include "kudu/util/status.h"

namespace kudu {

Status Slice::check_size(size_t expected_size) const {
  if (PREDICT_FALSE(size() != expected_size)) {
    return Status::Corruption(StringPrintf("Unexpected Slice size. "
        "Expected %zu but got %zu.", expected_size, size()), ToDebugString(100));
  }
  return Status::OK();
}

// Return a string that contains the copy of the referenced data.
std::string Slice::ToString() const {
  return std::string(reinterpret_cast<const char *>(data_), size_);
}

std::string Slice::ToDebugString(size_t max_len) const {
  size_t bytes_to_print = size_;
  bool abbreviated = false;
  if (max_len != 0 && bytes_to_print > max_len) {
    bytes_to_print = max_len;
    abbreviated = true;
  }

  int size = 0;
  for (int i = 0; i < bytes_to_print; i++) {
    if (!isgraph(data_[i])) {
      size += 4;
    } else {
      size++;
    }
  }
  if (abbreviated) {
    size += 20;  // extra padding
  }

  std::string ret;
  ret.reserve(size);
  for (int i = 0; i < bytes_to_print; i++) {
    if (!isgraph(data_[i])) {
      StringAppendF(&ret, "\\x%02x", data_[i] & 0xff);
    } else {
      ret.push_back(data_[i]);
    }
  }
  if (abbreviated) {
    StringAppendF(&ret, "...<%zd bytes total>", size_);
  }
  return ret;
}

}  // namespace kudu
