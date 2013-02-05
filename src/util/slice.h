// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef KUDU_UTIL_SLICE_H_
#define KUDU_UTIL_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include "faststring.h"
#include "gutil/strings/fastmem.h"
#include "gutil/stringprintf.h"

namespace kudu {

class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) { }

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) { }

  // Create a slice that refers to the contents of "s"
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) : data_(s), size_(strlen(s)) { }

  // Create a slice that refers to the contents of the faststring.
  // Note that further appends to the faststring may invalidate this slice.
  Slice(const faststring &s) : data_(s.data()), size_(s.size()) { }

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }

  // Return a mutable pointer to the beginning of the referenced data.
  char *mutable_data() { return const_cast<char *>(data_); }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  const char &operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() { data_ = ""; size_ = 0; }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  std::string ToString() const { return std::string(data_, size_); }

  std::string ToDebugString() const {
    int size = 0;
    for (int i = 0; i < size_; i++) {
      if (!isgraph(data_[i])) {
        size += 4;
      } else {
        size++;
      }
    }
    std::string ret;
    ret.reserve(size);
    for (int i = 0; i < size_; i++) {
      if (!isgraph(data_[i])) {
        StringAppendF(&ret, "\\x%02x", data_[i] & 0xff);
      } else {
        ret.push_back(data_[i]);
      }
    }
    return ret;
  }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (strings::memeq(data_, x.data_, x.size_)));
  }

 private:
  const char* data_;
  size_t size_;

  // Intentionally copyable
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (strings::memeq(x.data(), y.data(), x.size())));
}

inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

inline int Slice::compare(const Slice& b) const {
  const int min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = strings::fastmemcmp_inlined(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

}  // namespace kudu


#endif  // KUDU_UTIL_SLICE_H_
