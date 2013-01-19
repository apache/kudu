// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_UTIL_FASTSTRING_H
#define KUDU_UTIL_FASTSTRING_H

#include <boost/noncopyable.hpp>
#include <boost/scoped_array.hpp>
#include "gutil/strings/fastmem.h"

namespace kudu {

using boost::scoped_array;

class faststring : public boost::noncopyable {
public:
  faststring() :
    data_(new char[kInitialCapacity]),
    len_(0),
    capacity_(kInitialCapacity) {
  }

  explicit faststring(size_t capacity) :
    data_(new char[capacity]),
    len_(0),
    capacity_(capacity)
  {}

  void clear() {
    resize(0);
  }

  void resize(size_t newsize) {
    if (newsize > capacity_) {
      reserve(newsize);
    }
    len_ = newsize;
  }

  void reserve(size_t newcapacity) {
    if (PREDICT_TRUE(newcapacity <= capacity_)) return;

    scoped_array<char> newdata(new char[newcapacity]);
    strings::memcpy_inlined(&newdata[0], &data_[0], len_);
    capacity_ = newcapacity;
    data_.swap(newdata);
  }

  void append(const void *src_v, size_t count) {
    if (PREDICT_FALSE(len_ + count > capacity_)) {
      // Not enough space, need to reserve more.
      // Don't reserve exactly enough space for the new string -- that makes it
      // too easy to write perf bugs where you get O(n^2) append.
      // Instead, alwayhs expand by at least 50%.

      size_t to_reserve = len_ + count;
      if (len_ + count < len_ * 3 / 2) {
        to_reserve = len_ *  3 / 2;
      }
      reserve(to_reserve);
    }

    const char *src = reinterpret_cast<const char *>(src_v);
    // appending short values is common enough that this
    // actually helps, according to benchmarks. In theory
    // memcpy_inlined should already be just as good, but this
    // was ~20% faster for reading a large prefix-coded string file
    // where each string was only a few chars different
    if (count <= 4) {
      char *p = &data_[len_];
      for (int i = 0; i < count; i++) {
        *p++ = *src++;
      }
    } else {
      strings::memcpy_inlined(&data_[len_], src, count);
    }
    len_ += count;
  }

  void push_back(const char byte) {
    reserve(len_ + 1);
    data_[len_] = byte;
    len_++;
  }

  size_t length() const {
    return len_;
  }

  size_t size() const {
    return len_;
  }

  size_t capacity() const {
    return capacity_;
  }

  const char *data() const {
    return &data_[0];
  }

  char *data() {
    return &data_[0];
  }

  const char &at(size_t i) const {
    return data_[i];
  }

  const char &operator[](size_t i) const {
    return data_[i];
  }

  void assign_copy(const char *src, size_t len) {
    resize(len);
    memcpy(data(), src, len);
  }


private:
  enum {
    kInitialCapacity = 16
  };

  scoped_array<char> data_;
  size_t len_;
  size_t capacity_;
};

} // namespace kudu

#endif
