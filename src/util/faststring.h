// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_UTIL_FASTSTRING_H
#define KUDU_UTIL_FASTSTRING_H

#include <boost/noncopyable.hpp>
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/fastmem.h"

#include <string>

namespace kudu {

// A faststring is similar to a std::string, except that it is faster for many
// common use cases (in particular, resize() will fill with uninitialized data
// instead of memsetting to \0)
class faststring : public boost::noncopyable {
public:
  faststring() :
    data_(new uint8_t[kInitialCapacity]),
    len_(0),
    capacity_(kInitialCapacity) {
  }

  // Construct a string with the given capacity, in bytes.
  explicit faststring(size_t capacity) :
    data_(new uint8_t[capacity]),
    len_(0),
    capacity_(capacity)
  {}

  // Reset the valid length of the string to 0.
  //
  // This does not free up any memory. The capacity of the string remains unchanged.
  void clear() {
    resize(0);
  }

  // Resize the string to the given length.
  // If the new length is larger than the old length, the capacity is expanded as necessary.
  //
  // NOTE: in contrast to std::string's implementation, Any newly "exposed" bytes of data are
  // not cleared.
  void resize(size_t newsize) {
    if (newsize > capacity_) {
      reserve(newsize);
    }
    len_ = newsize;
  }

  // Reserve space for the given total amount of data. If the current capacity is already
  // larger than the newly requested capacity, this is a no-op (i.e. it does not ever free memory)
  void reserve(size_t newcapacity) {
    if (PREDICT_TRUE(newcapacity <= capacity_)) return;

    gscoped_array<uint8_t> newdata(new uint8_t[newcapacity]);
    strings::memcpy_inlined(&newdata[0], &data_[0], len_);
    capacity_ = newcapacity;
    data_.swap(newdata);
  }

  // Append the given data to the string, resizing capcaity as necessary.
  void append(const void *src_v, size_t count) {
    const uint8_t *src = reinterpret_cast<const uint8_t *>(src_v);
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

    // appending short values is common enough that this
    // actually helps, according to benchmarks. In theory
    // memcpy_inlined should already be just as good, but this
    // was ~20% faster for reading a large prefix-coded string file
    // where each string was only a few chars different
    if (count <= 4) {
      uint8_t *p = &data_[len_];
      for (int i = 0; i < count; i++) {
        *p++ = *src++;
      }
    } else {
      strings::memcpy_inlined(&data_[len_], src, count);
    }
    len_ += count;
  }

  // Append the given string to this string.
  void append(const std::string &str) {
    append(str.data(), str.size());
  }

  // Append the given character to this string.
  void push_back(const char byte) {
    reserve(len_ + 1);
    data_[len_] = byte;
    len_++;
  }

  // Return the valid length of this string.
  size_t length() const {
    return len_;
  }

  // Return the valid length of this string (identical to length())
  size_t size() const {
    return len_;
  }

  // Return the allocated capacity of this string.
  size_t capacity() const {
    return capacity_;
  }

  // Return a pointer to the data in this string. Note that this pointer
  // may be invalidated by any later non-const operation.
  const uint8_t *data() const {
    return &data_[0];
  }

  // Return a pointer to the data in this string. Note that this pointer
  // may be invalidated by any later non-const operation.
  uint8_t *data() {
    return &data_[0];
  }

  // Return the given element of this string. Note that this does not perform
  // any bounds checking.
  const uint8_t &at(size_t i) const {
    return data_[i];
  }

  // Return the given element of this string. Note that this does not perform
  // any bounds checking.
  const uint8_t &operator[](size_t i) const {
    return data_[i];
  }

  // Return the given element of this string. Note that this does not perform
  // any bounds checking.
  uint8_t &operator[](size_t i) {
    return data_[i];
  }

  // Reset the contents of this string by copying 'len' bytes from 'src'.
  void assign_copy(const uint8_t *src, size_t len) {
    resize(len);
    memcpy(data(), src, len);
  }

  // Reset the contents of this string by copying from the given std::string.
  void assign_copy(const std::string &str) {
    assign_copy(reinterpret_cast<const uint8_t *>(str.c_str()),
                str.size());
  }

  // Return a copy of this string as a std::string.
  std::string ToString() const {
    return std::string(reinterpret_cast<const char *>(data()),
                       len_);
  }

private:
  enum {
    kInitialCapacity = 16
  };

  gscoped_array<uint8_t> data_;
  size_t len_;
  size_t capacity_;
};

} // namespace kudu

#endif
