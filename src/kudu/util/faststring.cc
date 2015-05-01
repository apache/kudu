// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/faststring.h"

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {

void faststring::GrowByAtLeast(size_t count) {
  // Not enough space, need to reserve more.
  // Don't reserve exactly enough space for the new string -- that makes it
  // too easy to write perf bugs where you get O(n^2) append.
  // Instead, alwayhs expand by at least 50%.

  size_t to_reserve = len_ + count;
  if (len_ + count < len_ * 3 / 2) {
    to_reserve = len_ *  3 / 2;
  }
  GrowArray(to_reserve);
}

void faststring::GrowArray(size_t newcapacity) {
  DCHECK_GE(newcapacity, capacity_);
  gscoped_array<uint8_t> newdata(new uint8_t[newcapacity]);
  if (len_ > 0) {
    memcpy(&newdata[0], &data_[0], len_);
  }
  capacity_ = newcapacity;
  if (data_ != initial_data_) {
    delete[] data_;
  } else {
    ASAN_POISON_MEMORY_REGION(initial_data_, arraysize(initial_data_));
  }

  data_ = newdata.release();
  ASAN_POISON_MEMORY_REGION(data_ + len_, capacity_ - len_);
}


} // namespace kudu
