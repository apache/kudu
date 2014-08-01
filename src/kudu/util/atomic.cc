// Copyright (c) 2014, Cloudera, inc.

#include "kudu/util/atomic.h"

#include <stdint.h>

#include <glog/logging.h>

namespace kudu {

template<typename T>
AtomicInt<T>::AtomicInt(T initial_value) {
  Store(initial_value, kMemOrderNoBarrier);
}

template<typename T>
T AtomicInt<T>::FatalMemOrderNotSupported(const char* caller,
                                          const char* requested,
                                          const char* supported) {
  LOG(FATAL) << caller << " does not support " << requested << ": only "
             << supported << " are supported.";
  return 0;
}

template
class AtomicInt<int32_t>;

template
class AtomicInt<int64_t>;

} // namespace kudu
