// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_UTIL_ATOMIC_H
#define KUDU_UTIL_ATOMIC_H

#include <algorithm>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"

namespace kudu {

// See top-level comments in kudu/gutil/atomicops.h for further
// explanations of these levels.
enum MemoryOrder {
  // Relaxed memory ordering, doesn't use any barriers.
  kMemOrderNoBarrier = 0,

  // Ensures that no later memory access by the same thread can be
  // reordered ahead of the operation.
  kMemOrderAcquire = 1,

  // Ensures that no previous memory access by the same thread can be
  // reordered after the operation.
  kMemOrderRelease = 2,

  // Ensures that neither previous NOR later memory access by the same
  // thread can be reordered after the operation.
  kMemOrderBarrier = 3,
};

// Atomic integer class inspired by Impala's AtomicInt and
// std::atomic<> in C++11.
//
// NOTE: currently only int32_t and int64_t are supported.
//
// See also: kudu/gutil/atomicops.h
template<typename T>
class AtomicInt {
 public:
  // Initialize the underlying value to 'initial_value'. The
  // initilization performs a Store with 'kMemOrderNoBarrier'.
  explicit AtomicInt(T initial_value);

  // Returns the underlying value. Does not support 'kMemOrderBarrier'.
  T Load(MemoryOrder mem_order) const;

  // Sets the underlying value to 'new_value'. Does not support
  // 'kMemOrderBarrier'.
  void Store(T new_value, MemoryOrder mem_order);

  // Iff the underlying value is equal to 'expected_val', sets the
  // underlying value to 'new_value' and returns true; returns false
  // otherwise. Does not support 'kMemOrderBarrier'.
  bool CompareAndSwap(T expected_val, T new_value, MemoryOrder mem_order);

  // Iff the underlying value is equal to 'expected_val', sets the
  // underlying value to 'new_value' and returns
  // 'expected_val'. Otherwise, returns the current underlying
  // value. Does not support 'kMemOrderBarrier'.
  T CompareAndSwapVal(T expected_val, T new_value, MemoryOrder mem_order);

  // Sets the underlying value to 'new_value' iff 'new_value' is
  // greater than the current underlying value. Does not support
  // 'kMemOrderBarrier'.
  void StoreMax(T new_value, MemoryOrder mem_order);

  // Sets the underlying value to 'new_value' iff 'new_value' is less
  // than the current underlying value. Does not support
  // 'kMemOrderBarrier'.
  void StoreMin(T new_value, MemoryOrder mem_order);

  // Increments the underlying value by 1 and returns the new
  // underlying value. Does not support 'kMemOrderAcquire' or
  // 'kMemOrderRelease'.
  T Increment(MemoryOrder mem_order);

  // Increments the underlying value by 'delta' and returns the new
  // underlying value. Does not support 'kKemOrderAcquire' or
  // 'kMemOrderRelease'.
  T IncrementBy(T delta, MemoryOrder mem_order);

  // Sets the underlying value to 'new_value' and returns the previous
  // underlying value. Does not support 'kMemOrderBarrier'.
  T Exchange(T new_value, MemoryOrder mem_order);

 private:
  // If a method 'caller' doesn't support memory order described as
  // 'requested', exit by doing perform LOG(FATAL) logging the method
  // called, the requested memory order, and the supported memory
  // orders.
  static T FatalMemOrderNotSupported(const char* caller,
                                     const char* requested = "kMemOrderBarrier",
                                     const char* supported =
                                     "kMemNorderNoBarrier, kMemOrderAcquire, kMemOrderRelease");

  T value_;

  DISALLOW_COPY_AND_ASSIGN(AtomicInt);
};

template<typename T>
inline T AtomicInt<T>::Load(MemoryOrder mem_order) const {
  switch (mem_order) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_Load(&value_);
    }
    case kMemOrderBarrier: {
      return FatalMemOrderNotSupported("Load");
    }
    case kMemOrderAcquire: {
      return base::subtle::Acquire_Load(&value_);
    }
    case kMemOrderRelease: {
      return base::subtle::Release_Load(&value_);
    }
  }
}

template<typename T>
inline void AtomicInt<T>::Store(T new_value, MemoryOrder mem_order) {
  switch (mem_order) {
    case kMemOrderNoBarrier: {
      base::subtle::NoBarrier_Store(&value_, new_value);
      break;
    }
    case kMemOrderBarrier: {
      FatalMemOrderNotSupported("Store");
    }
    case kMemOrderAcquire: {
      base::subtle::Acquire_Store(&value_, new_value);
      break;
    }
    case kMemOrderRelease: {
      base::subtle::Release_Store(&value_, new_value);
      break;
    }
  }
}

template<typename T>
inline bool AtomicInt<T>::CompareAndSwap(T expected_val, T new_val, MemoryOrder mem_order) {
  return CompareAndSwapVal(expected_val, new_val, mem_order) == expected_val;
}

template<typename T>
inline T AtomicInt<T>::CompareAndSwapVal(T expected_val, T new_val, MemoryOrder mem_order) {
  switch (mem_order) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_CompareAndSwap(
          &value_, expected_val, new_val);
    }
    case kMemOrderBarrier: {
      return FatalMemOrderNotSupported("CompareAndSwap/CompareAndSwapVal");
    }
    case kMemOrderAcquire: {
      return base::subtle::Acquire_CompareAndSwap(
          &value_, expected_val, new_val);
    }
    case kMemOrderRelease: {
      return base::subtle::Release_CompareAndSwap(
          &value_, expected_val, new_val);
    }
  }
}


template<typename T>
inline T AtomicInt<T>::Increment(MemoryOrder mem_order) {
  return IncrementBy(1, mem_order);
}

template<typename T>
inline T AtomicInt<T>::IncrementBy(T delta, MemoryOrder mem_order) {
  switch (mem_order) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_AtomicIncrement(&value_, delta);
    }
    case kMemOrderBarrier: {
      return base::subtle::Barrier_AtomicIncrement(&value_, delta);
    }
    case kMemOrderAcquire: {
      return FatalMemOrderNotSupported("Increment/IncrementBy",
                                       "kMemOrderAcquire",
                                       "kMemOrderNoBarrier and kMemOrderBarrier");
    }
    case kMemOrderRelease: {
      return FatalMemOrderNotSupported("Increment/Incrementby",
                                       "kMemOrderAcquire",
                                       "kMemOrderNoBarrier and kMemOrderBarrier");
    }
  }
}

template<typename T>
inline T AtomicInt<T>::Exchange(T new_value, MemoryOrder mem_order) {
  switch (mem_order) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_AtomicExchange(&value_, new_value);
    }
    case kMemOrderBarrier: {
      return FatalMemOrderNotSupported("Exchange");
    }
    case kMemOrderAcquire: {
      return base::subtle::Acquire_AtomicExchange(&value_, new_value);
    }
    case kMemOrderRelease: {
      return base::subtle::Release_AtomicExchange(&value_, new_value);
    }
  }
}

template<typename T>
inline void AtomicInt<T>::StoreMax(T new_value, MemoryOrder mem_order) {
  T old_value = Load(mem_order);
  while (true) {
    T max_value = std::max(old_value, new_value);
    T prev_value = CompareAndSwapVal(old_value, max_value, mem_order);
    if (PREDICT_TRUE(old_value == prev_value)) {
      break;
    }
    old_value = prev_value;
  }
}

template<typename T>
inline void AtomicInt<T>::StoreMin(T new_value, MemoryOrder mem_order) {
  T old_value = Load(mem_order);
  while (true) {
    T min_value = std::min(old_value, new_value);
    T prev_value = CompareAndSwapVal(old_value, min_value, mem_order);
    if (PREDICT_TRUE(old_value == prev_value)) {
      break;
    }
    old_value = prev_value;
  }
}

} // namespace kudu
#endif /* KUDU_UTIL_ATOMIC_H */
