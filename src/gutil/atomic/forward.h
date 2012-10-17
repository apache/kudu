// Copyright 2010 Google Inc. All Rights Reserved.
//
// Fallback implementation of missing primitives.
// Many variants are possible, but only the ones currently in use are
// implemented.
// The functions are atomic_*_fom_*(...), and they are paremetrized with
// the atomic_t<T> types instead of the plain T types.
//
// IWYU pragma: private, include "concurrent/atomic/atomic.h"
// IWYU pragma: friend concurrent/atomic/.*

#ifndef CONCURRENT_ATOMIC_FORWARD_H_
#define CONCURRENT_ATOMIC_FORWARD_H_

namespace concurrent {
namespace detail {

template <typename A>
bool atomic_test_and_set_from_exchange(A volatile* p, int order) {
  return atomic_exchange_explicit
      (p, static_cast<typename A::value_type>(true), order);
}

template <typename A>
typename A::value_type atomic_fetch_and_from_compare_exchange(
                                                    A volatile* p,
                                                    typename A::value_type bits,
                                                    int order) {
  typename A::value_type expected
      = atomic_load_explicit(p, memory_order_relaxed);

  while (!atomic_compare_exchange_weak_explicit
         (p,
          &expected,
          static_cast<typename A::value_type>(expected & bits),
          order)) {
  }

  return expected;
}

template <typename A>
typename A::value_type atomic_fetch_or_from_compare_exchange(
                                                    A volatile* p,
                                                    typename A::value_type bits,
                                                     int order) {
  typename A::value_type expected
      = atomic_load_explicit(p, memory_order_relaxed);

  while (!atomic_compare_exchange_weak_explicit
         (p,
          &expected,
          static_cast<typename A::value_type>(expected | bits),
          order)) {
  }

  return expected;
}

template <typename A>
typename A::value_type atomic_fetch_xor_from_compare_exchange(
                                                    A volatile* p,
                                                    typename A::value_type bits,
                                                    int order) {
  typename A::value_type expected
      = atomic_load_explicit(p, memory_order_relaxed);

  while (!atomic_compare_exchange_weak_explicit
         (p,
          &expected,
          static_cast<typename A::value_type>(expected ^ bits),
          order)) {
  }

  return expected;
}

}  // namespace detail
}  // namespace concurrent

#endif  // CONCURRENT_ATOMIC_FORWARD_H_
