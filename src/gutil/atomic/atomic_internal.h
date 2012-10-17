// Copyright 2010 Google Inc. All Rights Reserved.
//
// concurrent/atomic.h internals
// IWYU pragma: private, include "concurrent/atomic/atomic.h"
// IWYU pragma: friend concurrent/atomic/.*

#ifndef CONCURRENT_ATOMIC_ATOMIC_INTERNAL_H_
#define CONCURRENT_ATOMIC_ATOMIC_INTERNAL_H_

#include "gutil/mutex.h"
#include "gutil/type_traits.h"

namespace concurrent {
namespace detail {

// By default disable all fetch_*() members
template <typename T> struct int_type { enum { value = false }; };
template <typename T> struct bits_type { enum { value = false }; };
template <typename T> struct flag_type { enum { value = false }; };

// Most significant bit of S
template <int S> struct msb {
  // n & (n-1) clears the lsb of n, so no_lsb is S without the lsb,
  // zero if S is a power of two, remaining bits of S otherwise
  enum { no_lsb = S & (S -1) };
  enum {
    value = no_lsb              // if not a power of two
        ? msb<no_lsb>::value    // then return msb of remaining bits
        : S                     // else return S itself
  };
};
template <> struct msb<0> {
  enum { value = 0 };
};

// S rounded up to a power of two
template <int S> struct ceil {
  enum { no_lsb = S & (S -1) };
  enum { value = (no_lsb ? 2 : 1) * msb<S>::value };
};

// sizeof(T) rounded up to a power of two
template <typename T> struct size_ceil {
  enum { value = ceil<sizeof(T)>::value };
};

// Fallback atomic representation: object + mutex.  Specialized in the
// platform headers for the supported native atomic type sizes.
template <int S, typename T> struct size_to_atomic {
  // Represented type
  typedef T value_type;
  // Native atomic type (if there is such), otherwise void
  typedef void native_type;

  // Is it a lock-free type
  enum { is_lockfree = false };

  char value[sizeof(T)];

  native_type volatile* address() const volatile {
    return const_cast<char volatile*>(value);
  }

  // TODO(user): replace with spin-lock
  // TODO(user): use a hash-table instead of an embedded lock?
  //               if yes, take care that value[] is still aligned
  //                 (does that imply faster memcpy?)
  mutable Mutex m;
};

template <typename T> struct atomic_t
  : public size_to_atomic<size_ceil<T>::value, T> {
};

// The atomic operations are implemented using functor templates
//   template <typename V, bool L> struct atomic_*_internal
// L = false selects the mutex-based fallback implementation
// L = true selects the native lock-free implementation

// CAS
template <typename V, bool L> struct atomic_compare_exchange_strong_internal;

template <typename V>
inline bool atomic_compare_exchange_strong_explicit(atomic_t<V> volatile* p,
                                                    V* expected,
                                                    V desired,
                                                    int order) {
  return atomic_compare_exchange_strong_internal<V, atomic_t<V>::is_lockfree>()
      (p, expected, desired, order);
}

template <typename V, bool L> struct atomic_compare_exchange_weak_internal;

template <typename V>
inline bool atomic_compare_exchange_weak_explicit(atomic_t<V> volatile* p,
                                                  V* expected,
                                                  V desired,
                                                  int order) {
  return atomic_compare_exchange_weak_internal<V, atomic_t<V>::is_lockfree>()
      (p, expected, desired, order);
}

// Load
template <typename V, bool L> struct atomic_load_internal;

template <typename V>
inline V atomic_load_explicit(atomic_t<V> volatile const* p, int order) {
  return atomic_load_internal<V, atomic_t<V>::is_lockfree>()(p, order);
}

// Exchange
template <typename V, bool L> struct atomic_exchange_internal;

template <typename V>
inline V atomic_exchange_explicit(atomic_t<V> volatile* p,
                                  V desired,
                                  int order) {
  return atomic_exchange_internal<V, atomic_t<V>::is_lockfree>()
      (p, desired, order);
}

// Store
template <typename V, bool L> struct atomic_store_internal;

template <typename V>
inline void atomic_store_explicit(atomic_t<V> volatile* p, V value, int order) {
  atomic_store_internal<V, atomic_t<V>::is_lockfree>()(p, value, order);
}

// +=
template <typename V, bool L> struct atomic_fetch_add_internal;

template <typename V>
inline V atomic_fetch_add_explicit(atomic_t<V> volatile* p, V diff, int order) {
  return atomic_fetch_add_internal
      <V, int_type<V>::value && atomic_t<V>::is_lockfree>()
      (p, diff, order);
}

// Test and Set
template <typename V, bool L> struct atomic_test_and_set_internal;

template <typename V>
inline bool atomic_test_and_set_explicit(atomic_t<V> volatile* p, int order) {
  return atomic_test_and_set_internal
      <V, flag_type<V>::value && atomic_t<V>::is_lockfree>()(p, order);
}

// &=
template <typename V, bool L> struct atomic_fetch_and_internal;

template <typename V>
inline V atomic_fetch_and_explicit(atomic_t<V> volatile* p, V bits, int order) {
  return atomic_fetch_and_internal
      <V, bits_type<V>::value && atomic_t<V>::is_lockfree>()
      (p, bits, order);
}

// |=
template <typename V, bool L> struct atomic_fetch_or_internal;

template <typename V>
inline V atomic_fetch_or_explicit(atomic_t<V> volatile* p, V bits, int order) {
  return atomic_fetch_or_internal
      <V, bits_type<V>::value && atomic_t<V>::is_lockfree>()
      (p, bits, order);
}

// ^=
template <typename V, bool L> struct atomic_fetch_xor_internal;

template <typename V>
inline V atomic_fetch_xor_explicit(atomic_t<V> volatile* p, V bits, int order) {
  return atomic_fetch_xor_internal
      <V, bits_type<V>::value && atomic_t<V>::is_lockfree>()
      (p, bits, order);
}

}  // namespace detail
}  // namespace concurrent

#endif  // CONCURRENT_ATOMIC_ATOMIC_INTERNAL_H_
