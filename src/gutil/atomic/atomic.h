// Copyright 2010 Google Inc. All Rights Reserved.
//
// Partial implementation of std::atomic (29. Atomic operations library)
//
// TODO(user): missing free (atomic_*) functions
//               missing MACROS
//               missing kill_dependency()
//
// ATM atomic<> can only be used with types that have a default constructor.
//
// We do not support some constructions that seem to be way too error-prone:
// - memory_order arguments do not default to memory_order_seq_cst
// - operator overloads (conversion, assigment etc)
// - there is no memory_order_consume (data dependency barrier)
//
// We do support some non-standard, non-portable extensions,
// avoid them in code that needs to be portable.
// Some of them are intentional/useful:
// - in debug mode pointers are default-initialized to clobber::ptr
// - atomic_bool supports fetch_and(), fetch_or(), fetch_xor()
// Some of these are unintentional consequences implementation details:
// - atomic_bool supports test_and_set() and clear()
// - atomic_flag supports the full range of operations supported by atomic_bool

#ifndef CONCURRENT_ATOMIC_ATOMIC_H_
#define CONCURRENT_ATOMIC_ATOMIC_H_

#if defined(__powerpc64__)
#include "gutil/atomic/atomic_std.h"
#else
#include <stddef.h>
#include <stdint.h>

#include "gutil/type_traits.h"

namespace concurrent {

typedef enum memory_order {
  memory_order_relaxed,
  // memory_order_consume = memory_order_acquire,
  memory_order_acquire,
  memory_order_release,
  memory_order_acq_rel,
  memory_order_seq_cst
} memory_order;

}  // namespace concurrent


#include "gutil/atomic/atomic_internal.h"
#include "gutil/atomic/locking.h"

#if defined(THREAD_SANITIZER)
#include "gutil/atomic/tsan.h"
#elif defined(__GNUC__) && (defined(__i386) || defined(__x86_64__))
#include "gutil/atomic/x86-gcc.h"
#else
struct clobber {
  // A pointer value that does not point to any object.
  // Preferably also a value that cannot be derefenced
  // and that stands out in a memory dump
  // (a magic number like 0xDEADBEEFBAADF00DULL).
  // Used for clobbering invalidated pointers in debug builds.
  static uintptr_t raw() {
    return reinterpret_cast<uintptr_t>(&raw);
  }
  template <typename T> static T* ptr() {
    return reinterpret_cast<T*>(raw());
  }

  // Value of first possible non-zero LSB in pointers:
  // The bottom Log(align()) bits of pointers to pointers
  // (or pointers to pointer-aligned structures) can be used to
  // store additional information (flags and counters).
  static uintptr_t align() { return 1; }
  // Mask guaranteed to result in 0 when applied to valid
  // pointers to pointers.
  static uintptr_t mask() { return align()-1; }

  // Check if a pointer to a pointer is a valid one:
  // can result in false negatives (accepting invalid pointers),
  // but not in false positives (rejecting valid pointers).
  template <typename T> static bool bad(T* p) {
    return p == ptr<T>();
  }
};
#endif


namespace concurrent {

template <typename T> class atomic {
 public:
  typedef T value_type;

  bool is_lock_free() const volatile {
    return detail::atomic_t<T>::is_lockfree;
  }
  T load(memory_order order) const volatile {
    return detail::atomic_load_explicit<T>(&raw_, order);
  }
  void store(T value, memory_order order) volatile {
    detail::atomic_store_explicit(&raw_, value, order);
  }
  T exchange(T value, memory_order order) volatile {
    return detail::atomic_exchange_explicit(&raw_, value, order);
  }
  bool compare_exchange_strong(T& expected,
                               T desired,
                               memory_order order) volatile {
    return detail::atomic_compare_exchange_strong_explicit
        (&raw_, &expected, desired, order);
  }
  bool compare_exchange_weak(T& expected,
                             T desired,
                             memory_order order) volatile {
    return detail::atomic_compare_exchange_weak_explicit
        (&raw_, &expected, desired, order);
  }

  atomic() /*= default*/ {
  }
  /*constexpr*/ atomic(T value) {
    store(value, memory_order_relaxed);
  }

  // These are only sensible for the atomic_itype (and pointer) variations,
  // but we save a lot of typing and compilation time by defining them here.
  T fetch_add(T diff, memory_order order) volatile {
    return detail::atomic_fetch_add_explicit
        (&raw_, static_cast<T>(diff), order);
  }
  T fetch_sub(T diff, memory_order order) volatile {
    return fetch_add(-diff, order);
  }
  T fetch_and(T bits, memory_order order) volatile {
    return detail::atomic_fetch_and_explicit
        (&raw_, static_cast<T>(bits), order);
  }
  T fetch_or(T bits, memory_order order) volatile {
    return detail::atomic_fetch_or_explicit
        (&raw_, static_cast<T>(bits), order);
  }
  T fetch_xor(T bits, memory_order order) volatile {
    return detail::atomic_fetch_xor_explicit
        (&raw_, static_cast<T>(bits), order);
  }
  // These are only sensible for the flag (and boolean) variations.
  T test_and_set(memory_order order) volatile {
    return detail::atomic_test_and_set_explicit(&raw_, order);
  }
  void clear(memory_order order) volatile {
    store(false, order);
  }

 private:
  detail::atomic_t<T> raw_;

  atomic(atomic const&) /*= delete*/;
  atomic& operator=(atomic const&) /*= delete*/;
  atomic& operator=(atomic const&) volatile /*= delete*/;

  // TODO(user):
  bool compare_exchange_weak(T&, T, memory_order, memory_order) volatile;
  bool compare_exchange_strong(T&, T, memory_order, memory_order) volatile;

  // TODO(user): these should be public, but they are very error-prone
  operator T() const volatile;
  T operator=(T value) volatile;
  T operator++(int) volatile;
  T operator++() volatile;
  T operator--(int) volatile;
  T operator--() volatile;
  T operator+=(T diff) volatile;
  T operator-=(T diff) volatile;
  T operator&=(T bits) volatile;
  T operator|=(T bits) volatile;
  T operator^=(T bits) volatile;
};

typedef atomic<bool> atomic_flag;

// We provide some non-standard, non-portable extensions for atomic_bool.
// Refer to the head of the file for details.
#define CONCURRENT_ATOMIC_BOOL_EXTRA_METHODS

typedef atomic<bool> atomic_bool;

typedef atomic<char> atomic_char;
typedef atomic<signed char> atomic_schar;
typedef atomic<unsigned char> atomic_uchar;
typedef atomic<short> atomic_short;
typedef atomic<unsigned short> atomic_ushort;
typedef atomic<int> atomic_int;
typedef atomic<unsigned int> atomic_uint;
typedef atomic<long> atomic_long;
typedef atomic<unsigned long> atomic_ulong;
typedef atomic<long long> atomic_llong;
typedef atomic<unsigned long long> atomic_ullong;

typedef atomic<uint8_t> atomic_uint8_t;
typedef atomic<int8_t> atomic_int8_t;
typedef atomic<uint16_t> atomic_uint16_t;
typedef atomic<int16_t> atomic_int16_t;
typedef atomic<uint32_t> atomic_uint32_t;
typedef atomic<int32_t> atomic_int32_t;
typedef atomic<uint64_t> atomic_uint64_t;
typedef atomic<int64_t> atomic_int64_t;
// typedef atomic<char16_t> atomic_char16_t;
// typedef atomic<char32_t> atomic_char32_t;
typedef atomic<wchar_t> atomic_wchar_t;

typedef atomic<uint_least8_t> atomic_uint_least8_t;
typedef atomic<int_least8_t> atomic_int_least8_t;
typedef atomic<uint_least16_t> atomic_uint_least16_t;
typedef atomic<int_least16_t> atomic_int_least16_t;
typedef atomic<uint_least32_t> atomic_uint_least32_t;
typedef atomic<int_least32_t> atomic_int_least32_t;
typedef atomic<uint_least64_t> atomic_uint_least64_t;
typedef atomic<int_least64_t> atomic_int_least64_t;

typedef atomic<uint_fast8_t> atomic_uint_fast8_t;
typedef atomic<int_fast8_t> atomic_int_fast8_t;
typedef atomic<uint_fast16_t> atomic_uint_fast16_t;
typedef atomic<int_fast16_t> atomic_int_fast16_t;
typedef atomic<uint_fast32_t> atomic_uint_fast32_t;
typedef atomic<int_fast32_t> atomic_int_fast32_t;
typedef atomic<uint_fast64_t> atomic_uint_fast64_t;
typedef atomic<int_fast64_t> atomic_int_fast64_t;

typedef atomic<intptr_t> atomic_intptr_t;
typedef atomic<uintptr_t> atomic_uintptr_t;
typedef atomic<size_t> atomic_size_t;
typedef atomic<ptrdiff_t> atomic_ptrdiff_t;
typedef atomic<intmax_t> atomic_intmax_t;
typedef atomic<uintmax_t> atomic_uintmax_t;


template <> class atomic<void*> {
 public:
  bool is_lock_free() const volatile {
    return raw_.is_lock_free();
  }
  void store(void* p, memory_order order) volatile {
    raw_.store(reinterpret_cast<uintptr_t>(p), order);
  }
  void* load(memory_order order) const volatile {
    return reinterpret_cast<void*>(raw_.load(order));
  }
  void* exchange(void* p, memory_order order) volatile {
    return reinterpret_cast<void*>
        (raw_.exchange(reinterpret_cast<uintptr_t>(p), order));
  }
  bool compare_exchange_weak(void*& expected,
                             void* desired,
                             memory_order order) volatile {
    return raw_.compare_exchange_weak(reinterpret_cast<uintptr_t&>(expected),
                                      reinterpret_cast<uintptr_t>(desired),
                                      order);
  }
  bool compare_exchange_strong(void*& expected,
                               void* desired,
                               memory_order order) volatile {
    return raw_.compare_exchange_strong(reinterpret_cast<uintptr_t&>(expected),
                                        reinterpret_cast<uintptr_t>(desired),
                                        order);
  }
  bool compare_exchange_weak(void const*& expected,
                             void const* desired,
                             memory_order order) volatile {
    return compare_exchange_weak(const_cast<void*&>(expected),
                                 const_cast<void*>(desired),
                                 order);
  }
  bool compare_exchange_strong(void const*& expected,
                               void const* desired,
                               memory_order order) volatile {
    return compare_exchange_strong(const_cast<void*&>(expected),
                                   const_cast<void*>(desired),
                                   order);
  }
  void* fetch_add(ptrdiff_t diff, memory_order order) volatile {
    return reinterpret_cast<void*>(raw_.fetch_add(diff, order));
  }
  void* fetch_sub(ptrdiff_t diff, memory_order order) volatile {
    return reinterpret_cast<void*>(raw_.fetch_sub(diff, order));
  }

  atomic() /*= default*/
#ifndef NDEBUG
      :raw_(clobber::raw())
#endif
  {
  }
  /*constexpr*/ atomic(void* p)
      : raw_(reinterpret_cast<uintptr_t>(p)) {
  }

 private:
  atomic_uintptr_t raw_;

  atomic(atomic const&) /*= delete*/;
  atomic& operator=(atomic const&) /*= delete*/;
  atomic& operator=(atomic const&) volatile /*= delete*/;

  // TODO(user):
  bool compare_exchange_weak(void*&, void*,
                             memory_order, memory_order) volatile;
  bool compare_exchange_strong(void*&, void*,
                               memory_order, memory_order) volatile;
  bool compare_exchange_weak(void const*&, void const*,
                             memory_order, memory_order) volatile;
  bool compare_exchange_strong(void const*&, void const*,
                               memory_order, memory_order) volatile;

  // TODO(user): these should be public, but they are very error-prone
  operator void*() const volatile;
  void* operator=(void* const value) volatile;
  void* operator+=(ptrdiff_t diff) volatile;
  void* operator-=(ptrdiff_t diff) volatile;
};

typedef atomic<void*> atomic_address;

// TODO(user): According to 29.5.3 [atomics.types.generic]
//               atomic<T*> must privately inherit from atomic_address,
//               but most likely it's meant to be public inheritance
//               (to allow conversions).
template <typename T> class atomic<T*> : public atomic_address {
  typedef typename base::remove_cv<T>::type* cvless_type;
 public:
  bool is_lock_free() const volatile {
    return atomic_address::is_lock_free();
  }
  void store(T* p, memory_order order) volatile {
    atomic_address::store(const_cast<cvless_type>(p), order);
  }
  T* load(memory_order order) const volatile {
    return static_cast<T*>(atomic_address::load(order));
  }
  T* exchange(T* p, memory_order order) volatile {
    return static_cast<T*>(atomic_address::exchange(const_cast<cvless_type>(p),
                                                    order));
  }
  bool compare_exchange_weak(T*& expected,
                             T* desired,
                             memory_order order) volatile {
    return atomic_address::compare_exchange_weak(
        reinterpret_cast<void*&>(const_cast<cvless_type&>(expected)),
        const_cast<cvless_type>(desired),
        order);
  }
  bool compare_exchange_strong(T*& expected,
                               T* desired,
                               memory_order order) volatile {
    return atomic_address::compare_exchange_strong(
        reinterpret_cast<void*&>(const_cast<cvless_type&>(expected)),
        const_cast<cvless_type>(desired),
        order);
  }
  T* fetch_add(ptrdiff_t diff, memory_order order) volatile {
    ptrdiff_t const size = sizeof(T);
    return static_cast<T*>(atomic_address::fetch_add(size*diff, order));
  }
  T* fetch_sub(ptrdiff_t diff, memory_order order) volatile {
    ptrdiff_t const size = sizeof(T);
    return static_cast<T*>(atomic_address::fetch_sub(size*diff, order));
  }

  operator T*() const volatile {
    return load(memory_order_seq_cst);
  }

  atomic() /*= default*/ {
  }
  /*constexpr*/ atomic(T* p)
      : atomic_address(const_cast<cvless_type>(p)) {
  }

 private:
  atomic(atomic const&) /*= delete*/;
  atomic& operator=(atomic const&) /*= delete*/;
  atomic& operator=(atomic const&) volatile /*= delete*/;

  // TODO(user):
  bool compare_exchange_weak(T*&, T*,
                             memory_order, memory_order) volatile;
  bool compare_exchange_strong(T*&, T*,
                               memory_order, memory_order) volatile;

  // TODO(user): these should be public, but they are very error-prone
  T* operator=(T const*) volatile;
  T* operator+=(ptrdiff_t) volatile;
  T* operator-=(ptrdiff_t) volatile;
  T* operator++(int) volatile;
  T* operator--(int) volatile;
  T* operator++() volatile;
  T* operator--() volatile;
};


inline void atomic_thread_fence(memory_order order) {
  detail::atomic_thread_fence_explicit(order);
}

inline void atomic_signal_fence(memory_order order) {
  detail::atomic_signal_fence_explicit(order);
}

}  // namespace concurrent

#endif  // __powerpc64__


#endif  // CONCURRENT_ATOMIC_ATOMIC_H_
