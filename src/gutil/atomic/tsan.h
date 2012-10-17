// Copyright 2011 Google Inc. All Rights Reserved.

#ifndef CONCURRENT_ATOMIC_TSAN_H_
#define CONCURRENT_ATOMIC_TSAN_H_

#ifndef THREAD_SANITIZER
#error Must be used only with ThreadSanitizer
#endif  // #ifndef THREAD_SANITIZER

#include "gutil/macros.h"
#include "gutil/atomic/forward.h"
#include "third_party/thread_sanitizer/v2/tsan_interface_atomic.h"

#define CONCURRENT_ATOMIC_TSAN_FAIL() (*(volatile int*)0x666 = 666)

struct clobber {
  static uintptr_t raw() {
    return reinterpret_cast<uintptr_t>(&raw);
  }
  template <typename T> static T* ptr() {
    return reinterpret_cast<T*>(raw());
  }
  static uintptr_t align() {
    return 1;
  }
  static uintptr_t mask() {
    return align()-1;
  }
  template <typename T> static bool bad(T* p) {
    return p == ptr<T>();
  }
};

namespace concurrent {
namespace detail {

static __tsan_memory_order tsan_mo(int order) {
  switch (order) {
    case memory_order_relaxed: return __tsan_memory_order_relaxed;
    case memory_order_acquire: return __tsan_memory_order_acquire;
    case memory_order_release: return __tsan_memory_order_release;
    case memory_order_acq_rel: return __tsan_memory_order_acq_rel;
    case memory_order_seq_cst: return __tsan_memory_order_seq_cst;
    default:                   return __tsan_memory_order_relaxed;
  }
}

template <> struct int_type<unsigned char> { enum { value = true }; };
template <> struct int_type<signed char> { enum { value = true }; };
template <> struct int_type<unsigned short> { enum { value = true }; };
template <> struct int_type<signed short> { enum { value = true }; };
template <> struct int_type<unsigned int> { enum { value = true }; };
template <> struct int_type<signed int> { enum { value = true }; };
template <> struct int_type<unsigned long> { enum { value = true }; };
template <> struct int_type<signed long> { enum { value = true }; };
template <> struct int_type<unsigned long long> { enum { value = true }; };
template <> struct int_type<signed long long> { enum { value = true }; };
template <> struct int_type<char> { enum { value = true }; };
template <> struct int_type<wchar_t> { enum { value = true }; };
template <typename T> struct int_type<T*> { enum { value = true }; };

template <> struct bits_type<unsigned char> { enum { value = true }; };
template <> struct bits_type<unsigned short> { enum { value = true }; };
template <> struct bits_type<unsigned int> { enum { value = true }; };
template <> struct bits_type<unsigned long> { enum { value = true }; };
template <> struct bits_type<unsigned long long> { enum { value = true }; };
template <> struct bits_type<bool> { enum { value = true }; };

template <> struct flag_type<bool> { enum { value = true }; };

template <typename T> struct size_to_atomic<1, T> {
  typedef T value_type;
  typedef __tsan_atomic8 native_type;
  enum { same_size = sizeof(value_type) == sizeof(native_type) };
  enum { is_lockfree = true };
  native_type value;
  native_type* address() const volatile {
    return const_cast<native_type*>(&value);
  }
};

template <typename T> struct size_to_atomic<2, T> {
  typedef T value_type;
  typedef __tsan_atomic16 native_type;
  enum { same_size = sizeof(value_type) == sizeof(native_type) };
  enum { is_lockfree = true };
  native_type value;
  native_type* address() const volatile {
    return const_cast<native_type*>(&value);
  }
};

template <typename T> struct size_to_atomic<4, T> {
  typedef T value_type;
  typedef __tsan_atomic32 native_type;
  enum { same_size = sizeof(value_type) == sizeof(native_type) };
  enum { is_lockfree = true };
  native_type value;
  native_type* address() const volatile {
    return const_cast<native_type*>(&value);
  }
};

template <typename T> struct size_to_atomic<8, T> {
  typedef T value_type;
  typedef __tsan_atomic64 native_type;
  enum { same_size = sizeof(value_type) == sizeof(native_type) };
  enum { is_lockfree = true };
  native_type value;
  native_type* address() const volatile {
    return const_cast<native_type*>(&value);
  }
};

inline void atomic_signal_fence_explicit(memory_order order) {
  __asm__ __volatile__("" ::: "memory");
}

inline void atomic_thread_fence_explicit(memory_order order) {
  __tsan_atomic_thread_fence(tsan_mo(order));
}

template <typename V> struct atomic_load_internal<V, true> {
  V operator()(atomic_t<V> volatile const* p, int order) {
    V rr;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 r = __tsan_atomic8_load(
          (__tsan_atomic8*)p->address(),
          tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 r = __tsan_atomic16_load(
          (__tsan_atomic16*)p->address(),
          tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 r = __tsan_atomic32_load(
          (__tsan_atomic32*)p->address(),
          tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 r = __tsan_atomic64_load(
          (__tsan_atomic64*)p->address(),
          tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return rr;
  }
};

template <typename V> struct atomic_store_internal<V, true> {
  void operator()(atomic_t<V> volatile* p, V value, int order) {
    if (sizeof(*p) == 1) {
      __tsan_atomic8 v = 0;
      memcpy(&v, &value, sizeof(value));
      __tsan_atomic8_store((__tsan_atomic8*)p->address(), v, tsan_mo(order));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 v = 0;
      memcpy(&v, &value, sizeof(value));
      __tsan_atomic16_store((__tsan_atomic16*)p->address(), v, tsan_mo(order));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 v = 0;
      memcpy(&v, &value, sizeof(value));
      __tsan_atomic32_store((__tsan_atomic32*)p->address(), v, tsan_mo(order));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 v = 0;
      memcpy(&v, &value, sizeof(value));
      __tsan_atomic64_store((__tsan_atomic64*)p->address(), v, tsan_mo(order));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
  }
};

template <typename V> struct atomic_fetch_add_internal<V, true> {
  V operator()(atomic_t<V> volatile* p, V diff, int order) {
    V rr;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic8 r = __tsan_atomic8_fetch_add(
          (__tsan_atomic8*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic16 r = __tsan_atomic16_fetch_add(
          (__tsan_atomic16*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic32 r = __tsan_atomic32_fetch_add(
          (__tsan_atomic32*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic64 r = __tsan_atomic64_fetch_add(
          (__tsan_atomic64*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return rr;
  }
};

template <typename V> struct atomic_fetch_and_internal<V, true> {
  V operator()(atomic_t<V> volatile* p, V diff, int order) {
    V rr;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic8 r = __tsan_atomic8_fetch_and(
          (__tsan_atomic8*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic16 r = __tsan_atomic16_fetch_and(
          (__tsan_atomic16*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic32 r = __tsan_atomic32_fetch_and(
          (__tsan_atomic32*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic64 r = __tsan_atomic64_fetch_and(
          (__tsan_atomic64*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return rr;
  }
};

template <typename V> struct atomic_fetch_or_internal<V, true> {
  V operator()(atomic_t<V> volatile* p, V diff, int order) {
    V rr;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic8 r = __tsan_atomic8_fetch_or(
          (__tsan_atomic8*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic16 r = __tsan_atomic16_fetch_or(
          (__tsan_atomic16*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic32 r = __tsan_atomic32_fetch_or(
          (__tsan_atomic32*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic64 r = __tsan_atomic64_fetch_or(
          (__tsan_atomic64*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return rr;
  }
};

template <typename V> struct atomic_fetch_xor_internal<V, true> {
  V operator()(atomic_t<V> volatile* p, V diff, int order) {
    V rr;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic8 r = __tsan_atomic8_fetch_xor(
          (__tsan_atomic8*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic16 r = __tsan_atomic16_fetch_xor(
          (__tsan_atomic16*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic32 r = __tsan_atomic32_fetch_xor(
          (__tsan_atomic32*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic64 r = __tsan_atomic64_fetch_xor(
          (__tsan_atomic64*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return rr;
  }
};

template <typename V> struct atomic_exchange_internal<V, true> {
  V operator()(atomic_t<V> volatile* p, V diff, int order) {
    V rr;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic8 r = __tsan_atomic8_exchange(
          (__tsan_atomic8*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic16 r = __tsan_atomic16_exchange(
          (__tsan_atomic16*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic32 r = __tsan_atomic32_exchange(
          (__tsan_atomic32*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 v = 0;
      memcpy(&v, &diff, sizeof(diff));
      __tsan_atomic64 r = __tsan_atomic64_exchange(
          (__tsan_atomic64*)p->address(), v, tsan_mo(order));
      memcpy(&rr, &r, sizeof(rr));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return rr;
  }
};

template <typename V> struct atomic_compare_exchange_strong_internal<V, true> {
  bool operator()(atomic_t<V> volatile* p, V* expected, V desired, int order) {
    bool r = false;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic8 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic8_compare_exchange_strong(
          (__tsan_atomic8*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic16 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic16_compare_exchange_strong(
          (__tsan_atomic16*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic32 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic32_compare_exchange_strong(
          (__tsan_atomic32*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic64 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic64_compare_exchange_strong(
          (__tsan_atomic64*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return r;
  }
};

template <typename V> struct atomic_compare_exchange_weak_internal<V, true> {
  bool operator()(atomic_t<V> volatile* p, V* expected, V desired, int order) {
    bool r = false;
    if (sizeof(*p) == 1) {
      __tsan_atomic8 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic8 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic8_compare_exchange_weak(
          (__tsan_atomic8*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else if (sizeof(*p) == 2) {
      __tsan_atomic16 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic16 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic16_compare_exchange_weak(
          (__tsan_atomic16*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else if (sizeof(*p) == 4) {
      __tsan_atomic32 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic32 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic32_compare_exchange_weak(
          (__tsan_atomic32*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else if (sizeof(*p) == 8) {
      __tsan_atomic64 cmp = 0;
      memcpy(&cmp, expected, sizeof(*expected));
      __tsan_atomic64 xch = 0;
      memcpy(&xch, &desired, sizeof(desired));
      r = __tsan_atomic64_compare_exchange_weak(
          (__tsan_atomic64*)p->address(), &cmp, xch, tsan_mo(order));
      if (!r)
        memcpy(expected, &cmp, sizeof(*expected));
    } else {
      CONCURRENT_ATOMIC_TSAN_FAIL();
    }
    return r;
  }
};

template <typename V> struct atomic_test_and_set_internal<V, true> {
  inline bool operator()(atomic_t<V> volatile* p, int order) {
    return atomic_test_and_set_from_exchange(p, order);
  }
};

}  // namespace detail
}  // namespace concurrent

#undef CONCURRENT_ATOMIC_TSAN_FAIL

#endif  // CONCURRENT_ATOMIC_TSAN_H_
