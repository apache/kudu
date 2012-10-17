// Copyright 2010 Google Inc. All Rights Reserved.
//
// atomic_*_internal<V,true> specializations and fences
// for the native atomic types on x86+GCC.
//
// See
//   Intel(R)64 and IA-32 Architectures Software Developer's Manual
//   CHAPTER 8 MULTIPLE-PROCESSOR MANAGEMENT
// at http://www.intel.com/products/processor/manuals/
//   AMD64 Architecture Programmer.s Manual
//   Volume 2: System Programming
//   Chapter 7 Memory System
// at http://developer.amd.com/documentation/guides/Pages/default.aspx
// for details.
//
// IWYU pragma: private, include "concurrent/atomic/atomic.h"
// IWYU pragma: friend concurrent/atomic/.*
//
// TODO(user): support GCC < 4.1?

#ifndef CONCURRENT_ATOMIC_X86_GCC_H_
#define CONCURRENT_ATOMIC_X86_GCC_H_

#include <stddef.h>
#include <stdint.h>

#include "gutil/atomicops.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/atomic/forward.h"

namespace concurrent {

namespace detail {
template <int S, typename T> struct size_to_atomic;
template <typename T> struct atomic_t;
template <typename T> struct bits_type;
template <typename T> struct flag_type;
template <typename T> struct int_type;
template <typename V, bool L> struct atomic_compare_exchange_strong_internal;
template <typename V, bool L> struct atomic_compare_exchange_weak_internal;
template <typename V, bool L> struct atomic_exchange_internal;
template <typename V, bool L> struct atomic_fetch_add_internal;
template <typename V, bool L> struct atomic_fetch_and_internal;
template <typename V, bool L> struct atomic_fetch_or_internal;
template <typename V, bool L> struct atomic_fetch_xor_internal;
template <typename V, bool L> struct atomic_load_internal;
template <typename V, bool L> struct atomic_store_internal;
template <typename V, bool L> struct atomic_test_and_set_internal;

// Outlined failure functions:
// Complicated dead code (e.g. LOG() statements in branches never taken)
// can prevent gcc from from inlining otherwise small and simple functions.
void DieOfBadMemoryOrder(int order) ATTRIBUTE_NORETURN;
}  // namespace detail

// Non-standard, non-portable extension
namespace detail {
template <size_t size> struct clobber_helper;

template <> struct clobber_helper<8> {
  static uintptr_t raw() {
    return static_cast<uintptr_t>(0xDEADBEEFBAADF00DULL);
  }
};
template <> struct clobber_helper<4> {
  static uintptr_t raw() {
    return static_cast<uintptr_t>(0xDEADF00DUL);
  }
};
}  // namespace detail
struct clobber : public detail::clobber_helper<sizeof(uintptr_t)> {
  template <typename T> static T* ptr() {
    return reinterpret_cast<T*>(raw());
  }

  static uintptr_t align() { return sizeof(uintptr_t); }
  static uintptr_t mask() { return align() - 1; }

  template <typename T> static bool bad(T* p) {
    return reinterpret_cast<uintptr_t>(p) & mask();
  }
};


namespace detail {

// 'memory_order_rmw_bug' is intended for internal use only,
// and not for end-users.
//
// Only supported by RMW operations and barriers, and only on gcc+x86:
//
//   atomic_RMW(..., memory_order_rmw_bug);
// alone is at least
//   atomic_RMW(..., memory_order_release);
//
//   atomic_thread_fence(memory_order_rmw_bug);
// alone is at least
//   atomic_thread_fence(memory_order_relaxed);
//
// together
//   atomic_RMW(..., memory_order_rmw_bug);
//   atomic_thread_fence(memory_order_rmw_bug);
// is equivalent to
//   atomic_RMW(..., memory_order_acq_rel);
//
// See atomic_thread_fence_rmw_bug() for details.
enum { memory_order_rmw_bug = memory_order_seq_cst + 1 };


// Compiler-specific explicit fences for internal use only.

// Full compiler memory barrier.
// Has no effect on CPU reorderings!
inline void atomic_signal_fence_acq_rel() {
  __asm__ __volatile__("" : : : "memory");
}


// CPU-specific explicit fences for internal use only.
// (Dropped support for outdated CPUs, so right now they are one-liners,
// but this may change in the future with new architectures to support.)

// Release fence.
inline void atomic_thread_fence_release() {
  // TODO(user): find out the right thing to do
  // __asm__ __volatile__("sfence" : : : "memory");
}

// Acquire fence.
inline void atomic_thread_fence_acquire() {
  __asm__ __volatile__("lfence" : : : "memory");
}

// Full memory barrier.
inline void atomic_thread_fence_acq_rel() {
#if defined(__x86_64__)
  __asm__ __volatile__("mfence" : : : "memory");
#else
  int dummy = 0;
  __asm__ __volatile__("xchg %[value], %[mem]"  // implicit "lock" prefix
                       : [value] "=r" (dummy)
                       : [mem] "m" (dummy)
                       : "memory");
#endif
}

// Acquire fence on Opteron, compiler fence otherwise.
//
// On Opteron Rev E non-locked RMW instructions may occasionally see
// the old state after a locked instruction (the lock does not always
// act as an acquire barrier).
// For details see: http://support.amd.com/us/Processor_TechDocs/25759.pdf
// errata 147: Potential Violation of Read Ordering Rules Between
//             Semaphore Operations and Unlocked Read-Modify-Write
//             Instructions
// 'Occasionally' means hundreds of machine days, so misusing
// 'memory_order_acq_rel_rmw_bug' results in bugs that will only
// show up in production and will be very hard to diagnose.
// On the other hand, in CAS loops the fence can be moved out of the loop,
// saving a few cycles (the lfence on Opteron, and a jump on other
// architectures).
// TODO(user): do we need a fence? won't a no-op non-RMW be sufficient?
//               check MSRC001_1023 before setting has_amd_lock_mb_bug?
inline void atomic_thread_fence_rmw_bug() {
  if (AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug) {
    __asm__ __volatile__("lfence" : : : "memory");
  }
}


// Fences based on the current implementation of the atomic operations below.
inline void atomic_signal_fence_explicit(memory_order order) {
  switch (order) {
    case memory_order_seq_cst:
    case memory_order_release:
    case memory_order_acquire:
    case memory_order_acq_rel:
      atomic_signal_fence_acq_rel();
      break;

    case memory_order_relaxed:
      break;

    default:
      detail::DieOfBadMemoryOrder(order);  // Doesn't return.
  }
}


inline void atomic_thread_fence_explicit(int order) {
  switch (order) {
    case memory_order_seq_cst:
      atomic_thread_fence_seq_cst();
      break;

    case memory_order_acquire:
    case memory_order_acq_rel:
      atomic_thread_fence_rmw_bug();
      break;

    case memory_order_release:
      atomic_signal_fence_acq_rel();
      break;

    case memory_order_relaxed:
      break;

    default:
      detail::DieOfBadMemoryOrder(order);  // Doesn't return.
  }
}


// Specializations for native atomic types
//
// TODO(user): check correct alignment as well:
// COMPILE_ASSERT(alignof(value_type) <= alignof(native_type), bad_alignof_T);
//
// We also assume that memcpy(&native_type, &value_type, sizeof(value_type))
// always results in an native_type with a valid bit-pattern.

template <typename T> struct size_to_atomic<1, T> {
  typedef T value_type;
  typedef uint8_t native_type;

  COMPILE_ASSERT(sizeof(value_type) <= sizeof(native_type), bad_sizeof_T);
  enum { same_size = sizeof(value_type) == sizeof(native_type) };

  enum { is_lockfree = true };

  native_type value;

  native_type volatile* address() const volatile {
    return const_cast<native_type volatile*>(&value);
  }
};

template <typename T> struct size_to_atomic<2, T> {
  typedef T value_type;
  typedef uint16_t native_type;

  COMPILE_ASSERT(sizeof(value_type) <= sizeof(native_type), bad_sizeof_T);
  enum { same_size = sizeof(value_type) == sizeof(native_type) };

  enum { is_lockfree = true };

  native_type value;

  native_type volatile* address() const volatile {
    return const_cast<native_type volatile*>(&value);
  }
};

template <typename T> struct size_to_atomic<4, T> {
  typedef T value_type;
  typedef uint32_t native_type;

  COMPILE_ASSERT(sizeof(value_type) <= sizeof(native_type), bad_sizeof_T);
  enum { same_size = sizeof(value_type) == sizeof(native_type) };

  enum { is_lockfree = true };

  native_type value;

  native_type volatile* address() const volatile {
    return const_cast<native_type volatile*>(&value);
  }
};

template <typename T> struct size_to_atomic<8, T> {
  typedef T value_type;
  // TODO(user): http://llvm.org/bugs/show_bug.cgi?id=9041
  typedef /*u*/int64_t native_type;

  COMPILE_ASSERT(sizeof(value_type) <= sizeof(native_type), bad_sizeof_T);
  enum { same_size = sizeof(value_type) == sizeof(native_type) };

  enum { is_lockfree = true };

  native_type value;

  native_type volatile* address() const volatile {
    return const_cast<native_type volatile*>(&value);
  }
};
#if !defined(__x86_64__)
typedef size_to_atomic<8, int64_t>::native_type native_64_t;
#endif

// Built-in types:
template <class T> struct builtin_type { enum { value = false }; };

template <> struct builtin_type<unsigned char> { enum { value = true }; };
template <> struct builtin_type<signed char> { enum { value = true }; };
template <> struct builtin_type<unsigned short> { enum { value = true }; };
template <> struct builtin_type<signed short> { enum { value = true }; };
template <> struct builtin_type<unsigned int> { enum { value = true }; };
template <> struct builtin_type<signed int> { enum { value = true }; };
template <> struct builtin_type<unsigned long> { enum { value = true }; };
template <> struct builtin_type<signed long> { enum { value = true }; };
template <> struct builtin_type<unsigned long long> { enum { value = true }; };
template <> struct builtin_type<signed long long> { enum { value = true }; };

template <> struct builtin_type<char> { enum { value = true }; };
template <> struct builtin_type<wchar_t> { enum { value = true }; };

template <> struct builtin_type<bool> { enum { value = true }; };

template <typename T> struct builtin_type<T*> { enum { value = true }; };

// Enable fetch_add()/fetch_sub() for these types:
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

// Enable fetch_and()/fetch_or()/fetch_xor() for these types:
template <> struct bits_type<unsigned char> { enum { value = true }; };
template <> struct bits_type<unsigned short> { enum { value = true }; };
template <> struct bits_type<unsigned int> { enum { value = true }; };
template <> struct bits_type<unsigned long> { enum { value = true }; };
template <> struct bits_type<unsigned long long> { enum { value = true }; };

template <> struct bits_type<bool> { enum { value = true }; };

// Enable test_and_set(), clear() for these types:
template <> struct flag_type<bool> { enum { value = true }; };

// 8/16/32/64 CAS

// Release semantics on Opteron and a full barrier otherwise.
// TODO(user): see TODO at atomic_thread_fence_rmw_bug() above:
//               is the "mov" enough to prevent the bug?
#if defined(__x86_64__)
template <typename V>
inline bool atomic_compare_exchange_strong_rmw_bug_native(
                                  typename atomic_t<V>::native_type volatile* p,
                                  V* expected,
                                  V desired) {
  bool success;
  // If we return the success value in something other than a bool,
  // GCC will do an unnecessary TESTB & SETE (& possibly MOVZB) as
  // a result. But if we want to set a correct bool value, we need
  // to do (or not) a MOVZB ourselves depending on sizeof(bool).
  // TODO(user): is bool non-8 bit ever?
  if (sizeof(success) == sizeof(uint8_t)) {
    // CMPXCHG needs '*expected' in the accumulator, and GCC needs
    // the success/failure return value in the accumulator on exit,
    // so we need to return (the possibly modified) '*expected'
    // value in another register/memory location. Hopefully we are
    // still better off than if we have loaded '*expected' into
    // the accumulator ourselves (as the compiler may already have
    // it there), or if we have returned the success status in
    // some other place (the compiler needs to move it to the
    // accumulator anyway).
    __asm__ __volatile__("lock; cmpxchg %[desired], %[mem]; "
                         "mov %[expected], %[expected_out]; "
                         "sete %[success]"
                         : [success] "=a" (success),
                           [expected_out] "=rm" (*expected)
                         : [expected] "a" (*expected),
                           [desired] "q" (desired),
                           [mem] "m" (*p)
                         : "memory", "cc");
  } else {
    __asm__ __volatile__("lock; cmpxchg %[desired], %[mem]; "
                         "mov %[expected], %[expected_out]; "
                         "sete %%al; movzb %%al, %[success]"
                         : [success] "=a" (success),
                           [expected_out] "=rm" (*expected)
                         : [expected] "a" (*expected),
                           [desired] "q" (desired),
                           [mem] "m" (*p)
                         : "memory", "cc");
  }
  return success;
}

#else
// TODO(user): http://gcc.gnu.org/bugzilla/show_bug.cgi?id=19005
template <typename V>
inline bool atomic_compare_exchange_strong_rmw_bug_native(
                                  typename atomic_t<V>::native_type volatile* p,
                                  V* expected,
                                  V desired) {
  bool success;
  if (sizeof(success) == sizeof(uint8_t)) {
    __asm__ __volatile__("lock; cmpxchg %[desired], %[mem]; "
                         "mov %[expected], %[expected_out]; "
                         "sete %[success]"
                         : [success] "=a" (success),
                           [expected_out] "=qm" (*expected)
                         : [expected] "a" (*expected),
                           [desired] "q" (desired),
                           [mem] "m" (*p)
                         : "memory", "cc");
  } else {
    __asm__ __volatile__("lock; cmpxchg %[desired], %[mem]; "
                         "mov %[expected], %[expected_out]; "
                         "sete %%al; movzb %%al, %[success]"
                         : [success] "=a" (success),
                           [expected_out] "=qm" (*expected)
                         : [expected] "a" (*expected),
                           [desired] "q" (desired),
                           [mem] "m" (*p)
                         : "memory", "cc");
  }
  return success;
}

template <typename V>
inline bool atomic_compare_exchange_strong_rmw_bug_native(
                                  native_64_t volatile* p,
                                  V* expected,
                                  V desired) {
  native_64_t native_expected = 0;
  memcpy(&native_expected, expected, sizeof(V));
  native_64_t native_desired = 0;
  memcpy(&native_desired, &desired, sizeof(V));
  native_64_t const found =
      __sync_val_compare_and_swap(p, native_expected, native_desired);
  if (found == native_expected) return true;
  memcpy(expected, &found, sizeof(V));
  return false;
}
#endif

template <typename V>
inline bool atomic_compare_exchange_strong_rmw_bug(
                                  typename atomic_t<V>::native_type volatile* p,
                                  V* expected,
                                  V desired) {
  typedef typename atomic_t<V>::native_type native_type;
  if (atomic_t<V>::same_size) {
    // We can simply reinterpret the arguments as the native atomic type
    return atomic_compare_exchange_strong_rmw_bug_native(p, expected, desired);
  } else {
    // Need to pad the values to the size of the native atomic type.
    // The padding *must* be a fixed bit pattern (0 here) even for the weak
    // variant: "the underlying type" in 29.6 [atomics.types.operations]
    // Note 24 refers to 'value_type', not to 'native_type'.
    // (The padding isn't returned via the updated 'expected' value,
    // so compare_exchange_weak() would never converge.)
    native_type native_expected = 0;
    memcpy(&native_expected, expected, sizeof(V));
    native_type native_desired = 0;
    memcpy(&native_desired, &desired, sizeof(V));
    bool const s = atomic_compare_exchange_strong_rmw_bug_native
        (p, &native_expected, native_desired);
    if (!s)
      memcpy(expected, &native_expected, sizeof(V));
    return s;
  }
}

template <typename V> struct atomic_compare_exchange_strong_internal<V, true> {
  inline bool operator()(atomic_t<V> volatile* p,
                         V* expected,
                         V desired,
                         int order) {
    switch (order) {
      case memory_order_relaxed:
      case memory_order_release:
      case memory_order_rmw_bug:
        return atomic_compare_exchange_strong_rmw_bug
            (p->address(), expected, desired);

      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_acquire:
      case memory_order_acq_rel:
      case memory_order_seq_cst: {
        bool const s = atomic_compare_exchange_strong_rmw_bug
            (p->address(), expected, desired);
        atomic_thread_fence_rmw_bug();
        return s;
      }
    }
  }
};

template <typename V> struct atomic_compare_exchange_weak_internal<V, true>
    : public atomic_compare_exchange_strong_internal<V, true> {
};


// Atomic8/16/32/64 load

template <typename N>
inline N atomic_load_native(N volatile* p) {
  return *p;
}

#if !defined(__x86_64__)
inline native_64_t atomic_load_native(native_64_t volatile* p) {
  native_64_t native_value;
  __asm__ __volatile__("movq %[mem], %%mm0; "
                       "movq %%mm0, %[value]; "
                       "emms"
                       : [value] "=m" (native_value)
                       : [mem] "m" (*p)
                       : "st", "st(1)", "st(2)", "st(3)",
                         "st(4)", "st(5)", "st(6)", "st(7)",
                         "mm0", "mm1", "mm2", "mm3",
                         "mm4", "mm5", "mm6", "mm7",
                         "memory");
  return native_value;
}
#endif

// Compiler memory_order_relaxed.
// CPU memory_order_acquire (except related to some SSE instructions).
template <typename V, bool builtin> struct atomic_load;

template <typename V> struct atomic_load<V, true> {
  inline V operator()(typename atomic_t<V>::native_type volatile const* p) {
    COMPILE_ASSERT(atomic_t<V>::same_size, builtin_with_nonsupported_size);
    return static_cast<V>(atomic_load_native(p));
  }
};
template <typename V> struct atomic_load<V, false> {
  inline V operator()(typename atomic_t<V>::native_type volatile const* p) {
    typename atomic_t<V>::native_type const tmp = atomic_load_native(p);
    V r;
    memcpy(&r, &tmp, sizeof(r));
    return r;
  }
};

template <typename V> struct atomic_load_internal<V, true> {
  inline V operator()(atomic_t<V> volatile const* p, int order) {
    switch (order) {
      case memory_order_acq_rel:
      case memory_order_release:
      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_seq_cst: {
        // See:
        // http://www.justsoftwaresolutions.co.uk/threading/intel-memory-ordering-and-c%2B%2B-memory-model.html
        // http://www.hpl.hp.com/personal/Hans_Boehm/c%2B%2Bmm/seq_con.html
        atomic_signal_fence_acq_rel();
        V const tmp(atomic_load<V, builtin_type<V>::value>()(p->address()));
        atomic_signal_fence_acq_rel();   // compiler 'lfence'
        return tmp;
      }

      case memory_order_relaxed:
        return atomic_load<V, builtin_type<V>::value>()(p->address());

      case memory_order_acquire: {
        V const tmp(atomic_load<V, builtin_type<V>::value>()(p->address()));
        atomic_signal_fence_acq_rel();   // compiler 'lfence'
        return tmp;
      }
    }
  }
};


// 8/16/32/64 Exchange

// Release semantics on Opteron and a full barrier otherwise.
template <typename V>
inline V atomic_exchange_rmw_bug_native(
                                  typename atomic_t<V>::native_type volatile* p,
                                  V desired) {
  __asm__ __volatile__("xchg %[value], %[mem]"  // implicit "lock" prefix
                       : [value] "=r" (desired)
                       : [mem] "m" (*p),
                         "0" (desired)
                       : "memory");
  return desired;
}

#if !defined(__x86_64__)
template <typename V>
inline V atomic_exchange_rmw_bug_native(native_64_t volatile* p,
                                        V desired) {
  native_64_t native_expected = 0;
  memcpy(&native_expected, const_cast<native_64_t*>(p), sizeof(V));
  native_64_t native_desired = 0;
  memcpy(&native_desired, &desired, sizeof(V));
  for (;;) {
    native_64_t const found =
        __sync_val_compare_and_swap(p, native_expected, native_desired);
    if (found == native_expected) {
      V tmp;
      memcpy(&tmp, &found, sizeof(V));
      return tmp;
    }
    memcpy(&native_expected, &found, sizeof(native_64_t));
  }
}
#endif

template <typename V>
inline V atomic_exchange_rmw_bug(typename atomic_t<V>::native_type volatile* p,
                                 V desired) {
  typedef typename atomic_t<V>::native_type native_type;
  if (atomic_t<V>::same_size) {
    // We can simply reinterpret the arguments as the native atomic type
    return atomic_exchange_rmw_bug_native(p, desired);
  } else {
    native_type native_desired = 0;
    memcpy(&native_desired, &desired, sizeof(desired));
    native_desired = atomic_exchange_rmw_bug_native(p, native_desired);
    memcpy(&desired, &native_desired, sizeof(desired));
    return desired;
  }
}

template <typename V> struct atomic_exchange_internal<V, true> {
  inline V operator()(atomic_t<V> volatile* p, V desired, int order) {
    switch (order) {
      case memory_order_relaxed:
      case memory_order_release:
      case memory_order_rmw_bug:
        return atomic_exchange_rmw_bug(p->address(), desired);

      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_acquire:
      case memory_order_acq_rel:
      case memory_order_seq_cst:
        desired = atomic_exchange_rmw_bug(p->address(), desired);
        atomic_thread_fence_rmw_bug();
        return desired;
    }
  }
};


// 8/16/32/64 Store

template <typename V>
inline void atomic_store_native(typename atomic_t<V>::native_type volatile* p,
                                V value) {
  *p = value;
}

#if !defined(__x86_64__)
template <typename V>
inline void atomic_store_native(native_64_t volatile* p, V value) {
  native_64_t native_value = 0;
  memcpy(&native_value, &value, sizeof(V));
  __asm__ __volatile__("movq %[value], %%mm0; "
                       "movq %%mm0, %[mem]; "
                       "emms"
                       : [mem] "=m" (*p)
                       : [value] "m" (native_value)
                       : "st", "st(1)", "st(2)", "st(3)",
                         "st(4)", "st(5)", "st(6)", "st(7)",
                         "mm0", "mm1", "mm2", "mm3",
                         "mm4", "mm5", "mm6", "mm7",
                         "memory");
}
#endif

// Compiler memory_order_relaxed.
// CPU memory_order_release (except related to some SSE instructions).
template <typename V, bool builtin> struct atomic_store;

template <typename V> struct atomic_store<V, true> {
  inline void operator()(typename atomic_t<V>::native_type volatile* p,
                         V value) {
    COMPILE_ASSERT(atomic_t<V>::same_size, builtin_with_nonsupported_size);
    atomic_store_native(p,
                        static_cast<typename atomic_t<V>::native_type>(value));
  }
};
template <typename V> struct atomic_store<V, false> {
  inline void operator()(typename atomic_t<V>::native_type volatile* p,
                         V value) {
    if (atomic_t<V>::same_size) {
      typename atomic_t<V>::native_type tmp;
      memcpy(&tmp, &value, sizeof(value));
      atomic_store_native(p, tmp);
    } else {
      typename atomic_t<V>::native_type tmp = 0;
      memcpy(&tmp, &value, sizeof(value));
      atomic_store_native(p, tmp);
    }
  }
};

template <typename V> struct atomic_store_internal<V, true> {
  inline void operator()(atomic_t<V> volatile* p, V value, int order) {
    switch (order) {
      case memory_order_seq_cst:
        atomic_signal_fence_acq_rel();   // compiler 'sfence'
        atomic_store<V, builtin_type<V>::value>()(p->address(), value);
        atomic_thread_fence_acq_rel();
        break;

      case memory_order_release:
        atomic_signal_fence_acq_rel();   // compiler 'sfence'
      case memory_order_relaxed:
        atomic_store<V, builtin_type<V>::value>()(p->address(), value);
        break;

      case memory_order_acq_rel:
      case memory_order_acquire:
      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.
    }
  }
};


// 8/16/32/64 +=

// Release semantics on Opteron and a full barrier otherwise.
template <typename V>
inline V atomic_fetch_add_rmw_bug(typename atomic_t<V>::native_type volatile* p,
                                  V diff) {
  __asm__ __volatile__("lock; xadd %[diff],%[mem]"
                       : "=r" (diff)
                       : [diff] "0" (diff),
                         [mem] "m" (*p)
                       : "memory", "cc");
  return diff;
}

#if !defined(__x86_64__)
template <typename V>
inline V atomic_fetch_add_rmw_bug(native_64_t volatile* p, V diff) {
  native_64_t native_diff = 0;
  memcpy(&native_diff, &diff, sizeof(V));
  native_64_t const old = __sync_fetch_and_add(p, native_diff);
  V tmp;
  memcpy(&tmp, &old, sizeof(V));
  return tmp;
}
#endif

template <typename V> struct atomic_fetch_add_internal<V, true> {
  inline V operator()(atomic_t<V> volatile* p, V diff, int order) {
    switch (order) {
      case memory_order_relaxed:
      case memory_order_release:
      case memory_order_rmw_bug:
        return atomic_fetch_add_rmw_bug(p->address(), diff);

      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_acquire:
      case memory_order_acq_rel:
      case memory_order_seq_cst:
        diff = atomic_fetch_add_rmw_bug(p->address(), diff);
        atomic_thread_fence_rmw_bug();
        return diff;
    }
  }
};


// Atomic8/16/32/64 Test and Set

template <typename V> struct atomic_test_and_set_internal<V, true> {
  inline bool operator()(atomic_t<V> volatile* p, int order) {
    return atomic_test_and_set_from_exchange(p, order);
  }
};


// 8/16/32/64 &=

template <typename V> struct atomic_fetch_and_internal<V, true> {
  inline V operator()(atomic_t<V> volatile* p, V bits, int order) {
    switch (order) {
      case memory_order_relaxed:
      case memory_order_release:
      case memory_order_rmw_bug:
        return atomic_fetch_and_from_compare_exchange
            (p, bits, memory_order_rmw_bug);

      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_acquire:
      case memory_order_acq_rel:
      case memory_order_seq_cst:
        bits = atomic_fetch_and_from_compare_exchange
            (p, bits, memory_order_rmw_bug);
        atomic_thread_fence_rmw_bug();
        return bits;
    }
  }
};



// 8/16/32/64 |=

template <typename V> struct atomic_fetch_or_internal<V, true> {
  inline V operator()(atomic_t<V> volatile* p, V bits, int order) {
    switch (order) {
      case memory_order_relaxed:
      case memory_order_release:
      case memory_order_rmw_bug:
        return atomic_fetch_or_from_compare_exchange
            (p, bits, memory_order_rmw_bug);

      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_acquire:
      case memory_order_acq_rel:
      case memory_order_seq_cst:
        bits = atomic_fetch_or_from_compare_exchange
            (p, bits, memory_order_rmw_bug);
        atomic_thread_fence_rmw_bug();
        return bits;
    }
  }
};



// 8/16/32/64 ^=

template <typename V> struct atomic_fetch_xor_internal<V, true> {
  inline V operator()(atomic_t<V> volatile* p, V bits, int order) {
    switch (order) {
      case memory_order_relaxed:
      case memory_order_release:
      case memory_order_rmw_bug:
        return atomic_fetch_xor_from_compare_exchange
            (p, bits, memory_order_rmw_bug);

      default:
        detail::DieOfBadMemoryOrder(order);  // Doesn't return.

      case memory_order_acquire:
      case memory_order_acq_rel:
      case memory_order_seq_cst:
        bits = atomic_fetch_xor_from_compare_exchange
            (p, bits, memory_order_rmw_bug);
        atomic_thread_fence_rmw_bug();
        return bits;
    }
  }
};



}  // namespace detail
}  // namespace concurrent

#endif  // CONCURRENT_ATOMIC_X86_GCC_H_
