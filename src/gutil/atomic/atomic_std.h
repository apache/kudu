// Copyright 2012 Google Inc. All Rights Reserved.

// Atomic operations for non-x86 platforms.
//
// This version uses libstdc++ atomic implementation, which is slightly
// different from //concurrent/atomic ABI(only for X86).
// The differences are (not limited to),
// 1) Enum memory_order has member memory_order_consume, which was
//    commented out in //concurrent/atomic/atomic.h(x86).
// 2) There are no extensions for atomic_bool. test_and_set(), fetch_and(),
//    fetch_or(), fetch_xor(), clear() are not supported by atomic_bool.
// 3) There is no public value_type type member in atomic types.
// 4) In gcc-4.6 libstdc++, generic atomic<T> does not implement
//    is_lock_free(), load(), store(), exchange(),
//    compare_exchange_strong(), etc functions. These functions
//    are implemented in gcc-4.7 and up.

#ifndef CONCURRENT_ATOMIC_ATOMIC_STD_H_
#define CONCURRENT_ATOMIC_ATOMIC_STD_H_

// It is generally not permitted to use <atomic>.
// Exemption granted for //concurrent/atomic.
#define OKAY_FOR_IMPLEMENTATION_OF_CONCURRENT_ATOMIC
#include <atomic>

namespace concurrent {

using std::memory_order_relaxed;
// Commented out to match x86 implementation.
// using std::memory_order_consume;
using std::memory_order_acquire;
using std::memory_order_release;
using std::memory_order_acq_rel;
using std::memory_order_seq_cst;
using std::memory_order;

using std::atomic_flag;
using std::atomic_bool;
using std::atomic_char;
using std::atomic_schar;
using std::atomic_uchar;
using std::atomic_short;
using std::atomic_ushort;
using std::atomic_int;
using std::atomic_uint;
using std::atomic_long;
using std::atomic_ulong;
using std::atomic_llong;
using std::atomic_ullong;

// Provided here to match x86 implementation.
typedef std::atomic<uint8_t> atomic_uint8_t;
typedef std::atomic<int8_t> atomic_int8_t;
typedef std::atomic<uint16_t> atomic_uint16_t;
typedef std::atomic<int16_t> atomic_int16_t;
typedef std::atomic<uint32_t> atomic_uint32_t;
typedef std::atomic<int32_t> atomic_int32_t;
typedef std::atomic<uint64_t> atomic_uint64_t;
typedef std::atomic<int64_t> atomic_int64_t;

// Commented out to match x86 implementation.
// using std::atomic_char16_t;
// using std::atomic_char32_t;
using std::atomic_wchar_t;

using std::atomic_uint_least8_t;
using std::atomic_int_least8_t;
using std::atomic_uint_least16_t;
using std::atomic_int_least16_t;
using std::atomic_uint_least32_t;
using std::atomic_int_least32_t;
using std::atomic_uint_least64_t;
using std::atomic_int_least64_t;

using std::atomic_uint_fast8_t;
using std::atomic_int_fast8_t;
using std::atomic_uint_fast16_t;
using std::atomic_int_fast16_t;
using std::atomic_uint_fast32_t;
using std::atomic_int_fast32_t;
using std::atomic_uint_fast64_t;
using std::atomic_int_fast64_t;

using std::atomic_intptr_t;
using std::atomic_uintptr_t;
using std::atomic_size_t;
using std::atomic_ptrdiff_t;
using std::atomic_intmax_t;
using std::atomic_uintmax_t;

// atomic_address was removed from C++11. Provided to match x86
// implementation.
typedef std::atomic<void*> atomic_address;

using std::atomic;

using std::atomic_thread_fence;
using std::atomic_signal_fence;

}  // namespace concurrent

#endif  // CONCURRENT_ATOMIC_ATOMIC_STD_H_
