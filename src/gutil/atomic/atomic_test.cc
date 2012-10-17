// Copyright 2010 Google Inc. All Rights Reserved.

#include "gutil/atomic/atomic.h"

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;

#include "gutil/atomicops.h"
#include "base/callback.h"
#include <glog/logging.h>
#include "gutil/logging-inl.h"
#include "base/synchronization.h"
#include "testing/base/public/benchmark.h"
#include "testing/base/public/gunit.h"
#include "third_party/boost/allowed/ptr_container/ptr_vector.hpp"
#include "thread/thread.h"

namespace concurrent {

namespace {


TEST(BoolTest, Lockfree) {
  atomic_bool a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(BoolTest, LoadStoreSanity) {
  atomic_bool a(true);
  EXPECT_EQ(true, a.load(memory_order_relaxed));
  a.store(false, memory_order_release);
  EXPECT_EQ(false, a.load(memory_order_acquire));
}

TEST(BoolTest, ExchangeSanity) {
  atomic_bool a(true);
  EXPECT_EQ(true, a.exchange(false, memory_order_acq_rel));
  EXPECT_EQ(false, a.load(memory_order_acquire));
}

TEST(BoolTest, CompareExchangeSanity) {
  atomic_bool a(true);
  bool e = false;
  EXPECT_FALSE(a.compare_exchange_strong(e, false, memory_order_acq_rel));
  EXPECT_EQ(true, a.load(memory_order_acquire));
  EXPECT_EQ(true, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, false, memory_order_acq_rel));
  EXPECT_EQ(false, a.load(memory_order_acquire));
  EXPECT_EQ(true, e);
}

// Enable the following tests if using //concurrent/atomic X86
// implementation, which extends standard, by adding fetch_and(),
// fetch_or(), fetch_xor(), test_and_set(), clear() support to
// atomic_bool.
#ifdef CONCURRENT_ATOMIC_BOOL_EXTRA_METHODS
TEST(BoolTest, TestAndSetSanity) {
  atomic_bool a(true);
  EXPECT_EQ(true, a.test_and_set(memory_order_acq_rel));
  EXPECT_EQ(true, a.load(memory_order_acquire));
  a.clear(memory_order_release);
  EXPECT_EQ(false, a.load(memory_order_acquire));
  EXPECT_EQ(false, a.test_and_set(memory_order_acq_rel));
  EXPECT_EQ(true, a.load(memory_order_acquire));
}

TEST(BoolTest, FetchBitSanity) {
  atomic_bool a(true);
  EXPECT_EQ(true, a.fetch_and(true, memory_order_acq_rel));
  EXPECT_EQ(true, a.fetch_and(false, memory_order_acq_rel));
  EXPECT_EQ(false, a.load(memory_order_acquire));
  EXPECT_EQ(false, a.fetch_or(false, memory_order_acq_rel));
  EXPECT_EQ(false, a.fetch_or(true, memory_order_acq_rel));
  EXPECT_EQ(true, a.load(memory_order_acquire));
  EXPECT_EQ(true, a.fetch_xor(false, memory_order_acq_rel));
  EXPECT_EQ(true, a.fetch_xor(true, memory_order_acq_rel));
  EXPECT_EQ(false, a.load(memory_order_acquire));
}
#endif  // CONCURRENT_ATOMIC_BOOL_EXTRA_METHODS


TEST(VoidTest, Lockfree) {
  atomic_address a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(VoidTest, LoadStoreSanity) {
  char c[2];
  void* const one = c+0;
  void* const two = c+1;
  atomic_address a(one);
  EXPECT_EQ(one, a.load(memory_order_relaxed));
  a.store(two, memory_order_release);
  EXPECT_EQ(two, a.load(memory_order_acquire));
}

TEST(VoidTest, ExchangeSanity) {
  char c[2];
  void* const one = c+0;
  void* const two = c+1;
  atomic_address a(one);
  EXPECT_EQ(one, a.exchange(two, memory_order_acq_rel));
  EXPECT_EQ(two, a.load(memory_order_acquire));
}

TEST(VoidTest, CompareExchangeSanity) {
  char c[2];
  void* const one = c+0;
  void* const two = c+1;
  atomic_address a(one);
  void* e = two;
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(one, a.load(memory_order_acquire));
  EXPECT_EQ(one, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(two, a.load(memory_order_acquire));
  EXPECT_EQ(one, e);
}

TEST(VoidTest, FetchAddSanity) {
  char c[3];
  void* const one = c+0;
  void* const two = c+1;
  void* const three = c+2;
  atomic_address a(two);
  EXPECT_EQ(two, a.fetch_add(0, memory_order_acq_rel));
  EXPECT_EQ(two, a.load(memory_order_acquire));
  EXPECT_EQ(two, a.fetch_add(1, memory_order_acq_rel));
  EXPECT_EQ(three, a.load(memory_order_acquire));
  EXPECT_EQ(three, a.fetch_add(-2, memory_order_acq_rel));
  EXPECT_EQ(one, a.load(memory_order_acquire));
}


template <typename T> struct PointerTest : public testing::Test {};
typedef testing::Types<
          char,
          char const ,
          unsigned int,
          unsigned int const,
          long long,
          long long const
        > PointerTypes;
TYPED_TEST_CASE(PointerTest, PointerTypes);

TYPED_TEST(PointerTest, Lockfree) {
  atomic<TypeParam*> a;
  EXPECT_TRUE(a.is_lock_free());
}

TYPED_TEST(PointerTest, LoadStoreSanity) {
  TypeParam c[2] = {};
  TypeParam* const one = c+0;
  TypeParam* const two = c+2;
  atomic<TypeParam*> a(one);
  EXPECT_EQ(one, a.load(memory_order_relaxed));
  a.store(two, memory_order_release);
  EXPECT_EQ(two, a.load(memory_order_acquire));
}

TYPED_TEST(PointerTest, ExchangeSanity) {
  TypeParam c[2] = {};
  TypeParam* const one = c+0;
  TypeParam* const two = c+2;
  atomic<TypeParam*> a(one);
  EXPECT_EQ(one, a.exchange(two, memory_order_acq_rel));
  EXPECT_EQ(two, a.load(memory_order_acquire));
}

TYPED_TEST(PointerTest, CompareExchangeSanity) {
  TypeParam c[2] = {};
  TypeParam* const one = c+0;
  TypeParam* const two = c+2;
  atomic<TypeParam*> a(one);
  TypeParam* e = two;
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(one, a.load(memory_order_acquire));
  EXPECT_EQ(one, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(two, a.load(memory_order_acquire));
  EXPECT_EQ(one, e);
}

TYPED_TEST(PointerTest, FetchAddSanity) {
  TypeParam c[3] = {};
  TypeParam* const one = c+0;
  TypeParam* const two = c+1;
  TypeParam* const three = c+2;
  atomic<TypeParam*> a(two);
  EXPECT_EQ(two, a.fetch_add(0, memory_order_acq_rel));
  EXPECT_EQ(two, a.load(memory_order_acquire));
  EXPECT_EQ(two, a.fetch_add(1, memory_order_acq_rel));
  EXPECT_EQ(three, a.load(memory_order_acquire));
  EXPECT_EQ(three, a.fetch_add(-2, memory_order_acq_rel));
  EXPECT_EQ(one, a.load(memory_order_acquire));
}


template <typename T> struct IntegerTest : public testing::Test {};
typedef testing::Types<
          char,
          signed char,
          short,
          int,
          long,
          long long,

          int8_t,
          int16_t,
          int32_t,
          int64_t,
          wchar_t,

          int_least8_t,
          int_least16_t,
          int_least32_t,
          int_least64_t,

          int_fast8_t,
          int_fast16_t,
          int_fast32_t,
          int_fast64_t,

          intptr_t,
          ptrdiff_t,
          intmax_t
        > IntegerTypes;
TYPED_TEST_CASE(IntegerTest, IntegerTypes);

TYPED_TEST(IntegerTest, Lockfree) {
  atomic<TypeParam> a;
  EXPECT_TRUE(a.is_lock_free());
}

TYPED_TEST(IntegerTest, LoadStoreSanity) {
  atomic<TypeParam> a(1);
  EXPECT_EQ(1, a.load(memory_order_relaxed));
  a.store(-2, memory_order_release);
  EXPECT_EQ(static_cast<TypeParam>(-2),
            a.load(memory_order_acquire));
}

TYPED_TEST(IntegerTest, ExchangeSanity) {
  atomic<TypeParam> a(1);
  EXPECT_EQ(1, a.exchange(-2, memory_order_acq_rel));
  EXPECT_EQ(static_cast<TypeParam>(-2),
            a.load(memory_order_acquire));
}

TYPED_TEST(IntegerTest, CompareExchangeSanity) {
  atomic<TypeParam> a(1);
  TypeParam e = -1;
  EXPECT_FALSE(a.compare_exchange_strong(e, 2, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, -2, memory_order_acq_rel));
  EXPECT_EQ(static_cast<TypeParam>(-2),
            a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
}

TYPED_TEST(IntegerTest, FetchAddSanity) {
  atomic<TypeParam> a(2);
  EXPECT_EQ(2, a.fetch_add(0, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire));
  EXPECT_EQ(2, a.fetch_add(1, memory_order_acq_rel));
  EXPECT_EQ(3, a.load(memory_order_acquire));
  EXPECT_EQ(3, a.fetch_add(-2, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire));
}


template <typename T> struct BitsTest : public testing::Test {};
typedef testing::Types<
          unsigned char,
          unsigned short,
          unsigned int,
          unsigned long,
          unsigned long long,

          uint8_t,
          uint16_t,
          uint32_t,
          uint64_t,

          uint_least8_t,
          uint_least16_t,
          uint_least32_t,
          uint_least64_t,

          uint_fast8_t,
          uint_fast16_t,
          uint_fast32_t,
          uint_fast64_t,

          uintptr_t,
          size_t,
          uintmax_t
        > BitsTypes;
TYPED_TEST_CASE(BitsTest, BitsTypes);

TYPED_TEST(BitsTest, Lockfree) {
  atomic<TypeParam> a;
  EXPECT_TRUE(a.is_lock_free());
}

TYPED_TEST(BitsTest, LoadStoreSanity) {
  atomic<TypeParam> a(1);
  EXPECT_EQ(1, a.load(memory_order_relaxed));
  a.store(-2, memory_order_release);
  EXPECT_EQ(static_cast<TypeParam>(-2),
            a.load(memory_order_acquire));
}

TYPED_TEST(BitsTest, ExchangeSanity) {
  atomic<TypeParam> a(1);
  EXPECT_EQ(1, a.exchange(-2, memory_order_acq_rel));
  EXPECT_EQ(static_cast<TypeParam>(-2),
            a.load(memory_order_acquire));
}

TYPED_TEST(BitsTest, CompareExchangeSanity) {
  atomic<TypeParam> a(1);
  TypeParam e = -1;
  EXPECT_FALSE(a.compare_exchange_strong(e, 2, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, -2, memory_order_acq_rel));
  EXPECT_EQ(static_cast<TypeParam>(-2),
            a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
}

TYPED_TEST(BitsTest, FetchAddSanity) {
  atomic<TypeParam> a(2);
  EXPECT_EQ(2, a.fetch_add(0, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire));
  EXPECT_EQ(2, a.fetch_add(1, memory_order_acq_rel));
  EXPECT_EQ(3, a.load(memory_order_acquire));
  EXPECT_EQ(3, a.fetch_add(-2, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire));
}

TYPED_TEST(BitsTest, FetchBitSanity) {
  atomic<TypeParam> a(0x70);
  EXPECT_EQ(0x70, a.fetch_and(0x33, memory_order_acq_rel));
  EXPECT_EQ(0x30, a.load(memory_order_acquire));
  EXPECT_EQ(0x30, a.fetch_or(0x11, memory_order_acq_rel));
  EXPECT_EQ(0x31, a.load(memory_order_acquire));
  EXPECT_EQ(0x31, a.fetch_xor(0x22, memory_order_acq_rel));
  EXPECT_EQ(0x13, a.load(memory_order_acquire));
}


// Disable the following tests if using GCC-4.6(and older) libstdc++
// <atomic> implementation. In gcc-4.6 and older libstdc++, generic
// atomic<T> does not implement is_lock_free(), load(), store(),
// exchange(), compare_exchange_strong(), etc functions. These functions
// are implemented in gcc-4.7 and up libstdc++ <atomic>.
// Notice that Clang sets GNUC version to 4.2, and there is no way
// to tell the exact libstdc++ version. We check GLIBC version for Clang.
// In that, gcc-4.7(including libstdc++) is paired with
// GRTEv3(eglibc2.15), and gcc-4.6(including libstdc++) is paired with
// GRTEv2(eglibc2.11).
#if !defined(_GLIBCXX_ATOMIC) || \
    __GNUC_PREREQ(4,7) || \
    (defined(__clang__) && __GLIBC_PREREQ(2,15))
struct locking {
  int i[16];
};

TEST(LockingTest, Lockfree) {
  atomic<locking> a;
  EXPECT_FALSE(a.is_lock_free());
}

TEST(LockingTest, LoadStoreSanity) {
  locking one = { { 1 } };
  locking two = { { 2 } };
  atomic<locking> a(one);
  EXPECT_EQ(1, a.load(memory_order_relaxed).i[0]);
  a.store(two, memory_order_release);
  EXPECT_EQ(2, a.load(memory_order_acquire).i[0]);
}

TEST(LockingTest, ExchangeSanity) {
  locking one = { { 1 } };
  locking two = { { 2 } };
  atomic<locking> a(one);
  EXPECT_EQ(1, a.exchange(two, memory_order_acq_rel).i[0]);
  EXPECT_EQ(2, a.load(memory_order_acquire).i[0]);
}

TEST(LockingTest, CompareExchangeSanity) {
  locking one = { { 1 } };
  locking two = { { 2 } };
  atomic<locking> a(one);
  locking e = { { 0 } };
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire).i[0]);
  EXPECT_EQ(1, e.i[0]);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire).i[0]);
  EXPECT_EQ(1, e.i[0]);
}

struct twoints {
  int x;
  int y;
};

TEST(TwointsTest, Lockfree) {
  atomic<twoints> a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(TwointsTest, LoadStoreSanity) {
  twoints one = { 1 };
  twoints two = { 2 };
  atomic<twoints> a(one);
  EXPECT_EQ(1, a.load(memory_order_relaxed).x);
  a.store(two, memory_order_release);
  EXPECT_EQ(2, a.load(memory_order_acquire).x);
}

TEST(TwointsTest, ExchangeSanity) {
  twoints one = { 1 };
  twoints two = { 2 };
  atomic<twoints> a(one);
  EXPECT_EQ(1, a.exchange(two, memory_order_acq_rel).x);
  EXPECT_EQ(2, a.load(memory_order_acquire).x);
}

TEST(TwointsTest, CompareExchangeSanity) {
  twoints one = { 1 };
  twoints two = { 2 };
  atomic<twoints> a(one);
  twoints e = { 0 };
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire).x);
  EXPECT_EQ(1, e.x);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire).x);
  EXPECT_EQ(1, e.x);
}

struct twoshorts {
  short x;
  short y;
};

TEST(TwoshortsTest, Lockfree) {
  atomic<twoshorts> a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(TwoshortsTest, LoadStoreSanity) {
  twoshorts one = { 1 };
  twoshorts two = { 2 };
  atomic<twoshorts> a(one);
  EXPECT_EQ(1, a.load(memory_order_relaxed).x);
  a.store(two, memory_order_release);
  EXPECT_EQ(2, a.load(memory_order_acquire).x);
}

TEST(TwoshortsTest, ExchangeSanity) {
  twoshorts one = { 1 };
  twoshorts two = { 2 };
  atomic<twoshorts> a(one);
  EXPECT_EQ(1, a.exchange(two, memory_order_acq_rel).x);
  EXPECT_EQ(2, a.load(memory_order_acquire).x);
}

TEST(TwoshortsTest, CompareExchangeSanity) {
  twoshorts one = { 1 };
  twoshorts two = { 2 };
  atomic<twoshorts> a(one);
  twoshorts e = { 0 };
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire).x);
  EXPECT_EQ(1, e.x);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire).x);
  EXPECT_EQ(1, e.x);
}

struct threechars {
  char c[3];
};

TEST(ThreecharsTest, Lockfree) {
  QCHECK_EQ(3, sizeof(threechars));
  atomic<threechars> a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(ThreecharsTest, LoadStoreSanity) {
  threechars one = { { 1 } };
  threechars two = { { 2 } };
  atomic<threechars> a(one);
  EXPECT_EQ(1, a.load(memory_order_relaxed).c[0]);
  a.store(two, memory_order_release);
  EXPECT_EQ(2, a.load(memory_order_acquire).c[0]);
}

TEST(ThreecharsTest, ExchangeSanity) {
  threechars one = { { 1 } };
  threechars two = { { 2 } };
  atomic<threechars> a(one);
  EXPECT_EQ(1, a.exchange(two, memory_order_acq_rel).c[0]);
  EXPECT_EQ(2, a.load(memory_order_acquire).c[0]);
}

TEST(ThreecharsTest, CompareExchangeSanity) {
  threechars one = { { 1 } };
  threechars two = { { 2 } };
  atomic<threechars> a(one);
  threechars e = { { 0 } };
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire).c[0]);
  EXPECT_EQ(1, e.c[0]);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire).c[0]);
  EXPECT_EQ(1, e.c[0]);
}

TEST(FloatTest, Lockfree) {
  atomic<float> a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(FloatTest, LoadStoreSanity) {
  float one = 1;
  float two = 2;
  atomic<float> a(one);
  EXPECT_EQ(1, a.load(memory_order_relaxed));
  a.store(two, memory_order_release);
  EXPECT_EQ(2, a.load(memory_order_acquire));
}

TEST(FloatTest, ExchangeSanity) {
  float one = 1;
  float two = 2;
  atomic<float> a(one);
  EXPECT_EQ(1, a.exchange(two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire));
}

TEST(FloatTest, CompareExchangeSanity) {
  float one = 1;
  float two = 2;
  atomic<float> a(one);
  float e = 0;
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
}

TEST(DoubleTest, Lockfree) {
  atomic<float> a;
  EXPECT_TRUE(a.is_lock_free());
}

TEST(DoubleTest, LoadStoreSanity) {
  float one = 1;
  float two = 2;
  atomic<float> a(one);
  EXPECT_EQ(1, a.load(memory_order_relaxed));
  a.store(two, memory_order_release);
  EXPECT_EQ(2, a.load(memory_order_acquire));
}

TEST(DoubleTest, ExchangeSanity) {
  float one = 1;
  float two = 2;
  atomic<float> a(one);
  EXPECT_EQ(1, a.exchange(two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire));
}

TEST(DoubleTest, CompareExchangeSanity) {
  float one = 1;
  float two = 2;
  atomic<float> a(one);
  float e = 0;
  EXPECT_FALSE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(1, a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
  EXPECT_TRUE(a.compare_exchange_strong(e, two, memory_order_acq_rel));
  EXPECT_EQ(2, a.load(memory_order_acquire));
  EXPECT_EQ(1, e);
}
#endif  // if !defined(_GLIBCXX_ATOMIC) ||
        // __GNUC_PREREQ(4,7)) ||
        // (defined(__clang__) && __GLIBC_PREREQ(2,15))


// Single-threaded benchmarks

using base::subtle::Atomic64;

// atomic<>::load()
template <typename T, memory_order ORDER>
void BM_Atomic_Load(int iters) {
  atomic<T> a(0);
  while (iters--) a.load(ORDER);
}

template <typename T>
void BM_Nobarrier_Load(int iters) {
  T a(0);
  while (iters--) base::subtle::NoBarrier_Load(&a);
}
BENCHMARK_TEMPLATE(BM_Nobarrier_Load, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, Atomic64, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_Load, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, Atomic32, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, uint16_t, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, uint8_t, memory_order_relaxed);

template <typename T>
void BM_Acquire_Load(int iters) {
  T a;
  while (iters--) base::subtle::Acquire_Load(&a);
}
BENCHMARK_TEMPLATE(BM_Acquire_Load, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, Atomic64, memory_order_acquire);
BENCHMARK_TEMPLATE(BM_Acquire_Load, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, Atomic32, memory_order_acquire);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, uint16_t, memory_order_acquire);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, uint8_t, memory_order_acquire);

BENCHMARK_TEMPLATE2(BM_Atomic_Load, Atomic64, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, Atomic32, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, uint16_t, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Load, uint8_t, memory_order_seq_cst);

// atomic<>::store()
template <typename T, memory_order ORDER>
void BM_Atomic_Store(int iters) {
  atomic<T> a;
  while (iters--) a.store(0, ORDER);
}

template <typename T>
void BM_Nobarrier_Store(int iters) {
  T a;
  while (iters--) base::subtle::NoBarrier_Store(&a, 0);
}
BENCHMARK_TEMPLATE(BM_Nobarrier_Store, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, Atomic64, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_Store, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, Atomic32, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, uint16_t, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, uint8_t, memory_order_relaxed);

template <typename T>
void BM_Release_Store(int iters) {
  T a;
  while (iters--) base::subtle::Release_Store(&a, 0);
}
BENCHMARK_TEMPLATE(BM_Release_Store, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, Atomic64, memory_order_release);
BENCHMARK_TEMPLATE(BM_Release_Store, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, Atomic32, memory_order_release);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, uint16_t, memory_order_release);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, uint8_t, memory_order_release);

BENCHMARK_TEMPLATE2(BM_Atomic_Store, Atomic64, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, Atomic32, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, uint16_t, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Store, uint8_t, memory_order_seq_cst);

// Load & store back
template <typename T>
void BM_Atomic_LoadStore(int iters, int load_order, int store_order) {
  atomic<T> s(0);
  atomic<T> d;
  T t(0);
  while (iters--) {
    t = s.load(static_cast<memory_order>(load_order));
    d.store(t, static_cast<memory_order>(store_order));
  }
}

template <typename T>
void BM_LoadStore(int iters, int load_order, int store_order) {
  T s;
  T d;
  T t(0);
  while (iters--) {
    switch (load_order) {
      case memory_order_relaxed: t = base::subtle::NoBarrier_Load(&s); break;
      case memory_order_acquire: t = base::subtle::Acquire_Load(&s); break;
    }
    switch (store_order) {
      case memory_order_relaxed: base::subtle::NoBarrier_Store(&d, t); break;
      case memory_order_release: base::subtle::Release_Store(&d, t); break;
    }
  }
}

#define LOADSTORE_ORDERS \
    ArgPair(memory_order_relaxed, memory_order_relaxed) \
    ->ArgPair(memory_order_relaxed, memory_order_release) \
    ->ArgPair(memory_order_acquire, memory_order_relaxed) \
    ->ArgPair(memory_order_acquire, memory_order_release)

BENCHMARK_TEMPLATE(BM_Atomic_LoadStore, Atomic64)->LOADSTORE_ORDERS;
BENCHMARK_TEMPLATE(BM_LoadStore, Atomic64)->LOADSTORE_ORDERS;
BENCHMARK_TEMPLATE(BM_Atomic_LoadStore, Atomic32)->LOADSTORE_ORDERS;
BENCHMARK_TEMPLATE(BM_LoadStore, Atomic32)->LOADSTORE_ORDERS;
BENCHMARK_TEMPLATE(BM_Atomic_LoadStore, uint16_t)->LOADSTORE_ORDERS;
BENCHMARK_TEMPLATE(BM_Atomic_LoadStore, uint8_t)->LOADSTORE_ORDERS;

// atomic<>::exchange()
template <typename T, memory_order ORDER>
void BM_Atomic_Exchange(int iters) {
  atomic<T> a;
  while (iters--) a.exchange(T(0), ORDER);
}

template <typename T>
void BM_Nobarrier_Exchange(int iters) {
  T a;
  while (iters--) base::subtle::NoBarrier_AtomicExchange(&a, 0);
}

BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, Atomic64, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_Exchange, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, Atomic32, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_Exchange, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, uint16_t, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, uint8_t, memory_order_relaxed);

BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, Atomic64, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, Atomic32, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, uint16_t, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Exchange, uint8_t, memory_order_seq_cst);

// Successful atomic<>::compare_exchange_strong()
template <typename T, memory_order ORDER>
void BM_Atomic_CAS_Success(int iters) {
  atomic<T> a(0);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (a.compare_exchange_strong(e, d, ORDER))
      ++s;
  }
  CHECK_EQ(iters, s);
}

template <typename T>
void BM_Nobarrier_CAS_Success(int iters) {
  T a(0);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (e == base::subtle::NoBarrier_CompareAndSwap(&a, e, d))
      ++s;
  }
  CHECK_EQ(iters, s);
}

BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, Atomic64, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_CAS_Success, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, Atomic32, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_CAS_Success, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, uint16_t, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, uint8_t, memory_order_relaxed);

template <typename T>
void BM_Acquire_CAS_Success(int iters) {
  T a(0);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (e == base::subtle::Acquire_CompareAndSwap(&a, e, d))
      ++s;
  }
  CHECK_EQ(iters, s);
}

BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, Atomic64, memory_order_acquire);
BENCHMARK_TEMPLATE(BM_Acquire_CAS_Success, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, Atomic32, memory_order_acquire);
BENCHMARK_TEMPLATE(BM_Acquire_CAS_Success, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, uint16_t, memory_order_acquire);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, uint8_t, memory_order_acquire);

template <typename T>
void BM_Release_CAS_Success(int iters) {
  T a(0);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (e == base::subtle::Release_CompareAndSwap(&a, e, d))
      ++s;
  }
  CHECK_EQ(iters, s);
}

BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, Atomic64, memory_order_release);
BENCHMARK_TEMPLATE(BM_Release_CAS_Success, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, Atomic32, memory_order_release);
BENCHMARK_TEMPLATE(BM_Release_CAS_Success, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, uint16_t, memory_order_release);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Success, uint8_t, memory_order_release);

template <typename T, memory_order ORDER>
void BM_Atomic_CAS_Failure(int iters) {
  atomic<T> a(1);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (a.compare_exchange_strong(e, d, ORDER))
      ++s;
  }
  CHECK_EQ(0, s);
}

template <typename T>
void BM_Nobarrier_CAS_Failure(int iters) {
  T a(1);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (e == base::subtle::NoBarrier_CompareAndSwap(&a, e, d))
      ++s;
  }
  CHECK_EQ(0, s);
}

BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, Atomic64, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_CAS_Failure, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, Atomic32, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_Nobarrier_CAS_Failure, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, uint16_t, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, uint8_t, memory_order_relaxed);

template <typename T>
void BM_Acquire_CAS_Failure(int iters) {
  T a(1);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (e == base::subtle::Acquire_CompareAndSwap(&a, e, d))
      ++s;
  }
  CHECK_EQ(0, s);
}

BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, Atomic64, memory_order_acquire);
BENCHMARK_TEMPLATE(BM_Acquire_CAS_Failure, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, Atomic32, memory_order_acquire);
BENCHMARK_TEMPLATE(BM_Acquire_CAS_Failure, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, uint16_t, memory_order_acquire);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, uint8_t, memory_order_acquire);

template <typename T>
void BM_Release_CAS_Failure(int iters) {
  T a(1);
  int s = 0;
  for (int i = iters; i--; ) {
    T e(0);
    T const d(0);
    if (e == base::subtle::Release_CompareAndSwap(&a, e, d))
      ++s;
  }
  CHECK_EQ(0, s);
}

BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, Atomic64, memory_order_release);
BENCHMARK_TEMPLATE(BM_Release_CAS_Failure, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, Atomic32, memory_order_release);
BENCHMARK_TEMPLATE(BM_Release_CAS_Failure, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, uint16_t, memory_order_release);
BENCHMARK_TEMPLATE2(BM_Atomic_CAS_Failure, uint8_t, memory_order_release);

// Failed atomic<>::compare_exchange_strong()
template < typename T, memory_order ORDER>
void BM_Atomic_Fetch_Add(int iters) {
  atomic<T> a;
  while (iters--) a.fetch_add(0, ORDER);
}

template < typename T>
void BM_NoBarrier_Increment(int iters) {
  T a;
  while (iters--) base::subtle::NoBarrier_AtomicIncrement(&a, 0);
}

BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, Atomic64, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_NoBarrier_Increment, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, Atomic32, memory_order_relaxed);
BENCHMARK_TEMPLATE(BM_NoBarrier_Increment, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, uint16_t, memory_order_relaxed);
BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, uint8_t, memory_order_relaxed);

template < typename T>
void BM_Barrier_Increment(int iters) {
  T a;
  while (iters--) base::subtle::Barrier_AtomicIncrement(&a, 0);
}

BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, Atomic64, memory_order_seq_cst);
BENCHMARK_TEMPLATE(BM_Barrier_Increment, Atomic64);
BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, Atomic32, memory_order_seq_cst);
BENCHMARK_TEMPLATE(BM_Barrier_Increment, Atomic32);
BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, uint16_t, memory_order_seq_cst);
BENCHMARK_TEMPLATE2(BM_Atomic_Fetch_Add, uint8_t, memory_order_seq_cst);


// Multi-threaded benchmarks

#define THREADS Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(12)->Arg(24)

template <typename T, memory_order ORDER>
int MT_Benchmark(void (*benchmark)(Barrier*, int, T, atomic<T>*),
                 int iters,
                 int nthreads,
                 atomic<T>* p) {
  iters = max(1, iters / nthreads) * nthreads;
  CHECK(iters);
  {
    Barrier start(nthreads + 1);
    boost::ptr_vector<ClosureThread> threads;
    threads.reserve(nthreads);
    for (int t = nthreads; t--; ) {
      Closure* const c = NewPermanentCallback(benchmark,
                                              &start,
                                              iters/nthreads,
                                              t+1,
                                              p);
      threads.push_back(new ClosureThread(c));
      threads.back().SetJoinable(true);
      threads.back().Start();
    }
    start.Block();
    for (int t = nthreads; t--; ) threads[t].Join();
  }
  SetBenchmarkItemsProcessed(iters);
  return iters;
}

// MT atomic<>::store()
template <typename T, memory_order ORDER>
void MT_Store(Barrier* start, int iters, T store, atomic<T>* p) {
  CHECK(iters);
  start->Block();
  for (int i = iters; i--; ) {
    p->store(store, ORDER);
  }
}

template <typename T, memory_order ORDER>
void BM_MT_Store(int iters, int nthreads) {
  atomic<T> a(0);
  MT_Benchmark<T, ORDER>(&MT_Store<T, ORDER>, iters, nthreads, &a);
  CHECK(0 < a.load(memory_order_relaxed)
        && a.load(memory_order_relaxed) <= nthreads);
}

BENCHMARK_TEMPLATE2(BM_MT_Store, Atomic64, memory_order_relaxed)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Store, Atomic64, memory_order_release)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Store, Atomic64, memory_order_seq_cst)->THREADS;

// MT atomic<>::exchange()
template <typename T, memory_order ORDER>
void MT_Exchange(Barrier* start, int iters, T value, atomic<T>* p) {
  CHECK(iters);
  start->Block();
  for (int i = iters; i--; ) {
    p->exchange(value, ORDER);
  }
}

template <typename T, memory_order ORDER>
void BM_MT_Exchange(int iters, int nthreads) {
  atomic<T> a(0);
  MT_Benchmark<T, ORDER>(&MT_Exchange<T, ORDER>, iters, nthreads, &a);
  CHECK(0 < a.load(memory_order_relaxed)
        && a.load(memory_order_relaxed) <= nthreads);
}

BENCHMARK_TEMPLATE2(BM_MT_Exchange, Atomic64, memory_order_relaxed)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Exchange, Atomic64, memory_order_acquire)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Exchange, Atomic64, memory_order_release)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Exchange, Atomic64, memory_order_acq_rel)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Exchange, Atomic64, memory_order_seq_cst)->THREADS;

// MT atomic<>::compare_exchange_strong() a fixed number of times
template <typename T, memory_order ORDER>
void MT_CAS(Barrier* start, int iters, T value, atomic<T>* p) {
  CHECK(iters);
  start->Block();
  T e(0);
  for (int i = iters; i--; ) {
    p->compare_exchange_strong(e, value, ORDER);
  }
}

template <typename T, memory_order ORDER>
void BM_MT_CAS(int iters, int nthreads) {
  atomic<T> a(0);
  MT_Benchmark<T, ORDER>(&MT_CAS<T, ORDER>, iters, nthreads, &a);
  CHECK(0 < a.load(memory_order_relaxed)
        && a.load(memory_order_relaxed) <= nthreads);
}

BENCHMARK_TEMPLATE2(BM_MT_CAS, Atomic64, memory_order_relaxed)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_CAS, Atomic64, memory_order_acquire)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_CAS, Atomic64, memory_order_release)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_CAS, Atomic64, memory_order_acq_rel)->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_CAS, Atomic64, memory_order_seq_cst)->THREADS;

// MT atomic<>::compare_exchange_strong() until success
template <typename T, memory_order ORDER>
void MT_Successful_CAS(Barrier* start, int iters, T, atomic<T>* p) {
  CHECK(iters);
  start->Block();
  T e(0);
  for (int i = iters; i--; ) {
    do {} while (!p->compare_exchange_strong(e, e+1, ORDER));
    ++e;
  }
}

template <typename T, memory_order ORDER>
void BM_MT_Successful_CAS(int iters, int nthreads) {
  atomic<T> a(0);
  iters = MT_Benchmark<T,ORDER>(&MT_Successful_CAS<T, ORDER>,
                                iters,
                                nthreads,
                                &a);
  CHECK_EQ(iters, a.load(memory_order_relaxed));
}

BENCHMARK_TEMPLATE2(BM_MT_Successful_CAS, Atomic64, memory_order_relaxed)
  ->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Successful_CAS, Atomic64, memory_order_acquire)
  ->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Successful_CAS, Atomic64, memory_order_release)
  ->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Successful_CAS, Atomic64, memory_order_acq_rel)
  ->THREADS;
BENCHMARK_TEMPLATE2(BM_MT_Successful_CAS, Atomic64, memory_order_seq_cst)
  ->THREADS;

// Benchmark a pair of operations together
template <typename T>
int MT_BenchmarkPair(void (*benchmark)(Barrier*, int, T, atomic<T>*, bool),
                     int iters,
                     int nthreads,
                     atomic<T>* p) {
  iters = max(1, iters / nthreads) * nthreads;
  CHECK(iters);
  {
    Barrier start(nthreads + 1);
    boost::ptr_vector<ClosureThread> threads;
    threads.reserve(nthreads);
    for (int t = nthreads; t--; ) {
      Closure* const c = NewPermanentCallback(benchmark,
                                              &start,
                                              iters/nthreads,
                                              t+1,
                                              p,
                                              t&1);
      threads.push_back(new ClosureThread(c));
      threads.back().SetJoinable(true);
      threads.back().Start();
    }
    start.Block();
    for (int t = nthreads; t--; ) threads[t].Join();
  }
  SetBenchmarkItemsProcessed(iters);
  return iters;
}

// MT atomic<>::load() and store()
template <typename T>
void MT_LoadStore(Barrier* start, int iters, T store, atomic<T>* p, bool order) {
  CHECK(iters);
  start->Block();
  if (order) {
    for (int i = iters; i--; ) {
      p->store(store, memory_order_seq_cst);
    }
    for (int i = iters; i--; ) {
      p->load(memory_order_seq_cst);
    }
  } else {
    for (int i = iters; i--; ) {
      p->load(memory_order_seq_cst);
    }
    for (int i = iters; i--; ) {
      p->store(store, memory_order_seq_cst);
    }
  }
}

template <typename T>
void BM_MT_LoadStore(int iters, int nthreads) {
  atomic<T> a(0);
  MT_BenchmarkPair<T>(&MT_LoadStore<T>, iters, nthreads, &a);
  CHECK(0 < a.load(memory_order_relaxed)
        && a.load(memory_order_relaxed) <= nthreads);
}

BENCHMARK_TEMPLATE(BM_MT_LoadStore, Atomic64)->THREADS;

// MT atomic<>::load() and exchange()
template <typename T>
void MT_LoadExchange(Barrier* start, int iters, T value, atomic<T>* p, bool order) {
  CHECK(iters);
  start->Block();
  if (order) {
    for (int i = iters; i--; ) {
      p->exchange(value, memory_order_seq_cst);
    }
    for (int i = iters; i--; ) {
      p->load(memory_order_seq_cst);
    }
  } else {
    for (int i = iters; i--; ) {
      p->load(memory_order_seq_cst);
    }
    for (int i = iters; i--; ) {
      p->exchange(value, memory_order_seq_cst);
    }
  }
}

template <typename T>
void BM_MT_LoadExchange(int iters, int nthreads) {
  atomic<T> a(0);
  MT_BenchmarkPair<T>(&MT_LoadExchange<T>, iters, nthreads, &a);
  CHECK(0 < a.load(memory_order_relaxed)
        && a.load(memory_order_relaxed) <= nthreads);
}

BENCHMARK_TEMPLATE(BM_MT_LoadExchange, Atomic64)->THREADS;

}  // namespace

}  // namespace concurrent
