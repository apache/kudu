// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/atomic.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <limits>
#include <vector>

namespace kudu {

using std::numeric_limits;
using std::vector;
using boost::assign::list_of;

// TODO Add some multi-threaded tests; currently AtomicInt is just a
// wrapper around 'atomicops.h', but should the underlying
// implemention change, it would help to have tests that make sure
// invariants are preserved in a multi-threaded environment.

template<typename T>
class AtomicIntTest : public ::testing::Test {
 public:

  AtomicIntTest()
      : max_(numeric_limits<T>::max()),
        min_(numeric_limits<T>::min()) {
    acquire_release_ = list_of(kMemOrderNoBarrier)(kMemOrderAcquire)(kMemOrderRelease);
    barrier_ = list_of(kMemOrderNoBarrier)(kMemOrderBarrier);
  }

  vector<MemoryOrder> acquire_release_;
  vector<MemoryOrder> barrier_;

  T max_;
  T min_;
};

typedef ::testing::Types<int32_t, int64_t, uint32_t, uint64_t> IntTypes;
TYPED_TEST_CASE(AtomicIntTest, IntTypes);

TYPED_TEST(AtomicIntTest, LoadStore) {
  BOOST_FOREACH(const MemoryOrder mem_order, this->acquire_release_) {
    AtomicInt<TypeParam> i(0);
    EXPECT_EQ(0, i.Load(mem_order));
    i.Store(42, mem_order);
    EXPECT_EQ(42, i.Load(mem_order));
    i.Store(this->min_, mem_order);
    EXPECT_EQ(this->min_, i.Load(mem_order));
    i.Store(this->max_, mem_order);
    EXPECT_EQ(this->max_, i.Load(mem_order));
  }
}

TYPED_TEST(AtomicIntTest, SetSwapExchange) {
  BOOST_FOREACH(const MemoryOrder mem_order, this->acquire_release_) {
    AtomicInt<TypeParam> i(0);
    EXPECT_TRUE(i.CompareAndSet(0, 5, mem_order));
    EXPECT_EQ(5, i.Load(mem_order));
    EXPECT_FALSE(i.CompareAndSet(0, 10, mem_order));

    EXPECT_EQ(5, i.CompareAndSwap(5, this->max_, mem_order));
    EXPECT_EQ(this->max_, i.CompareAndSwap(42, 42, mem_order));
    EXPECT_EQ(this->max_, i.CompareAndSwap(this->max_, this->min_, mem_order));

    EXPECT_EQ(this->min_, i.Exchange(this->max_, mem_order));
    EXPECT_EQ(this->max_, i.Load(mem_order));
  }
}

TYPED_TEST(AtomicIntTest, MinMax) {
  BOOST_FOREACH(const MemoryOrder mem_order, this->acquire_release_) {
    AtomicInt<TypeParam> i(0);

    i.StoreMax(100, mem_order);
    EXPECT_EQ(100, i.Load(mem_order));
    i.StoreMin(50, mem_order);
    EXPECT_EQ(50, i.Load(mem_order));

    i.StoreMax(25, mem_order);
    EXPECT_EQ(50, i.Load(mem_order));
    i.StoreMin(75, mem_order);
    EXPECT_EQ(50, i.Load(mem_order));

    i.StoreMax(this->max_, mem_order);
    EXPECT_EQ(this->max_, i.Load(mem_order));
    i.StoreMin(this->min_, mem_order);
    EXPECT_EQ(this->min_, i.Load(mem_order));
  }
}

TYPED_TEST(AtomicIntTest, Increment) {
  BOOST_FOREACH(const MemoryOrder mem_order, this->barrier_) {
    AtomicInt<TypeParam> i(0);
    EXPECT_EQ(1, i.Increment(mem_order));
    EXPECT_EQ(3, i.IncrementBy(2, mem_order));
    EXPECT_EQ(3, i.IncrementBy(0, mem_order));
  }
}

TEST(Atomic, AtomicBool) {
  vector<MemoryOrder> memory_orders =
      list_of(kMemOrderNoBarrier)(kMemOrderRelease)(kMemOrderAcquire);

  BOOST_FOREACH(const MemoryOrder mem_order, memory_orders) {
    AtomicBool b(false);
    EXPECT_FALSE(b.Load(mem_order));
    b.Store(true, mem_order);
    EXPECT_TRUE(b.Load(mem_order));
    EXPECT_TRUE(b.CompareAndSet(true, false, mem_order));
    EXPECT_FALSE(b.Load(mem_order));
    EXPECT_FALSE(b.CompareAndSet(true, false, mem_order));
    EXPECT_FALSE(b.CompareAndSwap(false, true, mem_order));
    EXPECT_TRUE(b.Load(mem_order));
    EXPECT_TRUE(b.Exchange(false, mem_order));
    EXPECT_FALSE(b.Load(mem_order));
  }
}

} // namespace kudu
