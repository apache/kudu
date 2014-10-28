// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include "kudu/util/object_pool.h"

namespace kudu {

// Simple class which maintains a count of how many objects
// are currently alive.
class MyClass {
 public:
  MyClass() {
    instance_count_++;
  }

  ~MyClass() {
    instance_count_--;
  }

  static int instance_count() {
    return instance_count_;
  }

  static void ResetCount() {
    instance_count_ = 0;
  }

 private:
  static int instance_count_;
};
int MyClass::instance_count_ = 0;

TEST(TestObjectPool, TestPooling) {
  MyClass::ResetCount();
  {
    ObjectPool<MyClass> pool;
    ASSERT_EQ(0, MyClass::instance_count());
    MyClass *a = pool.Construct();
    ASSERT_EQ(1, MyClass::instance_count());
    MyClass *b = pool.Construct();
    ASSERT_EQ(2, MyClass::instance_count());
    ASSERT_TRUE(a != b);
    pool.Destroy(b);
    ASSERT_EQ(1, MyClass::instance_count());
    MyClass *c = pool.Construct();
    ASSERT_EQ(2, MyClass::instance_count());
    ASSERT_TRUE(c == b) << "should reuse instance";
    pool.Destroy(c);

    ASSERT_EQ(1, MyClass::instance_count());
  }

  ASSERT_EQ(0, MyClass::instance_count())
    << "destructing pool should have cleared instances";
}

TEST(TestObjectPool, TestScopedPtr) {
  MyClass::ResetCount();
  ASSERT_EQ(0, MyClass::instance_count());
  ObjectPool<MyClass> pool;
  {
    ObjectPool<MyClass>::scoped_ptr sptr(
      pool.make_scoped_ptr(pool.Construct()));
    ASSERT_EQ(1, MyClass::instance_count());
  }
  ASSERT_EQ(0, MyClass::instance_count());
}

} // namespace kudu
