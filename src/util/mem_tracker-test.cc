// Copyright (c) 2014 Cloudera Inc.

#include "util/mem_tracker.h"

#include <boost/bind.hpp>

#include "util/test_util.h"

namespace kudu {

TEST(MemTrackerTest, SingleTrackerNoLimit) {
  MemTracker t(-1, "", NULL);
  EXPECT_FALSE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_FALSE(t.LimitExceeded());
}

TEST(MemTrackerTest, SingleTrackerWithLimit) {
  MemTracker t(11,"", NULL);
  EXPECT_TRUE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  EXPECT_TRUE(t.LimitExceeded());
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_FALSE(t.LimitExceeded());
}

TEST(MemTrackerTest, TrackerHierarchy) {
  MemTracker p(100, "", NULL);
  MemTracker c1(80, "", &p);
  MemTracker c2(50, "", &p);

  // everything below limits
  c1.Consume(60);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_FALSE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 0);
  EXPECT_FALSE(c2.LimitExceeded());
  EXPECT_FALSE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 60);
  EXPECT_FALSE(p.LimitExceeded());
  EXPECT_FALSE(p.AnyLimitExceeded());

  // p goes over limit
  c2.Consume(50);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_TRUE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 50);
  EXPECT_FALSE(c2.LimitExceeded());
  EXPECT_TRUE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 110);
  EXPECT_TRUE(p.LimitExceeded());

  // c2 goes over limit, p drops below limit
  c1.Release(20);
  c2.Consume(10);
  EXPECT_EQ(c1.consumption(), 40);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_FALSE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 60);
  EXPECT_TRUE(c2.LimitExceeded());
  EXPECT_TRUE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 100);
  EXPECT_FALSE(p.LimitExceeded());
}

class GcFunctionHelper {
 public:
  static const int NUM_RELEASE_BYTES = 1;

  explicit GcFunctionHelper(MemTracker* tracker) : tracker_(tracker) { }

  void GcFunc() { tracker_->Release(NUM_RELEASE_BYTES); }

 private:
  MemTracker* tracker_;
};

TEST(MemTrackerTest, GcFunctions) {
  MemTracker t(10, "", NULL);
  ASSERT_TRUE(t.has_limit());

  t.Consume(9);
  EXPECT_FALSE(t.LimitExceeded());

  // Test TryConsume()
  EXPECT_FALSE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 9);
  EXPECT_FALSE(t.LimitExceeded());

  // Attach GcFunction that releases 1 byte
  GcFunctionHelper gc_func_helper(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper));
  EXPECT_TRUE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());

  // GcFunction will be called even though TryConsume() fails
  EXPECT_FALSE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 9);
  EXPECT_FALSE(t.LimitExceeded());

  // GcFunction won't be called
  EXPECT_TRUE(t.TryConsume(1));
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());

  // Test LimitExceeded()
  t.Consume(1);
  EXPECT_EQ(t.consumption(), 11);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(t.consumption(), 10);

  // Add more GcFunctions, test that we only call them until the limit is no longer
  // exceeded
  GcFunctionHelper gc_func_helper2(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper2));
  GcFunctionHelper gc_func_helper3(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper3));
  t.Consume(1);
  EXPECT_EQ(t.consumption(), 11);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(t.consumption(), 10);
}


} // namespace kudu
