// Copyright (c) 2014 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/mem_tracker.h"

#include <tr1/unordered_map>
#include <vector>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>

#include "kudu/util/test_util.h"

namespace kudu {

using std::equal_to;
using std::tr1::hash;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using std::vector;

TEST(MemTrackerTest, SingleTrackerNoLimit) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(-1, "t");
  EXPECT_FALSE(t->has_limit());
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 10);
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 20);
  t->Release(15);
  EXPECT_EQ(t->consumption(), 5);
  EXPECT_FALSE(t->LimitExceeded());
  t->Release(5);
  EXPECT_EQ(t->consumption(), 0);
}

TEST(MemTrackerTest, SingleTrackerWithLimit) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(11, "t");
  EXPECT_TRUE(t->has_limit());
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 10);
  EXPECT_FALSE(t->LimitExceeded());
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 20);
  EXPECT_TRUE(t->LimitExceeded());
  t->Release(15);
  EXPECT_EQ(t->consumption(), 5);
  EXPECT_FALSE(t->LimitExceeded());
  t->Release(5);
}

TEST(MemTrackerTest, TrackerHierarchy) {
  shared_ptr<MemTracker> p = MemTracker::CreateTracker(100, "p");
  shared_ptr<MemTracker> c1 = MemTracker::CreateTracker(80, "c1", p);
  shared_ptr<MemTracker> c2 = MemTracker::CreateTracker(50, "c2", p);

  // everything below limits
  c1->Consume(60);
  EXPECT_EQ(c1->consumption(), 60);
  EXPECT_FALSE(c1->LimitExceeded());
  EXPECT_FALSE(c1->AnyLimitExceeded());
  EXPECT_EQ(c2->consumption(), 0);
  EXPECT_FALSE(c2->LimitExceeded());
  EXPECT_FALSE(c2->AnyLimitExceeded());
  EXPECT_EQ(p->consumption(), 60);
  EXPECT_FALSE(p->LimitExceeded());
  EXPECT_FALSE(p->AnyLimitExceeded());

  // p goes over limit
  c2->Consume(50);
  EXPECT_EQ(c1->consumption(), 60);
  EXPECT_FALSE(c1->LimitExceeded());
  EXPECT_TRUE(c1->AnyLimitExceeded());
  EXPECT_EQ(c2->consumption(), 50);
  EXPECT_FALSE(c2->LimitExceeded());
  EXPECT_TRUE(c2->AnyLimitExceeded());
  EXPECT_EQ(p->consumption(), 110);
  EXPECT_TRUE(p->LimitExceeded());

  // c2 goes over limit, p drops below limit
  c1->Release(20);
  c2->Consume(10);
  EXPECT_EQ(c1->consumption(), 40);
  EXPECT_FALSE(c1->LimitExceeded());
  EXPECT_FALSE(c1->AnyLimitExceeded());
  EXPECT_EQ(c2->consumption(), 60);
  EXPECT_TRUE(c2->LimitExceeded());
  EXPECT_TRUE(c2->AnyLimitExceeded());
  EXPECT_EQ(p->consumption(), 100);
  EXPECT_FALSE(p->LimitExceeded());
  c1->Release(40);
  c2->Release(60);
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
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(10, "");
  ASSERT_TRUE(t->has_limit());

  t->Consume(9);
  EXPECT_FALSE(t->LimitExceeded());

  // Test TryConsume()
  EXPECT_FALSE(t->TryConsume(2));
  EXPECT_EQ(t->consumption(), 9);
  EXPECT_FALSE(t->LimitExceeded());

  // Attach GcFunction that releases 1 byte
  GcFunctionHelper gc_func_helper(t.get());
  t->AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper));
  EXPECT_TRUE(t->TryConsume(2));
  EXPECT_EQ(t->consumption(), 10);
  EXPECT_FALSE(t->LimitExceeded());

  // GcFunction will be called even though TryConsume() fails
  EXPECT_FALSE(t->TryConsume(2));
  EXPECT_EQ(t->consumption(), 9);
  EXPECT_FALSE(t->LimitExceeded());

  // GcFunction won't be called
  EXPECT_TRUE(t->TryConsume(1));
  EXPECT_EQ(t->consumption(), 10);
  EXPECT_FALSE(t->LimitExceeded());

  // Test LimitExceeded()
  t->Consume(1);
  EXPECT_EQ(t->consumption(), 11);
  EXPECT_FALSE(t->LimitExceeded());
  EXPECT_EQ(t->consumption(), 10);

  // Add more GcFunctions, test that we only call them until the limit is no longer
  // exceeded
  GcFunctionHelper gc_func_helper2(t.get());
  t->AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper2));
  GcFunctionHelper gc_func_helper3(t.get());
  t->AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper3));
  t->Consume(1);
  EXPECT_EQ(t->consumption(), 11);
  EXPECT_FALSE(t->LimitExceeded());
  EXPECT_EQ(t->consumption(), 10);
  t->Release(10);
}

TEST(MemTrackerTest, STLContainerAllocator) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(-1, "t");
  MemTrackerAllocator<int> alloc(t);

  // Simple test: use the allocator in a vector.
  {
    vector<int, MemTrackerAllocator<int> > v(alloc);
    ASSERT_EQ(0, t->consumption());
    v.reserve(5);
    ASSERT_EQ(5 * sizeof(int), t->consumption());
    v.reserve(10);
    ASSERT_EQ(10 * sizeof(int), t->consumption());
  }
  ASSERT_EQ(0, t->consumption());

  // Complex test: use it in an unordered_map, where it must be rebound in
  // order to allocate the map's buckets.
  {
    unordered_map<int, int, equal_to<int>, hash<int>, MemTrackerAllocator<int> > um(
        10,
        equal_to<int>(),
        hash<int>(),
        alloc);

    // Don't care about the value (it depends on map internals).
    ASSERT_GT(t->consumption(), 0);
  }
  ASSERT_EQ(0, t->consumption());
}

TEST(MemTrackerTest, FindFunctionsTakeOwnership) {
  // In each test, ToString() would crash if the MemTracker is destroyed when
  // 'm' goes out of scope.

  shared_ptr<MemTracker> ref;
  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker(-1, "test");
    ASSERT_TRUE(MemTracker::FindTracker(m->id(), &ref));
  }
  LOG(INFO) << ref->ToString();
  ref.reset();

  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker(-1, "test");
    ref = MemTracker::FindOrCreateTracker(-1, m->id());
  }
  LOG(INFO) << ref->ToString();
  ref.reset();

  vector<shared_ptr<MemTracker> > refs;
  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker(-1, "test");
    MemTracker::ListTrackers(&refs);
  }
  BOOST_FOREACH(const shared_ptr<MemTracker>& r, refs) {
    LOG(INFO) << r->ToString();
  }
  refs.clear();
}

TEST(MemTrackerTest, ScopedTrackedConsumption) {
  shared_ptr<MemTracker> m = MemTracker::CreateTracker(-1, "test");
  ASSERT_EQ(0, m->consumption());
  {
    ScopedTrackedConsumption consumption(m, 1);
    ASSERT_EQ(1, m->consumption());

    consumption.Reset(3);
    ASSERT_EQ(3, m->consumption());
  }
  ASSERT_EQ(0, m->consumption());
}
} // namespace kudu
