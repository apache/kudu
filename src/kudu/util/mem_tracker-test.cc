// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/mem_tracker.h"

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_util.h"

namespace kudu {

using std::equal_to;
using std::hash;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

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
  static const int kNumReleaseBytes = 1;

  explicit GcFunctionHelper(MemTracker* tracker) : tracker_(tracker) { }

  void GcFunc() { tracker_->Release(kNumReleaseBytes); }

 private:
  MemTracker* tracker_;
};

TEST(MemTrackerTest, STLContainerAllocator) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(-1, "t");
  MemTrackerAllocator<int> vec_alloc(t);
  MemTrackerAllocator<pair<const int, int>> map_alloc(t);

  // Simple test: use the allocator in a vector.
  {
    vector<int, MemTrackerAllocator<int> > v(vec_alloc);
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
    unordered_map<int, int, hash<int>, equal_to<int>, MemTrackerAllocator<pair<const int, int>>> um(
        10,
        hash<int>(),
        equal_to<int>(),
        map_alloc);

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
    ref = MemTracker::FindOrCreateGlobalTracker(-1, m->id());
  }
  LOG(INFO) << ref->ToString();
  ref.reset();

  vector<shared_ptr<MemTracker> > refs;
  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker(-1, "test");
    MemTracker::ListTrackers(&refs);
  }
  for (const shared_ptr<MemTracker>& r : refs) {
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

TEST(MemTrackerTest, CollisionDetection) {
  shared_ptr<MemTracker> p = MemTracker::CreateTracker(-1, "parent");
  shared_ptr<MemTracker> c = MemTracker::CreateTracker(-1, "child", p);
  vector<shared_ptr<MemTracker>> all;

  // Three trackers: root, parent, and child.
  MemTracker::ListTrackers(&all);
  ASSERT_EQ(3, all.size());

  // Now only two because the child has been destroyed.
  c.reset();
  MemTracker::ListTrackers(&all);
  ASSERT_EQ(2, all.size());
  shared_ptr<MemTracker> not_found;
  ASSERT_FALSE(MemTracker::FindTracker("child", &not_found, p));

  // Let's duplicate the parent. It's not recommended, but it's allowed.
  shared_ptr<MemTracker> p2 = MemTracker::CreateTracker(-1, "parent");
  ASSERT_EQ(p->ToString(), p2->ToString());

  // Only when we do a Find() operation do we crash.
#ifndef NDEBUG
  const string kDeathMsg = "Multiple memtrackers with same id";
  EXPECT_DEATH({
    shared_ptr<MemTracker> found;
    MemTracker::FindTracker("parent", &found);
  }, kDeathMsg);
  EXPECT_DEATH({
    MemTracker::FindOrCreateGlobalTracker(-1, "parent");
  }, kDeathMsg);
#endif
}

TEST(MemTrackerTest, TestMultiThreadedRegisterAndDestroy) {
  std::atomic<bool> done(false);
  vector<std::thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&done]{
        while (!done.load()) {
          shared_ptr<MemTracker> t = MemTracker::FindOrCreateGlobalTracker(
              1000, "foo");
        }
      });
  }

  SleepFor(MonoDelta::FromSeconds(AllowSlowTests() ? 5 : 1));
  done.store(true);
  for (auto& t : threads) {
    t.join();
  }
}

TEST(MemTrackerTest, TestMultiThreadedCreateFind) {
  shared_ptr<MemTracker> p = MemTracker::CreateTracker(-1, "p");
  shared_ptr<MemTracker> c1 = MemTracker::CreateTracker(-1, "c1", p);
  std::atomic<bool> done(false);
  vector<std::thread> threads;
  threads.emplace_back([&]{
    while (!done.load()) {
      shared_ptr<MemTracker> c1_copy;
      CHECK(MemTracker::FindTracker(c1->id(), &c1_copy, p));
    }
  });
  for (int i = 0; i < 5; i++) {
    threads.emplace_back([&, i]{
      while (!done.load()) {
        shared_ptr<MemTracker> c2 =
            MemTracker::CreateTracker(-1, Substitute("ci-$0", i), p);
      }
    });
  }

  SleepFor(MonoDelta::FromMilliseconds(500));
  done.store(true);
  for (auto& t : threads) {
    t.join();
  }
}

} // namespace kudu
