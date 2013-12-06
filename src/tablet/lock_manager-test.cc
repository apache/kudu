// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/memory>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "util/env.h"
#include "tablet/lock_manager.h"
#include "util/stopwatch.h"
#include "util/test_util.h"
#include "util/thread_util.h"

using std::vector;
using std::tr1::shared_ptr;

DEFINE_int32(num_test_threads, 10, "number of stress test client threads");
DEFINE_int32(num_iterations, 1000, "number of iterations per client thread");

namespace kudu {
namespace tablet {

static const TransactionContext* kFakeTransaction =
  reinterpret_cast<TransactionContext*>(0xdeadbeef);

class LockManagerTest : public KuduTest {
 public:
  void VerifyAlreadyLocked(const Slice& key) {
    LockEntry *entry;
    ASSERT_EQ(LockManager::LOCK_BUSY,
              lock_manager_.TryLock(key, kFakeTransaction, LockManager::LOCK_EXCLUSIVE, &entry));
  }

  LockManager lock_manager_;
};

TEST_F(LockManagerTest, TestLockUnlockSingleRow) {
  Slice key_a("a");
  ScopedRowLock(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
  ScopedRowLock(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
  ScopedRowLock(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
}

// Test if the same transaction locks the same row multiple times.
// This should FATAL rather than deadlock.
TEST_F(LockManagerTest, TestMultiplyLockSameRow) {
  ASSERT_DEATH({
      Slice key_a("a");
      ScopedRowLock first_lock(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
      ScopedRowLock second_lock(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
    }, "Check failed.*Same transaction trying to lock row a");
}

TEST_F(LockManagerTest, TestLockUnlockMultipleRows) {
  Slice key_a("a"), key_b("b");
  for (int i = 0; i < 3; ++i) {
    ScopedRowLock l1(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
    ScopedRowLock l2(&lock_manager_, kFakeTransaction, key_b, LockManager::LOCK_EXCLUSIVE);
    VerifyAlreadyLocked(key_a);
    VerifyAlreadyLocked(key_b);
  }
}

TEST_F(LockManagerTest, TestRelockSameRow) {
  Slice key_a("a");
  ScopedRowLock row_lock(&lock_manager_, kFakeTransaction, key_a, LockManager::LOCK_EXCLUSIVE);
  VerifyAlreadyLocked(key_a);
}

class LmTestResource {
 public:
  explicit LmTestResource(const Slice* id)
    : id_(id),
      owner_(0),
      is_owned_(false) {
  }

  const Slice* id() const {
    return id_;
  }

  void acquire(uint64_t tid) {
    boost::unique_lock<boost::mutex> lock(lock_);
    CHECK(!is_owned_);
    CHECK_EQ(0, owner_);
    owner_ = tid;
    is_owned_ = true;
  }

  void release(uint64_t tid) {
    boost::unique_lock<boost::mutex> lock(lock_);
    CHECK(is_owned_);
    CHECK_EQ(tid, owner_);
    owner_ = 0;
    is_owned_ = false;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LmTestResource);

  const Slice* id_;
  boost::mutex lock_;
  uint64_t owner_;
  bool is_owned_;
};

class LmTestThread {
 public:
  LmTestThread(LockManager* manager, vector<const Slice*> keys,
               const vector<LmTestResource*> resources)
    : manager_(manager),
      keys_(keys),
      resources_(resources) {
  }

  void Start() {
    thread_.reset(new boost::thread(boost::bind(&LmTestThread::Run, this)));
  }

  void Run() {
    tid_ = Env::Default()->gettid();
    const TransactionContext* my_txn = reinterpret_cast<TransactionContext*>(tid_);

    std::sort(keys_.begin(), keys_.end());
    for (int i = 0; i < FLAGS_num_iterations; i++) {
      std::vector<shared_ptr<ScopedRowLock> > locks;
      // TODO: We don't have an API for multi-row
      BOOST_FOREACH(const Slice* key, keys_) {
        locks.push_back(shared_ptr<ScopedRowLock>(
                          new ScopedRowLock(manager_, my_txn,
                                            *key, LockManager::LOCK_EXCLUSIVE)));
      }

      BOOST_FOREACH(LmTestResource* r, resources_) {
        r->acquire(tid_);
      }
      BOOST_FOREACH(LmTestResource* r, resources_) {
        r->release(tid_);
      }
    }
  }

  void Join() {
    ThreadJoiner(thread_.get(), "LmTestThread").
      warn_after_ms(1000).
      warn_every_ms(5000).
      Join();
    thread_.reset(NULL);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LmTestThread);
  LockManager* manager_;
  vector<const Slice*> keys_;
  const vector<LmTestResource*> resources_;
  uint64_t tid_;
  gscoped_ptr<boost::thread> thread_;
};

static void runPerformanceTest(const char *test_type,
                               vector<shared_ptr<LmTestThread> > *threads) {
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  BOOST_FOREACH(const shared_ptr<LmTestThread>& t, *threads) {
    t->Start();
  }

  BOOST_FOREACH(const shared_ptr<LmTestThread>& t, *threads) {
    t->Join();
  }
  sw.stop();

  float num_cycles = FLAGS_num_iterations;
  num_cycles *= FLAGS_num_test_threads;

  float cycles_per_second = num_cycles / sw.elapsed().wall_seconds();
  float user_cpu_micros_per_cycle =
    (sw.elapsed().user / 1000.0) / cycles_per_second;
  float sys_cpu_micros_per_cycle =
    (sw.elapsed().system / 1000.0) / cycles_per_second;
  LOG(INFO) << "*** testing with " << FLAGS_num_test_threads << " threads, "
    << FLAGS_num_iterations << " iterations.";
  LOG(INFO) << test_type << " Lock/Unlock cycles per second:  "
    << cycles_per_second;
  LOG(INFO) << test_type << " User CPU per lock/unlock cycle: "
    << user_cpu_micros_per_cycle << "us";
  LOG(INFO) << test_type << " Sys CPU per lock/unlock cycle:  "
    << sys_cpu_micros_per_cycle << "us";
}

// Test running a bunch of threads at once that want an overlapping set of
// resources.
TEST_F(LockManagerTest, TestContention) {
  Slice slice_a("a");
  LmTestResource resource_a(&slice_a);
  Slice slice_b("b");
  LmTestResource resource_b(&slice_b);
  Slice slice_c("c");
  LmTestResource resource_c(&slice_c);
  vector<shared_ptr<LmTestThread> > threads;
  for (int i = 0; i < FLAGS_num_test_threads; ++i) {
    vector<LmTestResource*> resources;
    if (i % 3 == 0) {
      resources.push_back(&resource_a);
      resources.push_back(&resource_b);
    } else if (i % 3 == 1) {
      resources.push_back(&resource_b);
      resources.push_back(&resource_c);
    } else {
      resources.push_back(&resource_c);
      resources.push_back(&resource_a);
    }
    vector<const Slice*> keys;
    for (vector<LmTestResource*>::const_iterator r = resources.begin();
         r != resources.end(); ++r) {
      keys.push_back((*r)->id());
    }
    threads.push_back(shared_ptr<LmTestThread>(
        new LmTestThread(&lock_manager_, keys, resources)));
  }
  runPerformanceTest("Contended", &threads);
}

// Test running a bunch of threads at once that want different
// resources.
TEST_F(LockManagerTest, TestUncontended) {
  vector<string> slice_strings;
  for (int i = 0; i < FLAGS_num_test_threads; i++) {
    slice_strings.push_back(StringPrintf("slice%03d", i));
  }
  vector<Slice> slices;
  for (int i = 0; i < FLAGS_num_test_threads; i++) {
    slices.push_back(Slice(slice_strings[i]));
  }
  vector<shared_ptr<LmTestResource> > resources;
  for (int i = 0; i < FLAGS_num_test_threads; i++) {
    resources.push_back(
        shared_ptr<LmTestResource>(new LmTestResource(&slices[i])));
  }
  vector<shared_ptr<LmTestThread> > threads;
  for (int i = 0; i < FLAGS_num_test_threads; ++i) {
    vector<const Slice*> k;
    k.push_back(&slices[i]);
    vector<LmTestResource*> r;
    r.push_back(resources[i].get());
    threads.push_back(shared_ptr<LmTestThread>(
        new LmTestThread(&lock_manager_, k, r)));
  }
  runPerformanceTest("Uncontended", &threads);
}

} // namespace tablet
} // namespace kudu
