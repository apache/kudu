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

#include <boost/smart_ptr/detail/spinlock.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <mutex>
#include <stdio.h>
#include <thread>
#include <unistd.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/flags.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/rw_mutex.h"

DEFINE_int32(num_threads, 8, "Number of threads to test");

using std::thread;
using std::vector;

class my_spinlock : public boost::detail::spinlock {
 public:
  my_spinlock() {
    v_ = 0;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(my_spinlock);
};

struct PerCpuLock {
  struct PaddedLock {
    my_spinlock lock;
    char padding[CACHELINE_SIZE - sizeof(my_spinlock)];
  };

  PerCpuLock() {
    n_cpus_ = base::NumCPUs();
    CHECK_GT(n_cpus_, 0);
    locks_ = new PaddedLock[n_cpus_];
  }

  ~PerCpuLock() {
    delete [] locks_;
  }

  my_spinlock *get_lock() {
    int cpu = sched_getcpu();
    CHECK_LT(cpu, n_cpus_);
    return &locks_[cpu].lock;
  }

  int n_cpus_;
  PaddedLock *locks_;

};

struct SharedData {
  kudu::rw_spinlock rw_spinlock;
  kudu::RWMutex rwlock;
  std::mutex lock;
  kudu::percpu_rwlock per_cpu;
};


class NoopLock {
 public:
  void lock() {}
  void unlock() {}
};

// Some trivial workload to be done while
// holding the lock.
static float workload(float result) {
  for (int i = 0; i < 1; i++) {
    result += 1;
    result *= 2.1;
  }
  return result;
}

// Add a dependency on result - this will never
// be true, but prevents compiler optimizations
// from killing off the workload call
static void depend_on(float val) {
  if (val == 12345.0) {
    printf("hello world");
  }
}

void shared_rwlock_entry(SharedData *shared) {
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    shared->rwlock.lock_shared();
    result += workload(result);
    shared->rwlock.unlock_shared();
  }
  depend_on(result);
}

void shared_rw_spinlock_entry(SharedData *shared) {
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    shared->rw_spinlock.lock_shared();
    result += workload(result);
    shared->rw_spinlock.unlock_shared();
  }
  depend_on(result);
}

void shared_mutex_entry(SharedData *shared) {
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    shared->lock.lock();
    result += workload(result);
    shared->lock.unlock();
  }
  depend_on(result);
}

template<class LockType>
void own_mutex_entry() {
  LockType mylock;
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    mylock.lock();
    result += workload(result);
    mylock.unlock();
  }

  depend_on(result);
}

void percpu_rwlock_entry(SharedData *shared) {
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    kudu::rw_spinlock &l = shared->per_cpu.get_lock();
    l.lock_shared();
    result += workload(result);
    l.unlock_shared();
  }

  depend_on(result);
}


enum TestMethod {
  SHARED_RWLOCK,
  SHARED_MUTEX,
  OWN_MUTEX,
  OWN_SPINLOCK,
  PERCPU_RWLOCK,
  NO_LOCK,
  RW_SPINLOCK
};

void test_shared_lock(int num_threads, TestMethod method, const char *name) {
  vector<thread> threads;
  SharedData shared;

  for (int i = 0; i < num_threads; i++) {
    switch (method) {
      case SHARED_RWLOCK:
        threads.emplace_back(shared_rwlock_entry, &shared);
        break;
      case SHARED_MUTEX:
        threads.emplace_back(shared_mutex_entry, &shared);
        break;
      case OWN_MUTEX:
        threads.emplace_back(own_mutex_entry<std::mutex>);
        break;
      case OWN_SPINLOCK:
        threads.emplace_back(own_mutex_entry<my_spinlock>);
        break;
      case NO_LOCK:
        threads.emplace_back(own_mutex_entry<NoopLock>);
        break;
      case PERCPU_RWLOCK:
        threads.emplace_back(percpu_rwlock_entry, &shared);
        break;
      case RW_SPINLOCK:
        threads.emplace_back(shared_rw_spinlock_entry, &shared);
        break;
      default:
        CHECK(0) << "bad method: " << method;
    }
  }

  int64_t start = CycleClock::Now();
  for (thread& thr : threads) {
    thr.join();
  }
  int64_t end = CycleClock::Now();

  printf("%13s  % 7d  %" PRId64 "M\n", name, num_threads, (end-start)/1000000);
}

int main(int argc, char **argv) {
  kudu::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }
  kudu::InitGoogleLoggingSafe(argv[0]);

  printf("        Test   Threads  Cycles\n");
  printf("------------------------------\n");

  for (int num_threads = 1; num_threads <= FLAGS_num_threads; num_threads++) {
    test_shared_lock(num_threads, SHARED_RWLOCK, "shared_rwlock");
    test_shared_lock(num_threads, SHARED_MUTEX, "shared_mutex");
    test_shared_lock(num_threads, OWN_MUTEX, "own_mutex");
    test_shared_lock(num_threads, OWN_SPINLOCK, "own_spinlock");
    test_shared_lock(num_threads, NO_LOCK, "no_lock");
    test_shared_lock(num_threads, PERCPU_RWLOCK, "percpu_rwlock");
    test_shared_lock(num_threads, RW_SPINLOCK, "rw_spinlock");
  }

}
