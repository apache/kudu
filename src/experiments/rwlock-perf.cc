#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/smart_ptr/detail/spinlock.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "gutil/walltime.h"

#include <unistd.h>

DEFINE_int32(num_threads, 8, "Number of threads to test");

using namespace std;


class my_spinlock : public boost::detail::spinlock
{ 
private: 
    my_spinlock( my_spinlock const& ); 
    my_spinlock & operator=( my_spinlock const& ); 

public: 

  my_spinlock() {
    v_ = 0;
  }

}; 


struct per_cpu_lock {
  struct padded_lock {
    my_spinlock lock;
    char padding[CACHELINE_SIZE - sizeof(my_spinlock)];
  };

  per_cpu_lock() {
    errno = 0;
    n_cpus_ = sysconf(_SC_NPROCESSORS_CONF);
    CHECK_EQ(errno, 0) << strerror(errno);
    CHECK_GT(n_cpus_, 0);
    locks_ = new padded_lock[n_cpus_];
  }

  ~per_cpu_lock() {
    delete [] locks_;
  }

  my_spinlock *get_lock() {
    int cpu = sched_getcpu();
    CHECK_LT(cpu, n_cpus_);
    return &locks_[cpu].lock;
  }

  int n_cpus_;
  padded_lock *locks_;

};


struct shared_data {
  shared_data() {
    errno = 0;
  }

  boost::shared_mutex rwlock;
  boost::mutex lock;
  per_cpu_lock per_cpu;
};


class noop_lock {
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

void shared_rwlock_entry(shared_data *shared) {
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    shared->rwlock.lock_shared();
    result += workload(result);
    shared->rwlock.unlock_shared();
  }
  depend_on(result);
}

void shared_mutex_entry(shared_data *shared) {
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

void cpuid_spinlock_entry(shared_data *shared) {
  float result = 1;
  for (int i = 0; i < 1000000; i++) {
    my_spinlock *l = shared->per_cpu.get_lock();
    l->lock();
    result += workload(result);
    l->unlock();
  }

  depend_on(result);
}


enum TestMethod {
  SHARED_RWLOCK,
  SHARED_MUTEX,
  OWN_MUTEX,
  OWN_SPINLOCK,
  CPUID_SPINLOCK,
  NO_LOCK
};

void test_shared_lock(int num_threads,
                      TestMethod method,
                      const char *name) {
  boost::ptr_vector<boost::thread> threads;
  shared_data shared;

  for (int i = 0; i < num_threads; i++) {
    switch (method) {
      case SHARED_RWLOCK:
        threads.push_back(new boost::thread(
                            shared_rwlock_entry, &shared));
        break;
      case SHARED_MUTEX:
        threads.push_back(new boost::thread(
                            shared_mutex_entry, &shared));
        break;
      case OWN_MUTEX:
        threads.push_back(new boost::thread(
                            own_mutex_entry<boost::mutex>));
        break;
      case OWN_SPINLOCK:
        threads.push_back(new boost::thread(
                            own_mutex_entry<my_spinlock>));
        break;
      case NO_LOCK:
        threads.push_back(new boost::thread(
                            own_mutex_entry<noop_lock>));
        break;
      case CPUID_SPINLOCK:
        threads.push_back(new boost::thread(
                            cpuid_spinlock_entry, &shared));
        break;
      default:
        CHECK(0) << "bad method: " << method;
    }
  }

  int64_t start = CycleClock::Now();
  BOOST_FOREACH(boost::thread &thr, threads) {
    thr.join();
  }
  int64_t end = CycleClock::Now();

  printf("%13s  % 7d  %ldM\n",
         name, num_threads, (end-start)/1000000);
}

int main(int argc, char **argv) {
  printf("        Test   Threads  Cycles\n");
  printf("------------------------------\n");

  for (int num_threads = 1;
       num_threads < FLAGS_num_threads;
       num_threads++) {
//    test_shared_lock(num_threads, SHARED_RWLOCK, "shared_rwlock");
//    test_shared_lock(num_threads, SHARED_MUTEX, "shared_mutex");
    test_shared_lock(num_threads, OWN_MUTEX, "own_mutex");
    test_shared_lock(num_threads, OWN_SPINLOCK, "own_spinlock");
    test_shared_lock(num_threads, NO_LOCK, "no_lock");
    test_shared_lock(num_threads, CPUID_SPINLOCK, "cpuid_lock");
  }

}
