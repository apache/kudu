#ifndef KUDU_UTIL_PERCPU_RWLOCK_H
#define KUDU_UTIL_PERCPU_RWLOCK_H

#include <boost/smart_ptr/detail/spinlock.hpp>
#include <glog/logging.h>
#include "gutil/port.h"

namespace kudu {

// Simple subclass of boost::detail::spinlock since the superclass doesn't
// initialize its member to "unlocked" (for unknown reasons)
class simple_spinlock : public boost::detail::spinlock
{ 
private: 
    simple_spinlock( simple_spinlock const& ); 
    simple_spinlock & operator=( simple_spinlock const& ); 

public: 

  simple_spinlock() {
    v_ = 0;
  }
};


struct percpu_rwlock {
  struct padded_lock {
    simple_spinlock lock;
    char padding[CACHELINE_SIZE - sizeof(simple_spinlock)];
  };

  percpu_rwlock() {
    errno = 0;
    n_cpus_ = sysconf(_SC_NPROCESSORS_CONF);
    CHECK_EQ(errno, 0) << strerror(errno);
    CHECK_GT(n_cpus_, 0);
    locks_ = new padded_lock[n_cpus_];
  }

  ~percpu_rwlock() {
    delete [] locks_;
  }

  simple_spinlock &get_lock() {
    int cpu = sched_getcpu();
    CHECK_LT(cpu, n_cpus_);
    return locks_[cpu].lock;
  }

  void lock() {
    for (int i = 0; i < n_cpus_; i++) {
      locks_[i].lock.lock();
    }
  }

  void unlock() {
    for (int i = 0; i < n_cpus_; i++) {
      locks_[i].lock.unlock();
    }
  }

  int n_cpus_;
  padded_lock *locks_;

};



} // namespace kudu

#endif
