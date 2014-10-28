// Copyright (c) 2012, Cloudera, inc,
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_BOOST_MUTEX_UTILS_H
#define KUDU_BOOST_MUTEX_UTILS_H


// Similar to boost::lock_guard except that it takes
// a lock pointer, and checks against NULL. If the
// pointer is NULL, does nothing. Otherwise guards
// with the lock.
template<class LockType>
class lock_guard_maybe {
 public:
  explicit lock_guard_maybe(LockType *l) :
    lock_(l) {
    if (l != NULL) {
      l->lock();
    }
  }

  ~lock_guard_maybe() {
    if (lock_ != NULL) {
      lock_->unlock();
    }
  }

 private:
  LockType *lock_;
};

#endif
