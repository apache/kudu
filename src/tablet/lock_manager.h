// Copyright (c) 2013, Cloudera,inc.
// All rights reserved.
#ifndef KUDU_TABLET_LOCK_MANAGER_H
#define KUDU_TABLET_LOCK_MANAGER_H

#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include "util/slice.h"

namespace kudu { namespace tablet {

class LockManager;
struct LockEntry;

// The entry returned to a thread which has taken a lock.
// Callers should generally use ScopedRowLock (see below).
struct LockEntry : boost::noncopyable {
  boost::mutex *mutex;
};

// Super-simple lock manager implementation. This only supports exclusive
// locks, and makes no attempt to prevent deadlocks if a single thread
// takes multiple locks. Additionally, the locking is just based on the
// hash of the key, so two different lock keys may actually conflict.
//
// In the future when we want to support multi-row transactions of some kind
// we'll have to implement a proper lock manager with all its trappings,
// but this should be enough for the single-row use case.
class LockManager : boost::noncopyable {
 public:
  LockManager();

  enum LockStatus {
    LOCK_ACQUIRED = 0
  };

  enum LockMode {
    LOCK_EXCLUSIVE
  };

 private:
  friend class ScopedRowLock;

  LockStatus Lock(const Slice &key, LockMode mode, LockEntry *entry);
  void Release(LockEntry *lock);

  gscoped_array<boost::mutex> locks_;

  enum {
    kNumShards = 1024
  };
};


// Hold a lock on a given row, for the scope of this object.
// Usage:
//   {
//     ScopedRowLock(&manager, my_encoded_row_key, LOCK_EXCLUSIVE);
//     .. do stuff with the row ..
//   }
//   // lock is released when the object exits its scope.
class ScopedRowLock : boost::noncopyable {
 public:
  // Lock row in the given LockManager. The 'key' slice must remain
  // valid and un-changed for the duration of this object's lifetime.
  ScopedRowLock(LockManager *manager, const Slice &key,
                LockManager::LockMode mode);

  void Release();

  ~ScopedRowLock();

 private:
  LockManager *manager_;
  const Slice key_;
  const LockManager::LockMode mode_;

  bool acquired_;
  LockEntry entry_;
};

} // namespace tablet
} // namespace kudu
#endif
