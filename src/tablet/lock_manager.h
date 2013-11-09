// Copyright (c) 2013, Cloudera,inc.
// All rights reserved.
#ifndef KUDU_TABLET_LOCK_MANAGER_H
#define KUDU_TABLET_LOCK_MANAGER_H

#include "gutil/macros.h"
#include "util/slice.h"

namespace kudu { namespace tablet {

class LockManager;
class LockTable;
class LockEntry;
class TransactionContext;

// Super-simple lock manager implementation. This only supports exclusive
// locks, and makes no attempt to prevent deadlocks if a single thread
// takes multiple locks.
//
// In the future when we want to support multi-row transactions of some kind
// we'll have to implement a proper lock manager with all its trappings,
// but this should be enough for the single-row use case.
class LockManager {
 public:
  LockManager();
  ~LockManager();

  enum LockStatus {
    LOCK_ACQUIRED = 0,
    LOCK_BUSY = 1,
  };

  enum LockMode {
    LOCK_EXCLUSIVE
  };

 private:
  friend class ScopedRowLock;
  friend class LockManagerTest;

  LockStatus Lock(const Slice& key, const TransactionContext* tx,
                  LockMode mode, LockEntry **entry);
  LockStatus TryLock(const Slice& key, const TransactionContext* tx,
                     LockMode mode, LockEntry **entry);
  void Release(LockEntry *lock);

  LockTable *locks_;

  DISALLOW_COPY_AND_ASSIGN(LockManager);
};


// Hold a lock on a given row, for the scope of this object.
// Usage:
//   {
//     ScopedRowLock(&manager, my_encoded_row_key, LOCK_EXCLUSIVE);
//     .. do stuff with the row ..
//   }
//   // lock is released when the object exits its scope.
class ScopedRowLock {
 public:
  // Lock row in the given LockManager. The 'key' slice must remain
  // valid and un-changed for the duration of this object's lifetime.
  ScopedRowLock(LockManager *manager, const TransactionContext* ctx,
                const Slice &key, LockManager::LockMode mode);

  void Release();

  ~ScopedRowLock();

 private:
  DISALLOW_COPY_AND_ASSIGN(ScopedRowLock);

  LockManager *manager_;

  bool acquired_;
  LockEntry *entry_;
};

} // namespace tablet
} // namespace kudu
#endif
