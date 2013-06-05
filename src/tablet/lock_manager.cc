// Copyright (c) 2013, Cloudera,inc.
// All rights reserved.

#include <boost/thread/mutex.hpp>
#include <glog/logging.h>

#include "gutil/hash/city.h"
#include "tablet/lock_manager.h"

namespace kudu { namespace tablet {

ScopedRowLock::ScopedRowLock(LockManager *manager, const Slice &key,
                             LockManager::LockMode mode) :
  manager_(manager),
  key_(key),
  mode_(mode),
  acquired_(false)
{
  DCHECK_NOTNULL(manager_);
  LockManager::LockStatus ls = manager_->Lock(key, mode, &entry_);

  // We currently only support single-row mutations in this super-simple
  // lock manager, so we should always be able to acquire a lock -- no
  // deadlocks possible.
  CHECK_EQ(ls, LockManager::LOCK_ACQUIRED);
  acquired_ = true;
}

ScopedRowLock::~ScopedRowLock() {
  if (acquired_) {
    Release();
  }
}

void ScopedRowLock::Release() {
  CHECK(acquired_) << "already released";
  manager_->Release(&entry_);
  acquired_ = false;
}

LockManager::LockManager() :
  locks_(new boost::mutex[kNumShards])
{}

LockManager::LockStatus LockManager::Lock(const Slice &key,
                                          LockManager::LockMode mode,
                                          LockEntry *entry) {
  uint64_t hash = util_hash::CityHash64(
    reinterpret_cast<const char *>(key.data()), key.size());
  entry->mutex = &locks_[hash % kNumShards];
  entry->mutex->lock();
  return LOCK_ACQUIRED;
}

void LockManager::Release(LockEntry *lock) {
  DCHECK_NOTNULL(lock->mutex)->unlock();
  lock->mutex = NULL;
}

} // namespace tablet
} // namespace kudu
