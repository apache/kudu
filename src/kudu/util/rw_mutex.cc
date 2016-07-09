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

#include "kudu/util/rw_mutex.h"

#include <glog/logging.h>

namespace {

void unlock_rwlock(pthread_rwlock_t* rwlock) {
  int rv = pthread_rwlock_unlock(rwlock);
  DCHECK_EQ(0, rv) << strerror(rv);
}

} // anonymous namespace

namespace kudu {

RWMutex::RWMutex() {
  Init(Priority::PREFER_READING);
}

RWMutex::RWMutex(Priority prio) {
  Init(prio);
}

void RWMutex::Init(Priority prio) {
#ifdef __linux__
  // Adapt from priority to the pthread type.
  int kind = PTHREAD_RWLOCK_PREFER_READER_NP;
  switch (prio) {
    case Priority::PREFER_READING:
      kind = PTHREAD_RWLOCK_PREFER_READER_NP;
      break;
    case Priority::PREFER_WRITING:
      kind = PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP;
      break;
  }

  // Initialize the new rwlock with the user's preference.
  pthread_rwlockattr_t attr;
  int rv = pthread_rwlockattr_init(&attr);
  DCHECK_EQ(0, rv) << strerror(rv);
  rv = pthread_rwlockattr_setkind_np(&attr, kind);
  DCHECK_EQ(0, rv) << strerror(rv);
  rv = pthread_rwlock_init(&native_handle_, &attr);
  DCHECK_EQ(0, rv) << strerror(rv);
  rv = pthread_rwlockattr_destroy(&attr);
  DCHECK_EQ(0, rv) << strerror(rv);
#else
  int rv = pthread_rwlock_init(&native_handle_, NULL);
  DCHECK_EQ(0, rv) << strerror(rv);
#endif
}

RWMutex::~RWMutex() {
  int rv = pthread_rwlock_destroy(&native_handle_);
  DCHECK_EQ(0, rv) << strerror(rv);
}

void RWMutex::ReadLock() {
  int rv = pthread_rwlock_rdlock(&native_handle_);
  DCHECK_EQ(0, rv) << strerror(rv);
}

void RWMutex::ReadUnlock() {
  unlock_rwlock(&native_handle_);
}

bool RWMutex::TryReadLock() {
  int rv = pthread_rwlock_tryrdlock(&native_handle_);
  if (rv == EBUSY) {
    return false;
  }
  DCHECK_EQ(0, rv) << strerror(rv);
  return true;
}

void RWMutex::WriteLock() {
  int rv = pthread_rwlock_wrlock(&native_handle_);
  DCHECK_EQ(0, rv) << strerror(rv);
}

void RWMutex::WriteUnlock() {
  unlock_rwlock(&native_handle_);
}

bool RWMutex::TryWriteLock() {
  int rv = pthread_rwlock_trywrlock(&native_handle_);
  if (rv == EBUSY) {
    return false;
  }
  DCHECK_EQ(0, rv) << strerror(rv);
  return true;
}

} // namespace kudu
