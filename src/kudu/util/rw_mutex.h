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

#pragma once

#include <pthread.h>

#include "kudu/gutil/macros.h"

namespace kudu {

// Read/write mutex.
//
// Implemented as a thin wrapper around pthread_rwlock_t.
class RWMutex {
 public:

  // Possible fairness policies for the RWMutex.
  enum class Priority {
    // The lock will prioritize readers at the expense of writers.
    PREFER_READING,

    // The lock will prioritize writers at the expense of readers.
    //
    // Care should be taken when using this fairness policy, as it can lead to
    // unexpected deadlocks (e.g. a writer waiting on the lock will prevent
    // additional readers from acquiring it).
    PREFER_WRITING,
  };

  // Create an RWMutex that prioritizes readers.
  RWMutex();

  // Create an RWMutex with customized priority. This is a best effort; the
  // underlying platform may not support custom priorities.
  explicit RWMutex(Priority prio);

  ~RWMutex();

  void ReadLock();
  void ReadUnlock();
  bool TryReadLock();

  void WriteLock();
  void WriteUnlock();
  bool TryWriteLock();

  // Aliases for use with std::lock_guard and kudu::shared_lock.
  void lock() { WriteLock(); }
  void unlock() { WriteUnlock(); }
  bool try_lock() { return TryWriteLock(); }
  void lock_shared() { ReadLock(); }
  void unlock_shared() { ReadUnlock(); }
  bool try_lock_shared() { return TryReadLock(); }

 private:
  void Init(Priority prio);

  pthread_rwlock_t native_handle_;

  DISALLOW_COPY_AND_ASSIGN(RWMutex);
};

} // namespace kudu
