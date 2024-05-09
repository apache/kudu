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
#include <sys/types.h>

#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"

namespace kudu {

class StackTrace;

// A lock built around pthread_mutex_t. Does not allow recursion.
//
// The following checks will be performed in DEBUG mode:
//   Acquire(), TryAcquire() - the lock isn't already held.
//   Release() - the lock is already held by this thread.
//
class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Acquire();
  void Release();
  bool TryAcquire();

  void lock() { Acquire(); }
  void unlock() { Release(); }
  bool try_lock() { return TryAcquire(); }

#ifndef NDEBUG
  void AssertAcquired() const;
#else
  void AssertAcquired() const {}
#endif

 private:
  friend class ConditionVariable;

  pthread_mutex_t native_handle_;

#ifndef NDEBUG
  // Members and routines taking care of locks assertions.
  void CheckHeldAndUnmark();
  void CheckUnheldAndMark();
  std::string GetOwnerThreadInfo() const;

  // All private data is implicitly protected by native_handle_.
  // Be VERY careful to only access members under that lock.
  pid_t owning_tid_;
  std::unique_ptr<StackTrace> stack_trace_;
#endif

  DISALLOW_COPY_AND_ASSIGN(Mutex);
};

} // namespace kudu
