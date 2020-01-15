// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/util/condition_variable.h"

#include <sys/time.h>

#include <cerrno>
#include <cstdint>
#include <ctime>
#include <ostream>

#include <glog/logging.h>

#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/thread_restrictions.h"

namespace kudu {

ConditionVariable::ConditionVariable(Mutex* user_lock)
    : user_mutex_(&user_lock->native_handle_)
#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
    , user_lock_(user_lock)
#endif
{
  int rv = 0;
#if defined(__APPLE__)
  rv = pthread_cond_init(&condition_, nullptr);
#else
  // On Linux we can't use relative times like on macOS; reconfiguring the
  // condition variable to use the monotonic clock means we can use support
  // WaitFor with our MonoTime implementation.
  pthread_condattr_t attrs;
  rv = pthread_condattr_init(&attrs);
  DCHECK_EQ(0, rv);
  pthread_condattr_setclock(&attrs, CLOCK_MONOTONIC);
  rv = pthread_cond_init(&condition_, &attrs);
  pthread_condattr_destroy(&attrs);
#endif
  DCHECK_EQ(0, rv);
}

ConditionVariable::~ConditionVariable() {
  int rv = pthread_cond_destroy(&condition_);
  DCHECK_EQ(0, rv);
}

void ConditionVariable::Wait() const {
  ThreadRestrictions::AssertWaitAllowed();
#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
  user_lock_->CheckHeldAndUnmark();
#endif
  int rv = pthread_cond_wait(&condition_, user_mutex_);
  DCHECK_EQ(0, rv);
#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
  user_lock_->CheckUnheldAndMark();
#endif
}

bool ConditionVariable::WaitUntil(const MonoTime& until) const {
  ThreadRestrictions::AssertWaitAllowed();

  // Have we already timed out?
  MonoTime now = MonoTime::Now();
  if (now > until) {
    return false;
  }

#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
  user_lock_->CheckHeldAndUnmark();
#endif

#if defined(__APPLE__)
  // macOS does not provide a way to configure pthread_cond_timedwait() to use
  // monotonic clocks, so we must convert the deadline into a delta and perform
  // a relative wait.
  MonoDelta delta = until - now;
  struct timespec relative_time;
  delta.ToTimeSpec(&relative_time);
  int rv = pthread_cond_timedwait_relative_np(
      &condition_, user_mutex_, &relative_time);
#else
  struct timespec absolute_time;
  until.ToTimeSpec(&absolute_time);
  int rv = pthread_cond_timedwait(&condition_, user_mutex_, &absolute_time);
#endif
  DCHECK(rv == 0 || rv == ETIMEDOUT)
    << "unexpected pthread_cond_timedwait return value: " << rv;

#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
  user_lock_->CheckUnheldAndMark();
#endif
  return rv == 0;
}

bool ConditionVariable::WaitFor(const MonoDelta& delta) const {
  ThreadRestrictions::AssertWaitAllowed();

  // Negative delta means we've already timed out.
  int64_t nsecs = delta.ToNanoseconds();
  if (nsecs < 0) {
    return false;
  }

#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
  user_lock_->CheckHeldAndUnmark();
#endif

#if defined(__APPLE__)
  struct timespec relative_time;
  delta.ToTimeSpec(&relative_time);
  int rv = pthread_cond_timedwait_relative_np(
      &condition_, user_mutex_, &relative_time);
#else
  // The timeout argument to pthread_cond_timedwait is in absolute time.
  struct timespec absolute_time;
  MonoTime deadline = MonoTime::Now() + delta;
  deadline.ToTimeSpec(&absolute_time);
  int rv = pthread_cond_timedwait(&condition_, user_mutex_, &absolute_time);
#endif

  DCHECK(rv == 0 || rv == ETIMEDOUT)
    << "unexpected pthread_cond_timedwait return value: " << rv;
#ifdef FB_DO_NOT_REMOVE  // #ifndef NDEBUG
  user_lock_->CheckUnheldAndMark();
#endif
  return rv == 0;
}

void ConditionVariable::Broadcast() {
  int rv = pthread_cond_broadcast(&condition_);
  DCHECK_EQ(0, rv);
}

void ConditionVariable::Signal() {
  int rv = pthread_cond_signal(&condition_);
  DCHECK_EQ(0, rv);
}

}  // namespace kudu
