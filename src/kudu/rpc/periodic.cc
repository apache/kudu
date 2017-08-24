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

#include "kudu/rpc/periodic.h"

#include <algorithm>
#include <memory>
#include <mutex>

#include <boost/function.hpp>
#include <glog/logging.h>

#include "kudu/rpc/messenger.h"
#include "kudu/util/atomic.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"

using std::shared_ptr;
using std::weak_ptr;

namespace kudu {
namespace rpc {

shared_ptr<PeriodicTimer> PeriodicTimer::Create(
    shared_ptr<Messenger> messenger,
    RunTaskFunctor functor,
    MonoDelta period,
    double jitter_pct) {
  return std::make_shared<PeriodicTimer>(
      std::move(messenger), std::move(functor), period, jitter_pct);
}

PeriodicTimer::PeriodicTimer(
    shared_ptr<Messenger> messenger,
    RunTaskFunctor functor,
    MonoDelta period,
    double jitter_pct)
    : messenger_(std::move(messenger)),
      functor_(std::move(functor)),
      period_(period),
      jitter_pct_(jitter_pct),
      rng_(GetRandomSeed32()),
      started_(false) {
  DCHECK_GE(jitter_pct_, 0);
  DCHECK_LE(jitter_pct_, 1);
}

PeriodicTimer::~PeriodicTimer() {
  Stop();
}

void PeriodicTimer::Start(boost::optional<MonoDelta> next_task_delta) {
  if (!started_.CompareAndSwap(false, true)) {
    Snooze(std::move(next_task_delta));
    Callback();
  }
}

void PeriodicTimer::Stop() {
  started_.Store(false);
}

void PeriodicTimer::Snooze(boost::optional<MonoDelta> next_task_delta) {
  if (!started_.Load()) {
    return;
  }

  std::lock_guard<simple_spinlock> l(lock_);
  if (!next_task_delta) {
    // Given jitter percentage J and period P, this yields a delay somewhere
    // between (1-J)*P and (1+J)*P.
    next_task_delta = MonoDelta::FromMilliseconds(
        GetMinimumPeriod().ToMilliseconds() +
        rng_.NextDoubleFraction() *
        jitter_pct_ *
        (2 * period_.ToMilliseconds()));
  }
  next_task_time_ = MonoTime::Now() + *next_task_delta;
}

MonoDelta PeriodicTimer::GetMinimumPeriod() {
  // Given jitter percentage J and period P, this returns (1-J)*P, which is
  // the lowest possible jittered value.
  return MonoDelta::FromMilliseconds((1.0 - jitter_pct_) *
                                     period_.ToMilliseconds());
}

void PeriodicTimer::Callback() {
  if (!started_.Load()) {
    return;
  }

  // To simplify the implementation, a timer may have only one outstanding
  // callback scheduled at a time. This means that once the callback is
  // scheduled, the timer's task cannot run any earlier than whenever the
  // callback runs. Thus, the delay used when scheduling the callback dictates
  // the lowest possible value of 'next_task_delta' that Snooze() can honor.
  //
  // If the callback's delay is very low, Snooze() can honor a low
  // 'next_task_delta', but the callback will run often and burn more CPU
  // cycles. If the delay is very high, the timer will be more efficient but
  // the granularity for 'next_task_delta' will rise accordingly.
  //
  // As a "happy medium" we use GetMinimumPeriod() as the delay. This ensures
  // that a no-arg Snooze() on a jittered timer will always be honored, and as
  // long as the caller passes a value of at least GetMinimumPeriod() to
  // Snooze(), that too will be honored.
  MonoDelta delay = GetMinimumPeriod();
  bool run_task = false;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    MonoTime now = MonoTime::Now();
    if (now < next_task_time_) {
      // It's not yet time to run the task. Reduce the scheduled delay if
      // enough time has elapsed, but don't increase it.
      delay = std::min(delay, next_task_time_ - now);
    } else {
      // It's time to run the task. Although the next task time is reset now,
      // it may be reset again by virtue of running the task itself.
      run_task = true;
    }
  }
  if (run_task) {
    functor_();
    Snooze();
  }

  // Capture a weak_ptr reference into the submitted functor so that we can
  // safely handle the functor outliving its timer.
  weak_ptr<PeriodicTimer> w = shared_from_this();
  messenger_->ScheduleOnReactor([w](const Status& s) {
    if (!s.ok()) {
      // The reactor was shut down.
      return;
    }
    if (auto timer = w.lock()) {
      timer->Callback();
    }
  }, delay);
}

} // namespace rpc
} // namespace kudu
