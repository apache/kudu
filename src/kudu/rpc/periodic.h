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

#include <functional>
#include <memory>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/macros.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"

namespace kudu {
namespace rpc {

class Messenger;

// Repeatedly runs a task on a fixed period.
//
// PeriodicTimer is built using Messenger::ScheduleOnReactor() as its
// scheduling primitive. This is merely for convenience; it could also be built
// on libev, a hashed wheel timer, or something equivalent.
//
// PeriodicTimers have shared ownership, but that's largely an implementation
// detail to support asynchronous stopping. Users can treat them as exclusively
// owned (though care must be taken when writing the task functor; see Stop()
// for more details).
//
// TODO(adar): eventually we should build directly on libev as it supports
// timer cancelation, which would allow us to implement synchronous Stop(), use
// exclusive ownership, and remove the restriction that the delta passed
// into Snooze() be greater than GetMinimumPeriod().
class PeriodicTimer : public std::enable_shared_from_this<PeriodicTimer> {
 public:
  typedef std::function<void(void)> RunTaskFunctor;

  // Creates a new PeriodicTimer.
  //
  // A ref is taken on 'messenger', which is used for scheduling callbacks.
  //
  // 'functor' defines the user's task and is owned for the lifetime of the
  // PeriodicTimer. The task will run on the messenger's reactor threads so it
  // should do very little work (i.e. no I/O).
  //
  // 'period' defines the period between tasks while 'jitter_pct' (which must
  // be between 0 and 1) defines the percentage of the period that will be
  // jittered up or down randomly. Taken together, the periodicity of the
  // timer varies between (1-J)*P and (1+J)*P.
  static std::shared_ptr<PeriodicTimer> Create(
      std::shared_ptr<Messenger> messenger,
      RunTaskFunctor functor,
      MonoDelta period,
      double jitter_pct = 0.25);

  ~PeriodicTimer();

  // Starts the timer.
  //
  // The timer's task will run in accordance with the period and jitter mode
  // provided during timer construction.
  //
  // If 'next_task_delta' is set, it is used verbatim as the delay for the very
  // first task, with the configured period and jitter mode only applying to
  // subsequent tasks.
  //
  // Does nothing if the timer was already started.
  void Start(boost::optional<MonoDelta> next_task_delta = boost::none);

  // Snoozes the timer for one period.
  //
  // If 'next_task_delta' is set, it is used verbatim as the delay for the next
  // task. Subsequent tasks will revert to the timer's regular period. The
  // value of 'next_task_delta' must be greater than GetMinimumPeriod();
  // otherwise the task is not guaranteed to run in a timely manner.
  //
  // Note: Snooze() is not additive. That is, if called at time X and again at
  // time X + P/2, the timer is snoozed until X+P/2, not X+2P.
  //
  // Does nothing if the timer is stopped.
  void Snooze(boost::optional<MonoDelta> next_task_delta = boost::none);

  // Stops the timer.
  //
  // Stopping is asynchronous; that is, it is still possible for the task to
  // run after Stop() returns. Because of this, the task's functor should be
  // written to do nothing if objects it depends on have been destroyed.
  //
  // Does nothing if the timer is already stopped.
  void Stop();

 private:
  PeriodicTimer(std::shared_ptr<Messenger> messenger,
                RunTaskFunctor functor,
                MonoDelta period,
                double jitter_pct);

  // Calculate the minimum period for the timer, which varies depending on
  // 'jitter_pct_' and the output of the PRNG.
  MonoDelta GetMinimumPeriod();

  // Called by Messenger::ScheduleOnReactor when the timer fires.
  void Callback();

  // Schedules invocations of Callback() in the future.
  std::shared_ptr<Messenger> messenger_;

  // User-defined task functor.
  RunTaskFunctor functor_;

  // User-specified task period.
  const MonoDelta period_;

  // User-specified jitter percentage.
  const double jitter_pct_;

  // Protects 'rng_' and 'next_task_time_'.
  simple_spinlock lock_;

  // PRNG used when generating jitter.
  Random rng_;

  // Whether the timer is running or not.
  AtomicBool started_;

  // The next time at which the task's functor should be run.
  MonoTime next_task_time_;

  ALLOW_MAKE_SHARED(PeriodicTimer);

  DISALLOW_COPY_AND_ASSIGN(PeriodicTimer);
};

} // namespace rpc
} // namespace kudu
