// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: tkaftal@google.com (Tomasz Kaftal)
//
// The file provides various timer extensions based on the boost CPU timer
// library.
#ifndef SUPERSONIC_OPENSOURCE_TIMER_TIMER_H_
#define SUPERSONIC_OPENSOURCE_TIMER_TIMER_H_



#include "gutil/integral_types.h"
#include "gutil/macros.h"

#ifndef HAVE_BOOST_TIMER
#include "gutil/fake_boost_timer.h"
#else 
#include <boost/timer.hpp>
#endif
namespace bt = boost::timer;

// Converts time from boost nanoseconds into a double second representation.
inline static double toSeconds(const bt::nanosecond_type& nano) {
  return nano / static_cast<double>(1e9);
}
// Converts time from boost nanoseconds into int64 microsecond format.
inline static int64 toUSeconds(const bt::nanosecond_type& nano) {
  return static_cast<int64>(nano / 1e3);
}

// Abstract base class for lightweight timers defined below, ensures that
// the core functionalities maintain the same interface.
class TimerBase {
 public:
  TimerBase() : timer_(), start_fresh_(true) { }

  // Starts the timer.
  virtual void Start();

  // Stops the timer, but does not reset to zero.
  virtual void Stop();

  // Sets the elapsed counter to zero and stops if running.
  virtual void Reset();

  // Restarts the timer counting from zero.
  virtual void Restart();

  virtual double Get() const ABSTRACT;        // get the value in seconds
  virtual int64 GetInUsec() const ABSTRACT;   // get the value in microseconds

 protected:
  // Underlying boost timer.
  bt::cpu_timer timer_;

  // Switch saying whether the timer should pick up where it left off
  // or start fresh.
  bool start_fresh_;
};

// TODO(tkaftal): Inline virtual functions don't seem like a great idea, but
// both spy.cc and benchmark.cc use statically defined timers, so the calls may
// actually get inlined, plus we don't want overhead on timing. Does this
// make sense?
inline void TimerBase::Start() {
  // Do nothing, if timer is running.
  if (!timer_.is_stopped()) {
    return;
  }

  if (start_fresh_) {
    timer_.start();
  } else {
    timer_.resume();
  }
}

inline void TimerBase::Stop() {
  timer_.stop();
  start_fresh_ = false;
}

inline void TimerBase::Reset() {
  timer_.stop();
  start_fresh_ = true;
}

inline void TimerBase::Restart() {
  timer_.start();
}

// A timer class for measuring wall time.
// TODO(tkaftal): Add more precise info on accuracy, based on boost doc.
class WallTimer : public TimerBase {
 public:
  virtual double Get() const;
  virtual int64 GetInUsec() const;
};

inline double WallTimer::Get() const {
  return toSeconds(timer_.elapsed().wall);
}

inline int64 WallTimer::GetInUsec() const {
  return toUSeconds(timer_.elapsed().wall);
}

// Class for user-time measurements.
// TODO(tkaftal): Add more precise info on accuracy, based on boost doc.
class UserTimer : public TimerBase {
 public:
  virtual double Get() const;
  virtual int64 GetInUsec() const;
};

inline double UserTimer::Get() const {
  return toSeconds(timer_.elapsed().user);
}

inline int64 UserTimer::GetInUsec() const {
  return toUSeconds(timer_.elapsed().user);
}

// Class for measuring time spent in kernel.
// TODO(tkaftal): Add more precise info on accuracy, based on boost doc.
class SystemTimer : public TimerBase {
 public:
  virtual double Get() const;
  virtual int64 GetInUsec() const;
};

inline double SystemTimer::Get() const {
  return toSeconds(timer_.elapsed().system);
}

inline int64 SystemTimer::GetInUsec() const {
  return toUSeconds(timer_.elapsed().system);
}
#endif  // SUPERSONIC_OPENSOURCE_TIMER_TIMER_H_
