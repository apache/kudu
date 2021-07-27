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

#include <mutex>

#include "kudu/util/monotime.h"
#include "kudu/util/locks.h"

namespace kudu {

// Using this class we can keep measure how long something took to run.
// Use the Start() and Stop() to start the stop the timer and TimeElapsed()
// to get the time taken to run and if still running, time elapsed until that point
class Timer {

 public:
  Timer()
      : start_time_(MonoTime::Min()),
        end_time_(MonoTime::Min()) {
  }

  void Start() {
    if (start_time_ == MonoTime::Min()) {
      start_time_ = MonoTime::Now();
    }
  }

  void Stop() {
    DCHECK_NE(start_time_, MonoTime::Min());
    end_time_ = MonoTime::Now();
  }

  MonoDelta TimeElapsed() const {
    if (start_time_ == MonoTime::Min() && end_time_ == MonoTime::Min()) {
      return MonoDelta::FromSeconds(0);
    }
    if (end_time_ == MonoTime::Min()) {
      return (MonoTime::Now() - start_time_);
    }
    return (end_time_ - start_time_);
  }

  bool IsStopped() const {
    return (end_time_ != MonoTime::Min());
  }

 private:
  std::atomic<MonoTime> start_time_;
  std::atomic<MonoTime> end_time_;
};

}  // namespace kudu
