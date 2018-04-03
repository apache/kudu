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

#include <thread>

#include <glog/logging.h>
#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

namespace kudu {

// Periodically checks the number of open file descriptors belonging to this
// process, crashing if it exceeds some upper bound.
class PeriodicOpenFdChecker {
 public:
  // path_pattern: a glob-style pattern of which paths should be included while
  //               counting file descriptors
  // upper_bound:  the maximum number of file descriptors that should be open
  //               at any point in time
  PeriodicOpenFdChecker(Env* env, std::string path_pattern, int upper_bound)
    : env_(env),
      path_pattern_(std::move(path_pattern)),
      initial_fd_count_(CountOpenFds(env, path_pattern_)),
      max_fd_count_(upper_bound + initial_fd_count_),
      running_(1),
      started_(false) {}

  ~PeriodicOpenFdChecker() { Stop(); }

  void Start() {
    DCHECK(!started_);
    running_.Reset(1);
    check_thread_ = std::thread(&PeriodicOpenFdChecker::CheckThread, this);
    started_ = true;
  }

  void Stop() {
    if (started_) {
      running_.CountDown();
      check_thread_.join();
      started_ = false;
    }
  }

 private:
  void CheckThread() {
    LOG(INFO) << strings::Substitute(
        "Periodic open fd checker starting for path pattern $0"
        "(initial: $1 max: $2)",
        path_pattern_, initial_fd_count_, max_fd_count_);
    do {
      int open_fd_count = CountOpenFds(env_, path_pattern_);
      KLOG_EVERY_N_SECS(INFO, 1) << strings::Substitute("Open fd count: $0/$1",
                                                        open_fd_count,
                                                        max_fd_count_);
      CHECK_LE(open_fd_count, max_fd_count_);
    } while (!running_.WaitFor(MonoDelta::FromMilliseconds(100)));
  }

  Env* env_;
  const std::string path_pattern_;
  const int initial_fd_count_;
  const int max_fd_count_;

  CountDownLatch running_;
  std::thread check_thread_;
  bool started_;
};

} // namespace kudu
