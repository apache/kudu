// Copyright 2012 Google Inc. All Rights Reserved.
//
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
//
// Author: tkaftal@google.com (Tomasz Kaftal)
//
// The implementation of walltime functionalities.
#ifndef _GNU_SOURCE   // gcc3 at least defines it on the command line
#define _GNU_SOURCE   // Linux wants that for strptime in time.h
#endif

#include "kudu/gutil/walltime.h"

#if defined(__APPLE__)
#include <mach/clock.h>
#include <mach/mach.h>
#endif  // defined(__APPLE__)

#if defined(__APPLE__)
namespace walltime_internal {

GoogleOnceType timebase_info_once = GOOGLE_ONCE_INIT;
mach_timebase_info_data_t timebase_info;

void InitializeTimebaseInfo() {
  CHECK_EQ(KERN_SUCCESS, mach_timebase_info(&timebase_info))
      << "unable to initialize mach_timebase_info";
}
} // namespace walltime_internal
#endif

static void StringAppendStrftime(std::string* dst,
                                 const char* format,
                                 const struct tm* tm) {
  char space[1024];

  int result = strftime(space, sizeof(space), format, tm);

  if ((result >= 0) && (result < sizeof(space))) {
    // It fit
    dst->append(space, result);
    return;
  }

  int length = sizeof(space);
  for (int sanity = 0; sanity < 5; ++sanity) {
    length *= 2;
    auto buf = new char[length];

    result = strftime(buf, length, format, tm);
    if ((result >= 0) && (result < length)) {
      // It fit
      dst->append(buf, result);
      delete[] buf;
      return;
    }

    delete[] buf;
  }

  // sanity failure
  return;
}

WallTime WallTime_Now() {
#if defined(__APPLE__)
  mach_timespec_t ts;
  walltime_internal::GetCurrentTime(&ts);
  return ts.tv_sec + ts.tv_nsec / static_cast<double>(1e9);
#else
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec + ts.tv_nsec / static_cast<double>(1e9);
#endif  // defined(__APPLE__)
}

void StringAppendStrftime(std::string* dst,
                          const char* format,
                          time_t when,
                          bool local) {
  struct tm tm;
  bool conversion_error;
  if (local) {
    conversion_error = (localtime_r(&when, &tm) == nullptr);
  } else {
    conversion_error = (gmtime_r(&when, &tm) == nullptr);
  }
  if (conversion_error) {
    // If we couldn't convert the time, don't append anything.
    return;
  }
  StringAppendStrftime(dst, format, &tm);
}

std::string TimestampAsString(time_t timestamp_secs) {
  std::string ret;
  StringAppendStrftime(&ret, "%Y-%m-%d %H:%M:%S %Z", timestamp_secs, true);
  return ret;
}

std::string LocalTimeAsString() {
  return TimestampAsString(time(nullptr));
}
