// Copyright (c) 2012, Cloudera, inc
#ifndef KUDU_UTIL_STOPWATCH_H
#define KUDU_UTIL_STOPWATCH

#include <glog/logging.h>

#include "gutil/stringprintf.h"

namespace kudu {

// Macro for logging timing of a block. Usage:
//   LOG_TIMING(INFO, "doing some task")
// yields a log like:
// I1102 14:35:51.726186 23082 file.cc:167] Times for doing some task: real 3.729s user 3.570s sys 0.150s
#define LOG_TIMING(severity, description) \
  for (LogTiming _l(__FILE__, __LINE__, google::severity, description); \
       !_l.has_printed();                                               \
       _l.Print())


class Stopwatch;

typedef uint64_t nanosecond_type;

struct CpuTimes
{
  nanosecond_type wall;
  nanosecond_type user;
  nanosecond_type system;

  void clear() { wall = user = system = 0LL; }

  static const double NANOS_PER_SECOND = 1000000000.0;

  std::string ToString() const {
    return StringPrintf(
      "real %.3fs\tuser %.3fs\tsys %.3fs",
      wall/NANOS_PER_SECOND, user/NANOS_PER_SECOND, system/NANOS_PER_SECOND);
  }
};

class Stopwatch {
public:
  Stopwatch() :
      stopped_(true)
  {
    times_.clear();
  }

  void start() {
    stopped_ = false;
    GetTimes(&times_);
  }

  void stop() {
    if (stopped_) return;
    stopped_ = true;

    CpuTimes current;
    GetTimes(&current);
    times_.wall = current.wall - times_.wall;
    times_.user = current.user - times_.user;
    times_.system = current.system - times_.system;
  }

  CpuTimes elapsed() const {
    if (stopped_) return times_;

    CpuTimes current;
    GetTimes(&current);
    current.wall -= times_.wall;
    current.user -= times_.user;
    current.system -= times_.system;
    return current;
  }

  void resume() {
    if (!stopped_) return;

    CpuTimes current (times_);
    start();
    times_.wall   -= current.wall;
    times_.user   -= current.user;
    times_.system -= current.system;
  }

  bool is_stopped() const {
    return stopped_;
  }

private:
  static void GetTimes(CpuTimes *times) {
    struct rusage usage;
    CHECK_EQ(0, getrusage(RUSAGE_THREAD, &usage));
    struct timespec wall;

    CHECK_EQ(0, clock_gettime(CLOCK_MONOTONIC, &wall));
    times->wall   = wall.tv_sec * 1000000000L + wall.tv_nsec;
    times->user   = usage.ru_utime.tv_sec * 1000000000L + usage.ru_utime.tv_usec * 1000;
    times->system = usage.ru_stime.tv_sec * 1000000000L + usage.ru_stime.tv_usec * 1000;

  }

  bool stopped_;

  CpuTimes times_;
};


class LogTiming {
public:
  LogTiming(const char *file, int line, google::LogSeverity severity,
            const std::string &description) :
    file_(file),
    line_(line),
    severity_(severity),
    description_(description),
    has_printed_(false)
  {
    stopwatch_.start();
  }

  void Print() {
    stopwatch_.stop();
    google::LogMessage(file_, line_, severity_).stream()
      << "Times for " << description_ << ": "
      << stopwatch_.elapsed().ToString();
    has_printed_ = true;
  }

  bool has_printed() {
    return has_printed_;
  }

private:
  Stopwatch stopwatch_;
  const char *file_;
  const int line_;
  const google::LogSeverity severity_;
  const std::string description_;
  bool has_printed_;
};


}

#endif
