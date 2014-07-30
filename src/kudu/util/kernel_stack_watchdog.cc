// Copyright (c) 2014, Cloudera, inc.

#include "kudu/util/kernel_stack_watchdog.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <tr1/unordered_set>

#include "kudu/util/env.h"
#include "kudu/util/thread.h"
#include "kudu/util/status.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"

DEFINE_int32(hung_kernel_task_threshold_ms, 500,
             "Number of milliseconds after which the kernel task watchdog "
             "will log stack traces of hung threads.");
DEFINE_int32(hung_kernel_task_check_interval_ms, 200,
             "Number of milliseconds in between checks for hung kernel threads");

using std::tr1::unordered_set;
using strings::Substitute;

namespace kudu {

KernelStackWatchdog::KernelStackWatchdog()
  : finish_(1) {
  CHECK_OK(Thread::Create("kernel-watchdog", "kernel-watcher",
                          boost::bind(&KernelStackWatchdog::RunThread, this),
                          &thread_));
}

KernelStackWatchdog::~KernelStackWatchdog() {
  finish_.CountDown();
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
}

void KernelStackWatchdog::Watch(pid_t pid, const char* label) {
  Entry e;
  e.label = label;
  e.registered_time = MonoTime::Now(MonoTime::FINE);

  boost::lock_guard<simple_spinlock> l(lock_);
  InsertOrDie(&watched_, pid, e);
}

void KernelStackWatchdog::StopWatching(pid_t pid) {
  boost::lock_guard<simple_spinlock> l(lock_);
  watched_.erase(pid);
}

void KernelStackWatchdog::RunThread() {
  MonoTime next_check_time = MonoTime::Now(MonoTime::FINE);
  while (true) {
    if (finish_.WaitUntil(next_check_time)) {
      // Watchdog exiting.
      break;
    }
    MonoTime now = MonoTime::Now(MonoTime::FINE);
    next_check_time = now;
    next_check_time.AddDelta(MonoDelta::FromMilliseconds(
                               FLAGS_hung_kernel_task_check_interval_ms));

    PidMap pids;
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      pids = watched_;
    }

    BOOST_FOREACH(const PidMap::value_type& map_entry, pids) {
      pid_t p = map_entry.first;
      const Entry& e = map_entry.second;

      int paused_ms = now.GetDeltaSince(e.registered_time).ToMilliseconds();
      if (paused_ms > FLAGS_hung_kernel_task_threshold_ms) {
        faststring ret;
        Status s = ReadFileToString(Env::Default(), Substitute("/proc/$0/stack", p), &ret);
        if (!s.ok()) {
          // Can't read the kernel stack of the pid -- it's possible that the thread exited
          // while we were iterating, so just ignore it.
          continue;
        }

        LOG(WARNING) << "Thread " << p << " stuck at " << e.label
                     << " for " << paused_ms << "ms" << ":\n"
                     << ret.ToString();
      }
    }
  }
}

} // namespace kudu
