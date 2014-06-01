// Copyright (c) 2013, Cloudera, inc.
//
// This class defines a singleton thread which manages a map of other
// thread IDs to watch. Before performing some kernel operation which
// may stall (eg IO), threads may register themselves with the watchdog.
// Upon completion, they unregister themselves. The watchdog periodically
// wakes up, and if a thread has been registered longer than a configurable
// number of milliseconds, it will dump the kernel stack of that thread.
//
// This can be useful for diagnosing I/O stalls coming from the kernel,
// for example.
//
// A convenience macro SCOPED_WATCH_KERNEL_STACK is provided. Example usage:
//
// {
//   SCOPED_WATCH_KERNEL_STACK;
//   file->Write(...);
// }
//
// If the Write call takes too long, a stack will be logged to the console
// at WARNING level.
#ifndef KUDU_UTIL_KERNEL_STACK_WATCHDOG_H
#define KUDU_UTIL_KERNEL_STACK_WATCHDOG_H

#include <tr1/unordered_map>

#include <syscall.h>
#include "gutil/macros.h"
#include "gutil/singleton.h"
#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"
#include "util/locks.h"
#include "util/monotime.h"

#define SCOPED_WATCH_KERNEL_STACK() \
  ScopedWatchKernelStack _stack_watcher(__FILE__ ":" AS_STRING(__LINE__))

namespace kudu {

class Thread;

// Scoped object to register and unregister the calling thread with the
// watchdog.
class ScopedWatchKernelStack {
 public:
  explicit ScopedWatchKernelStack(const char* label);
  ~ScopedWatchKernelStack();

 private:
  const int tid_;
  DISALLOW_COPY_AND_ASSIGN(ScopedWatchKernelStack);
};

// Singleton thread which implements the watchdog.
class KernelStackWatchdog {
 public:
  static KernelStackWatchdog* GetInstance() {
    return Singleton<KernelStackWatchdog>::get();
  }

  void Watch(pid_t tid, const char* label);
  void StopWatching(pid_t pid);

 private:
  friend class Singleton<KernelStackWatchdog>;
  KernelStackWatchdog();
  ~KernelStackWatchdog();

  void RunThread();

  simple_spinlock lock_;
  struct Entry {
    const char* label;
    MonoTime registered_time;
  };
  typedef std::tr1::unordered_map<pid_t, Entry> PidMap;
  PidMap watched_;
  scoped_refptr<Thread> thread_;

  CountDownLatch finish_;

  DISALLOW_COPY_AND_ASSIGN(KernelStackWatchdog);
};

inline ScopedWatchKernelStack::ScopedWatchKernelStack(const char* label)
  : tid_(syscall(SYS_gettid)) {
  KernelStackWatchdog::GetInstance()->Watch(tid_, label);
}

inline ScopedWatchKernelStack::~ScopedWatchKernelStack() {
  KernelStackWatchdog::GetInstance()->StopWatching(tid_);
}

} // namespace kudu
#endif /* KUDU_UTIL_KERNEL_STACK_WATCHDOG_H */
