// Copyright 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_PSTACK_WATCHER_H
#define KUDU_UTIL_PSTACK_WATCHER_H

#include <string>
#include <vector>

#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace kudu {

// PstackWatcher is an object which will pstack the current process and print
// the results to stdout.  It does this after a certain timeout has occured.
class PstackWatcher {
 public:

  enum Flags {
    NO_FLAGS = 0,

    // Run 'thread apply all bt full', which is very verbose output
    DUMP_FULL = 1
  };

  // Static method to collect and write stack dump output to stdout of the current
  // process.
  static Status DumpStacks(int flags = NO_FLAGS);

  // Like the above but for any process, not just the current one.
  static Status DumpPidStacks(pid_t pid, int flags = NO_FLAGS);

  // Instantiate a watcher that writes a pstack to stdout after the given
  // timeout expires.
  explicit PstackWatcher(const MonoDelta& timeout);

  ~PstackWatcher();

  // Shut down the watcher and do not log a pstack.
  // This method is not thread-safe.
  void Shutdown();

  // Test whether the watcher is still running or has shut down.
  // Thread-safe.
  bool IsRunning() const;

  // Wait until the timeout expires and the watcher logs a pstack.
  // Thread-safe.
  void Wait() const;

 private:
  // Test for the existence of the given program in the system path.
  static Status HasProgram(const char* progname);

  // Get a stack dump using GDB directly.
  static Status RunGdbStackDump(pid_t pid, int flags);

  // Get a stack dump using the pstack or gstack program.
  static Status RunPstack(const std::string& progname, pid_t pid);

  // Invoke and wait for the stack dump program.
  static Status RunStackDump(const std::string& prog, const std::vector<std::string>& argv);

  // Run the thread that waits for the specified duration before logging a
  // pstack.
  void Run();

  const MonoDelta timeout_;
  bool running_;
  scoped_refptr<Thread> thread_;
  mutable Mutex lock_;
  mutable ConditionVariable cond_;
};

} // namespace kudu
#endif
