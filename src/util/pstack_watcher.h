// Copyright 2014 Cloudera, Inc.
#ifndef KUDU_UTIL_PSTACK_WATCHER_H
#define KUDU_UTIL_PSTACK_WATCHER_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <vector>
#include <string>

#include "util/monotime.h"
#include "util/status.h"
#include "util/thread.h"

namespace kudu {

// PstackWatcher is an object which will pstack the current process and print
// the results to stdout.  It does this after a certain timeout has occured.
class PstackWatcher {
 public:
  // Collect and write pstack output to stdout.
  static Status DumpStacks();

  // Instantiate a watcher that writes a pstack to stdout after the given
  // timeout expires.
  explicit PstackWatcher(const MonoDelta& timeout);

  ~PstackWatcher();

  // Shut down the watcher and do not log a pstack.
  void Shutdown();

  // Test whether the watcher is still running or has shut down.
  bool IsRunning() const;

  // Wait until the timeout expires and the watcher logs a pstack.
  void Wait() const;

 private:
  // Test for the existence of the given program in the system path.
  static Status HasProgram(const char* progname);

  // Get a stack dump using GDB directly.
  static Status RunGdbStackDump(pid_t pid);

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
  mutable boost::mutex lock_;
  mutable boost::condition_variable cond_;
};

} // namespace kudu
#endif
