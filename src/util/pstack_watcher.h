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
  // Instantiate a watcher that writes a pstack to stdout after the given
  // timeout expires.
  explicit PstackWatcher(const MonoDelta& timeout);

  ~PstackWatcher();

  // Shut down the watcher and do not log a pstack.
  void Shutdown();

  // Test whether the watcher is still running or has shut down.
  bool IsRunning();

 private:
  // Run the thread that waits for the specified duration before logging a
  // pstack.
  void Run();

  // Test for the existence of the given program in the system path.
  Status HasProgram(const char* progname);

  // Collect and write pstack output to stdout, if possible.
  void LogPstack();

  const MonoDelta timeout_;
  bool running_;
  scoped_refptr<Thread> thread_;
  boost::mutex lock_;
  boost::condition_variable cond_;
};

} // namespace kudu
#endif
