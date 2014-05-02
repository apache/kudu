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
  explicit PstackWatcher(const MonoDelta& timeout);
  ~PstackWatcher();
  void Shutdown();
  bool IsRunning();

 private:
  kudu::Status Start();
  void Run();
  Status HasProgram(const char* progname);
  void LogPstack();

  const MonoDelta timeout_;
  bool running_;
  scoped_refptr<Thread> thread_;
  boost::mutex lock_;
  boost::condition_variable cond_;
};

} // namespace kudu
#endif
