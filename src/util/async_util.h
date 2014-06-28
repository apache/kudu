// Copyright (c) 2013, Cloudera, inc.
//
// Utility functions which are handy when doing async/callback-based programming.
#ifndef KUDU_UTIL_ASYNC_UTIL_H
#define KUDU_UTIL_ASYNC_UTIL_H

#include "gutil/bind.h"
#include "gutil/callback.h"
#include "gutil/macros.h"
#include "util/status.h"
#include "util/countdown_latch.h"

namespace kudu {

// A callback which takes a Status. This is typically used for functions which
// produce asynchronous results and may fail.
typedef base::Callback<void(const Status& status)> StatusCallback;

// Simple class which can be used to make async methods synchronous.
// For example:
//   Synchronizer s;
//   SomeAsyncMethod(s.callback());
//   CHECK_OK(s.Wait());
class Synchronizer {
 public:
  Synchronizer()
    : l(1) {
  }
  void StatusCB(const Status& status) {
    s = status;
    l.CountDown();
  }
  StatusCallback AsStatusCallback() {
    // Synchronizers are often declared on the stack, so it doesn't make
    // sense for a callback to take a reference to its synchronizer.
    //
    // Note: this means the returned callback _must_ go out of scope before
    // its synchronizer.
    return base::Bind(&Synchronizer::StatusCB, base::Unretained(this));
  }
  Status Wait() {
    l.Wait();
    return s;
  }
  void Reset() {
    l.Reset(1);
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(Synchronizer);
  Status s;
  CountDownLatch l;
};

} // namespace kudu
#endif /* KUDU_UTIL_ASYNC_UTIL_H */
