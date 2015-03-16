// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_STATUS_CALLBACK_H
#define KUDU_UTIL_STATUS_CALLBACK_H

#include "kudu/gutil/callback_forward.h"

namespace kudu {

class Status;

// A callback which takes a Status. This is typically used for functions which
// produce asynchronous results and may fail.
typedef Callback<void(const Status& status)> StatusCallback;

// To be used when a function signature requires a StatusCallback but none
// is needed.
extern void DoNothingStatusCB(const Status& status);

// A closure (callback without arguments) that returns a Status indicating
// whether it was successful or not.
typedef Callback<Status(void)> StatusClosure;

// To be used when setting a StatusClosure is optional.
extern Status DoNothingStatusClosure();

} // namespace kudu

#endif
