// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <time.h>

#include "kudu/util/glibc_workaround.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/flags.h"
#include "kudu/util/status.h"

DEFINE_int32(test_timeout_after, 0,
             "Maximum total seconds allowed for all unit tests in the suite. Default: disabled");

// Start timer that kills the process if --test_timeout_after is exceeded before
// the tests complete.
static void CreateAndStartTimer(timer_t* timerid, struct sigevent* sevp, struct itimerspec* its);

// Gracefully kill the process.
static void KillTestOnTimeout(sigval_t sigval);

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  // InitGoogleTest() must precede ParseCommandLineFlags(), as the former
  // removes gtest-related flags from argv that would trip up the latter.
  ::testing::InitGoogleTest(&argc, argv);
  kudu::ParseCommandLineFlags(&argc, &argv, true);

  // Create the test-timeout timer.
  timer_t timerid;
  struct sigevent sevp;
  struct itimerspec its;
  CreateAndStartTimer(&timerid, &sevp, &its);

  int ret = RUN_ALL_TESTS();

  CHECK_ERR(::timer_delete(timerid));
  return ret;
}

static void CreateAndStartTimer(timer_t* timerid, struct sigevent* sevp, struct itimerspec* its) {
  kudu::ForceLinkingGlibcWorkaround();
  // Create the test-timeout timer.
  sevp->sigev_notify = SIGEV_THREAD;
  sevp->sigev_value.sival_ptr = timerid;
  sevp->sigev_notify_function = KillTestOnTimeout;
  sevp->sigev_notify_attributes = NULL;
  CHECK_ERR(::timer_create(CLOCK_MONOTONIC, sevp, timerid)) << "Unable to set timeout timer";

  // Start the timer.
  its->it_interval.tv_sec = 0;                      // No repeat.
  its->it_interval.tv_nsec = 0;
  its->it_value.tv_sec = FLAGS_test_timeout_after;  // Fire in timeout seconds.
  its->it_value.tv_nsec = 0;
  CHECK_ERR(::timer_settime(*timerid, 0, its, NULL));
}

static void KillTestOnTimeout(sigval_t sigval) {
  // Dump a pstack to stdout.
  WARN_NOT_OK(kudu::PstackWatcher::DumpStacks(), "Unable to print pstack");

  // ...and abort.
  LOG(FATAL) << "Maximum unit test time exceeded (" << FLAGS_test_timeout_after << " sec)";
}
