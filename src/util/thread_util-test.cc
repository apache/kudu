// Copyright (c) 2013, Cloudera, inc.

#include "util/thread_util.h"

#include <gtest/gtest.h>
#include "util/test_util.h"

namespace kudu {

class ThreadUtilTest : public KuduTest {};

// Join with a thread and emit warnings while waiting to join.
// This has to be manually verified.
TEST_F(ThreadUtilTest, TestJoinAndWarn) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in quick test mode, since this sleeps";
    return;
  }

  boost::thread thr(usleep, 1000*1000);
  ASSERT_STATUS_OK(ThreadJoiner(&thr, "sleeper thread")
                   .warn_after_ms(10)
                   .warn_every_ms(100)
                   .Join());
}

TEST_F(ThreadUtilTest, TestFailedJoin) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in quick test mode, since this sleeps";
    return;
  }

  boost::thread thr(usleep, 1000*1000);
  Status s = ThreadJoiner(&thr, "sleeper thread")
    .give_up_after_ms(50)
    .Join();
  ASSERT_STR_CONTAINS(s.ToString(), "Timed out after 50ms joining on sleeper thread");
}

static void TryJoinOnSelf(gscoped_ptr<boost::thread>* self) {
  Status s = ThreadJoiner(self->get(), "own thread").Join();
  // Use CHECK instead of ASSERT because gtest isn't thread-safe.
  CHECK(s.IsInvalidArgument());
}

// Try to join on the thread that is currently running.
TEST_F(ThreadUtilTest, TestJoinOnSelf) {
  gscoped_ptr<boost::thread> t;
  ASSERT_STATUS_OK(StartThread(boost::bind(TryJoinOnSelf, &t), &t));
  t->join();
  // Actual assertion is done by the thread spawned above.
}

} // namespace kudu
