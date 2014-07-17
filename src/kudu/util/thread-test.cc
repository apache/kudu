// Copyright (c) 2013, Cloudera, inc.

#include "kudu/util/thread.h"

#include <gtest/gtest.h>
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/test_util.h"

namespace kudu {

class ThreadTest : public KuduTest {};

// Join with a thread and emit warnings while waiting to join.
// This has to be manually verified.
TEST_F(ThreadTest, TestJoinAndWarn) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in quick test mode, since this sleeps";
    return;
  }

  scoped_refptr<Thread> holder;
  ASSERT_STATUS_OK(Thread::Create("test", "sleeper thread", usleep, 1000*1000, &holder));
  ASSERT_STATUS_OK(ThreadJoiner(holder.get())
                   .warn_after_ms(10)
                   .warn_every_ms(100)
                   .Join());
}

TEST_F(ThreadTest, TestFailedJoin) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping test in quick test mode, since this sleeps";
    return;
  }

  scoped_refptr<Thread> holder;
  ASSERT_STATUS_OK(Thread::Create("test", "sleeper thread", usleep, 1000*1000, &holder));
  Status s = ThreadJoiner(holder.get())
    .give_up_after_ms(50)
    .Join();
  ASSERT_STR_CONTAINS(s.ToString(), "Timed out after 50ms joining on sleeper thread");
}

static void TryJoinOnSelf() {
  Status s = ThreadJoiner(Thread::current_thread()).Join();
  // Use CHECK instead of ASSERT because gtest isn't thread-safe.
  CHECK(s.IsInvalidArgument());
}

// Try to join on the thread that is currently running.
TEST_F(ThreadTest, TestJoinOnSelf) {
  scoped_refptr<Thread> holder;
  ASSERT_STATUS_OK(Thread::Create("test", "test", TryJoinOnSelf, &holder));
  holder->Join();
  // Actual assertion is done by the thread spawned above.
}

TEST_F(ThreadTest, TestDoubleJoinIsNoOp) {
  scoped_refptr<Thread> holder;
  ASSERT_STATUS_OK(Thread::Create("test", "sleeper thread", usleep, 0, &holder));
  ThreadJoiner joiner(holder.get());
  ASSERT_STATUS_OK(joiner.Join());
  ASSERT_STATUS_OK(joiner.Join());
}

} // namespace kudu
