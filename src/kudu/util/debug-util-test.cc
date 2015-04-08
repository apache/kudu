// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <string>
#include <vector>
#include <glog/stl_logging.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::string;
using std::vector;

namespace kudu {

class DebugUtilTest : public KuduTest {
};

TEST_F(DebugUtilTest, TestStackTrace) {
  StackTrace t;
  t.Collect(1);
  string trace = t.Symbolize();
  ASSERT_STR_CONTAINS(trace, "kudu::DebugUtilTest_TestStackTrace_Test::TestBody");
}

TEST_F(DebugUtilTest, TestStackTraceInvalidTid) {
  string s = DumpThreadStack(1);
  ASSERT_STR_CONTAINS(s, "unable to deliver signal");
}

TEST_F(DebugUtilTest, TestStackTraceSelf) {
  string s = DumpThreadStack(syscall(SYS_gettid));
  ASSERT_STR_CONTAINS(s, "kudu::DebugUtilTest_TestStackTraceSelf_Test::TestBody()");
}

TEST_F(DebugUtilTest, TestStackTraceMainThread) {
  string s = DumpThreadStack(getpid());
  ASSERT_STR_CONTAINS(s, "kudu::DebugUtilTest_TestStackTraceMainThread_Test::TestBody()");
}

void SleeperThread(CountDownLatch* l) {
  // We use an infinite loop around WaitFor() instead of a normal Wait()
  // so that this test passes in TSAN. Without this, we run into this TSAN
  // bug which prevents the sleeping thread from handling signals:
  // https://code.google.com/p/thread-sanitizer/issues/detail?id=91
  while (!l->WaitFor(MonoDelta::FromMilliseconds(10))) {
  }
}

TEST_F(DebugUtilTest, TestSignalStackTrace) {
  CountDownLatch l(1);
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "test thread", &SleeperThread, &l, &t));

  // We have to loop a little bit because it takes a little while for the thread
  // to start up and actually call our function.
  string stack;
  for (int i = 0; i < 10000; i++) {
    stack = DumpThreadStack(t->tid());
    if (stack.find("SleeperThread") != string::npos) break;
    SleepFor(MonoDelta::FromMicroseconds(100));
  }
  ASSERT_STR_CONTAINS(stack, "SleeperThread");
  l.CountDown();
  t->Join();
}

// Test which dumps all known threads within this process.
// We don't validate the results in any way -- but this verifies that we can
// dump library threads such as the libc timer_thread and properly time out.
TEST_F(DebugUtilTest, TestDumpAllThreads) {
  vector<pid_t> tids;
  ASSERT_OK(ListThreads(&tids));
  BOOST_FOREACH(pid_t tid, tids) {
    LOG(INFO) << DumpThreadStack(tid);
  }
}

} // namespace kudu
