// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <dlfcn.h>
#ifdef __linux__
#include <link.h>
#endif
#include "kudu/util/debug-util.h"

#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/array_view.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::string;
using std::vector;

DECLARE_int32(test_timeout_after);
DECLARE_int32(stress_cpu_threads);

namespace kudu {

class DebugUtilTest : public KuduTest {
};

TEST_F(DebugUtilTest, TestStackTrace) {
  StackTrace t;
  t.Collect(0);
  string trace = t.Symbolize();
  ASSERT_STR_CONTAINS(trace, "kudu::DebugUtilTest_TestStackTrace_Test::TestBody");
}

// DumpThreadStack is only supported on Linux, since the implementation relies
// on the tgkill syscall which is not portable.
#if defined(__linux__)

namespace {
void SleeperThread(CountDownLatch* l) {
  // We use an infinite loop around WaitFor() instead of a normal Wait()
  // so that this test passes in TSAN. Without this, we run into this TSAN
  // bug which prevents the sleeping thread from handling signals:
  // https://code.google.com/p/thread-sanitizer/issues/detail?id=91
  while (!l->WaitFor(MonoDelta::FromMilliseconds(10))) {
  }
}

void fake_signal_handler(int signum) {}

bool IsSignalHandlerRegistered(int signum) {
  struct sigaction cur_action;
  CHECK_EQ(0, sigaction(signum, nullptr, &cur_action));
  return cur_action.sa_handler != SIG_DFL;
}
} // anonymous namespace

TEST_F(DebugUtilTest, TestStackTraceInvalidTid) {
  string s = DumpThreadStack(1);
  ASSERT_STR_CONTAINS(s, "unable to deliver signal");
}

TEST_F(DebugUtilTest, TestStackTraceSelf) {
  string s = DumpThreadStack(Thread::CurrentThreadId());
  ASSERT_STR_CONTAINS(s, "kudu::DebugUtilTest_TestStackTraceSelf_Test::TestBody()");
}

TEST_F(DebugUtilTest, TestStackTraceMainThread) {
  string s = DumpThreadStack(getpid());
  ASSERT_STR_CONTAINS(s, "kudu::DebugUtilTest_TestStackTraceMainThread_Test::TestBody()");
}

TEST_F(DebugUtilTest, TestSignalStackTrace) {
  CountDownLatch l(1);
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "test thread", [&l]() { SleeperThread(&l); }, &t));
  SCOPED_CLEANUP({
    // Allow the thread to finish.
    l.CountDown();
    t->Join();
  });

  // We have to loop a little bit because it takes a little while for the thread
  // to start up and actually call our function.
  //
  // Note: due to RELEASE build inlining, we need to make sure to pick a stack
  // frame that isn't optimized away.
  static constexpr const char* kTestThreadStackFrame =
      "kudu::ConditionVariable::WaitUntil()";
  ASSERT_EVENTUALLY([&]() {
    ASSERT_STR_CONTAINS(DumpThreadStack(t->tid()), kTestThreadStackFrame);
  });

  // Test that we can change the signal and that the stack traces still work,
  // on the new signal.
  ASSERT_FALSE(IsSignalHandlerRegistered(SIGHUP));
  ASSERT_OK(SetStackTraceSignal(SIGHUP));

  // Should now be registered.
  ASSERT_TRUE(IsSignalHandlerRegistered(SIGHUP));

  // SIGUSR2 should be relinquished.
  ASSERT_FALSE(IsSignalHandlerRegistered(SIGUSR2));

  // Stack traces should work using the new handler.
  ASSERT_STR_CONTAINS(DumpThreadStack(t->tid()), kTestThreadStackFrame);

  // Switch back to SIGUSR2 and ensure it changes back.
  ASSERT_OK(SetStackTraceSignal(SIGUSR2));
  ASSERT_TRUE(IsSignalHandlerRegistered(SIGUSR2));
  ASSERT_FALSE(IsSignalHandlerRegistered(SIGHUP));

  // Stack traces should work using the new handler.
  ASSERT_STR_CONTAINS(DumpThreadStack(t->tid()), kTestThreadStackFrame);

  // Register our own signal handler on SIGHUP, and ensure that
  // we get a bad Status if we try to use it.
  signal(SIGHUP, &fake_signal_handler);
  ASSERT_STR_CONTAINS(SetStackTraceSignal(SIGHUP).ToString(),
                      "unable to install signal handler");
  signal(SIGHUP, SIG_DFL);

  // Stack traces should be disabled
  ASSERT_STR_CONTAINS(DumpThreadStack(t->tid()), "unable to take thread stack");

  // Re-enable so that other tests pass.
  ASSERT_OK(SetStackTraceSignal(SIGUSR2));
}

// Test which dumps all known threads within this process.
// We don't validate the results in any way -- but this verifies that we can
// dump library threads such as the libc timer_thread and properly time out.
TEST_F(DebugUtilTest, TestSnapshot) {
  // HACK: prior tests in this suite start threads. Even though they Join on the
  // threads before the test case finishes, there is actually a very short
  // period of time after Join() returns but before the actual thread has exited
  // and removed itself from /proc/self/task/. That means that 'ListThreads' below
  // can sometimes show these threads from prior test cases, and then the assertions
  // in this test case would fail.
  //
  // So, we have to wait here for the number of running threads to level off to the
  // expected value.
  // Ensure Kernel Stack Watchdog is running.
  KernelStackWatchdog::GetInstance();
  int initial_thread_count =
      1 // main thread
      + 1 // KernelStackWatchdog
      + (FLAGS_test_timeout_after > 0 ? 1 : 0) // test timeout thread if running
      + FLAGS_stress_cpu_threads;
#ifdef THREAD_SANITIZER
  initial_thread_count++; // tsan signal thread
#endif
  // The test and runtime environment runs various utility threads (for example,
  // the kernel stack watchdog, the TSAN runtime thread, the test timeout thread, etc).
  // Count them before we start any additional threads for this test.
  ASSERT_EVENTUALLY([&]{
      vector<pid_t> threads;
      ASSERT_OK(ListThreads(&threads));
      ASSERT_EQ(initial_thread_count, threads.size()) << threads;
    });

  // Start a bunch of sleeping threads.
  const int kNumThreads = 30;
  CountDownLatch l(1);
  vector<scoped_refptr<Thread>> threads(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    ASSERT_OK(Thread::Create("test", "test thread",
                             [&l]() { SleeperThread(&l); }, &threads[i]));
  }

  SCOPED_CLEANUP({
    // Allow the thread to finish.
    l.CountDown();
    for (auto& t : threads) {
      t->Join();
    }
  });

  StackTraceSnapshot snap;
  ASSERT_OK(snap.SnapshotAllStacks());
  int count = 0;
  int groups = 0;
  snap.VisitGroups([&](ArrayView<StackTraceSnapshot::ThreadInfo> group) {
      groups++;
      for (const auto& info : group) {
        count++;
        LOG(INFO) << info.tid << " " << info.thread_name
                  << " (" << info.status.ToString() << ")";
      }
      LOG(INFO) << group[0].stack.ToHexString();
    });
  int tsan_threads = 0;
#ifdef THREAD_SANITIZER
  // TSAN starts an extra thread of its own.
  tsan_threads++;
#endif
  ASSERT_EQ(kNumThreads + initial_thread_count, count);
  // The threads might not have exactly identical stacks, but
  // we should have far fewer groups than the total number
  // of threads.
  ASSERT_LE(groups, kNumThreads / 2);
  ASSERT_EQ(tsan_threads, snap.num_failed());
}

TEST_F(DebugUtilTest, Benchmark) {
  CountDownLatch l(1);
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "test thread", [&l]() { SleeperThread(&l); }, &t));
  SCOPED_CLEANUP({
    // Allow the thread to finish.
    l.CountDown();
    t->Join();
  });

  for (bool symbolize : {false, true}) {
    MonoTime end_time = MonoTime::Now() + MonoDelta::FromSeconds(1);
    int count = 0;
    volatile int prevent_optimize = 0;
    while (MonoTime::Now() < end_time) {
      StackTrace trace;
      GetThreadStack(t->tid(), &trace);
      if (symbolize) {
        prevent_optimize += trace.Symbolize().size();
      }
      count++;
    }
    LOG(INFO) << "Throughput: " << count << " dumps/second (symbolize=" << symbolize << ")";
  }
}

int TakeStackTrace(struct dl_phdr_info* /*info*/, size_t /*size*/, void* data) {
  StackTrace* s = reinterpret_cast<StackTrace*>(data);
  s->Collect(0);
  return 0;
}

// Test that if we try to collect a stack trace while inside a libdl function
// call that we properly return the bogus stack indicating the issue.
//
// This doesn't work in ThreadSanitizer since we don't intercept dl_iterate_phdr
// in those builds (see note in unwind_safeness.cc).
#ifndef THREAD_SANITIZER
TEST_F(DebugUtilTest, TestUnwindWhileUnsafe) {
  StackTrace s;
  dl_iterate_phdr(&TakeStackTrace, &s);
  ASSERT_STR_CONTAINS(s.Symbolize(), "CouldNotCollectStackTraceBecauseInsideLibDl");
}
#endif

int DoNothingDlCallback(struct dl_phdr_info* /*info*/, size_t /*size*/, void* /*data*/) {
  return 0;
}

// Parameterized test which performs various operations which might be dangerous to
// collect a stack trace while the main thread tries to take stack traces.  These
// operations are all possibly executed on normal application threads, so we need to
// ensure that if we happen to gather the stack from a thread in the middle of the
// function that we don't crash or deadlock.
//
// Example self-deadlock if we didn't have the appropriate workarounds in place:
//  #0  __lll_lock_wait ()
//  #1  0x00007ffff6f16e42 in __GI___pthread_mutex_lock
//  #2  0x00007ffff6c8601f in __GI___dl_iterate_phdr
//  #3  0x0000000000695b02 in dl_iterate_phdr
//  #4  0x000000000056d013 in _ULx86_64_dwarf_find_proc_info
//  #5  0x000000000056d1d5 in fetch_proc_info (c=c@ent
//  #6  0x000000000056e2e7 in _ULx86_64_dwarf_find_save_
//  #7  0x000000000056c1b9 in _ULx86_64_dwarf_step (c=c@
//  #8  0x000000000056be21 in _ULx86_64_step
//  #9  0x0000000000566b1d in google::GetStackTrace
//  #10 0x00000000004dc4d1 in kudu::StackTrace::Collect
//  #11 kudu::(anonymous namespace)::HandleStackTraceSignal
//  #12 <signal handler called>
//  #13 0x00007ffff6f16e31 in __GI___pthread_mutex_lock
//  #14 0x00007ffff6c8601f in __GI___dl_iterate_phdr
//  #15 0x0000000000695b02 in dl_iterate_phdr
enum DangerousOp {
  DLOPEN_AND_CLOSE,
  DL_ITERATE_PHDR,
  GET_STACK_TRACE,
  MALLOC_AND_FREE
};
class RaceTest : public DebugUtilTest, public ::testing::WithParamInterface<DangerousOp> {};
INSTANTIATE_TEST_CASE_P(DifferentRaces, RaceTest,
                        ::testing::Values(DLOPEN_AND_CLOSE,
                                          DL_ITERATE_PHDR,
                                          GET_STACK_TRACE,
                                          MALLOC_AND_FREE));

void DangerousOperationThread(DangerousOp op, CountDownLatch* l) {
  while (l->count()) {
    switch (op) {
      case DLOPEN_AND_CLOSE: {
        // Check races against dlopen/dlclose.
        void* v = dlopen("libc.so.6", RTLD_LAZY);
        CHECK(v);
        dlclose(v);
        break;
      }

      case DL_ITERATE_PHDR: {
        // Check for races against dl_iterate_phdr.
        dl_iterate_phdr(&DoNothingDlCallback, nullptr);
        break;
      }

      case GET_STACK_TRACE: {
        // Check for reentrancy issues
        GetStackTrace();
        break;
      }

      case MALLOC_AND_FREE: {
        // Check large allocations in tcmalloc.
        volatile char* x = new char[1024 * 1024 * 2];
        delete[] x;
        break;
      }
      default:
        LOG(FATAL) << "unknown op";
    }
  }
}

// Starts a thread performing dangerous operations and then gathers
// its stack trace in a loop trying to trigger races.
TEST_P(RaceTest, TestStackTraceRaces) {
  DangerousOp op = GetParam();
  CountDownLatch l(1);
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "test thread",
                           [op, &l]() { DangerousOperationThread(op, &l); }, &t));
  SCOPED_CLEANUP({
    // Allow the thread to finish.
    l.CountDown();
    // Crash if we can't join the thread after a reasonable amount of time.
    // That probably indicates a deadlock.
    CHECK_OK(ThreadJoiner(t.get()).give_up_after_ms(10000).Join());
  });
  MonoTime end_time = MonoTime::Now() + MonoDelta::FromSeconds(1);
  while (MonoTime::Now() < end_time) {
    StackTrace trace;
    GetThreadStack(t->tid(), &trace);
  }
}

void BlockSignalsThread() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR2);
  for (int i = 0; i < 3; i++) {
    CHECK_ERR(pthread_sigmask((i % 2) ? SIG_UNBLOCK : SIG_BLOCK, &set, nullptr));
    SleepFor(MonoDelta::FromSeconds(1));
  }
}

TEST_F(DebugUtilTest, TestThreadBlockingSignals) {
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "test thread", &BlockSignalsThread, &t));
  SCOPED_CLEANUP({ t->Join(); });
  string ret;
  while (ret.find("unable to deliver signal") == string::npos) {
    ret = DumpThreadStack(t->tid());
    LOG(INFO) << ret;
  }
}

// Test stack traces which time out despite the destination thread not blocking
// signals.
TEST_F(DebugUtilTest, TestTimeouts) {
  const int kRunTimeSecs = AllowSlowTests() ? 5 : 1;

  CountDownLatch l(1);
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "test thread", [&l]() { SleeperThread(&l); }, &t));
  SCOPED_CLEANUP({
    // Allow the thread to finish.
    l.CountDown();
    t->Join();
  });

  // First, time a few stack traces to determine how long a non-timed-out stack
  // trace takes.
  vector<MicrosecondsInt64> durations;
  for (int i = 0; i < 20; i++) {
    StackTrace stack;
    auto st = GetMonoTimeMicros();
    ASSERT_OK(GetThreadStack(t->tid(), &stack));
    auto dur = GetMonoTimeMicros() - st;
    durations.push_back(dur);
  }

  // Compute the median to throw out outliers.
  std::sort(durations.begin(), durations.end());
  auto median_duration = durations[durations.size() / 2];
  LOG(INFO) << "Median duration: " << median_duration << "us";

  // Now take a bunch of stack traces with timeouts clustered around
  // the expected time. When we time out, we adjust the timeout to be
  // higher so the next attempt is less likely to time out. Conversely,
  // when we succeed, we adjust the timeout to be shorter so the next
  // attempt is more likely to time out. This has the effect of triggering
  // all the interesting cases: (a) success, (b) timeout, (c) timeout
  // exactly as the signal finishes.
  int num_timeouts = 0;
  int num_successes = 0;
  auto end_time = MonoTime::Now() + MonoDelta::FromSeconds(kRunTimeSecs);
  int64_t timeout_us = median_duration;
  while (MonoTime::Now() < end_time) {
    StackTraceCollector stc;
    // Allocate Stack on the heap so that if we get a use-after-free it
    // will be caught more easily by ASAN.
    std::unique_ptr<StackTrace> stack(new StackTrace());
    ASSERT_OK(stc.TriggerAsync(t->tid(), stack.get()));
    Status s = stc.AwaitCollection(MonoTime::Now() + MonoDelta::FromMicroseconds(timeout_us));
    if (s.ok()) {
      num_successes++;
      timeout_us--;
    } else if (s.IsTimedOut()) {
      num_timeouts++;
      timeout_us++;
    } else {
      FAIL() << "Unexpected status: " << s.ToString();
    }
  }
  LOG(INFO) << "Timed out " << num_timeouts << " times";
  LOG(INFO) << "Succeeded " << num_successes << " times";
}

#endif
} // namespace kudu
