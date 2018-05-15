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

#include "kudu/util/subprocess.h"

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

namespace kudu {

class SubprocessTest : public KuduTest {};

TEST_F(SubprocessTest, TestSimplePipe) {
  Subprocess p({ "/usr/bin/tr", "a-z", "A-Z" });
  p.ShareParentStdout(false);
  ASSERT_OK(p.Start());

  FILE* out = fdopen(p.ReleaseChildStdinFd(), "w");
  PCHECK(out);
  FILE* in = fdopen(p.from_child_stdout_fd(), "r");
  PCHECK(in);

  fprintf(out, "hello world\n");
  // We have to close 'out' or else tr won't write any output, since
  // it enters a buffered mode if it detects that its input is a FIFO.
  int err;
  RETRY_ON_EINTR(err, fclose(out));

  char buf[1024];
  ASSERT_EQ(buf, fgets(buf, sizeof(buf), in));
  ASSERT_STREQ("HELLO WORLD\n", &buf[0]);

  int wait_status = 0;
  ASSERT_OK(p.Wait(&wait_status));
  ASSERT_TRUE(WIFEXITED(wait_status));
  ASSERT_EQ(0, WEXITSTATUS(wait_status));
}

TEST_F(SubprocessTest, TestErrPipe) {
  Subprocess p({ "/usr/bin/tee", "/dev/stderr" });
  p.ShareParentStderr(false);
  ASSERT_OK(p.Start());

  FILE* out = fdopen(p.ReleaseChildStdinFd(), "w");
  PCHECK(out);

  fprintf(out, "Hello, World\n");

  // Same reasoning as above, flush to prevent tee buffering.
  int err;
  RETRY_ON_EINTR(err, fclose(out));

  FILE* in = fdopen(p.from_child_stderr_fd(), "r");
  PCHECK(in);

  char buf[1024];
  ASSERT_EQ(buf, fgets(buf, sizeof(buf), in));
  ASSERT_STREQ("Hello, World\n", &buf[0]);

  int wait_status = 0;
  ASSERT_OK(p.Wait(&wait_status));
  ASSERT_TRUE(WIFEXITED(wait_status));
  ASSERT_EQ(0, WEXITSTATUS(wait_status));
}

TEST_F(SubprocessTest, TestKill) {
  Subprocess p({ "/bin/cat" });
  ASSERT_OK(p.Start());

  ASSERT_OK(p.Kill(SIGKILL));

  int wait_status = 0;
  ASSERT_OK(p.Wait(&wait_status));
  ASSERT_TRUE(WIFSIGNALED(wait_status));
  ASSERT_EQ(SIGKILL, WTERMSIG(wait_status));

  // Test that calling Wait() a second time returns the same
  // cached value instead of trying to wait on some other process
  // that was assigned the same pid.
  wait_status = 0;
  ASSERT_OK(p.Wait(&wait_status));
  ASSERT_TRUE(WIFSIGNALED(wait_status));
  ASSERT_EQ(SIGKILL, WTERMSIG(wait_status));
}

// Writes enough bytes to stdout and stderr concurrently that if Call() were
// fully reading them one at a time, the test would deadlock.
TEST_F(SubprocessTest, TestReadFromStdoutAndStderr) {
  // Set an alarm to break out of any potential deadlocks (if the implementation
  // regresses).
  alarm(60);

  string stdout;
  string stderr;
  ASSERT_OK(Subprocess::Call({
    "/bin/bash",
    "-c",
    "dd if=/dev/urandom of=/dev/stdout bs=512 count=2048 &"
    "dd if=/dev/urandom of=/dev/stderr bs=512 count=2048 &"
    "wait"
  }, "", &stdout, &stderr));

  // Reset the alarm when the test is done
  SCOPED_CLEANUP({ alarm(0); })
}

// Test that environment variables can be passed to the subprocess.
TEST_F(SubprocessTest, TestEnvVars) {
  Subprocess p({ "/bin/bash", "-c", "echo $FOO" });
  p.SetEnvVars({{"FOO", "bar"}});
  p.ShareParentStdout(false);
  ASSERT_OK(p.Start());
  FILE* in = fdopen(p.from_child_stdout_fd(), "r");
  PCHECK(in);
  char buf[1024];
  ASSERT_EQ(buf, fgets(buf, sizeof(buf), in));
  ASSERT_STREQ("bar\n", &buf[0]);
  ASSERT_OK(p.Wait());
}

// Test that the the subprocesses CWD can be set.
TEST_F(SubprocessTest, TestCurrentDir) {
  string dir_path = GetTestPath("d");
  string file_path = JoinPathSegments(dir_path, "f");
  ASSERT_OK(Env::Default()->CreateDir(dir_path));
  std::unique_ptr<WritableFile> file;
  ASSERT_OK(Env::Default()->NewWritableFile(file_path, &file));

  Subprocess p({ "/bin/ls", "f" });
  p.SetCurrentDir(dir_path);
  p.ShareParentStdout(false);
  ASSERT_OK(p.Start());
  ASSERT_OK(p.Wait());

  int rc;
  ASSERT_OK(p.GetExitStatus(&rc, nullptr));
  EXPECT_EQ(0, rc);
}

// Tests writing to the subprocess stdin.
TEST_F(SubprocessTest, TestCallWithStdin) {
  string stdout;
  ASSERT_OK(Subprocess::Call({ "/bin/bash" },
                             "echo \"quick brown fox\"",
                             &stdout));
  EXPECT_EQ("quick brown fox\n", stdout);
}

// Test KUDU-1674: '/bin/bash -c "echo"' command below is expected to
// capture a string on stderr. This test validates that passing
// stderr alone doesn't result in SIGSEGV as reported in the bug and
// also check for sanity of stderr in the output.
TEST_F(SubprocessTest, TestReadSingleFD) {
  string stderr;
  const string str = "ApacheKudu";
  const string cmd_str = Substitute("/bin/echo -n $0 1>&2", str);
  ASSERT_OK(Subprocess::Call({"/bin/sh", "-c", cmd_str}, "", nullptr, &stderr));
  ASSERT_EQ(stderr, str);

  // Also sanity check other combinations.
  string stdout;
  ASSERT_OK(Subprocess::Call({"/bin/ls", "/dev/null"}, "", &stdout, nullptr));
  ASSERT_STR_CONTAINS(stdout, "/dev/null");

  ASSERT_OK(Subprocess::Call({"/bin/ls", "/dev/zero"}, "", nullptr, nullptr));
}

TEST_F(SubprocessTest, TestGetExitStatusExitSuccess) {
  Subprocess p({ "/bin/sh", "-c", "exit 0" });
  ASSERT_OK(p.Start());
  ASSERT_OK(p.Wait());
  int exit_status;
  string exit_info;
  ASSERT_OK(p.GetExitStatus(&exit_status, &exit_info));
  ASSERT_EQ(0, exit_status);
  ASSERT_STR_CONTAINS(exit_info, "process successfully exited");
}

TEST_F(SubprocessTest, TestGetExitStatusExitFailure) {
  static const vector<int> kStatusCodes = { 1, 255 };
  for (auto code : kStatusCodes) {
    Subprocess p({ "/bin/sh", "-c", Substitute("exit $0", code) });
    ASSERT_OK(p.Start());
    ASSERT_OK(p.Wait());
    int exit_status;
    string exit_info;
    ASSERT_OK(p.GetExitStatus(&exit_status, &exit_info));
    ASSERT_EQ(code, exit_status);
    ASSERT_STR_CONTAINS(exit_info,
                        Substitute("process exited with non-zero status $0",
                                   exit_status));
  }
}

TEST_F(SubprocessTest, TestGetExitStatusSignaled) {
  static const vector<int> kSignals = {
    SIGHUP,
    SIGABRT,
    SIGKILL,
    SIGTERM,
    SIGUSR2,
  };
  for (auto signum : kSignals) {
    Subprocess p({ "/bin/cat" });
    ASSERT_OK(p.Start());
    ASSERT_OK(p.Kill(signum));
    ASSERT_OK(p.Wait());
    int exit_status;
    string exit_info;
    ASSERT_OK(p.GetExitStatus(&exit_status, &exit_info));
    EXPECT_EQ(signum, exit_status);
    ASSERT_STR_CONTAINS(exit_info, Substitute("process exited on signal $0",
                                              signum));
  }
}

TEST_F(SubprocessTest, TestSubprocessDestroyWithCustomSignal) {
  string kTestFile = GetTestPath("foo");

  // Start a subprocess that creates kTestFile immediately and deletes it on exit.
  //
  // Note: it's important that the shell not invoke a command while waiting
  // to be killed (i.e. "sleep 60"); if it did, the signal could be delivered
  // just after the command starts but just before the shell decides to forward
  // signals to it, and we wind up with a deadlock.
  vector<string> argv = {
      "/bin/bash",
      "-c",
      Substitute(
          // Delete kTestFile on exit.
          "trap \"rm $0\" EXIT;"
          // Create kTestFile on start.
          "touch $0;"
          // Spin in a tight loop waiting to be killed.
          "while true;"
          "  do FOO=$$((FOO + 1));"
          "done", kTestFile)
  };

  {
    Subprocess s(argv);
    ASSERT_OK(s.Start());
    AssertEventually([&]{
        ASSERT_TRUE(env_->FileExists(kTestFile));
    });
  }

  // The subprocess went out of scope and was killed with SIGKILL, so it left
  // kTestFile behind.
  ASSERT_TRUE(env_->FileExists(kTestFile));

  ASSERT_OK(env_->DeleteFile(kTestFile));
  {
    Subprocess s(argv, SIGTERM);
    ASSERT_OK(s.Start());
    AssertEventually([&]{
        ASSERT_TRUE(env_->FileExists(kTestFile));
    });
  }

  // The subprocess was killed with SIGTERM, giving it a chance to delete kTestFile.
  ASSERT_FALSE(env_->FileExists(kTestFile));
}

// TEST KUDU-2208: Test subprocess interruption handling
void handler(int /* signal */) {
}

TEST_F(SubprocessTest, TestSubprocessInterruptionHandling) {
  // Create Subprocess thread
  pthread_t t;
  Subprocess p({ "/bin/sleep", "1" });
  atomic<bool> t_started(false);
  atomic<bool> t_finished(false);
  thread subprocess_thread([&]() {
    t = pthread_self();
    t_started = true;
    SleepFor(MonoDelta::FromMilliseconds(50));
    CHECK_OK(p.Start());
    CHECK_OK(p.Wait());
    t_finished = true;
  });

  // Set up a no-op signal handler for SIGUSR2.
  struct sigaction sa, sa_old;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = &handler;
  sigaction(SIGUSR2, &sa, &sa_old);

  SCOPED_CLEANUP({ sigaction(SIGUSR2, &sa_old, nullptr); });
  SCOPED_CLEANUP({ subprocess_thread.join(); });

  // Send kill signals to Subprocess thread
  LOG(INFO) << "Start sending kill signals to Subprocess thread";
  while (!t_finished) {
    if (t_started) {
      int err = pthread_kill(t, SIGUSR2);
      ASSERT_TRUE(err == 0 || err == ESRCH);
      if (err == ESRCH) {
        LOG(INFO) << "Async kill signal failed with err=" << err <<
            " because it tried to kill vanished subprocess_thread";
        ASSERT_TRUE(t_finished);
      }
      // Add microseconds delay to make the unit test runs faster and more reliable
      SleepFor(MonoDelta::FromMicroseconds(rand() % 1));
    }
  }
}

#ifdef __linux__
// This test requires a system with /proc/<pid>/stat.
TEST_F(SubprocessTest, TestGetProcfsState) {
  // This test should be RUNNING.
  Subprocess::ProcfsState state;
  ASSERT_OK(Subprocess::GetProcfsState(getpid(), &state));
  ASSERT_EQ(Subprocess::ProcfsState::RUNNING, state);

  // When started, /bin/sleep will be RUNNING (even though it's asleep).
  Subprocess sleep({"/bin/sleep", "1000"});
  ASSERT_OK(sleep.Start());
  ASSERT_OK(Subprocess::GetProcfsState(sleep.pid(), &state));
  ASSERT_EQ(Subprocess::ProcfsState::RUNNING, state);

  // After a SIGSTOP, it should be PAUSED.
  ASSERT_OK(sleep.Kill(SIGSTOP));
  ASSERT_OK(Subprocess::GetProcfsState(sleep.pid(), &state));
  ASSERT_EQ(Subprocess::ProcfsState::PAUSED, state);

  // After a SIGCONT, it should be RUNNING again.
  ASSERT_OK(sleep.Kill(SIGCONT));
  ASSERT_OK(Subprocess::GetProcfsState(sleep.pid(), &state));
  ASSERT_EQ(Subprocess::ProcfsState::RUNNING, state);
}
#endif

} // namespace kudu
