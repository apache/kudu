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

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/minidump.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

DECLARE_bool(enable_minidumps);
DECLARE_int32(max_minidumps);
DECLARE_string(minidump_path);

namespace kudu {

class MinidumpDeathTest : public KuduTest {
 protected:
  void WaitForMinidumps(int expected, const string& dir);
};

void MinidumpDeathTest::WaitForMinidumps(int expected, const string& dir) {
  AssertEventually([&] {
    vector<string> matches;
    ASSERT_OK(env_->Glob(JoinPathSegments(dir, "*.dmp"), &matches));
    ASSERT_EQ(expected, matches.size());
  });
}

// Test that registering the minidump exception handler results in creation of
// minidump files on crash. Also test that deleting excess minidump files works
// as expected.
TEST_F(MinidumpDeathTest, TestRegisterAndDelete) {
  FLAGS_enable_minidumps = true;
  FLAGS_minidump_path = JoinPathSegments(test_dir_, "minidumps");
  MinidumpExceptionHandler minidump_handler;
  ASSERT_DEATH({
    abort();
  },
  // Ensure that a stack trace is produced.
  "kudu::MinidumpDeathTest_TestRegisterAndDelete_Test::TestBody()");

  // Ensure that a minidump is produced.
  string minidump_dir = minidump_handler.minidump_dir();
  NO_FATALS(WaitForMinidumps(1, minidump_dir));

  // Now create more minidumps so we can clean them up.
  for (int num_dumps : {2, 3}) {
    kill(getpid(), SIGUSR1);
    NO_FATALS(WaitForMinidumps(num_dumps, minidump_dir));
  }

  FLAGS_max_minidumps = 2;
  ASSERT_OK(minidump_handler.DeleteExcessMinidumpFiles(env_));
  NO_FATALS(WaitForMinidumps(2, minidump_dir));
}

// Test that a CHECK() failure produces a stack trace and a minidump.
TEST_F(MinidumpDeathTest, TestCheckStackTraceAndMinidump) {
  FLAGS_enable_minidumps = true;
  FLAGS_minidump_path = JoinPathSegments(test_dir_, "minidumps");
  MinidumpExceptionHandler minidump_handler;
  ASSERT_DEATH({
    CHECK_EQ(1, 0);
  },
  // Ensure that a stack trace is produced.
  "kudu::MinidumpDeathTest_TestCheckStackTraceAndMinidump_Test::TestBody()");

  // Ensure that a minidump is produced.
  string minidump_dir = minidump_handler.minidump_dir();
  NO_FATALS(WaitForMinidumps(1, minidump_dir));
}

class MinidumpSignalDeathTest : public MinidumpDeathTest,
                                public ::testing::WithParamInterface<int> {
};

// Test that we get both a minidump and a stack trace for each supported signal.
TEST_P(MinidumpSignalDeathTest, TestHaveMinidumpAndStackTrace) {
  FLAGS_enable_minidumps = true;
  int signal = GetParam();

#if defined(ADDRESS_SANITIZER)
  // ASAN appears to catch SIGBUS, SIGSEGV, and SIGFPE and the process is not killed.
  if (signal == SIGBUS || signal == SIGSEGV || signal == SIGFPE) {
    return;
  }
#endif

#if defined(THREAD_SANITIZER)
  // TSAN appears to catch SIGTERM and the process is not killed.
  if (signal == SIGTERM) {
    return;
  }
#endif

  LOG(INFO) << "Testing signal: " << strsignal(signal);

  FLAGS_minidump_path = JoinPathSegments(test_dir_, "minidumps");
  MinidumpExceptionHandler minidump_handler;
  ASSERT_DEATH({
    kill(getpid(), signal);
  },
  // Ensure that a stack trace is produced.
  "kudu::MinidumpSignalDeathTest_TestHaveMinidumpAndStackTrace_Test::TestBody()");

  // Ensure that a mindump is produced, unless it's SIGTERM, which does not
  // create a minidump.
  int num_expected_minidumps = 1;
  if (signal == SIGTERM) {
    num_expected_minidumps = 0;
  }
  NO_FATALS(WaitForMinidumps(num_expected_minidumps, minidump_handler.minidump_dir()));
}

INSTANTIATE_TEST_CASE_P(DeadlySignals, MinidumpSignalDeathTest,
    ::testing::Values(SIGABRT, SIGBUS, SIGSEGV, SIGILL, SIGFPE, SIGTERM));

} // namespace kudu
