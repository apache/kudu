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

#include "kudu/util/pstack_watcher.h"

#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <memory>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::shared_ptr;
using std::string;
using strings::Substitute;

namespace kudu {

TEST(TestPstackWatcher, TestPstackWatcherCancellation) {
  PstackWatcher watcher(MonoDelta::FromSeconds(1000000));
  watcher.Shutdown();
}

TEST(TestPstackWatcher, TestWait) {
  PstackWatcher watcher(MonoDelta::FromMilliseconds(10));
  watcher.Wait();
}

TEST(TestPstackWatcher, TestDumpStacks) {
  ASSERT_OK(PstackWatcher::DumpStacks());
}

static FILE* RedirectStdout(string *temp_path) {
  string temp_dir;
  CHECK_OK(Env::Default()->GetTestDirectory(&temp_dir));
  *temp_path = Substitute("$0/pstack_watcher-dump.$1.txt",
                      temp_dir, getpid());
  FILE* reopened;
  POINTER_RETRY_ON_EINTR(reopened, freopen(temp_path->c_str(), "w", stdout));
  return reopened;
}

TEST(TestPstackWatcher, TestPstackWatcherRunning) {
  string stdout_file;
  int old_stdout;
  RETRY_ON_EINTR(old_stdout, dup(STDOUT_FILENO));
  CHECK_ERR(old_stdout);
  {
    FILE* out_fp = RedirectStdout(&stdout_file);
    PCHECK(out_fp != nullptr);
    SCOPED_CLEANUP({
        int err;
        RETRY_ON_EINTR(err, fclose(out_fp));
      });
    PstackWatcher watcher(MonoDelta::FromMilliseconds(500));
    while (watcher.IsRunning()) {
      SleepFor(MonoDelta::FromMilliseconds(1));
    }
  }
  int dup2_ret;
  RETRY_ON_EINTR(dup2_ret, dup2(old_stdout, STDOUT_FILENO));
  CHECK_ERR(dup2_ret);
  PCHECK(stdout = fdopen(STDOUT_FILENO, "w"));

  faststring contents;
  CHECK_OK(ReadFileToString(Env::Default(), stdout_file, &contents));
  ASSERT_STR_CONTAINS(contents.ToString(), "BEGIN STACKS");
  CHECK_ERR(unlink(stdout_file.c_str()));
  ASSERT_GE(fprintf(stdout, "%s\n", contents.ToString().c_str()), 0)
      << "errno=" << errno << ": " << ErrnoToString(errno);
}

} // namespace kudu
