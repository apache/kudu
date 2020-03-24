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
#include <functional>
#include <initializer_list>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/thread.h"

namespace kudu {

using std::string;
using std::vector;
using strings::SkipEmpty;
using strings::SkipWhitespace;
using strings::Split;
using strings::Substitute;

PstackWatcher::PstackWatcher(MonoDelta timeout)
    : timeout_(timeout), running_(true), cond_(&lock_) {
  CHECK_OK(Thread::Create("pstack_watcher", "pstack_watcher",
                          [this]() { this->Run(); }, &thread_));
}

PstackWatcher::~PstackWatcher() {
  Shutdown();
}

void PstackWatcher::Shutdown() {
  {
    MutexLock guard(lock_);
    running_ = false;
    cond_.Broadcast();
  }
  if (thread_) {
    CHECK_OK(ThreadJoiner(thread_.get()).Join());
    thread_.reset();
  }
}

bool PstackWatcher::IsRunning() const {
  MutexLock guard(lock_);
  return running_;
}

void PstackWatcher::Wait() const {
  MutexLock lock(lock_);
  while (running_) {
    cond_.Wait();
  }
}

void PstackWatcher::Run() {
  MutexLock guard(lock_);
  if (!running_) return;
  cond_.WaitFor(timeout_);
  if (!running_) return;

  WARN_NOT_OK(DumpStacks(DUMP_FULL), "Unable to print pstack from watcher");
  running_ = false;
  cond_.Broadcast();
}

Status PstackWatcher::HasProgram(const char* progname) {
  Subprocess proc({ "which", progname } );
  proc.DisableStderr();
  proc.DisableStdout();
  RETURN_NOT_OK_PREPEND(proc.Start(),
      Substitute("HasProgram($0): error running 'which'", progname));
  RETURN_NOT_OK(proc.Wait());
  int exit_status;
  string exit_info;
  RETURN_NOT_OK(proc.GetExitStatus(&exit_status, &exit_info));
  if (exit_status == 0) {
    return Status::OK();
  }
  return Status::NotFound(Substitute("can't find $0: $1", progname, exit_info));
}

Status PstackWatcher::HasGoodGdb() {
  // Check for the existence of gdb.
  RETURN_NOT_OK(HasProgram("gdb"));

  // gdb exists, run it and parse the output of --version. For example:
  //
  // GNU gdb (GDB) Red Hat Enterprise Linux (7.2-75.el6)
  // ...
  //
  // Or:
  //
  // GNU gdb (Ubuntu 7.11.1-0ubuntu1~16.5) 7.11.1
  // ...
  string stdout;
  RETURN_NOT_OK(Subprocess::Call({"gdb", "--version"}, "", &stdout));
  vector<string> lines = Split(stdout, "\n", SkipEmpty());
  if (lines.empty()) {
    return Status::Incomplete("gdb version not found");
  }
  vector<string> words = Split(lines[0], " ", SkipWhitespace());
  if (words.empty()) {
    return Status::Incomplete("could not parse gdb version");
  }
  string version = words[words.size() - 1];
  version = StripPrefixString(version, "(");
  version = StripSuffixString(version, ")");

  // The variable pretty print routine in older versions of gdb is buggy in
  // that it reads the values of all local variables, including uninitialized
  // ones. For some variable types with an embedded length (such as std::string
  // or std::vector), this can lead to all sorts of incorrect memory accesses,
  // causing deadlocks or seemingly infinite loops within gdb.
  //
  // It's not clear exactly when this behavior was fixed, so we whitelist the
  // oldest known good version: the one found in Ubuntu 14.04.
  //
  // See the following gdb bug reports for more information:
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=11868
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=12127
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=16196
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=16286
  autodigit_less lt;
  if (lt(version, "7.7")) {
    return Status::NotSupported("gdb version too old", version);
  }

  return Status::OK();
}

Status PstackWatcher::DumpStacks(int flags) {
  return DumpPidStacks(getpid(), flags);
}

Status PstackWatcher::DumpPidStacks(pid_t pid, int flags) {

  // Prefer GDB if available; it gives us line numbers and thread names.
  Status s = HasGoodGdb();
  if (s.ok()) {
    return RunGdbStackDump(pid, flags);
  }
  WARN_NOT_OK(s, "gdb not available");

  // Otherwise, try to use pstack or gstack.
  for (const auto& p : { "pstack", "gstack" }) {
    s = HasProgram(p);
    if (s.ok()) {
      return RunPstack(p, pid);
    }
    WARN_NOT_OK(s, Substitute("$0 not available", p));
  }

  return Status::ServiceUnavailable("Neither gdb, pstack, nor gstack appear to be installed.");
}

Status PstackWatcher::RunGdbStackDump(pid_t pid, int flags) {
  // Command: gdb -quiet -batch -nx -ex cmd1 -ex cmd2 /proc/$PID/exe $PID
  vector<string> argv;
  argv.emplace_back("gdb");
  // Don't print introductory version/copyright messages.
  argv.emplace_back("-quiet");
  // Exit after processing all of the commands below.
  argv.emplace_back("-batch");
  // Don't run commands from .gdbinit
  argv.emplace_back("-nx");
  argv.emplace_back("-ex");
  argv.emplace_back("set print pretty on");
  argv.emplace_back("-ex");
  argv.emplace_back("info threads");
  argv.emplace_back("-ex");
  argv.emplace_back("thread apply all bt");
  if (flags & DUMP_FULL) {
    argv.emplace_back("-ex");
    argv.emplace_back("thread apply all bt full");
  }
  string executable;
  Env* env = Env::Default();
  RETURN_NOT_OK(env->GetExecutablePath(&executable));
  argv.push_back(executable);
  argv.push_back(Substitute("$0", pid));
  return RunStackDump(argv);
}

Status PstackWatcher::RunPstack(const std::string& progname, pid_t pid) {
  string pid_string(Substitute("$0", pid));
  vector<string> argv;
  argv.push_back(progname);
  argv.push_back(pid_string);
  return RunStackDump(argv);
}

Status PstackWatcher::RunStackDump(const vector<string>& argv) {
  printf("************************ BEGIN STACKS **************************\n");
  if (fflush(stdout) == EOF) {
    return Status::IOError("Unable to flush stdout", ErrnoToString(errno), errno);
  }
  Subprocess pstack_proc(argv);
  RETURN_NOT_OK_PREPEND(pstack_proc.Start(), "RunStackDump proc.Start() failed");
  int ret;
  RETRY_ON_EINTR(ret, ::close(pstack_proc.ReleaseChildStdinFd()));
  if (ret == -1) {
    return Status::IOError("Unable to close child stdin", ErrnoToString(errno), errno);
  }
  RETURN_NOT_OK_PREPEND(pstack_proc.Wait(), "RunStackDump proc.Wait() failed");
  int exit_code;
  string exit_info;
  RETURN_NOT_OK_PREPEND(pstack_proc.GetExitStatus(&exit_code, &exit_info),
                        "RunStackDump proc.GetExitStatus() failed");
  if (exit_code != 0) {
    return Status::RuntimeError("RunStackDump proc.Wait() error", exit_info);
  }
  printf("************************* END STACKS ***************************\n");
  if (fflush(stdout) == EOF) {
    return Status::IOError("Unable to flush stdout", ErrnoToString(errno), errno);
  }

  return Status::OK();
}

} // namespace kudu
