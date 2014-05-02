// Copyright 2014 Cloudera Inc.

#include "util/pstack_watcher.h"

#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <tr1/memory>
#include <unistd.h>
#include <vector>

#include "gutil/strings/substitute.h"
#include "util/status.h"
#include "util/subprocess.h"
#include "util/test_util.h"

using kudu::Status;
using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

PstackWatcher::PstackWatcher(const MonoDelta& timeout)
  : timeout_(timeout),
    running_(true) {
  CHECK_OK(Thread::Create("pstack_watcher", "pstack_watcher",
                 boost::bind(&PstackWatcher::Run, this), &thread_));
}

PstackWatcher::~PstackWatcher() {
  Shutdown();
}

void PstackWatcher::Shutdown() {
  {
    boost::lock_guard<boost::mutex> guard(lock_);
    if (!running_) return;
    running_ = false;
    cond_.notify_all();
  }
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
  thread_.reset();
}

bool PstackWatcher::IsRunning() {
  boost::lock_guard<boost::mutex> guard(lock_);
  return running_;
}

void PstackWatcher::Run() {
  boost::system_time end_time = boost::get_system_time() +
        boost::posix_time::milliseconds(timeout_.ToMilliseconds());
  boost::unique_lock<boost::mutex> guard(lock_);
  while (true) {
    if (!running_) {
      return;
    }
    boost::system_time cur_time = boost::get_system_time();
    if (cur_time >= end_time) {
      break;
    }
    cond_.timed_wait(guard, end_time);
  }
  LogPstack();
  running_ = false;
}

Status PstackWatcher::HasProgram(const char* progname) {
  string which("which");
  vector<string> argv;
  argv.push_back(which);
  argv.push_back(progname);
  Subprocess proc(which, argv);
  proc.DisableStderr();
  Status status = proc.Start();
  if (!status.ok()) {
    LOG(WARNING) << "HasProgram(" << progname << "): error running 'which': "
                 << status.ToString();
    return status;
  }
  ::close(proc.ReleaseChildStdoutFd());
  int wait_status = 0;
  RETURN_NOT_OK(proc.Wait(&wait_status));
  if ((WIFEXITED(wait_status)) && (0 == WEXITSTATUS(wait_status))) {
    return Status::OK();
  }
  return Status::NotFound(StringPrintf("can't find %s", progname));
}

void PstackWatcher::LogPstack() {
  const char *progname = NULL;
  Status pstack_status = HasProgram("pstack");
  if (pstack_status.ok()) {
    progname = "pstack";
  } else {
    Status gstack_status = HasProgram("gstack");
    if (gstack_status.ok()) {
      progname = "gstack";
    }
  }
  if (!progname) {
    LOG(WARNING) << "neither pstack nor gstack appears to be installed.";
    return;
  }
  pid_t pid = getpid();
  string prog(progname);
  string pid_string(Substitute("$0", pid));
  vector<string> argv;
  argv.push_back(prog);
  argv.push_back(pid_string);
  Subprocess pstack_proc(prog, argv);
  CHECK_OK(pstack_proc.Start());
  close(pstack_proc.ReleaseChildStdinFd());

  printf("************************ BEGIN STACKS **************************\n");
  FILE* in = fdopen(pstack_proc.from_child_stdout_fd(), "r");
  char buf[16384] = { 0 };
  shared_ptr<FILE> in_wrapper(in, ::fclose);
  while (true) {
    int res = fread(buf, 1, sizeof(buf), in);
    if (res <= 0) {
      break;
    }
    fwrite(buf, 1, res, stdout);
  }
  printf("************************ END STACKS **************************\n");
  int ret;
  CHECK_OK(pstack_proc.Wait(&ret));
}

} // namespace kudu
