// Copyright 2014 Cloudera Inc.

#include "util/pstack_watcher.h"

#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <tr1/memory>
#include <unistd.h>
#include <vector>

#include "gutil/strings/substitute.h"
#include "util/errno.h"
#include "util/status.h"
#include "util/subprocess.h"

namespace kudu {

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

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

bool PstackWatcher::IsRunning() const {
  boost::lock_guard<boost::mutex> guard(lock_);
  return running_;
}

void PstackWatcher::Wait() const {
  boost::unique_lock<boost::mutex> lock(lock_);
  while (running_) {
    cond_.wait(lock);
  }
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
  WARN_NOT_OK(DumpStacks(), "Unable to print pstack from watcher");
  running_ = false;
  cond_.notify_all();
}

Status PstackWatcher::HasProgram(const char* progname) {
  string which("which");
  vector<string> argv;
  argv.push_back(which);
  argv.push_back(progname);
  Subprocess proc(which, argv);
  proc.DisableStderr();
  proc.DisableStdout();
  RETURN_NOT_OK_PREPEND(proc.Start(),
      Substitute("HasProgram($0): error running 'which'", progname));
  int wait_status = 0;
  RETURN_NOT_OK(proc.Wait(&wait_status));
  if ((WIFEXITED(wait_status)) && (0 == WEXITSTATUS(wait_status))) {
    return Status::OK();
  }
  return Status::NotFound(Substitute("can't find $0: exited?=$1, status=$2",
                                     progname,
                                     static_cast<bool>(WIFEXITED(wait_status)),
                                     WEXITSTATUS(wait_status)));
}

Status PstackWatcher::DumpStacks() {
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
    return Status::ServiceUnavailable("neither pstack nor gstack appears to be installed.");
  }
  pid_t pid = getpid();
  string prog(progname);
  string pid_string(Substitute("$0", pid));
  vector<string> argv;
  argv.push_back(prog);
  argv.push_back(pid_string);
  Subprocess pstack_proc(prog, argv);
  pstack_proc.ShareParentStdout(false);
  RETURN_NOT_OK_PREPEND(pstack_proc.Start(), "DumpStacks proc.Start() failed");
  if (::close(pstack_proc.ReleaseChildStdinFd()) == -1) {
    return Status::IOError("Unable to close child stdin", ErrnoToString(errno), errno);
  }

  printf("************************ BEGIN STACKS **************************\n");
  FILE* in = ::fdopen(pstack_proc.from_child_stdout_fd(), "r");
  if (in == NULL) {
    return Status::IOError("Unable to open child stdout for read", ErrnoToString(errno), errno);
  }
  char buf[16384] = { 0 };
  shared_ptr<FILE> in_wrapper(in, ::fclose);
  bool error = false;
  while (!error) {
    int bytes_to_write = ::fread(buf, 1, sizeof(buf), in);
    if (bytes_to_write <= 0) {
      break;
    }
    while (bytes_to_write > 0) {
      int bytes_written = ::fwrite(buf, 1, bytes_to_write, stdout);
      if (bytes_written <= 0) {
        error = true;
        break;
      }
      bytes_to_write -= bytes_written;
    }
  }
  printf("************************* END STACKS ***************************\n");
  int ret;
  RETURN_NOT_OK_PREPEND(pstack_proc.Wait(&ret), "DumpStacks proc.Wait() failed");
  if (ret == -1) {
    return Status::RuntimeError("DumpStacks proc.Wait() error", ErrnoToString(errno), errno);
  }
  return Status::OK();
}

} // namespace kudu
