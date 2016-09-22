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

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <ev++.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/gutil/once.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {

namespace {

static const char* kProcSelfFd =
#if defined(__APPLE__)
  "/dev/fd";
#else
  "/proc/self/fd";
#endif // defined(__APPLE__)

#if defined(__linux__)
#define READDIR readdir64
#define DIRENT dirent64
#else
#define READDIR readdir
#define DIRENT dirent
#endif

void DisableSigPipe() {
  struct sigaction act;

  act.sa_handler = SIG_IGN;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  PCHECK(sigaction(SIGPIPE, &act, nullptr) == 0);
}

void EnsureSigPipeDisabled() {
  static GoogleOnceType once = GOOGLE_ONCE_INIT;
  GoogleOnceInit(&once, &DisableSigPipe);
}

// Since opendir() calls malloc(), this must be called before fork().
// This function is not async-signal-safe.
Status OpenProcFdDir(DIR** dir) {
  *dir = opendir(kProcSelfFd);
  if (PREDICT_FALSE(dir == nullptr)) {
    return Status::IOError(Substitute("opendir(\"$0\") failed", kProcSelfFd),
                           ErrnoToString(errno), errno);
  }
  return Status::OK();
}

// Close the directory stream opened by OpenProcFdDir().
// This function is not async-signal-safe.
void CloseProcFdDir(DIR* dir) {
  if (PREDICT_FALSE(closedir(dir) == -1)) {
    LOG(WARNING) << "Unable to close fd dir: "
                 << Status::IOError(Substitute("closedir(\"$0\") failed", kProcSelfFd),
                                    ErrnoToString(errno), errno).ToString();
  }
}

// Close all open file descriptors other than stdin, stderr, stdout.
// Expects a directory stream created by OpenProdFdDir() as a parameter.
// This function is called after fork() and must not call malloc().
// The rule of thumb is to only call async-signal-safe functions in such cases
// if at all possible.
void CloseNonStandardFDs(DIR* fd_dir) {
  // This is implemented by iterating over the open file descriptors
  // rather than using sysconf(SC_OPEN_MAX) -- the latter is error prone
  // since it may not represent the highest open fd if the fd soft limit
  // has changed since the process started. This should also be faster
  // since iterating over all possible fds is likely to cause 64k+ syscalls
  // in typical configurations.
  //
  // Note also that this doesn't use any of the Env utility functions, to
  // make it as lean and mean as possible -- this runs in the subprocess
  // after a fork, so there's some possibility that various global locks
  // inside malloc() might be held, so allocating memory is a no-no.
  PCHECK(fd_dir != nullptr);
  int dir_fd = dirfd(fd_dir);

  struct DIRENT* ent;
  // readdir64() is not reentrant (it uses a static buffer) and it also
  // locks fd_dir->lock, so it must not be called in a multi-threaded
  // environment and is certainly not async-signal-safe.
  // However, it appears to be safe to call right after fork(), since only one
  // thread exists in the child process at that time. It also does not call
  // malloc() or free(). We could use readdir64_r() instead, but all that
  // buys us is reentrancy, and not async-signal-safety, due to the use of
  // dir->lock, so seems not worth the added complexity in lifecycle & plumbing.
  while ((ent = READDIR(fd_dir)) != nullptr) {
    uint32_t fd;
    if (!safe_strtou32(ent->d_name, &fd)) continue;
    if (!(fd == STDIN_FILENO  ||
          fd == STDOUT_FILENO ||
          fd == STDERR_FILENO ||
          fd == dir_fd))  {
      close(fd);
    }
  }
}

void RedirectToDevNull(int fd) {
  // We must not close stderr or stdout, because then when a new file descriptor
  // gets opened, it might get that fd number.  (We always allocate the lowest
  // available file descriptor number.)  Instead, we reopen that fd as
  // /dev/null.
  int dev_null = open("/dev/null", O_WRONLY);
  if (dev_null < 0) {
    PLOG(WARNING) << "failed to open /dev/null";
  } else {
    PCHECK(dup2(dev_null, fd));
  }
}

// Stateful libev watcher to help ReadFdsFully().
class ReadFdsFullyHelper {
 public:
  ReadFdsFullyHelper(const string& progname, ev::dynamic_loop* loop, int fd)
      : progname_(progname) {
    // Bind the watcher to the provided loop, to this functor, and to the
    // readable fd.
    watcher_.set(*loop);
    watcher_.set(this);
    watcher_.set(fd, ev::READ);

    // The watcher will now be polled when its loop is run.
    watcher_.start();
  }

  void operator() (ev::io &w, int revents) {
    DCHECK_EQ(ev::READ, revents);

    char buf[1024];
    ssize_t n = read(w.fd, buf, arraysize(buf));
    if (n == 0) {
      // EOF, stop watching.
      w.stop();
    } else if (n < 0) {
      // Interrupted by a signal, do nothing.
      if (errno == EINTR) return;

      // A fatal error. Store it and stop watching.
      status_ = Status::IOError("IO error reading from " + progname_,
                                ErrnoToString(errno), errno);
      w.stop();
    } else {
      // Add our bytes and keep watching.
      output_.append(buf, n);
    }
  }

  const Status& status() const { return status_; }
  const string& output() const { return output_; }

 private:
  const string progname_;

  ev::io watcher_;
  string output_;
  Status status_;
};

// Reads from all descriptors in 'fds' until EOF on all of them. If any read
// yields an error, it is returned. Otherwise, 'out' contains the bytes read
// for each fd, in the same order as was in 'fds'.
Status ReadFdsFully(const string& progname,
                    const vector<int>& fds,
                    vector<string>* out) {
  ev::dynamic_loop loop;

  // Set up a watcher for each fd.
  vector<unique_ptr<ReadFdsFullyHelper>> helpers;
  for (int fd : fds) {
    helpers.emplace_back(new ReadFdsFullyHelper(progname, &loop, fd));
  }

  // This will read until all fds return EOF.
  loop.run();

  // Check for failures.
  for (const auto& h : helpers) {
    if (!h->status().ok()) {
      return h->status();
    }
  }

  // No failures; write the output to the caller.
  for (const auto& h : helpers) {
    out->push_back(h->output());
  }
  return Status::OK();
}

} // anonymous namespace

Subprocess::Subprocess(string program, vector<string> argv)
    : program_(std::move(program)),
      argv_(std::move(argv)),
      state_(kNotStarted),
      child_pid_(-1),
      fd_state_(),
      child_fds_() {
  fd_state_[STDIN_FILENO]   = PIPED;
  fd_state_[STDOUT_FILENO]  = SHARED;
  fd_state_[STDERR_FILENO]  = SHARED;
  child_fds_[STDIN_FILENO]  = -1;
  child_fds_[STDOUT_FILENO] = -1;
  child_fds_[STDERR_FILENO] = -1;
}

Subprocess::~Subprocess() {
  if (state_ == kRunning) {
    LOG(WARNING) << "Child process " << child_pid_
                 << "(" << JoinStrings(argv_, " ") << ") "
                 << " was orphaned. Sending SIGKILL...";
    WARN_NOT_OK(Kill(SIGKILL), "Failed to send SIGKILL");
    int junk = 0;
    WARN_NOT_OK(Wait(&junk), "Failed to Wait()");
  }

  for (int i = 0; i < 3; ++i) {
    if (fd_state_[i] == PIPED && child_fds_[i] >= 0) {
      close(child_fds_[i]);
    }
  }
}

void Subprocess::SetFdShared(int stdfd, bool share) {
  CHECK_EQ(state_, kNotStarted);
  CHECK_NE(fd_state_[stdfd], DISABLED);
  fd_state_[stdfd] = share? SHARED : PIPED;
}

void Subprocess::DisableStderr() {
  CHECK_EQ(state_, kNotStarted);
  fd_state_[STDERR_FILENO] = DISABLED;
}

void Subprocess::DisableStdout() {
  CHECK_EQ(state_, kNotStarted);
  fd_state_[STDOUT_FILENO] = DISABLED;
}

#if defined(__APPLE__)
static int pipe2(int pipefd[2], int flags) {
  DCHECK_EQ(O_CLOEXEC, flags);

  int new_fds[2];
  if (pipe(new_fds) == -1) {
    return -1;
  }
  if (fcntl(new_fds[0], F_SETFD, O_CLOEXEC) == -1) {
    close(new_fds[0]);
    close(new_fds[1]);
    return -1;
  }
  if (fcntl(new_fds[1], F_SETFD, O_CLOEXEC) == -1) {
    close(new_fds[0]);
    close(new_fds[1]);
    return -1;
  }
  pipefd[0] = new_fds[0];
  pipefd[1] = new_fds[1];
  return 0;
}
#endif

Status Subprocess::Start() {
  if (state_ != kNotStarted) {
    return Status::IllegalState(
        Substitute("$0: illegal sub-process state", state_));
  }
  if (argv_.empty()) {
    return Status::InvalidArgument("argv must have at least one elem");
  }
  EnsureSigPipeDisabled();

  vector<char*> argv_ptrs;
  for (const string& arg : argv_) {
    argv_ptrs.push_back(const_cast<char*>(arg.c_str()));
  }
  argv_ptrs.push_back(nullptr);

  // Pipe from caller process to child's stdin
  // [0] = stdin for child, [1] = how parent writes to it
  int child_stdin[2] = {-1, -1};
  if (fd_state_[STDIN_FILENO] == PIPED) {
    PCHECK(pipe2(child_stdin, O_CLOEXEC) == 0);
  }
  // Pipe from child's stdout back to caller process
  // [0] = how parent reads from child's stdout, [1] = how child writes to it
  int child_stdout[2] = {-1, -1};
  if (fd_state_[STDOUT_FILENO] == PIPED) {
    PCHECK(pipe2(child_stdout, O_CLOEXEC) == 0);
  }
  // Pipe from child's stderr back to caller process
  // [0] = how parent reads from child's stderr, [1] = how child writes to it
  int child_stderr[2] = {-1, -1};
  if (fd_state_[STDERR_FILENO] == PIPED) {
    PCHECK(pipe2(child_stderr, O_CLOEXEC) == 0);
  }

  DIR* fd_dir = nullptr;
  RETURN_NOT_OK_PREPEND(OpenProcFdDir(&fd_dir), "Unable to open fd dir");
  unique_ptr<DIR, std::function<void(DIR*)>> fd_dir_closer(fd_dir,
                                                           CloseProcFdDir);
  int ret = fork();
  if (ret == -1) {
    return Status::RuntimeError("Unable to fork", ErrnoToString(errno), errno);
  }
  if (ret == 0) { // We are the child
    // Send the child a SIGTERM when the parent dies. This is done as early
    // as possible in the child's life to prevent any orphaning whatsoever
    // (e.g. from KUDU-402).
#if defined(__linux__)
    // TODO: prctl(PR_SET_PDEATHSIG) is Linux-specific, look into portable ways
    // to prevent orphans when parent is killed.
    prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif

    // stdin
    if (fd_state_[STDIN_FILENO] == PIPED) {
      PCHECK(dup2(child_stdin[0], STDIN_FILENO) == STDIN_FILENO);
    }
    // stdout
    switch (fd_state_[STDOUT_FILENO]) {
    case PIPED: {
      PCHECK(dup2(child_stdout[1], STDOUT_FILENO) == STDOUT_FILENO);
      break;
    }
    case DISABLED: {
      RedirectToDevNull(STDOUT_FILENO);
      break;
    }
    default: break;
    }
    // stderr
    switch (fd_state_[STDERR_FILENO]) {
    case PIPED: {
      PCHECK(dup2(child_stderr[1], STDERR_FILENO) == STDERR_FILENO);
      break;
    }
    case DISABLED: {
      RedirectToDevNull(STDERR_FILENO);
      break;
    }
    default: break;
    }

    CloseNonStandardFDs(fd_dir);

    execvp(program_.c_str(), &argv_ptrs[0]);
    PLOG(WARNING) << "Couldn't exec " << program_;
    _exit(errno);
  } else {
    // We are the parent
    child_pid_ = ret;
    // Close child's side of the pipes
    if (fd_state_[STDIN_FILENO]  == PIPED) close(child_stdin[0]);
    if (fd_state_[STDOUT_FILENO] == PIPED) close(child_stdout[1]);
    if (fd_state_[STDERR_FILENO] == PIPED) close(child_stderr[1]);
    // Keep parent's side of the pipes
    child_fds_[STDIN_FILENO]  = child_stdin[1];
    child_fds_[STDOUT_FILENO] = child_stdout[0];
    child_fds_[STDERR_FILENO] = child_stderr[0];
  }

  state_ = kRunning;
  return Status::OK();
}

Status Subprocess::DoWait(int* ret, int options) {
  if (state_ == kExited) {
    if (ret != nullptr) {
      *ret = cached_rc_;
    }
    return Status::OK();
  }
  if (state_ != kRunning) {
    return Status::IllegalState(
        Substitute("$0: illegal sub-process state", state_));
  }

  int proc_exit_info;
  int rc = waitpid(child_pid_, &proc_exit_info, options);
  if (rc == -1) {
    return Status::RuntimeError("Unable to wait on child",
                                ErrnoToString(errno),
                                errno);
  }
  if ((options & WNOHANG) && rc == 0) {
    return Status::TimedOut("");
  }

  CHECK_EQ(rc, child_pid_);
  child_pid_ = -1;
  cached_rc_ = proc_exit_info;
  state_ = kExited;
  if (ret != nullptr) {
    *ret = proc_exit_info;
  }
  return Status::OK();
}

Status Subprocess::Kill(int signal) {
  if (state_ != kRunning) {
    return Status::IllegalState("Sub-process is not running");
  }
  if (kill(child_pid_, signal) != 0) {
    return Status::RuntimeError("Unable to kill",
                                ErrnoToString(errno),
                                errno);
  }
  return Status::OK();
}

Status Subprocess::Call(const string& arg_str) {
  vector<string> argv = Split(arg_str, " ");
  return Call(argv, nullptr, nullptr);
}

Status Subprocess::Call(const vector<string>& argv,
                        string* stdout_out,
                        string* stderr_out) {
  VLOG(2) << "Invoking command: " << argv;
  Subprocess p(argv[0], argv);

  if (stdout_out) {
    p.ShareParentStdout(false);
  }
  if (stderr_out) {
    p.ShareParentStderr(false);
  }
  RETURN_NOT_OK_PREPEND(p.Start(),
                        "Unable to fork " + argv[0]);
  int err = close(p.ReleaseChildStdinFd());
  if (PREDICT_FALSE(err != 0)) {
    return Status::IOError("Unable to close child process stdin", ErrnoToString(errno), errno);
  }

  vector<int> fds;
  if (stdout_out) {
    fds.push_back(p.from_child_stdout_fd());
  }
  if (stderr_out) {
    fds.push_back(p.from_child_stderr_fd());
  }
  vector<string> out;
  RETURN_NOT_OK(ReadFdsFully(argv[0], fds, &out));

  if (stdout_out) {
    *stdout_out = std::move(out[0]);
  }
  if (stderr_out) {
    *stderr_out = std::move(out[1]);
  }

  int retcode;
  RETURN_NOT_OK_PREPEND(p.Wait(&retcode), "Unable to wait() for " + argv[0]);

  if (PREDICT_FALSE(retcode != 0)) {
    return Status::RuntimeError(Substitute(
        "Subprocess '$0' terminated with non-zero exit status $1",
        argv[0],
        retcode));
  }
  return Status::OK();
}

int Subprocess::CheckAndOffer(int stdfd) const {
  CHECK_EQ(state_, kRunning);
  CHECK_EQ(fd_state_[stdfd], PIPED);
  return child_fds_[stdfd];
}

int Subprocess::ReleaseChildFd(int stdfd) {
  CHECK_EQ(state_, kRunning);
  CHECK_GE(child_fds_[stdfd], 0);
  CHECK_EQ(fd_state_[stdfd], PIPED);
  int ret = child_fds_[stdfd];
  child_fds_[stdfd] = -1;
  return ret;
}

pid_t Subprocess::pid() const {
  CHECK_EQ(state_, kRunning);
  return child_pid_;
}

} // namespace kudu
