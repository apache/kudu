// Copyright (c) 2014, Cloudera, inc.

#include "util/subprocess.h"

#include <boost/foreach.hpp>
#include <dirent.h>
#include <glog/logging.h>
#include <fcntl.h>
#include <string>
#include <vector>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "gutil/once.h"
#include "gutil/port.h"
#include "gutil/strings/join.h"
#include "gutil/strings/numbers.h"
#include "util/debug-util.h"
#include "util/errno.h"

using std::string;
using std::vector;

namespace kudu {

namespace {
void DisableSigPipe() {
  struct sigaction act;

  act.sa_handler = SIG_IGN;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  PCHECK(sigaction(SIGPIPE, &act, NULL) == 0);
}

void EnsureSigPipeDisabled() {
  static GoogleOnceType once = GOOGLE_ONCE_INIT;
  GoogleOnceInit(&once, &DisableSigPipe);
}

// Close all open file descriptors other than stdin, stderr, stdout.
void CloseNonStandardFDs() {
#ifndef __linux__
#error This function is Linux-specific.
#endif
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
  errno = 0;
  DIR* d = opendir("/proc/self/fd");
  int dir_fd = dirfd(d);
  PCHECK(d != NULL);

  struct dirent64 *ent;
  while ((ent = readdir64(d)) != NULL) {
    uint32_t fd;
    if (!safe_strtou32(ent->d_name, &fd)) continue;
    if (fd >= 3 && fd != dir_fd) {
      close(fd);
    }
  }

  PCHECK(closedir(d) == 0);
}

} // anonymous namespace

Subprocess::Subprocess(const string& program,
                       const vector<string>& argv)
  : program_(program),
    argv_(argv),
    started_(false),
    child_pid_(-1),
    to_child_stdin_(-1),
    from_child_stdout_(-1),
    disable_stderr_(false),
    disable_stdout_(false) {
}

Subprocess::~Subprocess() {
  if (started_) {
    LOG(WARNING) << "Child process " << child_pid_
                 << "(" << JoinStrings(argv_, " ") << ") "
                 << " was orphaned. Sending SIGKILL...";
    WARN_NOT_OK(Kill(SIGKILL), "Failed to send SIGKILL");
    int junk = 0;
    WARN_NOT_OK(Wait(&junk), "Failed to Wait()");
  }

  if (to_child_stdin_ >= 0) {
    close(to_child_stdin_);
  }
  if (from_child_stdout_ >= 0) {
    close(from_child_stdout_);
  }
}

void Subprocess::DisableStderr() {
  CHECK(!started_);
  disable_stderr_ = true;
}

void Subprocess::DisableStdout() {
  CHECK(!started_);
  disable_stdout_ = true;
}

static void RedirectToDevNull(int fd) {
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

Status Subprocess::Start() {
  EnsureSigPipeDisabled();

  if (argv_.size() < 1) {
    return Status::InvalidArgument("argv must have at least one elem");
  }

  vector<char*> argv_ptrs;
  BOOST_FOREACH(const string& arg, argv_) {
    argv_ptrs.push_back(const_cast<char*>(arg.c_str()));
  }
  argv_ptrs.push_back(NULL);

  // Pipe from caller process to child's stdin
  // [0] = stdin for child, [1] = how parent writes to it
  int child_stdin[2];
  PCHECK(pipe2(child_stdin, O_CLOEXEC) == 0);

  // Pipe from child's stdout back to caller process
  int child_stdout[2];
  // [0] = how parent reads from child's stdout, [1] = how child writes to it
  PCHECK(pipe2(child_stdout, O_CLOEXEC) == 0);

  int ret = fork();
  if (ret == -1) {
    return Status::RuntimeError("Unable to fork", ErrnoToString(errno), errno);
  }
  if (ret == 0) {
    // We are the child
    PCHECK(dup2(child_stdin[0], STDIN_FILENO) == STDIN_FILENO);
    PCHECK(dup2(child_stdout[1], STDOUT_FILENO) == STDOUT_FILENO);
    CloseNonStandardFDs();
    if (disable_stderr_) {
      RedirectToDevNull(STDERR_FILENO);
    }
    if (disable_stdout_) {
      RedirectToDevNull(STDOUT_FILENO);
    }

    execvp(program_.c_str(), &argv_ptrs[0]);
    PLOG(WARNING) << "Couldn't exec";
    _exit(errno);
  } else {
    // We are the parent
    child_pid_ = ret;

    close(child_stdin[0]);
    close(child_stdout[1]);
    to_child_stdin_ = child_stdin[1];
    from_child_stdout_ = child_stdout[0];
  }

  started_ = true;
  return Status::OK();
}

Status Subprocess::Wait(int* ret) {
  return DoWait(ret, 0);
}

Status Subprocess::WaitNoBlock(int* ret) {
  return DoWait(ret, WNOHANG);
}

Status Subprocess::DoWait(int* ret, int options) {
  CHECK(started_);
  int rc = waitpid(child_pid_, ret, options);
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
  started_ = false;
  return Status::OK();
}

Status Subprocess::Kill(int signal) {
  CHECK(started_);
  if (kill(child_pid_, signal) != 0) {
    return Status::RuntimeError("Unable to kill",
                                ErrnoToString(errno),
                                errno);
  }
  return Status::OK();
}

int Subprocess::ReleaseChildStdinFd() {
  CHECK(started_);
  CHECK_GE(to_child_stdin_, 0);
  int ret = to_child_stdin_;
  to_child_stdin_ = -1;
  return ret;
}

int Subprocess::ReleaseChildStdoutFd() {
  CHECK(started_);
  CHECK_GE(from_child_stdout_, 0);
  int ret = from_child_stdout_;
  from_child_stdout_ = -1;
  return ret;
}

} // namespace kudu
