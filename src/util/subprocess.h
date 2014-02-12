// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_UTIL_SUBPROCESS_H
#define KUDU_UTIL_SUBPROCESS_H

#include <glog/logging.h>
#include <string>
#include <vector>

#include "gutil/macros.h"
#include "util/status.h"

namespace kudu {

// Wrapper around a spawned subprocess.
//
// This takes care of creating pipes to/from the subprocess and offers
// basic functionality to wait on it or send signals.
//
// Note that, when the Subprocess object is destructed, the child process
// will be forcibly SIGKILLed to avoid orphaning processes.
class Subprocess {
 public:
  Subprocess(const std::string& exec_path,
             const std::vector<std::string>& argv);
  ~Subprocess();

  // Start the subprocess.
  //
  // Thie returns a bad Status if the fork() fails. However,
  // note that if the executable path was incorrect such that
  // exec() fails, this will still return Status::OK. You must
  // use Wait() to check for failure.
  Status Start();

  // Wait for the subprocess to exit. The return value is the same as
  // that of the waitpid() syscall.
  Status Wait(int* ret);

  // Like the above, but does not block. This returns Status::TimedOut
  // immediately if the child has not exited. Otherwise returns Status::OK
  // and sets *ret.
  Status WaitNoBlock(int* ret);

  // Send a signal to the subprocess.
  // Note that this does not reap the process -- you must still Wait()
  // in order to reap it.
  Status Kill(int signal);

  // Return the pipe fd to the child's standard input.
  int to_child_stdin_fd() const {
    CHECK(started_);
    return to_child_stdin_;
  }

  // Release control of the file descriptor for the child's stdin.
  // Writes to this FD show up on stdin in the subprocess.
  int ReleaseChildStdinFd();

  // Return the pipe fd from the child's standard output.
  int from_child_stdout_fd() const {
    CHECK(started_);
    return from_child_stdout_;
  }

  // Release control of the file descriptor for the child's stdout.
  // Writes to this FD show up on stdin in the subprocess.
  int ReleaseChildStdoutFd();

 private:
  Status DoWait(int* ret, int options);

  std::string exec_path_;
  std::vector<std::string> argv_;

  bool started_;
  int child_pid_;
  int to_child_stdin_;
  int from_child_stdout_;

  DISALLOW_COPY_AND_ASSIGN(Subprocess);
};

} // namespace kudu
#endif /* KUDU_UTIL_SUBPROCESS_H */
