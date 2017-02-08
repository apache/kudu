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

#include "kudu/util/minidump.h"

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>

#if defined(__linux__)
#include <breakpad/client/linux/handler/exception_handler.h>
#include <breakpad/common/linux/linux_libc_support.h>
#endif // defined(__linux__)

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/linux_syscall_support.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/errno.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using kudu::env_util::CreateDirIfMissing;
using std::string;

#if defined(__linux__)
static constexpr bool kMinidumpPlatformSupported = true;
#else
static constexpr bool kMinidumpPlatformSupported = false;
#endif // defined(__linux__)

DECLARE_string(log_dir);

DEFINE_bool(enable_minidumps, kMinidumpPlatformSupported,
            "Whether to enable minidump generation upon process crash or SIGUSR1. "
            "Currently only supported on Linux systems.");
TAG_FLAG(enable_minidumps, advanced);
TAG_FLAG(enable_minidumps, evolving);
static bool ValidateMinidumpEnabled(const char* /*flagname*/, bool value) {
  if (value && !kMinidumpPlatformSupported) {
    return false; // NOLINT(*)
  }
  return true;
}
DEFINE_validator(enable_minidumps, &ValidateMinidumpEnabled);

DEFINE_string(minidump_path, "minidumps", "Directory to write minidump files to. This "
    "can be either an absolute path or a path relative to --log_dir. Each daemon will "
    "create an additional sub-directory to prevent naming conflicts and to make it "
    "easier to identify a crashing daemon. Minidump files contain crash-related "
    "information in a compressed format. Minidumps will be written when a daemon exits "
    "unexpectedly, for example on an unhandled exception or signal, or when a "
    "SIGUSR1 signal is sent to the process. Cannot be set to an empty value.");
TAG_FLAG(minidump_path, evolving);
// The minidump path cannot be empty.
static bool ValidateMinidumpPath(const char* /*flagname*/, const string& value) {
  return !value.empty();
}
DEFINE_validator(minidump_path, &ValidateMinidumpPath);

DEFINE_int32(max_minidumps, 9, "Maximum number of minidump files to keep per daemon. "
    "Older files are removed first. Set to 0 to keep all minidump files.");
TAG_FLAG(max_minidumps, evolving);

DEFINE_int32(minidump_size_limit_hint_kb, 20480, "Size limit hint for minidump files in "
    "KB. If a minidump exceeds this value, then breakpad will reduce the stack memory it "
    "collects for each thread from 8KB to 2KB. However it will always include the full "
    "stack memory for the first 20 threads, including the thread that crashed.");
TAG_FLAG(minidump_size_limit_hint_kb, advanced);
TAG_FLAG(minidump_size_limit_hint_kb, evolving);

#if !defined(__linux__)
namespace google_breakpad {
// Define this as an empty class to avoid an undefined symbol error on Mac.
class ExceptionHandler {
 public:
  ExceptionHandler() {}
  ~ExceptionHandler() {}
};
} // namespace google_breakpad
#endif // !defined(__linux__)

namespace kudu {

static sigset_t GetSigset(int signo) {
  sigset_t signals;
  CHECK_EQ(0, sigemptyset(&signals));
  CHECK_EQ(0, sigaddset(&signals, signo));
  return signals;
}

#if defined(__linux__)

// Called by the exception handler before minidump is produced.
// Minidump is only written if this returns true.
static bool FilterCallback(void* /*context*/) {
  return true;
}

// Write two null-terminated strings and a newline to both stdout and stderr.
static void WriteLineStdoutStderr(const char* msg1, const char* msg2) {
  // We use Breakpad's reimplementation of strlen(), called my_strlen(), from
  // linux_libc_support.h to avoid calling into libc.
  // A comment from linux_libc_support.h is reproduced here:
  // "This header provides replacements for libc functions that we need. If we
  // call the libc functions directly we risk crashing in the dynamic linker as
  // it tries to resolve uncached PLT entries."
  int msg1_len = my_strlen(msg1);
  int msg2_len = my_strlen(msg2);

  // We use sys_write() from linux_syscall_support.h here per the
  // recommendation of the breakpad docs for the same reasons as above.
  for (int fd : {STDOUT_FILENO, STDERR_FILENO}) {
    sys_write(fd, msg1, msg1_len);
    sys_write(fd, msg2, msg2_len);
    sys_write(fd, "\n", 1);
  }
}

// Callback for breakpad. It is called whenever a minidump file has been
// written and should not be called directly. It logs the event before breakpad
// crashes the process. Due to the process being in a failed state we write to
// stdout/stderr and let the surrounding redirection make sure the output gets
// logged. The calls might still fail in unknown scenarios as the process is in
// a broken state. However we don't rely on them as the minidump file has been
// written already.
static bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor,
                         void* context, bool succeeded) {

  // Indicate whether a minidump file was written successfully. Write message
  // to stdout/stderr, which will usually be captured in the INFO/ERROR log.
  if (succeeded) {
    WriteLineStdoutStderr("Wrote minidump to ", descriptor.path());
  } else {
    WriteLineStdoutStderr("Failed to write minidump to ", descriptor.path());
  }

  // If invoked by a user signal, return the actual success or failure of
  // writing the minidump file so that we can print a user-friendly error
  // message if writing the minidump fails.
  bool is_user_signal = context != nullptr && *reinterpret_cast<bool*>(context);
  if (is_user_signal) {
    return succeeded;
  }

  // For crash signals. If we didn't want to invoke the previously-installed
  // signal handler from glog, we would return the value received in
  // 'succeeded' as described in the breakpad documentation. If this callback
  // function returned true, breakpad would not invoke the previously-installed
  // signal handler; instead, it would invoke the default signal handler, which
  // would cause the process to crash immediately after writing the minidump.
  //
  // We make this callback always return false so that breakpad will invoke any
  // previously-installed signal handler afterward. We want that to happen
  // because the glog signal handlers print a helpful stacktrace on crash.
  // That's convenient to have, because unlike a minidump, it doesn't need to
  // be decoded to be useful for debugging.
  return false;
}

// Failure function that simply calls abort().
static void AbortFailureFunction() {
  abort();
}

bool MinidumpExceptionHandler::WriteMinidump() {
  bool user_signal = true;
  return google_breakpad::ExceptionHandler::WriteMinidump(minidump_dir(),
                                                          &DumpCallback,
                                                          &user_signal);
}

Status MinidumpExceptionHandler::InitMinidumpExceptionHandler() {
  minidump_dir_ = FLAGS_minidump_path;
  if (minidump_dir_[0] != '/') {
    minidump_dir_ = JoinPathSegments(FLAGS_log_dir, minidump_dir_);
  }

  // Create the first-level minidump directory.
  Env* env = Env::Default();
  RETURN_NOT_OK_PREPEND(CreateDirIfMissing(env, minidump_dir_),
                        "Error creating top-level minidump directory");

  // Add the executable name to the path where minidumps will be written. This makes
  // identification easier and prevents name collisions between the files.
  // This is also consistent with how Impala organizes its minidump files.
  const char* exe_name = gflags::ProgramInvocationShortName();
  minidump_dir_ = JoinPathSegments(minidump_dir_, exe_name);

  // Create the directory if it is not there. The minidump doesn't get written if there is
  // no directory.
  RETURN_NOT_OK_PREPEND(CreateDirIfMissing(env, minidump_dir_),
                        "Error creating minidump directory");

  // Verify that the minidump directory really is a directory. We canonicalize
  // in case it's a symlink to a directory.
  string canonical_minidump_path;
  RETURN_NOT_OK(env->Canonicalize(minidump_dir_, &canonical_minidump_path));
  bool is_dir;
  RETURN_NOT_OK(env->IsDirectory(canonical_minidump_path, &is_dir));
  if (!is_dir) {
    return Status::IOError("Unable to create minidump directory", canonical_minidump_path);
  }

  google_breakpad::MinidumpDescriptor desc(minidump_dir_);

  // Limit filesize if configured.
  if (FLAGS_minidump_size_limit_hint_kb > 0) {
    size_t size_limit = 1024 * static_cast<int64_t>(FLAGS_minidump_size_limit_hint_kb);
    LOG(INFO) << "Setting minidump size limit to "
              << HumanReadableNumBytes::ToStringWithoutRounding(size_limit);
    desc.set_size_limit(size_limit);
  }

  // If we don't uninstall the glog failure function when minidumps are enabled
  // then we get two (2) stack traces printed from a LOG(FATAL) or CHECK(): one
  // from the glog failure function and one from the glog signal handler. That
  // is because we always return false in DumpCallback() in the non-user signal
  // case.
  google::InstallFailureFunction(&AbortFailureFunction);

  breakpad_handler_.reset(
      new google_breakpad::ExceptionHandler(desc,           // Path to minidump directory.
                                            FilterCallback, // Indicates whether to write the dump.
                                            DumpCallback,   // Output a log message when dumping.
                                            nullptr,        // Optional context for callbacks.
                                            true,           // Whether to install a crash handler.
                                            -1));           // -1: Use in-process dump generation.

  return Status::OK();
}

Status MinidumpExceptionHandler::RegisterMinidumpExceptionHandler() {
  if (!FLAGS_enable_minidumps) return Status::OK();

  // Ensure only one active instance is alive per process at any given time.
  CHECK_EQ(0, MinidumpExceptionHandler::current_num_instances_.fetch_add(1));
  RETURN_NOT_OK(InitMinidumpExceptionHandler());
  RETURN_NOT_OK(StartUserSignalHandlerThread());
  return Status::OK();
}

void MinidumpExceptionHandler::UnregisterMinidumpExceptionHandler() {
  if (!FLAGS_enable_minidumps) return;

  StopUserSignalHandlerThread();
  CHECK_EQ(1, MinidumpExceptionHandler::current_num_instances_.fetch_sub(1));
}

Status MinidumpExceptionHandler::StartUserSignalHandlerThread() {
  user_signal_handler_thread_running_.store(true, std::memory_order_relaxed);
  return Thread::Create("minidump", "sigusr1-handler",
                        &MinidumpExceptionHandler::RunUserSignalHandlerThread,
                        this, &user_signal_handler_thread_);
}

void MinidumpExceptionHandler::StopUserSignalHandlerThread() {
  user_signal_handler_thread_running_.store(false, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_release); // Store before signal.
  // Send SIGUSR1 signal to thread, which will wake it up.
  kill(getpid(), SIGUSR1);
  user_signal_handler_thread_->Join();
}

void MinidumpExceptionHandler::RunUserSignalHandlerThread() {
  sigset_t signals = GetSigset(SIGUSR1);
  while (true) {
    int signal;
    int err = sigwait(&signals, &signal);
    CHECK(err == 0) << "sigwait(): " << ErrnoToString(err) << ": " << err;
    CHECK_EQ(SIGUSR1, signal);
    if (!user_signal_handler_thread_running_.load(std::memory_order_relaxed)) {
      // Exit thread if we are shutting down.
      return;
    }
    if (!WriteMinidump()) {
      LOG(WARNING) << "Received USR1 signal but failed to write minidump";
    }
  }
}

#else // defined(__linux__)

// At the time of writing, we don't support breakpad on Mac so we just stub out
// all the methods defined in the header file.

Status MinidumpExceptionHandler::InitMinidumpExceptionHandler() {
  return Status::OK();
}

// No-op on non-Linux platforms.
Status MinidumpExceptionHandler::RegisterMinidumpExceptionHandler() {
  return Status::OK();
}

void MinidumpExceptionHandler::UnregisterMinidumpExceptionHandler() {
}

bool MinidumpExceptionHandler::WriteMinidump() {
  return true;
}

Status MinidumpExceptionHandler::StartUserSignalHandlerThread() {
  return Status::OK();
}

void MinidumpExceptionHandler::StopUserSignalHandlerThread() {
}

void MinidumpExceptionHandler::RunUserSignalHandlerThread() {
}

#endif // defined(__linux__)

std::atomic<int> MinidumpExceptionHandler::current_num_instances_;

MinidumpExceptionHandler::MinidumpExceptionHandler() {
  CHECK_OK(RegisterMinidumpExceptionHandler());
}

MinidumpExceptionHandler::~MinidumpExceptionHandler() {
  UnregisterMinidumpExceptionHandler();
}

Status MinidumpExceptionHandler::DeleteExcessMinidumpFiles(Env* env) {
  // Do not delete minidump files if minidumps are disabled.
  if (!FLAGS_enable_minidumps) return Status::OK();

  int32_t max_minidumps = FLAGS_max_minidumps;
  // Disable rotation if set to 0 or less.
  if (max_minidumps <= 0) return Status::OK();

  // Minidump filenames are created by breakpad in the following format, for example:
  // 7b57915b-ee6a-dbc5-21e59491-5c60a2cf.dmp.
  string pattern = JoinPathSegments(minidump_dir(), "*.dmp");

  // Use mtime to determine which minidumps to delete. While this could
  // potentially be ambiguous if many minidumps were created in quick
  // succession, users can always increase 'FLAGS_max_minidumps' if desired
  // in order to work around the problem.
  return env_util::DeleteExcessFilesByPattern(env, pattern, max_minidumps);
}

string MinidumpExceptionHandler::minidump_dir() const {
  return minidump_dir_;
}

Status BlockSigUSR1() {
  sigset_t signals = GetSigset(SIGUSR1);
  int ret = pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  if (ret == 0) return Status::OK();
  return Status::InvalidArgument("pthread_sigmask", ErrnoToString(ret), ret);
}

} // namespace kudu
