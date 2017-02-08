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
#include "kudu/util/logging.h"

#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <glog/logging.h>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/spinlock.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/async_logger.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/minidump.h"
#include "kudu/util/status.h"

DEFINE_string(log_filename, "",
    "Prefix of log filename - "
    "full path is <log_dir>/<log_filename>.[INFO|WARN|ERROR|FATAL]");
TAG_FLAG(log_filename, stable);

DEFINE_bool(log_async, true,
            "Enable asynchronous writing to log files. This improves "
            "latency and stability.");
TAG_FLAG(log_async, hidden);

DEFINE_int32(log_async_buffer_bytes_per_level, 2 * 1024 * 1024,
             "The number of bytes of buffer space used by each log "
             "level. Only relevant when --log_async is enabled.");
TAG_FLAG(log_async_buffer_bytes_per_level, hidden);

DEFINE_int32(max_log_files, 10,
    "Maximum number of log files to retain per severity level. The most recent "
    "log files are retained. If set to 0, all log files are retained.");
TAG_FLAG(max_log_files, runtime);
TAG_FLAG(max_log_files, experimental);

DEFINE_bool(log_redact_user_data, true,
    "Whether log and error messages will have row data redacted.");
TAG_FLAG(log_redact_user_data, runtime);
TAG_FLAG(log_redact_user_data, experimental);

#define PROJ_NAME "kudu"

bool logging_initialized = false;

using namespace std; // NOLINT(*)
using namespace boost::uuids; // NOLINT(*)

using base::SpinLock;
using base::SpinLockHolder;

namespace kudu {

__thread bool tls_redact_user_data = true;
const char* const kRedactionMessage = "<redacted>";

namespace {

class SimpleSink : public google::LogSink {
 public:
  explicit SimpleSink(LoggingCallback cb) : cb_(std::move(cb)) {}

  virtual ~SimpleSink() OVERRIDE {
  }

  virtual void send(google::LogSeverity severity, const char* full_filename,
                    const char* base_filename, int line,
                    const struct ::tm* tm_time,
                    const char* message, size_t message_len) OVERRIDE {
    LogSeverity kudu_severity;
    switch (severity) {
      case google::INFO:
        kudu_severity = SEVERITY_INFO;
        break;
      case google::WARNING:
        kudu_severity = SEVERITY_WARNING;
        break;
      case google::ERROR:
        kudu_severity = SEVERITY_ERROR;
        break;
      case google::FATAL:
        kudu_severity = SEVERITY_FATAL;
        break;
      default:
        LOG(FATAL) << "Unknown glog severity: " << severity;
    }
    cb_.Run(kudu_severity, full_filename, line, tm_time, message, message_len);
  }

 private:

  LoggingCallback cb_;
};

SpinLock logging_mutex(base::LINKER_INITIALIZED);

// There can only be a single instance of a SimpleSink.
//
// Protected by 'logging_mutex'.
SimpleSink* registered_sink = nullptr;

// Records the logging severity after the first call to
// InitGoogleLoggingSafe{Basic}. Calls to UnregisterLoggingCallback()
// will restore stderr logging back to this severity level.
//
// Protected by 'logging_mutex'.
int initial_stderr_severity;

void EnableAsyncLogging() {
  debug::ScopedLeakCheckDisabler leaky;

  // Enable Async for every level except for FATAL. Fatal should be synchronous
  // to ensure that we get the fatal log message written before exiting.
  for (auto level : { google::INFO, google::WARNING, google::ERROR }) {
    auto* orig = google::base::GetLogger(level);
    auto* async = new AsyncLogger(orig, FLAGS_log_async_buffer_bytes_per_level);
    async->Start();
    google::base::SetLogger(level, async);
  }
}

void UnregisterLoggingCallbackUnlocked() {
  CHECK(logging_mutex.IsHeld());
  CHECK(registered_sink);

  // Restore logging to stderr, then remove our sink. This ordering ensures
  // that no log messages are missed.
  google::SetStderrLogging(initial_stderr_severity);
  google::RemoveLogSink(registered_sink);
  delete registered_sink;
  registered_sink = nullptr;
}

void FlushCoverageOnExit() {
  // Coverage flushing is not re-entrant, but this might be called from a
  // crash signal context, so avoid re-entrancy.
  static __thread bool in_call = false;
  if (in_call) return;
  in_call = true;

  // The failure writer will be called multiple times per exit.
  // We only need to flush coverage once. We use a 'once' here so that,
  // if another thread is already flushing, we'll block and wait for them
  // to finish before allowing this thread to call abort().
  static std::once_flag once;
  std::call_once(once, [] {
      static const char msg[] = "Flushing coverage data before crash...\n";
      write(STDERR_FILENO, msg, arraysize(msg));
      TryFlushCoverage();
    });
  in_call = false;
}

// On SEGVs, etc, glog will call this function to write the error to stderr. This
// implementation is copied from glog with the exception that we also flush coverage
// the first time it's called.
//
// NOTE: this is only used in coverage builds!
void FailureWriterWithCoverage(const char* data, int size) {
  FlushCoverageOnExit();

  // Original implementation from glog:
  if (write(STDERR_FILENO, data, size) < 0) {
    // Ignore errors.
  }
}

// GLog "failure function". This is called in the case of LOG(FATAL) to
// ensure that we flush coverage even on crashes.
//
// NOTE: this is only used in coverage builds!
void FlushCoverageAndAbort() {
  FlushCoverageOnExit();
  abort();
}
} // anonymous namespace

void InitGoogleLoggingSafe(const char* arg) {
  SpinLockHolder l(&logging_mutex);
  if (logging_initialized) return;

  google::InstallFailureSignalHandler();

  if (!FLAGS_log_filename.empty()) {
    for (int severity = google::INFO; severity <= google::FATAL; ++severity) {
      google::SetLogSymlink(severity, FLAGS_log_filename.c_str());
    }
  }

  // This forces our logging to use /tmp rather than looking for a
  // temporary directory if none is specified. This is done so that we
  // can reliably construct the log file name without duplicating the
  // complex logic that glog uses to guess at a temporary dir.
  if (FLAGS_log_dir.empty()) {
    FLAGS_log_dir = "/tmp";
  }

  if (!FLAGS_logtostderr) {
    // Verify that a log file can be created in log_dir by creating a tmp file.
    ostringstream ss;
    random_generator uuid_generator;
    ss << FLAGS_log_dir << "/" << PROJ_NAME "_test_log." << uuid_generator();
    const string file_name = ss.str();
    ofstream test_file(file_name.c_str());
    if (!test_file.is_open()) {
      ostringstream error_msg;
      error_msg << "Could not open file in log_dir " << FLAGS_log_dir;
      perror(error_msg.str().c_str());
      // Unlock the mutex before exiting the program to avoid mutex d'tor assert.
      logging_mutex.Unlock();
      exit(1);
    }
    remove(file_name.c_str());
  }

  google::InitGoogleLogging(arg);

  // In coverage builds, we should flush coverage before exiting on crash.
  // This way, fault injection tests still capture coverage of the daemon
  // that "crashed".
  if (IsCoverageBuild()) {
    // We have to use both the "failure writer" and the "FailureFunction".
    // This allows us to handle both LOG(FATAL) and unintended crashes like
    // SEGVs.
    google::InstallFailureWriter(FailureWriterWithCoverage);
    google::InstallFailureFunction(FlushCoverageAndAbort);
  }

  // Needs to be done after InitGoogleLogging
  if (FLAGS_log_filename.empty()) {
    CHECK_STRNE(google::ProgramInvocationShortName(), "UNKNOWN")
        << ": must initialize gflags before glog";
    FLAGS_log_filename = google::ProgramInvocationShortName();
  }

  // File logging: on.
  // Stderr logging threshold: FLAGS_stderrthreshold.
  // Sink logging: off.
  initial_stderr_severity = FLAGS_stderrthreshold;

  // For minidump support. Must be called before logging threads started.
  CHECK_OK(BlockSigUSR1());

  if (FLAGS_log_async) {
    EnableAsyncLogging();
  }

  logging_initialized = true;
}

void InitGoogleLoggingSafeBasic(const char* arg) {
  SpinLockHolder l(&logging_mutex);
  if (logging_initialized) return;

  google::InitGoogleLogging(arg);

  // This also disables file-based logging.
  google::LogToStderr();

  // File logging: off.
  // Stderr logging threshold: INFO.
  // Sink logging: off.
  initial_stderr_severity = google::INFO;
  logging_initialized = true;
}

void RegisterLoggingCallback(const LoggingCallback& cb) {
  SpinLockHolder l(&logging_mutex);
  CHECK(logging_initialized);

  if (registered_sink) {
    LOG(WARNING) << "Cannot register logging callback: one already registered";
    return;
  }

  // AddLogSink() claims to take ownership of the sink, but it doesn't
  // really; it actually expects it to remain valid until
  // google::ShutdownGoogleLogging() is called.
  registered_sink = new SimpleSink(cb);
  google::AddLogSink(registered_sink);

  // Even when stderr logging is ostensibly off, it's still emitting
  // ERROR-level stuff. This is the default.
  google::SetStderrLogging(google::ERROR);

  // File logging: yes, if InitGoogleLoggingSafe() was called earlier.
  // Stderr logging threshold: ERROR.
  // Sink logging: on.
}

void UnregisterLoggingCallback() {
  SpinLockHolder l(&logging_mutex);
  CHECK(logging_initialized);

  if (!registered_sink) {
    LOG(WARNING) << "Cannot unregister logging callback: none registered";
    return;
  }

  UnregisterLoggingCallbackUnlocked();
  // File logging: yes, if InitGoogleLoggingSafe() was called earlier.
  // Stderr logging threshold: initial_stderr_severity.
  // Sink logging: off.
}

void GetFullLogFilename(google::LogSeverity severity, string* filename) {
  ostringstream ss;
  ss << FLAGS_log_dir << "/" << FLAGS_log_filename << "."
     << google::GetLogSeverityName(severity);
  *filename = ss.str();
}

void ShutdownLoggingSafe() {
  SpinLockHolder l(&logging_mutex);
  if (!logging_initialized) return;

  if (registered_sink) {
    UnregisterLoggingCallbackUnlocked();
  }

  google::ShutdownGoogleLogging();

  logging_initialized = false;
}

void LogCommandLineFlags() {
  LOG(INFO) << "Flags (see also /varz are on debug webserver):" << endl
            << google::CommandlineFlagsIntoString();
}

Status DeleteExcessLogFiles(Env* env) {
  int32_t max_log_files = FLAGS_max_log_files;
  // Ignore bad input or disable log rotation.
  if (max_log_files <= 0) return Status::OK();

  for (int severity = 0; severity < google::NUM_SEVERITIES; ++severity) {
    // Build glob pattern for input
    // e.g. /var/log/kudu/kudu-master.*.INFO.*
    string pattern = strings::Substitute("$0/$1.*.$2.*", FLAGS_log_dir, FLAGS_log_filename,
                                         google::GetLogSeverityName(severity));

    // Keep the 'max_log_files' most recent log files, as compared by
    // modification time. Glog files contain a second-granularity timestamp in
    // the name, so this could potentially use the filename sort order as
    // guaranteed by glob, however this code has been adapted from Impala which
    // uses mtime to determine which files to delete, and there haven't been any
    // issues in production settings.
    RETURN_NOT_OK(env_util::DeleteExcessFilesByPattern(env, pattern, max_log_files));
  }
  return Status::OK();
}

// Support for the special THROTTLE_MSG token in a log message stream.
ostream& operator<<(ostream &os, const PRIVATE_ThrottleMsg& /*unused*/) {
  using google::LogMessage;
#ifdef DISABLE_RTTI
  LogMessage::LogStream *log = static_cast<LogMessage::LogStream*>(&os);
#else
  LogMessage::LogStream *log = dynamic_cast<LogMessage::LogStream*>(&os);
#endif
  CHECK(log && log == log->self())
      << "You must not use COUNTER with non-glog ostream";
  int ctr = log->ctr();
  if (ctr > 0) {
    os << " [suppressed " << ctr << " similar messages]";
  }
  return os;
}

} // namespace kudu
