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
#ifndef KUDU_UTIL_DEBUG_UTIL_H
#define KUDU_UTIL_DEBUG_UTIL_H

#include <sys/types.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/util/status.h"

#define FUTEX_WAIT 0
#define FUTEX_WAKE 1
#define FUTEX_PRIVATE_FLAG 128

namespace kudu {

template <typename T> class ArrayView;
class MonoTime;
class StackTrace;
class StackTraceCollector;

namespace stack_trace_internal {
struct SignalData;
}

// Return true if coverage is enabled.
bool IsCoverageBuild();

// Try to flush coverage info. If another thread is already flushing
// coverage, this returns without doing anything, since flushing coverage
// is not thread-safe or re-entrant.
void TryFlushCoverage();

// Return a list of all of the thread IDs currently running in this process.
// Not async-safe.
Status ListThreads(std::vector<pid_t>* tids);

// Set which POSIX signal number should be used internally for triggering
// stack traces. If the specified signal handler is already in use, this
// returns an error, and stack traces will be disabled.
Status SetStackTraceSignal(int signum);

// Return the stack trace of the given thread, stringified and symbolized.
//
// Note that the symbolization happens on the calling thread, not the target
// thread, so this is relatively low-impact on the target.
//
// This is safe to use against the current thread, the main thread, or any other
// thread. It requires that the target thread has not blocked POSIX signals. If
// it has, an error message will be returned.
//
// This function is thread-safe.
//
// NOTE: if Kudu is running inside a debugger, this can be annoying to a developer since
// it internally uses signals that will cause the debugger to stop. Consider checking
// 'IsBeingDebugged()' from os-util.h before using this function for non-critical use
// cases.
std::string DumpThreadStack(int64_t tid);

// Capture the thread stack of another thread
//
// NOTE: if Kudu is running inside a debugger, this can be annoying to a developer since
// it internally uses signals that will cause the debugger to stop. Consider checking
// 'IsBeingDebugged()' from os-util.h before using this function for non-critical use
// cases.
Status GetThreadStack(int64_t tid, StackTrace* stack);

// Return the current stack trace, stringified.
std::string GetStackTrace();

// Return the current stack trace, in hex form. This is significantly
// faster than GetStackTrace() above, so should be used in performance-critical
// places like TRACE() calls. If you really need blazing-fast speed, though,
// use HexStackTraceToString() into a stack-allocated buffer instead --
// this call causes a heap allocation for the std::string.
//
// Note that this is much more useful in the context of a static binary,
// since addr2line wouldn't know where shared libraries were mapped at
// runtime.
std::string GetStackTraceHex();

// This is the same as GetStackTraceHex(), except multi-line in a format that
// looks very similar to GetStackTrace() but without symbols. Because it's in
// that format, the tool stacktrace_addr2line.pl in the kudu build-support
// directory can symbolize it automatically (to the extent that addr2line(1)
// is able to find the symbols).
std::string GetLogFormatStackTraceHex();

// Collect the current stack trace in hex form into the given buffer.
//
// The resulting trace just includes the hex addresses, space-separated. This is suitable
// for later stringification by pasting into 'addr2line' for example.
//
// This function is async-safe.
void HexStackTraceToString(char* buf, size_t size);

// Efficient class for collecting and later stringifying a stack trace.
//
// Requires external synchronization.
class StackTrace {
 public:

  // Constructs a new (uncollected) stack trace.
  StackTrace()
    : num_frames_(0) {
  }

  // Resets the stack trace to an uncollected state.
  void Reset() {
    num_frames_ = 0;
  }

  // Returns true if Collect() (but not Reset()) has been called on this stack trace.
  bool HasCollected() const {
    return num_frames_ > 0;
  }

  // Copies the contents of 's' into this stack trace.
  void CopyFrom(const StackTrace& s) {
    memcpy(this, &s, sizeof(s));
  }

  // Returns true if the stack trace 's' matches this trace.
  bool Equals(const StackTrace& s) const {
    return s.num_frames_ == num_frames_ &&
      strings::memeq(frames_, s.frames_,
                     num_frames_ * sizeof(frames_[0]));
  }

  // Comparison operator for use in sorting.
  bool LessThan(const StackTrace& s) const;

  // Collect and store the current stack trace. Skips the top 'skip_frames' frames
  // from the stack. For example, a value of '1' will skip whichever function
  // called the 'Collect()' function. The 'Collect' function itself is always skipped.
  //
  // This function is async-safe.
  void Collect(int skip_frames = 0);

  int num_frames() const {
    return num_frames_;
  }

  void* frame(int i) const {
    DCHECK_LE(i, num_frames_);
    return frames_[i];
  }

  enum Flags {
    // Do not fix up the addresses on the stack to try to point to the 'call'
    // instructions instead of the return address. This is necessary when dumping
    // addresses to be interpreted by 'pprof', which does this fix-up itself.
    NO_FIX_CALLER_ADDRESSES = 1,

    // Prefix each hex address with '0x'. This is required by the go version
    // of pprof when parsing stack traces.
    HEX_0X_PREFIX = 1 << 1,
  };

  // Stringify the trace into the given buffer.
  // The resulting output is hex addresses suitable for passing into 'addr2line'
  // later.
  //
  // Async-safe.
  void StringifyToHex(char* buf, size_t size, int flags = 0) const;

  // Same as above, but returning a std::string.
  // This is not async-safe.
  std::string ToHexString(int flags = 0) const;

  // Return a string with a symbolized backtrace in a format suitable for
  // printing to a log file.
  // This is not async-safe.
  std::string Symbolize() const;

  // Return a string with a hex-only backtrace in the format typically used in
  // log files. Similar to the format given by Symbolize(), but symbols are not
  // resolved (only the hex addresses are given).
  std::string ToLogFormatHexString() const;

  uint64_t HashCode() const;

 private:
  enum {
    // The maximum number of stack frames to collect.
    kMaxFrames = 16,

    // The max number of characters any frame requires in string form.
    kHexEntryLength = 16
  };

  int num_frames_;
  void* frames_[kMaxFrames];
};

// Utility class for gathering a process-wide snapshot of the stack traces
// of all threads.
class StackTraceSnapshot {
 public:
  // The information about each thread will be gathered in a struct.
  struct ThreadInfo {
    // The TID of the thread.
    int64_t tid;

    // The status of collection. If a thread exits during collection or
    // was blocking signals, it's possible to have an error here.
    Status status;

    // The name of the thread.
    // May be missing if 'status' is not OK or if thread name collection was
    // disabled.
    std::string thread_name;

    // The current stack trace of the thread.
    // Always missing if 'status' is not OK.
    StackTrace stack;
  };
  using VisitorFunc = std::function<void(ArrayView<ThreadInfo> group)>;

  void set_capture_thread_names(bool c) {
    capture_thread_names_ = c;
  }

  // Snapshot the stack traces of all threads in the process. This may return a bad
  // Status in the case that stack traces aren't supported on the platform, or if
  // the process is running inside a debugger.
  //
  // NOTE: this may take some time and should not be called in a latency-sensitive
  // context.
  Status SnapshotAllStacks();

  // After having collected stacks, visit them, grouped by shared
  // stack trace. The visitor function will be called once per group.
  // Each group is guaranteed to be non-empty.
  //
  // Any threads which failed to collect traces are returned as a single group
  // having empty stack traces.
  //
  // REQUIRES: a previous successful call to SnapshotAllStacks().
  void VisitGroups(const VisitorFunc& visitor);

  // Return the number of threads which were interrogated for a stack trace.
  //
  // NOTE: this includes threads which failed to collect.
  int num_threads() const { return infos_.size(); }

  // Return the number of threads which failed to collect a stack trace.
  int num_failed() const { return num_failed_; }

 private:
  std::vector<StackTraceSnapshot::ThreadInfo> infos_;
  std::vector<StackTraceCollector> collectors_;
  int num_failed_ = 0;

  bool capture_thread_names_ = true;
};


// Class to collect the stack trace of another thread within this process.
// This allows for more advanced use cases than 'DumpThreadStack(tid)' above.
// Namely, this provides an asynchronous trigger/collect API so that many
// stack traces can be collected from many different threads in parallel using
// different instances of this object.
class StackTraceCollector {
 public:
  StackTraceCollector() = default;
  StackTraceCollector(StackTraceCollector&& other) noexcept;
  ~StackTraceCollector();

  // Send the asynchronous request to the the thread with TID 'tid'
  // to collect its stack trace into '*stack'.
  //
  // NOTE: 'stack' must remain a valid pointer until AwaitCollection() has
  // completed.
  //
  // Returns OK if the signal was sent successfully.
  Status TriggerAsync(int64_t tid, StackTrace* stack);

  // Wait for the stack trace to be collected from the target thread.
  //
  // REQUIRES: TriggerAsync() has returned successfully.
  Status AwaitCollection(MonoTime deadline);

 private:
  DISALLOW_COPY_AND_ASSIGN(StackTraceCollector);

  // Safely sets 'sig_data_' back to nullptr after having sent an asynchronous
  // stack trace request. See implementation for details.
  //
  // Returns true if the stack trace was collected before revocation
  // and false if it was not.
  //
  // POSTCONDITION: sig_data_ == nullptr
  bool RevokeSigData();

  int64_t tid_ = 0;
  stack_trace_internal::SignalData* sig_data_ = nullptr;
};

} // namespace kudu

#endif
