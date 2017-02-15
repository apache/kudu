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

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/thread.h"

namespace google_breakpad {
class ExceptionHandler;
} // namespace google_breakpad

namespace kudu {

class Env;
class Status;

// While an instance of this class is in scope, a Breakpad minidump handler
// will generate a minidump for the current program if it crashes or if it
// received a USR1 signal. This class must be instantiated after initializing
// the gflags library. When used in conjuction with glog, or other signal
// handling facilities, this class must be invoked after installing those
// signal handlers.
//
// The BlockSigUSR1() function should be called before spawning any threads in
// order to block the USR1 signal from crashing the process. This class relies
// on that signal being blocked by all threads in order to safely generate
// minidumps in response to the USR1 signal.
//
// Only one instance of this class may be instantiated at a time.
//
// For more information on Google Breakpad, see its documentation at:
// http://chromium.googlesource.com/breakpad/breakpad/+/master/docs/getting_started_with_breakpad.md
class MinidumpExceptionHandler {
 public:
  MinidumpExceptionHandler();
  ~MinidumpExceptionHandler();

  // Write a minidump immediately. Can be used to generate a minidump
  // independently of a crash. Should not be called from a signal handler or a
  // crash context because it uses the heap.
  bool WriteMinidump();

  // Deletes excess minidump files beyond the configured max of
  // 'FLAGS_max_minidumps'. Uses the file's modified time to determine recency.
  // Does nothing if 'FLAGS_enabled_minidumps' is false.
  Status DeleteExcessMinidumpFiles(Env* env);

  // Get the path to the directory that will be used for writing minidumps.
  std::string minidump_dir() const;

 private:
  Status InitMinidumpExceptionHandler();
  Status RegisterMinidumpExceptionHandler();
  void UnregisterMinidumpExceptionHandler();

  Status StartUserSignalHandlerThread();
  void StopUserSignalHandlerThread();
  void RunUserSignalHandlerThread();

  // The number of instnaces of this class that are currently in existence.
  // We keep this counter in order to force a crash if more than one is running
  // at a time, as a sanity check.
  static std::atomic<int> current_num_instances_;

  #ifndef __APPLE__
  std::atomic<bool> user_signal_handler_thread_running_;// Unused in macOS build.
  #endif

  scoped_refptr<Thread> user_signal_handler_thread_;

  // Breakpad ExceptionHandler. It registers its own signal handlers to write
  // minidump files during process crashes, but can also be used to write
  // minidumps directly.
  std::unique_ptr<google_breakpad::ExceptionHandler> breakpad_handler_;

  // Directory in which we store our minidumps.
  std::string minidump_dir_;
};

// Block SIGUSR1 from threads handling it.
// This should be called by the process before spawning any threads so that a
// USR1 signal will cause a minidump to be generated instead of a crash.
Status BlockSigUSR1();

} // namespace kudu
