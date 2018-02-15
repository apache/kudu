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
#ifndef KUDU_UTIL_ROLLING_LOG_H
#define KUDU_UTIL_ROLLING_LOG_H

#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class WritableFile;

// A simple rolling log.
//
// This creates a log which spans multiple files in a specified directory.
// After a log file reaches a specified size threshold, it automatically rolls
// to the next file in the sequence.
//
// The files are named similarly to glog log files and use the following pattern:
//
// <log_dir>/<program-name>.<hostname>.<user-name>.<log-name>.<timestamp>.<sequence>.<pid>
//   log_dir:      the log_dir specified in the constructor
//   program-name: argv[0], as determined by google::ProgramInvocationShortName()
//   hostname:     the local machine hostname
//   user-name:    the current user name
//   log-name:     the log_name specified in the constructor
//   timestamp:    the wall clock time when the log file was created, in
//                 YYYYmmdd-HHMMSS fixed-length format.
//   sequence:     a sequence number which is used to disambiguate when the log file is
//                 rolled multiple times within a second
//   pid:          the pid of the daemon
//
// The log implementation does not ensure durability of the log or its files in any way.
// This class is not thread-safe and must be externally synchronized.
class RollingLog {
 public:
  RollingLog(Env* env, std::string log_dir, std::string log_name);

  ~RollingLog();

  // Open the log.
  // It is optional to call this function. Append() will automatically open
  // the log as necessary if it is not open.
  Status Open();

  // Set the pre-compression size threshold at which the log file will be rolled.
  // If the log is already open, this applies for the the current and any future
  // log file.
  //
  // NOTE: This is the limit on a single segment of the log, not a limit on the total
  // size of the log.
  //
  // NOTE: The threshold is checked _after_ each call to Append(). So, the size of
  // the log may overshoot this threshold by as much as the size of a single appended
  // message.
  void SetRollThresholdBytes(int64_t size);

  // Set the total number of log segments to be retained. When the log is rolled,
  // old segments are removed to achieve the targeted number of segments.
  void SetMaxNumSegments(int num_segments);

  // If compression is enabled, log files are compressed.
  // NOTE: this requires that the passed-in Env instance is the local file system.
  void SetCompressionEnabled(bool compress);

  // Append the given data to the current log file.
  //
  // If, after appending this data, the file size has crossed the configured roll
  // threshold, a new empty log file is created. Note that this is a synchronous API and
  // causes potentially-blocking IO on the current thread. However, this does not fsync()
  // or otherwise ensure durability of the appended data.
  Status Append(StringPiece data) WARN_UNUSED_RESULT;

  // Close the log.
  Status Close();

  // Return the number of times this log has rolled since it was first opened.
  int roll_count() const {
    return roll_count_;
  }

 private:
  std::string GetLogFileName(int sequence) const;

  // Get a glob pattern matching all log files written by this instance.
  std::string GetLogFilePattern() const;

  // Compress the given path, writing a new file '<path>.gz'.
  Status CompressFile(const std::string& path) const;

  Env* const env_;
  const std::string log_dir_;
  const std::string log_name_;

  int64_t roll_threshold_bytes_;
  int max_num_segments_;

  std::unique_ptr<WritableFile> file_;
  bool compress_after_close_;

  int roll_count_ = 0;

  DISALLOW_COPY_AND_ASSIGN(RollingLog);
};

} // namespace kudu
#endif /* KUDU_UTIL_ROLLING_LOG_H */
