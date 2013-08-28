// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef KUDU_UTIL_LOGGING_H
#define KUDU_UTIL_LOGGING_H

#include <string>
#include <glog/logging.h>

namespace kudu {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
void InitGoogleLoggingSafe(const char* arg);

// Returns the full pathname of the symlink to the most recent log
// file corresponding to this severity
void GetFullLogFilename(google::LogSeverity severity, std::string* filename);

// Shuts down the google logging library. Call before exit to ensure that log files are
// flushed. May only be called once.
void ShutdownLogging();

// Writes all command-line flags to the log at level INFO.
void LogCommandLineFlags();
} // namespace kudu

#endif // KUDU_UTIL_LOGGING_H
