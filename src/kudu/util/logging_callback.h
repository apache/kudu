// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_LOGGING_CALLBACK_H
#define KUDU_UTIL_LOGGING_CALLBACK_H

#include <ctime>
#include <string>

#include "kudu/gutil/callback_forward.h"

namespace kudu {

enum LogSeverity {
  SEVERITY_INFO,
  SEVERITY_WARNING,
  SEVERITY_ERROR,
  SEVERITY_FATAL
};

// Callback for simple logging.
//
// 'message' is NOT terminated with an endline.
typedef Callback<void(LogSeverity severity,
                      const char* filename,
                      int line_number,
                      const struct ::tm* time,
                      const char* message,
                      size_t message_len)> LoggingCallback;

} // namespace kudu

#endif
