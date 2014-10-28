// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef BASE_LOGGING_H
#define BASE_LOGGING_H

#ifdef KUDU_HEADERS_USE_GLOG
#include <glog/logging.h>
#else

#include <stdlib.h> // for exit()

// Stubbed versions of macros defined in glog/logging.h, intended for
// environments where glog headers aren't available.
//
// Add more as needed.

#ifndef DCHECK
#define DCHECK(condition) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_EQ
#define DCHECK_EQ(val1, val2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_NE
#define DCHECK_NE(val1, val2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_LE
#define DCHECK_LE(val1, val2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_LT
#define DCHECK_LT(val1, val2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_GE
#define DCHECK_GE(val1, val2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_GT
#define DCHECK_GT(val1, val2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_NOTNULL
#define DCHECK_NOTNULL(val) (val)
#endif

#ifndef DCHECK_STREQ
#define DCHECK_STREQ(str1, str2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_STRCASEEQ
#define DCHECK_STRCASEEQ(str1, str2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_STRNE
#define DCHECK_STRNE(str1, str2) while (false) kudu::internal_logging::NullLog()
#endif

#ifndef DCHECK_STRCASENE
#define DCHECK_STRCASENE(str1, str2) while (false) kudu::internal_logging::NullLog()
#endif

// Log levels. LOG ignores them, so their values are abitrary.

#ifndef INFO
#define INFO 0
#endif

#ifndef WARNING
#define WARNING 1
#endif

#ifndef FATAL
#define FATAL 2
#endif

#ifndef LOG
#define LOG(level) kudu::internal_logging::CerrLog(level)
#endif

#ifndef CHECK
#define CHECK(condition) \
  (condition) ? 0 : LOG(FATAL) << "Check failed: " #condition " "
#endif

namespace kudu {

namespace internal_logging {

class NullLog {
 public:
  template<class T>
  NullLog& operator<<(const T& t) {
    return *this;
  }
};

class CerrLog {
 public:
  CerrLog(int severity)
    : severity_(severity),
      has_logged_(false) {
  }

  ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == FATAL) {
      exit(1);
    }
  }

  template<class T>
  CerrLog& operator<<(const T& t) {
    has_logged_ = true;
    std::cerr << t;
    return *this;
  }

 private:
  const int severity_;
  bool has_logged_;
};

} // namespace internal_logging
} // namespace kudu

#endif

#endif
