// Copyright (c) 2013, Cloudera, inc,
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_STRERROR_R
#define KUDU_STRERROR_R

#include <string>

namespace kudu {

void ErrnoToCString(int err, char *buf, size_t buf_len);

// Return a string representing an errno.
inline static std::string ErrnoToString(int err) {
  char buf[512];
  ErrnoToCString(err, buf, sizeof(buf));
  return std::string(buf);
}

} // namespace kudu

#endif
