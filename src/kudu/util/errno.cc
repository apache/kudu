// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

/*
 * Get the POSIX strerror_r.
 *
 * By default, glibc uses its own, non-POSIX, definition of strerror_r.  We want
 * the POSIX version. 
 *
 * Note: This must come first!  If you include anything before this point, you
 * might get the non-standard strerror_r definition.
 */
#undef _GNU_SOURCE
#define _XOPEN_SOURCE 600
#include <string.h>
#define _GNU_SOURCE
#undef _XOPEN_SOURCE

namespace kudu {

void ErrnoToCString(int err, char *buf, size_t buf_len) {
  if (strerror_r(err, buf, buf_len)) {
    static const char UNKNOWN_ERROR[] = "unknown error";
    if (buf_len >= sizeof(UNKNOWN_ERROR)) {
      strcpy(buf, UNKNOWN_ERROR); // NOLINT(runtime/printf)
    } else {
      memset(buf, 0, buf_len);
    }
  }
}
} // namespace kudu
