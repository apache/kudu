// Copyright (c) 2012, Cloudera, inc.

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
#include <stdio.h>
#include <string.h>
#define _GNU_SOURCE
#undef _XOPEN_SOURCE

namespace kudu {

void ErrnoToCString(int err, char *buf, size_t buf_len) {
  if (strerror_r(err, buf, buf_len)) {
    snprintf(buf, buf_len, "unknown error");
  }
}

}
