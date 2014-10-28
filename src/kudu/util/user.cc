// Copyright (c) 2013, Cloudera, inc,
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/user.h"

#include <sys/types.h>
#include <errno.h>
#include <pwd.h>
#include <unistd.h>

#include <string>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {

Status GetLoggedInUser(string* user_name) {
  DCHECK(user_name != NULL);

  struct passwd pwd;
  struct passwd *result;

  size_t bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (bufsize == -1) {  // Value was indeterminate.
    bufsize = 16384;    // Should be more than enough, per the man page.
  }

  gscoped_ptr<char[], FreeDeleter> buf(static_cast<char *>(malloc(bufsize)));
  if (buf.get() == NULL) {
    return Status::RuntimeError("Malloc failed", ErrnoToString(errno), errno);
  }

  int ret = getpwuid_r(getuid(), &pwd, buf.get(), bufsize, &result);
  if (result == NULL) {
    if (ret == 0) {
      return Status::NotFound("Current logged-in user not found! This is an unexpected error.");
    } else {
      // Errno in ret
      return Status::RuntimeError("Error calling getpwuid_r()", ErrnoToString(ret), ret);
    }
  }

  *user_name = pwd.pw_name;

  return Status::OK();
}

} // namespace kudu
