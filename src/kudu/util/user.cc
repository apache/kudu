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

#include "kudu/util/user.h"

#include <pwd.h>
#include <string.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace {

Status DoGetLoggedInUser(string* user_name) {
  const char* override_username = getenv("KUDU_USER_NAME");
  if (override_username && strlen(override_username)) {
    VLOG(1) << "Overriding logged-in user name to " << override_username;
    *user_name = override_username;
    return Status::OK();
  }

  DCHECK(user_name != nullptr);

  struct passwd pwd;
  struct passwd *result;

  // Get the system-defined limit for usernames. If the value was indeterminate,
  // use a constant that should be more than enough, per the man page.
  int64_t retval = sysconf(_SC_GETPW_R_SIZE_MAX);
  size_t bufsize = retval > 0 ? retval : 16384;

  gscoped_ptr<char[], FreeDeleter> buf(static_cast<char *>(malloc(bufsize)));
  if (buf.get() == nullptr) {
    return Status::RuntimeError("malloc failed", ErrnoToString(errno), errno);
  }

  int ret = getpwuid_r(getuid(), &pwd, buf.get(), bufsize, &result);
  if (result == nullptr) {
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

} // anonymous namespace

Status GetLoggedInUser(string* user_name) {
  static std::once_flag once;
  static string* once_user_name;
  static Status* once_status;
  std::call_once(once, [](){
      string u;
      Status s = DoGetLoggedInUser(&u);
      debug::ScopedLeakCheckDisabler ignore_leaks;
      once_status = new Status(std::move(s));
      once_user_name = new string(std::move(u));
    });

  RETURN_NOT_OK(*once_status);
  *user_name = *once_user_name;
  return Status::OK();
}

} // namespace kudu
