// Copyright (c) 2013, Cloudera, inc,
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_USER_H
#define KUDU_UTIL_USER_H

#include <string>

#include "kudu/util/status.h"

namespace kudu {

// Get current logged-in user with getpwuid_r().
// user name is written to user_name.
Status GetLoggedInUser(std::string* user_name);

} // namespace kudu

#endif // KUDU_UTIL_USER_H
