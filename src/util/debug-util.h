// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_DEBUG_UTIL_H
#define KUDU_UTIL_DEBUG_UTIL_H

#include <string>

namespace kudu {

// Return the current stack trace, stringified.
std::string GetStackTrace();

}
#endif
