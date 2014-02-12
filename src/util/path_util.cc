// Copyright (c) 2014, Cloudera, inc.

#include "util/path_util.h"

#include <glog/logging.h>
#include <string>

using std::string;

namespace kudu {

std::string JoinPathSegments(const std::string &a,
                             const std::string &b) {
  CHECK(!a.empty()) << "empty first component: " << a;
  CHECK(!b.empty() && b[0] != '/')
    << "second path component must be non-empty and relative: "
    << b;
  if (a[a.size() - 1] == '/') {
    return a + b;
  } else {
    return a + "/" + b;
  }
}

std::string DirName(const std::string& path) {
  size_t last_slash = path.rfind('/');
  if (last_slash == string::npos) {
    return ".";
  } else {
    return path.substr(0, last_slash);
  }
}

} // namespace kudu
