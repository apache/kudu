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

#include "kudu/util/path_util.h"

// Use the POSIX version of dirname(3).
#include <libgen.h>

#include <cstring>
#if defined(__APPLE__)
#include <mutex>
#endif // defined(__APPLE__)
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/util/env.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"


using std::string;
using std::vector;
using strings::SkipEmpty;
using strings::Split;

namespace kudu {

const char kTmpInfix[] = ".kudutmp";
const char kOldTmpInfix[] = ".tmp";

string JoinPathSegments(const string& a, const string& b) {
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

vector<string> JoinPathSegmentsV(const vector<string>& v, const string& s) {
  vector<string> out;
  for (const string& path : v) {
    out.emplace_back(JoinPathSegments(path, s));
  }
  return out;
}

vector<string> SplitPath(const string& path) {
  if (path.empty()) return {};
  vector<string> segments;
  if (path[0] == '/') segments.emplace_back("/");
  vector<StringPiece> pieces = Split(path, "/", SkipEmpty());
  for (const StringPiece& piece : pieces) {
    segments.emplace_back(piece.data(), piece.size());
  }
  return segments;
}

string DirName(const string& path) {
  gscoped_ptr<char[], FreeDeleter> path_copy(strdup(path.c_str()));
#if defined(__APPLE__)
  static std::mutex lock;
  std::lock_guard<std::mutex> l(lock);
#endif // defined(__APPLE__)
  return ::dirname(path_copy.get());
}

string BaseName(const string& path) {
  gscoped_ptr<char[], FreeDeleter> path_copy(strdup(path.c_str()));
  return basename(path_copy.get());
}

Status FindExecutable(const string& binary,
                      const vector<string>& search,
                      string* path) {
  string p;

  // First, check specified locations. This is necessary to check first so that
  // the system binaries won't be found before the specified search locations.
  for (const auto& location : search) {
    p = JoinPathSegments(location, binary);
    if (Env::Default()->FileExists(p)) {
      *path = p;
      return Status::OK();
    }
  }

  // Next check if the binary is on the PATH.
  Status s = Subprocess::Call({ "which", binary }, "", &p);
  if (s.ok()) {
    StripTrailingNewline(&p);
    *path = p;
    return Status::OK();
  }

  return Status::NotFound("Unable to find binary", binary);
}

} // namespace kudu
