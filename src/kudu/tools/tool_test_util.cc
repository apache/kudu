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
//
// Utility functions for generating data for use by tools and tests.

#include "kudu/tools/tool_test_util.h"

#include <ostream>
#include <vector>

#include <glog/logging.h>

#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

using std::string;
using std::vector;

namespace kudu {
namespace tools {

string GetKuduToolAbsolutePath() {
  static const string kKuduCtlFileName = "kudu";
  string exe;
  CHECK_OK(Env::Default()->GetExecutablePath(&exe));
  const string binroot = DirName(exe);
  const string tool_abs_path = JoinPathSegments(binroot, kKuduCtlFileName);
  CHECK(Env::Default()->FileExists(tool_abs_path))
      << kKuduCtlFileName << " binary not found at " << tool_abs_path;
  return tool_abs_path;
}

Status RunKuduTool(const vector<string>& args, string* out, string* err,
                   const std::string& in) {
  vector<string> total_args = { GetKuduToolAbsolutePath() };

  // Speed up filesystem-based operations.
  total_args.emplace_back("--unlock_unsafe_flags");
  total_args.emplace_back("--never_fsync");

  // Do not colorize glog's output (i.e. messages logged via LOG()) even
  // if the GLOG_colorlogtostderr environment variable is set. This is to avoid
  // failing of tests that depend on the exact output from the tool
  // (e.g., the exact location of some substring/character in the output line).
  total_args.emplace_back("--nocolorlogtostderr");

  total_args.insert(total_args.end(), args.begin(), args.end());
  return Subprocess::Call(total_args, in, out, err);
}

} // namespace tools
} // namespace kudu
