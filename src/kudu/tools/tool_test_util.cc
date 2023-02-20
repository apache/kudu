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

#include <cstdio>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

using std::map;
using std::string;
using std::vector;
using strings::Split;
using strings::Substitute;

DECLARE_string(block_manager);

namespace kudu {
namespace tools {

string GetKuduToolAbsolutePath() {
  string path;
  CHECK_OK(GetKuduToolAbsolutePathSafe(&path));
  return path;
}

Status RunKuduTool(const vector<string>& args, string* out, string* err,
                   const string& in, map<string, string> env_vars) {
  vector<string> total_args = { GetKuduToolAbsolutePath() };

  // Some scenarios might add unsafe flags for testing purposes.
  total_args.emplace_back("--unlock_unsafe_flags");

  // Speed up filesystem-based operations.
  total_args.emplace_back("--never_fsync");

  // Do not colorize glog's output (i.e. messages logged via LOG()) even
  // if the GLOG_colorlogtostderr environment variable is set. This is to avoid
  // failing of tests that depend on the exact output from the tool
  // (e.g., the exact location of some substring/character in the output line).
  total_args.emplace_back("--nocolorlogtostderr");

  // Kudu masters and tablet servers run as a part of external mini-cluster use
  // shorter keys. Newer OS distros have OpenSSL built with the default security
  // level higher than 0, so it's necessary to override it on the client
  // side as well to allow clients to accept and verify TLS certificates.
  total_args.emplace_back("--openssl_security_level_override=0");

  total_args.emplace_back(Substitute("--block_manager=$0", FLAGS_block_manager));

  total_args.insert(total_args.end(), args.begin(), args.end());
  return Subprocess::Call(total_args, in, out, err, std::move(env_vars));
}

Status RunActionPrependStdoutStderr(const string& arg_str) {
  string stdout;
  string stderr;
  RETURN_NOT_OK_PREPEND(RunKuduTool(Split(arg_str, " ", strings::SkipEmpty()),
                                    &stdout, &stderr),
      Substitute("error running '$0': stdout: $1, stderr: $2", arg_str, stdout, stderr));
  return Status::OK();
}

} // namespace tools
} // namespace kudu
