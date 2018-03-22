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

#include <algorithm>
#include <array>
#include <cerrno>
#include <fstream> // IWYU pragma: keep
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/diagnostics_log_parser.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

using std::array;
using std::ifstream;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

const char* kLogPathArg = "path";

namespace {

Status ParseStacksFromPath(const string& path) {
  errno = 0;
  ifstream in(path);
  if (!in.is_open()) {
    return Status::IOError(ErrnoToString(errno));
  }
  StackDumpingLogVisitor dlv;
  LogParser lp(&dlv);
  string line;
  int line_number = 0;
  while (std::getline(in, line)) {
    line_number++;
    RETURN_NOT_OK_PREPEND(lp.ParseLine(std::move(line)),
                          Substitute("at line $0", line_number));
  }

  return Status::OK();
}

Status ParseStacks(const RunnerContext& context) {
  vector<string> paths = context.variadic_args;
  // The file names are such that lexicographic sorting reflects
  // timestamp-based sorting.
  std::sort(paths.begin(), paths.end());
  for (const auto& path : paths) {
    RETURN_NOT_OK_PREPEND(ParseStacksFromPath(path),
                          Substitute("failed to parse stacks from $0", path));
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildDiagnoseMode() {
  unique_ptr<Action> parse_stacks =
      ActionBuilder("parse_stacks", &ParseStacks)
      .Description("Parse sampled stack traces out of a diagnostics log")
      .AddRequiredVariadicParameter({ kLogPathArg, "path to log file(s) to parse" })
      .Build();

  return ModeBuilder("diagnose")
      .Description("Diagnostic tools for Kudu servers and clusters")
      .AddAction(std::move(parse_stacks))
      .Build();
}

} // namespace tools
} // namespace kudu
