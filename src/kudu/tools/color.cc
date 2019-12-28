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

#include "kudu/tools/color.h"

#include <unistd.h>

#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"

DEFINE_string(color, "auto",
              "Specifies whether output should be colorized. The default value "
              "'auto' colorizes output if the output is a terminal. The other "
              "valid values are 'always' or 'never'.");
TAG_FLAG(color, stable);

static bool ValidateColorFlag(const char* flagname, const std::string& value) {
  if (value == "always" ||
      value == "auto" ||
      value == "never") {
    return true;
  }
  LOG(ERROR) << "option 'color' expects \"always\", \"auto\", or \"never\"";
  return false;
}
DEFINE_validator(color, &ValidateColorFlag);


namespace kudu {
namespace tools {

namespace {
bool UseColor() {
  if (FLAGS_color == "never") return false;
  if (FLAGS_color == "always") return true;
  return isatty(STDOUT_FILENO);
}

const char* StringForCode(AnsiCode color) {
  if (!UseColor()) return "";

  // Codes from: https://en.wikipedia.org/wiki/ANSI_escape_code
  switch (color) {
    case AnsiCode::RED:    return "\x1b[31m";
    case AnsiCode::GREEN:  return "\x1b[32m";
    case AnsiCode::YELLOW: return "\x1b[33m";
    case AnsiCode::BLUE:   return "\x1b[34m";
    case AnsiCode::RESET:  return "\x1b[m";
  }
  LOG(FATAL);
  return "";
}
} // anonymous namespace

std::string Color(AnsiCode color, StringPiece s) {
  return strings::Substitute("$0$1$2",
                             StringForCode(color),
                             s,
                             StringForCode(AnsiCode::RESET));
}

} // namespace tools
} // namespace kudu
