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

#include "kudu/util/version_util.h"

#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using std::ostream;
using std::string;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {

bool Version::operator==(const Version& other) const {
  return this->major == other.major &&
         this->minor == other.minor &&
         this->maintenance == other.maintenance &&
         this->extra == other.extra;
}

string Version::ToString() const {
  return extra.empty()
      ? Substitute("$0.$1.$2", major, minor, maintenance)
      : Substitute("$0.$1.$2-$3", major, minor, maintenance, extra);
}

ostream& operator<<(ostream& os, const Version& v) {
  return os << v.ToString();
}

Status ParseVersion(const string& version_str,
                    Version* v) {
  static const char* const kDelimiter = "-";

  DCHECK(v);
  const Status invalid_ver_err =
      Status::InvalidArgument("invalid version string", version_str);
  auto v_str = version_str;
  StripWhiteSpace(&v_str);
  const vector<string> main_and_extra = Split(v_str, kDelimiter);
  if (main_and_extra.empty()) {
    return invalid_ver_err;
  }
  const vector<string> maj_min_maint = Split(main_and_extra.front(), ".");
  if (maj_min_maint.size() != 3) {
    return invalid_ver_err;
  }
  Version temp_v;
  if (!SimpleAtoi(maj_min_maint[0], &temp_v.major) ||
      !SimpleAtoi(maj_min_maint[1], &temp_v.minor) ||
      !SimpleAtoi(maj_min_maint[2], &temp_v.maintenance)) {
    return invalid_ver_err;
  }
  temp_v.extra = JoinStringsIterator(std::next(main_and_extra.begin()),
                                     main_and_extra.end(), kDelimiter);
  temp_v.raw_version = version_str;
  *v = std::move(temp_v);

  return Status::OK();
}

} // namespace kudu
