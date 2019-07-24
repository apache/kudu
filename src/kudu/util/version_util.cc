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

#include <regex.h>

#include <mutex>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using std::ostream;
using std::string;
using strings::Substitute;

namespace kudu {

bool Version::operator==(const Version& other) const {
  return this->major == other.major &&
         this->minor == other.minor &&
         this->maintenance == other.maintenance &&
         this->extra_delimiter == other.extra_delimiter &&
         this->extra == other.extra;
}

string Version::ToString() const {
  return extra.empty()
      ? Substitute("$0.$1.$2", major, minor, maintenance)
      : Substitute("$0.$1.$2$3$4", major, minor, maintenance, *extra_delimiter, extra);
}

ostream& operator<<(ostream& os, const Version& v) {
  return os << v.ToString();
}

Status ParseVersion(const string& version_str,
                    Version* v) {
  static regex_t re;
  static std::once_flag once;
  static const char* kVersionPattern =
      "^([[:digit:]]+)\\." // <major>.
      "([[:digit:]]+)\\."  // <minor>.
      "([[:digit:]]+)"     // <maintenance>
      "([.-].*)?$";        // [<delimiter><extra>]
  std::call_once(once, []{
      CHECK_EQ(0, regcomp(&re, kVersionPattern, REG_EXTENDED));
    });

  DCHECK(v);
  const Status invalid_ver_err =
      Status::InvalidArgument("invalid version string", version_str);
  auto v_str = version_str;
  StripWhiteSpace(&v_str);

  regmatch_t matches[5];
  if (regexec(&re, v_str.c_str(), arraysize(matches), matches, 0) != 0) {
    return invalid_ver_err;
  }
#define PARSE_REQUIRED_COMPONENT(idx, lhs)                              \
  {                                                                     \
    int i = (idx);                                                      \
    if (matches[i].rm_so == -1 ||                                       \
        !SimpleAtoi(v_str.substr(matches[i].rm_so,                      \
                                 matches[i].rm_eo - matches[i].rm_so),  \
                    (lhs))) {                                           \
      return invalid_ver_err;                                           \
    }                                                                   \
  }
  Version temp_v;
  PARSE_REQUIRED_COMPONENT(1, &temp_v.major);
  PARSE_REQUIRED_COMPONENT(2, &temp_v.minor);
  PARSE_REQUIRED_COMPONENT(3, &temp_v.maintenance);
#undef PARSE_REQUIRED_COMPONENT
  if (matches[4].rm_so != -1) {
    int extra_comp_off = matches[4].rm_so + 1; // skip the delimiter
    int extra_comp_len = matches[4].rm_eo - extra_comp_off;
    if (extra_comp_len > 0) {
      temp_v.extra_delimiter = v_str[extra_comp_off - 1];
      temp_v.extra = v_str.substr(extra_comp_off, extra_comp_len);
    }
  }
  temp_v.raw_version = version_str;
  *v = std::move(temp_v);

  return Status::OK();
}

} // namespace kudu
