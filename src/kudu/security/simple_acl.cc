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

#include "kudu/security/simple_acl.h"

#include <ctype.h>

#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/status.h"

using std::set;
using std::string;
using std::vector;

namespace kudu {
namespace security {

SimpleAcl::SimpleAcl() {
}

SimpleAcl::~SimpleAcl() {
}

Status SimpleAcl::ParseFlag(const string& flag) {
  vector<StringPiece> fields = strings::Split(flag, ",", strings::SkipWhitespace());
  set<string> users;
  for (const auto& field : fields) {
    if (field.empty()) continue;
    // if any field is a wildcard, no need to include the rest.
    if (flag == "*") {
      Reset({"*"});
      return Status::OK();
    }


    // Leave open the use of various special characters at the start of each
    // username. We reserve some special characters that might be useful in
    // ACLs:
    // '!': might be interpreted as "not"
    // '@': often used to read data from a file
    // '#': comments
    // '$': maybe variable expansion?
    // '%': used by sudoers for groups
    // '*': only allowed for special wildcard ACL above
    // '-', '+', '=': useful for allow/deny style ACLs
    // <quote characters>: in case we want to add quoted strings
    // whitespace: down right confusing
    static const char* kReservedStartingCharacters = "!@#$%*-=+'\"";
    if (strchr(kReservedStartingCharacters, field[0]) ||
        isspace(field[0])) {
      return Status::NotSupported("invalid username", field.ToString());
    }

    users.insert(field.ToString());
  }

  Reset(std::move(users));
  return Status::OK();
}

void SimpleAcl::Reset(set<string> users) {
  users_ = std::move(users);
}

bool SimpleAcl::UserAllowed(const string& username) {
  return ContainsKey(users_, "*") || ContainsKey(users_, username);
}

} // namespace security
} // namespace kudu
