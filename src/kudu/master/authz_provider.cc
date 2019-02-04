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

#include "kudu/master/authz_provider.h"

#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/flag_tags.h"

DEFINE_string(trusted_user_acl, "",
    "Comma-separated list of trusted users who may access the cluster "
    "without being authorized against fine-grained permissions.");
TAG_FLAG(trusted_user_acl, experimental);

using std::string;
using std::vector;

namespace kudu {
namespace master {

AuthzProvider::AuthzProvider() {
  vector<string> acls = strings::Split(FLAGS_trusted_user_acl, ",", strings::SkipEmpty());
  std::move(acls.begin(), acls.end(), std::inserter(trusted_users_, trusted_users_.end()));
}

bool AuthzProvider::IsTrustedUser(const string& user) {
  return ContainsKey(trusted_users_, user);
}

} // namespace master
} // namespace kudu
