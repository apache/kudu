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

#include "kudu/sentry/sentry_authorizable_scope.h"

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {

namespace sentry {

TEST(SentryAuthorizableScopeTest, TestImplyScope) {
  SentryAuthorizableScope server(SentryAuthorizableScope::Scope::SERVER);
  SentryAuthorizableScope database(SentryAuthorizableScope::Scope::DATABASE);
  SentryAuthorizableScope table(SentryAuthorizableScope::Scope::TABLE);
  SentryAuthorizableScope column(SentryAuthorizableScope::Scope::COLUMN);

  vector<SentryAuthorizableScope> scopes({ column, table, database, server });
  vector<SentryAuthorizableScope> lower_scopes;
  vector<SentryAuthorizableScope> higher_scopes({ server, database, table, column });

  // A higher scope in the hierarchy can imply a lower scope in the hierarchy,
  // not vice versa.
  for (const auto& scope : scopes) {
    lower_scopes.emplace_back(scope);
    higher_scopes.pop_back();
    for (const auto &lower_scope : lower_scopes) {
      ASSERT_TRUE(scope.Implies(lower_scope));
    }
    for (const auto &higher_scope : higher_scopes) {
      ASSERT_FALSE(scope.Implies(higher_scope));
    }
  }
}

TEST(SentryAuthorizableScopeTest, TestFromString) {
  const vector<pair<string, SentryAuthorizableScope::Scope>> valid_scopes = {
      {"server", SentryAuthorizableScope::Scope::SERVER},
      {"database", SentryAuthorizableScope::Scope::DATABASE},
      {"table", SentryAuthorizableScope::Scope::TABLE},
      {"column", SentryAuthorizableScope::Scope::COLUMN}
  };
  SentryAuthorizableScope scope;
  for (const auto& valid_scope : valid_scopes) {
    ASSERT_OK(SentryAuthorizableScope::FromString(valid_scope.first, &scope));
    ASSERT_EQ(valid_scope.second, scope.scope());
  }

  // Unsupported scope returns invalid argument error.
  const vector<string> invalid_scopes({ "UNINITIALIZED", "tablecolumn" });
  for (const auto& invalid_scope : invalid_scopes) {
    Status s = SentryAuthorizableScope::FromString(invalid_scope, &scope);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }
}

} // namespace sentry
} // namespace kudu
