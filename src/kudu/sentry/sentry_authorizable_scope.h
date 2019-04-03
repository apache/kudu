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

#pragma once

#include <iosfwd>
#include <string>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/util/bitset.h"
#include "kudu/util/status.h"

namespace kudu {
namespace sentry {

// A carbon copy of Sentry authorizable scope, which indicates the
// hierarchy of authorizables (server → database → table → column).
// For example, authorizable 'server=server1->db=a' has database
// level scope, while authorizable 'server=server1' has server level
// scope.
//
// A privilege scope can imply another following rules defined in
// Implies().
class SentryAuthorizableScope {
 public:

  // Note that 'UNINITIALIZED' is not an actual scope but
  // only to represent the uninitialized state.
  enum Scope {
    UNINITIALIZED,
    SERVER,
    DATABASE,
    TABLE,
    COLUMN,
  };
  static const size_t kScopeMaxVal = Scope::COLUMN + 1;

  // The default constructor is useful when creating an authorizable scope
  // from string.
  SentryAuthorizableScope();

  explicit SentryAuthorizableScope(Scope scope);

  Scope scope() const {
    return scope_;
  }

  // Create an authorizable scope from string.
  static Status FromString(const std::string& str,
                           SentryAuthorizableScope* scope) WARN_UNUSED_RESULT;

  // Returns true if one authorizable scope can imply another. In general,
  // 1.  an authorizable scope implies itself.
  // 2.  a higher scope in the hierarchy implies a lower scope in the hierarchy.
  bool Implies(const SentryAuthorizableScope& other) const;

 private:
  Scope scope_;
};

static constexpr const char* const kSever = "SERVER";
static constexpr const char* const kDatabase = "DATABASE";
static constexpr const char* const kTable = "TABLE";
static constexpr const char* const kColumn = "COLUMN";

const char* ScopeToString(SentryAuthorizableScope::Scope scope);

std::ostream& operator<<(std::ostream& o, SentryAuthorizableScope::Scope scope);

typedef FixedBitSet<SentryAuthorizableScope::Scope, SentryAuthorizableScope::kScopeMaxVal>
    AuthorizableScopesSet;

} // namespace sentry
} // namespace kudu
