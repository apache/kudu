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

#include <cstdint>

#include <ostream>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace sentry {

const char* ScopeToString(SentryAuthorizableScope::Scope scope) {
  switch (scope) {
    case SentryAuthorizableScope::Scope::UNINITIALIZED: return "UNINITIALIZED";
    case SentryAuthorizableScope::Scope::SERVER: return kSever;
    case SentryAuthorizableScope::Scope::DATABASE: return kDatabase;
    case SentryAuthorizableScope::Scope::TABLE: return kTable;
    case SentryAuthorizableScope::Scope::COLUMN: return kColumn;
  }
  LOG(FATAL) << static_cast<uint16_t>(scope) << ": unknown scope";
  return nullptr;
}

std::ostream& operator<<(std::ostream& o, SentryAuthorizableScope::Scope scope) {
  return o << ScopeToString(scope);
}

SentryAuthorizableScope::SentryAuthorizableScope()
  : scope_(Scope::UNINITIALIZED) {
}

SentryAuthorizableScope::SentryAuthorizableScope(Scope scope)
  : scope_(scope) {
}

Status SentryAuthorizableScope::FromString(const string& str,
                                           SentryAuthorizableScope* scope) {
  if (boost::iequals(str, kSever)) {
    scope->scope_ = Scope::SERVER;
  } else if (boost::iequals(str, kDatabase)) {
    scope->scope_ = Scope::DATABASE;
  } else if (boost::iequals(str, kTable)) {
    scope->scope_ = Scope::TABLE;
  } else if (boost::iequals(str, kColumn)) {
    scope->scope_ = Scope::COLUMN;
  } else {
    return Status::InvalidArgument(Substitute("unknown SentryAuthorizableScope: $0", str));
  }

  return Status::OK();
}

bool SentryAuthorizableScope::Implies(const SentryAuthorizableScope& other) const {
  switch (scope_) {
    case Scope::COLUMN:
      return other.scope_ == Scope::COLUMN;
    case Scope::TABLE:
      return other.scope_ == Scope::TABLE ||
             other.scope_ == Scope::COLUMN;
    case Scope::DATABASE:
      return other.scope_ == Scope::DATABASE ||
             other.scope_ == Scope::TABLE ||
             other.scope_ == Scope::COLUMN;
    case Scope::SERVER:
      return other.scope_ == Scope::SERVER ||
             other.scope_ == Scope::DATABASE ||
             other.scope_ == Scope::TABLE ||
             other.scope_ == Scope::COLUMN;
    default:
      LOG(FATAL) << "unsupported SentryAuthorizableScope";
      break;
  }
  return false;
}

} // namespace sentry
} // namespace kudu
