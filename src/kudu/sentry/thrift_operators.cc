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

#include <set>
#include <string>

#include "kudu/sentry/sentry_policy_service_types.h"

// Thrift does not automatically generate operator< definitions for generated
// classes, however it will happily translate a Thrift set in to a C++ set,
// which requires the key type to implement operator<. Since Sentry uses Thrift
// types as map keys and set items, we must provide our own definition. See
// http://mail-archives.apache.org/mod_mbox/thrift-user/201311.mbox/%3cBAY407-EAS7268C0ADCDA8F02D874F8EB1F30@phx.gbl%3e
// for more discussion.

namespace sentry {

// Returns true if lhs < rhs, false if lhs > rhs, and passes through if the two
// are equal.
#define RETURN_IF_DIFFERENT_LT(lhs, rhs) do { \
  if ((lhs) != (rhs)) { \
    return (lhs) < (rhs); \
  } \
} while (false)

// Returns true if the optional field in 'this' is less than the optional field
// in 'other', and false if greater than. Passes through if the two are equal.
// Unset fields compare less than set fields.
#define OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(field) do { \
  if (this->__isset.field && other.__isset.field) { \
    RETURN_IF_DIFFERENT_LT(this->field, other.field); \
  } else if (this->__isset.field || other.__isset.field) { \
    return other.__isset.field; \
  } \
} while (false)

bool TSentryRole::operator<(const TSentryRole& other) const {
  RETURN_IF_DIFFERENT_LT(this->roleName, other.roleName);
  RETURN_IF_DIFFERENT_LT(this->groups, other.groups);
  RETURN_IF_DIFFERENT_LT(this->grantorPrincipal, other.grantorPrincipal);
  return false;
}

bool TSentryGroup::operator<(const TSentryGroup& other) const {
  return this->groupName < other.groupName;
}

bool TSentryPrivilege::operator<(const TSentryPrivilege& other) const {
  RETURN_IF_DIFFERENT_LT(this->privilegeScope, other.privilegeScope);
  RETURN_IF_DIFFERENT_LT(this->serverName, other.serverName);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(dbName);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(tableName);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(URI);
  RETURN_IF_DIFFERENT_LT(this->action, other.action);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(createTime);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(grantOption);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(columnName);
  return false;
}

bool TSentryAuthorizable::operator<(const TSentryAuthorizable& other) const {
  RETURN_IF_DIFFERENT_LT(this->server, other.server);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(uri);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(db);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(table);
  OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT(column);
  return false;
}

#undef OPTIONAL_FIELD_RETURN_IF_DIFFERENT_LT
#undef RETURN_IF_DIFFERENT_LT

} // namespace sentry
