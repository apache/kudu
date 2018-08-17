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

bool TSentryRole::operator<(const TSentryRole& other) const {
  return this->roleName < other.roleName
      && this->groups < other.groups
      && this->grantorPrincipal < other.grantorPrincipal;
}

bool TSentryGroup::operator<(const TSentryGroup& other) const {
  return this->groupName < other.groupName;
}

bool TSentryPrivilege::operator<(const TSentryPrivilege& other) const {
  return this->privilegeScope < other.privilegeScope
      && this->serverName < other.serverName
      && this->dbName < other.dbName
      && this->tableName < other.tableName
      && this->URI < other.URI
      && this->action < other.action
      && this->createTime < other.createTime
      && this->grantOption < other.grantOption
      && this->columnName < other.columnName;
}

bool TSentryAuthorizable::operator<(const TSentryAuthorizable& other) const {
  return this->server < other.server
      && this->uri < other.uri
      && this->db < other.db
      && this->table < other.table
      && this->column < other.column;
}

} // namespace sentry
