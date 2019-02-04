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

#include <set>
#include <string>

#include <gflags/gflags_declare.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(server_name);

namespace kudu {
namespace master {

inline Status DropRole(sentry::SentryClient* sentry_client,
                       const std::string& role_name) {
  ::sentry::TDropSentryRoleRequest role_req;
  role_req.__set_requestorUserName("test-admin");
  role_req.__set_roleName(role_name);
  return sentry_client->DropRole(role_req);
}

inline Status CreateRoleAndAddToGroups(sentry::SentryClient* sentry_client,
                                       const std::string& role_name,
                                       const std::string& group_name) {
  ::sentry::TCreateSentryRoleRequest role_req;
  role_req.__set_requestorUserName("test-admin");
  role_req.__set_roleName(role_name);
  RETURN_NOT_OK(sentry_client->CreateRole(role_req));

  ::sentry::TSentryGroup group;
  group.groupName = group_name;
  std::set<::sentry::TSentryGroup> groups;
  groups.insert(group);
  ::sentry::TAlterSentryRoleAddGroupsRequest group_request;
  ::sentry::TAlterSentryRoleAddGroupsResponse group_response;
  group_request.__set_requestorUserName("test-admin");
  group_request.__set_roleName(role_name);
  group_request.__set_groups(groups);
  return sentry_client->AlterRoleAddGroups(group_request, &group_response);
}

inline Status AlterRoleGrantPrivilege(sentry::SentryClient* sentry_client,
                                      const std::string& role_name,
                                      const ::sentry::TSentryPrivilege& privilege) {
  ::sentry::TAlterSentryRoleGrantPrivilegeRequest privilege_request;
  ::sentry::TAlterSentryRoleGrantPrivilegeResponse privilege_response;
  privilege_request.__set_requestorUserName("test-admin");
  privilege_request.__set_roleName(role_name);
  privilege_request.__set_privilege(privilege);
  return sentry_client->AlterRoleGrantPrivilege(privilege_request, &privilege_response);
}

// Returns a server level TSentryPrivilege with the server name, action
// and grant option.
inline ::sentry::TSentryPrivilege GetServerPrivilege(
    const std::string& action,
    const ::sentry::TSentryGrantOption::type& grant_option =
        ::sentry::TSentryGrantOption::DISABLED) {
  ::sentry::TSentryPrivilege privilege;
  privilege.__set_privilegeScope("SERVER");
  privilege.__set_serverName(FLAGS_server_name);
  privilege.__set_action(action);
  privilege.__set_grantOption(grant_option);
  return privilege;
}

// Returns a database level TSentryPrivilege with the given database name, action
// and grant option.
inline ::sentry::TSentryPrivilege GetDatabasePrivilege(
    const std::string& db_name,
    const std::string& action,
    const ::sentry::TSentryGrantOption::type& grant_option =
        ::sentry::TSentryGrantOption::DISABLED) {
  ::sentry::TSentryPrivilege privilege = GetServerPrivilege(action, grant_option);
  privilege.__set_privilegeScope("DATABASE");
  privilege.__set_dbName(db_name);
  return privilege;
}

// Returns a table level TSentryPrivilege with the given table name, database name,
// action and grant option.
inline ::sentry::TSentryPrivilege GetTablePrivilege(
    const std::string& db_name,
    const std::string& table_name,
    const std::string& action,
    const ::sentry::TSentryGrantOption::type& grant_option =
        ::sentry::TSentryGrantOption::DISABLED) {
  ::sentry::TSentryPrivilege privilege = GetDatabasePrivilege(db_name, action, grant_option);
  privilege.__set_privilegeScope("TABLE");
  privilege.__set_tableName(table_name);
  return privilege;
}

// Returns a column level TSentryPrivilege with the given column name, table name,
// database name, action and grant option.
inline ::sentry::TSentryPrivilege GetColumnPrivilege(
    const std::string& db_name,
    const std::string& table_name,
    const std::string& column_name,
    const std::string& action,
    const ::sentry::TSentryGrantOption::type& grant_option =
        ::sentry::TSentryGrantOption::DISABLED) {
  ::sentry::TSentryPrivilege privilege = GetTablePrivilege(db_name, table_name,
                                                 action, grant_option);
  privilege.__set_privilegeScope("COLUMN");
  privilege.__set_columnName(column_name);
  return privilege;
}

} // namespace master
} // namespace kudu
