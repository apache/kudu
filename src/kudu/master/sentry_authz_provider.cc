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

#include "kudu/master/sentry_authz_provider.h"

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/table_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/sentry_privileges_fetcher.h"
#include "kudu/security/token.pb.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/util/slice.h"
#include "kudu/util/trace.h"

DECLARE_string(sentry_service_rpc_addresses);

using kudu::security::ColumnPrivilegePB;
using kudu::security::TablePrivilegePB;
using kudu::sentry::SentryAction;
using kudu::sentry::SentryAuthorizableScope;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

namespace {

// Whether the given privileges 'privileges_branch' allows for the specified
// action ('required_action') in the specified scope ('required_scope')
// with GRANT ALL option, if any required ('requires_all_with_grant').
bool IsActionAllowed(SentryAction::Action required_action,
                     SentryAuthorizableScope::Scope required_scope,
                     SentryGrantRequired requires_all_with_grant,
                     const SentryPrivilegesBranch& privileges_branch) {
  // In general, a privilege implies another when:
  // 1. the authorizable from the former implies the authorizable from the latter
  //    (authorizable with a higher scope on the hierarchy can imply authorizables
  //    with a lower scope on the hierarchy, but not vice versa), and
  // 2. the action from the former implies the action from the latter, and
  // 3. grant option from the former implies the grant option from the latter.
  //
  // See org.apache.sentry.policy.common.CommonPrivilege. Note that policy validation
  // in CommonPrivilege also allows wildcard authorizable matching. For example,
  // authorizable 'server=server1->db=*' can imply authorizable 'server=server1'.
  // However, wildcard authorizable granting is neither practical nor useful (semantics
  // of granting such privilege are not supported in Apache Hive, Impala and Hue. And
  // 'server=server1->db=*' has exactly the same meaning as 'server=server1'). Therefore,
  // wildcard authorizable matching is dropped in this implementation.
  //
  // Moreover, because ListPrivilegesByUser lists all Sentry privileges granted to the
  // user that match the authorizable of each scope in the input authorizable hierarchy,
  // privileges with lower scope will also be returned in the response. This contradicts
  // rule (1) mentioned above. Therefore, we need to validate privilege scope, in addition
  // to action and grant option. Otherwise, privilege escalation can happen.
  TRACE("Evaluating privileges");
  SentryAction action(required_action);
  SentryAuthorizableScope scope(required_scope);
  const auto& privileges = privileges_branch.privileges();
  for (const auto& privilege : privileges) {
    // A grant option cannot imply the other if the latter is set but the
    // former is not.
    if (requires_all_with_grant == REQUIRED && !privilege.all_with_grant) {
      continue;
    }
    // Both privilege scope and action need to imply the other.
    if (SentryAuthorizableScope(privilege.scope).Implies(scope)) {
      for (const auto& allowed_action : privilege.allowed_actions) {
        if (SentryAction(allowed_action).Implies(action)) {
          return true;
        }
      }
    }
  }
  return false;
}

} // anonymous namespace

SentryAuthzProvider::SentryAuthzProvider(
    scoped_refptr<MetricEntity> metric_entity)
    : fetcher_(std::move(metric_entity)) {
}

SentryAuthzProvider::~SentryAuthzProvider() {
  Stop();
}

Status SentryAuthzProvider::Start() {
  return fetcher_.Start();
}

void SentryAuthzProvider::Stop() {
  fetcher_.Stop();
}

Status SentryAuthzProvider::ResetCache() {
  return fetcher_.ResetCache();
}

bool SentryAuthzProvider::IsEnabled() {
  return !FLAGS_sentry_service_rpc_addresses.empty();
}

Status SentryAuthzProvider::AuthorizeCreateTable(const string& table_name,
                                                 const string& user,
                                                 const string& owner) {
  // If the table is being created with a different owner than the user,
  // then the creating user must have 'ALL ON DATABASE' with grant. See
  // design doc in [SENTRY-2151](https://issues.apache.org/jira/browse/SENTRY-2151).
  //
  // Otherwise, table creation requires 'CREATE ON DATABASE' privilege.
  SentryAction::Action action;
  SentryGrantRequired grant_option;
  if (user == owner) {
    action = SentryAction::Action::CREATE;
    grant_option = NOT_REQUIRED;
  } else {
    action = SentryAction::Action::ALL;
    grant_option = REQUIRED;
  }
  // Note: in our request to Sentry, we shouldn't cache table- or column-level
  // privileges for the table, since Sentry may automatically grant privileges
  // upon creation of new tables that caching might miss.
  return Authorize(SentryAuthorizableScope::Scope::DATABASE, action,
                   table_name, user, grant_option,
                   SentryCaching::SERVER_AND_DB_ONLY);
}

Status SentryAuthzProvider::AuthorizeDropTable(const string& table_name,
                                               const string& user) {
  // Table deletion requires 'DROP ON TABLE' privilege.
  return Authorize(SentryAuthorizableScope::Scope::TABLE,
                   SentryAction::Action::DROP,
                   table_name, user);
}

Status SentryAuthzProvider::AuthorizeAlterTable(const string& old_table,
                                                const string& new_table,
                                                const string& user) {
  // For table alteration (without table rename) requires 'ALTER ON TABLE'
  // privilege;
  // For table alteration (with table rename) requires
  //  1. 'ALL ON TABLE <old-table>',
  //  2. 'CREATE ON DATABASE <new-database>'.
  // See [SENTRY-2264](https://issues.apache.org/jira/browse/SENTRY-2264).
  // TODO(hao): add inline hierarchy validation to avoid multiple RPCs.
  if (old_table == new_table) {
    return Authorize(SentryAuthorizableScope::Scope::TABLE,
                     SentryAction::Action::ALTER,
                     old_table, user);
  }
  RETURN_NOT_OK(Authorize(SentryAuthorizableScope::Scope::TABLE,
                          SentryAction::Action::ALL,
                          old_table, user));
  // Note: in our request to Sentry, we shouldn't cache table- or column-level
  // privileges for the table, since Sentry may automatically alter privileges
  // upon altering tables that caching might miss.
  return Authorize(SentryAuthorizableScope::Scope::DATABASE,
                   SentryAction::Action::CREATE, new_table, user,
                   SentryGrantRequired::NOT_REQUIRED,
                   SentryCaching::SERVER_AND_DB_ONLY);
}

Status SentryAuthzProvider::AuthorizeGetTableMetadata(const string& table_name,
                                                      const string& user) {
  // Retrieving table metadata requires 'METADATA ON TABLE' privilege.
  return Authorize(SentryAuthorizableScope::Scope::TABLE,
                   SentryAction::Action::METADATA,
                   table_name, user);
}

Status SentryAuthzProvider::AuthorizeListTables(const string& user,
                                                unordered_set<string>* table_names,
                                                bool* checked_table_names) {
  if (IsTrustedUser(user)) {
    *checked_table_names = false;
    return Status::OK();
  }
  unordered_set<string> authorized_tables;
  unordered_map<string, vector<string>> tables_by_db;
  for (auto table_name : *table_names) {
    Slice db_slice;
    Slice unused_table_slice;
    Status s = ParseHiveTableIdentifier(table_name, &db_slice, &unused_table_slice);
    if (!s.ok()) {
      continue;
    }
    LookupOrInsert(&tables_by_db, db_slice.ToString(), {}).emplace_back(std::move(table_name));
  }
  for (auto db_and_tables : tables_by_db) {
    auto tables_in_db = db_and_tables.second;
    DCHECK(!tables_in_db.empty());
    // Authorize database-level privileges first in case the user has
    // database-level privileges. This would allow us to avoid authorizing each
    // indiviudual table.
    // Note: the exact table isn't particularly important, as long as we pass
    // in a table within the database we're interested in.
    const string& first_table_name_in_db = tables_in_db[0];
    Status s = Authorize(SentryAuthorizableScope::Scope::DATABASE, SentryAction::METADATA,
                         first_table_name_in_db, user);
    if (s.ok()) {
      for (auto table_name : tables_in_db) {
        EmplaceOrDie(&authorized_tables, std::move(table_name));
      }
    } else {
      for (auto table_name : tables_in_db) {
        s = AuthorizeGetTableMetadata(table_name, user);
        if (s.ok()) {
          EmplaceOrDie(&authorized_tables, std::move(table_name));
        }
      }
    }
  }
  *table_names = authorized_tables;
  *checked_table_names = true;
  return Status::OK();
}

Status SentryAuthzProvider::FillTablePrivilegePB(const string& table_name,
                                                 const string& user,
                                                 const SchemaPB& schema_pb,
                                                 TablePrivilegePB* pb) {
  DCHECK(pb);
  DCHECK(pb->has_table_id());
  if (AuthzProvider::IsTrustedUser(user)) {
    pb->set_delete_privilege(true);
    pb->set_insert_privilege(true);
    pb->set_scan_privilege(true);
    pb->set_update_privilege(true);
    return Status::OK();
  }
  static ColumnPrivilegePB scan_col_privilege;
  scan_col_privilege.set_scan_privilege(true);

  // Note: it might seem like we could cache these TablePrivilegePBs rather
  // than parsing them from Sentry privileges every time. This is tricky
  // because the column-level privileges depend on the input schema, which may
  // be different upon subsequent calls to this function.
  SentryPrivilegesBranch privileges_branch;
  RETURN_NOT_OK(fetcher_.GetSentryPrivileges(
      SentryAuthorizableScope::TABLE, table_name, user,
      SentryCaching::ALL, &privileges_branch));
  unordered_set<string> scannable_col_names;
  static const SentryAuthorizableScope kTableScope(SentryAuthorizableScope::TABLE);
  for (const auto& privilege : privileges_branch.privileges()) {
    if (SentryAuthorizableScope(privilege.scope).Implies(kTableScope)) {
      // Pull out any privileges at the table scope or higher.
      if (ContainsKey(privilege.allowed_actions, SentryAction::ALL) ||
          ContainsKey(privilege.allowed_actions, SentryAction::OWNER)) {
        // Generate privilege with everything.
        pb->set_delete_privilege(true);
        pb->set_insert_privilege(true);
        pb->set_scan_privilege(true);
        pb->set_update_privilege(true);
        return Status::OK();
      }
      if (ContainsKey(privilege.allowed_actions, SentryAction::DELETE)) {
        pb->set_delete_privilege(true);
      }
      if (ContainsKey(privilege.allowed_actions, SentryAction::INSERT)) {
        pb->set_insert_privilege(true);
      }
      if (ContainsKey(privilege.allowed_actions, SentryAction::SELECT)) {
        pb->set_scan_privilege(true);
      }
      if (ContainsKey(privilege.allowed_actions, SentryAction::UPDATE)) {
        pb->set_update_privilege(true);
      }
    } else if (!pb->scan_privilege() &&
               (ContainsKey(privilege.allowed_actions, SentryAction::ALL) ||
                ContainsKey(privilege.allowed_actions, SentryAction::OWNER) ||
                ContainsKey(privilege.allowed_actions, SentryAction::SELECT))) {
      // Pull out any scan privileges at the column scope.
      DCHECK_EQ(SentryAuthorizableScope::COLUMN, privilege.scope);
      DCHECK(!privilege.column_name.empty());
      EmplaceIfNotPresent(&scannable_col_names, privilege.column_name);
    }
  }
  // If we got any column-level scan privileges and we don't already have
  // table-level scan privileges, set them now.
  if (!pb->scan_privilege()) {
    for (const auto& col : schema_pb.columns()) {
      if (ContainsKey(scannable_col_names, col.name())) {
        InsertIfNotPresent(pb->mutable_column_privileges(), col.id(), scan_col_privilege);
      }
    }
  }
  return Status::OK();
}

Status SentryAuthzProvider::Authorize(SentryAuthorizableScope::Scope scope,
                                      SentryAction::Action action,
                                      const string& table_ident,
                                      const string& user,
                                      SentryGrantRequired require_grant_option,
                                      SentryCaching caching) {
  if (AuthzProvider::IsTrustedUser(user)) {
    return Status::OK();
  }

  SentryPrivilegesBranch privileges;
  RETURN_NOT_OK(fetcher_.GetSentryPrivileges(scope, table_ident, user,
      caching, &privileges));
  if (IsActionAllowed(action, scope, require_grant_option, privileges)) {
    return Status::OK();
  }

  // Log a warning if the action is not authorized for debugging purpose, and
  // only return a generic error to users to avoid a side channel leak, e.g.
  // whether table A exists.
  LOG(WARNING) << Substitute("Action <$0> on table <$1> with authorizable scope "
                             "<$2> is not permitted for user <$3>",
                             sentry::ActionToString(action),
                             table_ident,
                             sentry::ScopeToString(scope),
                             user);
  return Status::NotAuthorized("unauthorized action");
}

} // namespace master
} // namespace kudu
