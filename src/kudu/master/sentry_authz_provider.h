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

#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/authz_provider.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace sentry {
class TSentryAuthorizable;
class TSentryPrivilege;
} // namespace sentry

namespace kudu {

class SchemaPB;

namespace security {
class TablePrivilegePB;
} // namespace security

namespace master {

// Utility struct to facilitate evaluating the privileges of a given
// authorizable. This is preferred to using Sentry's Thrift responses directly,
// since useful information has already been parsed to generate this struct
// (e.g. the SentryActions and scope).
struct AuthorizablePrivileges {
  AuthorizablePrivileges(sentry::SentryAuthorizableScope::Scope scope,
                         std::string db,
                         std::string table,
                         std::string column)
    : scope(scope),
      db_name(std::move(db)),
      table_name(std::move(table)),
      column_name(std::move(column)) {
#ifndef NDEBUG
    switch (scope) {
      case sentry::SentryAuthorizableScope::COLUMN:
        CHECK(!column_name.empty());
        FALLTHROUGH_INTENDED;
      case sentry::SentryAuthorizableScope::TABLE:
        CHECK(!table_name.empty());
        FALLTHROUGH_INTENDED;
      case sentry::SentryAuthorizableScope::DATABASE:
        CHECK(!db_name.empty());
        break;
      case sentry::SentryAuthorizableScope::SERVER:
        break;
      default:
        LOG(FATAL) << "not reachable";
    }
#endif
  }

  // Whether the privilege 'ALL' or 'OWNER' has been granted with Sentry's
  // grant option enabled. Note that the grant option can be granted on any
  // action, but for Kudu, we only use it with 'ALL' or 'OWNER'.
  bool all_with_grant = false;

  // The scope of the authorizable being granted privileges.
  const sentry::SentryAuthorizableScope::Scope scope;

  // The set of actions on which privileges are granted.
  sentry::SentryActionsSet granted_privileges;

  // The fields of the authorizable.
  std::string db_name;
  std::string table_name;
  std::string column_name;
};

// A representation of the Sentry privilege hierarchy branch for a single table
// (including privileges for the table's ancestors and descendents) for a
// single user.
struct SentryPrivilegesBranch {
  // Set of privileges are granted.
  std::vector<AuthorizablePrivileges> privileges;

  // Returns whether or not this implies the given action and scope for the
  // given table.
  bool Implies(sentry::SentryAuthorizableScope::Scope required_scope,
               sentry::SentryAction::Action required_action,
               bool requires_all_with_grant) const;
};

// An implementation of AuthzProvider that connects to the Sentry service
// for authorization metadata and allows or denies the actions performed by
// users based on the metadata.
//
// This class is thread-safe after Start() is called.
class SentryAuthzProvider : public AuthzProvider {
 public:
  explicit SentryAuthzProvider(scoped_refptr<MetricEntity> metric_entity = {});

  ~SentryAuthzProvider();

  // Start SentryAuthzProvider instance which connects to the Sentry service.
  Status Start() override WARN_UNUSED_RESULT;

  void Stop() override;

  // Returns true if the SentryAuthzProvider should be enabled.
  static bool IsEnabled();

  // The following authorizing methods will fail if:
  //   - the operation is not authorized
  //   - the Sentry service is unreachable
  //   - Sentry fails to resolve the group mapping of the user
  //   - the specified '--kudu_service_name' is a non-admin user in Sentry
  // TODO(hao): add early failure recognition when SENTRY-2440 is done.

  Status AuthorizeCreateTable(const std::string& table_name,
                              const std::string& user,
                              const std::string& owner) override WARN_UNUSED_RESULT;

  Status AuthorizeDropTable(const std::string& table_name,
                            const std::string& user) override WARN_UNUSED_RESULT;

  // Note that the caller should normalize the table name if case sensitivity is not
  // enforced for naming. e.g. when HMS integration is enabled.
  Status AuthorizeAlterTable(const std::string& old_table,
                             const std::string& new_table,
                             const std::string& user) override WARN_UNUSED_RESULT;

  Status AuthorizeGetTableMetadata(const std::string& table_name,
                                   const std::string& user) override WARN_UNUSED_RESULT;

  Status FillTablePrivilegePB(const std::string& table_name,
                              const std::string& user,
                              const SchemaPB& schema_pb,
                              security::TablePrivilegePB* pb) override WARN_UNUSED_RESULT;

 private:
  friend class SentryAuthzProviderFilterPrivilegesTest;
  FRIEND_TEST(SentryAuthzProviderStaticTest, TestPrivilegesWellFormed);
  FRIEND_TEST(TestAuthzHierarchy, TestAuthorizableScope);
  FRIEND_TEST(SentryAuthzProviderFilterPrivilegesScopeTest, TestFilterInvalidResponses);
  FRIEND_TEST(SentryAuthzProviderFilterPrivilegesScopeTest, TestFilterValidResponses);

  // Utility function to determine whether the given privilege is a well-formed
  // possibly Kudu-related privilege describing a descendent or ancestor of the
  // requested authorizable in the Sentry hierarchy tree, i.e. it:
  // - has a Kudu-related action (e.g. ALL, INSERT, UPDATE, etc.),
  // - has a Kudu-related authorizable scope (e.g. SERVER, DATABASE, etc.),
  // - all fields of equal or higher scope to the privilege's scope are set;
  //   none lower are set, and
  // - all fields that are set match those set by the input authorizable.
  static bool SentryPrivilegeIsWellFormed(
      const ::sentry::TSentryPrivilege& privilege,
      const ::sentry::TSentryAuthorizable& requested_authorizable,
      sentry::SentryAuthorizableScope::Scope* scope,
      sentry::SentryAction::Action* action);

  // Returns the set of scope fields expected to be non-empty in a Sentry
  // response with the given authorizable scope. All fields of equal or higher
  // scope are expected to be set.
  static const sentry::AuthorizableScopesSet& ExpectedNonEmptyFields(
      sentry::SentryAuthorizableScope::Scope scope);

  // Returns the set of scope fields expected to be empty in a Sentry response
  // with the given authorizable scope. All fields of lower scope are expected
  // to be empty.
  static const sentry::AuthorizableScopesSet& ExpectedEmptyFields(
      sentry::SentryAuthorizableScope::Scope scope);

  // Fetches the user's privileges from Sentry for the authorizable specified
  // by the given table and scope.
  Status GetSentryPrivileges(sentry::SentryAuthorizableScope::Scope scope,
                             const std::string& table_name,
                             const std::string& user,
                             SentryPrivilegesBranch* privileges);

  // Checks if the user can perform an action on the table identifier (in the format
  // <database-name>.<table-name>), based on the given authorizable scope and the
  // grant option. Note that the authorizable scope should be equal or higher than
  // 'TABLE' scope.
  //
  // If the operation is not authorized, returns Status::NotAuthorized().
  // Note that the authorization process is case insensitive for the
  // authorizables.
  Status Authorize(sentry::SentryAuthorizableScope::Scope scope,
                   sentry::SentryAction::Action action,
                   const std::string& table_ident,
                   const std::string& user,
                   bool require_grant_option = false);

  scoped_refptr<MetricEntity> metric_entity_;
  thrift::HaClient<sentry::SentryClient> ha_client_;
};

} // namespace master
} // namespace kudu
