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

#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/port.h"
#include "kudu/master/authz_provider.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/status.h"

namespace kudu {

namespace master {

// An implementation of AuthzProvider that connects to the Sentry service
// for authorization metadata and allows or denies the actions performed by
// users based on the metadata.
//
// This class is thread-safe after Start() is called.
class SentryAuthzProvider : public AuthzProvider {
 public:

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

 private:
  FRIEND_TEST(TestAuthzHierarchy, TestAuthorizableScope);

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

  thrift::HaClient<sentry::SentryClient> ha_client_;
};

} // namespace master
} // namespace kudu
