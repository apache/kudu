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

#include "kudu/sentry/sentry_client.h"

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry-test-base.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using sentry::TAlterSentryRoleAddGroupsRequest;
using sentry::TAlterSentryRoleAddGroupsResponse;
using sentry::TAlterSentryRoleGrantPrivilegeRequest;
using sentry::TAlterSentryRoleGrantPrivilegeResponse;
using sentry::TCreateSentryRoleRequest;
using sentry::TDropSentryRoleRequest;
using sentry::TListSentryPrivilegesRequest;
using sentry::TListSentryPrivilegesResponse;
using sentry::TSentryAuthorizable;
using sentry::TSentryGroup;
using sentry::TSentryPrivilege;
using std::set;
using std::string;
using std::vector;

namespace kudu {
namespace sentry {

class SentryClientTest : public SentryTestBase,
                         public ::testing::WithParamInterface<bool> {
 public:
  bool KerberosEnabled() const {
    return GetParam();
  }
};

INSTANTIATE_TEST_CASE_P(KerberosEnabled, SentryClientTest, ::testing::Bool());

TEST_P(SentryClientTest, TestMiniSentryLifecycle) {
  // Create an HA Sentry client and ensure it automatically reconnects after service interruption.
  thrift::HaClient<SentryClient> client;
  thrift::ClientOptions sentry_client_opts;
  if (KerberosEnabled()) {
    sentry_client_opts.enable_kerberos = true;
    sentry_client_opts.service_principal = "sentry";
  }

  ASSERT_OK(client.Start(vector<HostPort>({sentry_->address()}),
                         sentry_client_opts));
  auto smoketest = [&]() -> Status {
    return client.Execute([](SentryClient* client) -> Status {
        TCreateSentryRoleRequest create_req;
        create_req.requestorUserName = "test-admin";
        create_req.roleName = "test-role";
        RETURN_NOT_OK(client->CreateRole(create_req));

        TDropSentryRoleRequest drop_req;
        drop_req.requestorUserName = "test-admin";
        drop_req.roleName = "test-role";
        RETURN_NOT_OK(client->DropRole(drop_req));
        return Status::OK();
    });
  };

  ASSERT_OK(smoketest());

  ASSERT_OK(sentry_->Stop());
  ASSERT_OK(sentry_->Start());
  ASSERT_OK(smoketest());

  ASSERT_OK(sentry_->Pause());
  ASSERT_OK(sentry_->Resume());
  ASSERT_OK(smoketest());
}

// Basic functionality test of the Sentry client. The goal is not an exhaustive
// test of Sentry's role handling, but instead verification that the client can
// communicate with the Sentry service, and errors are converted to Status
// instances.
TEST_P(SentryClientTest, TestCreateDropRole) {

  { // Create a role
    TCreateSentryRoleRequest req;
    req.requestorUserName = "test-admin";
    req.roleName = "viewer";
    ASSERT_OK(sentry_client_->CreateRole(req));

    // Attempt to create the role again.
    Status s = sentry_client_->CreateRole(req);
    ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  }

  { // Attempt to create a role as a non-admin user.
    TCreateSentryRoleRequest req;
    req.requestorUserName = "joe-interloper";
    req.roleName = "fuzz";
    Status s = sentry_client_->CreateRole(req);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  { // Attempt to drop the role as a non-admin user.
    TDropSentryRoleRequest req;
    req.requestorUserName = "joe-interloper";
    req.roleName = "viewer";
    Status s = sentry_client_->DropRole(req);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  { // Drop the role
    TDropSentryRoleRequest req;
    req.requestorUserName = "test-admin";
    req.roleName = "viewer";
    ASSERT_OK(sentry_client_->DropRole(req));

    // Attempt to drop the role again.
    Status s = sentry_client_->DropRole(req);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
}

// Similar to above test to verify that the client can communicate with the
// Sentry service to list privileges, and errors are converted to Status
// instances.
TEST_P(SentryClientTest, TestListPrivileges) {
  // Attempt to access Sentry privileges without setting the user/principal name.
  TSentryAuthorizable authorizable;
  authorizable.server = "server";
  authorizable.db = "db";
  authorizable.table = "table";
  TListSentryPrivilegesRequest request;
  request.__set_requestorUserName("joe-interloper");
  request.__set_authorizableHierarchy(authorizable);
  TListSentryPrivilegesResponse response;
  Status s = sentry_client_->ListPrivilegesByUser(request, &response);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  // Attempt to access Sentry privileges by a non admin user.
  request.__set_principalName("viewer");
  s = sentry_client_->ListPrivilegesByUser(request, &response);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Attempt to access Sentry privileges by a user without
  // group mapping.
  request.requestorUserName = "user-without-mapping";
  s = sentry_client_->ListPrivilegesByUser(request, &response);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Attempt to access Sentry privileges of a non-exist user.
  request.requestorUserName = "test-admin";
  s = sentry_client_->ListPrivilegesByUser(request, &response);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // List the privileges of user 'test-user' .
  request.__set_principalName("test-user");
  ASSERT_OK(sentry_client_->ListPrivilegesByUser(request, &response));
}

// Similar to above test to verify that the client can communicate with the
// Sentry service to add roles to groups (this allows the users from the
// groups have all privilege the role has), and errors are converted to Status
// instances.
TEST_P(SentryClientTest, TestAlterRoleAddGroups) {
  // Attempt to alter role by a non admin user.

  TSentryGroup group;
  group.groupName = "user";
  set<TSentryGroup> groups;
  groups.insert(group);

  TAlterSentryRoleAddGroupsRequest group_request;
  TAlterSentryRoleAddGroupsResponse group_response;
  group_request.__set_requestorUserName("joe-interloper");
  group_request.__set_roleName("viewer");
  group_request.__set_groups(groups);

  Status s = sentry_client_->AlterRoleAddGroups(group_request, &group_response);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Attempt to alter a non-exist role.
  group_request.__set_requestorUserName("test-admin");
  s = sentry_client_->AlterRoleAddGroups(group_request, &group_response);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Alter role 'viewer' to add group 'user'.
  TCreateSentryRoleRequest role_request;
  role_request.__set_requestorUserName("test-admin");
  role_request.__set_roleName("viewer");
  ASSERT_OK(sentry_client_->CreateRole(role_request));
  ASSERT_OK(sentry_client_->AlterRoleAddGroups(group_request, &group_response));
}

// Similar to above test to verify that the client can communicate with the
// Sentry service to grant privileges, and errors are converted to Status
// instances.
TEST_P(SentryClientTest, TestGrantPrivilege) {
  // Alter role 'viewer' to add group 'user'.

  TSentryGroup group;
  group.groupName = "user";
  set<TSentryGroup> groups;
  groups.insert(group);

  TAlterSentryRoleAddGroupsRequest group_request;
  TAlterSentryRoleAddGroupsResponse group_response;
  group_request.__set_requestorUserName("test-admin");
  group_request.__set_roleName("viewer");
  group_request.__set_groups(groups);

  TCreateSentryRoleRequest role_request;
  role_request.__set_requestorUserName("test-admin");
  role_request.__set_roleName("viewer");
  ASSERT_OK(sentry_client_->CreateRole(role_request));
  ASSERT_OK(sentry_client_->AlterRoleAddGroups(group_request, &group_response));

  // Attempt to alter role by a non admin user.
  TAlterSentryRoleGrantPrivilegeRequest privilege_request;
  TAlterSentryRoleGrantPrivilegeResponse privilege_response;
  privilege_request.__set_requestorUserName("joe-interloper");
  privilege_request.__set_roleName("viewer");
  TSentryPrivilege privilege;
  privilege.__set_serverName("server");
  privilege.__set_dbName("db");
  privilege.__set_tableName("table");
  privilege.__set_action("SELECT");
  privilege_request.__set_privilege(privilege);
  Status s = sentry_client_->AlterRoleGrantPrivilege(privilege_request, &privilege_response);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Attempt to alter a non-exist role.
  privilege_request.__set_requestorUserName("test-admin");
  privilege_request.__set_roleName("not-exist");
  s = sentry_client_->AlterRoleGrantPrivilege(privilege_request, &privilege_response);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  privilege_request.__set_roleName("viewer");
  ASSERT_OK(sentry_client_->AlterRoleGrantPrivilege(privilege_request, &privilege_response));
}

} // namespace sentry
} // namespace kudu
