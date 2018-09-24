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

#include <map>
#include <string>

#include <gtest/gtest.h>

#include "kudu/rpc/sasl_common.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {
namespace sentry {

class SentryClientTest : public KuduTest,
                         public ::testing::WithParamInterface<bool> {
 public:
  bool KerberosEnabled() const {
    return GetParam();
  }
};
INSTANTIATE_TEST_CASE_P(KerberosEnabled, SentryClientTest, ::testing::Bool());

TEST_P(SentryClientTest, TestMiniSentryLifecycle) {
  MiniSentry mini_sentry;
  ASSERT_OK(mini_sentry.Start());

  ASSERT_OK(mini_sentry.Stop());
  ASSERT_OK(mini_sentry.Start());

  ASSERT_OK(mini_sentry.Pause());
  ASSERT_OK(mini_sentry.Resume());
}

// Basic functionality test of the Sentry client. The goal is not an exhaustive
// test of Sentry's role handling, but instead verification that the client can
// communicate with the Sentry service, and errors are converted to Status
// instances.
TEST_P(SentryClientTest, TestCreateDropRole) {
  MiniKdc kdc;
  MiniSentry sentry;
  thrift::ClientOptions sentry_client_opts;

  if (KerberosEnabled()) {
    ASSERT_OK(kdc.Start());

    string spn = "sentry/127.0.0.1@KRBTEST.COM";
    string ktpath;
    ASSERT_OK(kdc.CreateServiceKeytab("sentry/127.0.0.1", &ktpath));

    ASSERT_OK(rpc::SaslInit());
    sentry.EnableKerberos(kdc.GetEnvVars()["KRB5_CONFIG"], spn, ktpath);

    ASSERT_OK(kdc.CreateUserPrincipal("kudu"));
    ASSERT_OK(kdc.Kinit("kudu"));
    ASSERT_OK(kdc.SetKrb5Environment());
    sentry_client_opts.enable_kerberos = true;
    sentry_client_opts.service_principal = "sentry";
  }
  ASSERT_OK(sentry.Start());

  SentryClient client(sentry.address(), sentry_client_opts);
  ASSERT_OK(client.Start());

  { // Create a role
    ::sentry::TCreateSentryRoleRequest req;
    req.requestorUserName = "test-admin";
    req.roleName = "viewer";
    ASSERT_OK(client.CreateRole(req));

    // Attempt to create the role again.
    Status s = client.CreateRole(req);
    ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  }

  { // Attempt to create a role as a non-admin user.
    ::sentry::TCreateSentryRoleRequest req;
    req.requestorUserName = "joe-interloper";
    req.roleName = "fuzz";
    Status s = client.CreateRole(req);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  { // Attempt to drop the role as a non-admin user.
    ::sentry::TDropSentryRoleRequest req;
    req.requestorUserName = "joe-interloper";
    req.roleName = "viewer";
    Status s = client.DropRole(req);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  { // Drop the role
    ::sentry::TDropSentryRoleRequest req;
    req.requestorUserName = "test-admin";
    req.roleName = "viewer";
    ASSERT_OK(client.DropRole(req));

    // Attempt to drop the role again.
    Status s = client.DropRole(req);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
}
} // namespace sentry
} // namespace kudu
