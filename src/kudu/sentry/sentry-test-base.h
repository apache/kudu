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

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace sentry {

class SentryTestBase : public KuduTest,
                       public ::testing::WithParamInterface<bool> {
 public:

  void SetUp() override {
    KuduTest::SetUp();
    thrift::ClientOptions sentry_client_opts;
    sentry_.reset(new MiniSentry());
    if (kerberos_enabled_) {
      kdc_.reset(new MiniKdc(MiniKdcOptions()));
      ASSERT_OK(kdc_->Start());

      // Create a service principal for the Sentry, and configure it to use it.
      std::string spn = strings::Substitute("sentry/$0", sentry_->address().host());
      std::string ktpath;
      ASSERT_OK(kdc_->CreateServiceKeytab(spn, &ktpath));

      sentry_->EnableKerberos(kdc_->GetEnvVars()["KRB5_CONFIG"],
                              strings::Substitute("$0@KRBTEST.COM", spn),
                              ktpath);

      ASSERT_OK(rpc::SaslInit());
      // Create a principal 'kudu' for the SentryAuthzProvider, and configure it
      // to use it.
      ASSERT_OK(kdc_->CreateUserPrincipal("kudu"));
      ASSERT_OK(kdc_->Kinit("kudu"));
      ASSERT_OK(kdc_->SetKrb5Environment());
      sentry_client_opts.enable_kerberos = true;
      sentry_client_opts.service_principal = "sentry";
    }

    ASSERT_OK(sentry_->Start());
    sentry_client_.reset(new SentryClient(sentry_->address(), sentry_client_opts));
    ASSERT_OK(sentry_client_->Start());
  }

  void TearDown() override {
    if (sentry_client_) {
      ASSERT_OK(sentry_client_->Stop());
    }
    ASSERT_OK(sentry_->Stop());
    KuduTest::TearDown();
  }

 protected:
  const bool kerberos_enabled_ = GetParam();
  std::unique_ptr<MiniKdc> kdc_;
  std::unique_ptr<MiniSentry> sentry_;
  std::unique_ptr<SentryClient> sentry_client_;
};

} // namespace sentry
} // namespace kudu
