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

#include <utility>

#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::pair;

namespace kudu {
namespace security {

// Test for various certificate-related functionality in the security library.
// These do not cover CA certificate mananagement part; check
// cert_management-test.cc for those.
class CertTest : public KuduTest {
 public:
  void SetUp() override {
    ASSERT_OK(ca_cert_.FromString(kCaCert, DataFormat::PEM));
    ASSERT_OK(ca_private_key_.FromString(kCaPrivateKey, DataFormat::PEM));
    ASSERT_OK(ca_public_key_.FromString(kCaPublicKey, DataFormat::PEM));
    ASSERT_OK(ca_exp_cert_.FromString(kCaExpiredCert, DataFormat::PEM));
    ASSERT_OK(ca_exp_private_key_.FromString(kCaExpiredPrivateKey,
                                             DataFormat::PEM));
  }

 protected:
  Cert ca_cert_;
  PrivateKey ca_private_key_;
  PublicKey ca_public_key_;

  Cert ca_exp_cert_;
  PrivateKey ca_exp_private_key_;
};

// Check X509 certificate/private key matching: match cases.
TEST_F(CertTest, CertMatchesRsaPrivateKey) {
  const pair<const Cert*, const PrivateKey*> cases[] = {
    { &ca_cert_,      &ca_private_key_      },
    { &ca_exp_cert_,  &ca_exp_private_key_  },
  };
  for (const auto& e : cases) {
    EXPECT_OK(e.first->CheckKeyMatch(*e.second));
  }
}

// Check X509 certificate/private key matching: mismatch cases.
TEST_F(CertTest, CertMismatchesRsaPrivateKey) {
  const pair<const Cert*, const PrivateKey*> cases[] = {
    { &ca_cert_,      &ca_exp_private_key_  },
    { &ca_exp_cert_,  &ca_private_key_      },
  };
  for (const auto& e : cases) {
    const Status s = e.first->CheckKeyMatch(*e.second);
    EXPECT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "certificate does not match private key");
  }
}

} // namespace security
} // namespace kudu
