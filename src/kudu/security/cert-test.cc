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

#include <thread>
#include <utility>
#include <vector>

#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>

#include "kudu/gutil/strings/strip.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/barrier.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::pair;
using std::string;
using std::thread;
using std::vector;

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
    // Sanity checks.
    ASSERT_OK(ca_cert_.CheckKeyMatch(ca_private_key_));
    ASSERT_OK(ca_exp_cert_.CheckKeyMatch(ca_exp_private_key_));
  }

 protected:
  Cert ca_cert_;
  PrivateKey ca_private_key_;
  PublicKey ca_public_key_;

  Cert ca_exp_cert_;
  PrivateKey ca_exp_private_key_;
};

// Regression test to make sure that GetKuduKerberosPrincipalOidNid is thread
// safe. OpenSSL 1.0.0's OBJ_create method is not thread safe.
TEST_F(CertTest, GetKuduKerberosPrincipalOidNidConcurrent) {
  int kConcurrency = 16;
  Barrier barrier(kConcurrency);

  vector<thread> threads;
  for (int i = 0; i < kConcurrency; i++) {
    threads.emplace_back([&] () {
        barrier.Wait();
        CHECK_NE(NID_undef, GetKuduKerberosPrincipalOidNid());
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

// Check input/output of the X509 certificates in PEM format.
TEST_F(CertTest, CertInputOutputPEM) {
  const Cert& cert = ca_cert_;
  string cert_str;
  ASSERT_OK(cert.ToString(&cert_str, DataFormat::PEM));
  RemoveExtraWhitespace(&cert_str);

  string ca_input_cert(kCaCert);
  RemoveExtraWhitespace(&ca_input_cert);
  EXPECT_EQ(ca_input_cert, cert_str);
}

// Check that Cert behaves in a predictable way if given invalid PEM data.
TEST_F(CertTest, CertInvalidInput) {
  // Providing files which guaranteed to exists, but do not contain valid data.
  // This is to make sure the init handles that situation correctly and
  // does not choke on the wrong input data.
  Cert c;
  ASSERT_FALSE(c.FromFile("/bin/sh", DataFormat::PEM).ok());
}

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

TEST_F(CertTest, TestGetKuduSpecificFieldsWhenMissing) {
  EXPECT_EQ(boost::none, ca_cert_.UserId());
  EXPECT_EQ(boost::none, ca_cert_.KuduKerberosPrincipal());
}

TEST_F(CertTest, DnsHostnameInSanField) {
  const string hostname_foo_bar = "foo.bar.com";
  const string hostname_mega_giga = "mega.giga.io";
  const string hostname_too_long =
      "toooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo."
      "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "ng.hostname.io";

  Cert cert;
  ASSERT_OK(cert.FromString(kCertDnsHostnamesInSan, DataFormat::PEM));

  EXPECT_EQ("C = US, ST = CA, O = MyCompany, CN = MyName, emailAddress = my@email.com",
            cert.IssuerName());
  vector<string> hostnames = cert.Hostnames();
  ASSERT_EQ(3, hostnames.size());
  EXPECT_EQ(hostname_mega_giga, hostnames[0]);
  EXPECT_EQ(hostname_foo_bar, hostnames[1]);
  EXPECT_EQ(hostname_too_long, hostnames[2]);
}

} // namespace security
} // namespace kudu
