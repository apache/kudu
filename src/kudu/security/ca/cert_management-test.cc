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

#include "kudu/security/ca/cert_management.h"

#include <memory>
#include <utility>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/cert.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace security {
namespace ca {

class CertManagementTest : public KuduTest {
 public:
  void SetUp() override {
    ASSERT_OK(ca_cert_.FromString(kCaCert, DataFormat::PEM));
    ASSERT_OK(ca_private_key_.FromString(kCaPrivateKey, DataFormat::PEM));
    ASSERT_OK(ca_public_key_.FromString(kCaPublicKey, DataFormat::PEM));
    ASSERT_OK(ca_exp_cert_.FromString(kCaExpiredCert, DataFormat::PEM));
    ASSERT_OK(ca_exp_private_key_.FromString(kCaExpiredPrivateKey, DataFormat::PEM));
    // Sanity checks.
    ASSERT_OK(ca_cert_.CheckKeyMatch(ca_private_key_));
    ASSERT_OK(ca_exp_cert_.CheckKeyMatch(ca_exp_private_key_));
  }

 protected:
  CertRequestGenerator::Config PrepareConfig(
      const string& common_name) {
    return { common_name };
  }

  // Create a new private key in 'key' and return a CSR associated with that
  // key.
  template<class CSRGen = CertRequestGenerator>
  CertSignRequest PrepareTestCSR(CertRequestGenerator::Config config,
                                 PrivateKey* key) {
    CHECK_OK(GeneratePrivateKey(512, key));
    CSRGen gen(std::move(config));
    CHECK_OK(gen.Init());
    CertSignRequest req;
    CHECK_OK(gen.GenerateRequest(*key, &req));
    return req;
  }

  Cert ca_cert_;
  PrivateKey ca_private_key_;
  PublicKey ca_public_key_;

  Cert ca_exp_cert_;
  PrivateKey ca_exp_private_key_;
};

// Check for basic constraints while initializing
// CertRequestGenerator objects.
TEST_F(CertManagementTest, RequestGeneratorConstraints) {
  // Missing CN
  {
    const CertRequestGenerator::Config gen_config = PrepareConfig("");
    CertRequestGenerator gen(gen_config);
    const Status s = gen.Init();
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "missing end-entity CN");
  }
}

// Check for the basic functionality of the CertRequestGenerator class:
// check it's able to generate keys of expected number of bits and that it
// reports an error if trying to generate a key of unsupported number of bits.
TEST_F(CertManagementTest, RequestGeneratorBasics) {
  const CertRequestGenerator::Config gen_config = PrepareConfig("my-cn");

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(gen_config);
  ASSERT_OK(gen.Init());
  string key_str;
  ASSERT_OK(key.ToString(&key_str, DataFormat::PEM));
  // Check for non-supported number of bits for the key.
  Status s = GeneratePrivateKey(7, &key);
  ASSERT_TRUE(s.IsRuntimeError());
}

// Check that CertSigner behaves in a predictable way if given non-matching
// CA private key and certificate.
TEST_F(CertManagementTest, SignerInitWithMismatchedCertAndKey) {
  PrivateKey key;
  const auto& csr = PrepareTestCSR(PrepareConfig("test-cn"), &key);
  {
    Cert cert;
    Status s = CertSigner(&ca_cert_, &ca_exp_private_key_)
        .Sign(csr, &cert);

    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsRuntimeError()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "certificate does not match private key");
  }
  {
    Cert cert;
    Status s = CertSigner(&ca_exp_cert_, &ca_private_key_)
        .Sign(csr, &cert);
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsRuntimeError()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "certificate does not match private key");
  }
}

// Check how CertSigner behaves if given expired CA certificate
// and corresponding private key.
TEST_F(CertManagementTest, SignerInitWithExpiredCert) {
  const CertRequestGenerator::Config gen_config = PrepareConfig("test-cn");
  PrivateKey key;
  CertSignRequest req = PrepareTestCSR(gen_config, &key);

  // Signer works fine even with expired CA certificate.
  Cert cert;
  ASSERT_OK(CertSigner(&ca_exp_cert_, &ca_exp_private_key_).Sign(req, &cert));
  ASSERT_OK(cert.CheckKeyMatch(key));
}

// Generate X509 CSR and issues corresponding certificate.
TEST_F(CertManagementTest, SignCert) {
  CertRequestGenerator::Config gen_config;
  gen_config.cn = "test-cn";
  gen_config.user_id = "test-uid";
  gen_config.kerberos_principal = "kudu/foo.bar.com@bar.com";
  PrivateKey key;
  const auto& csr = PrepareTestCSR(gen_config, &key);
  Cert cert;
  ASSERT_OK(CertSigner(&ca_cert_, &ca_private_key_).Sign(csr, &cert));
  ASSERT_OK(cert.CheckKeyMatch(key));

  EXPECT_EQ("C = US, ST = CA, O = MyCompany, CN = MyName, emailAddress = my@email.com",
            cert.IssuerName());
  EXPECT_EQ("CN = test-cn, UID = test-uid", cert.SubjectName());
  EXPECT_EQ(gen_config.user_id, *cert.UserId());
  EXPECT_EQ(gen_config.kerberos_principal, *cert.KuduKerberosPrincipal());
}

// Generate X509 CA CSR and sign the result certificate.
TEST_F(CertManagementTest, SignCaCert) {
  const CertRequestGenerator::Config gen_config(
      PrepareConfig("8C084CF6-A30B-4F5B-9673-A73E62E29A9D"));
  PrivateKey key;
  const auto& csr = PrepareTestCSR<CaCertRequestGenerator>(gen_config, &key);
  Cert cert;
  ASSERT_OK(CertSigner(&ca_cert_, &ca_private_key_).Sign(csr, &cert));
  ASSERT_OK(cert.CheckKeyMatch(key));
}

// Test the creation and use of a CA which uses a self-signed CA cert
// generated on the fly.
TEST_F(CertManagementTest, TestSelfSignedCA) {
  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  // Create a key and CSR for the tablet server.
  const auto& config = PrepareConfig("some-tablet-server");
  PrivateKey ts_key;
  CertSignRequest ts_csr = PrepareTestCSR(config, &ts_key);

  // Sign it using the self-signed CA.
  Cert ts_cert;
  ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(ts_csr, &ts_cert));
  ASSERT_OK(ts_cert.CheckKeyMatch(ts_key));
}

// Check the transformation chains for X509 CSRs:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_F(CertManagementTest, X509CsrFromAndToString) {
  static const DataFormat kFormats[] = { DataFormat::PEM, DataFormat::DER };

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(PrepareConfig("test-cn"));
  ASSERT_OK(gen.Init());
  CertSignRequest req_ref;
  ASSERT_OK(gen.GenerateRequest(key, &req_ref));

  for (auto format : kFormats) {
    SCOPED_TRACE(Substitute("X509 CSR format: $0", DataFormatToString(format)));
    string str_req_ref;
    ASSERT_OK(req_ref.ToString(&str_req_ref, format));
    CertSignRequest req;
    ASSERT_OK(req.FromString(str_req_ref, format));
    string str_req;
    ASSERT_OK(req.ToString(&str_req, format));
    ASSERT_EQ(str_req_ref, str_req);
  }
}

// Check the transformation chains for X509 certs:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_F(CertManagementTest, X509FromAndToString) {
  static const DataFormat kFormats[] = { DataFormat::PEM, DataFormat::DER };

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(PrepareConfig("test-cn"));
  ASSERT_OK(gen.Init());
  CertSignRequest req;
  ASSERT_OK(gen.GenerateRequest(key, &req));

  Cert cert_ref;
  ASSERT_OK(CertSigner(&ca_cert_, &ca_private_key_)
            .Sign(req, &cert_ref));

  for (auto format : kFormats) {
    SCOPED_TRACE(Substitute("X509 format: $0", DataFormatToString(format)));
    string str_cert_ref;
    ASSERT_OK(cert_ref.ToString(&str_cert_ref, format));
    Cert cert;
    ASSERT_OK(cert.FromString(str_cert_ref, format));
    string str_cert;
    ASSERT_OK(cert.ToString(&str_cert, format));
    ASSERT_EQ(str_cert_ref, str_cert);
  }
}

} // namespace ca
} // namespace security
} // namespace kudu
