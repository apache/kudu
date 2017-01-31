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
#include <thread>
#include <utility>
#include <vector>

#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/cert.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::vector;
using std::thread;
using strings::Substitute;

namespace kudu {
namespace security {
namespace ca {

class CertManagementTest : public KuduTest {
 public:
  CertManagementTest() :
      pem_dir_(GetTestPath("pem")),
      ca_cert_file_(JoinPathSegments(pem_dir_, "ca.cert.pem")),
      ca_private_key_file_(JoinPathSegments(pem_dir_, "ca.pkey.pem")),
      ca_public_key_file_(JoinPathSegments(pem_dir_, "ca.pubkey.pem")),
      ca_exp_cert_file_(JoinPathSegments(pem_dir_, "ca.exp.cert.pem")),
      ca_exp_private_key_file_(JoinPathSegments(pem_dir_, "ca.exp.pkey.pem")) {
  }

  void SetUp() override {
    ASSERT_OK(env_->CreateDir(pem_dir_));
    ASSERT_OK(WriteStringToFile(env_, kCaCert, ca_cert_file_));
    ASSERT_OK(WriteStringToFile(env_, kCaPrivateKey, ca_private_key_file_));
    ASSERT_OK(WriteStringToFile(env_, kCaPublicKey, ca_public_key_file_));
    ASSERT_OK(WriteStringToFile(env_, kCaExpiredCert, ca_exp_cert_file_));
    ASSERT_OK(WriteStringToFile(env_, kCaExpiredPrivateKey,
        ca_exp_private_key_file_));
  }

 protected:
  // Different sharing scenarios for request generator and signer.
  enum SharingType {
    DEDICATED,
    SHARED
  };

  // Different init patterns for request generator and signer.
  enum InitType {
    SINGLE_INIT,
    MULTIPLE_INIT
  };

  CertRequestGenerator::Config PrepareConfig(
      const string& uuid,
      const vector<string>& hostnames = {},
      const vector<string>& ips = {}) const {
    const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    const string comment = string(test_info->test_case_name()) + "." +
      test_info->name();
    return {
      "US",               // country
      "CA",               // state
      "San Francisco",    // locality
      "ASF",              // org
      "The Kudu Project", // unit
      uuid,               // uuid
      comment,            // comment
      hostnames,          // hostnames
      ips,                // ips
    };
  }

  // Run multiple threads which do certificate signing request generation
  // and signing those in parallel.  The 'is_shared' and 'multi_init' parameters
  // are to specify whether the threads use shared
  // CertRequestGenerator/CertSigner instances and whether every thread
  // initializes the shared instance it's using.
  void SignMultiThread(size_t num_threads, size_t iter_num,
                       SharingType sharing_type, InitType init_type) {
    const CertRequestGenerator::Config gen_config(
        PrepareConfig("757F3158-DCB5-4D6C-8054-5348BB4AEA07",
                      {"localhost"}, {"127.0.0.1"}));

    CertRequestGenerator gen_shared(gen_config);
    if (SINGLE_INIT == init_type) {
      ASSERT_OK(gen_shared.Init());
    }
    CertSigner signer_shared;
    if (SINGLE_INIT == init_type) {
      ASSERT_OK(signer_shared.InitFromFiles(ca_cert_file_, ca_private_key_file_));
    }

    vector<thread> threads;
    threads.reserve(num_threads);
    for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
      // 'thread_idx' is captured by value to avoid data races
      threads.emplace_back([&, thread_idx]() {
        for (size_t i = 0; i < iter_num; ++i) {
          CertRequestGenerator gen_local(gen_config);
          CertSigner signer_local;

          CertRequestGenerator& gen = (SHARED == sharing_type) ? gen_shared
                                                               : gen_local;
          CertSigner& signer = (SHARED == sharing_type) ? signer_shared
                                                        : signer_local;

          if (DEDICATED == sharing_type) {
            CHECK_OK(gen.Init());
          }
          const size_t sel = i % 4;
          const size_t key_bits = (sel + 1) * 512;
          PrivateKey key;
          CHECK_OK(GeneratePrivateKey(key_bits, &key));
          CertSignRequest req;
          CHECK_OK(gen.GenerateRequest(key, &req));
          if (DEDICATED == sharing_type) {
            CHECK_OK(signer.InitFromFiles(ca_cert_file_, ca_private_key_file_));
          }
          Cert cert;
          CHECK_OK(signer.Sign(req, &cert));
        }
      });
    }
    for (auto& e : threads) {
      e.join();
    }
  }

  const string pem_dir_;

  const string ca_cert_file_;
  const string ca_private_key_file_;
  const string ca_public_key_file_;

  const string ca_exp_cert_file_;
  const string ca_exp_private_key_file_;
};

// Check input/output of RSA private keys in PEM format.
TEST_F(CertManagementTest, RsaPrivateKeyInputOutputPEM) {
  PrivateKey key;
  ASSERT_OK(key.FromFile(ca_private_key_file_, DataFormat::PEM));
  string key_str;
  key.ToString(&key_str, DataFormat::PEM);
  RemoveExtraWhitespace(&key_str);

  string ca_input_key(kCaPrivateKey);
  RemoveExtraWhitespace(&ca_input_key);
  EXPECT_EQ(ca_input_key, key_str);
}

// Check input/output of RSA public keys in PEM format.
TEST_F(CertManagementTest, RsaPublicKeyInputOutputPEM) {
  PublicKey key;
  ASSERT_OK(key.FromFile(ca_public_key_file_, DataFormat::PEM));
  string str_key;
  key.ToString(&str_key, DataFormat::PEM);
  RemoveExtraWhitespace(&str_key);

  string ref_str_key(kCaPublicKey);
  RemoveExtraWhitespace(&ref_str_key);
  EXPECT_EQ(ref_str_key, str_key);
}

// Check extraction of the public part out from RSA private keys par.
TEST_F(CertManagementTest, RsaExtractPublicPartFromPrivateKey) {
  // Load the reference RSA private key.
  PrivateKey private_key;
  ASSERT_OK(private_key.FromString(kCaPrivateKey, DataFormat::PEM));

  PublicKey public_key;
  ASSERT_OK(private_key.GetPublicKey(&public_key));
  string str_public_key;
  public_key.ToString(&str_public_key, DataFormat::PEM);
  RemoveExtraWhitespace(&str_public_key);

  string ref_str_public_key(kCaPublicKey);
  RemoveExtraWhitespace(&ref_str_public_key);
  EXPECT_EQ(ref_str_public_key, str_public_key);
}

// Check input/output of the X509 certificates in PEM format.
TEST_F(CertManagementTest, CertInputOutputPEM) {
  Cert cert;
  ASSERT_OK(cert.FromFile(ca_cert_file_, DataFormat::PEM));
  string cert_str;
  cert.ToString(&cert_str, DataFormat::PEM);
  RemoveExtraWhitespace(&cert_str);

  string ca_input_cert(kCaCert);
  RemoveExtraWhitespace(&ca_input_cert);
  EXPECT_EQ(ca_input_cert, cert_str);
}

// Check for basic SAN-related constraints while initializing
// CertRequestGenerator objects.
TEST_F(CertManagementTest, RequestGeneratorSanConstraints) {
  const string kEntityUUID = "D94FBF10-6F40-4F9F-BC82-F96A1C4F2CFB";

  // No hostnames, nor IP addresses are given to populate X509v3 SAN extension.
  {
    const CertRequestGenerator::Config gen_config = PrepareConfig(kEntityUUID);
    CertRequestGenerator gen(gen_config);
    const Status s = gen.Init();
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "SAN: missing DNS names and IP addresses");
  }

  // An empty hostname
  {
    const CertRequestGenerator::Config gen_config =
        PrepareConfig(kEntityUUID, {"localhost", ""});
    CertRequestGenerator gen(gen_config);
    const Status s = gen.Init();
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "SAN: an empty hostname");
  }

  // An empty IP address
  {
    const CertRequestGenerator::Config gen_config =
        PrepareConfig(kEntityUUID, {}, {"127.0.0.1", ""});
    CertRequestGenerator gen(gen_config);
    const Status s = gen.Init();
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "SAN: an empty IP address");
  }

  // Missing UUID
  {
    const CertRequestGenerator::Config gen_config =
        PrepareConfig("", {"localhost"});
    CertRequestGenerator gen(gen_config);
    const Status s = gen.Init();
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "missing end-entity UUID/name");
  }
}

// Check for the basic functionality of the CertRequestGenerator class:
// check it's able to generate keys of expected number of bits and that it
// reports an error if trying to generate a key of unsupported number of bits.
TEST_F(CertManagementTest, RequestGeneratorBasics) {
  const CertRequestGenerator::Config gen_config =
      PrepareConfig("702C1C5E-CF02-4EDC-8883-07ECDEC8CE97", {"localhost"});

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  ASSERT_OK(GeneratePrivateKey(2048, &key));
  CertRequestGenerator gen(gen_config);
  ASSERT_OK(gen.Init());
  string key_str;
  key.ToString(&key_str, DataFormat::PEM);
  // Check for non-supported number of bits for the key.
  Status s = GeneratePrivateKey(7, &key);
  ASSERT_TRUE(s.IsRuntimeError());
}

// Check that CertSigner behaves in a predictable way if given non-expected
// content for the CA private key/certificate.
TEST_F(CertManagementTest, SignerInitWithWrongFiles) {
  // Providing files which guaranteed to exists, but do not contain valid data.
  // This is to make sure the init handles that situation correctly and
  // does not choke on the wrong input data.
  CertSigner signer;
  ASSERT_FALSE(signer.InitFromFiles("/bin/sh", "/bin/cat").ok());
}

// Check that CertSigner behaves in a predictable way if given non-matching
// CA private key and certificate.
TEST_F(CertManagementTest, SignerInitWithMismatchedCertAndKey) {
  {
    CertSigner signer;
    Status s = signer.InitFromFiles(ca_cert_file_, ca_exp_private_key_file_);
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsRuntimeError()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "CA certificate and private key do not match");
  }
  {
    CertSigner signer;
    Status s = signer.InitFromFiles(ca_exp_cert_file_, ca_private_key_file_);
    const string err_msg = s.ToString();
    ASSERT_TRUE(s.IsRuntimeError()) << err_msg;
    ASSERT_STR_CONTAINS(err_msg, "CA certificate and private key do not match");
  }
}

// Check how CertSigner behaves if given expired CA certificate
// and corresponding private key.
TEST_F(CertManagementTest, SignerInitWithExpiredCert) {
  const CertRequestGenerator::Config gen_config(
      PrepareConfig("F4466090-BBF8-4042-B72F-BB257500C45A", {"localhost"}));
  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(2048, &key));
  CertRequestGenerator gen(gen_config);
  ASSERT_OK(gen.Init());
  CertSignRequest req;
  ASSERT_OK(gen.GenerateRequest(key, &req));
  CertSigner signer;
  // Even if the certificate is expired, the signer should initialize OK.
  ASSERT_OK(signer.InitFromFiles(ca_exp_cert_file_, ca_exp_private_key_file_));
  Cert cert;
  // Signer works fine even with expired CA certificate.
  ASSERT_OK(signer.Sign(req, &cert));
}

// Generate X509 CSR and issues corresponding certificate: everything is done
// in a single-threaded fashion.
TEST_F(CertManagementTest, SignCert) {
  const CertRequestGenerator::Config gen_config(
      PrepareConfig("904A97F9-545A-4746-86D1-85D433FF3F9C",
                    {"localhost"}, {"127.0.0.1", "127.0.10.20"}));
  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(2048, &key));
  CertRequestGenerator gen(gen_config);
  ASSERT_OK(gen.Init());
  CertSignRequest req;
  ASSERT_OK(gen.GenerateRequest(key, &req));
  CertSigner signer;
  ASSERT_OK(signer.InitFromFiles(ca_cert_file_, ca_private_key_file_));
  Cert cert;
  ASSERT_OK(signer.Sign(req, &cert));
}

// Generate X509 CA CSR and sign the result certificate.
TEST_F(CertManagementTest, SignCaCert) {
  const CertRequestGenerator::Config gen_config(
      PrepareConfig("8C084CF6-A30B-4F5B-9673-A73E62E29A9D"));
  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(2048, &key));
  CaCertRequestGenerator gen(gen_config);
  ASSERT_OK(gen.Init());
  CertSignRequest req;
  ASSERT_OK(gen.GenerateRequest(key, &req));
  CertSigner signer;
  ASSERT_OK(signer.InitFromFiles(ca_cert_file_, ca_private_key_file_));
  Cert cert;
  ASSERT_OK(signer.Sign(req, &cert));
}

// Test the creation and use of a CA which uses a self-signed CA cert
// generated on the fly.
TEST_F(CertManagementTest, TestSelfSignedCA) {
  shared_ptr<PrivateKey> ca_key;
  shared_ptr<Cert> ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  // Create a key for the tablet server.
  auto ts_key = std::make_shared<PrivateKey>();
  ASSERT_OK(GeneratePrivateKey(2048, ts_key.get()));

  // Prepare a CSR for a tablet server that wants signing.
  CertSignRequest ts_csr;
  {
    CertRequestGenerator gen(PrepareConfig(
        "some-tablet-server",
        {"localhost"}, {"127.0.0.1", "127.0.10.20"}));
    ASSERT_OK(gen.Init());
    ASSERT_OK(gen.GenerateRequest(*ts_key, &ts_csr));
  }

  // Sign it using the self-signed CA.
  Cert ts_cert;
  {
    CertSigner signer;
    ASSERT_OK(signer.Init(ca_cert, ca_key));
    ASSERT_OK(signer.Sign(ts_csr, &ts_cert));
  }
}

// Check the transformation chains for X509 CSRs:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_F(CertManagementTest, X509CsrFromAndToString) {
  static const DataFormat kFormats[] = { DataFormat::PEM, DataFormat::DER };

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(PrepareConfig(
      "4C931ADC-3945-4E05-8DB2-447327BF8F62", {"localhost"}));
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
  CertRequestGenerator gen(PrepareConfig(
      "86F676E9-4E77-4DDC-B15C-596E74B03D90", {"localhost"}));
  ASSERT_OK(gen.Init());
  CertSignRequest req;
  ASSERT_OK(gen.GenerateRequest(key, &req));

  CertSigner signer;
  ASSERT_OK(signer.InitFromFiles(ca_cert_file_, ca_private_key_file_));
  Cert cert_ref;
  ASSERT_OK(signer.Sign(req, &cert_ref));

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

// Generate CSR and issue corresponding certificate in a multi-threaded fashion:
// every thread uses its own instances of CertRequestGenerator and CertSigner,
// which were initialized earlier (i.e. those threads do not call Init()).
TEST_F(CertManagementTest, SignMultiThreadExclusiveSingleInit) {
  ASSERT_NO_FATAL_FAILURE(SignMultiThread(32, 16, DEDICATED, SINGLE_INIT));
}

// Generate CSR and issue corresponding certificate in a multi-threaded fashion:
// every thread uses its own instances of CertRequestGenerator and CertSigner,
// and every thread initializes those shared objects by itself.
TEST_F(CertManagementTest, SignMultiThreadExclusiveMultiInit) {
  ASSERT_NO_FATAL_FAILURE(SignMultiThread(16, 32, DEDICATED, MULTIPLE_INIT));
}

// Generate CSR and issue corresponding certificate in a multi-thread fashion:
// all threads use shared instances of CertRequestGenerator and CertSigner,
// which were initialized earlier (i.e. those threads do not call Init()).
TEST_F(CertManagementTest, SignMultiThreadSharedSingleInit) {
  ASSERT_NO_FATAL_FAILURE(SignMultiThread(32, 16, SHARED, SINGLE_INIT));
}

} // namespace ca
} // namespace security
} // namespace kudu
