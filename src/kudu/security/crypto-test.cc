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

#include <cstring>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/url-coding.h"

using std::pair;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace security {

// Test for various crypto-related functionality in the security library.
class CryptoTest : public KuduTest {
 public:
  CryptoTest() :
      pem_dir_(GetTestPath("pem")),
      private_key_file_(JoinPathSegments(pem_dir_, "private_key.pem")),
      public_key_file_(JoinPathSegments(pem_dir_, "public_key.pem")),
      corrupted_private_key_file_(JoinPathSegments(pem_dir_,
          "corrupted.private_key.pem")),
      corrupted_public_key_file_(JoinPathSegments(pem_dir_,
          "corrupted.public_key.pem")) {
  }

  void SetUp() override {
    ASSERT_OK(env_->CreateDir(pem_dir_));
    ASSERT_OK(WriteStringToFile(env_, kCaPrivateKey, private_key_file_));
    ASSERT_OK(WriteStringToFile(env_, kCaPublicKey, public_key_file_));
    ASSERT_OK(WriteStringToFile(env_,
        string(kCaPrivateKey, strlen(kCaPrivateKey) / 2),
        corrupted_private_key_file_));
    ASSERT_OK(WriteStringToFile(env_,
        string(kCaPublicKey, strlen(kCaPublicKey) / 2),
        corrupted_public_key_file_));
  }

 protected:
  template<typename Key>
  void CheckToAndFromString(const Key& key_ref, DataFormat format) {
    SCOPED_TRACE(Substitute("ToAndFromString for $0 format",
                            DataFormatToString(format)));
    string key_ref_str;
    ASSERT_OK(key_ref.ToString(&key_ref_str, format));
    Key key;
    ASSERT_OK(key.FromString(key_ref_str, format));
    string key_str;
    ASSERT_OK(key.ToString(&key_str, format));
    ASSERT_EQ(key_ref_str, key_str);
  }

  const string pem_dir_;

  const string private_key_file_;
  const string public_key_file_;
  const string corrupted_private_key_file_;
  const string corrupted_public_key_file_;
};

// Check input/output of RSA private keys in PEM format.
TEST_F(CryptoTest, RsaPrivateKeyInputOutputPEM) {
  PrivateKey key;
  ASSERT_OK(key.FromFile(private_key_file_, DataFormat::PEM));
  string key_str;
  key.ToString(&key_str, DataFormat::PEM);
  RemoveExtraWhitespace(&key_str);

  string ref_key_str(kCaPrivateKey);
  RemoveExtraWhitespace(&ref_key_str);
  EXPECT_EQ(ref_key_str, key_str);
}

// Check input of corrupted RSA private keys in PEM format.
TEST_F(CryptoTest, CorruptedRsaPrivateKeyInputPEM) {
  static const string kFiles[] = {
      corrupted_private_key_file_,
      public_key_file_,
      corrupted_public_key_file_,
      "/bin/sh"
  };
  for (const auto& file : kFiles) {
    PrivateKey key;
    const Status s = key.FromFile(file, DataFormat::PEM);
    EXPECT_TRUE(s.IsRuntimeError()) << s.ToString();
  }
}

// Check input/output of RSA public keys in PEM format.
TEST_F(CryptoTest, RsaPublicKeyInputOutputPEM) {
  PublicKey key;
  ASSERT_OK(key.FromFile(public_key_file_, DataFormat::PEM));
  string key_str;
  key.ToString(&key_str, DataFormat::PEM);
  RemoveExtraWhitespace(&key_str);

  string ref_key_str(kCaPublicKey);
  RemoveExtraWhitespace(&ref_key_str);
  EXPECT_EQ(ref_key_str, key_str);
}

// Check input of corrupted RSA public keys in PEM format.
TEST_F(CryptoTest, CorruptedRsaPublicKeyInputPEM) {
  static const string kFiles[] = {
      corrupted_public_key_file_,
      private_key_file_,
      corrupted_private_key_file_,
      "/bin/sh"
  };
  for (const auto& file : kFiles) {
    PublicKey key;
    const Status s = key.FromFile(file, DataFormat::PEM);
    EXPECT_TRUE(s.IsRuntimeError()) << s.ToString();
  }
}

// Check extraction of the public part from RSA private keys par.
TEST_F(CryptoTest, RsaExtractPublicPartFromPrivateKey) {
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

class CryptoKeySerDesTest :
    public CryptoTest,
    public ::testing::WithParamInterface<DataFormat> {
};

// Check the transformation chains for RSA public/private keys:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_P(CryptoKeySerDesTest, ToAndFromString) {
  const auto format = GetParam();

  // Generate private RSA key.
  PrivateKey private_key;
  ASSERT_OK(GeneratePrivateKey(2048, &private_key));
  NO_FATALS(CheckToAndFromString(private_key, format));

  // Extract public part of the key.
  PublicKey public_key;
  ASSERT_OK(private_key.GetPublicKey(&public_key));
  NO_FATALS(CheckToAndFromString(public_key, format));
}

INSTANTIATE_TEST_CASE_P(
    DataFormats, CryptoKeySerDesTest,
    ::testing::Values(DataFormat::DER, DataFormat::PEM));

// Check making crypto signatures against the reference data.
TEST_F(CryptoTest, MakeVerifySignatureRef) {
  static const vector<pair<string, string>> kRefElements = {
    { kDataTiny,    kSignatureTinySHA512 },
    { kDataShort,   kSignatureShortSHA512 },
    { kDataLong,    kSignatureLongSHA512 },
  };

  // Load the reference RSA private key.
  PrivateKey private_key;
  ASSERT_OK(private_key.FromString(kCaPrivateKey, DataFormat::PEM));

  // Load the reference RSA public key.
  PublicKey public_key;
  ASSERT_OK(public_key.FromString(kCaPublicKey, DataFormat::PEM));

  for (const auto& e : kRefElements) {
    string sig;
    ASSERT_OK(private_key.MakeSignature(DigestType::SHA512, e.first, &sig));

    // Ad-hoc verification: check the produced signature matches the reference.
    string sig_base64;
    Base64Encode(sig, &sig_base64);
    EXPECT_EQ(e.second, sig_base64);

    // Verify the signature cryptographically.
    EXPECT_OK(public_key.VerifySignature(DigestType::SHA512, e.first, sig));
  }
}

TEST_F(CryptoTest, VerifySignatureWrongData) {
  static const vector<string> kRefSignatures = {
    kSignatureTinySHA512,
    kSignatureShortSHA512,
    kSignatureLongSHA512,
  };

  // Load the reference RSA public key.
  PublicKey key;
  ASSERT_OK(key.FromString(kCaPublicKey, DataFormat::PEM));

  for (const auto& e : kRefSignatures) {
    string signature;
    ASSERT_TRUE(Base64Decode(e, &signature));
    Status s = key.VerifySignature(DigestType::SHA512,
        "non-expected-data", signature);
    EXPECT_TRUE(s.IsCorruption()) << s.ToString();
  }
}


} // namespace security
} // namespace kudu
