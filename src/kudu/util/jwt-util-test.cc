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
//
// Copied from Impala and adapted to Kudu.

#include "kudu/util/jwt-util.h"

#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/defaults.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/jwt-util-internal.h"
#include "kudu/util/jwt_test_certs.h"
#include "kudu/util/mini_oidc.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/web_callback_registry.h"

namespace kudu {

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using kudu::MiniOidc;

/// Utility class for creating a file that will be automatically deleted upon test
/// completion.
class TempTestDataFile {
 public:
  // Creates a temporary file with the specified contents.
  explicit TempTestDataFile(const std::string& contents);

  /// Returns the absolute path to the file.
  const std::string& Filename() const { return name_; }

 private:
  std::string name_;
  std::unique_ptr<WritableFile> tmp_file_;
};

TempTestDataFile::TempTestDataFile(const std::string& contents)
  : name_("/tmp/jwks_XXXXXX") {
  string created_filename;
  WritableFileOptions opts;
  opts.is_sensitive = false;
  Status status;
  status = Env::Default()->NewTempWritableFile(opts, &name_[0], &created_filename, &tmp_file_);
  if (!status.ok()) {
    std::cerr << Substitute("Error creating temp file: $0", created_filename);
  }

  status = WriteStringToFile(Env::Default(), contents, created_filename);
  if (!status.ok()) {
    std::cerr << Substitute("Error writing contents to temp file: $0", created_filename);
  }

  name_ = created_filename;
}

TEST(JwtUtilTest, LoadJwksFile) {
  // Load JWKS from file.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_FALSE(jwks->IsEmpty());
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ("rs256", key1->get_algorithm());
  ASSERT_EQ(kRsaPubKeyPem, key1->get_key());

  std::string non_existing_kid("public:c424b67b-fe28-45d7-b015-f79da5-xxxxx");
  const JWTPublicKey* key3 = jwks->LookupRSAPublicKey(non_existing_kid);
  ASSERT_FALSE(key3 != nullptr);
}

TEST(JwtUtilTest, LoadInvalidJwksFiles) {
  // JWK without kid.
  std::unique_ptr<TempTestDataFile> jwks_file(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "    }"
      "  ]"
      "}"));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "parsing key #0")
      << " Actual error: " << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "'kid' property is required")
      << "actual error: " << status.ToString();

  // Invalid JSON format, missing "]" and "}".
  jwks_file.reset(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"kid\": \"public:c424b67b-fe28-45d7-b015-f79da50b5b21\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "}"));
  status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "Missing a comma or ']' after an array element")
      << " Actual error: " << status.ToString();

  // JWKS with empty key id.
  jwks_file.reset(new TempTestDataFile(
      Substitute(kJwksRsaFileFormat, "", "RS256", kRsaPubKeyJwkN, kRsaPubKeyJwkE,
          "", "RS256", kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE)));
  status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "parsing key #0")
      << " Actual error: " << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "'kid' property must be a non-empty string")
      << " Actual error: " << status.ToString();

  // JWKS with empty key value.
  jwks_file.reset(new TempTestDataFile(
      Substitute(kJwksRsaFileFormat, kKid1, "RS256", "", "", kKid2, "RS256", "", "")));
  status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "parsing key #0")
      << " Actual error: " << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "'n' and 'e' properties must be a non-empty string")
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtHS256) {
  // Cryptographic algorithm: HS256.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "Yx57JSBzhGFDgDj19CabRpH/+kiaKqI6UZI6lDunQKw=";
  TempTestDataFile jwks_file(
      Substitute(kJwksHsFileFormat, kKid1, "HS256", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS256")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("kudu"))
                   .sign(jwt::algorithm::hs256(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtHS384) {
  // Cryptographic algorithm: HS384.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret =
      "TlqmKRc2PNQJXTC3Go7eAadwPxA7x9byyXCi5I8tSvxrE77tYbuF5pfZAyswrkou";
  TempTestDataFile jwks_file(
      Substitute(kJwksHsFileFormat, kKid1, "HS384", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS384")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("kudu"))
                   .sign(jwt::algorithm::hs384(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtHS512) {
  // Cryptographic algorithm: HS512.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "ywc6DN7+iRw1E5HOqzvrsYodykSLFutT28KN3bJnLZcZpPCNjn0b6gbMfXPcxeY"
                         "VyuWWGDxh6gCDwPMejbuEEg==";
  TempTestDataFile jwks_file(
      Substitute(kJwksHsFileFormat, kKid1, "HS512", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS512")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("kudu"))
                   .sign(jwt::algorithm::hs512(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtRS256) {
  // Cryptographic algorithm: RS256.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsaPubKeyPem, key1->get_key());

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYjViMjEiLCJ0"
      "eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoia3VkdSJ9.OcAO1KFnYMyVyMXWqeWKlvZIDIYmeQYZ"
      "hMYcjOuGi5KKuBF5J7IlSl14EM6EhswQJ54pP8EPhMyHHNqncUOPt-QN9foS39aA5XpNPqOOWShQvZLQweEsogjfab66"
      "gWPO7baXGg8npqMBxpnvM5mjz4TIg6kwT4R_9p1NBGYmU5DQS6_jb7OfpMxY8bezKwL_iJB9yPgTlgZA5IJ0DPkIydcQ"
      "ejz3ycLy-75G8GWK78WgtOq2ejwpCsrPo3QlaqQH1reDPBit_2xme8ypwgGztc3Nss1ZF8g5U69WTdhP2Dy5k7iFXKua"
      "PHD5HBAFJiP1KVWMpuGX_POewU_ibt7v8g",
      token);

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier = jwt::verify()
                      .allow_algorithm(jwt::algorithm::rs256(kRsaPubKeyPem, "", "", ""))
                      .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtRS384) {
  // Cryptographic algorithm: RS384.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS384",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS384", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsaPubKeyPem, key1->get_key());

  // Create a JWT token and sign it with RS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS384")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::rs384(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtRS512) {
  // Cryptographic algorithm: RS512.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS512",
      kRsa512PubKeyJwkN, kRsa512PubKeyJwkE, kKid2, "RS512",
      kRsa512InvalidPubKeyJwkN, kRsa512PubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa512PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with RS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::rs512(kRsa512PubKeyPem, kRsa512PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtPS256) {
  // Cryptographic algorithm: PS256.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "PS256",
      kRsa1024PubKeyJwkN, kRsa1024PubKeyJwkE, kKid2, "PS256",
      kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa1024PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with PS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS256")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::ps256(kRsa1024PubKeyPem, kRsa1024PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtPS384) {
  // Cryptographic algorithm: PS384.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "PS384",
      kRsa2048PubKeyJwkN, kRsa2048PubKeyJwkE, kKid2, "PS384",
      kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa2048PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with PS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS384")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::ps384(kRsa2048PubKeyPem, kRsa2048PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtPS512) {
  // Cryptographic algorithm: PS512.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "PS512",
      kRsa4096PubKeyJwkN, kRsa4096PubKeyJwkE, kKid2, "PS512",
      kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa4096PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with PS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS512")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::ps512(kRsa4096PubKeyPem, kRsa4096PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtES256) {
  // Cryptographic algorithm: ES256.
  TempTestDataFile jwks_file(Substitute(kJwksEcFileFormat, kKid1, "P-256",
      kEcdsa256PubKeyJwkX, kEcdsa256PubKeyJwkY));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kEcdsa256PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with ES256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES256")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("kudu"))
                   .sign(jwt::algorithm::es256(
                       kEcdsa256PubKeyPem, kEcdsa256PrivKeyPem, "", ""));

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier =
      jwt::verify()
          .allow_algorithm(jwt::algorithm::es256(kEcdsa256PubKeyPem, "", "", ""))
          .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtES384) {
  // Cryptographic algorithm: ES384.
  TempTestDataFile jwks_file(Substitute(kJwksEcFileFormat, kKid1, "P-384",
      kEcdsa384PubKeyJwkX, kEcdsa384PubKeyJwkY));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kEcdsa384PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with ES384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES384")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("kudu"))
                   .sign(jwt::algorithm::es384(
                       kEcdsa384PubKeyPem, kEcdsa384PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtES512) {
  // Cryptographic algorithm: ES512.
  TempTestDataFile jwks_file(Substitute(kJwksEcFileFormat, kKid1, "P-521",
      kEcdsa521PubKeyJwkX, kEcdsa521PubKeyJwkY));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kEcdsa521PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with ES512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES512")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("kudu"))
                   .sign(jwt::algorithm::es512(
                       kEcdsa521PubKeyPem, kEcdsa521PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtNotVerifySignature) {
  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));

  // Do not verify signature.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  Status status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("kudu", username);
}

TEST(JwtUtilTest, VerifyJwtFailMismatchingAlgorithms) {
  // JWT algorithm is not matching with algorithm in JWK.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token, but set mismatching algorithm.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kKid1)
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  // Failed to verify the token due to mismatching algorithms.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_TRUE(status.IsNotAuthorized()) << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(),
      "JWT algorithm 'rs512' is not matching with JWK algorithm 'rs256'");
}

TEST(JwtUtilTest, VerifyJwtFailKeyNotFound) {
  // The key cannot be found in JWKS.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token with a key ID which can not be found in JWKS.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id("unfound-key-id")
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  // Failed to verify the token since key is not found in JWKS.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_TRUE(status.IsNotAuthorized()) << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "Invalid JWK ID in the JWT token");
}

TEST(JwtUtilTest, VerifyJwtTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  // Verify the token by trying each key in JWK set and there is one matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .sign(jwt::algorithm::rs512(kRsa512PubKeyPem, kRsa512PrivKeyPem, "", ""));
  // Verify the token by trying each key in JWK set, but there is no matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutSignature) {
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token without signature.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").sign(jwt::algorithm::none{});
  // Failed to verify the unsigned token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_TRUE(status.IsNotAuthorized()) << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "Unsecured JWT");
}

TEST(JwtUtilTest, VerifyJwtFailExpiredToken) {
  // Sign JWT token with RS256.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kKid1)
          .set_issued_at(std::chrono::system_clock::now())
          .set_expires_at(std::chrono::system_clock::now() - std::chrono::seconds{10})
          .set_payload_claim("username", picojson::value("kudu"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));

  // Verify the token, including expiring time.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_TRUE(status.IsNotAuthorized()) << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "JWT verification failed: token expired");
}

namespace {

// Returns a simple JWKS to be used by tokens signed by 'rsa_pub_key_pem' and
// 'rsa_priv_key_pem'.
void SimpleJWKSHandler(const Webserver::WebRequest& /*req*/,
                       Webserver::PrerenderedWebResponse* resp) {
  resp->output <<
      Substitute(kJwksRsaFileFormat, kKid1, "RS256",
          kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
          kRsaPubKeyJwkE);
  resp->status_code = HttpStatusCode::Ok;
}

class JWKSMockServer {
 public:
  // Registers a path handler for a single JWKS to be used by tokens signed by
  // 'rsa_pub_key_pem' and 'rsa_priv_key_pem'.
  Status Start() {
    WebserverOptions opts;
    opts.port = 0;
    opts.bind_interface = "127.0.0.1";
    webserver_.reset(new Webserver(opts));
    webserver_->RegisterPrerenderedPathHandler("/jwks", "JWKS", SimpleJWKSHandler,
                                               /*is_styled*/false, /*is_on_nav_bar*/false);
    RETURN_NOT_OK(webserver_->Start());
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(webserver_->GetBoundAddresses(&addrs));
    url_ = Substitute("http://$0/jwks", addrs[0].ToString());
    return Status::OK();
  }

  // Register a path handler for every account ID in 'account_id_to_resp' that
  // returns the correspodning HTTP response.
  Status StartWithAccounts(const unordered_map<string, string>& account_id_to_resp) {
    DCHECK(!webserver_);
    WebserverOptions opts;
    opts.port = 0;
    opts.bind_interface = "127.0.0.1";
    webserver_.reset(new Webserver(opts));
    for (const auto& ar : account_id_to_resp) {
      const auto& account_id = ar.first;
      const auto& jwks = ar.second;
      webserver_->RegisterPrerenderedPathHandler(Substitute("/jwks/$0", account_id), account_id,
          [account_id, jwks] (const Webserver::WebRequest& /*req*/,
                              Webserver::PrerenderedWebResponse* resp) {
            resp->output << jwks;
            resp->status_code = HttpStatusCode::Ok;
          },
          /*is_styled*/false, /*is_on_nav_bar*/false);
    }
    RETURN_NOT_OK(webserver_->Start());
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(webserver_->GetBoundAddresses(&addrs));
    url_ = Substitute("http://$0/jwks", addrs[0].ToString());
    return Status::OK();
  }

  const string& url() const {
    return url_;
  }

  string url_for_account(const string& account_id) const {
    return Substitute("$0/$1", url_, account_id);
  }
 private:
  unique_ptr<Webserver> webserver_;
  string url_;
};

} // anonymous namespace

TEST(JwtUtilTest, VerifyJWKSUrl) {
  JWKSMockServer jwks_server;
  ASSERT_OK(jwks_server.Start());

  auto encoded_token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWT")
          .set_algorithm("RS256")
          .set_key_id(kKid1)
          .set_subject("kudu")
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYjViMjEiLCJ0"
      "eXAiOiJKV1QifQ.eyJpc3MiOiJhdXRoMCIsInN1YiI6Imt1ZHUifQ.VENjfXICRV1lr2M-jBElI_qaBZNFILjkXr1Amg"
      "poH8xlI41EFN8RVkihuJtFijUOEFxJ537LCEonDBHsouO9iQlrxh0AobIjB1QraqG1BoQLnKWF78E-rhPN2K1aueGed7"
      "A86lkIEB4s7VU9dSDtR3bwbP5RFaf3XRZ6TyVh0h5sdMo91YKpS6nLCvYh2OSIbsUJCSNu4BoCmDz97Wq1xLiDoRfAJh"
      "BZiHQeHO38ydRMWIeto78pV2s9sf1CdwVwycuJOfnKY_-M5-fl1hW_25kSTNt33L57a5BgbGZ1sabWP3AD__-HYD2muR"
      "klbfyYn_ghqjL7ihY2ECaZzZ0Utw",
      encoded_token);
  KeyBasedJwtVerifier jwt_verifier(jwks_server.url(), /*is_local_file*/false);
  ASSERT_OK(jwt_verifier.Init());
  string subject;
  ASSERT_OK(jwt_verifier.VerifyToken(encoded_token, &subject));
  ASSERT_EQ("kudu", subject);
}

namespace {
const char kValidAccount[] = "new-phone";
const char kInvalidAccount[] = "who-is-this";
const char kMissingAccount[] = "no-one";
} // anonymous namespace

TEST(JwtUtilTest, VerifyOIDCDiscoveryEndpoint) {
  MiniOidcOptions opts;
  opts.account_ids = {
    { kValidAccount, /*is_valid*/true },
    { kInvalidAccount, /*is_valid*/false },
  };
  MiniOidc oidc(std::move(opts));
  ASSERT_OK(oidc.Start());
  const PerAccountKeyBasedJwtVerifier jwt_verifier(oidc.url(),
                                                   /*jwks_verify_server_certificate*/ false,
                                                   /*jwks_ca_certificate*/ "");

  // Create and verify a token on the happy path.
  const string kSubject = "kudu";
  auto valid_user_token =
      oidc.CreateJwt(kValidAccount, kSubject, /*is_valid*/true);
  string subject;
  ASSERT_OK(jwt_verifier.VerifyToken(valid_user_token, &subject));
  ASSERT_EQ(kSubject, subject);

  // Verify some expected failure scenarios.
  const unordered_map<string, string> invalid_jwts {
    { oidc.CreateJwt(kInvalidAccount, kSubject, false), "invalid issuer with invalid "
       "subject" },
    { oidc.CreateJwt(kInvalidAccount, kSubject, true), "invalid issuer with valid subject" },
    { oidc.CreateJwt(kValidAccount, kSubject, false), "valid issuer with invalid key id" },
    { oidc.CreateJwt(kMissingAccount, kSubject, true), "missing account" },
  };

  for (const auto& [jwt, msg] : invalid_jwts) {
    string invalid_subject;
    const Status s = jwt_verifier.VerifyToken(jwt, &invalid_subject);
    EXPECT_FALSE(s.ok()) << Substitute("failed case $0: $1", msg, s.ToString());
  }
}

TEST(JwtUtilTest, VerifyJWKSDiscoveryEndpointMultipleClients) {
  MiniOidcOptions opts;
  opts.account_ids = {
      { kValidAccount, /*is_valid*/true }
  };
  MiniOidc oidc(std::move(opts));
  ASSERT_OK(oidc.Start());
  PerAccountKeyBasedJwtVerifier jwt_verifier(oidc.url(),
                                             /*jwks_verify_server_certificate*/ false,
                                             /*jwks_ca_certificate*/ "");

  {
    const string kSubject = "kudu";
    auto valid_user_token =
        oidc.CreateJwt(kValidAccount, kSubject, /*is_valid*/true);
    string subject;
    ASSERT_OK(jwt_verifier.VerifyToken(valid_user_token, &subject));
    ASSERT_EQ(kSubject, subject);

    int constexpr n = 8;
    std::vector<std::thread> threads;
    threads.reserve(n);
    CountDownLatch latch(n);

    for (int i = 0; i < n; i++) {
      threads.emplace_back([&](){
        string subject;
        CHECK_OK(jwt_verifier.VerifyToken(valid_user_token, &subject));
        CHECK_EQ(kSubject, subject);
        latch.CountDown();
      });
    }

    latch.Wait();
    SCOPED_CLEANUP({
      for (auto& t : threads) {
        t.join();
      }
    });
  }
}

} // namespace kudu
