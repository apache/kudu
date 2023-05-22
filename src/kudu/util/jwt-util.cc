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

#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/ec.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <functional>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/defaults.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/rapidjson.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hash_util.h"
#include "kudu/util/jwt-util-internal.h"
#include "kudu/util/monotime.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/openssl_util_bio.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"
#include "kudu/util/thread.h"

using kudu::security::DataFormat;
using kudu::security::GetOpenSSLErrors;
using kudu::security::ToString;
using kudu::security::c_unique_ptr;
using kudu::security::ssl_make_unique;
using rapidjson::Document;
using rapidjson::Value;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using strings::Substitute;
using strings::WebSafeBase64Unescape;

DEFINE_int32(jwks_update_frequency_s, 60,
    "The time in seconds to wait between downloading JWKS from the specified URL.");
DEFINE_int32(jwks_pulling_timeout_s, 10,
    "The time in seconds for connection timed out when pulling JWKS from the specified URL.");

static bool ValidateBiggerThanZero(const char* name, const int32_t val) {
  if (val <= 0) {
    LOG(ERROR) << Substitute("Invalid value for $0 flag: $1", name, val);
    return false;
  }
  return true;
}

DEFINE_validator(jwks_update_frequency_s, &ValidateBiggerThanZero);
DEFINE_validator(jwks_pulling_timeout_s, &ValidateBiggerThanZero);

namespace kudu {

namespace security {

template<> struct SslTypeTraits<BIGNUM> {
  static constexpr auto kFreeFunc = &BN_free;
};

// Need this function because of template instantiation, but it's never used.
int WriteDerFuncNotImplementedEC(BIO* /*ununsed*/, EC_KEY* /*unused*/) {
  LOG(DFATAL) << "this should never be called";
  return -1;
}
template<> struct SslTypeTraits<EC_KEY> {
  static constexpr auto kFreeFunc = &EC_KEY_free;
  static constexpr auto kWritePemFunc = &PEM_write_bio_EC_PUBKEY;
  static constexpr auto kWriteDerFunc = &WriteDerFuncNotImplementedEC;
};

// Need this function because of template instantiation, but it's never used.
int WriteDerNotImplementedRSA(BIO* /*unused*/, RSA* /*unused*/) {
  LOG(DFATAL) << "this should never be called";
  return -1;
}
template<> struct SslTypeTraits<RSA> {
  static constexpr auto kFreeFunc = &RSA_free;
  static constexpr auto kWritePemFunc = &PEM_write_bio_RSA_PUBKEY;
  static constexpr auto kWriteDerFunc = &WriteDerNotImplementedRSA;
};

} // namespace security


// JWK Set (JSON Web Key Set) is JSON data structure that represents a set of JWKs.
// This class parses JWKS file.
class JWKSetParser {
  public:
  explicit JWKSetParser(JWKSSnapshot* jwks) : jwks_(jwks) {}

  // Perform the parsing and populate JWKS's internal map. Return error status if
  // encountering any error.
  Status Parse(const Document& rules_doc) {
    bool found_keys = false;
    for (Value::ConstMemberIterator member = rules_doc.MemberBegin();
         member != rules_doc.MemberEnd(); ++member) {
      if (strcmp("keys", member->name.GetString()) == 0) {
        found_keys = true;
        RETURN_NOT_OK(ParseKeys(member->value));
      } else {
        return Status::InvalidArgument(
            Substitute(
                "Unexpected property '$0' must be removed", member->name.GetString()));
      }
    }
    if (!found_keys) {
      return Status::InvalidArgument("An array of keys is required");
    }
    return Status::OK();
  }

 private:
  JWKSSnapshot* jwks_;

  static string NameOfTypeOfJsonValue(const Value& value) {
    switch (value.GetType()) {
      case rapidjson::kNullType:
        return "Null";
      case rapidjson::kFalseType:
      case rapidjson::kTrueType:
        return "Bool";
      case rapidjson::kObjectType:
        return "Object";
      case rapidjson::kArrayType:
        return "Array";
      case rapidjson::kStringType:
        return "String";
      case rapidjson::kNumberType:
        if (value.IsInt()) return "Integer";
        if (value.IsDouble()) return "Float";
      default:
        DCHECK(false);
        return "Unknown";
    }
  }

  // Parse an array of keys.
  Status ParseKeys(const Value& keys) {
    if (!keys.IsArray()) {
      return Status::InvalidArgument(
          Substitute(
              "'keys' must be of type Array but is a '$0'", NameOfTypeOfJsonValue(keys)));
    }
    if (keys.Size() == 0) {
      return Status::InvalidArgument(Substitute("'keys' must be a non empty Array"));
    }
    for (rapidjson::SizeType key_idx = 0; key_idx < keys.Size(); ++key_idx) {
      const Value& key = keys[key_idx];
      if (!key.IsObject()) {
        return Status::InvalidArgument(
            Substitute("parsing key #$0, key should be a JSON Object but is a '$1'.",
                key_idx, NameOfTypeOfJsonValue(key)));
      }
      Status status = ParseKey(key);
      if (!status.ok()) {
        Status parse_status = Status::InvalidArgument(Substitute("parsing key #$0, ", key_idx));
        return parse_status.CloneAndAppend(status.message());
      }
    }
    return Status::OK();
  }

  // Parse a public key and populate JWKS's internal map.
  Status ParseKey(const Value& json_key) {
    std::unordered_map<std::string, std::string> kv_map;
    string key;
    string value;
    for (Value::ConstMemberIterator member = json_key.MemberBegin();
         member != json_key.MemberEnd(); ++member) {
      key = string(member->name.GetString());
      RETURN_NOT_OK(ReadKeyProperty(key, json_key, &value, /*required*/ false));
      if (!EmplaceIfNotPresent(&kv_map, key, value)) {
        LOG(WARNING) << "Duplicate property of JWK: " << key;
      }
    }

    const auto* key_type = FindOrNull(kv_map, "kty");
    if (!key_type) return Status::InvalidArgument("'kty' property is required");
    const auto* key_id = FindOrNull(kv_map, "kid");
    if (!key_id) return Status::InvalidArgument("'kid' property is required");
    if (key_id->empty()) {
      return Status::InvalidArgument(Substitute("'kid' property must be a non-empty string"));
    }

    string key_type_lower;
    ToLowerCase(*key_type, &key_type_lower);
    if (key_type_lower == "oct") {
      unique_ptr<JWTPublicKey> jwt_pub_key;
      RETURN_NOT_OK(HSJWTPublicKeyBuilder::CreateJWKPublicKey(kv_map, &jwt_pub_key));
      jwks_->AddHSKey(*key_id, std::move(jwt_pub_key));
    } else if (key_type_lower == "rsa") {
      unique_ptr<JWTPublicKey> jwt_pub_key;
      RETURN_NOT_OK(RSAJWTPublicKeyBuilder::CreateJWKPublicKey(kv_map, &jwt_pub_key));
      jwks_->AddRSAPublicKey(*key_id, std::move(jwt_pub_key));
    } else if (key_type_lower == "ec") {
      unique_ptr<JWTPublicKey> jwt_pub_key;
      RETURN_NOT_OK(ECJWTPublicKeyBuilder::CreateJWKPublicKey(kv_map, &jwt_pub_key));
      jwks_->AddECPublicKey(*key_id, std::move(jwt_pub_key));
    } else {
      return Status::InvalidArgument(Substitute("Unsupported kty: '$0'", key_type));
    }
    return Status::OK();
  }

  // Reads a key property of the given name and assigns the property value to the out
  // parameter. A true return value indicates success.
  template <typename T>
  Status ReadKeyProperty(
      const string& name, const Value& json_key, T* value, bool required = true) {
    const Value& json_value = json_key[name.c_str()];
    if (json_value.IsNull()) {
      if (required) {
        return Status::InvalidArgument(Substitute("'$0' property is required and cannot be null",
                                                  name));
      }
      return Status::OK();

    }
    return ValidateTypeAndExtractValue(name, json_value, value);
  }

// Extract a value stored in a rapidjson::Value and assign it to the out parameter.
// The type will be validated before extraction. A true return value indicates success.
// The name parameter is only used to generate an error message upon failure.
#define EXTRACT_VALUE(json_type, cpp_type)                                             \
  Status ValidateTypeAndExtractValue(                                                  \
      const string& name, const Value& json_value, cpp_type* value) {                  \
    if (!json_value.Is##json_type()) {                                                 \
      return Status::InvalidArgument(                                                  \
          Substitute("'$0' property must be of type " #json_type " but is a $1", name, \
              NameOfTypeOfJsonValue(json_value)));                                     \
    }                                                                                  \
    *value = json_value.Get##json_type();                                              \
    return Status::OK();                                                               \
  }

  EXTRACT_VALUE(String, string)
  // EXTRACT_VALUE(Bool, bool)
};

namespace {

// Utility function to handle exceptions from the jwt-cpp library.
Status HandleEx(const char* const msg, const std::exception& e) {
  const auto err = GetOpenSSLErrors();
  return Status::NotAuthorized(err.empty()
      ? Substitute("$0: $1", msg, e.what())
      : Substitute("$0: $1 ($2)", msg, e.what(), err));
}

} // anonymous namespace

//
// JWTPublicKey member functions.
//
// Verify JWT's signature for the given decoded token with jwt-cpp API.
Status JWTPublicKey::Verify(
    const DecodedJWT& decoded_jwt, const std::string& algorithm) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // Verify if algorithms are matching.
  if (algorithm_ != algorithm) {
    return Status::NotAuthorized(
        Substitute("JWT algorithm '$0' is not matching with JWK algorithm '$1'",
            algorithm, algorithm_));
  }

  try {
    // Call jwt-cpp API to verify token's signature.
    verifier_.verify(decoded_jwt);
  } catch (const std::exception& e) {
    return HandleEx("JWT verification failed", e);
  }
  return Status::OK();
}

// Create a JWKPublicKey of HS from the JWK.
Status HSJWTPublicKeyBuilder::CreateJWKPublicKey(
    const JsonKVMap& kv_map, unique_ptr<JWTPublicKey>* pub_key_out) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // Octet Sequence keys for HS256, HS384 or HS512.
  // JWK Sample:
  // {
  //   "kty":"oct",
  //   "alg":"HS256",
  //   "k":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
  //   "kid":"Id that can be uniquely Identified"
  // }
  auto it_alg = kv_map.find("alg");
  if (it_alg == kv_map.end()) return Status::InvalidArgument("'alg' property is required");
  string algorithm;
  ToLowerCase(it_alg->second, &algorithm);

  if (algorithm.empty()) {
    return Status::InvalidArgument(Substitute("'alg' property must be a non-empty string"));
  }
  auto it_k = kv_map.find("k");
  if (it_k == kv_map.end()) return Status::InvalidArgument("'k' property is required");
  if (it_k->second.empty()) {
    return Status::InvalidArgument(Substitute("'k' property must be a non-empty string"));
  }

  unique_ptr<JWTPublicKey> jwt_pub_key;
  try {
    if (algorithm == "hs256") {
      jwt_pub_key.reset(new HS256JWTPublicKey(algorithm, it_k->second));
    } else if (algorithm == "hs384") {
      jwt_pub_key.reset(new HS384JWTPublicKey(algorithm, it_k->second));
    } else if (algorithm == "hs512") {
      jwt_pub_key.reset(new HS512JWTPublicKey(algorithm, it_k->second));
    } else {
      return Status::InvalidArgument(Substitute("Invalid 'alg' property value: '$0'", algorithm));
    }
  } catch (const std::exception& e) {
    return HandleEx("failed to initialize verifier", e);
  }
  *pub_key_out = std::move(jwt_pub_key);
  return Status::OK();
}

// Create a JWKPublicKey of RSA from the JWK.
Status RSAJWTPublicKeyBuilder::CreateJWKPublicKey(
    const JsonKVMap& kv_map, unique_ptr<JWTPublicKey>* pub_key_out) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // JWK Sample:
  // {
  //   "kty":"RSA",
  //   "alg":"RS256",
  //   "n":"sttddbg-_yjXzcFpbMJB1fI9...Q_QDhvqXx8eQ1r9smM",
  //   "e":"AQAB",
  //   "kid":"Id that can be uniquely Identified"
  // }
  auto it_alg = kv_map.find("alg");
  if (it_alg == kv_map.end()) return Status::InvalidArgument("'alg' property is required");
  string algorithm;
  ToLowerCase(it_alg->second, &algorithm);
  if (algorithm.empty()) {
    return Status::InvalidArgument(Substitute("'alg' property must be a non-empty string"));
  }

  auto it_n = kv_map.find("n");
  auto it_e = kv_map.find("e");
  if (it_n == kv_map.end() || it_e == kv_map.end()) {
    return Status::InvalidArgument("'n' and 'e' properties are required");
  }
  if (it_n->second.empty() || it_e->second.empty()) {
    return Status::InvalidArgument("'n' and 'e' properties must be a non-empty string");
  }
  // Converts public key to PEM encoded form.
  string pub_key;
  RETURN_NOT_OK_PREPEND(ConvertJwkToPem(it_n->second, it_e->second, pub_key),
                        Substitute("invalid public key 'n':'$0', 'e':'$1'",
                                   it_n->second, it_e->second));
  unique_ptr<JWTPublicKey> jwt_pub_key;
  try {
    if (algorithm == "rs256") {
      jwt_pub_key.reset(new RS256JWTPublicKey(algorithm, pub_key));
    } else if (algorithm == "rs384") {
      jwt_pub_key.reset(new RS384JWTPublicKey(algorithm, pub_key));
    } else if (algorithm == "rs512") {
      jwt_pub_key.reset(new RS512JWTPublicKey(algorithm, pub_key));
    } else if (algorithm == "ps256") {
      jwt_pub_key.reset(new PS256JWTPublicKey(algorithm, pub_key));
    } else if (algorithm == "ps384") {
      jwt_pub_key.reset(new PS384JWTPublicKey(algorithm, pub_key));
    } else if (algorithm == "ps512") {
      jwt_pub_key.reset(new PS512JWTPublicKey(algorithm, pub_key));
    } else {
      return Status::InvalidArgument(Substitute("Invalid 'alg' property value: '$0'", algorithm));
    }
  } catch (const std::exception& e) {
    return HandleEx("failed to initialize verifier", e);
  }

  *pub_key_out = std::move(jwt_pub_key);
  return Status::OK();
}

// Convert JWK's RSA public key to PEM format using OpenSSL API.
Status RSAJWTPublicKeyBuilder::ConvertJwkToPem(
    const string& base64_n, const string& base64_e, string& pub_key) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  string str_n;
  if (!WebSafeBase64Unescape(base64_n, &str_n)) {
    return Status::InvalidArgument("malformed 'n' key component");
  }
  string str_e;
  if (!WebSafeBase64Unescape(base64_e, &str_e)) {
    return Status::InvalidArgument("malformed 'e' key component");
  }
  auto mod = ssl_make_unique(BN_bin2bn(
      reinterpret_cast<const unsigned char*>(str_n.c_str()),
      static_cast<int>(str_n.size()),
      nullptr));
  auto exp = ssl_make_unique(BN_bin2bn(
      reinterpret_cast<const unsigned char*>(str_e.c_str()),
      static_cast<int>(str_e.size()),
      nullptr));
  auto rsa = ssl_make_unique(RSA_new());
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  rsa->n = mod.release();
  rsa->e = exp.release();
#else
  // RSA_set0_key is a new API introduced in OpenSSL version 1.1
  OPENSSL_RET_NOT_OK(RSA_set0_key(
      rsa.get(), mod.release(), exp.release(), nullptr), "failed to set RSA key");
#endif
  return ToString(&pub_key, DataFormat::PEM, rsa.get());
}

// Create a JWKPublicKey of EC (ES256, ES384 or ES512) from the JWK.
Status ECJWTPublicKeyBuilder::CreateJWKPublicKey(
        const JsonKVMap& kv_map, unique_ptr<JWTPublicKey>* pub_key_out) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // JWK Sample:
  // {
  //   "kty":"EC",
  //   "crv":"P-256",
  //   "x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
  //   "y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
  //   "kid":"Id that can be uniquely Identified"
  // }
  string algorithm;
  int eccgrp;
  auto it_crv = kv_map.find("crv");
  if (it_crv != kv_map.end()) {
    string curve;
    ToUpperCase(it_crv->second, &curve);
    if (curve == "P-256") {
      algorithm = "es256";
      eccgrp = NID_X9_62_prime256v1;
    } else if (curve == "P-384") {
      algorithm = "es384";
      eccgrp = NID_secp384r1;
    } else if (curve == "P-521") {
      algorithm = "es512";
      eccgrp = NID_secp521r1;
    } else {
      return Status::NotSupported(Substitute("Unsupported crv: '$0'", curve));
    }
  } else {
    auto it_alg = kv_map.find("alg");
    if (it_alg == kv_map.end()) {
      return Status::InvalidArgument("'alg' or 'crv' property is required");
    }
    ToLowerCase(it_alg->second, &algorithm);
    if (algorithm.empty()) {
      return Status::InvalidArgument(Substitute("'alg' property must be a non-empty string"));
    }
    if (algorithm == "es256") {
      // ECDSA using P-256 and SHA-256 (OBJ_txt2nid("prime256v1")).
      eccgrp = NID_X9_62_prime256v1;
    } else if (algorithm == "es384") {
      // ECDSA using P-384 and SHA-384 (OBJ_txt2nid("secp384r1")).
      eccgrp = NID_secp384r1;
    } else if (algorithm == "es512") {
      // ECDSA using P-521 and SHA-512 (OBJ_txt2nid("secp521r1")).
      eccgrp = NID_secp521r1;
    } else {
      return Status::NotSupported(Substitute("Unsupported alg: '$0'", algorithm));
    }
  }

  auto it_x = kv_map.find("x");
  auto it_y = kv_map.find("y");
  if (it_x == kv_map.end() || it_y == kv_map.end()) {
    return Status::InvalidArgument("'x' and 'y' properties are required");
  }
  if (it_x->second.empty() || it_y->second.empty()) {
    return Status::InvalidArgument("'x' and 'y' properties must be a non-empty string");
  }
  // Convert the public key into PEM format.
  string pub_key;
  RETURN_NOT_OK_PREPEND(ConvertJwkToPem(eccgrp, it_x->second, it_y->second, pub_key),
                        Substitute("invalid public key 'x':'$0', 'y':'$1'",
                                   it_x->second, it_y->second));

  JWTPublicKey* jwt_pub_key = nullptr;
  try {
    if (algorithm == "es256") {
      jwt_pub_key = new ES256JWTPublicKey(algorithm, pub_key);
    } else if (algorithm == "es384") {
      jwt_pub_key = new ES384JWTPublicKey(algorithm, pub_key);
    } else {
      DCHECK(algorithm == "es512");
      jwt_pub_key = new ES512JWTPublicKey(algorithm, pub_key);
    }
  } catch (const std::exception& e) {
    return HandleEx("failed to initialize verifier", e);
  }

  pub_key_out->reset(jwt_pub_key);
  return Status::OK();
}

// Convert JWK's EC public key to PEM format using OpenSSL API.
Status ECJWTPublicKeyBuilder::ConvertJwkToPem(int eccgrp,
                                              const string& base64_x,
                                              const string& base64_y,
                                              string& pub_key) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  string ascii_x;
  if (!WebSafeBase64Unescape(base64_x, &ascii_x)) {
    return Status::InvalidArgument("malformed 'x' key component");
  }
  string ascii_y;
  if (!WebSafeBase64Unescape(base64_y, &ascii_y)) {
    return Status::InvalidArgument("malformed 'y' key component");
  }
  auto x = ssl_make_unique(BN_bin2bn(
      reinterpret_cast<const unsigned char*>(ascii_x.c_str()),
      static_cast<int>(ascii_x.size()),
      nullptr));
  auto y = ssl_make_unique(BN_bin2bn(
      reinterpret_cast<const unsigned char*>(ascii_y.c_str()),
      static_cast<int>(ascii_y.size()),
      nullptr));
  auto ec_key = ssl_make_unique(EC_KEY_new_by_curve_name(eccgrp));
  OPENSSL_RET_IF_NULL(ec_key, "failed to create EC key");
  EC_KEY_set_asn1_flag(ec_key.get(), OPENSSL_EC_NAMED_CURVE);
  OPENSSL_RET_NOT_OK(EC_KEY_set_public_key_affine_coordinates(
      ec_key.get(), x.get(), y.get()), "failed to set public key");

  return ToString(&pub_key, DataFormat::PEM, ec_key.get());
}

//
// JWKSSnapshot member functions.
//

// Load JWKS from the given local json file.
Status JWKSSnapshot::LoadKeysFromFile(const string& jwks_file_path) {
  hs_key_map_.clear();
  rsa_pub_key_map_.clear();

  // Read the file.
  FILE* jwks_file = fopen(jwks_file_path.c_str(), "r");
  if (jwks_file == nullptr) {
    return Status::RuntimeError(
        Substitute("Could not open JWKS file '$0'; $1", jwks_file_path, strerror(errno)));
  }
  // Check for an empty file and ignore it.
  struct stat jwks_file_stats;
  if (fstat(fileno(jwks_file), &jwks_file_stats)) {
    fclose(jwks_file);
    return Status::RuntimeError(
        Substitute("Error reading JWKS file '$0'; $1", jwks_file_path, strerror(errno)));
  }
  if (jwks_file_stats.st_size == 0) {
    fclose(jwks_file);
    return Status::OK();
  }

  char readBuffer[65536];
  rapidjson::FileReadStream stream(jwks_file, readBuffer, sizeof(readBuffer));
  Document jwks_doc;
  jwks_doc.ParseStream(stream);
  fclose(jwks_file);
  if (jwks_doc.HasParseError()) {
    return Status::InvalidArgument(GetParseError_En(jwks_doc.GetParseError()));
  }
  if (!jwks_doc.IsObject()) {
    return Status::InvalidArgument("root element must be a JSON Object");
  }
  if (!jwks_doc.HasMember("keys")) {
    return Status::InvalidArgument("keys is required");
  }

  JWKSetParser jwks_parser(this);
  return jwks_parser.Parse(jwks_doc);
}

// Download JWKS from the given URL with Kudu's EasyCurl wrapper.
Status JWKSSnapshot::LoadKeysFromUrl(
    const std::string& jwks_url, bool jwks_verify_server_certificate, uint64_t cur_jwks_checksum,
    bool* is_changed) {
  kudu::EasyCurl curl;
  kudu::faststring dst;
  *is_changed = false;

  curl.set_timeout(
      kudu::MonoDelta::FromMilliseconds(static_cast<int64_t>(FLAGS_jwks_pulling_timeout_s) * 1000));
  curl.set_verify_peer(jwks_verify_server_certificate);

  // TODO support CurlAuthType by calling kudu::EasyCurl::set_auth().
  RETURN_NOT_OK_PREPEND(curl.FetchURL(jwks_url, &dst),
      Substitute("Error downloading JWKS from '$0'", jwks_url));
  if (dst.size() > 0) {
    // Verify if the checksum of the downloaded JWKS has been changed.
    jwks_checksum_ = HashUtil::FastHash64(dst.data(), dst.size(), /*seed*/ 0xcafebeef);
    if (jwks_checksum_ == cur_jwks_checksum) {
      return Status::OK();
    }
    // Append '\0' so that the in-memory object could be parsed as StringStream.
    dst.push_back('\0');
#ifndef NDEBUG
    VLOG(3) << "JWKS: " << dst.data();
#endif
    // Parse in-memory JWKS JSON object as StringStream.
    Document jwks_doc;
    jwks_doc.Parse(reinterpret_cast<char*>(dst.data()));
    if (jwks_doc.HasParseError()) {
      return Status::InvalidArgument(GetParseError_En(jwks_doc.GetParseError()));
    }
    if (!jwks_doc.IsObject()) {
      return Status::InvalidArgument("root element must be a JSON Object");
    }
    if (!jwks_doc.HasMember("keys")) {
      return Status::InvalidArgument("keys is required");
    }

    // Load and initialize public keys.
    JWKSetParser jwks_parser(this);
    RETURN_NOT_OK(jwks_parser.Parse(jwks_doc));
  }

  *is_changed = true;
  return Status::OK();
}

void JWKSSnapshot::AddHSKey(const std::string& key_id,
                            unique_ptr<JWTPublicKey> jwk_pub_key) {
  if (hs_key_map_.find(key_id) == hs_key_map_.end()) {
    hs_key_map_[key_id] = std::move(jwk_pub_key);
  } else {
    LOG(WARNING) << "Duplicate key ID of JWK for HS key: " << key_id;
  }
}

void JWKSSnapshot::AddRSAPublicKey(const std::string& key_id,
                                   unique_ptr<JWTPublicKey> jwk_pub_key) {
  if (rsa_pub_key_map_.find(key_id) == rsa_pub_key_map_.end()) {
    rsa_pub_key_map_[key_id] = std::move(jwk_pub_key);
  } else {
    LOG(WARNING) << "Duplicate key ID of JWK for RSA public key: " << key_id;
  }
}

void JWKSSnapshot::AddECPublicKey(const std::string& key_id,
                                  unique_ptr<JWTPublicKey> jwk_pub_key) {
  if (ec_pub_key_map_.find(key_id) == ec_pub_key_map_.end()) {
    ec_pub_key_map_[key_id] = std::move(jwk_pub_key);
  } else {
    LOG(WARNING) << "Duplicate key ID of JWK for EC public key: " << key_id;
  }
}

const JWTPublicKey* JWKSSnapshot::LookupHSKey(const std::string& kid) const {
  return FindPointeeOrNull(hs_key_map_, kid);
}

const JWTPublicKey* JWKSSnapshot::LookupRSAPublicKey(const std::string& kid) const {
  return FindPointeeOrNull(rsa_pub_key_map_, kid);
}

const JWTPublicKey* JWKSSnapshot::LookupECPublicKey(const std::string& kid) const {
  return FindPointeeOrNull(ec_pub_key_map_, kid);
}

//
// JWKSMgr member functions.
//

JWKSMgr::~JWKSMgr() {
  shut_down_promise_.Set(true);
  if (jwks_update_thread_ != nullptr) jwks_update_thread_->Join();
}

Status JWKSMgr::Init(const std::string& jwks_uri, bool jwks_verify_server_certificate,
                     bool is_local_file) {
  jwks_uri_ = jwks_uri;
  jwks_verify_server_certificate_ = jwks_verify_server_certificate;
  std::shared_ptr<JWKSSnapshot> new_jwks = std::make_shared<JWKSSnapshot>();
  if (is_local_file) {
    RETURN_NOT_OK_PREPEND(new_jwks->LoadKeysFromFile(jwks_uri), "Failed to load JWKS");
    SetJWKSSnapshot(new_jwks);
  } else {
    bool is_changed = false;
    RETURN_NOT_OK_PREPEND(new_jwks->LoadKeysFromUrl(jwks_uri, jwks_verify_server_certificate,
                                                    current_jwks_checksum_,
                                                    &is_changed),
                          "Failed to load JWKS");
    DCHECK(is_changed);
    if (is_changed) SetJWKSSnapshot(new_jwks);

    // Start a working thread to periodically check the JWKS URL for updates.
    RETURN_NOT_OK(Thread::Create("JWT", "JWKS-mgr",
        [this] { return UpdateJWKSThread(); }, &jwks_update_thread_));
  }

  // This is only a warning as JWKS information might be changing over time, if for some reason,
  // the file which URI is pointing at becomes empty, it'll still be downloaded, but no keys will be
  // verified successfully (due to no public keys in the JWKS to do so).
  // Since the UpdateJWKSThread is still alive, if the JWKS file/endpoint is fixed, then the
  // verification will be successful again.
  if (new_jwks->IsEmpty()) LOG(WARNING) << "JWKS file is empty.";
  return Status::OK();
}

void JWKSMgr::UpdateJWKSThread() {
  std::shared_ptr<JWKSSnapshot> new_jwks;
  const MonoDelta &timeout = MonoDelta::FromSeconds(FLAGS_jwks_update_frequency_s);

  while (true) {
    if (shut_down_promise_.WaitFor(timeout) != nullptr) {
      // Shutdown has happened, stop updating JWKS.
      break;
    }

    new_jwks = std::make_shared<JWKSSnapshot>();
    bool is_changed = false;
    Status status =
        new_jwks->LoadKeysFromUrl(jwks_uri_, jwks_verify_server_certificate_,
                                  current_jwks_checksum_, &is_changed);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to update JWKS: " << status.ToString();
    } else if (is_changed) {
      SetJWKSSnapshot(new_jwks);
      if (new_jwks->IsEmpty()) {
        LOG(WARNING) << "New JWKS snapshot is empty.";
      }
    }
    new_jwks.reset();
  }
  // The promise must be set to true.
  DCHECK(shut_down_promise_.Get());
}

JWKSSnapshotPtr JWKSMgr::GetJWKSSnapshot() const {
  std::lock_guard<std::mutex> l(current_jwks_lock_);
  DCHECK(current_jwks_.get() != nullptr);
  JWKSSnapshotPtr jwks = current_jwks_;
  return jwks;
}

void JWKSMgr::SetJWKSSnapshot(const JWKSSnapshotPtr& new_jwks) {
  std::lock_guard<std::mutex> l(current_jwks_lock_);
  DCHECK(new_jwks.get() != nullptr);
  current_jwks_ = new_jwks;
  current_jwks_checksum_ = new_jwks->GetChecksum();
}

//
// JWTHelper member functions.
//

JWTHelper::~JWTHelper() {
}

struct JWTHelper::JWTDecodedToken {
  explicit JWTDecodedToken(DecodedJWT  decoded_jwt) : decoded_jwt_(std::move(decoded_jwt)) {}
  DecodedJWT decoded_jwt_;
};

JWTHelper* JWTHelper::jwt_helper_ = new JWTHelper();

void JWTHelper::TokenDeleter::operator()(JWTHelper::JWTDecodedToken* token) const {
  delete token;
}

Status JWTHelper::Init(const std::string& jwks_file_path) {
  jwks_mgr_.reset(new JWKSMgr());
  RETURN_NOT_OK(jwks_mgr_->Init(jwks_file_path,
                                /*jwks_verify_server_certificate*/ false,
                                /*is_local_file*/ true));
  if (!initialized_) initialized_ = true;
  return Status::OK();
}

Status JWTHelper::Init(const std::string& jwks_uri, bool jwks_verify_server_certificate) {
  jwks_mgr_.reset(new JWKSMgr());
  RETURN_NOT_OK(jwks_mgr_->Init(jwks_uri, jwks_verify_server_certificate, false));
  if (!initialized_) initialized_ = true;
  return Status::OK();
}

JWKSSnapshotPtr JWTHelper::GetJWKS() const {
  DCHECK(initialized_);
  return jwks_mgr_->GetJWKSSnapshot();
}

// Decode the given JWT token.
Status JWTHelper::Decode(const string& token, UniqueJWTDecodedToken& decoded_token_out) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  try {
    // Call jwt-cpp API to decode the JWT token with default jwt::json_traits
    // (jwt::picojson_traits).
    decoded_token_out.reset(new JWTDecodedToken(jwt::decode(token)));
#ifndef NDEBUG
    std::stringstream msg;
    msg << "JWT token header: ";
    for (auto& [key, val] : decoded_token_out.get()->decoded_jwt_.get_header_claims()) {
      msg << key << "=" << val.to_json().serialize() << ";";
    }
    msg << " JWT token payload: ";
    for (auto& [key, val] : decoded_token_out.get()->decoded_jwt_.get_payload_claims()) {
      msg << key << "=" << val.to_json().serialize() << ";";
    }
    VLOG(3) << msg.str();
#endif
  } catch (const std::invalid_argument& e) {
    return HandleEx("token is not in correct format", e);
  } catch (const std::runtime_error& e) {
    return HandleEx("base64 decoding failed or invalid JSON", e);
  } catch (const std::exception& e) {
    return HandleEx("unexpected error while decoding JWT", e);
  }
  return Status::OK();
}

// Validate the token's signature with public key.
Status JWTHelper::Verify(const JWTDecodedToken* decoded_token) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  DCHECK(initialized_);

  const auto& decoded_jwt = decoded_token->decoded_jwt_;

  if (decoded_jwt.get_signature().empty()) {
    // Don't accept JWT without a signature.
    return Status::NotAuthorized("Unsecured JWT");
  }
  if (jwks_mgr_ == nullptr) {
    // Skip to signature validation if JWKS file or url is not specified.
    return Status::OK();
  }

  JWKSSnapshotPtr jwks = GetJWKS();
  if (jwks->IsEmpty()) {
    return Status::NotAuthorized("Verification failed, no matching valid key");
  }

  try {
    string algorithm;
    ToLowerCase(decoded_jwt.get_algorithm(), &algorithm);
    string prefix = algorithm.substr(0, 2);
    if (decoded_jwt.has_key_id()) {
      // Get key id from token's header and use it to retrieve the public key from JWKS.
      std::string key_id = decoded_jwt.get_key_id();

      const JWTPublicKey* pub_key = nullptr;
      if (prefix == "hs") {
        pub_key = jwks->LookupHSKey(key_id);
      } else if (prefix == "rs" || prefix == "ps") {
        pub_key = jwks->LookupRSAPublicKey(key_id);
      } else if (prefix == "es") {
        pub_key = jwks->LookupECPublicKey(key_id);
      } else {
        return Status::NotAuthorized(
            Substitute("Unsupported cryptographic algorithm '$0' for JWT", algorithm));
      }
      if (pub_key == nullptr) {
        return Status::NotAuthorized("Invalid JWK ID in the JWT token");
      }
      // Use the public key to verify the token's signature.
      RETURN_NOT_OK(pub_key->Verify(decoded_jwt, algorithm));
    } else {
      // According to RFC 7517 (JSON Web Key), 'kid' is OPTIONAL so it's possible there
      // is no key id in the token's header. In this case, get all of public keys from
      // JWKS for the family of algorithms.
      const JWKSSnapshot::JWTPublicKeyMap* key_map = nullptr;
      if (prefix == "hs") {
        key_map = jwks->GetAllHSKeys();
      } else if (prefix == "rs" || prefix == "ps") {
        key_map = jwks->GetAllRSAPublicKeys();
      } else if (prefix == "es") {
        key_map = jwks->GetAllECPublicKeys();
      } else {
        return Status::NotAuthorized(
            Substitute("Unsupported cryptographic algorithm '$0' for JWT", algorithm));
      }
      if (key_map->empty()) {
        return Status::NotAuthorized("Verification failed, no matching key");
      }
      Status status;
      // Try each key with matching algorithm util the signature is verified.
      for (const auto& key : *key_map) {
        status = key.second->Verify(decoded_jwt, algorithm);
        if (status.ok()) return status;
      }
      return status;
    }
  } catch (const std::bad_cast& e) {
    return HandleEx("claim was present but not a string", e);
  } catch (const jwt::error::claim_not_present_exception& e) {
    return HandleEx("claim not present in JWT token", e);
  } catch (const std::exception& e) {
    return HandleEx("token verification failed", e);
  }
  return Status::OK();
}

Status JWTHelper::GetCustomClaimUsername(const JWTDecodedToken* decoded_token,
    const string& jwt_custom_claim_username, string& username) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  DCHECK(!jwt_custom_claim_username.empty());
  const auto& decoded_jwt = decoded_token->decoded_jwt_;
  try {
    // Get value of custom claim 'username' from the token payload.
    if (!decoded_jwt.has_payload_claim(jwt_custom_claim_username)) {
      return Status::NotAuthorized(
          Substitute("Claim '$0' was not present", jwt_custom_claim_username));
    }

    username = decoded_jwt.get_payload_claim(jwt_custom_claim_username).to_json().to_str();

    if (username.empty()) {
      return Status::NotAuthorized(
          Substitute("Claim '$0' is empty", jwt_custom_claim_username));
    }
  } catch (const std::runtime_error& e) {
    return HandleEx(Substitute("claim '$0' was not present",
                               jwt_custom_claim_username).c_str(), e);
  } catch (const std::exception& e) {
    return HandleEx("unexpected error while processing JWT claim", e);
  }

  return Status::OK();
}

Status KeyBasedJwtVerifier::Init() const {
  if (!jwt_->IsInitialised()) {
    if (is_local_file_) {
      return jwt_->Init(jwks_uri_);
    }

    return jwt_->Init(jwks_uri_, jwks_verify_server_certificate_);
  }

  return Status::OK();
}

Status KeyBasedJwtVerifier::VerifyToken(const string& bytes_raw, string* subject) const {
  RETURN_NOT_OK(Init());

  JWTHelper::UniqueJWTDecodedToken decoded_token;
  RETURN_NOT_OK(JWTHelper::Decode(bytes_raw, decoded_token));
  RETURN_NOT_OK(jwt_->Verify(decoded_token.get()));
  if (!decoded_token->decoded_jwt_.has_subject()) {
    return Status::InvalidArgument("token does not include subject");
  }
  *subject = decoded_token->decoded_jwt_.get_subject();
  return Status::OK();
}

Status PerAccountKeyBasedJwtVerifier::JWTHelperForToken(const JWTHelper::JWTDecodedToken& token,
                                                        JWTHelper** helper) const {
  if (!token.decoded_jwt_.has_issuer()) {
    return Status::InvalidArgument("Expected token to have 'issuer' field");
  }

  // Parse the account ID from the 'iss' field of the JWT. If we already have a
  // JWTHelper for it, use it.
  const auto& issuer = token.decoded_jwt_.get_issuer();
  std::vector<string> issuer_pieces = strings::Split(issuer, "/");
  if (issuer_pieces.empty()) {
    return Status::InvalidArgument("cannot parse 'issuer' field");
  }
  const auto& account_id = issuer_pieces.back();

  {
    const std::lock_guard<simple_spinlock> l(jwt_by_account_id_map_lock_);
    const auto* unique_helper = FindOrNull(jwt_by_account_id_, account_id);

    if (unique_helper) {
      *helper = unique_helper->get();
      return Status::OK();
    }
  }

  // Otherwise, use the OIDC Discovery Endpoint to determine what 'jwks_uri' to
  // use.
  kudu::EasyCurl curl;
  kudu::faststring dst;
  const auto discovery_endpoint = Substitute("$0?accountId=$1", oidc_uri_, account_id);
  curl.set_timeout(
      kudu::MonoDelta::FromSeconds(static_cast<int64_t>(FLAGS_jwks_pulling_timeout_s)));
  curl.set_verify_peer(false);
  RETURN_NOT_OK_PREPEND(curl.FetchURL(discovery_endpoint, &dst),
      Substitute("Error downloading contents of Discovery Endpoint from '$0'", discovery_endpoint));
  string jwks_uri;

  if (dst.size() <= 0) {
    return Status::RuntimeError("Discovery Endpoint returned an empty document");
  }

  dst.push_back('\0');
  Document endpoint_doc;
  endpoint_doc.Parse(reinterpret_cast<char*>(dst.data()));
#define RETURN_INVALID_IF(stmt, msg)     \
  if (PREDICT_FALSE(stmt)) {             \
    return Status::InvalidArgument(msg); \
  }

  RETURN_INVALID_IF(endpoint_doc.HasParseError(), GetParseError_En(endpoint_doc.GetParseError()));
  RETURN_INVALID_IF(!endpoint_doc.IsObject(), "root element must be a JSON Object");
  auto jwks_uri_member = endpoint_doc.FindMember("jwks_uri");
  RETURN_INVALID_IF(jwks_uri_member == endpoint_doc.MemberEnd(), "jwks_uri is required");
  RETURN_INVALID_IF(!jwks_uri_member->value.IsString(), "jwks_uri must be a string");
  jwks_uri = string(jwks_uri_member->value.GetString());
#undef RETURN_INVALID_IF

  // TODO(zchovan): this implementation expects there to be a small number of
  // accounts, as it creates a JWKS refresh thread for each account. Group the
  // refreshes into a single thread or threadpool.
  auto new_helper = std::make_shared<JWTHelper>();
  RETURN_NOT_OK_PREPEND(new_helper->Init(jwks_uri,
                                         jwks_verify_server_certificate_),
                        "Error initializing JWT helper");

  {
    const std::lock_guard<simple_spinlock> l(jwt_by_account_id_map_lock_);
    LookupOrEmplace(&jwt_by_account_id_, account_id, std::move(new_helper));
    *helper = FindPointeeOrNull(jwt_by_account_id_, account_id);
  }

  return Status::OK();
}

Status PerAccountKeyBasedJwtVerifier::Init() const {
  for (auto& [account_id, verifier] : jwt_by_account_id_) {
    RETURN_NOT_OK(verifier->Init(Substitute("$0?accountId=$1", oidc_uri_, account_id),
                                            jwks_verify_server_certificate_));
  }
  return Status::OK();
}

Status PerAccountKeyBasedJwtVerifier::VerifyToken(const string& bytes_raw, string* subject) const {
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  RETURN_NOT_OK(JWTHelper::Decode(bytes_raw, decoded_token));
  JWTHelper* jwt;
  RETURN_NOT_OK(JWTHelperForToken(*decoded_token, &jwt));
  RETURN_NOT_OK(jwt->Verify(decoded_token.get()));
  if (!decoded_token->decoded_jwt_.has_subject()) {
    return Status::InvalidArgument("token does not include subject");
  }
  *subject = decoded_token->decoded_jwt_.get_subject();
  return Status::OK();
}

} // namespace kudu
