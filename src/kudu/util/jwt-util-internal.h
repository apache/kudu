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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <jwt-cpp/jwt.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/logging.h"
#include "kudu/util/promise.h"
#include "kudu/util/thread.h"
#include "kudu/util/status.h"

using DecodedJWT = jwt::decoded_jwt<jwt::traits::kazuho_picojson>;
using JWTVerifier = jwt::verifier<jwt::default_clock, jwt::traits::kazuho_picojson>;

namespace kudu {

// Key-Value map for parsing Json keys.
typedef std::unordered_map<std::string, std::string> JsonKVMap;

// JWTPublicKey:
// This class represent cryptographic public key for JSON Web Token (JWT) verification.
class JWTPublicKey {
 public:
  JWTPublicKey(std::string algorithm, std::string pub_key)
  : verifier_(jwt::verify()), algorithm_(std::move(algorithm)), public_key_(std::move(pub_key)) {}

  // Verify the given decoded token.
  Status Verify(const DecodedJWT& decoded_jwt, const std::string& algorithm) const;

  const std::string& get_algorithm() const { return algorithm_; }
  const std::string& get_key() const { return public_key_; }

 protected:
  // JWT Verifier.
  JWTVerifier verifier_;

 private:
  // Signing Algorithm:
  // Currently support following JSON Web Algorithms (JWA):
  // HS256, HS384, HS512, RS256, RS384, and RS512.
  const std::string algorithm_;
  // Public key value:
  // For EC and RSA families of algorithms, it's the public key converted in PEM-encoded
  // format since jwt-cpp APIs only accept EC/RSA public keys in PEM-encoded format.
  // For HMAC-SHA2, it's Octet Sequence key representing secret key.
  const std::string public_key_;
};

// JWT Public Key for HS256.
// HS256: HMAC using SHA-256.
class HS256JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  HS256JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::hs256(pub_key));
  }
};

// JWT Public Key for HS384.
// HS384: HMAC using SHA-384.
class HS384JWTPublicKey : public JWTPublicKey {
 public:
  // Throw exception if failed to initialize the JWT verifier.
  HS384JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::hs384(pub_key));
  }
};

// JWT Public Key for HS512.
// HS512: HMAC using SHA-512.
class HS512JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  HS512JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::hs512(pub_key));
  }
};

// JWT Public Key for RS256.
// RS256: RSASSA-PKCS1-v1_5 using SHA-256.
class RS256JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  RS256JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::rs256(pub_key, "", "", ""));
  }
};

// JWT Public Key for RS384.
// RS384: RSASSA-PKCS1-v1_5 using SHA-384.
class RS384JWTPublicKey : public JWTPublicKey {
 public:
  // Throw exception if failed to initialize the JWT verifier.
  RS384JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::rs384(pub_key, "", "", ""));
  }
};

// JWT Public Key for RS512.
// RS512: RSASSA-PKCS1-v1_5 using SHA-512.
class RS512JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  RS512JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::rs512(pub_key, "", "", ""));
  }
};

// JWT Public Key for PS256.
// PS256: RSASSA-PSS using SHA-256 and MGF1 with SHA-256.
// RSASSA-PSS is the probabilistic version of RSA.
class PS256JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  PS256JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::ps256(pub_key, "", "", ""));
  }
};

// JWT Public Key for PS384.
// PS384: RSASSA-PSS using SHA-384 and MGF1 with SHA-384.
class PS384JWTPublicKey : public JWTPublicKey {
 public:
  // Throw exception if failed to initialize the JWT verifier.
  PS384JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::ps384(pub_key, "", "", ""));
  }
};

// JWT Public Key for PS512.
// PS512: RSASSA-PSS using SHA-512 and MGF1 with SHA-512.
class PS512JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  PS512JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::ps512(pub_key, "", "", ""));
  }
};

// JWT Public Key for ES256.
// ES256: ECDSA using P-256 and SHA-256.
class ES256JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  ES256JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::es256(pub_key, "", "", ""));
  }
};

// JWT Public Key for ES384.
// ES384: ECDSA using P-384 and SHA-384.
class ES384JWTPublicKey : public JWTPublicKey {
 public:
  // Throw exception if failed to initialize the JWT verifier.
  ES384JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::es384(pub_key, "", "", ""));
  }
};

// JWT Public Key for ES512.
// ES512: ECDSA using P-521 and SHA-512.
class ES512JWTPublicKey : public JWTPublicKey {
 public:
  // Throw JWT exception if failed to initialize the verifier.
  ES512JWTPublicKey(std::string algorithm, const std::string& pub_key)
      : JWTPublicKey(std::move(algorithm), pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::es512(pub_key, "", "", ""));
  }
};

// Construct a JWKPublicKey of HS from the JWK.
class HSJWTPublicKeyBuilder {
 public:
  static Status CreateJWKPublicKey(const JsonKVMap& kv_map,
                                   std::unique_ptr<JWTPublicKey>* pub_key_out);
};

// Construct a JWKPublicKey of RSA from the JWK.
class RSAJWTPublicKeyBuilder {
 public:
  static Status CreateJWKPublicKey(const JsonKVMap& kv_map,
                                   std::unique_ptr<JWTPublicKey>* pub_key_out);

 private:
  // Convert public key of RSA from JWK format to PEM encoded format by using OpenSSL
  // APIs.
  static bool ConvertJwkToPem(
      const std::string& base64_n, const std::string& base64_e, std::string& pub_key);
};

// Construct a JWKPublicKey of EC from the JWK.
class ECJWTPublicKeyBuilder {
 public:
  static Status CreateJWKPublicKey(const JsonKVMap& kv_map,
                                   std::unique_ptr<JWTPublicKey>* pub_key_out);

 private:
  // Convert public key of EC from JWK format to PEM encoded format by using OpenSSL
  // APIs.
  static bool ConvertJwkToPem(int eccgrp, const std::string& base64_x,
      const std::string& base64_y, std::string& pub_key);
};

// This class load the JWKS from file or URL, store keys in an internal maps for each
// family of algorithms, and provides API to retrieve key by key-id.
// It's a snapshot of the current JWKS. The JWKSMgr maintains a consistent copy of this
// and updates it atomically when the public keys in JWKS are changed. Clients can obtain
// an immutable copy. Class instances can be created through the implicitly-defined
// default and copy constructors.
class JWKSSnapshot final {
 public:
  JWKSSnapshot() = default;
  JWKSSnapshot(const JWKSSnapshot&) = default;

  // Map from a key ID (kid) to a JWTPublicKey.
  typedef std::unordered_map<std::string, std::unique_ptr<JWTPublicKey>> JWTPublicKeyMap;

  // Load JWKS stored in a JSON file. Returns an error if problems were encountered
  // while parsing/constructing the Json Web keys. If no keys were given in the file,
  // the internal maps will be empty.
  Status LoadKeysFromFile(const std::string& jwks_file_path);
  // Download JWKS JSON file from the given URL, then load the public keys if the
  // checksum of JWKS object is changed. If no keys were given in the URL, the internal
  // maps will be empty.
  Status LoadKeysFromUrl(const std::string& jwks_url, bool jwks_verify_server_certificate,
                         uint64_t cur_jwks_hash, bool* is_changed);


  // Look up the key ID in the internal key maps and returns the key if the lookup was
  // successful, otherwise return nullptr.
  const JWTPublicKey* LookupRSAPublicKey(const std::string& kid) const;
  const JWTPublicKey* LookupHSKey(const std::string& kid) const;
  const JWTPublicKey* LookupECPublicKey(const std::string& kid) const;

  // Return number of keys for each family of algorithms.
  int GetHSKeyNum() const { return static_cast<int>(hs_key_map_.size()); }
  // Return number of keys for RSA.
  int GetRSAPublicKeyNum() const { return static_cast<int>(rsa_pub_key_map_.size()); }
  // Return number of keys for EC.
  int GetECPublicKeyNum() const { return static_cast<int>(ec_pub_key_map_.size()); }

  // Return all keys for HS.
  const JWTPublicKeyMap* GetAllHSKeys() const { return &hs_key_map_; }
  // Return all keys for RSA.
  const JWTPublicKeyMap* GetAllRSAPublicKeys() const { return &rsa_pub_key_map_; }
  // Return all keys for EC.
  const JWTPublicKeyMap* GetAllECPublicKeys() const { return &ec_pub_key_map_; }

  // Return TRUE if there is no key.
  bool IsEmpty() const {
    return hs_key_map_.empty() && rsa_pub_key_map_.empty() && ec_pub_key_map_.empty();
  }

  uint64_t GetChecksum() const { return jwks_checksum_; }

 private:
  friend class JWKSetParser;

  // Following two functions are called inside Init().
  // Add a RSA public key.
  void AddRSAPublicKey(const std::string& key_id, std::unique_ptr<JWTPublicKey> jwk_pub_key);
  // Add a HS key.
  void AddHSKey(const std::string& key_id, std::unique_ptr<JWTPublicKey> jwk_pub_key);
  // Add an EC public key.
  void AddECPublicKey(const std::string& key_id, std::unique_ptr<JWTPublicKey> jwk_pub_key);

  // Note: According to section 4.5 of RFC 7517 (JSON Web Key), different keys might use
  // the same "kid" value is if they have different "kty" (key type) values but are
  // considered to be equivalent alternatives by the application using them. So keys
  // for each "kty" are saved in different maps.

  // Octet Sequence keys for HS256 (HMAC using SHA-256), HS384 and HS512.
  // kty (key type): oct.
  JWTPublicKeyMap hs_key_map_;
  // Public keys for RSA family of algorithms: RS256, RS384, RS512, PS256, PS384, PS512.
  // kty (key type): RSA.
  JWTPublicKeyMap rsa_pub_key_map_;
  // Public keys for EC family of algorithms: ES256, ES384, ES512.
  // kty (key type): EC.
  JWTPublicKeyMap ec_pub_key_map_;

  // 64 bit checksum of JWKS object.
  // This variable is only used when downloading JWKS from the given URL.
  uint64_t jwks_checksum_ = 0;
};

// An immutable shared JWKS snapshot.
typedef std::shared_ptr<const JWKSSnapshot> JWKSSnapshotPtr;

// JSON Web Key Set (JWKS) conveys the public keys used by the signing party to the
// clients that need to validate signatures. It represents a cryptographic key set in
// JSON data structure.
// This class works as JWKS manager, which load the JWKS from local file or URL.
// Init() should be called during the initialization of the daemon.
// The class is thread safe.
class JWKSMgr {
 public:
   JWKSMgr() {}

  // Destructor is only called for backend tests
  ~JWKSMgr();

  // Load JWKS stored in a JSON file. Returns an error if problems were encountered
  // while parsing/constructing the Json Web keys. If no keys were given in the file,
  // the internal maps will be empty.
  // If the given jwks_uri is a URL, start a working thread which will periodically
  // checks the JWKS URL for updates. This provides support for key rotation.
  Status Init(const std::string& jwks_uri, bool jwks_verify_server_certificate,
              bool is_local_file);

  // Returns a read only snapshot of the current JWKS. This function should be called
  // after calling Init().
  JWKSSnapshotPtr GetJWKSSnapshot() const;

 private:
  // Atomically replaces a JWKS snapshot with a new copy.
  void SetJWKSSnapshot(const JWKSSnapshotPtr& new_jwks);

  // Helper function for working thread which periodically checks the JWKS URL for
  // updates.
  void UpdateJWKSThread();

  // Thread that runs UpdateJWKSThread(). This thread will exit when the
  // shut_down_promise_ is set.
  scoped_refptr<Thread> jwks_update_thread_;
  Promise<bool> shut_down_promise_;

  // JWKS URI.
  std::string jwks_uri_;

  // JWKS insecure TLS
  bool jwks_verify_server_certificate_;

  // The snapshot of the current JWKS. When the checksum of downloaded JWKS json object
  // has been changed, the public keys will be reloaded and the content of this pointer
  // will be atomically swapped.
  JWKSSnapshotPtr current_jwks_;
  // 64 bit checksum of current JWKS object.
  uint64_t current_jwks_checksum_ = 0;

  // Protects current_jwks_.
  mutable std::mutex current_jwks_lock_;
};

} // namespace kudu
