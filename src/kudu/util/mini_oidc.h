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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class Webserver;

struct MiniOidcOptions {
  // Port to bind to.
  //
  // Default: 0 (ephemeral port).
  uint16_t port = 0;

  // Lifetime of generated JWTs in milliseconds
  uint64_t lifetime_ms = 0;

  // Maps account IDs to add to whether or not to create JWKS with invalid keys.
  std::unordered_map<std::string, bool> account_ids;

  // String that contains the server_certificate that is used to establish secure
  // https connection to the JWKS server.
  std::string server_certificate;

  // The private key belonging to the server certificate
  std::string private_key_file;
};

// Serves the following endpoints for testing a cluster:
//
// OIDC Discovery Endpoint: <url>/.well-known/openid-configuration/<account-id>
// - Returns a JSON document containing the JWKS endpoint associated with the
//   given account ID, among other metadata
// - See https://swagger.io/docs/specification/authentication/openid-connect-discovery/
//   for more details
//
// JWKS Endpoint: <url>/jwks/<account-id>
// - Returns a JSON document containing the JWKS associated with the given
//   account ID
// - See https://auth0.com/docs/secure/tokens/json-web-tokens/json-web-key-sets
//   for more details
//
// Verification of a JWT associated with a given account ID (typically denoted
// as the final segment of the 'iss' field in the JWT payload) comprises of the
// following steps:
// - Query the OIDC Discovery Endpoint, specifying an 'accountId' argument from
//   the JWT.
// - The endpoint returns a document containing the field 'jwks_uri', the JWKS
//   Endpoint.
// - Query the JWKS Endpoint returns the JWKS associated with the account ID.
// - The JWKS is used to verify the JWT.
class MiniOidc {
 public:
  explicit MiniOidc(MiniOidcOptions options);
  ~MiniOidc();

  Status Start() WARN_UNUSED_RESULT;
  void Stop();

  // Creates a JWT with the given `account_id` and `subject`. If `is_valid` is set to false, the
  // created token will be invalid.
  std::string CreateJwt(const std::string& account_id,
                        const std::string& subject,
                        bool is_valid);

  const std::string& url() const {
    DCHECK(!oidc_url_.empty());
    return oidc_url_;
  }

  const std::string& jwks_url() const {
    DCHECK(!jwks_url_.empty());
    return jwks_url_;
  }
 private:
  MiniOidcOptions options_;

  std::unique_ptr<Webserver> oidc_server_;

  std::string oidc_url_;

  std::unique_ptr<Webserver> jwks_server_;

  std::string jwks_url_;
};

} // namespace kudu
