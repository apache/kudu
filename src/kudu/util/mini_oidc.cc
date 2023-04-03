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
#include "kudu/util/mini_oidc.h"

#include <chrono>
#include <functional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/defaults.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/jwt_test_certs.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/web_callback_registry.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

// $0: account_id
// $1: jwks_uri
const char kDiscoveryFormat[] = R"({
    "issuer": "auth0/$0",
    "token_endpoint": "dummy.endpoint.com",
    "response_types_supported": [
        "id_token"
    ],
    "claims_supported": [
        "sub",
        "aud",
        "iss",
        "exp"
    ],
    "subject_types_supported": [
        "public"
    ],
    "id_token_signing_alg_values_supported": [
        "RS256"
    ],
    "jwks_uri": "$1"
})";

void OidcDiscoveryHandler(const Webserver::WebRequest& req,
                          Webserver::PrerenderedWebResponse* resp,
                          const string& jwks_server_url) {
  const auto* account_id = FindOrNull(req.parsed_args, "accountid");
  if (!account_id) {
    resp->output << "expected 'accountId' query";
    resp->status_code = HttpStatusCode::BadRequest;
    return;
  }
  resp->output << Substitute(
      kDiscoveryFormat, *account_id, Substitute("$0/$1", jwks_server_url, *account_id));
  resp->status_code = HttpStatusCode::Ok;
}

}  // anonymous namespace

MiniOidc::MiniOidc(MiniOidcOptions options) : options_(std::move(options)) {
  if (options_.lifetime_ms == 0) {
    // Setting default TTL for JWTs to 60 minutes
    options_.lifetime_ms = 3600000;
  }
}

// Explicitly defined outside the header to ensure users of the header don't
// need to include member destructors.
MiniOidc::~MiniOidc() {
  Stop();
}

Status MiniOidc::Start() {
  // Start the JWKS server and register path handlers for each of the accounts
  // we've been configured to server.
  WebserverOptions jwks_opts;
  jwks_opts.port = 0;
  jwks_opts.bind_interface = "localhost";
  jwks_opts.certificate_file = options_.server_certificate;
  jwks_opts.private_key_file = options_.private_key_file;

  jwks_server_.reset(new Webserver(jwks_opts));

  for (const auto& [account_id, valid] : options_.account_ids) {
    jwks_server_->RegisterPrerenderedPathHandler(
        Substitute("/jwks/$0", account_id),
        account_id,
        [account_id = account_id, valid = valid](const Webserver::WebRequest&  /*req*/,
                                                 Webserver::PrerenderedWebResponse* resp) {
          // NOTE: 'kKid1' points at a valid key, while 'kKid2' points at an
          // invalid key.
          resp->output << Substitute(kJwksRsaFileFormat, kKid1,
                                     "RS256",
                                     valid ? kRsaPubKeyJwkN : kRsaInvalidPubKeyJwkN,
                                     kRsaPubKeyJwkE, kKid2,
                                     "RS256",
                                     kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE),
              resp->status_code = HttpStatusCode::Ok;
        },
        /*is_styled*/ false,
        /*is_on_nav_bar*/ false);
  }
  LOG(INFO) << "Starting JWKS server";
  RETURN_NOT_OK(jwks_server_->Start());
  vector<Sockaddr> advertised_addrs;
  Sockaddr addr;
  RETURN_NOT_OK(jwks_server_->GetAdvertisedAddresses(&advertised_addrs));
  // calling ParseString() to verify the address components
  RETURN_NOT_OK(addr.ParseString(advertised_addrs[0].host(), advertised_addrs[0].port()));
  string protocol = "https";
  if (jwks_opts.certificate_file.empty() && jwks_opts.password_file.empty()) {
    protocol = "http";
  }

  const string jwks_url = Substitute("$0://localhost:$1/jwks",
                                     protocol,
                                     advertised_addrs[0].port());

  // Now start the OIDC Discovery server that points to the JWKS endpoints.
  WebserverOptions oidc_opts;
  oidc_opts.port = 0;
  oidc_opts.bind_interface = "localhost";
  oidc_server_.reset(new Webserver(oidc_opts));
  oidc_server_->RegisterPrerenderedPathHandler(
      "/.well-known/openid-configuration",
      "openid-configuration",
      // Pass the 'accountId' query arguments to return a response that
      // points to the JWKS endpoint for the account.
      [jwks_url](const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        OidcDiscoveryHandler(req, resp, jwks_url);
      },
      /*is_styled*/ false,
      /*is_on_nav_bar*/ false);

  LOG(INFO) << "Starting OIDC Discovery server";
  RETURN_NOT_OK(oidc_server_->Start());
  advertised_addrs.clear();
  RETURN_NOT_OK(oidc_server_->GetAdvertisedAddresses(&advertised_addrs));
  // calling ParseString() to verify the address components
  RETURN_NOT_OK(addr.ParseString(advertised_addrs[0].host(), advertised_addrs[0].port()));
  oidc_url_ = Substitute("http://$0/.well-known/openid-configuration", addr.ToString());
  return Status::OK();
}

void MiniOidc::Stop() {
  if (oidc_server_) {
    oidc_server_->Stop();
  }
  if (jwks_server_) {
    jwks_server_->Stop();
  }
}

string MiniOidc::CreateJwt(const string& account_id, const string& subject, bool is_valid) {
  auto lifetime = std::chrono::milliseconds(options_.lifetime_ms);

  return jwt::create()
      .set_issuer(Substitute("auth0/$0", account_id))
      .set_type("JWT")
      .set_algorithm("RS256")
      .set_key_id(is_valid ? kKid1 : kKid2)
      .set_subject(subject)
      .set_not_before(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + lifetime)
      .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
}

}  // namespace kudu
