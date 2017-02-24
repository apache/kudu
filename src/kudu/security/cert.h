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

#include <string>

#include <boost/optional/optional_fwd.hpp>

#include "kudu/security/openssl_util.h"

namespace kudu {

class Status;

namespace security {

class PrivateKey;
class PublicKey;

// Convert an X509_NAME object to a human-readable string.
std::string X509NameToString(X509_NAME* name);

// Return the OpenSSL NID for the custom X509 extension where we store
// our Kerberos principal in IPKI certs.
int GetKuduKerberosPrincipalOidNid();

class Cert : public RawDataWrapper<X509> {
 public:
  Status FromString(const std::string& data, DataFormat format) WARN_UNUSED_RESULT;
  Status ToString(std::string* data, DataFormat format) const WARN_UNUSED_RESULT;
  Status FromFile(const std::string& fpath, DataFormat format) WARN_UNUSED_RESULT;

  std::string SubjectName() const;
  std::string IssuerName() const;

  // Return the 'userId' extension of this cert, if set.
  boost::optional<std::string> UserId() const;

  // Return the Kerberos principal encoded in this certificate, if set.
  boost::optional<std::string> KuduKerberosPrincipal() const;

  // Check whether the specified private key matches the certificate.
  // Return Status::OK() if key match the certificate.
  Status CheckKeyMatch(const PrivateKey& key) const WARN_UNUSED_RESULT;

  // Returns the 'tls-server-end-point' channel bindings for the certificate as
  // specified in RFC 5929.
  Status GetServerEndPointChannelBindings(std::string* channel_bindings) const WARN_UNUSED_RESULT;

  // Adopts the provided X509 certificate, and increments the reference count.
  void AdoptAndAddRefRawData(X509* data);

  // Returns the certificate's public key.
  Status GetPublicKey(PublicKey* key) const WARN_UNUSED_RESULT;
};

class CertSignRequest : public RawDataWrapper<X509_REQ> {
 public:
  Status FromString(const std::string& data, DataFormat format) WARN_UNUSED_RESULT;
  Status ToString(std::string* data, DataFormat format) const WARN_UNUSED_RESULT;
  Status FromFile(const std::string& fpath, DataFormat format) WARN_UNUSED_RESULT;

  // Returns a shallow clone of the CSR (only a reference count is incremented).
  CertSignRequest Clone() const;

  // Returns the CSR's public key.
  Status GetPublicKey(PublicKey* key) const WARN_UNUSED_RESULT;
};

} // namespace security
} // namespace kudu
