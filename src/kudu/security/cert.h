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

#include <memory>
#include <string>
#include <vector>

#include <openssl/asn1.h>

#include "kudu/gutil/port.h"
#include "kudu/security/openssl_util.h"

typedef struct X509_name_st X509_NAME;

namespace boost {
template <class T>
class optional;
}

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

// A wrapper class around the STACK_OF(X509) object. This can either hold one certificate or
// a chain of certificates.
// TODO(unknown): Currently, there isn't a mechanism to add to the chain. Implement it when needed.
class Cert : public RawDataWrapper<STACK_OF(X509)> {
 public:
  Status FromString(const std::string& data, DataFormat format) WARN_UNUSED_RESULT;
  Status ToString(std::string* data, DataFormat format) const WARN_UNUSED_RESULT;
  Status FromFile(const std::string& fpath, DataFormat format) WARN_UNUSED_RESULT;

  int chain_len() const { return sk_X509_num(data_.get()); }

  std::string SubjectName() const;
  std::string IssuerName() const;

  // Return DNS names from the SAN extension field of the end-user cert.
  std::vector<std::string> Hostnames() const;

  // Return the 'userId' extension of the end-user cert, if set.
  boost::optional<std::string> UserId() const;

  // Return the Kerberos principal encoded in the end-user certificate, if set.
  boost::optional<std::string> KuduKerberosPrincipal() const;

  // Check whether the specified private key matches the end-user certificate.
  // Return Status::OK() if key match the end-user certificate.
  Status CheckKeyMatch(const PrivateKey& key) const WARN_UNUSED_RESULT;

  // Returns the 'tls-server-end-point' channel bindings for the end-user certificate as
  // specified in RFC 5929.
  Status GetServerEndPointChannelBindings(std::string* channel_bindings) const WARN_UNUSED_RESULT;

  // Adopts the provided STACK_OF(X509), and increments the reference count of the X509 cert
  // contained within it. Currently, only one certificate should be contained in the stack.
  void AdoptAndAddRefRawData(RawDataType* data);

  // Adopts the provided X509 certificate, and replaces the current underlying STACK_OF(X509).
  void AdoptX509(X509* cert);

  // Adopts the provided X509 certificate, increments its reference count and replaces the current
  // underlying STACK_OF(X509).
  void AdoptAndAddRefX509(X509* cert);

  // Returns the end-user certificate's public key.
  Status GetPublicKey(PublicKey* key) const WARN_UNUSED_RESULT;

  // Get the first certificate in the chain, otherwise known as the 'end-user' certificate.
  X509* GetTopOfChainX509() const;
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
