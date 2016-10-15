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

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

// Forward declarations for the relevant OpenSSL typedefs
// in addition to openssl_util.h.
typedef struct asn1_string_st ASN1_INTEGER;
typedef struct env_md_st EVP_MD;
typedef struct rsa_st RSA;
typedef struct x509_st X509;
typedef struct X509_req_st X509_REQ;

// STACK_OF(X509_EXTENSION)
struct stack_st_X509_EXTENSION; // IWYU pragma: keep

namespace kudu {
namespace security {

class Cert;
class CertSignRequest;
class PrivateKey;

namespace ca {

// Base utility class for issuing X509 CSRs.
class CertRequestGeneratorBase {
 public:
  CertRequestGeneratorBase() = default;
  virtual ~CertRequestGeneratorBase() = default;

  virtual Status Init() = 0;
  virtual bool Initialized() const = 0;

  // Generate X509 CSR using the specified key. To obtain the key,
  // call the GeneratePrivateKey() function.
  Status GenerateRequest(const PrivateKey& key, CertSignRequest* ret) const WARN_UNUSED_RESULT;

 protected:
  // Push the specified extension into the stack provided.
  static Status PushExtension(stack_st_X509_EXTENSION* st,
                              int32_t nid,
                              StringPiece value) WARN_UNUSED_RESULT;

  // Set the certificate-specific subject fields into the specified request.
  virtual Status SetSubject(X509_REQ* req) const = 0;

  // Set the certificate-specific extensions into the specified request.
  virtual Status SetExtensions(X509_REQ* req) const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(CertRequestGeneratorBase);
};

// An utility class that facilitates issuing certificate signing requests
// (a.k.a. X509 CSRs).
class CertRequestGenerator : public CertRequestGeneratorBase {
 public:
  // Properties for the generated X509 CSR. The 'hostname' is for the name of
  // the machine the requestor is to use the certificate at. Valid configuration
  // should contain non-empty 'hostname' field.
  struct Config {
    // FQDN name to put into the 'DNS' fields of the subjectAltName extension.
    std::string hostname;
    // userId (UID)
    boost::optional<std::string> user_id;
    // Our custom extension which stores the full Kerberos principal for IPKI certs.
    boost::optional<std::string> kerberos_principal;
  };

  // 'config' contains the properties to fill into the X509 attributes of the
  // CSR.
  explicit CertRequestGenerator(Config config);
  ~CertRequestGenerator();

  Status Init() override WARN_UNUSED_RESULT;
  bool Initialized() const override;

  CertRequestGenerator& enable_self_signing() {
    CHECK(!is_initialized_);
    for_self_signing_ = true;
    return *this;
  }

 protected:
  Status SetSubject(X509_REQ* req) const override WARN_UNUSED_RESULT;
  Status SetExtensions(X509_REQ* req) const override WARN_UNUSED_RESULT;

 private:
  const Config config_;
  stack_st_X509_EXTENSION* extensions_ = nullptr;
  bool is_initialized_ = false;
  bool for_self_signing_ = false;
};

// An utility class that facilitates issuing of root CA self-signed certificate
// signing requests.
class CaCertRequestGenerator : public CertRequestGeneratorBase {
 public:
  // Properties for the generated X509 CA CSR.
  struct Config {
    // Common name (CN); e.g. 'master 239D6D2F-BDD2-4463-8933-78D9559C2124'.
    // Don't put hostname/FQDN in here: for CA cert it does not make sense and
    // it might be longer than 64 characters which is the limit specified
    // by RFC5280. The limit is enforced by the OpenSSL library.
    std::string cn;
  };

  explicit CaCertRequestGenerator(Config config);
  ~CaCertRequestGenerator();

  Status Init() override WARN_UNUSED_RESULT;
  bool Initialized() const override;

 protected:
  Status SetSubject(X509_REQ* req) const override WARN_UNUSED_RESULT;
  Status SetExtensions(X509_REQ* req) const override WARN_UNUSED_RESULT;

 private:
  const Config config_;
  stack_st_X509_EXTENSION* extensions_;
  mutable simple_spinlock lock_;
  bool is_initialized_; // protected by lock_
};

// An utility class for issuing and signing certificates.
//
// This is used in "fluent" style. For example:
//
//    CHECK_OK(CertSigner(&my_ca_cert, &my_ca_key)
//      .set_expiration_interval(MonoDelta::FromSeconds(3600))
//      .Sign(csr, &cert));
//
// As such, this class is not guaranteed thread-safe.
class CertSigner {
 public:
  // Generate a self-signed certificate authority using the given key
  // and CSR configuration.
  static Status SelfSignCA(const PrivateKey& key,
                           CaCertRequestGenerator::Config config,
                           int64_t cert_expiration_seconds,
                           Cert* cert) WARN_UNUSED_RESULT;

  // Generate a self-signed certificate using the given key and CSR
  // configuration.
  static Status SelfSignCert(const PrivateKey& key,
                             CertRequestGenerator::Config config,
                             Cert* cert) WARN_UNUSED_RESULT;

  // Create a CertSigner.
  //
  // The given cert and key must stay valid for the lifetime of the
  // cert signer. See class documentation above for recommended usage.
  //
  // 'ca_cert' may be nullptr in order to perform self-signing (though
  // the SelfSignCA() static method above is recommended).
  CertSigner(const Cert* ca_cert, const PrivateKey* ca_private_key);
  ~CertSigner() = default;

  // Set the expiration interval for certs signed by this signer.
  // This may be changed at any point.
  CertSigner& set_expiration_interval(MonoDelta expiration) {
    exp_interval_sec_ = expiration.ToSeconds();
    return *this;
  }

  Status Sign(const CertSignRequest& req, Cert* ret) const WARN_UNUSED_RESULT;

 private:

  static Status CopyExtensions(X509_REQ* req, X509* x) WARN_UNUSED_RESULT;
  static Status FillCertTemplateFromRequest(X509_REQ* req, X509* tmpl) WARN_UNUSED_RESULT;
  static Status DigestSign(const EVP_MD* md, EVP_PKEY* pkey, X509* x) WARN_UNUSED_RESULT;
  static Status GenerateSerial(c_unique_ptr<ASN1_INTEGER>* ret) WARN_UNUSED_RESULT;

  Status DoSign(const EVP_MD* digest, int32_t exp_seconds, X509 *ret) const WARN_UNUSED_RESULT;

  // The expiration interval of certs signed by this signer.
  int32_t exp_interval_sec_ = 24 * 60 * 60;

  // The CA cert. null if this CertSigner is configured for self-signing.
  const Cert* const ca_cert_;

  // The CA private key. If configured for self-signing, this is the
  // private key associated with the target cert.
  const PrivateKey* const ca_private_key_;

  DISALLOW_COPY_AND_ASSIGN(CertSigner);
};

} // namespace ca
} // namespace security
} // namespace kudu
