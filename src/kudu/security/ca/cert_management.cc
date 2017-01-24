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

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>

#include <glog/logging.h>
#include <openssl/conf.h>
#ifndef OPENSSL_NO_ENGINE
#include <openssl/engine.h>
#endif
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

using std::lock_guard;
using std::move;
using std::ostringstream;
using std::shared_ptr;
using std::string;
using strings::Substitute;

namespace kudu {
namespace security {

template<> struct SslTypeTraits<ASN1_INTEGER> {
  static constexpr auto free = &ASN1_INTEGER_free;
};
template<> struct SslTypeTraits<BIGNUM> {
  static constexpr auto free = &BN_free;
};
template<> struct SslTypeTraits<X509> {
  static constexpr auto free = &X509_free;
};
template<> struct SslTypeTraits<X509_REQ> {
  static constexpr auto free = &X509_REQ_free;
};
template<> struct SslTypeTraits<X509_EXTENSION> {
  static constexpr auto free = &X509_EXTENSION_free;
};
template<> struct SslTypeTraits<EVP_PKEY> {
  static constexpr auto free = &EVP_PKEY_free;
};

namespace ca {

CertRequestGeneratorBase::CertRequestGeneratorBase(Config config)
    : config_(move(config)) {
}

CertRequestGenerator::~CertRequestGenerator() {
  sk_X509_EXTENSION_pop_free(extensions_, X509_EXTENSION_free);
}

Status CertRequestGeneratorBase::GenerateRequest(const Key& key,
                                                 CertSignRequest* ret) const {
  CHECK(ret);
  CHECK(Initialized());
  auto req = ssl_make_unique(X509_REQ_new());
  CERT_RET_NOT_OK(X509_REQ_set_pubkey(req.get(), key.GetRawData()),
                  "error setting X509 public key");
  X509_NAME* name = X509_REQ_get_subject_name(req.get());
  CHECK(name);

#define CERT_SET_SUBJ_FIELD(field, code, err_msg) \
  do { \
    const string& f = (field); \
    if (!f.empty()) { \
      CERT_RET_NOT_OK(X509_NAME_add_entry_by_txt(name, (code), MBSTRING_ASC,  \
          reinterpret_cast<const unsigned char*>(f.c_str()), -1, -1, 0), \
         ("error setting subject " # err_msg)); \
    } \
  } while (false)

  CERT_SET_SUBJ_FIELD(config_.country, "C", "country");
  CERT_SET_SUBJ_FIELD(config_.state, "ST", "state");
  CERT_SET_SUBJ_FIELD(config_.locality, "L", "locality/city");
  CERT_SET_SUBJ_FIELD(config_.org, "O", "organization");
  CERT_SET_SUBJ_FIELD(config_.unit, "OU", "organizational unit");
  CERT_SET_SUBJ_FIELD(config_.uuid, "CN", "common name");
#undef CERT_SET_SUBJ_FIELD

  // Set necessary extensions into the request.
  RETURN_NOT_OK(SetExtensions(req.get()));

  // And finally sign the result.
  CERT_RET_NOT_OK(X509_REQ_sign(req.get(), key.GetRawData(), EVP_sha256()),
                  "error signing X509 request");
  ret->AdoptRawData(req.release());

  return Status::OK();
}

Status CertRequestGeneratorBase::PushExtension(stack_st_X509_EXTENSION* st,
                                               int32_t nid, const char* value) {
  auto ex = ssl_make_unique(
      X509V3_EXT_conf_nid(nullptr, nullptr, nid, const_cast<char*>(value)));
  if (!ex) {
    return Status::RuntimeError("error configuring extension");
  }
  CERT_RET_NOT_OK(sk_X509_EXTENSION_push(st, ex.release()),
                  "error pushing extension into the stack");
  return Status::OK();
}

CertRequestGenerator::CertRequestGenerator(Config config)
    : CertRequestGeneratorBase(config),
      extensions_(nullptr),
      is_initialized_(false) {
}

Status CertRequestGenerator::Init() {
  InitializeOpenSSL();

  lock_guard<simple_spinlock> guard(lock_);
  CHECK(!is_initialized_);
  if (config_.uuid.empty()) {
    return Status::InvalidArgument("missing end-entity UUID/name");
  }
  // Check that the config contain at least one entity (DNS name/IP address)
  // to bind the generated certificate.
  if (config_.hostnames.empty() && config_.ips.empty()) {
    return Status::InvalidArgument("SAN: missing DNS names and IP addresses");
  }

  extensions_ = sk_X509_EXTENSION_new_null();

  // Permitted usages for the generated keys is set via X509 V3
  // standard/extended key usage attributes.
  // See https://www.openssl.org/docs/man1.0.1/apps/x509v3_config.html
  // for details.

  // The generated certificates are for using as TLS certificates for
  // both client and server.
  RETURN_NOT_OK(PushExtension(extensions_, NID_key_usage,
                              "critical,digitalSignature,keyEncipherment"));
  // The generated certificates should be good for authentication
  // of a server to a client and vice versa: the intended users of the
  // certificates are tablet servers which are going to talk to master
  // and other tablet servers via TLS channels.
  RETURN_NOT_OK(PushExtension(extensions_, NID_ext_key_usage,
                              "critical,serverAuth,clientAuth"));
  // The generated certificates are not intended to be used as CA certificates
  // (i.e. they cannot be used to sign/issue certificates).
  RETURN_NOT_OK(PushExtension(extensions_, NID_basic_constraints,
                              "critical,CA:FALSE"));
  ostringstream san_hosts;
  for (size_t i = 0; i < config_.hostnames.size(); ++i) {
    const string& hostname = config_.hostnames[i];
    if (hostname.empty()) {
      // Basic validation: check for emptyness. Probably, more advanced
      // validation is needed here.
      return Status::InvalidArgument("SAN: an empty hostname");
    }
    if (i != 0) {
      san_hosts << ",";
    }
    san_hosts << "DNS." << i << ":" << hostname;
  }
  ostringstream san_ips;
  for (size_t i = 0; i < config_.ips.size(); ++i) {
    const string& ip = config_.ips[i];
    if (ip.empty()) {
      // Basic validation: check for emptyness. Probably, more advanced
      // validation is needed here.
      return Status::InvalidArgument("SAN: an empty IP address");
    }
    if (i != 0) {
      san_ips << ",";
    }
    san_ips << "IP." << i << ":" << ip;
  }
  // Encode hostname and IP address into the subjectAlternativeName attribute.
  const string alt_name = san_hosts.str() +
      ((!san_hosts.str().empty() && !san_ips.str().empty()) ? "," : "") +
      san_ips.str();
  RETURN_NOT_OK(PushExtension(extensions_, NID_subject_alt_name,
                              alt_name.c_str()));
  if (!config_.comment.empty()) {
    // Add the comment if it's not empty.
    RETURN_NOT_OK(PushExtension(extensions_, NID_netscape_comment,
                                config_.comment.c_str()));
  }
  is_initialized_ = true;

  return Status::OK();
}

bool CertRequestGenerator::Initialized() const {
  lock_guard<simple_spinlock> guard(lock_);
  return is_initialized_;
}

Status CertRequestGenerator::SetExtensions(X509_REQ* req) const {
  CERT_RET_NOT_OK(X509_REQ_add_extensions(req, extensions_),
                  "error setting X509 request extensions");
  return Status::OK();
}

CaCertRequestGenerator::CaCertRequestGenerator(Config config)
    : CertRequestGeneratorBase(config),
      extensions_(nullptr),
      is_initialized_(false) {
}

CaCertRequestGenerator::~CaCertRequestGenerator() {
  sk_X509_EXTENSION_pop_free(extensions_, X509_EXTENSION_free);
}

Status CaCertRequestGenerator::Init() {
  InitializeOpenSSL();

  lock_guard<simple_spinlock> guard(lock_);
  if (is_initialized_) {
    return Status::OK();
  }
  if (config_.uuid.empty()) {
    return Status::InvalidArgument("missing CA service UUID/name");
  }

  extensions_ = sk_X509_EXTENSION_new_null();

  // Permitted usages for the generated keys is set via X509 V3
  // standard/extended key usage attributes.
  // See https://www.openssl.org/docs/man1.0.1/apps/x509v3_config.html
  // for details.

  // The target ceritifcate is a CA certificate: it's for signing X509 certs.
  RETURN_NOT_OK(PushExtension(extensions_, NID_key_usage,
                              "critical,keyCertSign"));
  // The generated certificates are for the private CA service.
  RETURN_NOT_OK(PushExtension(extensions_, NID_basic_constraints,
                              "critical,CA:TRUE"));
  if (!config_.comment.empty()) {
    // Add the comment if it's not empty.
    RETURN_NOT_OK(PushExtension(extensions_, NID_netscape_comment,
                                config_.comment.c_str()));
  }
  is_initialized_ = true;

  return Status::OK();
}

bool CaCertRequestGenerator::Initialized() const {
  lock_guard<simple_spinlock> guard(lock_);
  return is_initialized_;
}

Status CaCertRequestGenerator::SetExtensions(X509_REQ* req) const {
  CERT_RET_NOT_OK(X509_REQ_add_extensions(req, extensions_),
                  "error setting X509 request extensions");
  return Status::OK();
}

CertSigner::CertSigner()
    : is_initialized_(false) {
}

Status CertSigner::Init(shared_ptr<Cert> ca_cert,
                        shared_ptr<Key> ca_private_key) {
  CHECK(ca_cert && ca_cert->GetRawData());
  CHECK(ca_private_key && ca_private_key->GetRawData());
  InitializeOpenSSL();

  std::lock_guard<simple_spinlock> guard(lock_);
  CHECK(!is_initialized_);

  ca_cert_ = std::move(ca_cert);
  ca_private_key_ = std::move(ca_private_key);
  is_initialized_ = true;
  return Status::OK();
}

Status CertSigner::InitForSelfSigning(shared_ptr<Key> private_key) {
  CHECK(private_key);
  InitializeOpenSSL();

  std::lock_guard<simple_spinlock> guard(lock_);
  CHECK(!is_initialized_);

  ca_private_key_ = std::move(private_key);
  is_initialized_ = true;
  return Status::OK();
}

Status CertSigner::InitFromFiles(const string& ca_cert_path,
                                 const string& ca_private_key_path) {
  InitializeOpenSSL();

  std::lock_guard<simple_spinlock> guard(lock_);
  CHECK(!is_initialized_);
  auto cert = std::make_shared<Cert>();
  auto key = std::make_shared<Key>();
  RETURN_NOT_OK(cert->FromFile(ca_cert_path, DataFormat::PEM));
  RETURN_NOT_OK(key->FromFile(ca_private_key_path,
                              DataFormat::PEM));
  CERT_RET_NOT_OK(X509_check_private_key(cert->GetRawData(),
                                         key->GetRawData()),
                  Substitute("$0, $1: CA certificate and private key "
                             "do not match",
                             ca_cert_path, ca_private_key_path));
  ca_cert_ = std::move(cert);
  ca_private_key_ = std::move(key);
  is_initialized_ = true;
  return Status::OK();
}

bool CertSigner::Initialized() const {
  lock_guard<simple_spinlock> guard(lock_);
  return is_initialized_;
}

const Cert& CertSigner::ca_cert() const {
  lock_guard<simple_spinlock> guard(lock_);
  DCHECK(is_initialized_);
  return *CHECK_NOTNULL(ca_cert_.get());
}

const Key& CertSigner::ca_private_key() const {
  lock_guard<simple_spinlock> guard(lock_);
  DCHECK(is_initialized_);
  return *ca_private_key_;
}

Status CertSigner::Sign(const CertSignRequest& req, Cert* ret) const {
  CHECK(ret);
  CHECK(Initialized());
  auto x509 = ssl_make_unique(X509_new());
  RETURN_NOT_OK(FillCertTemplateFromRequest(req.GetRawData(), x509.get()));
  RETURN_NOT_OK(DoSign(EVP_sha256(), exp_interval_sec_, x509.get()));
  ret->AdoptRawData(x509.release());

  return Status::OK();
}

// This is modeled after code in copy_extensions() function from
// $OPENSSL_ROOT/apps/apps.c with OpenSSL 1.0.2.
Status CertSigner::CopyExtensions(X509_REQ* req, X509* x) {
  CHECK(req);
  CHECK(x);
  STACK_OF(X509_EXTENSION)* exts = X509_REQ_get_extensions(req);
  auto exts_cleanup = MakeScopedCleanup([&exts]() {
    sk_X509_EXTENSION_pop_free(exts, X509_EXTENSION_free);
  });
  for (size_t i = 0; i < sk_X509_EXTENSION_num(exts); ++i) {
    X509_EXTENSION* ext = sk_X509_EXTENSION_value(exts, i);
    ASN1_OBJECT* obj = X509_EXTENSION_get_object(ext);
    int32_t idx = X509_get_ext_by_OBJ(x, obj, -1);
    if (idx != -1) {
      // If extension exits, delete all extensions of same type.
      do {
        auto tmpext = ssl_make_unique(X509_get_ext(x, idx));
        X509_delete_ext(x, idx);
        idx = X509_get_ext_by_OBJ(x, obj, -1);
      } while (idx != -1);
    }
    CERT_RET_NOT_OK(X509_add_ext(x, ext, -1), "error adding extension");
  }

  return Status::OK();
}

Status CertSigner::FillCertTemplateFromRequest(X509_REQ* req, X509* tmpl) {
  CHECK(req);
  if (!req->req_info ||
      !req->req_info->pubkey ||
      !req->req_info->pubkey->public_key ||
      !req->req_info->pubkey->public_key->data) {
    return Status::RuntimeError("corrupted CSR: no public key");
  }
  auto pub_key = ssl_make_unique(X509_REQ_get_pubkey(req));
  if (!pub_key) {
    return Status::RuntimeError("error unpacking public key from CSR");
  }
  const int rc = X509_REQ_verify(req, pub_key.get());
  if (rc < 0) {
    return Status::RuntimeError("CSR signature verification error");
  }
  if (rc == 0) {
    return Status::RuntimeError("CSR signature mismatch");
  }
  CERT_RET_NOT_OK(X509_set_subject_name(tmpl, X509_REQ_get_subject_name(req)),
                  "error setting cert subject name");
  RETURN_NOT_OK(CopyExtensions(req, tmpl));
  CERT_RET_NOT_OK(X509_set_pubkey(tmpl, pub_key.get()),
                  "error setting cert public key");
  return Status::OK();
}

Status CertSigner::DigestSign(const EVP_MD* md, EVP_PKEY* pkey, X509* x) {
  CERT_RET_NOT_OK(X509_sign(x, pkey, md), "error signing certificate");
  return Status::OK();
}

Status CertSigner::GenerateSerial(c_unique_ptr<ASN1_INTEGER>* ret) {
  auto btmp = ssl_make_unique(BN_new());
  CERT_RET_NOT_OK(BN_pseudo_rand(btmp.get(), 64, 0, 0),
                  "error generating random number");
  auto serial = ssl_make_unique(ASN1_INTEGER_new());
  CERT_RET_IF_NULL(BN_to_ASN1_INTEGER(btmp.get(), serial.get()),
                   "error converting number into ASN1 representation");
  if (ret) {
    ret->swap(serial);
  }
  return Status::OK();
}

Status CertSigner::DoSign(const EVP_MD* digest, int32_t exp_seconds,
                          X509* ret) const {
  CHECK(ret);

  // Version 3 (v3) of X509 certificates. The integer value is one less
  // than the version it represents. This is not a typo. :)
  static const int kX509V3 = 2;

  // If we have a CA cert, then the CA is the issuer.
  // Otherwise, we are self-signing so the target cert is also the issuer.
  X509* issuer_cert = ca_cert_ ? ca_cert_->GetRawData() : ret;
  X509_NAME* issuer_name = X509_get_subject_name(issuer_cert);
  CERT_RET_NOT_OK(X509_set_issuer_name(ret, issuer_name),
                  "error setting issuer name");
  c_unique_ptr<ASN1_INTEGER> serial;
  RETURN_NOT_OK(GenerateSerial(&serial));
  // set version to v3
  CERT_RET_NOT_OK(X509_set_version(ret, kX509V3), "error setting cert version");
  CERT_RET_NOT_OK(X509_set_serialNumber(ret, serial.get()),
                  "error setting cert serial");
  CERT_RET_IF_NULL(X509_gmtime_adj(X509_get_notBefore(ret), 0L),
                   "error setting cert validity time");
  CERT_RET_IF_NULL(X509_gmtime_adj(X509_get_notAfter(ret), exp_seconds),
                   "error setting cert expiration time");
  RETURN_NOT_OK(DigestSign(digest, ca_private_key_->GetRawData(), ret));

  return Status::OK();
}

} // namespace ca
} // namespace security
} // namespace kudu
