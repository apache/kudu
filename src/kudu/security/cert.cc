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

#include "kudu/security/cert.h"

#include <string>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/openssl_util_bio.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace security {

string X509NameToString(X509_NAME* name) {
  CHECK(name);
  auto bio = ssl_make_unique(BIO_new(BIO_s_mem()));
  OPENSSL_CHECK_OK(X509_NAME_print_ex(bio.get(), name, 0, XN_FLAG_ONELINE));

  BUF_MEM* membuf;
  OPENSSL_CHECK_OK(BIO_get_mem_ptr(bio.get(), &membuf));
  return string(membuf->data, membuf->length);
}

Status Cert::FromString(const std::string& data, DataFormat format) {
  return ::kudu::security::FromString(data, format, &data_);
}

Status Cert::ToString(std::string* data, DataFormat format) const {
  return ::kudu::security::ToString(data, format, data_.get());
}

Status Cert::FromFile(const std::string& fpath, DataFormat format) {
  return ::kudu::security::FromFile(fpath, format, &data_);
}

string Cert::SubjectName() const {
  return X509NameToString(X509_get_subject_name(data_.get()));
}

string Cert::IssuerName() const {
  return X509NameToString(X509_get_issuer_name(data_.get()));
}

Status Cert::CheckKeyMatch(const PrivateKey& key) const {
  OPENSSL_RET_NOT_OK(X509_check_private_key(data_.get(), key.GetRawData()),
                     "certificate does not match private key");
  return Status::OK();
}

Status Cert::GetServerEndPointChannelBindings(string* channel_bindings) const {
  // Find the signature type of the certificate. This corresponds to the digest
  // (hash) algorithm, and the public key type which signed the cert.

#if OPENSSL_VERSION_NUMBER >= 0x10002000L
  int signature_nid = X509_get_signature_nid(data_.get());
#else
  // Older version of OpenSSL appear not to have a public way to get the
  // signature digest method from a certificate. Instead, we reach into the
  // 'private' internals.
  int signature_nid = OBJ_obj2nid(data_->sig_alg->algorithm);
#endif

  // Retrieve the digest algorithm type.
  int digest_nid;
  int public_key_nid;
  OBJ_find_sigid_algs(signature_nid, &digest_nid, &public_key_nid);

  // RFC 5929: if the certificate's signatureAlgorithm uses no hash functions or
  // uses multiple hash functions, then this channel binding type's channel
  // bindings are undefined at this time (updates to is channel binding type may
  // occur to address this issue if it ever arises).
  //
  // TODO(dan): can the multiple hash function scenario actually happen? What
  // does OBJ_find_sigid_algs do in that scenario?
  if (digest_nid == NID_undef) {
    return Status::NotSupported("server certificate has no signature digest (hash) algorithm");
  }

  // RFC 5929: if the certificate's signatureAlgorithm uses a single hash
  // function, and that hash function is either MD5 [RFC1321] or SHA-1
  // [RFC3174], then use SHA-256 [FIPS-180-3];
  if (digest_nid == NID_md5 || digest_nid == NID_sha1) {
    digest_nid = NID_sha256;
  }

  const EVP_MD* md = EVP_get_digestbynid(digest_nid);
  OPENSSL_RET_IF_NULL(md, "digest for nid not found");

  // Create a digest BIO. All data written to the BIO will be sent through the
  // digest (hash) function. The digest BIO requires a null BIO to writethrough to.
  auto null_bio = ssl_make_unique(BIO_new(BIO_s_null()));
  auto md_bio = ssl_make_unique(BIO_new(BIO_f_md()));
  OPENSSL_RET_NOT_OK(BIO_set_md(md_bio.get(), md), "failed to set digest for BIO");
  BIO_push(md_bio.get(), null_bio.get());

  // Write the cert to the digest BIO.
  RETURN_NOT_OK(ToBIO(md_bio.get(), DataFormat::DER, data_.get()));

  // Read the digest from the BIO and append it to 'channel_bindings'.
  char buf[EVP_MAX_MD_SIZE];
  int digest_len = BIO_gets(md_bio.get(), buf, sizeof(buf));
  OPENSSL_RET_NOT_OK(digest_len, "failed to get cert digest from BIO");
  channel_bindings->assign(buf, digest_len);
  return Status::OK();
}

void Cert::AdoptAndAddRefRawData(X509* data) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  CHECK_GT(CRYPTO_add(&data->references, 1, CRYPTO_LOCK_X509), 1) << "X509 use-after-free detected";
#else
  OPENSSL_CHECK_OK(X509_up_ref(data)) << "X509 use-after-free detected: " << GetOpenSSLErrors();
#endif
  AdoptRawData(data);
}

Status Cert::GetPublicKey(PublicKey* key) const {
  EVP_PKEY* raw_key = X509_get_pubkey(data_.get());
  OPENSSL_RET_IF_NULL(raw_key, "unable to get certificate public key");
  key->AdoptRawData(raw_key);
  return Status::OK();
}

Status CertSignRequest::FromString(const std::string& data, DataFormat format) {
  return ::kudu::security::FromString(data, format, &data_);
}

Status CertSignRequest::ToString(std::string* data, DataFormat format) const {
  return ::kudu::security::ToString(data, format, data_.get());
}

Status CertSignRequest::FromFile(const std::string& fpath, DataFormat format) {
  return ::kudu::security::FromFile(fpath, format, &data_);
}

CertSignRequest CertSignRequest::Clone() const {
  CHECK_GT(CRYPTO_add(&data_->references, 1, CRYPTO_LOCK_X509_REQ), 1)
    << "X509_REQ use-after-free detected";

  CertSignRequest clone;
  clone.AdoptRawData(GetRawData());
  return clone;
}

Status CertSignRequest::GetPublicKey(PublicKey* key) const {
  EVP_PKEY* raw_key = X509_REQ_get_pubkey(data_.get());
  OPENSSL_RET_IF_NULL(raw_key, "unable to get CSR public key");
  key->AdoptRawData(raw_key);
  return Status::OK();
}

} // namespace security
} // namespace kudu
