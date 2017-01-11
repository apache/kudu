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

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

// Forward declarations for the relevant OpenSSL typedefs
// in addition to openssl_util.h.
typedef struct asn1_string_st ASN1_INTEGER;
typedef struct env_md_st EVP_MD;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct rsa_st RSA;
typedef struct x509_st X509;
typedef struct X509_req_st X509_REQ;
struct stack_st_X509_EXTENSION; // STACK_OF(X509_EXTENSION)

namespace kudu {
namespace security {
namespace ca {

template <typename T>
using c_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

// Acceptable formats for X509 certificates, X509 CSRs, and private keys.
enum class DataFormat {
  DER = 0,    // DER/ASN1 format (binary)
  PEM = 1,    // PEM format (ASCII)
};

// Data format representation as a string.
const std::string& DataFormatToString(DataFormat fmt);

// Basic wrapper for objects of xxx_st type in the OpenSSL crypto library.
class BasicWrapper {
 public:
  virtual ~BasicWrapper() = default;

  Status FromFile(const std::string& fpath, DataFormat format);
  Status FromString(const std::string& data, DataFormat format);

  Status ToString(std::string* data, DataFormat format) const;

 protected:
  virtual Status FromBIO(BIO* bio, DataFormat format) = 0;
  virtual Status ToBIO(BIO* bio, DataFormat format) const = 0;
};

// A wrapper for a private key.
class Key : public BasicWrapper {
 public:
  typedef EVP_PKEY RawDataType;

  RawDataType* GetRawData() const {
    return data_.get();
  }

  void AdoptRawData(RawDataType* data);

 protected:
  Status FromBIO(BIO* bio, DataFormat format) override;
  Status ToBIO(BIO* bio, DataFormat format) const override;

 private:
  c_unique_ptr<RawDataType> data_;
};

// A wrapper for a X509 certificate.
class Cert : public BasicWrapper {
 public:
  typedef X509 RawDataType;

  RawDataType* GetRawData() const {
    return data_.get();
  }

  void AdoptRawData(RawDataType* data);

 protected:
  Status FromBIO(BIO* bio, DataFormat format) override;
  Status ToBIO(BIO* bio, DataFormat format) const override;

 private:
  c_unique_ptr<RawDataType> data_;
};

// A wrapper for a X509 CSR (certificate signing request).
class CertSignRequest : public BasicWrapper {
 public:
  typedef X509_REQ RawDataType;

  RawDataType* GetRawData() const {
    return data_.get();
  }

  void AdoptRawData(RawDataType* data);

 protected:
  Status FromBIO(BIO* bio, DataFormat format) override;
  Status ToBIO(BIO* bio, DataFormat format) const override;

 private:
  c_unique_ptr<RawDataType> data_;
};

// Utility method to generate private RSA keys.
Status GeneratePrivateKey(int num_bits, Key* ret);

// Base utility class for issuing X509 CSRs.
class CertRequestGeneratorBase {
 public:
  // Properties for the generated X509 CSR.  Using server UUID for the common
  // name field.
  struct Config {
    const std::string country;  // subject field: C
    const std::string state;    // subject field: ST
    const std::string locality; // subject field: L
    const std::string org;      // subject field: O
    const std::string unit;     // subject field: OU
    const std::string uuid;     // subject field: CN
    const std::string comment;  // custom extension: Netscape Comment
    const std::vector<std::string> hostnames; // subjectAltName extension (DNS:)
    const std::vector<std::string> ips;       // subjectAltName extension (IP:)
  };

  explicit CertRequestGeneratorBase(Config config);
  virtual ~CertRequestGeneratorBase() = default;

  virtual Status Init() = 0;
  virtual bool Initialized() const = 0;

  // Generate X509 CSR using the specified key. To obtain the key,
  // call the GeneratePrivateKey() function.
  Status GenerateRequest(const Key& key, CertSignRequest* ret) const;

 protected:
  // Push the specified extension into the stack provided.
  static Status PushExtension(stack_st_X509_EXTENSION* st, int32_t nid,
                              const char* value);
  // Set the certificate-specific extensions into the specified request.
  virtual Status SetExtensions(X509_REQ* req) const = 0;

  const Config config_;

 private:
  DISALLOW_COPY_AND_ASSIGN(CertRequestGeneratorBase);
};

// An utility class that facilitates issuing certificate signing requests
// (a.k.a. X509 CSRs).
class CertRequestGenerator : public CertRequestGeneratorBase {
 public:
  // The CertRequestGenerator object is bound to the server UUID, hostnames
  // and IP addresses specified by the 'config' parameter. The hostnames and
  // IP addresses are put into the X509v3 SAN extension (subject alternative
  // name, a.k.a. subjectAltName). The SAN can be used while verifying the
  // generated certificates during TLS handshake.
  explicit CertRequestGenerator(Config config);
  ~CertRequestGenerator();

  Status Init() override;
  bool Initialized() const override;

 protected:
  Status SetExtensions(X509_REQ* req) const override;

 private:
  stack_st_X509_EXTENSION* extensions_;
  mutable simple_spinlock lock_;
  bool is_initialized_; // protected by lock_
};

// An utility class that facilitates issuing of root CA self-signed certificate
// signing requests.
class CaCertRequestGenerator : public CertRequestGeneratorBase {
 public:
  explicit CaCertRequestGenerator(Config config);
  ~CaCertRequestGenerator();

  Status Init() override;
  bool Initialized() const override;

 protected:
  Status SetExtensions(X509_REQ* req) const override;

 private:
  stack_st_X509_EXTENSION* extensions_;
  mutable simple_spinlock lock_;
  bool is_initialized_; // protected by lock_
};

// An utility class for issuing and signing certificates.
class CertSigner {
 public:
  struct Config {
    const int32_t exp_interval_sec;
    const std::string ca_cert_path;
    const std::string ca_private_key_path;
  };

  explicit CertSigner(Config config);
  ~CertSigner() = default;

  Status Init();
  bool Initialized() const;

  const Cert& ca_cert() const;
  const Key& ca_private_key() const;

  Status Sign(const CertSignRequest& req, Cert* ret) const;

 private:
  static Status CopyExtensions(X509_REQ* req, X509* x);
  static Status FillCertTemplateFromRequest(X509_REQ* req, X509* tmpl);
  static Status DigestSign(const EVP_MD* md, EVP_PKEY* pkey, X509* x);
  static Status GenerateSerial(c_unique_ptr<ASN1_INTEGER>* ret);

  Status DoSign(const EVP_MD* digest, int32_t exp_seconds, X509 *ret) const;

  const std::string ca_cert_path_;
  const std::string ca_private_key_path_;
  const Config config_;
  mutable simple_spinlock lock_;
  bool is_initialized_; // protected by lock_
  Cert ca_cert_;
  Key ca_private_key_;

  DISALLOW_COPY_AND_ASSIGN(CertSigner);
};

} // namespace ca
} // namespace security
} // namespace kudu
