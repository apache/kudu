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
#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/util/status.h"
#include "kudu/gutil/strings/substitute.h"

// Forward declarations for the OpenSSL typedefs.
typedef struct bio_st BIO;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;
typedef struct x509_st X509;
typedef struct X509_req_st X509_REQ;

#define CERT_CHECK_OK(call) \
  CHECK_GT((call), 0)

#define CERT_RET_NOT_OK(call, msg) \
  if ((call) <= 0) { \
    return Status::RuntimeError(Substitute("$0: $1", \
        (msg), GetOpenSSLErrors())); \
  }

#define CERT_RET_IF_NULL(call, msg) \
  if ((call) == nullptr) { \
    return Status::RuntimeError(Substitute("$0: $1", \
        (msg), GetOpenSSLErrors())); \
  }

namespace kudu {
namespace security {

// Initializes static state required by the OpenSSL library.
//
// Safe to call multiple times.
void InitializeOpenSSL();

// Fetch the last error message from the OpenSSL library.
std::string GetOpenSSLErrors();

// A generic wrapper for OpenSSL structures.
template <typename T>
using c_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

// For each SSL type, the Traits class provides the important OpenSSL
// API functions.
template<class SSL_TYPE>
struct SslTypeTraits {};

template<class SSL_TYPE>
c_unique_ptr<SSL_TYPE> ssl_make_unique(SSL_TYPE* d) {
  return {d, SslTypeTraits<SSL_TYPE>::free};
}

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

} // namespace security
} // namespace kudu
