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

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

// Forward declarations for the OpenSSL typedefs.
typedef struct X509_req_st X509_REQ;
typedef struct bio_st BIO;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;
typedef struct x509_st X509;

#define OPENSSL_CHECK_OK(call) \
  CHECK_GT((call), 0)

#define OPENSSL_RET_NOT_OK(call, msg) \
  if ((call) <= 0) { \
    return Status::RuntimeError((msg), GetOpenSSLErrors()); \
  }

#define OPENSSL_RET_IF_NULL(call, msg) \
  if ((call) == nullptr) { \
    return Status::RuntimeError((msg), GetOpenSSLErrors()); \
  }

// Scoped helper which DCHECKs that on both scope entry and exit, there are no
// pending OpenSSL errors for the current thread.
//
// This allows us to avoid calling ERR_clear_error() defensively before every
// OpenSSL call, but rather call it only when we get an error code indicating
// there may be some pending error.
//
// Example usage:
//
//    void MyFunc() {
//      SCOPED_OPENSSL_NO_PENDING_ERRORS;
//      ... use OpenSSL APIs ...
//    }
#define SCOPED_OPENSSL_NO_PENDING_ERRORS \
  kudu::security::internal::ScopedCheckNoPendingSSLErrors _no_ssl_errors(__PRETTY_FUNCTION__)

namespace kudu {
namespace security {

using PasswordCallback = std::function<std::string(void)>;

// Disable initialization of OpenSSL. Must be called before
// any call to InitializeOpenSSL().
Status DisableOpenSSLInitialization() WARN_UNUSED_RESULT;

// Initializes static state required by the OpenSSL library.
// This is a no-op if DisableOpenSSLInitialization() has been called.
//
// Safe to call multiple times.
void InitializeOpenSSL();

// Fetches errors from the OpenSSL error error queue, and stringifies them.
//
// The error queue will be empty after this method returns.
//
// See man(3) ERR_get_err for more discussion.
std::string GetOpenSSLErrors();

// Returns a string representation of the provided error code, which must be
// from a prior call to the SSL_get_error function.
//
// If necessary, the OpenSSL error queue may be inspected and emptied as part of
// this call, and/or 'errno' may be inspected. As a result, this method should
// only be used directly after the error occurs, and from the same thread.
//
// See man(3) SSL_get_error for more discussion.
std::string GetSSLErrorDescription(int error_code);

// Runs the shell command 'cmd' which should give a password to a private key file
// as the output.
//
// 'password' is populated with the password string if the command was a success.
// An error Status object is returned otherwise.
Status GetPasswordFromShellCommand(const std::string& cmd, std::string* password);

// A generic wrapper for OpenSSL structures.
template <typename T>
using c_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

// For each SSL type, the Traits class provides the important OpenSSL
// API functions.
template<typename SSL_TYPE>
struct SslTypeTraits {};

template<> struct SslTypeTraits<X509> {
  static constexpr auto kFreeFunc = &X509_free;
  static constexpr auto kReadPemFunc = &PEM_read_bio_X509;
  static constexpr auto kReadDerFunc = &d2i_X509_bio;
  static constexpr auto kWritePemFunc = &PEM_write_bio_X509;
  static constexpr auto kWriteDerFunc = &i2d_X509_bio;
};
template<> struct SslTypeTraits<X509_EXTENSION> {
  static constexpr auto kFreeFunc = &X509_EXTENSION_free;
};
template<> struct SslTypeTraits<X509_REQ> {
  static constexpr auto kFreeFunc = &X509_REQ_free;
  static constexpr auto kReadPemFunc = &PEM_read_bio_X509_REQ;
  static constexpr auto kReadDerFunc = &d2i_X509_REQ_bio;
  static constexpr auto kWritePemFunc = &PEM_write_bio_X509_REQ;
  static constexpr auto kWriteDerFunc = &i2d_X509_REQ_bio;
};
template<> struct SslTypeTraits<EVP_PKEY> {
  static constexpr auto kFreeFunc = &EVP_PKEY_free;
};
template<> struct SslTypeTraits<SSL_CTX> {
  static constexpr auto kFreeFunc = &SSL_CTX_free;
};

template<typename SSL_TYPE, typename Traits = SslTypeTraits<SSL_TYPE>>
c_unique_ptr<SSL_TYPE> ssl_make_unique(SSL_TYPE* d) {
  return {d, Traits::kFreeFunc};
}

// Acceptable formats for keys, X509 certificates and X509 CSRs.
enum class DataFormat {
  DER = 0,    // DER/ASN1 format (binary): for representing object on the wire
  PEM = 1,    // PEM format (ASCII): for storing on filesystem, printing, etc.
};

// Data format representation as a string.
const std::string& DataFormatToString(DataFormat fmt);

// Template wrapper for dynamically allocated entities with custom deleter.
// Mostly, using it for xxx_st types from the OpenSSL crypto library.
template<typename Type>
class RawDataWrapper {
 public:
  typedef Type RawDataType;

  RawDataType* GetRawData() const {
    return data_.get();
  }

  void AdoptRawData(RawDataType* d) {
    data_ = ssl_make_unique(d);
  }

 protected:
  c_unique_ptr<RawDataType> data_;
};


namespace internal {

// Implementation of SCOPED_OPENSSL_NO_PENDING_ERRORS. Use the macro form
// instead of directly instantiating the implementation class.
struct ScopedCheckNoPendingSSLErrors {
 public:
  explicit ScopedCheckNoPendingSSLErrors(const char* func)
      : func_(func) {
    DCHECK_EQ(ERR_peek_error(), 0)
        << "Expected no pending OpenSSL errors on " << func_
        << " entry, but had: " << GetOpenSSLErrors();
  }
  ~ScopedCheckNoPendingSSLErrors() {
    DCHECK_EQ(ERR_peek_error(), 0)
        << "Expected no pending OpenSSL errors on " << func_
        << " exit, but had: " << GetOpenSSLErrors();
  }

 private:
  const char* const func_;
};

} // namespace internal
} // namespace security
} // namespace kudu
