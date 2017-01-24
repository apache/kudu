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

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

// Forward declarations for the OpenSSL typedefs.
typedef struct bio_st BIO;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

#define OPENSSL_CHECK_OK(call) \
  CHECK_GT((call), 0)

#define OPENSSL_RET_NOT_OK(call, msg) \
  if ((call) <= 0) { \
    return Status::RuntimeError(::strings::Substitute("$0: $1", \
        (msg), GetOpenSSLErrors())); \
  }

#define OPENSSL_RET_IF_NULL(call, msg) \
  if ((call) == nullptr) { \
    return Status::RuntimeError(::strings::Substitute("$0: $1", \
        (msg), GetOpenSSLErrors())); \
  }

namespace kudu {
namespace security {

// Initializes static state required by the OpenSSL library.
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


// A generic wrapper for OpenSSL structures.
template <typename T>
using c_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

// For each SSL type, the Traits class provides the important OpenSSL
// API functions.
template<typename SSL_TYPE>
struct SslTypeTraits {};

template<typename SSL_TYPE, typename Traits = SslTypeTraits<SSL_TYPE>>
c_unique_ptr<SSL_TYPE> ssl_make_unique(SSL_TYPE* d) {
  return {d, Traits::free};
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

} // namespace security
} // namespace kudu
