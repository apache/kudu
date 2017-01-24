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

#include <openssl/pem.h>
#include <openssl/x509.h>

#include "kudu/security/openssl_util.h"
#include "kudu/security/openssl_util_bio.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace security {

template<> struct SslTypeTraits<X509> {
  static constexpr auto free = &X509_free;
  static constexpr auto read_pem = &PEM_read_bio_X509;
  static constexpr auto read_der = &d2i_X509_bio;
  static constexpr auto write_pem = &PEM_write_bio_X509;
  static constexpr auto write_der = &i2d_X509_bio;
};
template<> struct SslTypeTraits<X509_REQ> {
  static constexpr auto free = &X509_REQ_free;
  static constexpr auto read_pem = &PEM_read_bio_X509_REQ;
  static constexpr auto read_der = &d2i_X509_REQ_bio;
  static constexpr auto write_pem = &PEM_write_bio_X509_REQ;
  static constexpr auto write_der = &i2d_X509_REQ_bio;
};

Status Cert::FromString(const std::string& data, DataFormat format) {
  return ::kudu::security::FromString(data, format, &data_);
}

Status Cert::ToString(std::string* data, DataFormat format) const {
  return ::kudu::security::ToString(data, format, data_.get());
}

Status Cert::FromFile(const std::string& fpath, DataFormat format) {
  return ::kudu::security::FromFile(fpath, format, &data_);
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

} // namespace security
} // namespace kudu
