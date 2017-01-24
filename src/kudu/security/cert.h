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

#include "kudu/security/openssl_util.h"

// Forward declarations for the OpenSSL typedefs.
typedef struct x509_st X509;
typedef struct X509_req_st X509_REQ;

namespace kudu {

class Status;

namespace security {

class Cert : public RawDataWrapper<X509> {
 public:
  Status FromString(const std::string& data, DataFormat format);
  Status ToString(std::string* data, DataFormat format) const;
  Status FromFile(const std::string& fpath, DataFormat format);
};

class CertSignRequest : public RawDataWrapper<X509_REQ> {
 public:
  Status FromString(const std::string& data, DataFormat format);
  Status ToString(std::string* data, DataFormat format) const;
  Status FromFile(const std::string& fpath, DataFormat format);
};

} // namespace security
} // namespace kudu
