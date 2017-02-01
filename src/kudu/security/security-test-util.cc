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

#include "kudu/security/security-test-util.h"

#include <glog/logging.h>

#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"

namespace kudu {
namespace security {

using ca::CaCertRequestGenerator;
using ca::CertSigner;

Status GenerateSelfSignedCAForTests(PrivateKey* ca_key, Cert* ca_cert) {
  // Create a key for the self-signed CA.
  RETURN_NOT_OK(GeneratePrivateKey(512, ca_key));

  CaCertRequestGenerator::Config config;
  config.uuid = "test-ca-uuid";
  RETURN_NOT_OK(CertSigner::SelfSignCA(*ca_key, config, ca_cert));
  return Status::OK();
}


} // namespace security
} // namespace kudu
