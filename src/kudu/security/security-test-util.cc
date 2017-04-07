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
#include "kudu/security/tls_context.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace security {

using ca::CaCertRequestGenerator;
using ca::CertSigner;

Status GenerateSelfSignedCAForTests(PrivateKey* ca_key, Cert* ca_cert) {
  static const int64_t kRootCaCertExpirationSeconds = 24 * 60 * 60;
  // Create a key for the self-signed CA.
  RETURN_NOT_OK(GeneratePrivateKey(512, ca_key));

  CaCertRequestGenerator::Config config = { "test-ca-cn" };
  RETURN_NOT_OK(CertSigner::SelfSignCA(*ca_key,
                                       config,
                                       kRootCaCertExpirationSeconds,
                                       ca_cert));
  return Status::OK();
}

std::ostream& operator<<(std::ostream& o, PkiConfig c) {
    switch (c) {
      case PkiConfig::NONE: o << "NONE"; break;
      case PkiConfig::SELF_SIGNED: o << "SELF_SIGNED"; break;
      case PkiConfig::TRUSTED: o << "TRUSTED"; break;
      case PkiConfig::SIGNED: o << "SIGNED"; break;
      case PkiConfig::EXTERNALLY_SIGNED: o << "EXTERNALLY_SIGNED"; break;
    }
    return o;
}

Status ConfigureTlsContext(PkiConfig config,
                           const Cert& ca_cert,
                           const PrivateKey& ca_key,
                           TlsContext* tls_context) {
  switch (config) {
    case PkiConfig::NONE: break;
    case PkiConfig::SELF_SIGNED:
      RETURN_NOT_OK(tls_context->GenerateSelfSignedCertAndKey());
      break;
    case PkiConfig::TRUSTED:
      RETURN_NOT_OK(tls_context->AddTrustedCertificate(ca_cert));
      break;
    case PkiConfig::SIGNED: {
      RETURN_NOT_OK(tls_context->AddTrustedCertificate(ca_cert));
      RETURN_NOT_OK(tls_context->GenerateSelfSignedCertAndKey());
      Cert cert;
      RETURN_NOT_OK(CertSigner(&ca_cert, &ca_key).Sign(*tls_context->GetCsrIfNecessary(), &cert));
      RETURN_NOT_OK(tls_context->AdoptSignedCert(cert));
      break;
    };
    case PkiConfig::EXTERNALLY_SIGNED: {
      // Write certificate to file.
      std::string cert_path = JoinPathSegments(GetTestDataDirectory(), "kudu-test-cert.pem");
      RETURN_NOT_OK(CreateSSLServerCert(cert_path));
      // Write private key to file.
      std::string key_path = JoinPathSegments(GetTestDataDirectory(), "kudu-test-key.pem");
      RETURN_NOT_OK(CreateSSLPrivateKey(key_path));
      RETURN_NOT_OK(tls_context->LoadCertificateAndKey(cert_path, key_path));
      RETURN_NOT_OK(tls_context->LoadCertificateAuthority(cert_path));
    };
  }
  return Status::OK();
}

} // namespace security
} // namespace kudu
