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

#include "kudu/master/master_cert_authority.h"

#include <memory>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using boost::optional;
using std::string;
using std::unique_ptr;

using kudu::security::Cert;
using kudu::security::CertSignRequest;
using kudu::security::PrivateKey;
using kudu::security::ca::CaCertRequestGenerator;
using kudu::security::ca::CertSigner;

DEFINE_int32(ipki_ca_key_size, 2048,
             "the number of bits for self-signed root CA cert used by Kudu IPKI "
             "(Internal Private Key Infrastructure, a.k.a. Internal Kudu CA)");
TAG_FLAG(ipki_ca_key_size, experimental);

DEFINE_int64(ipki_ca_cert_expiration_seconds, 10 * 365 * 24 * 60 * 60,
             "validity interval for self-signed root CA certifcate "
             "issued by Kudu IPKI "
             "(Internal Private Key Infrastructure, a.k.a. Internal Kudu CA)");
TAG_FLAG(ipki_ca_cert_expiration_seconds, experimental);

DEFINE_int64(ipki_server_cert_expiration_seconds, 10 * 365 * 24 * 60 * 60,
             "validity interval for server certificates issued by Kudu IPKI "
             "(Internal Private Key Infrastructure, a.k.a. Internal Kudu CA)");
TAG_FLAG(ipki_server_cert_expiration_seconds, experimental);

namespace kudu {
namespace master {

Status MasterCertAuthority::Generate(security::PrivateKey* key,
                                     security::Cert* cert) {
  CHECK(key);
  CHECK(cert);
  // Create a key and cert for the self-signed CA.
  CaCertRequestGenerator::Config config = { "kudu-ipki-ca" };
  RETURN_NOT_OK(GeneratePrivateKey(FLAGS_ipki_ca_key_size, key));
  return CertSigner::SelfSignCA(*key,
                                config,
                                FLAGS_ipki_ca_cert_expiration_seconds,
                                cert);
}

MasterCertAuthority::MasterCertAuthority(string server_uuid)
    : server_uuid_(std::move(server_uuid)) {
}

MasterCertAuthority::~MasterCertAuthority() {
}

Status MasterCertAuthority::Init(unique_ptr<PrivateKey> key,
                                 unique_ptr<Cert> cert) {
  CHECK(key);
  CHECK(cert);

  // Cache the exported DER-format cert.
  string ca_cert_der;
  RETURN_NOT_OK(cert->ToString(&ca_cert_der, security::DataFormat::DER));

  ca_private_key_ = std::move(key);
  ca_cert_ = std::move(cert);
  ca_cert_der_ = std::move(ca_cert_der);
  return Status::OK();
}

Status MasterCertAuthority::SignServerCSR(const CertSignRequest& csr,
                                          Cert* cert) {
  CHECK(ca_cert_ && ca_private_key_) << "not initialized";
  RETURN_NOT_OK_PREPEND(CertSigner(ca_cert_.get(), ca_private_key_.get())
                        .set_expiration_interval(MonoDelta::FromSeconds(
                            FLAGS_ipki_server_cert_expiration_seconds))
                        .Sign(csr, cert),
                        "failed to sign cert");
  return Status::OK();
}

Status MasterCertAuthority::SignServerCSR(const string& csr_der, const rpc::RemoteUser& user,
                                          string* cert_der) {
  CHECK(ca_cert_ && ca_private_key_) << "not initialized";

  CertSignRequest csr;
  RETURN_NOT_OK_PREPEND(csr.FromString(csr_der, security::DataFormat::DER),
                        "could not parse CSR");
  Cert cert;
  RETURN_NOT_OK(SignServerCSR(csr, &cert));

  // Validate that the cert has an included user ID.
  // It may seem funny to validate after signing, but we already have the functions
  // to get the cert details out of a Cert object, and not out of a CSR object.
  optional<string> cert_uid = cert.UserId();
  if (cert_uid != user.username()) {
    return Status::NotAuthorized(strings::Substitute(
        "CSR did not contain expected username. (CSR: '$0' RPC: '$1')",
        cert_uid.value_or(""),
        user.username()));
  }
  optional<string> cert_principal = cert.KuduKerberosPrincipal();
  if (cert_principal != user.principal()) {
    return Status::NotAuthorized(strings::Substitute(
        "CSR did not contain expected krb5 principal (CSR: '$0' RPC: '$1')",
        cert_principal.value_or(""),
        user.principal().value_or("")));
  }

  RETURN_NOT_OK_PREPEND(cert.ToString(cert_der, security::DataFormat::DER),
                        "failed to signed cert as DER format");
  return Status::OK();
}

} // namespace master
} // namespace kudu
