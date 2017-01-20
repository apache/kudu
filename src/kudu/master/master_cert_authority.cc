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

#include <gflags/gflags.h>

#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;

using kudu::security::Cert;
using kudu::security::CertSignRequest;
using kudu::security::PrivateKey;
using kudu::security::ca::CaCertRequestGenerator;
using kudu::security::ca::CertSigner;

DEFINE_int32(master_ca_rsa_key_length_bits, 2048,
             "The number of bits to use for the master's self-signed "
             "certificate authority private key.");
TAG_FLAG(master_ca_rsa_key_length_bits, experimental);

namespace kudu {
namespace master {

namespace {

CaCertRequestGenerator::Config PrepareCaConfig(const string& server_uuid) {
  // TODO(aserbin): do we actually have to set all these fields given we
  // aren't using a web browser to connect?
  return {
    "US",                     // country
    "CA",                     // state
    "San Francisco",          // locality
    "ASF",                    // org
    "The Kudu Project",       // unit
    server_uuid,              // uuid
    "Self-signed master CA",  // comment
    {},                       // hostnames
    {},                       // ips
  };
}

} // anonymous namespace

MasterCertAuthority::MasterCertAuthority(string server_uuid)
    : server_uuid_(std::move(server_uuid)) {
}

MasterCertAuthority::~MasterCertAuthority() {
}

// Generate
Status MasterCertAuthority::Generate(security::PrivateKey* key,
                                     security::Cert* cert) const {
  CHECK(key);
  CHECK(cert);
  // Create a key and cert for the self-signed CA.
  RETURN_NOT_OK(GeneratePrivateKey(FLAGS_master_ca_rsa_key_length_bits, key));
  return CertSigner::SelfSignCA(*key, PrepareCaConfig(server_uuid_), cert);
}

Status MasterCertAuthority::Init(unique_ptr<PrivateKey> key,
                                 unique_ptr<Cert> cert) {
  CHECK(key);
  CHECK(cert);
  ca_private_key_ = std::move(key);
  ca_cert_ = std::move(cert);
  return Status::OK();
}

Status MasterCertAuthority::SignServerCSR(const string& csr_der, string* cert_der) {
  CHECK(ca_cert_ && ca_private_key_) << "not initialized";

  // TODO(PKI): before signing, should we somehow verify the CSR's
  // hostname/server_uuid matches what we think is the hostname? can the signer
  // modify the CSR to add fields, etc, indicating when/where it was signed?
  // maybe useful for debugging.

  CertSignRequest csr;
  RETURN_NOT_OK_PREPEND(csr.FromString(csr_der, security::DataFormat::DER),
                        "could not parse CSR");
  // TODO(PKI): need to set expiration interval on the signed CA cert!
  Cert cert;
  RETURN_NOT_OK_PREPEND(CertSigner(ca_cert_.get(), ca_private_key_.get())
                        .Sign(csr, &cert),
                        "failed to sign cert");

  RETURN_NOT_OK_PREPEND(cert.ToString(cert_der, security::DataFormat::DER),
                        "failed to signed cert as DER format");
  return Status::OK();
}


} // namespace master
} // namespace kudu
