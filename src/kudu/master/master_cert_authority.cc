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

#include <gflags/gflags.h>
#include <memory>
#include <string>

#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/flag_tags.h"

using std::make_shared;
using std::shared_ptr;
using std::string;

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

Status MasterCertAuthority::Init() {
  CHECK(!ca_private_key_);

  // Create a key for the self-signed CA.
  shared_ptr<PrivateKey> key(make_shared<PrivateKey>());
  RETURN_NOT_OK(GeneratePrivateKey(FLAGS_master_ca_rsa_key_length_bits,
                                   key.get()));

  // Generate a CSR for the CA.
  CertSignRequest ca_csr;
  {
    CaCertRequestGenerator gen(PrepareCaConfig(server_uuid_));
    RETURN_NOT_OK(gen.Init());
    RETURN_NOT_OK(gen.GenerateRequest(*key, &ca_csr));
  }

  // Self-sign the CA's CSR.
  auto ca_cert = make_shared<Cert>();
  {
    CertSigner ca_signer;
    RETURN_NOT_OK(ca_signer.InitForSelfSigning(key));
    RETURN_NOT_OK(ca_signer.Sign(ca_csr, ca_cert.get()));
  }

  // Initialize our signer with the new CA.
  auto signer = make_shared<CertSigner>();
  RETURN_NOT_OK(signer->Init(ca_cert, key));

  cert_signer_ = std::move(signer);
  ca_cert_ = std::move(ca_cert);
  ca_private_key_ = std::move(key);
  return Status::OK();
}

Status MasterCertAuthority::SignServerCSR(const string& csr_der, string* cert_der) {
  CHECK(cert_signer_) << "not initialized";

  // TODO(PKI): before signing, should we somehow verify the CSR's
  // hostname/server_uuid matches what we think is the hostname? can the signer
  // modify the CSR to add fields, etc, indicating when/where it was signed?
  // maybe useful for debugging.

  CertSignRequest csr;
  RETURN_NOT_OK_PREPEND(csr.FromString(csr_der, security::DataFormat::DER),
                        "could not parse CSR");
  Cert cert;
  RETURN_NOT_OK_PREPEND(cert_signer_->Sign(csr, &cert),
                        "failed to sign cert");

  RETURN_NOT_OK_PREPEND(cert.ToString(cert_der, security::DataFormat::DER),
                        "failed to signed cert as DER format");
  return Status::OK();
}


} // namespace master
} // namespace kudu
