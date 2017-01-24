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

#include "kudu/security/server_cert_manager.h"

#include <memory>
#include <string>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/util/flag_tags.h"
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/openssl_util.h"

using std::string;
using std::unique_ptr;

DEFINE_int32(server_rsa_key_length_bits, 2048,
             "The number of bits to use for the server's private RSA key. This is used "
             "for TLS connections to and from clients and other servers.");
TAG_FLAG(server_rsa_key_length_bits, experimental);

namespace kudu {
namespace security {

using ca::CertRequestGenerator;

ServerCertManager::ServerCertManager(string server_uuid)
    : server_uuid_(std::move(server_uuid)) {
}

ServerCertManager::~ServerCertManager() {
}

Status ServerCertManager::Init() {
  MutexLock lock(lock_);
  CHECK(!key_);

  unique_ptr<PrivateKey> key(new PrivateKey());
  RETURN_NOT_OK_PREPEND(GeneratePrivateKey(FLAGS_server_rsa_key_length_bits,
                        key.get()),
                        "could not generate private key");

  // TODO(aserbin): do these fields actually have to be set?
  const CertRequestGenerator::Config config = {
    "US",               // country
    "CA",               // state
    "San Francisco",    // locality
    "ASF",              // org
    "The Kudu Project", // unit
    server_uuid_,       // uuid
    "",                 // comment
    {"localhost"},      // hostnames TODO(PKI): use real hostnames
    {"127.0.0.1"},      // ips
  };

  CertRequestGenerator gen(config);
  CertSignRequest csr;
  RETURN_NOT_OK_PREPEND(gen.Init(), "could not initialize CSR generator");
  RETURN_NOT_OK_PREPEND(gen.GenerateRequest(*key, &csr), "could not generate CSR");
  RETURN_NOT_OK_PREPEND(csr.ToString(&csr_der_, DataFormat::DER),
                        "unable to output DER format CSR");
  key_ = std::move(key);
  return Status::OK();
}


Status ServerCertManager::AdoptSignedCert(const string& cert_der) {
  MutexLock lock(lock_);
  // If we already have a cert, then no need to adopt a new one.
  // We heartbeat to multiple masters in parallel, and so we might
  // get multiple valid signed certs from different masters.
  if (signed_cert_) {
    return Status::OK();
  }
  unique_ptr<Cert> new_cert(new Cert());
  RETURN_NOT_OK_PREPEND(new_cert->FromString(cert_der, DataFormat::DER),
                        "could not parse DER data");

  LOG(INFO) << "Adopting new signed X509 certificate";
  signed_cert_ = std::move(new_cert);
  // No longer need to get a signed cert, so we can forget our CSR.
  csr_der_.clear();
  return Status::OK();
}

// Return a new CSR, if this tablet server's cert has not yet been
// signed. If the cert is already signed, returns null.
boost::optional<string> ServerCertManager::GetCSRIfNecessary() const {
  MutexLock lock(lock_);
  if (signed_cert_) return boost::none;
  return csr_der_;
}

bool ServerCertManager::has_signed_cert() const {
  MutexLock lock(lock_);
  return signed_cert_ != nullptr;
}

} // namespace security
} // namespace kudu
