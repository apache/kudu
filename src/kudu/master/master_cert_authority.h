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

#include <memory>
#include <string>

#include "kudu/gutil/macros.h"

namespace kudu {

class Status;

namespace security {

class Cert;
class PrivateKey;

namespace ca {
class CertSigner;
} // namespace ca
} // namespace security

namespace master {

class MasterCertAuthorityTest;

// Implements the X509 certificate-authority functionality of the Master.
//
// This is used in the "built-in PKI" mode of operation. The master generates
// its own self-signed CA certificate, and then signs CSRs provided by tablet
// in their heartbeats.
//
// This class is thread-safe after initialization.
class MasterCertAuthority {
 public:
  explicit MasterCertAuthority(std::string server_uuid);
  virtual ~MasterCertAuthority();

  // Generate a private key and corresponding self-signed root CA certificate
  // bound to the aggregated server UUID. Does not require Init() to be called.
  // Calling this method does not have side-effects on the instance, i.e.
  // even this method has been called on object, it's still necessary
  // to call Init() prior to calling SignServerCSR() method.
  Status Generate(security::PrivateKey* key, security::Cert* cert) const;

  // Initializes the MasterCertAuthority with the given private key
  // and CA certificate. This method is called when the master server
  // is elected as a leader -- upon that event it initializes the certificate
  // authority with the information read from the system table.
  Status Init(std::unique_ptr<security::PrivateKey> key,
              std::unique_ptr<security::Cert> cert);

  // Sign the given CSR 'csr_der' provided by a server in the cluster.
  //
  // The CSR should be provided in the DER format.
  // The resulting certificate, also in DER format, is returned in 'cert_der'.
  //
  // REQUIRES: Init() must be called first.
  //
  // NOTE:  This method is not going to be called in parallel with Init()
  //        due to the current design, so there is no internal synchronization
  //        to keep the internal state consistent.
  Status SignServerCSR(const std::string& csr_der, std::string* cert_der);

 private:
  friend class ::kudu::master::MasterCertAuthorityTest;
  // The UUID of the master. This is used as a field in the certificate.
  const std::string server_uuid_;

  std::unique_ptr<security::PrivateKey> ca_private_key_;
  std::unique_ptr<security::Cert> ca_cert_;

  DISALLOW_COPY_AND_ASSIGN(MasterCertAuthority);
};

} // namespace master
} // namespace kudu
