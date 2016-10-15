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

#include <ostream>

#include "kudu/util/status.h"

namespace kudu {
namespace security {

class Cert;
class PrivateKey;
class TlsContext;

Status GenerateSelfSignedCAForTests(PrivateKey* ca_key, Cert* ca_cert);

// Describes the options for configuring a TlsContext.
enum class PkiConfig {
  // The TLS context has no TLS cert and no trusted certs.
  NONE,
  // The TLS context has a self-signed TLS cert and no trusted certs.
  SELF_SIGNED,
  // The TLS context has no TLS cert and a trusted cert.
  TRUSTED,
  // The TLS context has a signed TLS cert and trusts the corresponding signing cert.
  SIGNED,
  // The TLS context has a externally signed TLS cert and trusts the corresponding signing cert.
  EXTERNALLY_SIGNED,
};

// PkiConfig pretty-printer.
std::ostream& operator<<(std::ostream& o, PkiConfig c);

Status ConfigureTlsContext(PkiConfig config,
                           const Cert& ca_cert,
                           const PrivateKey& ca_key,
                           TlsContext* tls_context);

} // namespace security
} // namespace kudu
