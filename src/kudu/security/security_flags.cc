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

#include "kudu/security/security_flags.h"

// The list of ciphers and minimum TLS protocol versions are influenced by the
// Mozilla Security Server Side TLS recommendations accessed March 2021, at
// https://wiki.mozilla.org/Security/Server_Side_TLS
namespace kudu {
namespace security {

// This is TLSv1.2-related section from the "intermediate compatibility" cipher
// list of the Mozilla Security Server Side TLS recommendations without the
// DH AES ciphers: they are not included since we are not configured to use
// the DH key agreement.
const char* const SecurityDefaults::SecurityDefaults::kDefaultTlsCiphers =
    "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305";

// This is the "modern compatibility" TLSv1.3 cipher list of the Mozilla
// Security Server Side TLS recommendations, accessed March 2021.
// https://wiki.mozilla.org/Security/Server_Side_TLS
const char* const SecurityDefaults::SecurityDefaults::kDefaultTlsCipherSuites =
    "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256";

// According to the cipher lists above, the minimum supported TLS version among
// all the cipher suites is TLSv1.2.
const char* const SecurityDefaults::SecurityDefaults::kDefaultTlsMinVersion = "TLSv1.2";

} // namespace security
} // namespace kudu
