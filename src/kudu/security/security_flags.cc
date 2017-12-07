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

namespace kudu {
namespace security {

// This is the "modern compatibility" cipher list of the Mozilla Security
// Server Side TLS recommendations, accessed Feb. 2017, with the addition of
// the non ECDH/DH AES cipher suites from the "intermediate compatibility"
// list. These additional ciphers maintain compatibility with RHEL 6.5 and
// below. The DH AES ciphers are not included since we are not configured to
// use DH key agreement.
const char* const SecurityDefaults::SecurityDefaults::kDefaultTlsCiphers =
                                   "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                                   "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:"
                                   "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
                                   "ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA384:"
                                   "ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA256:"
                                   "AES256-GCM-SHA384:AES128-GCM-SHA256:"
                                   "AES256-SHA256:AES128-SHA256:"
                                   "AES256-SHA:AES128-SHA";

const char* const SecurityDefaults::SecurityDefaults::kDefaultTlsMinVersion = "TLSv1";

} // namespace security
} // namespace kudu
