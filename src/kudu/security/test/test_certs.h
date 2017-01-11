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
//

#pragma once

namespace kudu {
namespace security {
namespace ca {

//
// Set of certificates and private keys used for certificate generation
// and signing tests (declarations).  See the .cc file for the actual data.
//

// Valid root CA cerificate (PEM format).
extern const char kCaCert[];
// The private key (RSA, 2048 bits) for the certificate above.
// This is 2048 bit RSA key, in PEM format.
extern const char kCaPrivateKey[];

// Expired root CA certificate (PEM format).
extern const char kCaExpiredCert[];
// The private key for the expired CA certificate described above.
// This is 2048 bit RSA key, in PEM format.
extern const char kCaExpiredPrivateKey[];

} // namespace ca
} // namespace security
} // namespace kudu

