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

#include <string>

#include "kudu/util/status.h"

namespace kudu {

extern const char* kRsaPrivKeyPem;
extern const char* kRsaPubKeyPem;
// The public keys in JWK format were converted from PEM formatted crypto keys with
// pem-to-jwk tool at https://hub.docker.com/r/danedmunds/pem-to-jwk/
extern const char* kRsaPubKeyJwkN;
extern const char* kRsaPubKeyJwkE;
extern const char* kRsaInvalidPubKeyJwkN;

extern const char* kRsa512PrivKeyPem;
extern const char* kRsa512PubKeyPem;
extern const char* kRsa512PubKeyJwkN;
extern const char* kRsa512PubKeyJwkE;
extern const char* kRsa512InvalidPubKeyJwkN;

extern const char* kRsa1024PrivKeyPem;
extern const char* kRsa1024PubKeyPem;
extern const char* kRsa1024PubKeyJwkN;
extern const char* kRsa1024PubKeyJwkE;

extern const char* kRsa2048PrivKeyPem;
extern const char* kRsa2048PubKeyPem;
extern const char* kRsa2048PubKeyJwkN;
extern const char* kRsa2048PubKeyJwkE;

extern const char* kRsa4096PrivKeyPem;
extern const char* kRsa4096PubKeyPem;
extern const char* kRsa4096PubKeyJwkN;
extern const char* kRsa4096PubKeyJwkE;

extern const char* kEcdsa521PrivKeyPem;
extern const char* kEcdsa521PubKeyPem;
extern const char* kEcdsa521PubKeyJwkX;
extern const char* kEcdsa521PubKeyJwkY;

extern const char* kEcdsa384PrivKeyPem;
extern const char* kEcdsa384PubKeyPem;
extern const char* kEcdsa384PubKeyJwkX;
extern const char* kEcdsa384PubKeyJwkY;

extern const char* kEcdsa256PrivKeyPem;
extern const char* kEcdsa256PubKeyPem;
extern const char* kEcdsa256PubKeyJwkX;
extern const char* kEcdsa256PubKeyJwkY;

extern const char* kKid1;
extern const char* kKid2;

extern const char* kJwksHsFileFormat;
extern const char* kJwksRsaFileFormat;
extern const char* kJwksEcFileFormat;

std::string CreateTestJWT(bool is_valid);
Status CreateTestJWKSFile(const std::string& dir, const std::string& file_name);

}  // namespace kudu
