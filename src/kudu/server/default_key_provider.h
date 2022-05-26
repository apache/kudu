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

#include "kudu/server/key_provider.h"

namespace kudu {
namespace security {

class DefaultKeyProvider : public KeyProvider {
public:
  ~DefaultKeyProvider() override {}
  Status DecryptServerKey(const std::string& encrypted_server_key,
                          std::string* server_key) override {
    return EncryptServerKey(encrypted_server_key, server_key);
  }

  Status EncryptServerKey(const std::string& server_key,
                          std::string* encrypted_server_key) override {
    *encrypted_server_key = server_key;
#ifdef __linux__
    memfrob(encrypted_server_key->data(), server_key.length());
#else
    // On Linux, memfrob() bitwise XORs the data with the magic number that is
    // the answer to the ultimate question of life, the universe, and
    // everything. On Mac, we do this manually.
    const uint8_t kMagic = 42;
    for (auto i = 0; i < server_key.length(); ++i) {
      encrypted_server_key->data()[i] ^= kMagic;
    }
#endif
    return Status::OK();
  }
};
} // namespace security
} // namespace kudu
