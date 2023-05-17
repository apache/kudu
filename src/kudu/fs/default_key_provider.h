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
#include <openssl/rand.h>

#include "kudu/fs/key_provider.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/util/openssl_util.h"

namespace kudu {
namespace security {

class DefaultKeyProvider : public KeyProvider {
public:
  ~DefaultKeyProvider() override {}
  Status DecryptEncryptionKey(const std::string& encryption_key,
                              const std::string& /*iv*/,
                              const std::string& /*key_version*/,
                              std::string* decrypted_key) override {
    *decrypted_key = strings::a2b_hex(encryption_key);
#ifdef __linux__
    memfrob(decrypted_key->data(), decrypted_key->length());
#else
    // On Linux, memfrob() bitwise XORs the data with the magic number that is
    // the answer to the ultimate question of life, the universe, and
    // everything. On Mac, we do this manually.
    const uint8_t kMagic = 42;
    for (auto i = 0; i < decrypted_key->length(); ++i) {
      decrypted_key->data()[i] ^= kMagic;
    }
#endif
    *decrypted_key = strings::b2a_hex(*decrypted_key);
    return Status::OK();
  }

  Status GenerateEncryptionKey(std::string* encryption_key,
                               std::string* iv,
                               std::string* key_version) override {
    uint8_t key_bytes[32];
    uint8_t iv_bytes[32];
    int num_bytes = 16;
    std::string dek;
    OPENSSL_RET_NOT_OK(RAND_bytes(key_bytes, num_bytes),
                       "Failed to generate random key");
    strings::b2a_hex(key_bytes, &dek, num_bytes);
    OPENSSL_RET_NOT_OK(RAND_bytes(iv_bytes, num_bytes),
                       "Failed to generate random key");
    strings::b2a_hex(iv_bytes, iv, num_bytes);
    DecryptEncryptionKey(dek, *iv, *key_version, encryption_key);
    *key_version = "encryptionkey@0";
    return Status::OK();
  }
};
} // namespace security
} // namespace kudu
