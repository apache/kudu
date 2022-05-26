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

#include "kudu/server/default_key_provider.h"

#include <string>

#include <gtest/gtest.h>

#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {
namespace security {

class DefaultKeyProviderTest : public KuduTest {
 protected:
  DefaultKeyProvider key_provider_;
};

TEST_F(DefaultKeyProviderTest, TestEncryptAndDecrypt) {
  string key = "foo";
  string encrypted_key;
  string decrypted_key;
  ASSERT_OK(key_provider_.EncryptServerKey(key, &encrypted_key));
  ASSERT_OK(key_provider_.DecryptServerKey(encrypted_key, &decrypted_key));
  ASSERT_NE(key, encrypted_key);
  ASSERT_EQ(key, decrypted_key);
}

} // namespace security
} // namespace kudu
