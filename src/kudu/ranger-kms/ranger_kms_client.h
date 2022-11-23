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
#include <utility>
#include <vector>

#include "kudu/util/status.h"
#include "kudu/gutil/strings/split.h"

namespace kudu {
namespace security {
class RangerKMSClient {
 public:
  RangerKMSClient(const std::string& kms_url, std::string cluster_key_name)
    : kms_urls_(strings::Split(kms_url, ",", strings::SkipEmpty())),
      cluster_key_name_(std::move(cluster_key_name)) {}

  Status DecryptKey(const std::string& encrypted_key,
                    const std::string& iv,
                    const std::string& key_version,
                    std::string* decrypted_key);

  Status GenerateEncryptedServerKey(std::string* encrypted_key,
                                    std::string* iv,
                                    std::string* key_version);

 private:
  std::vector<std::string> kms_urls_;
  std::string cluster_key_name_;

};
} // namespace security
} // namespace kudu
