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

#include "kudu/ranger-kms/ranger_kms_client.h"

#include <string>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonreader.h"

using rapidjson::Value;
using std::string;
using std::vector;
using strings::a2b_hex;
using strings::b2a_hex;
using strings::Substitute;
using strings::WebSafeBase64Escape;
using strings::WebSafeBase64Unescape;

namespace kudu {
namespace security {

Status RangerKMSClient::DecryptEncryptionKey(const string& encryption_key,
                                             const string& iv,
                                             const string& key_version,
                                             string* decrypted_key) {
  EasyJson payload;
  payload.Set("name", cluster_key_name_);
  string iv_plain = a2b_hex(iv);
  string iv_b64;
  WebSafeBase64Escape(iv_plain, &iv_b64);
  payload.Set("iv", iv_b64);
  string eek_plain = a2b_hex(encryption_key);
  string eek_b64;
  WebSafeBase64Escape(eek_plain, &eek_b64);
  payload.Set("material", eek_b64);
  EasyCurl curl;
  curl.set_auth(CurlAuthType::SPNEGO);
  vector<string> urls;
  urls.reserve(kms_urls_.size());
  for (const auto& url : kms_urls_) {
    urls.emplace_back(Substitute("$0/v1/keyversion/$1/_eek?eek_op=decrypt",
                                 url, key_version));
  }
  faststring resp;
  RETURN_NOT_OK_PREPEND(
      curl.PostToURL(urls, payload.ToString(), &resp, {"Content-Type: application/json"}),
      "failed to decrypt encryption key");
  JsonReader r(resp.ToString());
  RETURN_NOT_OK(r.Init());
  string dek_b64;
  RETURN_NOT_OK(r.ExtractString(r.root(), "material", &dek_b64));
  string dek_plain;
  WebSafeBase64Unescape(dek_b64, &dek_plain);
  *decrypted_key = b2a_hex(dek_plain);
  return Status::OK();
}

Status RangerKMSClient::GenerateEncryptionKey(string* encryption_key,
                                              string* iv,
                                              string* key_version) {
  EasyCurl curl;
  curl.set_auth(CurlAuthType::SPNEGO);
  vector<string> urls;
  urls.reserve(kms_urls_.size());
  for (const auto& url : kms_urls_) {
    urls.emplace_back(Substitute("$0/v1/key/$1/_eek?eek_op=generate&num_keys=1",
                      url, cluster_key_name_));
  }
  faststring resp;
  RETURN_NOT_OK_PREPEND(curl.FetchURL(urls, &resp), "failed to generate encryption key");
  JsonReader r(resp.ToString());
  RETURN_NOT_OK(r.Init());
  vector<const Value*> keys;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &keys));
  string iv_b64;
  DCHECK_GT(keys.size(), 0);
  const Value* key = keys[0];
  RETURN_NOT_OK(r.ExtractString(key, "iv", &iv_b64));
  string iv_plain;
  if (!WebSafeBase64Unescape(iv_b64, &iv_plain)) {
    return Status::Corruption("Invalid IV received");
  }
  *iv = b2a_hex(iv_plain);
  RETURN_NOT_OK(r.ExtractString(key, "versionName", key_version));
  const Value* ekv = nullptr;
  RETURN_NOT_OK(r.ExtractObject(key, "encryptedKeyVersion", &ekv));
  string key_b64;
  RETURN_NOT_OK(r.ExtractString(ekv, "material", &key_b64));
  string key_plain;
  if (!WebSafeBase64Unescape(key_b64, &key_plain)) {
    return Status::Corruption("Invalid encryption key received");
  }
  *encryption_key = b2a_hex(key_plain);
  return Status::OK();
}

} // namespace security
} // namespace kudu
