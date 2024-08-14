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

#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/monotime.h"

// We should set a value greater than the Apache Ranger to ensure the
// robustness of the key generation.
// The default value of Apache Ranger we can get from [1].
//
// [1]https://github.com/apache/ranger/blob/4e365456f6533ee5515c5070c92e355198922c81/agents-common/src/main/java/org/apache/ranger/plugin/util/PolicyRefresher.java#L92
DEFINE_int32(ranger_kms_client_generate_key_max_retry_time_s, 40,
             "The maximum retry time for generating encryption keys using "
             "the Ranger KMS client. The maximum effective time for adding "
             "a new account to Apache Ranger is about 30 seconds, and the retry "
             "time for using the client to generate the key should be greater "
             "than this value.");

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

Status RangerKMSClient::GenerateEncryptionKeyFromKMS(const string& key_name,
                                                     string* encryption_key,
                                                     string* iv,
                                                     string* key_version) {
  EasyCurl curl;
  curl.set_auth(CurlAuthType::SPNEGO);
  vector<string> urls;
  urls.reserve(kms_urls_.size());
  for (const auto& url : kms_urls_) {
    urls.emplace_back(Substitute("$0/v1/key/$1/_eek?eek_op=generate&num_keys=1",
                      url, key_name));
  }

  faststring resp;
  const MonoTime deadline = MonoTime::Now() +
      MonoDelta::FromSeconds(FLAGS_ranger_kms_client_generate_key_max_retry_time_s);
  Status s;
  int backoff_ms = 300;
  constexpr const char* const kErrorMsg = "Failed to generate server key.";

  do {
    s = curl.FetchURL(urls, &resp);
    if (s.ok()) {
      break;
    }

    LOG(WARNING) << kErrorMsg << " Status: " << s.ToString();
    if (MonoTime::Now() >= deadline) {
      // Timeout
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(backoff_ms));

    backoff_ms += 300;
    // Sleep for a maximum of 1800 milliseconds.
    if (backoff_ms > 1800) {
      backoff_ms = 1800;
    }
  } while (true);

  if (PREDICT_FALSE(!s.ok())) {
    LOG_AND_RETURN(ERROR, s.CloneAndPrepend(kErrorMsg));
  }

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

Status RangerKMSClient::GenerateTenantKey(const string& tenant_id,
                                          string* encryption_key,
                                          string* iv,
                                          string* key_version) {
  return GenerateEncryptionKeyFromKMS(tenant_id, encryption_key, iv, key_version);
}

Status RangerKMSClient::GenerateEncryptionKey(string* encryption_key,
                                              string* iv,
                                              string* key_version) {
  return GenerateEncryptionKeyFromKMS(cluster_key_name_, encryption_key, iv, key_version);
}

} // namespace security
} // namespace kudu
