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

#include "kudu/util/curl_util.h"

#include <gtest/gtest.h>

#include "kudu/util/debug/sanitizer_scopes.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

// When using a thread sanitizer, there will be a data race when timeout.
TEST(CurlUtilTest, TestTimeout) {
  debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
  EasyCurl curl;
  faststring dst;
  curl.set_timeout(MonoDelta::FromMilliseconds(1));
  Status s = curl.FetchURL("http://not_exist_host:12345", &dst);
  ASSERT_TRUE(s.IsTimedOut());
}

} // namespace kudu
