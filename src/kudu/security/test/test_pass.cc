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

#include "kudu/security/test/test_pass.h"

#include "kudu/util/env.h"
#include "kudu/util/path_util.h"

using std::string;

namespace kudu {
namespace security {

Status CreateTestHTPasswd(const string& dir,
                          string* passwd_file) {

  // In the format of user:realm:digest. Digest is generated bases on
  // password 'test'.
  const char *kHTPasswd = "test:0.0.0.0:e4c02fbc8e89377a942ffc6b1bc3a566";
  *passwd_file = JoinPathSegments(dir, "test.passwd");
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kHTPasswd, *passwd_file));
  return Status::OK();
}

} // namespace security
} // namespace kudu
