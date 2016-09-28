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
// Utility functions for generating data for use by tools and tests.

#include "kudu/tools/tool_test_util.h"

#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace tools {

string GetKuduCtlAbsolutePath() {
  static const string kKuduCtlFileName = "kudu";
  string exe;
  CHECK_OK(Env::Default()->GetExecutablePath(&exe));
  const string binroot = DirName(exe);
  const string tool_abs_path = JoinPathSegments(binroot, kKuduCtlFileName);
  CHECK(Env::Default()->FileExists(tool_abs_path))
      << kKuduCtlFileName << " binary not found at " << tool_abs_path;
  return tool_abs_path;
}

} // namespace tools
} // namespace kudu
