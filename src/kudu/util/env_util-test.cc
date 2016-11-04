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

#include "kudu/util/env_util.h"

#include <gflags/gflags.h>
#include <memory>
#include <sys/statvfs.h>

#include "kudu/util/test_util.h"

DECLARE_int64(disk_reserved_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);

namespace kudu {

using std::string;
using std::unique_ptr;

class EnvUtilTest: public KuduTest {
};

TEST_F(EnvUtilTest, TestDiskSpaceCheck) {
  Env* env = Env::Default();

  const int64_t kRequestedBytes = 0;
  int64_t reserved_bytes = 0;
  ASSERT_OK(env_util::VerifySufficientDiskSpace(env, test_dir_, kRequestedBytes, reserved_bytes));

  // Make it seem as if the disk is full and specify that we should have
  // reserved 200 bytes. Even asking for 0 bytes should return an error
  // indicating we are out of space.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;
  reserved_bytes = 200;
  Status s = env_util::VerifySufficientDiskSpace(env, test_dir_, kRequestedBytes, reserved_bytes);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(ENOSPC, s.posix_code());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient disk space");
}

} // namespace kudu
