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

#include <sys/statvfs.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/stl_logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(disk_reserved_bytes_free_for_testing);

using std::string;
using std::unique_ptr;
using std::unordered_set;
using strings::Substitute;

namespace kudu {
namespace env_util {

class EnvUtilTest: public KuduTest {
};

// Assert that Status 's' indicates there is not enough space left on the
// device for the request.
static void AssertNoSpace(const Status& s) {
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(ENOSPC, s.posix_code());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient disk space");
}

TEST_F(EnvUtilTest, TestDiskSpaceCheck) {
  const int64_t kZeroRequestedBytes = 0;
  const int64_t kRequestOnePercentReservation = -1;
  int64_t reserved_bytes = 0;
  ASSERT_OK(VerifySufficientDiskSpace(env_, test_dir_, kZeroRequestedBytes, reserved_bytes));

  // Check 1% reservation logic. We loop this in case there are other FS
  // operations happening concurrent with this test.
  NO_FATALS(AssertEventually([&] {
    SpaceInfo space_info;
    ASSERT_OK(env_->GetSpaceInfo(test_dir_, &space_info));
    // Try for 1 less byte than 1% free. This request should be rejected.
    int64_t target_free_bytes = (space_info.capacity_bytes / 100) - 1;
    int64_t bytes_to_request = std::max<int64_t>(0, space_info.free_bytes - target_free_bytes);
    NO_FATALS(AssertNoSpace(VerifySufficientDiskSpace(env_, test_dir_, bytes_to_request,
                                                      kRequestOnePercentReservation)));
  }));

  // Make it seem as if the disk is full and specify that we should have
  // reserved 200 bytes. Even asking for 0 bytes should return an error
  // indicating we are out of space.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;
  reserved_bytes = 200;
  NO_FATALS(AssertNoSpace(VerifySufficientDiskSpace(env_, test_dir_, kZeroRequestedBytes,
                                                    reserved_bytes)));
}

// Ensure that we can recursively create directories using both absolute and
// relative paths.
TEST_F(EnvUtilTest, TestCreateDirsRecursively) {
  // Absolute path.
  string path = JoinPathSegments(test_dir_, "a/b/c");
  ASSERT_OK(CreateDirsRecursively(env_, path));
  bool is_dir;
  ASSERT_OK(env_->IsDirectory(path, &is_dir));
  ASSERT_TRUE(is_dir);

  // Repeating the previous command should also succeed (it should be a no-op).
  ASSERT_OK(CreateDirsRecursively(env_, path));
  ASSERT_OK(env_->IsDirectory(path, &is_dir));
  ASSERT_TRUE(is_dir);

  // Relative path.
  ASSERT_OK(env_->ChangeDir(test_dir_)); // Change to test dir to keep CWD clean.
  string rel_base = Substitute("$0-$1", CURRENT_TEST_CASE_NAME(), CURRENT_TEST_NAME());
  ASSERT_FALSE(env_->FileExists(rel_base));
  path = JoinPathSegments(rel_base, "x/y/z");
  ASSERT_OK(CreateDirsRecursively(env_, path));
  ASSERT_OK(env_->IsDirectory(path, &is_dir));
  ASSERT_TRUE(is_dir);

  // Directory creation should fail if a file is a part of the path.
  path = JoinPathSegments(test_dir_, "x/y/z");
  string file_path = JoinPathSegments(test_dir_, "x"); // Conflicts with 'path'.
  ASSERT_FALSE(env_->FileExists(path));
  ASSERT_FALSE(env_->FileExists(file_path));
  // Create an empty file in the path.
  unique_ptr<WritableFile> out;
  ASSERT_OK(env_->NewWritableFile(file_path, &out));
  ASSERT_OK(out->Close());
  ASSERT_TRUE(env_->FileExists(file_path));
  // Fail.
  Status s = CreateDirsRecursively(env_, path);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "File exists");

  // We should be able to create a directory tree even when a symlink exists as
  // part of the path.
  path = JoinPathSegments(test_dir_, "link/a/b");
  string link_path = JoinPathSegments(test_dir_, "link");
  string real_dir = JoinPathSegments(test_dir_, "real_dir");
  ASSERT_OK(env_->CreateDir(real_dir));
  PCHECK(symlink(real_dir.c_str(), link_path.c_str()) == 0);
  ASSERT_OK(CreateDirsRecursively(env_, path));
  ASSERT_OK(env_->IsDirectory(path, &is_dir));
  ASSERT_TRUE(is_dir);
}

// Ensure that DeleteExcessFilesByPattern() works.
// We ensure that the number of files remaining after running it is the number
// expected, and we manually set the modification times on the relevant files
// to allow us to test that files are deleted oldest-first.
TEST_F(EnvUtilTest, TestDeleteExcessFilesByPattern) {
  string dir = JoinPathSegments(test_dir_, "excess");
  ASSERT_OK(env_->CreateDir(dir));
  vector<string> filenames = {"a", "b", "c", "d"};
  int now_sec = GetCurrentTimeMicros() / 1000;
  for (int i = 0; i < filenames.size(); i++) {
    const string& filename = filenames[i];
    string path = JoinPathSegments(dir, filename);
    unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(path, &file));
    ASSERT_OK(file->Close());

    // Set the last-modified time of the file.
    struct timeval target_time { .tv_sec = now_sec + (i * 2), .tv_usec = 0 };
    struct timeval times[2] = { target_time, target_time };
    ASSERT_EQ(0, utimes(path.c_str(), times)) << errno;
  }
  vector<string> children;
  ASSERT_OK(env_->GetChildren(dir, &children));
  ASSERT_EQ(6, children.size()); // 4 files plus "." and "..".
  ASSERT_OK(DeleteExcessFilesByPattern(env_, dir + "/*", 2));
  ASSERT_OK(env_->GetChildren(dir, &children));
  ASSERT_EQ(4, children.size()); // 2 files plus "." and "..".
  unordered_set<string> children_set(children.begin(), children.end());
  unordered_set<string> expected_set({".", "..", "c", "d"});
  ASSERT_EQ(expected_set, children_set) << children;
}

} // namespace env_util
} // namespace kudu
