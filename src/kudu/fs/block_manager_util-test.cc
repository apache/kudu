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

#include "kudu/fs/block_manager_util.h"

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/repeated_field.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace fs {

using google::protobuf::RepeatedPtrField;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

TEST_F(KuduTest, Lifecycle) {
  string kType = "asdf";
  string kFileName = GetTestPath("foo");
  string kUuid = "a_uuid";

  // Test that the metadata file was created.
  {
    PathInstanceMetadataFile file(env_, kUuid, kType, kFileName);
    ASSERT_OK(file.Create({ kUuid }));
  }
  ASSERT_TRUE(env_->FileExists(kFileName));

  // Test that we could open and parse it.
  {
    PathInstanceMetadataFile file(env_, kUuid, kType, kFileName);
    ASSERT_OK(file.LoadFromDisk());
    const PathInstanceMetadataPB* md = file.metadata();
    ASSERT_EQ(kType, md->block_manager_type());
    const PathSetPB& path_set = md->path_set();
    ASSERT_EQ(kUuid, path_set.uuid());
    ASSERT_EQ(1, path_set.all_uuids_size());
    ASSERT_EQ(kUuid, path_set.all_uuids(0));
  }

  // Test that expecting a different type of block manager fails.
  {
    PathInstanceMetadataFile file(env_, kUuid, "other type", kFileName);
    PathInstanceMetadataPB pb;
    ASSERT_TRUE(file.LoadFromDisk().IsIOError());
  }
}

TEST_F(KuduTest, Locking) {
  string kType = "asdf";
  const string kFileName = GetTestPath("foo");
  string kUuid = "a_uuid";

  PathInstanceMetadataFile file(env_, kUuid, kType, kFileName);
  ASSERT_OK(file.Create({ kUuid }));

  PathInstanceMetadataFile first(env_, "", kType, kFileName);
  ASSERT_OK(first.LoadFromDisk());
  ASSERT_EQ(kUuid, first.uuid());
  ASSERT_OK(first.Lock());

  // Note: we must use a death test here because file locking is only
  // disallowed across processes, and death tests spawn child processes.
  ASSERT_DEATH({
    PathInstanceMetadataFile second(env_, "", kType, kFileName);
    CHECK_OK(second.LoadFromDisk());
    CHECK_EQ(kUuid, second.uuid());
    CHECK_OK(second.Lock());
  }, "Could not lock");

  ASSERT_OK(first.Unlock());
  ASSERT_DEATH({
    PathInstanceMetadataFile second(env_, "", kType, kFileName);
    CHECK_OK(second.LoadFromDisk());
    CHECK_EQ(kUuid, second.uuid());
    Status s = second.Lock();
    if (s.ok()) {
      LOG(FATAL) << "Lock successfully acquired";
    } else {
      LOG(FATAL) << "Could not lock: " << s.ToString();
    }
  }, "Lock successfully acquired");
}

} // namespace fs
} // namespace kudu
