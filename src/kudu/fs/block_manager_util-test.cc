// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/fs/block_manager_util.h"

#include <string>

#include <gtest/gtest.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace fs {

using std::string;

TEST_F(KuduTest, TestPathInstanceMetadataFile) {
  string kType = "asdf";
  string kFileName = JoinPathSegments(GetTestDataDirectory(), "foo");

  // Test that the metadata file was created.
  {
    PathInstanceMetadataFile file(env_.get(), kType, kFileName);
    ASSERT_OK(file.Create());
  }
  ASSERT_TRUE(env_->FileExists(kFileName));

  // Test that we could open and parse it.
  {
    PathInstanceMetadataFile file(env_.get(), kType, kFileName);
    ASSERT_OK(file.LoadFromDisk());
    ASSERT_TRUE(file.metadata()->has_uuid());
  }

  // Test that expecting a different type of block manager fails.
  {
    PathInstanceMetadataFile file(env_.get(), "other type", kFileName);
    PathInstanceMetadataPB pb;
    ASSERT_TRUE(file.LoadFromDisk().IsIOError());
  }

  // Test that we can lock the file.
  {
    PathInstanceMetadataFile first(env_.get(), kType, kFileName);
    ASSERT_OK(first.LoadFromDisk());
    ASSERT_OK(first.Lock());

    ASSERT_DEATH({
      PathInstanceMetadataFile second(env_.get(), kType, kFileName);
      CHECK_OK(second.LoadFromDisk());
      CHECK_OK(second.Lock());
    }, "Could not lock");

    ASSERT_OK(first.Unlock());
    ASSERT_DEATH({
      PathInstanceMetadataFile second(env_.get(), kType, kFileName);
      CHECK_OK(second.LoadFromDisk());
      Status s = second.Lock();
      if (s.ok()) {
        LOG(FATAL) << "Lock successfully acquired";
      } else {
        LOG(FATAL) << "Could not lock: " << s.ToString();
      }
    }, "Lock successfully acquired");
  }
}

} // namespace fs
} // namespace kudu
