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

  // Test that we could open and parse it, returning back the PB (or not).
  {
    PathInstanceMetadataFile file(env_.get(), kType, kFileName);
    ASSERT_OK(file.Open(NULL));

    PathInstanceMetadataPB pb;
    ASSERT_OK(file.Open(&pb));
    ASSERT_TRUE(pb.has_uuid());
  }

  // Test that expecting a different type of block manager fails.
  {
    PathInstanceMetadataFile file(env_.get(), "other type", kFileName);
    PathInstanceMetadataPB pb;
    ASSERT_TRUE(file.Open(&pb).IsIOError());
  }
}

} // namespace fs
} // namespace kudu
