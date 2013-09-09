// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include <tr1/memory>

#include <gtest/gtest.h>

#include "util/status.h"
#include "util/test_util.h"
#include "util/env.h"
#include "util/env_util.h"

namespace kudu {

using std::string;
using std::tr1::shared_ptr;

class TestEnv : public KuduTest {
};

const uint32_t one_mb = 1024 * 1024;

TEST_F(TestEnv, TestPreallocate) {
  string test_path = GetTestPath("test_env_wf");
  shared_ptr<WritableFile> file;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env_.get(), test_path, &file));

  // pre-allocate 1 MB
  ASSERT_STATUS_OK(file->PreAllocate(one_mb));
  ASSERT_STATUS_OK(file->Sync());

  // the writable file size should report 0
  ASSERT_EQ(file->Size(), 0);
  // but the real size of the file on disk should report 1MB
  uint64_t size;
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(size, one_mb);

  // write 1 MB
  uint8_t scratch[one_mb];
  Slice slice(scratch, one_mb);
  ASSERT_STATUS_OK(file->Append(slice));
  ASSERT_STATUS_OK(file->Sync());

  // the writable file size should now report 1 MB
  ASSERT_EQ(file->Size(), one_mb);
  ASSERT_STATUS_OK(file->Close());
  // and the real size for the file on disk should match ony the
  // written size
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(one_mb, size);
}

// To test consecutive pre-allocations we need higher pre-allocations since the
// mmapped regions grow in size until 2MBs (so smaller pre-allocations will easily
// be smaller than the mmapped regions size).
TEST_F(TestEnv, TestConsecutivePreallocate) {
  string test_path = GetTestPath("test_env_wf");
  shared_ptr<WritableFile> file;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env_.get(), test_path, &file));

  // pre-allocate 64 MB
  ASSERT_STATUS_OK(file->PreAllocate(64 * one_mb));
  ASSERT_STATUS_OK(file->Sync());

  // the writable file size should report 0
  ASSERT_EQ(file->Size(), 0);
  // but the real size of the file on disk should report 64 MBs
  uint64_t size;
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(size, 64 * one_mb);

  // write 1 MB
  uint8_t scratch[one_mb];
  Slice slice(scratch, one_mb);
  ASSERT_STATUS_OK(file->Append(slice));
  ASSERT_STATUS_OK(file->Sync());

  // the writable file size should now report 1 MB
  ASSERT_EQ(one_mb, file->Size());
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(64 * one_mb, size);

  // pre-allocate 64 additional MBs
  ASSERT_STATUS_OK(file->PreAllocate(64 * one_mb));
  ASSERT_STATUS_OK(file->Sync());

  // the writable file size should now report 1 MB
  ASSERT_EQ(one_mb, file->Size());
  // while the real file size should report 128 MB's
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(128 * one_mb, size);

  // write another MB
  ASSERT_STATUS_OK(file->Append(slice));
  ASSERT_STATUS_OK(file->Sync());

  // the writable file size should now report 2 MB
  ASSERT_EQ(file->Size(), 2 * one_mb);
  // while the real file size should reamin at 128 MBs
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(128 * one_mb, size);

  // close the file (which ftruncates it to the real size)
  ASSERT_STATUS_OK(file->Close());
  // and the real size for the file on disk should match only the written size
  ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
  ASSERT_EQ(2* one_mb, size);
}

}  // namespace kudu
