// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include <tr1/memory>

#include <gtest/gtest.h>

#include "util/status.h"
#include "util/test_util.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/memenv/memenv.h"

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

class ShortReadRandomAccessFile : public RandomAccessFile {
 public:
  explicit ShortReadRandomAccessFile(const shared_ptr<RandomAccessFile>& wrapped)
    : wrapped_(wrapped) {
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      uint8_t *scratch) const {
    CHECK_GT(n, 0);
    // Divide the requested amount of data by a small integer,
    // and issue the shorter read to the underlying file.
    int short_n = n / ((rand() % 3) + 1);
    if (short_n == 0) {
      short_n = 1;
    }

    VLOG(1) << "Reading " << short_n << " instead of " << n;

    return wrapped_->Read(offset, short_n, result, scratch);
  }

  virtual Status Size(uint64_t *size) const {
    return wrapped_->Size(size);
  }

 private:
  const shared_ptr<RandomAccessFile> wrapped_;
};

static void WriteTestFile(Env* env, const string& path, size_t size) {
  // Write 64KB of data to a file, with a simple pattern stored in it.
  shared_ptr<WritableFile> wf;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env, path, &wf));
  faststring data;
  data.resize(size);
  for (int i = 0; i < data.size(); i++) {
    data[i] = (i * 31) & 0xff;
  }
  ASSERT_STATUS_OK(wf->Append(Slice(data)));
  ASSERT_STATUS_OK(wf->Close());
}

static void VerifyTestData(const Slice& read_data, size_t offset) {
  for (int i = 0; i < read_data.size(); i++) {
    size_t file_offset = offset + i;
    ASSERT_EQ((file_offset * 31) & 0xff, read_data[i]) << "failed at " << i;
  }
}

TEST_F(TestEnv, TestReadFully) {
  SeedRandom();
  const string kTestPath = "test";
  const int kFileSize = 64 * 1024;
  gscoped_ptr<Env> mem(NewMemEnv(Env::Default()));

  WriteTestFile(mem.get(), kTestPath, kFileSize);
  ASSERT_NO_FATAL_FAILURE();

  // Reopen for read
  shared_ptr<RandomAccessFile> raf;
  ASSERT_STATUS_OK(env_util::OpenFileForRandom(mem.get(), kTestPath, &raf));

  ShortReadRandomAccessFile sr_raf(raf);

  const int kReadLength = 10000;
  Slice s;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[kReadLength]);

  // Verify that ReadFully reads the whole requested data.
  ASSERT_STATUS_OK(env_util::ReadFully(&sr_raf, 0, kReadLength, &s, scratch.get()));
  ASSERT_EQ(s.data(), scratch.get()) << "Should have returned a contiguous copy";
  ASSERT_EQ(kReadLength, s.size());

  // Verify that the data read was correct.
  VerifyTestData(s, 0);

  // Verify that ReadFully fails with an IOError at EOF.
  Status status = env_util::ReadFully(&sr_raf, kFileSize - 100, 200, &s, scratch.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.IsIOError());
  ASSERT_STR_CONTAINS(status.ToString(), "EOF");
}

}  // namespace kudu
