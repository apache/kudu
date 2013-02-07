// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Modified for kudu:
// - use gtest

#include <gtest/gtest.h>

#include "util/env.h"
#include "util/memenv/memenv.h"
#include "util/test_macros.h"

#include <string>
#include <vector>

namespace kudu {

class MemEnvTest : public ::testing::Test {
 public:
  Env* env_;

  MemEnvTest()
      : env_(NewMemEnv(Env::Default())) {
  }
  ~MemEnvTest() {
    delete env_;
  }
};

TEST_F(MemEnvTest, Basics) {
  uint64_t file_size;
  WritableFile* writable_file;
  std::vector<std::string> children;

  ASSERT_STATUS_OK(env_->CreateDir("/dir"));

  // Check that the directory is empty.
  ASSERT_TRUE(!env_->FileExists("/dir/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize("/dir/non_existent", &file_size).ok());
  ASSERT_STATUS_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(0, children.size());

  // Create a file.
  ASSERT_STATUS_OK(env_->NewWritableFile("/dir/f", &writable_file));
  delete writable_file;

  // Check that the file exists.
  ASSERT_TRUE(env_->FileExists("/dir/f"));
  ASSERT_STATUS_OK(env_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(0, file_size);
  ASSERT_STATUS_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(1, children.size());
  ASSERT_EQ("f", children[0]);

  // Write to the file.
  ASSERT_STATUS_OK(env_->NewWritableFile("/dir/f", &writable_file));
  ASSERT_STATUS_OK(writable_file->Append("abc"));
  delete writable_file;

  // Check for expected size.
  ASSERT_STATUS_OK(env_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(3, file_size);

  // Check that renaming works.
  ASSERT_TRUE(!env_->RenameFile("/dir/non_existent", "/dir/g").ok());
  ASSERT_STATUS_OK(env_->RenameFile("/dir/f", "/dir/g"));
  ASSERT_TRUE(!env_->FileExists("/dir/f"));
  ASSERT_TRUE(env_->FileExists("/dir/g"));
  ASSERT_STATUS_OK(env_->GetFileSize("/dir/g", &file_size));
  ASSERT_EQ(3, file_size);

  // Check that opening non-existent file fails.
  SequentialFile* seq_file;
  RandomAccessFile* rand_file;
  ASSERT_TRUE(!env_->NewSequentialFile("/dir/non_existent", &seq_file).ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_TRUE(!env_->NewRandomAccessFile("/dir/non_existent", &rand_file).ok());
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_TRUE(!env_->DeleteFile("/dir/non_existent").ok());
  ASSERT_STATUS_OK(env_->DeleteFile("/dir/g"));
  ASSERT_TRUE(!env_->FileExists("/dir/g"));
  ASSERT_STATUS_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(0, children.size());
  ASSERT_STATUS_OK(env_->DeleteDir("/dir"));
}

TEST_F(MemEnvTest, ReadWrite) {
  WritableFile* writable_file;
  SequentialFile* seq_file;
  RandomAccessFile* rand_file;
  Slice result;
  uint8_t scratch[100];

  ASSERT_STATUS_OK(env_->CreateDir("/dir"));

  ASSERT_STATUS_OK(env_->NewWritableFile("/dir/f", &writable_file));
  ASSERT_STATUS_OK(writable_file->Append("hello "));
  ASSERT_STATUS_OK(writable_file->Append("world"));
  delete writable_file;

  // Read sequentially.
  ASSERT_STATUS_OK(env_->NewSequentialFile("/dir/f", &seq_file));
  ASSERT_STATUS_OK(seq_file->Read(5, &result, scratch)); // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_STATUS_OK(seq_file->Skip(1));
  ASSERT_STATUS_OK(seq_file->Read(1000, &result, scratch)); // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_STATUS_OK(seq_file->Read(1000, &result, scratch)); // Try reading past EOF.
  ASSERT_EQ(0, result.size());
  ASSERT_STATUS_OK(seq_file->Skip(100)); // Try to skip past end of file.
  ASSERT_STATUS_OK(seq_file->Read(1000, &result, scratch));
  ASSERT_EQ(0, result.size());
  delete seq_file;

  // Random reads.
  ASSERT_STATUS_OK(env_->NewRandomAccessFile("/dir/f", &rand_file));
  ASSERT_STATUS_OK(rand_file->Read(6, 5, &result, scratch)); // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_STATUS_OK(rand_file->Read(0, 5, &result, scratch)); // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_STATUS_OK(rand_file->Read(10, 100, &result, scratch)); // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_TRUE(!rand_file->Read(1000, 5, &result, scratch).ok());
  delete rand_file;
}

TEST_F(MemEnvTest, Locks) {
  FileLock* lock;

  // These are no-ops, but we test they return success.
  ASSERT_STATUS_OK(env_->LockFile("some file", &lock));
  ASSERT_STATUS_OK(env_->UnlockFile(lock));
}

TEST_F(MemEnvTest, Misc) {
  std::string test_dir;
  ASSERT_STATUS_OK(env_->GetTestDirectory(&test_dir));
  ASSERT_TRUE(!test_dir.empty());

  WritableFile* writable_file;
  ASSERT_STATUS_OK(env_->NewWritableFile("/a/b", &writable_file));

  // These are no-ops, but we test they return success.
  ASSERT_STATUS_OK(writable_file->Sync());
  ASSERT_STATUS_OK(writable_file->Flush());
  ASSERT_STATUS_OK(writable_file->Close());
  delete writable_file;
}

TEST_F(MemEnvTest, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  uint8_t* scratch = new uint8_t[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, static_cast<char>(i));
  }

  WritableFile* writable_file;
  ASSERT_STATUS_OK(env_->NewWritableFile("/dir/f", &writable_file));
  ASSERT_STATUS_OK(writable_file->Append("foo"));
  ASSERT_STATUS_OK(writable_file->Append(write_data));
  delete writable_file;

  SequentialFile* seq_file;
  Slice result;
  ASSERT_STATUS_OK(env_->NewSequentialFile("/dir/f", &seq_file));
  ASSERT_STATUS_OK(seq_file->Read(3, &result, scratch)); // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_STATUS_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(reinterpret_cast<const char *>(result.data()),
                     result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete seq_file;
  delete [] scratch;
}


}  // namespace kudu
