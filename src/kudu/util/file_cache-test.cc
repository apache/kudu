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

#include "kudu/util/file_cache.h"

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/cache.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"  // IWYU pragma: keep
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(cache_force_single_shard);
DECLARE_int32(file_cache_expiry_period_ms);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

template <class FileType>
class FileCacheTest : public KuduTest {
 public:
  FileCacheTest()
      : rand_(SeedRandom()),
        initial_open_fds_(CountOpenFds(env_)) {
    // Simplify testing of the actual cache capacity.
    FLAGS_cache_force_single_shard = true;

    // Speed up tests that check the number of descriptors.
    FLAGS_file_cache_expiry_period_ms = 1;
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ReinitCache(1));
  }

 protected:
  Status ReinitCache(int max_open_files) {
    cache_.reset(new FileCache<FileType>("test",
                                         env_,
                                         max_open_files,
                                         nullptr));
    return cache_->Init();
  }

  Status WriteTestFile(const string& name, const string& data) {
    unique_ptr<RWFile> f;
    RETURN_NOT_OK(env_->NewRWFile(name, &f));
    RETURN_NOT_OK(f->Write(0, data));
    return Status::OK();
  }

  void AssertFdsAndDescriptors(int num_expected_fds,
                               int num_expected_descriptors) {
    ASSERT_EQ(initial_open_fds_ + num_expected_fds, CountOpenFds(env_));

    // The expiry thread may take some time to run.
    ASSERT_EVENTUALLY([&]() {
      ASSERT_EQ(num_expected_descriptors, cache_->NumDescriptorsForTests());
    });
  }

  Random rand_;
  const int initial_open_fds_;
  unique_ptr<FileCache<FileType>> cache_;
};

typedef ::testing::Types<RWFile, RandomAccessFile> FileTypes;
TYPED_TEST_CASE(FileCacheTest, FileTypes);

TYPED_TEST(FileCacheTest, TestBasicOperations) {
  // Open a non-existent file.
  {
    shared_ptr<TypeParam> f;
    ASSERT_TRUE(this->cache_->OpenExistingFile(
        "/does/not/exist", &f).IsNotFound());
    NO_FATALS(this->AssertFdsAndDescriptors(0, 0));
  }

  const string kFile1 = this->GetTestPath("foo");
  const string kFile2 = this->GetTestPath("bar");
  const string kData1 = "test data 1";
  const string kData2 = "test data 2";

  // Create some test files.
  ASSERT_OK(this->WriteTestFile(kFile1, kData1));
  ASSERT_OK(this->WriteTestFile(kFile2, kData2));
  NO_FATALS(this->AssertFdsAndDescriptors(0, 0));

  {
    // Open a test file. It should open an fd and create a descriptor.
    shared_ptr<TypeParam> f1;
    ASSERT_OK(this->cache_->OpenExistingFile(kFile1, &f1));
    NO_FATALS(this->AssertFdsAndDescriptors(1, 1));

    // Spot check the test data by comparing sizes.
    for (int i = 0; i < 3; i++) {
      uint64_t size;
      ASSERT_OK(f1->Size(&size));
      ASSERT_EQ(kData1.size(), size);
      NO_FATALS(this->AssertFdsAndDescriptors(1, 1));
    }

    // Open the same file a second time. It should reuse the existing
    // descriptor and not open a second fd.
    shared_ptr<TypeParam> f2;
    ASSERT_OK(this->cache_->OpenExistingFile(kFile1, &f2));
    NO_FATALS(this->AssertFdsAndDescriptors(1, 1));
    {
      Cache::UniqueHandle uh(
          this->cache_->cache_->Lookup(kFile1, Cache::EXPECT_IN_CACHE),
          Cache::HandleDeleter(this->cache_->cache_.get()));
      ASSERT_TRUE(uh.get());
    }

    // Open a second file. This will create a new descriptor, but evict the fd
    // opened for the first file, so the fd count should remain constant.
    shared_ptr<TypeParam> f3;
    ASSERT_OK(this->cache_->OpenExistingFile(kFile2, &f3));
    NO_FATALS(this->AssertFdsAndDescriptors(1, 2));
    {
      Cache::UniqueHandle uh(
          this->cache_->cache_->Lookup(kFile1, Cache::EXPECT_IN_CACHE),
          Cache::HandleDeleter(this->cache_->cache_.get()));
      ASSERT_FALSE(uh.get());
    }
    {
      Cache::UniqueHandle uh(
          this->cache_->cache_->Lookup(kFile2, Cache::EXPECT_IN_CACHE),
          Cache::HandleDeleter(this->cache_->cache_.get()));
      ASSERT_TRUE(uh.get());
    }
  }

  // The descriptors are all out of scope, but the open fds remain in the cache.
  NO_FATALS(this->AssertFdsAndDescriptors(1, 0));

  // With the cache gone, so are the cached fds.
  this->cache_.reset();
  ASSERT_EQ(this->initial_open_fds_, CountOpenFds(this->env_));
}

TYPED_TEST(FileCacheTest, TestDeletion) {
  // Deleting a file that doesn't exist does nothing/
  ASSERT_TRUE(this->cache_->DeleteFile("/does/not/exist").IsNotFound());

  // Create a test file, then delete it. It will be deleted immediately.
  const string kFile1 = this->GetTestPath("foo");
  const string kData1 = "test data 1";
  ASSERT_OK(this->WriteTestFile(kFile1, kData1));
  ASSERT_TRUE(this->env_->FileExists(kFile1));
  ASSERT_OK(this->cache_->DeleteFile(kFile1));
  ASSERT_FALSE(this->env_->FileExists(kFile1));

  // Trying to delete it again fails.
  ASSERT_TRUE(this->cache_->DeleteFile(kFile1).IsNotFound());

  // Create another test file, open it, then delete it. The delete is not
  // effected until the last open descriptor is closed. In between, the
  // cache won't allow the file to be opened again.
  const string kFile2 = this->GetTestPath("bar");
  const string kData2 = "test data 2";
  ASSERT_OK(this->WriteTestFile(kFile2, kData2));
  ASSERT_TRUE(this->env_->FileExists(kFile2));
  {
    shared_ptr<TypeParam> f1;
    ASSERT_OK(this->cache_->OpenExistingFile(kFile2, &f1));
    ASSERT_EQ(this->initial_open_fds_ + 1, CountOpenFds(this->env_));
    ASSERT_OK(this->cache_->DeleteFile(kFile2));
    {
      shared_ptr<TypeParam> f2;
      ASSERT_TRUE(this->cache_->OpenExistingFile(kFile2, &f2).IsNotFound());
    }
    ASSERT_TRUE(this->cache_->DeleteFile(kFile2).IsNotFound());
    ASSERT_TRUE(this->env_->FileExists(kFile2));
    ASSERT_EQ(this->initial_open_fds_ + 1, CountOpenFds(this->env_));
  }
  ASSERT_FALSE(this->env_->FileExists(kFile2));
  ASSERT_EQ(this->initial_open_fds_, CountOpenFds(this->env_));

  // Create a test file, open it, and let it go out of scope before
  // deleting it. The deletion should evict the fd and close it, despite
  // happening after the descriptor is gone.
  const string kFile3 = this->GetTestPath("baz");
  const string kData3 = "test data 3";
  ASSERT_OK(this->WriteTestFile(kFile3, kData3));
  {
    shared_ptr<TypeParam> f3;
    ASSERT_OK(this->cache_->OpenExistingFile(kFile3, &f3));
  }
  ASSERT_TRUE(this->env_->FileExists(kFile3));
  ASSERT_EQ(this->initial_open_fds_ + 1, CountOpenFds(this->env_));
  ASSERT_OK(this->cache_->DeleteFile(kFile3));
  ASSERT_FALSE(this->env_->FileExists(kFile3));
  ASSERT_EQ(this->initial_open_fds_, CountOpenFds(this->env_));
}

TYPED_TEST(FileCacheTest, TestInvalidation) {
  const string kFile1 = this->GetTestPath("foo");
  const string kData1 = "test data 1";
  ASSERT_OK(this->WriteTestFile(kFile1, kData1));

  // Open the file.
  shared_ptr<TypeParam> f;
  ASSERT_OK(this->cache_->OpenExistingFile(kFile1, &f));

  // Write a new file and rename it in place on top of file1.
  const string kFile2 = this->GetTestPath("foo2");
  const string kData2 = "test data 2 (longer than original)";
  ASSERT_OK(this->WriteTestFile(kFile2, kData2));
  ASSERT_OK(this->env_->RenameFile(kFile2, kFile1));

  // We should still be able to access the file, since it has a cached fd.
  uint64_t size;
  ASSERT_OK(f->Size(&size));
  ASSERT_EQ(kData1.size(), size);

  // If we invalidate it from the cache and try again, it should crash because
  // the existing descriptor was invalidated.
  this->cache_->Invalidate(kFile1);
  ASSERT_DEATH({ f->Size(&size); }, "invalidated");

  // But if we re-open the path again, the new descriptor should read the
  // new data.
  shared_ptr<TypeParam> f2;
  ASSERT_OK(this->cache_->OpenExistingFile(kFile1, &f2));
  ASSERT_OK(f2->Size(&size));
  ASSERT_EQ(kData2.size(), size);
}


TYPED_TEST(FileCacheTest, TestHeavyReads) {
  const int kNumFiles = 20;
  const int kNumIterations = 100;
  const int kCacheCapacity = 5;

  ASSERT_OK(this->ReinitCache(kCacheCapacity));

  // Randomly generate some data.
  string data;
  for (int i = 0; i < 1000; i++) {
    data += Substitute("$0", this->rand_.Next());
  }

  // Write that data to a bunch of files and open them through the cache.
  vector<shared_ptr<TypeParam>> opened_files;
  for (int i = 0; i < kNumFiles; i++) {
    string filename = this->GetTestPath(Substitute("$0", i));
    ASSERT_OK(this->WriteTestFile(filename, data));
    shared_ptr<TypeParam> f;
    ASSERT_OK(this->cache_->OpenExistingFile(filename, &f));
    opened_files.push_back(f);
  }

  // Read back the data at random through the cache.
  unique_ptr<uint8_t[]> buf(new uint8_t[data.length()]);
  for (int i = 0; i < kNumIterations; i++) {
    int idx = this->rand_.Uniform(opened_files.size());
    const auto& f = opened_files[idx];
    uint64_t size;
    ASSERT_OK(f->Size(&size));
    Slice s(buf.get(), size);
    ASSERT_OK(f->Read(0, &s));
    ASSERT_EQ(data, s);
    ASSERT_LE(CountOpenFds(this->env_),
              this->initial_open_fds_ + kCacheCapacity);
  }
}

TYPED_TEST(FileCacheTest, TestNoRecursiveDeadlock) {
  // This test triggered a deadlock in a previous implementation, when expired
  // weak_ptrs were removed from the descriptor map in the descriptor's
  // destructor.
  alarm(60);
  auto cleanup = MakeScopedCleanup([]() {
    alarm(0);
  });

  const string kFile = this->GetTestPath("foo");
  ASSERT_OK(this->WriteTestFile(kFile, "test data"));

  vector<std::thread> threads;
  for (int i = 0; i < 2; i++) {
    threads.emplace_back([&]() {
      for (int i = 0; i < 10000; i++) {
        shared_ptr<TypeParam> f;
        CHECK_OK(this->cache_->OpenExistingFile(kFile, &f));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

class RandomAccessFileCacheTest : public FileCacheTest<RandomAccessFile> {
};

TEST_F(RandomAccessFileCacheTest, TestMemoryFootprintDoesNotCrash) {
  const string kFile = this->GetTestPath("foo");
  ASSERT_OK(this->WriteTestFile(kFile, "test data"));

  shared_ptr<RandomAccessFile> f;
  ASSERT_OK(this->cache_->OpenExistingFile(kFile, &f));

  // This used to crash due to a kudu_malloc_usable_size() call on a memory
  // address that wasn't the start of an actual heap allocation.
  LOG(INFO) << f->memory_footprint();
}

} // namespace kudu
