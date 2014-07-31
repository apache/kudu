// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include <tr1/memory>
#include <sys/types.h>
#include <fcntl.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <boost/foreach.hpp>

#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/path_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/memenv/memenv.h"

namespace kudu {

using std::string;
using std::tr1::shared_ptr;

static const uint32_t kOneMb = 1024 * 1024;

class TestEnv : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    CheckFallocateSupport();
  }

  // Verify that fallocate() is supported in the test directory.
  // Some local file systems like ext3 do not support it, and we don't
  // want to fail tests on those systems.
  //
  // Sets fallocate_supported_ based on the result.
  void CheckFallocateSupport() {
    static bool checked = false;
    if (checked) return;

    int fd = creat(GetTestPath("check-fallocate").c_str(), S_IWUSR);
    PCHECK(fd >= 0);
    int err = fallocate(fd, 0, 0, 4096);
    if (err != 0) {
      PCHECK(errno == ENOTSUP);
      fallocate_supported_ = false;
    } else {
      fallocate_supported_ = true;
    }
    close(fd);

    checked = true;
  }

 protected:

  void VerifyTestData(const Slice& read_data, size_t offset) {
    for (int i = 0; i < read_data.size(); i++) {
      size_t file_offset = offset + i;
      ASSERT_EQ((file_offset * 31) & 0xff, read_data[i]) << "failed at " << i;
    }
  }

  void MakeVectors(int num_slices, int slice_size, int num_iterations,
                   gscoped_ptr<faststring[]>* data, vector<vector<Slice > >* vec) {
    data->reset(new faststring[num_iterations * num_slices]);
    vec->resize(num_iterations);

    int data_idx = 0;
    int byte_idx = 0;
    for (int vec_idx = 0; vec_idx < num_iterations; vec_idx++) {
      vector<Slice>& iter_vec = vec->at(vec_idx);
      iter_vec.resize(num_slices);
      for (int i = 0; i < num_slices; i++) {
        (*data)[data_idx].resize(slice_size);
        for (int j = 0; j < slice_size; j++) {
          (*data)[data_idx][j] = (byte_idx * 31) & 0xff;
          ++byte_idx;
        }
        iter_vec[i]= Slice((*data)[data_idx]);
        ++data_idx;
      }
    }
  }

  void ReadAndVerifyTestData(RandomAccessFile* raf, size_t offset, size_t n) {
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[n]);
    Slice s;
    ASSERT_STATUS_OK(env_util::ReadFully(raf, offset, n, &s,
                                         scratch.get()));
    ASSERT_EQ(n, s.size());
    ASSERT_NO_FATAL_FAILURE(VerifyTestData(s, offset));
  }

  void TestAppendVector(size_t num_slices, size_t slice_size, size_t iterations,
                        bool fast, bool pre_allocate, const WritableFileOptions& opts) {
    const string kTestPath = GetTestPath("test_env_appendvec_read_append");
    shared_ptr<WritableFile> file;
    ASSERT_STATUS_OK(env_util::OpenFileForWrite(opts, env_.get(), kTestPath, &file));

    if (pre_allocate) {
      ASSERT_STATUS_OK(file->PreAllocate(num_slices * slice_size * iterations));
      ASSERT_STATUS_OK(file->Sync());
    }

    gscoped_ptr<faststring[]> data;
    vector<vector<Slice> > input;

    MakeVectors(num_slices, slice_size, iterations, &data, &input);

    shared_ptr<RandomAccessFile> raf;

    if (!fast) {
      ASSERT_STATUS_OK(env_util::OpenFileForRandom(env_.get(), kTestPath, &raf));
    }

    srand(123);

    const string test_descr = strings::Substitute(
        "appending a vector of slices(number of slices=$0,size of slice=$1 b) $2 times",
        num_slices, slice_size, iterations);
    LOG_TIMING(INFO, test_descr)  {
      for (int i = 0; i < iterations; i++) {
        if (fast || random() % 2) {
          ASSERT_STATUS_OK(file->AppendVector(input[i]));
        } else {
          BOOST_FOREACH(const Slice& slice, input[i]) {
            ASSERT_STATUS_OK(file->Append(slice));
          }
        }
        if (!fast) {
          // Verify as write. Note: this requires that file is pre-allocated, otherwise
          // the ReadFully() fails with EINVAL.
          ASSERT_NO_FATAL_FAILURE(ReadAndVerifyTestData(raf.get(), num_slices * slice_size * i,
                                                        num_slices * slice_size));
        }
      }
    }

    // Verify the entire file
    ASSERT_STATUS_OK(file->Close());

    if (fast) {
      ASSERT_STATUS_OK(env_util::OpenFileForRandom(env_.get(), kTestPath, &raf));
    }
    for (int i = 0; i < iterations; i++) {
      ASSERT_NO_FATAL_FAILURE(ReadAndVerifyTestData(raf.get(), num_slices * slice_size * i,
                                                    num_slices * slice_size));
    }
  }

  void DoTestPreallocate(const WritableFileOptions& opts) {
    LOG(INFO) << "Testing PreAllocate() with mmap "
              << (opts.mmap_file ? "enabled" : "disabled");

    string test_path = GetTestPath("test_env_wf");
    shared_ptr<WritableFile> file;
    ASSERT_STATUS_OK(env_util::OpenFileForWrite(opts, env_.get(), test_path, &file));

    // pre-allocate 1 MB
    ASSERT_STATUS_OK(file->PreAllocate(kOneMb));
    ASSERT_STATUS_OK(file->Sync());

    // the writable file size should report 0
    ASSERT_EQ(file->Size(), 0);
    // but the real size of the file on disk should report 1MB
    uint64_t size;
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(size, kOneMb);

    // write 1 MB
    uint8_t scratch[kOneMb];
    Slice slice(scratch, kOneMb);
    ASSERT_STATUS_OK(file->Append(slice));
    ASSERT_STATUS_OK(file->Sync());

    // the writable file size should now report 1 MB
    ASSERT_EQ(file->Size(), kOneMb);
    ASSERT_STATUS_OK(file->Close());
    // and the real size for the file on disk should match ony the
    // written size
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(kOneMb, size);
  }

  void DoTestConsecutivePreallocate(const WritableFileOptions& opts) {
    LOG(INFO) << "Testing consecutive PreAllocate() with mmap "
              << (opts.mmap_file ? "enabled" : "disabled");

    string test_path = GetTestPath("test_env_wf");
    shared_ptr<WritableFile> file;
    ASSERT_STATUS_OK(env_util::OpenFileForWrite(opts, env_.get(), test_path, &file));

    // pre-allocate 64 MB
    ASSERT_STATUS_OK(file->PreAllocate(64 * kOneMb));
    ASSERT_STATUS_OK(file->Sync());

    // the writable file size should report 0
    ASSERT_EQ(file->Size(), 0);
    // but the real size of the file on disk should report 64 MBs
    uint64_t size;
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(size, 64 * kOneMb);

    // write 1 MB
    uint8_t scratch[kOneMb];
    Slice slice(scratch, kOneMb);
    ASSERT_STATUS_OK(file->Append(slice));
    ASSERT_STATUS_OK(file->Sync());

    // the writable file size should now report 1 MB
    ASSERT_EQ(kOneMb, file->Size());
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(64 * kOneMb, size);

    // pre-allocate 64 additional MBs
    ASSERT_STATUS_OK(file->PreAllocate(64 * kOneMb));
    ASSERT_STATUS_OK(file->Sync());

    // the writable file size should now report 1 MB
    ASSERT_EQ(kOneMb, file->Size());
    // while the real file size should report 128 MB's
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(128 * kOneMb, size);

    // write another MB
    ASSERT_STATUS_OK(file->Append(slice));
    ASSERT_STATUS_OK(file->Sync());

    // the writable file size should now report 2 MB
    ASSERT_EQ(file->Size(), 2 * kOneMb);
    // while the real file size should reamin at 128 MBs
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(128 * kOneMb, size);

    // close the file (which ftruncates it to the real size)
    ASSERT_STATUS_OK(file->Close());
    // and the real size for the file on disk should match only the written size
    ASSERT_STATUS_OK(env_->GetFileSize(test_path, &size));
    ASSERT_EQ(2* kOneMb, size);
  }

  void DoTestAppendVector(const WritableFileOptions& opts) {
    LOG(INFO) << "Testing AppendVector() with mmap "
              << (opts.mmap_file ? "enabled" : "disabled");
    LOG(INFO) << "Testing AppendVector() only, NO pre-allocation";
    ASSERT_NO_FATAL_FAILURE(TestAppendVector(2000, 1024, 5, true, false, opts));

    if (!fallocate_supported_) {
      LOG(INFO) << "fallocate not supported, skipping preallocated runs";
    } else {
      LOG(INFO) << "Testing AppendVector() only, WITH pre-allocation";
      ASSERT_NO_FATAL_FAILURE(TestAppendVector(2000, 1024, 5, true, true, opts));
      LOG(INFO) << "Testing AppendVector() together with Append() and Read(), WITH pre-allocation";
      ASSERT_NO_FATAL_FAILURE(TestAppendVector(128, 4096, 5, false, true, opts));
    }
  }

  static bool fallocate_supported_;
};

bool TestEnv::fallocate_supported_ = false;

TEST_F(TestEnv, TestPreallocate) {
  if (!fallocate_supported_) {
    LOG(INFO) << "fallocate not supported, skipping test";
    return;
  }
  WritableFileOptions opts;
  opts.mmap_file = true;
  ASSERT_NO_FATAL_FAILURE(DoTestPreallocate(opts));
  opts.mmap_file = false;
  ASSERT_NO_FATAL_FAILURE(DoTestPreallocate(opts));
}

// To test consecutive pre-allocations we need higher pre-allocations since the
// mmapped regions grow in size until 2MBs (so smaller pre-allocations will easily
// be smaller than the mmapped regions size).
TEST_F(TestEnv, TestConsecutivePreallocate) {
  if (!fallocate_supported_) {
    LOG(INFO) << "fallocate not supported, skipping test";
    return;
  }
  WritableFileOptions opts;
  opts.mmap_file = true;
  ASSERT_NO_FATAL_FAILURE(DoTestConsecutivePreallocate(opts));
  opts.mmap_file = false;
  ASSERT_NO_FATAL_FAILURE(DoTestConsecutivePreallocate(opts));
}

class ShortReadRandomAccessFile : public RandomAccessFile {
 public:
  explicit ShortReadRandomAccessFile(const shared_ptr<RandomAccessFile>& wrapped)
    : wrapped_(wrapped) {
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      uint8_t *scratch) const OVERRIDE {
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

  virtual Status Size(uint64_t *size) const OVERRIDE {
    return wrapped_->Size(size);
  }

 private:
  const shared_ptr<RandomAccessFile> wrapped_;
};

// Write 'size' bytes of data to a file, with a simple pattern stored in it.
static void WriteTestFile(Env* env, const string& path, size_t size) {
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

TEST_F(TestEnv, TestAppendVector) {
  WritableFileOptions opts;
  opts.mmap_file = true;
  ASSERT_NO_FATAL_FAILURE(DoTestAppendVector(opts));
  opts.mmap_file = false;
  ASSERT_NO_FATAL_FAILURE(DoTestAppendVector(opts));
}

TEST_F(TestEnv, TestGetExecutablePath) {
  string p;
  ASSERT_STATUS_OK(Env::Default()->GetExecutablePath(&p));
  ASSERT_TRUE(HasSuffixString(p, "env-test")) << p;
}

TEST_F(TestEnv, TestOpenEmptyRandomAccessFile) {
  Env* env = Env::Default();
  string test_file = JoinPathSegments(GetTestDataDirectory(), "test_file");
  ASSERT_NO_FATAL_FAILURE(WriteTestFile(env, test_file, 0));
  RandomAccessFile* readable_file;
  ASSERT_OK(env->NewRandomAccessFile(test_file, &readable_file));
  shared_ptr<RandomAccessFile> deleter(readable_file);
  uint64_t size;
  ASSERT_OK(readable_file->Size(&size));
  ASSERT_EQ(0, size);
}

}  // namespace kudu
