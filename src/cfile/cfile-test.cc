// Copyright (c) 2012, Cloudera, inc

#include <boost/scoped_array.hpp>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <stdlib.h>

#include "gutil/stringprintf.h"
#include "util/env.h"
#include "util/test_macros.h"

#include "cfile.h"
#include "cfile_reader.h"
#include "cfile.pb.h"
#include "index_block.h"
#include "index_btree.h"

namespace kudu { namespace cfile {

class StringSink: public WritableFile {
 public:
  ~StringSink() { }

  const std::string& contents() const { return contents_; }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }

  virtual Status Append(const Slice& data) {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

static void WriteTestFileStrings(
  const string &path, int num_entries) {

  Status s;

  WritableFile *file;
  s = Env::Default()->NewWritableFile(path, &file);

  shared_ptr<WritableFile> sink(file);
  WriterOptions opts;
  // Use a smaller block size to exercise multi-level
  // indexing.
  opts.block_size = 1024;
  Writer w(opts, STRING, PREFIX, sink);

  ASSERT_STATUS_OK(w.Start());

  // Append given number of values to the test tree
  char data[20];
  for (int i = 0; i < num_entries; i++) {
    int len = snprintf(data, sizeof(data), "hello %d", i);
    Slice slice(data, len);

    Status s = w.AppendEntries(&slice, 1);
    // Dont use ASSERT because it accumulates all the logs
    // even for successes
    if (!s.ok()) {
      FAIL() << "Failed Append(" << i << ")";
    }
  }

  ASSERT_STATUS_OK(w.Finish());
}

static void WriteTestFile(const string &path,
                          int num_entries) {
  Status s;

  WritableFile *file;
  s = Env::Default()->NewWritableFile(path, &file);

  shared_ptr<WritableFile> sink(file);
  WriterOptions opts;
  // Use a smaller block size to exercise multi-level
  // indexing.
  opts.block_size = 256;
  Writer w(opts, UINT32, GROUP_VARINT, sink);

  ASSERT_STATUS_OK(w.Start());

  // Append given number of values to the test tree
  for (int i = 0; i < num_entries; i++) {
    uint32_t val = i * 10;
    Status s = w.AppendEntries(&val, 1);
    // Dont use ASSERT because it accumulates all the logs
    // even for successes
    if (!s.ok()) {
      FAIL() << "Failed Append(" << i << ")";
    }
  }

  ASSERT_STATUS_OK(w.Finish());
}

static void TimeReadFile(const string &path) {
  Env *env = Env::Default();
  Status s;

  RandomAccessFile *raf;
  uint64_t size;
  ASSERT_STATUS_OK(env->NewRandomAccessFile(path, &raf));
  ASSERT_STATUS_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  ASSERT_STATUS_OK(reader.Init());

  CFileIterator *iter_ptr;
  ASSERT_STATUS_OK( reader.NewIterator(&iter_ptr) );
  scoped_ptr<CFileIterator> iter(iter_ptr);
  iter->SeekToOrdinal(0);


  switch (reader.data_type()) {
    case UINT32:
    {
      boost::scoped_array<uint32_t> v(new uint32_t[8192]);
      uint64_t sum = 0;
      int count = 0;
      while (iter->HasNext()) {
        int n;
        ASSERT_STATUS_OK_FAST(iter->GetNextValues(8192, &v[0], &n));
        for (int i = 0; i < n; i++) {
          sum += v[i];
        }
        count += n;
      }
      LOG(INFO) << "Sum: " << sum;
      LOG(INFO) << "Count: " << count;
      break;
    }
    case STRING:
    {
      boost::scoped_array<Slice> v(new Slice[100]);
      uint64_t sum_lens = 0;
      int count = 0;
      while (iter->HasNext()) {
        int n;
        ASSERT_STATUS_OK_FAST(iter->GetNextValues(100, &v[0], &n));
        for (int i = 0; i < n; i++) {
          sum_lens += v[i].size();
        }
        count += n;
      }
      LOG(INFO) << "Sum of value lengths: " << sum_lens;
      LOG(INFO) << "Count: " << count;
      break;
    }
    default:
      FAIL() << "Unknown type: " << reader.data_type();
  }
}


TEST(TestCFile, TestWrite100MFileInts) {
  LOG(INFO) << "Starting writefile";
  WriteTestFile("/tmp/cfile-Test100M", 100000000);
  LOG(INFO) << "Done writing";

  LOG(INFO) << "Starting readfile";
  TimeReadFile("/tmp/cfile-Test100M");
  LOG(INFO) << "End readfile";
}

TEST(TestCFile, TestWrite100MFileStrings) {
  LOG(INFO) << "Starting writefile";
  WriteTestFileStrings("/tmp/cfile-Test100M-Strings", 100000000);
  LOG(INFO) << "Done writing";

  LOG(INFO) << "Starting readfile";
  TimeReadFile("/tmp/cfile-Test100M-Strings");
  LOG(INFO) << "End readfile";
}

TEST(TestCFile, TestReadWriteInts) {
  Env *env = Env::Default();

  string path = "/tmp/cfile-TestReadWrite";
  WriteTestFile(path, 10000);

  RandomAccessFile *raf;
  uint64_t size;
  ASSERT_STATUS_OK(env->NewRandomAccessFile(path, &raf));
  ASSERT_STATUS_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  ASSERT_STATUS_OK(reader.Init());

  BlockPointer ptr;

  CFileIterator *iter_ptr;
  ASSERT_STATUS_OK( reader.NewIterator(&iter_ptr) );
  scoped_ptr<CFileIterator> iter(iter_ptr);

  ASSERT_STATUS_OK(iter->SeekToOrdinal(5000));
  ASSERT_EQ(5000u, iter->GetCurrentOrdinal());

  // Seek to last key exactly, should succeed
  ASSERT_STATUS_OK(iter->SeekToOrdinal(9999));
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

  // Seek to after last key. Should result in not found.
  ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());

  // Seek to start of file
  ASSERT_STATUS_OK(iter->SeekToOrdinal(0));
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());

  // Fetch all data.
  boost::scoped_array<uint32_t> out(new uint32_t[10000]);
  int n;
  ASSERT_STATUS_OK(iter->GetNextValues(10000, &out[0], &n));
  ASSERT_EQ(10000, n);
}

TEST(TestCFile, TestReadWriteStrings) {
  Env *env = Env::Default();

  string path = "/tmp/cfile-TestReadWriteStrings";
  WriteTestFileStrings(path, 10000);
  RandomAccessFile *raf;
  uint64_t size;
  ASSERT_STATUS_OK(env->NewRandomAccessFile(path, &raf));
  ASSERT_STATUS_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  ASSERT_STATUS_OK(reader.Init());

  BlockPointer ptr;

  CFileIterator *iter_ptr;
  ASSERT_STATUS_OK( reader.NewIterator(&iter_ptr) );
  scoped_ptr<CFileIterator> iter(iter_ptr);

  ASSERT_STATUS_OK(iter->SeekToOrdinal(5000));
  ASSERT_EQ(5000u, iter->GetCurrentOrdinal());
  Slice s;
  int n;
  ASSERT_STATUS_OK(iter->GetNextValues(1, &s, &n));
  ASSERT_EQ(1, n);
  ASSERT_EQ(string("hello 5000"), s.ToString());

  // Seek to last key exactly, should succeed
  ASSERT_STATUS_OK(iter->SeekToOrdinal(9999));
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

  // Seek to after last key. Should result in not found.
  ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());


  ////////
  // Now try some seeks by the value instead of position
  /////////
  s = "hello 5000.5";
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(&s));
  ASSERT_EQ(5001u, iter->GetCurrentOrdinal());
  ASSERT_STATUS_OK(iter->GetNextValues(1, &s, &n));
  ASSERT_EQ(1, n);
  ASSERT_EQ(string("hello 5001"), s.ToString());



  // Seek to start of file
  ASSERT_STATUS_OK(iter->SeekToOrdinal(0));
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());

  // Fetch all data.
  boost::scoped_array<Slice> out(new Slice[10000]);
  ASSERT_STATUS_OK(iter->GetNextValues(10000, &out[0], &n));
  ASSERT_EQ(10000, n);
}


} // namespace cfile
} // namespace kudu

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
