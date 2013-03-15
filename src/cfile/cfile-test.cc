// Copyright (c) 2012, Cloudera, inc

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <stdlib.h>

#include "common/columnblock.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/stringprintf.h"
#include "util/env.h"
#include "util/test_macros.h"
#include "util/stopwatch.h"

#include "cfile.h"
#include "cfile_reader.h"
#include "cfile.pb.h"
#include "index_block.h"
#include "index_btree.h"

namespace kudu { namespace cfile {


template<DataType type>
void CopyOne(CFileIterator *it,
             typename TypeTraits<type>::cpp_type *ret,
             Arena *arena) {
  ColumnBlock cb(GetTypeInfo(type), ret,
                 TypeTraits<type>::size, 1, arena);
  size_t n = 1;
  ASSERT_STATUS_OK(it->CopyNextValues(&n, &cb));
  ASSERT_EQ(1, n);
}


// Fast unrolled summing of a vector.
// GCC's auto-vectorization doesn't work here, because there isn't
// enough guarantees on alignment and it can't seem to decude the
// constant stride.
template<class Indexable>
uint64_t FastSum(const Indexable &data, size_t n) {
  uint64_t sums[4] = {0, 0, 0, 0};
  int rem = n;
  int i = 0;
  while (rem >= 4) {
    sums[0] += data[i];
    sums[1] += data[i+1];
    sums[2] += data[i+2];
    sums[3] += data[i+3];
    i += 4;
    rem -= 4;
  }
  while (rem > 0) {
    sums[3] += data[i++];
    rem--;
  }
  return sums[0] + sums[1] + sums[2] + sums[3];
}

static void WriteTestFileStrings(
  const string &path, int num_entries,
  const char *format) {

  Status s;

  WritableFile *file;
  s = Env::Default()->NewWritableFile(path, &file);

  shared_ptr<WritableFile> sink(file);
  WriterOptions opts;
  opts.write_posidx = true;
  opts.write_validx = true;
  // Use a smaller block size to exercise multi-level
  // indexing.
  opts.block_size = 1024;
  Writer w(opts, STRING, PREFIX, sink);

  ASSERT_STATUS_OK(w.Start());

  // Append given number of values to the test tree
  char data[20];
  for (int i = 0; i < num_entries; i++) {
    int len = snprintf(data, sizeof(data), format, i);
    Slice slice(data, len);

    Status s = w.AppendEntries(&slice, 1, 0);
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
  opts.write_posidx = true;
  // Use a smaller block size to exercise multi-level
  // indexing.
  opts.block_size = 100;
  Writer w(opts, UINT32, GROUP_VARINT, sink);

  ASSERT_STATUS_OK(w.Start());

  uint32_t block[8096];
  size_t stride = sizeof(uint32_t);

  // Append given number of values to the test tree
  int i = 0;
  while (i < num_entries) {
    int towrite = std::min(num_entries - i, 8096);
    for (int j = 0; j < towrite; j++) {
      block[j] = i++ * 10;
    }

    Status s = w.AppendEntries(block, towrite, stride);
    // Dont use ASSERT because it accumulates all the logs
    // even for successes
    if (!s.ok()) {
      FAIL() << "Failed Append(" << (i - towrite) << ")";
    }
  }

  ASSERT_STATUS_OK(w.Finish());
}

static void TimeReadFile(const string &path, size_t *count_ret) {
  Env *env = Env::Default();
  Status s;

  RandomAccessFile *raf;
  uint64_t size;
  ASSERT_STATUS_OK(env->NewRandomAccessFile(path, &raf));
  ASSERT_STATUS_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  ASSERT_STATUS_OK(reader.Init());

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK( reader.NewIterator(&iter) );
  iter->SeekToOrdinal(0);

  Arena arena(8192, 8*1024*1024);
  int count = 0;
  switch (reader.data_type()) {
    case UINT32:
    {
      ScopedColumnBlock<UINT32> cb(8192);

      uint64_t sum = 0;
      while (iter->HasNext()) {
        size_t n = cb.size();
        ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
        sum += FastSum(cb, n);
        count += n;
        cb.arena()->Reset();
      }
      LOG(INFO) << "Sum: " << sum;
      LOG(INFO) << "Count: " << count;
      break;
    }
    case STRING:
    {
      ScopedColumnBlock<STRING> cb(100);
      uint64_t sum_lens = 0;
      while (iter->HasNext()) {
        size_t n = cb.size();
        ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
        for (int i = 0; i < n; i++) {
          sum_lens += cb[i].size();
        }
        count += n;
        cb.arena()->Reset();
      }
      LOG(INFO) << "Sum of value lengths: " << sum_lens;
      LOG(INFO) << "Count: " << count;
      break;
    }
    default:
      FAIL() << "Unknown type: " << reader.data_type();
  }
  *count_ret = count;
}

#ifdef NDEBUG
// Only run the 100M entry tests in non-debug mode.
// They take way too long with debugging enabled.

TEST(TestCFile, TestWrite100MFileInts) {
  LOG_TIMING(INFO, "writing 100m ints") {
    LOG(INFO) << "Starting writefile";
    WriteTestFile("/tmp/cfile-Test100M", 100000000);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M ints") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile("/tmp/cfile-Test100M", &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}

TEST(TestCFile, TestWrite100MFileStrings) {
  LOG_TIMING(INFO, "writing 100M strings") {
    LOG(INFO) << "Starting writefile";
    WriteTestFileStrings("/tmp/cfile-Test100M-Strings", 100000000,
                         "hello %d");
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M strings") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile("/tmp/cfile-Test100M-Strings", &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}
#endif

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

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK( reader.NewIterator(&iter) );

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
  ScopedColumnBlock<UINT32> out(10000);
  size_t n = 10000;
  ASSERT_STATUS_OK(iter->CopyNextValues(&n, &out));
  ASSERT_EQ(10000, n);

  for (int i = 0; i < 10000; i++) {
    if (out[i] != i * 10) {
      FAIL() << "mismatch at index " << i
             << " expected: " << (i * 10)
             << " got: " << out[i];
    }
  }

  TimeReadFile("/tmp/cfile-TestReadWrite", &n);
  ASSERT_EQ(10000, n);
}

TEST(TestCFile, TestReadWriteStrings) {
  Env *env = Env::Default();

  const int nrows = 10000;
  string path = "/tmp/cfile-TestReadWriteStrings";
  WriteTestFileStrings(path, nrows, "hello %04d");
  RandomAccessFile *raf;
  uint64_t size;
  ASSERT_STATUS_OK(env->NewRandomAccessFile(path, &raf));
  ASSERT_STATUS_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  ASSERT_STATUS_OK(reader.Init());

  size_t reader_nrows;
  ASSERT_STATUS_OK(reader.CountRows(&reader_nrows));
  ASSERT_EQ(nrows, reader_nrows);

  BlockPointer ptr;

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK( reader.NewIterator(&iter) );

  Arena arena(1024, 1024*1024);

  ASSERT_STATUS_OK(iter->SeekToOrdinal(5000));
  ASSERT_EQ(5000u, iter->GetCurrentOrdinal());
  Slice s;
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 5000"), s.ToString());

  // Seek to last key exactly, should succeed
  ASSERT_STATUS_OK(iter->SeekToOrdinal(9999));
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

  // Seek to after last key. Should result in not found.
  ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());


  ////////
  // Now try some seeks by the value instead of position
  /////////
  bool exact;
  s = "hello 5000.5";
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(&s, &exact));
  ASSERT_FALSE(exact);
  ASSERT_EQ(5001u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 5001"), s.ToString());

  s = "hello 9000";
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(&s, &exact));
  ASSERT_TRUE(exact);
  ASSERT_EQ(9000u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 9000"), s.ToString());

  // after last entry
  s = "hello 9999x";
  EXPECT_TRUE(iter->SeekAtOrAfter(&s, &exact).IsNotFound());

  // before first entry
  s = "hello";
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(&s, &exact));
  ASSERT_FALSE(exact);
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 0000"), s.ToString());

  // to last entry
  s = "hello 9999";
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(&s, &exact));
  ASSERT_TRUE(exact);
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 9999"), s.ToString());

  // Seek to start of file
  ASSERT_STATUS_OK(iter->SeekToOrdinal(0));
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 0000"), s.ToString());

  // Reseek to start and fetch all data.
  ASSERT_STATUS_OK(iter->SeekToOrdinal(0));

  ScopedColumnBlock<STRING> cb(10000);
  size_t n = 10000;
  ASSERT_STATUS_OK(iter->CopyNextValues(&n, &cb));
  ASSERT_EQ(10000, n);
}


} // namespace cfile
} // namespace kudu

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
