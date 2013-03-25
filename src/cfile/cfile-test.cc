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

#include "cfile-test-base.h"
#include "cfile.h"
#include "cfile_reader.h"
#include "cfile.pb.h"
#include "index_block.h"
#include "index_btree.h"

namespace kudu { namespace cfile {

class TestCFile : public CFileTestBase {
};


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

#ifdef NDEBUG
// Only run the 100M entry tests in non-debug mode.
// They take way too long with debugging enabled.

TEST_F(TestCFile, TestWrite100MFileInts) {
  const string path = GetTestPath("Test100M");
  LOG_TIMING(INFO, "writing 100m ints") {
    LOG(INFO) << "Starting writefile";
    WriteTestFileInts(path, GROUP_VARINT, NO_COMPRESSION, 100000000);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M ints") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(path, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}

TEST_F(TestCFile, TestWrite100MFileStrings) {
  const string path = GetTestPath("Test100MStrings");
  LOG_TIMING(INFO, "writing 100M strings") {
    LOG(INFO) << "Starting writefile";
    WriteTestFileStrings(path, PREFIX, NO_COMPRESSION,
                         100000000, "hello %d");
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M strings") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(path, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}
#endif

TEST_F(TestCFile, TestReadWriteInts) {
  Env *env = env_.get();

  const string path = GetTestPath("cfile");
  WriteTestFileInts(path, GROUP_VARINT, NO_COMPRESSION, 10000);

  gscoped_ptr<CFileReader> reader;
  ASSERT_STATUS_OK(CFileReader::Open(env, path, ReaderOptions(), &reader));

  BlockPointer ptr;

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK( reader->NewIterator(&iter) );

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
    out[i] = 0;
  }
  

  // Fetch all data using small batches of only a few rows.
  // This should catch edge conditions like a batch lining up exactly
  // with the end of a block.
  unsigned int seed = time(NULL);
  LOG(INFO) << "Using random seed: " << seed;
  srand(seed);
  iter->SeekToOrdinal(0);
  ColumnBlock advancing_block = out;
  size_t fetched = 0;
  while (fetched < 10000) {
    ASSERT_TRUE(iter->HasNext());
    size_t batch_size = random() % 5 + 1;
    size_t n = batch_size;
    ASSERT_STATUS_OK(iter->CopyNextValues(&n, &advancing_block));
    ASSERT_LE(n, batch_size);
    advancing_block.Advance(n);
    fetched += n;
  }
  ASSERT_FALSE(iter->HasNext());

  // Re-verify
  for (int i = 0; i < 10000; i++) {
    if (out[i] != i * 10) {
      FAIL() << "mismatch at index " << i
             << " expected: " << (i * 10)
             << " got: " << out[i];
    }
    out[i] = 0;
  }


  TimeReadFile(path, &n);
  ASSERT_EQ(10000, n);
}

TEST_F(TestCFile, TestReadWriteStrings) {
  Env *env = env_.get();

  const int nrows = 10000;
  const string path = GetTestPath("cfile");
  WriteTestFileStrings(path, PREFIX, NO_COMPRESSION, nrows, "hello %04d");

  gscoped_ptr<CFileReader> reader;
  ASSERT_STATUS_OK(CFileReader::Open(env, path, ReaderOptions(), &reader));

  size_t reader_nrows;
  ASSERT_STATUS_OK(reader->CountRows(&reader_nrows));
  ASSERT_EQ(nrows, reader_nrows);

  BlockPointer ptr;

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK( reader->NewIterator(&iter) );

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
