// Copyright (c) 2012, Cloudera, inc

#include <boost/scoped_array.hpp>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <stdlib.h>

#include "util/env.h"
#include "util/test_macros.h"
#include "util/test_util.h"
#include "util/status.h"

#include "cfile.h"
#include "cfile_reader.h"
#include "cfile.pb.h"
#include "cfile-test-base.h"
#include "compression_codec.h"
#include "index_block.h"
#include "index_btree.h"


namespace kudu {
namespace cfile {

static void TestCompressionCodec(CompressionType compression) {
  const int kInputSize = 64;

  shared_ptr<CompressionCodec> codec;
  uint8_t ibuffer[kInputSize];
  uint8_t ubuffer[kInputSize];
  size_t compressed;

  // Fill the test input buffer
  for (int i = 0 ; i < kInputSize; ++i) ibuffer[i] = '0';

  // Get the specified compression codec
  ASSERT_STATUS_OK(GetCompressionCodec(compression, &codec));

  // Allocate the compression buffer
  size_t max_compressed = codec->MaxCompressedLength(kInputSize);
  ASSERT_LT(max_compressed, (kInputSize * 2));
  gscoped_array<uint8_t> cbuffer(new uint8_t[max_compressed]);

  // Compress and uncompress
  ASSERT_STATUS_OK(codec->Compress(Slice(ibuffer, kInputSize), cbuffer.get(), &compressed));
  ASSERT_STATUS_OK(codec->Uncompress(Slice(cbuffer.get(), compressed), ubuffer, kInputSize));
  ASSERT_EQ(0, memcmp(ibuffer, ubuffer, kInputSize));
}

class TestCompression : public CFileTestBase {
protected:
  void TestReadWriteCompressed(CompressionType compression) {
    const size_t nrows = 10000;
    string path = GetTestPath("TestReadWriteCompressed");
    size_t rdrows;

    WriteTestFileStrings(path, PREFIX, compression, nrows, "hello %04d");
    TimeReadFile(path, &rdrows);
    ASSERT_EQ(nrows, rdrows);

    WriteTestFileInts(path, GROUP_VARINT, compression, nrows);
    TimeReadFile(path, &rdrows);
    ASSERT_EQ(nrows, rdrows);
  }
};

TEST_F(TestCompression, TestNoCompressionCodec) {
  shared_ptr<CompressionCodec> codec;
  ASSERT_STATUS_OK(GetCompressionCodec(NO_COMPRESSION, &codec));
  ASSERT_EQ(NULL, codec.get());
}

TEST_F(TestCompression, TestSnappyCompressionCodec) {
  TestCompressionCodec(SNAPPY);
}

TEST_F(TestCompression, TestLz4CompressionCodec) {
  TestCompressionCodec(LZ4);
}

TEST_F(TestCompression, TestZlibCompressionCodec) {
  TestCompressionCodec(ZLIB);
}

TEST_F(TestCompression, TestCFileNoCompressionReadWrite) {
  TestReadWriteCompressed(NO_COMPRESSION);
}

TEST_F(TestCompression, TestCFileSnappyReadWrite) {
  TestReadWriteCompressed(SNAPPY);
}

TEST_F(TestCompression, TestCFileLZ4ReadWrite) {
  TestReadWriteCompressed(SNAPPY);
}

TEST_F(TestCompression, TestCFileZlibReadWrite) {
  TestReadWriteCompressed(ZLIB);
}

} // namespace cfile
} // namespace kudu
