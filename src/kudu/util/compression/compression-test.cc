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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using std::vector;

class TestCompression : public KuduTest, public ::testing::WithParamInterface<CompressionType> {
 public:
  void TestCompressionCodec() {
    CompressionType compression = GetParam();

    const int kInputSize = 64;

    const CompressionCodec *codec;
    uint8_t ibuffer[kInputSize];
    uint8_t ubuffer[kInputSize];
    size_t compressed;

    // Fill the test input buffer
    memset(ibuffer, 'Z', kInputSize);

    // Get the specified compression codec
    ASSERT_OK(GetCompressionCodec(compression, &codec));

    // Allocate the compression buffer
    size_t max_compressed = codec->MaxCompressedLength(kInputSize);
    ASSERT_LT(max_compressed, (kInputSize * 2));
    gscoped_array<uint8_t> cbuffer(new uint8_t[max_compressed]);

    // Compress slice and uncompress
    ASSERT_OK(codec->Compress(Slice(ibuffer, kInputSize), cbuffer.get(), &compressed));
    ASSERT_OK(codec->Uncompress(Slice(cbuffer.get(), compressed), ubuffer, kInputSize));
    ASSERT_EQ(0, memcmp(ibuffer, ubuffer, kInputSize));

    // Compress slices and uncompress
    vector<Slice> islices;
    const int kStep = 7;
    for (int i = 0; i < kInputSize; i += kStep)
      islices.emplace_back(ibuffer + i, std::min(kStep, kInputSize - i));
    ASSERT_OK(codec->Compress(islices, cbuffer.get(), &compressed));
    ASSERT_OK(codec->Uncompress(Slice(cbuffer.get(), compressed), ubuffer, kInputSize));
    ASSERT_EQ(0, memcmp(ibuffer, ubuffer, kInputSize));
  }

  void Benchmark() {
    CompressionType compression = GetParam();
    const int kMaterialCount = 16;
    const int kInputSize = 8;
    const int kSliceCount = 1024;

    // Prepare materials.
    char materials[kMaterialCount][kInputSize];
    Random r(0);  // Use seed '0' means generate the same data for all compression algorithms.
    for (auto& material : materials) {
      RandomString(material, kInputSize, &r);
    }

    // Prepare input slices.
    vector<Slice> islices;
    islices.reserve(kSliceCount);
    for (int i = 0; i < kSliceCount; ++i) {
      islices.emplace_back(Slice(materials[r.Next() % kMaterialCount], kInputSize));
    }

    // Get the specified compression codec.
    const CompressionCodec *codec;
    GetCompressionCodec(compression, &codec);

    // Allocate the compression buffer.
    size_t max_compressed = codec->MaxCompressedLength(kSliceCount * kInputSize);
    gscoped_array<uint8_t> cbuffer(new uint8_t[max_compressed]);

    // Execute Compress.
    size_t compressed;
    {
      uint64_t total_len = 0;
      uint64_t compressed_len = 0;
      Stopwatch sw;
      sw.start();
      while (sw.elapsed().wall_seconds() < 3) {
        codec->Compress(islices, cbuffer.get(), &compressed);
        total_len += kSliceCount * kInputSize;
        compressed_len += compressed;
      }
      sw.stop();
      double mbps = (total_len >> 20) / sw.elapsed().user_cpu_seconds();
      LOG(INFO) << GetCompressionCodecName(compression) << " compress throughput: "
                << mbps << " MB/sec, ratio: " << static_cast<double>(compressed_len) / total_len;
    }

    // Execute Uncompress.
    {
      uint8_t ubuffer[kSliceCount * kInputSize];
      uint64_t total_len = 0;
      Stopwatch sw;
      sw.start();
      while (sw.elapsed().wall_seconds() < 3) {
        codec->Uncompress(Slice(cbuffer.get(), compressed), ubuffer, kSliceCount * kInputSize);
        total_len += kInputSize;
      }
      sw.stop();
      double mbps = (total_len >> 10) / sw.elapsed().user_cpu_seconds();
      LOG(INFO) << GetCompressionCodecName(compression) << " uncompress throughput: "
                << mbps << " KB/sec";
    }
  }
};

INSTANTIATE_TEST_CASE_P(TestCompressionParameterized, TestCompression,
                        ::testing::Values(SNAPPY, LZ4, ZLIB));

TEST_F(TestCompression, TestNoCompressionCodec) {
  const CompressionCodec* codec;
  ASSERT_OK(GetCompressionCodec(NO_COMPRESSION, &codec));
  ASSERT_EQ(nullptr, codec);
}

TEST_P(TestCompression, TestSnappyCompressionCodec) {
  TestCompressionCodec();
}

TEST_P(TestCompression, TestSimpleBenchmark) {
  Benchmark();
}

} // namespace kudu
