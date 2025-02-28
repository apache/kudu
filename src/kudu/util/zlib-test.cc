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

#include "kudu/util/zlib.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>

#include <gtest/gtest.h>
#include <zconf.h>
#include <zlib.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::ostringstream;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace zlib {

class ZlibTest : public KuduTest {
 public:
  ZlibTest()
      : rand_(SeedRandom()) {
  }

  void GenerateRandomData(uint8_t* out, size_t size) {
    uint64_t* p64 = reinterpret_cast<uint64_t*>(out);
    for (size_t i = 0; i < size / 8; ++i) {
      *p64++ = rand_.Next64();
    }
    const uint8_t* p8_end = out + size;
    uint8_t* p8 = reinterpret_cast<uint8_t*>(p64);
    if (p8 < p8_end) {
      const uint64_t val = rand_.Next64();
      uint8_t bit_shift = 0;
      do {
        *p8++ = static_cast<uint8_t>(val >> bit_shift);
        bit_shift += 8;
      } while (p8 < p8_end);
    }
  }

protected:
  Random rand_;
};

TEST_F(ZlibTest, CompressUncompress) {
  // Set this to be a multiple of the maximum possible LZ77 window size.
  constexpr const size_t kMaxSize = 64 * 1024;
  static_assert((kMaxSize / (1 << MAX_WBITS)) >= 2);
  static_assert(Z_NO_COMPRESSION <= Z_BEST_COMPRESSION);

  SKIP_IF_SLOW_NOT_ALLOWED();

  unique_ptr<uint8_t[]> chunk(new uint8_t[kMaxSize]);
  for (size_t size = 8; size <= kMaxSize;) {
    GenerateRandomData(chunk.get(), size);
    for (int level = Z_NO_COMPRESSION; level <= Z_BEST_COMPRESSION; ++level) {
      SCOPED_TRACE(Substitute("compression level $0 size $1", level, size));
      Slice data_p(chunk.get(), size);
      ostringstream oss_c;
      ASSERT_OK(CompressLevel(data_p, level, &oss_c));
      const string& data_str_c(oss_c.str());
      Slice data_c(data_str_c.data(), data_str_c.size());
      ostringstream oss_p;
      ASSERT_OK(Uncompress(data_c, &oss_p));
      const string& data_str_p(oss_p.str());
      ASSERT_EQ(size, data_str_p.size());
      ASSERT_EQ(0, memcmp(data_str_p.c_str(), chunk.get(), size));
    }
    size += (rand_.Uniform(1024) + 1);
  }
}

TEST_F(ZlibTest, UncompressCorruptedData) {
  constexpr const size_t kMaxSize = 64 * 1024;
  constexpr const int kLevel = Z_BEST_SPEED;

  SKIP_IF_SLOW_NOT_ALLOWED();

  unique_ptr<uint8_t[]> chunk(new uint8_t[kMaxSize]);
  for (size_t size = 256; size <= kMaxSize;) {
    SCOPED_TRACE(Substitute("size $0", size));
    GenerateRandomData(chunk.get(), size);
    Slice data_p(chunk.get(), size);
    ostringstream oss_c;
    ASSERT_OK(CompressLevel(data_p, kLevel, &oss_c));
    const string& data_str_c(oss_c.str());

    for (size_t bytes_num = 1; bytes_num < 32; ++bytes_num) {
      Slice data_c(data_str_c.data(), data_str_c.size() - bytes_num);
      ostringstream oss_p;
      const auto& s = Uncompress(data_c, &oss_p);
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "truncated gzip data");
    }

    {
      // Reverse the sequence of bytes in the input data.
      string data_str_c_reversed;
      data_str_c_reversed.reserve(data_str_c.size());
      std::reverse_copy(data_str_c.begin(),
                        data_str_c.end(),
                        std::back_inserter(data_str_c_reversed));
      Slice data_c(data_str_c_reversed.data(), data_str_c_reversed.size());
      ostringstream oss_p;
      const auto& s = Uncompress(data_c, &oss_p);
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "zlib error: DATA_ERROR");
    }

    {
      // Replace the second half of the data with a bogus ASCII symbol.
      string data_str_cc = data_str_c;
      auto it = data_str_cc.begin();
      std::advance(it, size / 2);
      std::fill(it, data_str_cc.end(), 'x');
      Slice data_c(data_str_cc.data(), data_str_cc.size());
      ostringstream oss_p;
      const auto& s = Uncompress(data_c, &oss_p);
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "zlib error: DATA_ERROR");
    }
    size += (rand_.Uniform(1024) + 1);
  }
}

} // namespace zlib
} // namespace kudu
