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
#include <memory>
#include <ostream>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/bloomfile-test-base.h"
#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/endian.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"

using std::shared_ptr;

namespace kudu {
namespace cfile {

using fs::CountingReadableBlock;
using fs::ReadableBlock;
using std::unique_ptr;

class BloomFileTest : public BloomFileTestBase {

 protected:
  void VerifyBloomFile() {
    // Verify all the keys that we inserted probe as present.
    for (uint64_t i = 0; i < FLAGS_n_keys; i++) {
      uint64_t i_byteswapped = BigEndian::FromHost64(i << kKeyShift);
      Slice s(reinterpret_cast<char *>(&i_byteswapped), sizeof(i));

      bool present = false;
      ASSERT_OK_FAST(bfr_->CheckKeyPresent(BloomKeyProbe(s), &present));
      ASSERT_TRUE(present);
    }

    int positive_count = 0;
    // Check that the FP rate for keys we didn't insert is what we expect.
    for (uint64_t i = 0; i < FLAGS_n_keys; i++) {
      uint64_t key = random();
      Slice s(reinterpret_cast<char *>(&key), sizeof(key));

      bool present = false;
      ASSERT_OK_FAST(bfr_->CheckKeyPresent(BloomKeyProbe(s), &present));
      if (present) {
        positive_count++;
      }
    }

    double fp_rate = static_cast<double>(positive_count) / FLAGS_n_keys;
    LOG(INFO) << "fp_rate: " << fp_rate << "(" << positive_count << "/" << FLAGS_n_keys << ")";
    ASSERT_LT(fp_rate, FLAGS_fp_rate + FLAGS_fp_rate * 0.20f)
      << "Should be no more than 1.2x the expected FP rate";
  }
};


TEST_F(BloomFileTest, TestWriteAndRead) {
  ASSERT_NO_FATAL_FAILURE(WriteTestBloomFile());
  ASSERT_OK(OpenBloomFile());
  VerifyBloomFile();
}

#ifdef NDEBUG
TEST_F(BloomFileTest, Benchmark) {
  ASSERT_NO_FATAL_FAILURE(WriteTestBloomFile());
  ASSERT_OK(OpenBloomFile());

  uint64_t count_present = ReadBenchmark();

  double hit_rate = static_cast<double>(count_present) /
    static_cast<double>(FLAGS_benchmark_queries);
  LOG(INFO) << "Hit Rate: " << hit_rate <<
    "(" << count_present << "/" << FLAGS_benchmark_queries << ")";

  if (FLAGS_benchmark_should_hit) {
    ASSERT_EQ(count_present, FLAGS_benchmark_queries);
  } else {
    ASSERT_LT(hit_rate, FLAGS_fp_rate + FLAGS_fp_rate * 0.20f)
      << "Should be no more than 1.2x the expected FP rate";
  }
}
#endif

TEST_F(BloomFileTest, TestLazyInit) {
  ASSERT_NO_FATAL_FAILURE(WriteTestBloomFile());

  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test");
  int64_t initial_mem_usage = tracker->consumption();

  // Open the bloom file using a "counting" readable block.
  unique_ptr<ReadableBlock> block;
  ASSERT_OK(fs_manager_->OpenBlock(block_id_, &block));
  size_t bytes_read = 0;
  unique_ptr<ReadableBlock> count_block(
      new CountingReadableBlock(std::move(block), &bytes_read));

  // Lazily opening the bloom file should not trigger any reads,
  // and the file size should be available before Init().
  gscoped_ptr<BloomFileReader> reader;
  ReaderOptions opts;
  opts.parent_mem_tracker = tracker;
  ASSERT_OK(BloomFileReader::OpenNoInit(std::move(count_block), opts, &reader));
  ASSERT_EQ(0, bytes_read);
  int64_t lazy_mem_usage = tracker->consumption();
  ASSERT_GT(lazy_mem_usage, initial_mem_usage);
  ASSERT_GT(reader->FileSize(), 0);

  // But initializing it should (only the first time), and the bloom's
  // memory usage should increase.
  ASSERT_OK(reader->Init());
  ASSERT_GT(bytes_read, 0);
  size_t bytes_read_after_init = bytes_read;
  ASSERT_OK(reader->Init());
  ASSERT_EQ(bytes_read_after_init, bytes_read);
  ASSERT_GT(tracker->consumption(), lazy_mem_usage);

  // And let's test non-lazy open for good measure; it should yield the
  // same number of bytes read.
  ASSERT_OK(fs_manager_->OpenBlock(block_id_, &block));
  bytes_read = 0;
  count_block.reset(new CountingReadableBlock(std::move(block), &bytes_read));
  ASSERT_OK(BloomFileReader::Open(std::move(count_block), ReaderOptions(), &reader));
  ASSERT_EQ(bytes_read_after_init, bytes_read);
}

} // namespace cfile
} // namespace kudu
