// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/cfile/bloomfile-test-base.h"

namespace kudu {
namespace cfile {

class BloomFileTest : public BloomFileTestBase {

 protected:
  void VerifyBloomFile() {
    // Verify all the keys that we inserted probe as present.
    for (uint64_t i = 0; i < FLAGS_n_keys; i++) {
      uint64_t i_byteswapped = BigEndian::FromHost64(i << kKeyShift);
      Slice s(reinterpret_cast<char *>(&i_byteswapped), sizeof(i));

      bool present = false;
      ASSERT_STATUS_OK_FAST(bfr_->CheckKeyPresent(BloomKeyProbe(s), &present));
      ASSERT_TRUE(present);
    }

    int positive_count = 0;
    // Check that the FP rate for keys we didn't insert is what we expect.
    for (uint64 i = 0; i < FLAGS_n_keys; i++) {
      uint64_t key = random();
      Slice s(reinterpret_cast<char *>(&key), sizeof(key));

      bool present = false;
      ASSERT_STATUS_OK_FAST(bfr_->CheckKeyPresent(BloomKeyProbe(s), &present));
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
  ASSERT_STATUS_OK(OpenBloomFile());
  VerifyBloomFile();
}

#ifdef NDEBUG
TEST_F(BloomFileTest, Benchmark) {
  ASSERT_NO_FATAL_FAILURE(WriteTestBloomFile());
  ASSERT_STATUS_OK(OpenBloomFile());

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

} // namespace cfile
} // namespace kudu
