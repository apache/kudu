// Copyright (c) 2013, Cloudera, inc.

#include <arpa/inet.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/bloomfile.h"
#include "kudu/gutil/endian.h"
#include "kudu/util/env.h"
#include "kudu/util/memenv/memenv.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"


DEFINE_int32(bloom_size_bytes, 4*1024, "Size of each bloom filter");
DEFINE_int32(n_keys, 10*1000, "Number of keys to insert into the file");
DEFINE_double(fp_rate, 0.01f, "False positive rate to aim for");

DEFINE_int64(benchmark_queries, 1000000, "Number of probes to benchmark");
DEFINE_bool(benchmark_should_hit, false, "Set to true for the benchmark to query rows which match");

namespace kudu {
namespace cfile {

static const int kKeyShift = 2;

static void AppendBlooms(BloomFileWriter *bfw) {
  uint64_t key_buf;
  Slice key_slice(reinterpret_cast<const uint8_t *>(&key_buf),
                  sizeof(key_buf));

  for (uint64_t i = 0; i < FLAGS_n_keys; i++) {
    // Shift the key left a bit so that while querying, we can
    // get a good mix of hits and misses while still staying within
    // the real key range.
    key_buf = BigEndian::FromHost64(i << kKeyShift);
    ASSERT_STATUS_OK_FAST(bfw->AppendKeys(&key_slice, 1));
  }
}

static void WriteTestBloomFile(Env *env, const string &path) {
  WritableFile *file;
  ASSERT_STATUS_OK(env->NewWritableFile(path, &file));
  shared_ptr<WritableFile> sink(file);

  // Set sizing based on flags
  BloomFilterSizing sizing = BloomFilterSizing::BySizeAndFPRate(
    FLAGS_bloom_size_bytes, FLAGS_fp_rate);
  ASSERT_NEAR(sizing.n_bytes(), FLAGS_bloom_size_bytes, FLAGS_bloom_size_bytes * 0.05);
  ASSERT_GT(FLAGS_n_keys, sizing.expected_count())
    << "Invalid parameters: --n_keys isn't set large enough to fill even "
    << "one bloom filter of the requested --bloom_size_bytes";

  BloomFileWriter bfw(sink, sizing);

  ASSERT_STATUS_OK(bfw.Start());
  AppendBlooms(&bfw);
  ASSERT_STATUS_OK(bfw.Finish());
}

// Verify that all the entries we put in the bloom file check as
// present, and verify that entries we didn't put in have the
// expected false positive rate.
static void VerifyBloomFile(Env *env, const string &path) {
  gscoped_ptr<BloomFileReader> bfr;
  ASSERT_STATUS_OK(BloomFileReader::Open(env, path, &bfr));

  // Verify all the keys that we inserted probe as present.
  for (uint64_t i = 0; i < FLAGS_n_keys; i++) {
    uint64_t i_byteswapped = BigEndian::FromHost64(i << kKeyShift);
    Slice s(reinterpret_cast<char *>(&i_byteswapped), sizeof(i));

    bool present = false;
    ASSERT_STATUS_OK_FAST(bfr->CheckKeyPresent(BloomKeyProbe(s), &present));
    ASSERT_TRUE(present);
  }

  int positive_count = 0;
  // Check that the FP rate for keys we didn't insert is what we expect.
  for (uint64 i = 0; i < FLAGS_n_keys; i++) {
    uint64_t key = random();
    Slice s(reinterpret_cast<char *>(&key), sizeof(key));

    bool present = false;
    ASSERT_STATUS_OK_FAST(bfr->CheckKeyPresent(BloomKeyProbe(s), &present));
    if (present) {
      positive_count++;
    }
  }

  double fp_rate = static_cast<double>(positive_count) / FLAGS_n_keys;
  LOG(INFO) << "fp_rate: " << fp_rate << "(" << positive_count << "/" << FLAGS_n_keys << ")";
  ASSERT_LT(fp_rate, FLAGS_fp_rate + FLAGS_fp_rate * 0.20f)
    << "Should be no more than 1.2x the expected FP rate";
}


TEST(TestBloomFile, TestWriteAndRead) {
  gscoped_ptr<Env> env(NewMemEnv(Env::Default()));

  string path("/test-bloomfile");
  ASSERT_NO_FATAL_FAILURE(
    WriteTestBloomFile(env.get(), path));
  VerifyBloomFile(env.get(), path);
}

#ifdef NDEBUG
TEST(TestBloomFile, Benchmark) {
  gscoped_ptr<Env> env(NewMemEnv(Env::Default()));

  string path("/test-bloomfile");
  ASSERT_NO_FATAL_FAILURE(
    WriteTestBloomFile(env.get(), path));

  gscoped_ptr<BloomFileReader> bfr;
  ASSERT_STATUS_OK(BloomFileReader::Open(env.get(), path, &bfr));

  uint64_t count_present = 0;
  LOG_TIMING(INFO, StringPrintf("Running %ld queries", FLAGS_benchmark_queries)) {

    for (uint64_t i = 0; i < FLAGS_benchmark_queries; i++) {
      uint64_t key = random() % FLAGS_n_keys;
      key <<= kKeyShift;
      if (!FLAGS_benchmark_should_hit) {
        // Since the keys are bitshifted, setting the last bit
        // ensures that none of the queries will match.
        key |= 1;
      }

      key = BigEndian::FromHost64(key);

      Slice s(reinterpret_cast<uint8_t *>(&key), sizeof(key));
      bool present;
      CHECK_OK(bfr->CheckKeyPresent(BloomKeyProbe(s), &present));
      if (present) count_present++;
    }
  }

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
