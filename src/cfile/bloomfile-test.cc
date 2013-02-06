// Copyright (c) 2013, Cloudera, inc.

#include <arpa/inet.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "cfile/bloomfile.h"
#include "util/env.h"
#include "util/memenv/memenv.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"


DEFINE_int32(bloom_size_bytes, 4*1024, "Size of each bloom filter");
DEFINE_int32(n_keys, 10*1000, "Number of keys to insert into the file");
DEFINE_double(fp_rate, 0.01f, "False positive rate to aim for");

DEFINE_int64(benchmark_queries, 1000000, "Number of probes to benchmark");

namespace kudu {
namespace cfile {

static void AppendBlooms(BloomFileWriter *bfw) {
  for (uint32_t i = 0; i < FLAGS_n_keys; i++) {
    // Byte-swap the keys so that they're inserted in ascending
    // lexicographic order.
    // TODO: spent a while debugging this - would be good to add DCHECK
    // to the index builder code to ensure that keys go upward only
    uint32_t i_byteswapped = htonl(i);

    Slice s(reinterpret_cast<char *>(&i_byteswapped), sizeof(i));
    bfw->AppendKeys(&s, 1);
  }
}

static void WriteTestBloomFile(Env *env, const string &path) {
  WritableFile *file;
  ASSERT_STATUS_OK( env->NewWritableFile(path, &file) );
  shared_ptr<WritableFile> sink(file);

  // Set sizing based on flags
  BloomFilterSizing sizing = BloomFilterSizing::BySizeAndFPRate(
    FLAGS_bloom_size_bytes, FLAGS_fp_rate);
  ASSERT_NEAR( sizing.n_bytes(), FLAGS_bloom_size_bytes, FLAGS_bloom_size_bytes * 0.05 );
  ASSERT_GT(FLAGS_n_keys, sizing.expected_count())
    << "Invalid parameters: --n_keys isn't set large enough to fill even "
    << "one bloom filter of the requested --bloom_size_bytes";

  BloomFileWriter bfw(sink, sizing);


  ASSERT_STATUS_OK(bfw.Start());
  AppendBlooms(&bfw);
  ASSERT_STATUS_OK( bfw.Finish() );
}

// Verify that all the entries we put in the bloom file check as
// present, and verify that entries we didn't put in have the
// expected false positive rate.
static void VerifyBloomFile(Env *env, const string &path) {
  BloomFileReader *bfr_ptr;
  ASSERT_STATUS_OK( BloomFileReader::Open(env, path, &bfr_ptr) );
  scoped_ptr<BloomFileReader> bfr(bfr_ptr);

  // Verify all the keys that we inserted probe as present.
  for (int i = 0; i < FLAGS_n_keys; i++) {
    uint32_t i_byteswapped = htonl(i);
    Slice s(reinterpret_cast<char *>(&i_byteswapped), sizeof(i));

    bool present = false;
    ASSERT_STATUS_OK_FAST( bfr->CheckKeyPresent(s, &present) );
    ASSERT_TRUE(present);
  }

  int positive_count = 0;
  // Check that the FP rate for keys we didn't insert is what we expect.
  for (int i = 0; i < FLAGS_n_keys; i++) {
    uint32_t i = random();
    Slice s(reinterpret_cast<char *>(&i), sizeof(i));

    bool present = false;
    ASSERT_STATUS_OK_FAST( bfr->CheckKeyPresent(s, &present) );
    if (present) {
      positive_count++;
    }
  }

  double fp_rate = (double)positive_count / (double)FLAGS_n_keys;
  LOG(INFO) << "fp_rate: " << fp_rate << "(" << positive_count << "/" << FLAGS_n_keys << ")";
  ASSERT_LT(fp_rate, FLAGS_fp_rate + FLAGS_fp_rate * 0.20f)
    << "Should be no more than 1.2x the expected FP rate";
}


TEST(TestBloomFile, TestWriteAndRead) {
  scoped_ptr<Env> env(NewMemEnv(Env::Default()));

  string path("/test-bloomfile");
  ASSERT_NO_FATAL_FAILURE(
    WriteTestBloomFile(env.get(), path));
  VerifyBloomFile(env.get(), path);
}

#ifdef NDEBUG
TEST(TestBloomFile, Benchmark) {
  scoped_ptr<Env> env(NewMemEnv(Env::Default()));

  string path("/test-bloomfile");
  ASSERT_NO_FATAL_FAILURE(
    WriteTestBloomFile(env.get(), path));

  BloomFileReader *bfr_ptr;
  ASSERT_STATUS_OK( BloomFileReader::Open(env.get(), path, &bfr_ptr) );
  scoped_ptr<BloomFileReader> bfr(bfr_ptr);

  LOG_TIMING(INFO, StringPrintf("Running %ld queries", FLAGS_benchmark_queries)) {
    uint64_t count_present = 0;
    for (int i = 0; i < FLAGS_benchmark_queries; i++) {
      Slice s(reinterpret_cast<char *>(&i), sizeof(i));
      bool present;
      CHECK_OK( bfr->CheckKeyPresent(s, &present) ); 
      if (present) count_present++;
    }
  }
}
#endif

} // namespace kudu
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
