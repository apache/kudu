// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include <gtest/gtest.h>

#include "kudu/util/hdr_histogram.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {

static const int kSigDigits = 2;

class HdrHistogramTest : public KuduTest {
};

TEST_F(HdrHistogramTest, SimpleTest) {
  uint64_t highest_val = 10000LU;

  HdrHistogram hist(highest_val, kSigDigits);
  ASSERT_EQ(0, hist.CountInBucketForValue(1));
  hist.Increment(1);
  ASSERT_EQ(1, hist.CountInBucketForValue(1));
  hist.IncrementBy(1, 3);
  ASSERT_EQ(4, hist.CountInBucketForValue(1));
  hist.Increment(10);
  ASSERT_EQ(1, hist.CountInBucketForValue(10));
  hist.Increment(20);
  ASSERT_EQ(1, hist.CountInBucketForValue(20));
  ASSERT_EQ(0, hist.CountInBucketForValue(1000));
  hist.Increment(1000);
  hist.Increment(1001);
  ASSERT_EQ(2, hist.CountInBucketForValue(1000));
}

TEST_F(HdrHistogramTest, TestCoordinatedOmission) {
  uint64_t interval = 1000;
  int loop_iters = 100;
  int64_t normal_value = 10;
  HdrHistogram hist(1000000LU, kSigDigits);
  for (int i = 1; i <= loop_iters; i++) {
    // Simulate a periodic "large value" that would exhibit coordinated
    // omission were this loop to sleep on 'interval'.
    int64_t value = (i % normal_value == 0) ? interval * 10 : normal_value;

    hist.IncrementWithExpectedInterval(value, interval);
  }
  ASSERT_EQ(loop_iters - (loop_iters / normal_value),
            hist.CountInBucketForValue(normal_value));
  for (int i = interval; i <= interval * 10; i += interval) {
    ASSERT_EQ(loop_iters / normal_value, hist.CountInBucketForValue(i));
  }
}

static void load_percentiles(HdrHistogram* hist, uint64_t real_max) {
  hist->IncrementBy(10, 80);
  hist->IncrementBy(100, 10);
  hist->IncrementBy(1000, 5);
  hist->IncrementBy(10000, 3);
  hist->IncrementBy(100000, 1);
  hist->IncrementBy(real_max, 1);
}

static void validate_percentiles(HdrHistogram* hist, uint64_t specified_max, uint64_t real_max) {
  CHECK_EQ(10, hist->MinValue());
  CHECK_EQ(real_max, hist->MaxValue());
  CHECK(568 - 1 < hist->MeanValue());
  CHECK(568 + 1 > hist->MeanValue());
  CHECK_EQ(100, hist->TotalCount());
  CHECK_EQ(10, hist->ValueAtPercentile(80));
  CHECK_EQ(100, hist->ValueAtPercentile(90));
  CHECK_EQ(hist->LowestEquivalentValue(specified_max), hist->ValueAtPercentile(99));
  CHECK_EQ(hist->LowestEquivalentValue(specified_max), hist->ValueAtPercentile(99.99));
  CHECK_EQ(hist->LowestEquivalentValue(specified_max), hist->ValueAtPercentile(100));
}

TEST_F(HdrHistogramTest, PercentileAndCopyTest) {
  uint64_t specified_max = 10000;
  uint64_t real_max = 1000000; // higher than allowed
  HdrHistogram hist(specified_max, kSigDigits);
  load_percentiles(&hist, real_max);
  validate_percentiles(&hist, specified_max, real_max);

  HdrHistogram copy(hist);
  validate_percentiles(&copy, specified_max, real_max);
}

} // namespace kudu
