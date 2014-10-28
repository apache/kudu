// Copyright (c) 2014 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/random_util.h"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "kudu/util/random.h"
#include "kudu/util/test_util.h"

namespace kudu {

class RandomUtilTest : public KuduTest {
 protected:
  RandomUtilTest() : rng_(SeedRandom()) {}

  Random rng_;

  static const int kLenMax = 100;
  static const int kNumTrials = 100;
};


// Tests that after certain number of invocations NormalDist(), the
// actual mean of all samples is within the specified standard
// deviation of the target mean.
TEST_F(RandomUtilTest, TestNormalDist) {
  const double kMean = 5.0;
  const double kStdDev = 0.01;
  const int kNumIters = 100000;

  double sum = 0.0;
  for (int i = 0; i < kNumIters; ++i) {
    sum += NormalDist(kMean, kStdDev);
  }

  ASSERT_LE(fabs((sum / static_cast<double>(kNumIters)) - kMean), kStdDev);
}

namespace {

// Checks string defined at start is set to \0 everywhere but [from, to)
void CheckEmpty(char* start, int from, int to, int stop) {
  DCHECK_LE(0, from);
  DCHECK_LE(from, to);
  DCHECK_LE(to, stop);
  for (int j = 0; (j == from ? j = to : j) < stop; ++j) {
    CHECK_EQ(start[j], '\0') << "Index " << j << " not null after defining"
                             << "indices [" << from << "," << to << ") of "
                             << "a nulled string [0," << stop << ").";
  }
}

} // anonymous namespace

// Makes sure that RandomString only writes the specified amount
TEST_F(RandomUtilTest, TestRandomString) {
  char start[kLenMax];

  for (int i = 0; i < kNumTrials; ++i) {
    memset(start, '\0', kLenMax);
    int to = rng_.Uniform(kLenMax + 1);
    int from = rng_.Uniform(to + 1);
    RandomString(start + from, to - from, &rng_);
    CheckEmpty(start, from, to, kLenMax);
  }

  // Corner case
  memset(start, '\0', kLenMax);
  RandomString(start, 0, &rng_);
  CheckEmpty(start, 0, 0, kLenMax);
}

} // namespace kudu
