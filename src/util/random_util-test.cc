// Copyright (c) 2014 Cloudera Inc.

#include "util/random_util.h"

#include <cmath>

#include "util/test_util.h"

namespace kudu {

class RandomUtilTest : public KuduTest {
 protected:

  virtual void SetUp() {
    KuduTest::SetUp();
    SeedRandom();
  }
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

  ASSERT_LE(abs((sum / static_cast<double>(kNumIters)) - kMean), kStdDev);
}

} // namespace kudu
