// Copyright (c) 2014, Cloudera, inc.

#include "util/random_util.h"

#include <cmath>
#include <cstdlib>

namespace kudu {

namespace {
const double kTwoPi = 6.283185307179586476925286;
} // anonymous namespace

// Adapted from WebRTC source code
// (webrtc/trunk/modules/video_coding/main/test/test_util.cc) and
// http://en.wikipedia.org/wiki/Box%E2%80%93Muller_transform
double NormalDist(double mean, double std_dev) {
  double uniform1 = (rand() + 1.0) / (RAND_MAX + 1.0);
  double uniform2 = (rand() + 1.0) / (RAND_MAX + 1.0);

  return (mean + std_dev * sqrt(-2 * log(uniform1)) * cos(kTwoPi * uniform2));
}

} // namespace kudu
