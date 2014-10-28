// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/random_util.h"

#include <cmath>
#include <cstdlib>
#include <cstring>

#include "kudu/util/random.h"

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

void RandomString(void* dest, size_t n, Random* rng) {
  size_t i = 0;
  uint32_t random = rng->Next();
  char* cdest = static_cast<char*>(dest);
  static const size_t sz = sizeof(random);
  if (n >= sz) {
    for (i = 0; i <= n - sz; i += sz) {
      memcpy(&cdest[i], &random, sizeof(random));
      random = rng->Next();
    }
  }
  memcpy(cdest + i, &random, n - i);
}

} // namespace kudu
