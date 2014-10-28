// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_UTIL_RANDOM_UTIL_H
#define KUDU_UTIL_RANDOM_UTIL_H

#include <cstdlib>

namespace kudu {

class Random;

// Creates a normal distribution variable using the
// Box-Muller transform.
double NormalDist(double mean, double std_dev);

// Writes exactly n random bytes to dest using the parameter Random generator.
// Note RandomString() does not null-terminate its strings, though '\0' could
// be written to dest with the same probability as any other byte.
void RandomString(void* dest, size_t n, Random* rng);

} // namespace kudu

#endif // KUDU_UTIL_RANDOM_UTIL_H
