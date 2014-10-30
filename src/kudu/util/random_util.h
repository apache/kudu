// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_UTIL_RANDOM_UTIL_H
#define KUDU_UTIL_RANDOM_UTIL_H

#include <cstdlib>
#include <stdint.h>

namespace kudu {

class Random;

// Writes exactly n random bytes to dest using the parameter Random generator.
// Note RandomString() does not null-terminate its strings, though '\0' could
// be written to dest with the same probability as any other byte.
void RandomString(void* dest, size_t n, Random* rng);

// Generate a 32-bit random seed from several sources, including timestamp,
// pid & tid.
uint32_t GetRandomSeed32();

} // namespace kudu

#endif // KUDU_UTIL_RANDOM_UTIL_H
