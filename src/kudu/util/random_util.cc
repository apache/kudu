// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/random_util.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>

#include "kudu/util/env.h"
#include "kudu/util/random.h"
#include "kudu/gutil/walltime.h"

namespace kudu {

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

uint32_t GetRandomSeed32() {
  uint32_t seed = static_cast<uint32_t>(GetCurrentTimeMicros());
  seed *= getpid();
  seed *= Env::Default()->gettid();
  return seed;
}

} // namespace kudu
