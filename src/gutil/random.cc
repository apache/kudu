// The implementation is based on the Mersenne Twister available at:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/emt.html .
//
#include "gutil/random.h"

#include <sys/time.h>

#include <string.h>
#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;  // For max.
#include <string>
using std::string;

#include <glog/logging.h>
#include "gutil/logging-inl.h"
#include "gutil/port.h"
#include "gutil/scoped_ptr.h"
#include "gutil/walltime.h"

// The golden values used in MT
static const int kMT_M      = 397;
static const uint32 kUMask  = 0x80000000;
static const uint32 kLMask  = 0x7fffffff;
static const uint32 kMatrix[] =  { 0x0, 0x9908b0df };

const int MTRandom::kMTNumWords;

// Utility method to generate a weak seed. Return value is the
// number of bytes written to buffer. (Always less than or equal to length.)
static int WeakSeed(uint8* buffer, int bufferlen) {
  int offset = 0;
  char * seed_buffer = reinterpret_cast<char*>(buffer);
  // TID. (Probably only 16 bits)
  if (bufferlen >= offset + 2) {
    UNALIGNED_STORE16(seed_buffer + offset, getpid());
    offset += 2;
  }

  // CycleClock
  if (bufferlen >= offset + 8) {
    UNALIGNED_STORE64(seed_buffer + offset, CycleClock::Now());
    offset += 8;
  }

  // Time of day.
  if (bufferlen >= offset + 4) {
    struct timeval start_time;
    gettimeofday(&start_time, NULL);
    UNALIGNED_STORE32(seed_buffer + offset, start_time.tv_usec);
    offset += 4;
    if (bufferlen >= offset + 4) {
      UNALIGNED_STORE32(seed_buffer + offset, start_time.tv_sec);
      offset += 4;
    }
  }

  // Get the hostname.
  if (bufferlen > offset &&
      gethostname(seed_buffer + offset, bufferlen - offset) == 0) {
    offset += strlen(seed_buffer + offset);
  }
  return offset;
}

// This implements step 2 and the right half of step 3.
static inline uint32 twist(uint32 a, uint32 b) {
  // y <- (x[i] & U) | (x[i+1] & L)
  // y1 <- XOR (y>>1)
  // return y1
  return (((a & 0x80000000) | (b & 0x7fffffff)) >> 1) ^
         kMatrix[(b & 0x01)];
}

static inline uint32 mix30(uint32 a) {
  // Mix the high bits back in with the low bits.
  return a ^ (a >> 30);
}

// Implements all of step 2 and step 3.
void MTRandom::Cycle() {
  // for i in 0 .. n
  // x[i] <- x[(i + m) % n] ^ twist(x[i], x[(i+1) % n])
  // Use two for loops + residual to avoid the mod operation. The second loop
  // uses a negative index (BACK) to reference the beginning of the array.
  const int BACK = -(kMTNumWords - kMT_M);
  uint32* mt = context_.buffer;
  for (; mt < context_.buffer + (kMTNumWords - kMT_M); mt++) {
    mt[0] = mt[kMT_M] ^ twist(mt[0], mt[1]);
  }
  for (; mt < context_.buffer + (kMTNumWords - 1); mt++) {
    mt[0] = mt[BACK] ^ twist(mt[0], mt[1]);
  }
  mt[0] = mt[BACK] ^ twist(mt[0], context_.buffer[0]);
  context_.randcnt = 0;
}

// Raw copy into the mt array.
void MTRandom::InitRaw(const uint32* seed, int num_words) {
  CHECK_EQ(num_words, kMTNumWords)
    << ": InitRaw num_words mismatch.";
  memcpy(context_.buffer, seed, kMTNumWords * sizeof(*seed));
  context_.randcnt = kMTNumWords;
}

void MTRandom::InitSeed(uint32 seed) {
  // The original version of initialize is based on Knuth, Vol 2., 3rd ed.
  // p. 102-106. Linear congruential generator:  x1 = ( x0 * A ) % M.
  // A = 69069, M = 2^32
  //
  // The second variant of initialize uses a modified linear congruential
  // generater, which is supposed to more effectively distribute the MSB
  // of the seed across the key space.
  // x[i] = ( mix30(x[i-1]) * A + i) % M
  // M = 2^32, A = 1812433253
  //
  // We use a variant of the second, with the parameters:
  // M = 2^32, A = 1664525
  context_.pool = 0;
  uint32* mt = context_.buffer;
  mt[0] = seed;
  for (int i = 1; i < kMTNumWords; ++i) {
    mt[i] = 1664525 * mix30(mt[i-1]) + i;
  }
  context_.randcnt = kMTNumWords;
}

// Array initialization is identical to the Matsumoto version.
void MTRandom::InitArray(const uint32* seed, const int seed_length) {
  CHECK_GT(seed_length, 0);
  context_.pool = 0;
  uint32* mt = context_.buffer;

  // Initial linear congruential generator, using parameters
  // M = 2^32, A = 1812433253UL
  mt[0] = 19650218UL;
  for (int k = 1; k < kMTNumWords; ++k) {
    mt[k] = 1812433253UL * mix30(mt[k-1]) + k;
  }

  // Mix and incorporate seed array.
  int i = 1;
  for (int j = 0, k = max(kMTNumWords, seed_length) ; k > 0; k--) {
    mt[i] = (mt[i] ^ (mix30(mt[i-1]) * 1664525UL)) + seed[j] + j;
    j = (j + 1) % seed_length;
    if (++i >= kMTNumWords) {
      mt[0] = mt[kMTNumWords - 1];
      i = 1;
    }
  }
  for (int k = kMTNumWords - 1; k > 0; k--) {
    mt[i] = (mt[i] ^ (mix30(mt[i-1]) * 1566083941UL)) - i;
    if (++i >= kMTNumWords) {
      mt[0] = mt[kMTNumWords - 1];
      i = 1;
    }
  }

  // Ensures that the state array is non-zero.
  mt[0] = 0x80000000UL;

  // By setting randcnt to kMTNumWords, Cycle() will be called
  // the first time a number is required.
  context_.randcnt = kMTNumWords;
}

// Peel off bytes one at a time from the pool.  If the
// byte pool is empty, fetch another uint32.
uint8  MTRandom::Rand8() {
  if (context_.poolsize == 0) {
    context_.pool = MTRandom::Rand32();
    context_.poolsize = 3;
  } else {
    context_.pool >>= 8;
    --context_.poolsize;
  }
  return context_.pool & 0x000000ff;
}

uint16 MTRandom::Rand16() {
  uint16 rv = MTRandom::Rand8();
  return (rv << 8) | MTRandom::Rand8();
}

uint32 MTRandom::Rand32() {
  if (context_.randcnt >= kMTNumWords) {
    Cycle();
  }
  // implements step 5.
  uint32 y = context_.buffer[context_.randcnt++];
  y ^= (y >> 11);
  y ^= (y << 7)  & 0x9d2c5680;
  y ^= (y << 15) & 0xefc60000;
  y ^= (y >> 18);
  return y;
}

uint64 MTRandom::Rand64() {
  uint64 rv = MTRandom::Rand32();
  return (rv << 32) | MTRandom::Rand32();
}

// -------------------------------------------------------------------
//
// Everything below here is framework code.

MTRandom::MTRandom(uint32 seed) {
  Reset(seed);
}

// Seed MTRandom using an array of uint32.
MTRandom::MTRandom(const uint32* seed, int num_words) {
  Reset(seed, num_words);
}

MTRandom::MTRandom() {
  memset(&context_, 0, sizeof(MTContext));
  uint32 buffer[32];
  memset(buffer, 0, sizeof(buffer));
  int len = WeakSeed(reinterpret_cast<uint8*>(buffer),
                     sizeof(buffer));
  InitArray(buffer, (len + sizeof(uint32) - 1)/ sizeof(uint32));
}

// Scrub the memory and delete.
MTRandom::~MTRandom() {
  memset(&context_, 0, sizeof(MTContext));
}

MTRandom* MTRandom::Clone() const {
  MTRandom *twin = new MTRandom(0);
  memcpy(&(twin->context_), &context_, sizeof(MTContext));
  return twin;
}

void MTRandom::Reset(uint32 seed) {
  memset(&context_, 0, sizeof(MTContext));
  InitSeed(seed);
}

void MTRandom::Reset(const uint32* seed, int num_words) {
  CHECK_EQ(num_words, kMTNumWords)
    << ": MTRandom must be initialized with a buffer of size " << kMTNumWords;

  memset(&context_, 0, sizeof(MTContext));
  InitRaw(seed, num_words);
}
