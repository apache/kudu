// ZP7 (Zach's Peppy Parallel-Prefix-Popcountin' PEXT/PDEP Polyfill)
//
// Copyright (c) 2020 Zach Wegner
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Based on https://github.com/zwegner/zp7 as of
// a1ed4e5ace07f7d69cb50af1cbd37df4fa3d87af
//
// This is imported rather than included via thirdparty since the upstream
// project has no header file. It has been modified as follows:
//
// - remove 'pdep' implementations since we only need 'pext'.
// - separate the clmul and non-accelerated variant into separate
//   functions so they can be switched at runtime.
// - put inside a kudu namespace
// - disable UBSAN for some undefined integer casts

#include "kudu/common/zp7.h"

#include "kudu/gutil/port.h"

#ifdef __x86_64__
#include <emmintrin.h>
#elif defined(__aarch64__)
#include "kudu/util/sse2neon.h"
#endif

#ifndef __aarch64__
#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ < 5
#define USE_INLINE_ASM_CLMUL
#else
#include <wmmintrin.h>
#endif
#endif

#define N_BITS      (6)

namespace kudu {

typedef struct {
  uint64_t mask;
  uint64_t ppp_bit[N_BITS];
} zp7_masks_64_t;

// If we don't have access to the CLMUL instruction, emulate it with
// shifts and XORs
static inline uint64_t prefix_sum(uint64_t x) {
  for (int i = 0; i < N_BITS; i++)
    x ^= x << (1 << i);
  return x;
}

// GCC <5 doesn't properly handle the _pext_u64 intrinsic inside
// a function with a specified target attribute. So, use inline
// assembly instead.
#ifdef USE_INLINE_ASM_CLMUL
static inline __m128i asm_mm_clmulepi64_si128(__m128i a, __m128i b) {
  asm ("pclmulqdq $0, %1, %0"
       : "+x" (a)
       : "xm" (b));
  return a;
}
#define CLMUL asm_mm_clmulepi64_si128
#else
#define CLMUL(a, b) (_mm_clmulepi64_si128(a, b, 0))
#endif


// Parallel-prefix-popcount. This is used by both the PEXT/PDEP polyfills.
// It can also be called separately and cached, if the mask values will be used
// more than once (these can be shared across PEXT and PDEP calls if they use
// the same masks).
//
// This variant depends on the CLMUL instruction.
#ifndef __aarch64__
__attribute__((target("pclmul")))
#endif // __aarch64__
ATTRIBUTE_NO_SANITIZE_INTEGER
static zp7_masks_64_t zp7_ppp_64_clmul(uint64_t mask) {
  zp7_masks_64_t r;
  r.mask = mask;

  // Count *unset* bits
  mask = ~mask;

  // Move the mask and -2 to XMM registers for CLMUL
  __m128i m = _mm_cvtsi64_si128(mask);
  __m128i neg_2 = _mm_cvtsi64_si128(-2LL);
  for (int i = 0; i < N_BITS - 1; i++) {
    // Do a 1-bit parallel prefix popcount, shifted left by 1,
    // in one carry-less multiply by -2.
    __m128i bit = CLMUL(m, neg_2);
    r.ppp_bit[i] = _mm_cvtsi128_si64(bit);

    // Get the carry bit of the 1-bit parallel prefix popcount. On
    // the next iteration, we will sum this bit to get the next mask
    m = _mm_and_si128(m, bit);
  }
  // For the last iteration, we can use a regular multiply by -2 instead of a
  // carry-less one (or rather, a strength reduction of that, with
  // neg/add/etc), since there can't be any carries anyways. That is because
  // the last value of m (which has one bit set for every 32nd unset mask bit)
  // has at most two bits set in it, when mask is zero and thus there are 64
  // bits set in ~mask. If two bits are set, one of them is the top bit, which
  // gets shifted out, since we're counting bits below each mask bit.
  r.ppp_bit[N_BITS - 1] = -_mm_cvtsi128_si64(m) << 1;

  return r;
}

// Implementation that doesn't depend on CLMUL
ATTRIBUTE_NO_SANITIZE_INTEGER
static zp7_masks_64_t zp7_ppp_64_simple(uint64_t mask) {
  zp7_masks_64_t r;
  r.mask = mask;

  // Count *unset* bits
  mask = ~mask;
  for (int i = 0; i < N_BITS - 1; i++) {
    // Do a 1-bit parallel prefix popcount, shifted left by 1
    uint64_t bit = prefix_sum(mask << 1);
    r.ppp_bit[i] = bit;

    // Get the carry bit of the 1-bit parallel prefix popcount. On
    // the next iteration, we will sum this bit to get the next mask
    mask &= bit;
  }
  // The last iteration won't carry, so just use neg/shift. See the CLMUL
  // case above for justification.
  r.ppp_bit[N_BITS - 1] = -mask << 1;
  return r;
}

// PEXT

static uint64_t zp7_pext_pre_64(uint64_t a, const zp7_masks_64_t *masks) {
  // Mask only the bits that are set in the input mask. Otherwise they collide
  // with input bits and screw everything up
  a &= masks->mask;

  // For each bit in the PPP, shift right only those bits that are set in
  // that bit's mask
  for (int i = 0; i < N_BITS; i++) {
    uint64_t shift = 1 << i;
    uint64_t bit = masks->ppp_bit[i];
    // Shift only the input bits that are set in
    a = (a & ~bit) | ((a & bit) >> shift);
  }
  return a;
}

uint64_t zp7_pext_64_simple(uint64_t a, uint64_t mask) {
  zp7_masks_64_t masks = zp7_ppp_64_simple(mask);
  return zp7_pext_pre_64(a, &masks);
}

uint64_t zp7_pext_64_clmul(uint64_t a, uint64_t mask) {
  zp7_masks_64_t masks = zp7_ppp_64_clmul(mask);
  return zp7_pext_pre_64(a, &masks);
}

} // namespace kudu
