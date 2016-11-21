// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/cfile/bitshuffle_arch_wrapper.h"

#include <stdint.h>

// Include the bitshuffle header once to get the default (non-AVX2)
// symbols.
#include <bitshuffle.h>

// Include the bitshuffle header again, but this time importing the
// AVX2-compiled symbols by defining some macros.
#undef BITSHUFFLE_H
#define bshuf_compress_lz4_bound bshuf_compress_lz4_bound_avx2
#define bshuf_compress_lz4 bshuf_compress_lz4_avx2
#define bshuf_decompress_lz4 bshuf_decompress_lz4_avx2
#include <bitshuffle.h> // NOLINT(*)
#undef bshuf_compress_lz4_bound
#undef bshuf_compress_lz4
#undef bshuf_decompress_lz4

#include "kudu/gutil/cpu.h"

using base::CPU;

namespace kudu {
namespace bitshuffle {

// On Linux, we dynamically dispatch using the 'ifunc' attribute, which dynamically
// resolves the function implementation on first call.
// ------------------------------------------------------------
#ifndef __APPLE__

// First the actual 'resolver' functions, which pick which implementation to use.
extern "C" {
decltype(&bshuf_compress_lz4_bound) bshuf_compress_lz4_bound_ifunc() {
  return CPU().has_avx2() ? &bshuf_compress_lz4_bound_avx2 : &bshuf_compress_lz4_bound;
}
decltype(&bshuf_compress_lz4) bshuf_compress_lz4_ifunc() {
  return CPU().has_avx2() ? &bshuf_compress_lz4_avx2 : &bshuf_compress_lz4;
}
decltype(&bshuf_decompress_lz4) bshuf_decompress_lz4_ifunc() {
  return CPU().has_avx2() ? &bshuf_decompress_lz4_avx2 : &bshuf_decompress_lz4;
}
} // extern "C"

// Then the ifunc declarations.
__attribute__((ifunc("bshuf_compress_lz4_ifunc")))
int64_t compress_lz4(void* in, void* out, size_t size,
                     size_t elem_size, size_t block_size);

__attribute__((ifunc("bshuf_decompress_lz4_ifunc")))
int64_t decompress_lz4(void* in, void* out, size_t size,
                       size_t elem_size, size_t block_size);

__attribute__((ifunc("bshuf_compress_lz4_bound_ifunc")))
size_t compress_lz4_bound(size_t size, size_t elem_size, size_t block_size);


// On OSX, we don't do the ifunc magic, and instead just pass through to non-AVX
// bitshuffle.
// ------------------------------------------------------------
#else
int64_t compress_lz4(void* in, void* out, size_t size,
                     size_t elem_size, size_t block_size) {
  return bshuf_compress_lz4(in, out, size, elem_size, block_size);
}
int64_t decompress_lz4(void* in, void* out, size_t size,
                       size_t elem_size, size_t block_size) {
  return bshuf_decompress_lz4(in, out, size, elem_size, block_size);
}
size_t compress_lz4_bound(size_t size, size_t elem_size, size_t block_size) {
  return bshuf_compress_lz4_bound(size, elem_size, block_size);
}
#endif // defined(__APPLE__)

} // namespace bitshuffle
} // namespace kudu
