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
#pragma once

#include <cstdint>
#include <vector>

namespace kudu {

////////////////////////////////////////////////////////////
// Expose these internal functions for unit testing.
// Do not call them outside of tests!
// See .cc file for docs.
////////////////////////////////////////////////////////////
namespace internal {
void ZeroNullValues(int type_size,
                    int dst_idx,
                    int n_rows,
                    uint8_t* dst_values_buf,
                    uint8_t* non_null_bitmap);

void CopyNonNullBitmap(const uint8_t* non_null_bitmap,
                       const uint8_t* sel_bitmap,
                       int dst_idx,
                       int n_rows,
                       uint8_t* dst_non_null_bitmap);

void CopySelectedRows(const std::vector<uint16_t>& sel_rows,
                      int type_size,
                      const uint8_t* __restrict__ src_buf,
                      uint8_t* __restrict__ dst_buf);


enum class PextMethod {
#ifdef __x86_64__
  kPextInstruction,
  kClmul,
#endif
  kSimple
};
extern PextMethod g_pext_method;

std::vector<PextMethod> GetAvailablePextMethods();

} // namespace internal
} // namespace kudu
