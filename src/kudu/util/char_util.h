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

#include <cstddef>
#include <cstdint>
#include <limits>

#include "kudu/util/slice.h"

namespace kudu {

  // Minimum and maxium length for VARCHAR [1,65535]
  constexpr uint16_t kMinVarcharLength = 1;
  constexpr uint16_t kMaxVarcharLength = std::numeric_limits<uint16_t>::max();

  // Copy and truncate a slice. The Slice returned owns its memory.
  //
  // max_utf8_length is the number of UTF-8 characters/symbols (not bytes) to
  // truncate to.
  //
  // The method doesn't validate the string is well-formed UTF-8.
  Slice UTF8Truncate(Slice val, size_t max_utf8_length);
} // namespace kudu
