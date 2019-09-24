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

#include "kudu/util/char_util.h"

#include <string.h>

namespace kudu {

Slice UTF8Truncate(Slice val, size_t max_utf8_length) {
  size_t num_utf8_chars = 0;
  size_t num_bytes = 0;
  auto str = val.data();
  for (auto i = 0; i < val.size(); ++i) {
    num_utf8_chars += (*str++ & 0xc0) != 0x80;
    num_bytes++;
    if (num_utf8_chars > max_utf8_length) {
      num_bytes--;
      num_utf8_chars--;
      break;
    }
  }
  // as num_bytes <= val.size() we can use that to allocate the new slice data
  // and copy the first num_bytes from val.data() to it.
  auto relocated = new uint8_t[num_bytes];
  memcpy(relocated, val.data(), num_bytes);
  return Slice(relocated, num_bytes);
}

} // namespace kudu
