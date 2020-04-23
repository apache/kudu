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

#include "kudu/util/memory/arena.h"

namespace kudu {

// Handles the memory allocated alongside a RowBlock for variable-length
// cells.
//
// When scanning rows into a RowBlock, the rows may contain variable-length
// data (eg BINARY columns). In this case, the data cannot be inlined directly
// into the columnar data arrays that are part of the RowBlock and instead need
// to be allocated out of a separate Arena. This class wraps that Arena.
struct RowBlockMemory {
  Arena arena;

  explicit RowBlockMemory(int arena_size = 32 * 1024) : arena(arena_size) {}
  void Reset() { arena.Reset(); }
};

}  // namespace kudu
