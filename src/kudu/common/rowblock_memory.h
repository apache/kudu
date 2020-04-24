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

#include <functional>
#include <vector>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/memory/arena.h"

namespace kudu {

class RowBlockRefCounted;

// Handles the memory allocated alongside a RowBlock for variable-length
// cells.
//
// When scanning rows into a RowBlock, the rows may contain variable-length
// data (eg BINARY columns). In this case, the data cannot be inlined directly
// into the columnar data arrays that are part of the RowBlock and instead need
// to be allocated out of a separate Arena. This class wraps that Arena.
//
// In some cases (eg "plain" or "dictionary" encodings), the underlying blocks may contain
// string data in a non-encoded form. In that case, instead of copying strings, we can
// refer to the strings within those data blocks themselves, and hold a reference to
// the underlying block. This class holds those reference counts as well.
struct RowBlockMemory {
  Arena arena;

  explicit RowBlockMemory(int arena_size = 32 * 1024) : arena(arena_size) {}
  ~RowBlockMemory() { Reset(); }

  void Reset() {
    arena.Reset();
    for (auto& f : to_release_) {
      f();
    }
    to_release_.clear();
  }

  // Retain a reference, typically to a BlockHandle. This is templatized to avoid
  // a circular dependency between kudu/common/ and kudu/cfile/
  template<class T>
  void RetainReference(const scoped_refptr<T>& item) {
    // TODO(todd) if this ever ends up being a hot code path, we could
    // probably optimize by having a small hashset of pointers. If an
    // element is already in the set, we don't need to add a second copy.
    T* raw = item.get();
    raw->AddRef();
    to_release_.emplace_back([=]() { raw->Release(); });
  }

 private:
  std::vector<std::function<void()>> to_release_;
};

}  // namespace kudu
