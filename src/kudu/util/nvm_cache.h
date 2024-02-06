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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include <glog/logging.h>

#include "kudu/util/cache.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace util {

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  Cache::EvictionCallback* eviction_callback;
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  uint32_t key_length;
  uint32_t val_length;
  std::atomic<int32_t> refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  uint8_t* kv_data;

  Slice key() const {
    return Slice(kv_data, key_length);
  }

  Slice value() const {
    return Slice(&kv_data[key_length], val_length);
  }

  uint8_t* mutable_val_ptr() const {
    return &kv_data[key_length];
  }
};
} // namespace util

// Convenience macro for invoking CanUseNVMCacheForTests.
#define RETURN_IF_NO_NVM_CACHE(memory_type) do { \
  if ((memory_type) == Cache::MemoryType::NVM && !CanUseNVMCacheForTests()) { \
    LOG(WARNING) << "test is skipped; NVM cache cannot be created"; \
    return; \
  } \
} while (0)

// Returns true if an NVM cache can be created on this machine; false otherwise.
//
// Only intended for use in unit tests.
bool CanUseNVMCacheForTests();

// Create a new LRU cache with a fixed size capacity. This implementation
// of Cache uses the least-recently-used eviction policy and stored in NVM.
template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::NVM>(size_t capacity, const std::string& id);

}  // namespace kudu
