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

#include "kudu/util/threadlocal.h"

#include <boost/optional/optional.hpp>
#include <array>
#include <memory>
#include <utility>

namespace kudu {

// A small thread-local cache for arbitrary objects.
//
// This can be used as a contention-free "lookaside" type cache for frequently-accessed
// objects to avoid having to go to a less-efficient centralized cache.
//
// 'Key' must be copyable, and comparable using operator==().
// 'T' has no particular requirements.
template<class Key, class T>
class ThreadLocalCache {
 public:
  // The number of entries in the cache.
  // NOTE: this should always be a power of two for good performance, so that the
  // compiler can optimize the modulo operations into bit-mask operations.
  static constexpr int kItemCapacity = 4;

  // Look up a key in the cache. Returns either the existing entry with this key,
  // or nullptr if no entry matched.
  T* Lookup(const Key& key) {
    // Our cache is so small that a linear search is likely to be more efficient than
    // any kind of actual hashing. We always start the search at wherever we most
    // recently found a hit.
    for (int i = 0; i < kItemCapacity; i++) {
      int idx = (last_hit_ + i) % kItemCapacity;
      auto& p = cache_[idx];
      if (p.first == key) {
        last_hit_ = idx;
        return p.second.get_ptr();
      }
    }
    return nullptr;
  }

  // Insert a new entry into the cache. If the cache is full (as it usually is in the
  // steady state), this replaces one of the existing entries. The 'args' are forwarded
  // to T's constructor.
  //
  // NOTE: entries returned by a previous call to Lookup() may possibly be invalidated
  // by this function.
  template<typename ... Args>
  T* EmplaceNew(const Key& key, Args&&... args) {
    auto& p = cache_[next_slot_++ % kItemCapacity];
    p.second.emplace(std::forward<Args>(args)...);
    p.first = key;
    return p.second.get_ptr();
  }

  // Get the the cache instance for this thread, creating it if it has not yet been
  // created.
  //
  // The instance is automatically deleted and any cached items destructed when the
  // thread exits.
  static ThreadLocalCache* GetInstance() {
    INIT_STATIC_THREAD_LOCAL(ThreadLocalCache, tl_instance_);
    return tl_instance_;
  }

 private:
  using EntryPair = std::pair<Key, boost::optional<T>>;
  std::array<EntryPair, kItemCapacity> cache_;

  // The next slot that we will write into. We always modulo this by the capacity
  // before use.
  uint8_t next_slot_ = 0;
  // The slot where we last got a cache hit, so we can start our search at the same
  // spot, optimizing for the case of repeated lookups of the same hot element.
  uint8_t last_hit_ = 0;

  static_assert(kItemCapacity <= 1 << (sizeof(next_slot_) * 8),
                "next_slot_ must be large enough for capacity");
  static_assert(kItemCapacity <= 1 << (sizeof(last_hit_) * 8),
                "last_hit_ must be large enough for capacity");

  DECLARE_STATIC_THREAD_LOCAL(ThreadLocalCache, tl_instance_);
};

// Define the thread-local storage for the ThreadLocalCache template.
// We can't use DEFINE_STATIC_THREAD_LOCAL here because the commas in the
// template arguments confuse the C preprocessor.
template<class K, class T>
__thread ThreadLocalCache<K,T>* ThreadLocalCache<K,T>::tl_instance_;

} // namespace kudu
