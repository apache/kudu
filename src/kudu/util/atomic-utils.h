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

#include <algorithm>
#include <atomic>

namespace kudu {

// TODO(aserbin): remove this and replace its usage at call sites with
//                std::fetch_max() when switching to C++26
template<typename T>
inline void AtomicStoreMax(std::atomic<T>& val, T new_val) {
  do {
    T old_val = val.load(std::memory_order_relaxed);
    T max_val = std::max(old_val, new_val);
    T prev_val = old_val;
    if (val.compare_exchange_weak(prev_val,
                                  max_val,
                                  std::memory_order_release,
                                  std::memory_order_relaxed)) {
      return;
    }
    old_val = prev_val;
  } while (true);
}

// TODO(aserbin): remove this and replace its usage at call sites with
//                std::fetch_min() when switching to C++26
template<typename T>
inline void AtomicStoreMin(std::atomic<T>& val, T new_val) {
  do {
    T old_val = val.load(std::memory_order_relaxed);
    T min_val = std::min(old_val, new_val);
    T prev_val = old_val;
    if (val.compare_exchange_weak(prev_val,
                                  min_val,
                                  std::memory_order_release,
                                  std::memory_order_relaxed)) {
      return;
    }
    old_val = prev_val;
  } while (true);
}

} // namespace kudu
