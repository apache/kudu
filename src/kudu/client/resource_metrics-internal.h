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
#ifndef KUDU_CLIENT_RESOURCE_METRICS_INTERNAL_H
#define KUDU_CLIENT_RESOURCE_METRICS_INTERNAL_H

#include <list>
#include <map>
#include <mutex>
#include <stdint.h>
#include <set>
#include <string>

#include <sparsehash/dense_hash_map>

#include "kudu/gutil/hash/hash.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/locks.h"

namespace kudu {

namespace client {

class ResourceMetrics::Data {
 public:
  Data();
  ~Data();

  // Return a copy of the current counter map.
  std::map<std::string, int64_t> Get() const;

  // Increment the given counter.
  ATTRIBUTE_DEPRECATED("Use StringPiece variant instead")
  void Increment(const std::string& name, int64_t amount);

  // Increment the given counter. The memory referred to by 'name' must
  // remain alive for the lifetime of this ResourceMetrics object.
  void Increment(StringPiece name, int64_t amount);

  // Return metric's current value.
  int64_t GetMetric(const std::string& name) const;

 private:
  mutable simple_spinlock lock_;
  google::dense_hash_map<StringPiece, int64_t, GoodFastHash<StringPiece>> counters_;
  std::set<std::string> owned_strings_;
};

} // namespace client
} // namespace kudu

#endif
