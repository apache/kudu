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
#include <string>
#include <utility>

namespace kudu {

class KeyRangePB;

// A KeyRange describes the range in Tablet.
class KeyRange {
 public:
  KeyRange(std::string start_key,
           std::string stop_key,
           uint64_t size_bytes)
      : start_key_(std::move(start_key)),
        stop_key_(std::move(stop_key)),
        size_bytes_(size_bytes) {
  }

  // Serializes a KeyRange into a protobuf message.
  void ToPB(KeyRangePB* pb) const;

  const std::string& start_primary_key() const {
    return start_key_;
  }

  const std::string& stop_primary_key() const {
    return stop_key_;
  }

  const uint64_t size_bytes() const {
    return size_bytes_;
  }

 private:
  std::string start_key_;
  std::string stop_key_;
  uint64_t size_bytes_;
};

} // namespace kudu
