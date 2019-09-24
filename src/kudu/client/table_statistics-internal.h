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

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace client {

using std::string;
using strings::Substitute;

class KuduTableStatistics::Data {
 public:
  Data(uint64_t on_disk_size, uint64_t live_row_count)
      : on_disk_size_(on_disk_size),
        live_row_count_(live_row_count) {
  }

  ~Data() {
  }

  string ToString() const {
    string display_string = "";
    display_string += Substitute("on disk size: $0\n", on_disk_size_);
    display_string += Substitute("live row count: $0\n", live_row_count_);
    return display_string;
  }

  const uint64_t on_disk_size_;
  const uint64_t live_row_count_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu
