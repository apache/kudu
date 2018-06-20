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

#include "kudu/common/key_range.h"

#include "kudu/common/common.pb.h"

namespace kudu {

void KeyRange::ToPB(KeyRangePB* pb) const {
  if (!start_key_.empty()) {
    pb->set_start_primary_key(start_key_);
  }
  if (!stop_key_.empty()) {
    pb->set_stop_primary_key(stop_key_);
  }
  pb->set_size_bytes_estimates(size_bytes_);
}

} // namespace kudu
