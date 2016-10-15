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

#include "kudu/clock/system_unsync_time.h"

#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/walltime.h"

namespace kudu {
namespace clock {

Status SystemUnsyncTime::Init() {
  LOG(WARNING) << "NTP support is disabled. Clock error bounds will not "
               << "be accurate. This configuration is not suitable for "
               << "distributed clusters.";
  return Status::OK();
}

Status SystemUnsyncTime::WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) {
  *now_usec = GetCurrentTimeMicros();
  *error_usec = 0;
  return Status::OK();
}

} // namespace clock
} // namespace kudu
