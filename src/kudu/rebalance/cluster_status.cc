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
#include "kudu/rebalance/cluster_status.h"

#include <ostream>

#include <glog/logging.h>

#include "kudu/tablet/tablet.pb.h"  // IWYU pragma: keep

namespace kudu {
namespace cluster_summary {

const char* const HealthCheckResultToString(HealthCheckResult cr) {
  switch (cr) {
    case HealthCheckResult::HEALTHY:
      return "HEALTHY";
    case HealthCheckResult::RECOVERING:
      return "RECOVERING";
    case HealthCheckResult::UNDER_REPLICATED:
      return "UNDER_REPLICATED";
    case HealthCheckResult::CONSENSUS_MISMATCH:
      return "CONSENSUS_MISMATCH";
    case HealthCheckResult::UNAVAILABLE:
      return "UNAVAILABLE";
    default:
      LOG(FATAL) << "Unknown CheckResult";
  }
}

// Return a string representation of 'sh'.
const char* const ServerHealthToString(ServerHealth sh) {
  switch (sh) {
    case ServerHealth::HEALTHY:
      return "HEALTHY";
    case ServerHealth::UNAUTHORIZED:
      return "UNAUTHORIZED";
    case ServerHealth::UNAVAILABLE:
      return "UNAVAILABLE";
    case ServerHealth::WRONG_SERVER_UUID:
      return "WRONG_SERVER_UUID";
    default:
      LOG(FATAL) << "Unknown ServerHealth";
  }
}

// Return a string representation of 'type'.
const char* const ServerTypeToString(ServerType type) {
  switch (type) {
    case ServerType::MASTER:
      return "Master";
    case ServerType::TABLET_SERVER:
      return "Tablet Server";
    default:
      LOG(FATAL) << "Unknown ServerType";
  }
}

} // namespace cluster_summary
} // namespace kudu
