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
#include <string>
#include <thread>
#include <vector>

#include "kudu/util/monotime.h"

namespace kudu {
namespace cluster {
class ExternalMiniCluster;
}  // namespace cluster

// A task that periodically checks master's tablet server's Web UI pages.
class PeriodicWebUIChecker {
 public:
  PeriodicWebUIChecker(const cluster::ExternalMiniCluster& cluster,
                       MonoDelta period,
                       const std::string& tablet_id = "",
                       const std::vector<std::string>& master_pages = {
                           "/dump-entities",
                           "/masters",
                           "/mem-trackers",
                           "/metrics",
                           "/stacks",
                           "/tables",
                           "/tablet-servers",
                           "/threadz",
                           "/threadz?group=all", },
                       const std::vector<std::string>& ts_pages = {
                           "/maintenance-manager",
                           "/mem-trackers",
                           "/metrics",
                           "/scans",
                           "/stacks",
                           "/tablets",
                           "/threadz",
                           "/threadz?group=all",
                       });

  ~PeriodicWebUIChecker();

 private:
  void CheckThread();

  const MonoDelta period_;
  std::atomic<bool> is_running_;
  std::thread checker_;
  std::vector<std::string> urls_;
};

} // namespace kudu
