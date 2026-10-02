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

#include <string>
#include <unordered_map>
#include <vector>

namespace kudu {
namespace cluster_summary {
struct TabletSummary;
}  // namespace cluster_summary

namespace tools {

// A tablet that would lose quorum if the location were taken offline.
struct BlockingTablet {
  std::string tablet_id;
  std::string table_name;
  int rf = 0;
  // More than 'rf' while a replacement replica is being added.
  int voters_in_config = 0;
  int majority = 0;
  // Includes voters on unhealthy tablet servers.
  int voters_on_target = 0;
  int healthy_voters_off_target = 0;
  // For information only: non-voters don't count toward quorum.
  int non_voters_on_target = 0;
};

// The result of checking a single location.
struct LocationVerdict {
  std::string location;
  bool safe = false;
  std::vector<BlockingTablet> blocking;
};

// Checks whether 'target_location' can be taken offline. 'loc_by_ts' maps
// tablet server UUIDs to locations and 'rf_by_table' maps table IDs to
// replication factors. Tablets of tables missing from 'rf_by_table' are
// skipped with a warning.
//
// A tablet blocks the location if its healthy voters outside the location
// don't form a majority of max(RF, voters in config). Unlike the rebalancer's
// placement policy check, this looks at tablets regardless of their health,
// since an under-replicated tablet is the one most at risk.
LocationVerdict EvaluateLocation(
    const std::string& target_location,
    const std::vector<cluster_summary::TabletSummary>& tablets,
    const std::unordered_map<std::string, std::string>& loc_by_ts,
    const std::unordered_map<std::string, int>& rf_by_table);

// Returns the IDs of tablets with a Raft config change in progress: a pending
// config, a replica marked for replacement, or a non-voter. Their replicas are
// likely to move soon.
std::vector<std::string> FindTabletsInFlux(
    const std::vector<cluster_summary::TabletSummary>& tablets);

} // namespace tools
} // namespace kudu
