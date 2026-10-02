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

#include "kudu/tools/location_maintenance.h"

#include <algorithm>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/rebalance/cluster_status.h"

using kudu::cluster_summary::ConsensusConfigType;
using kudu::cluster_summary::TabletSummary;
using std::string;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace tools {

LocationVerdict EvaluateLocation(const string& target_location,
                                 const vector<TabletSummary>& tablets,
                                 const unordered_map<string, string>& loc_by_ts,
                                 const unordered_map<string, int>& rf_by_table) {
  LocationVerdict v;
  v.location = target_location;
  v.safe = true;
  for (const auto& t : tablets) {
    const int* rf = FindOrNull(rf_by_table, t.table_id);
    if (!rf) {
      // Shouldn't happen, but skip the tablet rather than guess.
      LOG(WARNING) << "no replication factor for table id '" << t.table_id
                   << "' referenced by tablet " << t.id << "; skipping";
      continue;
    }

    BlockingTablet bt;
    bt.tablet_id = t.id;
    bt.table_name = t.table_name;
    bt.rf = *rf;
    for (const auto& r : t.replicas) {
      const bool on_target =
          FindWithDefault(loc_by_ts, r.ts_uuid, string()) == target_location;
      if (r.is_voter) {
        ++bt.voters_in_config;
        if (on_target) {
          ++bt.voters_on_target;
        } else if (r.ts_healthy) {
          ++bt.healthy_voters_off_target;
        }
      } else if (on_target) {
        ++bt.non_voters_on_target;
      }
    }
    // A tablet being re-replicated has an extra voter until the old replica is
    // evicted, which raises the majority it needs.
    bt.majority = std::max(*rf, bt.voters_in_config) / 2 + 1;
    if (bt.healthy_voters_off_target < bt.majority) {
      v.blocking.push_back(std::move(bt));
      v.safe = false;
    }
  }
  return v;
}

vector<string> FindTabletsInFlux(const vector<TabletSummary>& tablets) {
  vector<string> in_flux;
  for (const auto& t : tablets) {
    for (const auto& r : t.replicas) {
      // Only replicas on healthy tablet servers report a consensus state.
      if (!r.consensus_state) {
        continue;
      }
      const auto& cstate = *r.consensus_state;
      if (cstate.type == ConsensusConfigType::PENDING ||
          !cstate.replace_uuids.empty() ||
          !cstate.non_voter_uuids.empty()) {
        in_flux.push_back(t.id);
        break;
      }
    }
  }
  return in_flux;
}

} // namespace tools
} // namespace kudu
