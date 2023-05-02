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

#include "kudu/master/placement_policy.h"

#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

using std::multimap;
using std::optional;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

namespace {

typedef std::pair<std::shared_ptr<TSDescriptor>, vector<double>> ts_stats;

double GetTSLoad(TSDescriptor* desc, const optional<string>& dimension) {
  // TODO (oclarms): get the number of times this tablet server has recently been
  //  selected to create a tablet replica by dimension.
  return desc->RecentReplicaCreations() + desc->num_live_replicas(dimension);
}

double GetTSRangeLoad(TSDescriptor* desc, const string& range_start_key, const string& table_id) {
  return desc->RecentReplicaCreationsByRange(range_start_key, table_id)
  + desc->num_live_replicas_by_range(range_start_key, table_id);
}

double GetTSTableLoad(TSDescriptor* desc, const string& table_id) {
  return desc->RecentReplicaCreationsByTable(table_id) + desc->num_live_replicas_by_table(table_id);
}

// Iterates through `src_replicas_stats` and returns set of tablet servers that have the least
// replicas of a particular stat (range, table, total) that is determined by 'index'.
vector<ts_stats> TabletServerTieBreaker(const vector<ts_stats>& src_replicas_stats,
                                        int index) {
  double min = std::numeric_limits<double>::max();
  vector<ts_stats> result;
  for (const auto& stats : src_replicas_stats) {
    if (stats.second[index] < min) {
      min = stats.second[index];
      result.clear();
      result.emplace_back(stats);
    } else if (stats.second[index] == min) {
      result.emplace_back(stats);
    }
  }
  return result;
}

// Given exactly two choices in 'two_choices', pick the better tablet server on
// which to place a tablet replica. Ties are broken using 'rng'.
shared_ptr<TSDescriptor> PickBetterTabletServer(
    const TSDescriptorVector& two_choices,
    const optional<string>& dimension,
    ThreadSafeRandom* rng) {
  CHECK_EQ(2, two_choices.size());

  const auto& a = two_choices[0];
  const auto& b = two_choices[1];

  // When creating replicas, we consider two aspects of load:
  //   (1) how many tablet replicas are already on the server (if dimension is
  //       not std::nullopt, only return the number of tablet replicas in the dimension),
  // and
  //   (2) how often we've chosen this server recently.
  //
  // The first factor will attempt to put more replicas on servers that
  // are under-loaded (eg because they have newly joined an existing cluster, or have
  // been reformatted and re-joined).
  //
  // The second factor will ensure that we take into account the recent selection
  // decisions even if those replicas are still in the process of being created (and thus
  // not yet reported by the server). This is important because, while creating a table,
  // we batch the selection process before sending any creation commands to the
  // servers themselves.
  //
  // TODO(wdberkeley): in the future we may want to factor in other items such
  // as available disk space, actual request load, etc.
  double load_a = GetTSLoad(a.get(), dimension);
  double load_b = GetTSLoad(b.get(), dimension);
  if (load_a < load_b) {
    return a;
  }
  if (load_b < load_a) {
    return b;
  }
  // If the load is the same, we can just pick randomly.
  return two_choices[rng->Uniform(2)];
}

// Given a set of tablet servers in 'ts_choices', pick the best tablet server on which to place a
// tablet replica based on the existing number of replicas per the given range. The tiebreaker
// is the number of replicas per table, then number of replicas overall. If still tied, use rng.
shared_ptr<TSDescriptor> PickTabletServer(const TSDescriptorVector& ts_choices,
                                          const string& range_key_start,
                                          const string& table_id,
                                          const optional<string>& dimension,
                                          ThreadSafeRandom* rng) {
  CHECK_GE(ts_choices.size(), 2);
  vector<ts_stats> replicas_stats;
  // Find the number of replicas per the given range, the given table,
  // and total replicas for each tablet server.
  for (const auto& ts : ts_choices) {
    auto* ts_desc = ts.get();
    auto tablets_by_range = GetTSRangeLoad(ts_desc, range_key_start, table_id);
    auto tablets_by_table = GetTSTableLoad(ts_desc, table_id);
    auto tablets = GetTSLoad(ts_desc, dimension);
    const vector<double> tablet_count = {tablets_by_range, tablets_by_table, tablets};
    replicas_stats.emplace_back(ts, tablet_count);
  }
  // Given set of tablet servers with stats about replicas per given range, table,
  // and total replicas, find tablet servers with the least replicas per given range.
  vector<ts_stats> replicas_by_range = TabletServerTieBreaker(replicas_stats, 0);
  CHECK(!replicas_by_range.empty());
  if (replicas_by_range.size() == 1) {
    return replicas_by_range[0].first;
  }
  // Given set of tablet servers with the least replicas per given range,
  // find tablet servers with the least replicas per given table.
  vector<ts_stats> replicas_by_table = TabletServerTieBreaker(replicas_by_range, 1);
  CHECK(!replicas_by_table.empty());
  if (replicas_by_table.size() == 1) {
    return replicas_by_table[0].first;
  }
  // Given set of tablet servers with the least replicas per range and table,
  // find tablet servers with the least replicas overall.
  vector<ts_stats> replicas_total = TabletServerTieBreaker(replicas_by_table, 2);
  CHECK(!replicas_total.empty());
  if (replicas_total.size() == 1) {
    return replicas_total[0].first;
  }
  // No more tiebreakers, randomly select a tablet server.
  return replicas_total[rng->Uniform(replicas_total.size())].first;
}

} // anonymous namespace

PlacementPolicy::PlacementPolicy(TSDescriptorVector descs,
                                 ThreadSafeRandom* rng)
    : ts_num_(descs.size()),
      rng_(rng) {
  CHECK(rng_);
  for (auto& desc : descs) {
    EmplaceOrDie(&known_ts_ids_, desc->permanent_uuid());
    string location = desc->location() ? *desc->location() : "";
    LookupOrEmplace(&ltd_, std::move(location),
                    TSDescriptorVector()).emplace_back(std::move(desc));
  }
}

Status PlacementPolicy::PlaceTabletReplicas(int nreplicas,
                                            const optional<string>& dimension,
                                            const optional<string>& range_key_start,
                                            const optional<string>& table_id,
                                            TSDescriptorVector* ts_descs) const {
  DCHECK(ts_descs);

  // Two-level approach for placing replicas:
  //   1) select locations to place tablet replicas
  //   2) in each location, select tablet servers to place replicas
  ReplicaLocationsInfo locations_info;
  RETURN_NOT_OK(SelectReplicaLocations(nreplicas, &locations_info));
  for (const auto& elem : locations_info) {
    const auto& loc = elem.first;
    const auto loc_nreplicas = elem.second;
    const auto& ts_descriptors = FindOrDie(ltd_, loc);
    RETURN_NOT_OK(SelectReplicas(
        ts_descriptors, loc_nreplicas, dimension, range_key_start, table_id, ts_descs));
  }
  return Status::OK();
}

Status PlacementPolicy::PlaceExtraTabletReplica(
    TSDescriptorVector existing,
    const optional<string>& dimension,
    const optional<string>& range_key_start,
    const optional<string>& table_id,
    shared_ptr<TSDescriptor>* ts_desc) const {
  DCHECK(ts_desc);

  // Convert input vector into a set, filtering out replicas that are located
  // at unknown (i.e. unavailable) tablet servers.
  int total_replicas_num = 0;
  int unavailable_replicas_num = 0;
  set<shared_ptr<TSDescriptor>> existing_set;
  for (auto& ts : existing) {
    ++total_replicas_num;
    if (!ContainsKey(known_ts_ids_, ts->permanent_uuid())) {
      ++unavailable_replicas_num;
      continue;
    }
    EmplaceOrDie(&existing_set, std::move(ts));
  }
  VLOG(1) << Substitute("$0 out of $1 existing replicas are unavailable",
                        unavailable_replicas_num, total_replicas_num);

  ReplicaLocationsInfo location_info;
  for (const auto& desc : existing_set) {
    // It's OK to use an empty string in the meaning of 'no location'
    // (instead of e.g. std::nullopt) because valid location strings begin with
    // '/' and therefore are nonempty.
    string location = desc->location() ? *desc->location() : "";
    ++LookupOrEmplace(&location_info, std::move(location), 0);
  }

  string location;
  RETURN_NOT_OK_PREPEND(
      SelectLocation(total_replicas_num, location_info, &location),
      "could not select location for extra replica");
  const auto* location_ts_descs_ptr = FindOrNull(ltd_, location);
  if (!location_ts_descs_ptr) {
    return Status::IllegalState(
        Substitute("'$0': no info on tablet servers at location", location));
  }
  auto replica = SelectReplica(
      *location_ts_descs_ptr, dimension, range_key_start, table_id, existing_set);
  if (!replica) {
    return Status::NotFound("could not find tablet server for extra replica");
  }
  *ts_desc = std::move(replica);

  return Status::OK();
}

double PlacementPolicy::GetLocationLoad(
    const string& location,
    const ReplicaLocationsInfo& locations_info) const {
  // Get information on the distribution of already existing tablet replicas
  // among tablet servers in the specified location.
  const auto& ts_descriptors = FindOrDie(ltd_, location);
  CHECK(!ts_descriptors.empty());
  // Count the number of already existing replicas at the specified location.
  auto num_live_replicas = accumulate(
        ts_descriptors.begin(), ts_descriptors.end(), 0,
        [](int val, const shared_ptr<TSDescriptor>& desc) {
          return val + desc->num_live_replicas();
        });
  // Add the number of to-be-replicas slated for the placement at the specified
  // location.
  const auto* location_rep_num_ptr = FindOrNull(locations_info, location);
  if (location_rep_num_ptr) {
    num_live_replicas += *location_rep_num_ptr;
  }
  return static_cast<double>(num_live_replicas) / ts_descriptors.size();
}

Status PlacementPolicy::SelectReplicaLocations(
    int nreplicas,
    ReplicaLocationsInfo* locations_info) const {
  DCHECK(locations_info);

  ReplicaLocationsInfo result_info;
  auto placed_replicas_num = 0;
  while (placed_replicas_num < nreplicas) {
    // Check if any location is available. If not, that's the case when
    // placing an additional replica in any of the existing locations would
    // violate the 'one tablet replica per tablet server' policy.
    string location;
    RETURN_NOT_OK_PREPEND(
        SelectLocation(nreplicas, result_info, &location),
        Substitute("could not find next location after placing $0 out of $1 "
                   "tablet replicas", placed_replicas_num, nreplicas));
    ++result_info[location];
    if (++placed_replicas_num == nreplicas) {
      // Placed the required number of replicas.
      break;
    }
  }

  *locations_info = std::move(result_info);
  return Status::OK();
}

Status PlacementPolicy::SelectReplicas(const TSDescriptorVector& source_ts_descs,
                                       int nreplicas,
                                       const optional<string>& dimension,
                                       const optional<string>& range_key_start,
                                       const optional<string>& table_id,
                                       TSDescriptorVector* result_ts_descs) const {
  if (nreplicas > source_ts_descs.size()) {
    return Status::InvalidArgument(
        Substitute("too few to choose from: $0 total, $1 requested",
                   source_ts_descs.size(), nreplicas));
  }

  // Keep track of servers we've already selected, so that we don't attempt to
  // put two replicas on the same host.
  set<shared_ptr<TSDescriptor>> already_selected;
  for (auto i = 0; i < nreplicas; ++i) {
    auto ts = SelectReplica(
        source_ts_descs, dimension, range_key_start, table_id, already_selected);
    CHECK(ts);

    // Increment the number of pending replicas so that we take this selection
    // into account when assigning replicas for other tablets of the same table.
    // This value decays back to 0 over time.
    ts->IncrementRecentReplicaCreations();
    if (range_key_start && table_id) {
      ts->IncrementRecentReplicaCreationsByRangeAndTable(range_key_start.value(), table_id.value());
    }
    result_ts_descs->emplace_back(ts);
    EmplaceOrDie(&already_selected, std::move(ts));
  }
  return Status::OK();
}

// The new replica selection algorithm takes into consideration the number of
// replicas per range of the set of replicas that is being placed. If there's a
// tie between multiple tablet servers, the number of replicas per table of the
// set of replicas being placed will be considered. If there's still a tie between
// multiple tablet servers, the total number of replicas will be considered. If
// there's still a tie, a tablet server will be randomly chosen.
//
// The old replica selection algorithm follows the idea from
// "Power of Two Choices in Randomized Load Balancing"[1]. For each replica,
// we randomly select two tablet servers, and then assign the replica to the
// less-loaded one of the two. This has some nice properties:
//
// 1) because the initial selection of two servers is random, we get good
//    spreading of replicas across the cluster. In contrast, if we sorted by
//    load and always picked under-loaded servers first, we'd end up causing
//    all tablets of a new table to be placed on an empty server. This wouldn't
//    give good load balancing of that table.
//
// 2) because we pick the less-loaded of two random choices, we do end up with a
//    weighting towards filling up the underloaded one over time, without
//    the extreme scenario above.
//
// 3) because we don't follow any sequential pattern, every server is equally
//    likely to replicate its tablets to every other server. In contrast, a
//    round-robin design would enforce that each server only replicates to its
//    adjacent nodes in the TS sort order, limiting recovery bandwidth (see
//    KUDU-1317).
//
// [1] http://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
//
shared_ptr<TSDescriptor> PlacementPolicy::SelectReplica(
    const TSDescriptorVector& ts_descs,
    const optional<string>& dimension,
    const optional<string>& range_key_start,
    const optional<string>& table_id,
    const set<shared_ptr<TSDescriptor>>& excluded) const {
  if (range_key_start && table_id) {
    TSDescriptorVector ts_choices;
    auto choices_size = ts_descs.size() - excluded.size();
    ReservoirSample(ts_descs, choices_size, excluded, rng_, &ts_choices);
    DCHECK_EQ(ts_choices.size(), choices_size);
    if (ts_choices.size() > 1) {
      return PickTabletServer(
          ts_choices, range_key_start.value(), table_id.value(), dimension, rng_);
    }
    if (ts_choices.size() == 1) {
      return ts_choices.front();
    }
    return nullptr;
  } else {
    // Pick two random servers, excluding those we've already picked.
    // If we've only got one server left, 'two_choices' will actually
    // just contain one element.
    TSDescriptorVector two_choices;
    ReservoirSample(ts_descs, 2, excluded, rng_, &two_choices);
    DCHECK_LE(two_choices.size(), 2);

    if (two_choices.size() == 2) {
      // Pick the better of the two.
      return PickBetterTabletServer(two_choices, dimension, rng_);
    }
    if (two_choices.size() == 1) {
      return two_choices.front();
    }
    return nullptr;
  }
}

Status PlacementPolicy::SelectLocation(
    int num_replicas,
    const ReplicaLocationsInfo& locations_info,
    string* location) const {
  DCHECK(location);

  const auto num_locations = ltd_.size();

  // A pair of the location-per-load maps. The idea is to get a group to select
  // the best location based on the load, while not placing the majority of
  // replicas in same location, if possible. Using multimap (but not
  // unordered_multimap) in order to maintain the entries in load-sorted order.
  multimap<double, string> location_per_load;
  multimap<double, string> location_per_load_overflow;
  for (const auto& elem : ltd_) {
    const auto& location = elem.first;
    const auto* location_replicas_num_ptr = FindOrNull(locations_info, location);
    if (location_replicas_num_ptr) {
      const auto location_num_tservers = elem.second.size();
      const auto location_replicas_num = *location_replicas_num_ptr;
      CHECK_LE(location_replicas_num, location_num_tservers);
      if (location_replicas_num == location_num_tservers) {
        // It's not possible to place more than one replica of the same tablet
        // per tablet server.
        continue;
      }
      // When placing the replicas of a tablet, it's necessary to take into
      // account number of available locations, since the maximum number
      // of replicas per non-overflow location depends on that. For example,
      // in case of 2 locations the best placement for 4 replicas would be
      // (2 + 2), while in case of 4 and more locations that's (1 + 1 + 1 + 1).
      // Similarly, in case of 2 locations and 6 replicas, the best placement
      // is (3 + 3), while for 3 locations that's (2 + 2 + 2). In case of
      // distributing 3 replicas among 2 locations, 1 + 2 is better than 3 + 0
      // because if all servers in the first location become unavailable, in the
      // former case the tablet is still available, while in the latter it's not.
      // Also, 1 + 2 is better than 0 + 3 because in case of catastrophic
      // non-recoverable failure of the second location, no replica survives and
      // all data is lost in the latter case, while in the former case there will
      // be 1 replica and it may be used for manual data recovery.
      if ((num_locations == 2 && location_replicas_num + 1 > num_replicas / 2) ||
          (num_locations > 2 && location_replicas_num + 1 >= (num_replicas + 1) / 2)) {
        // If possible, avoid placing the majority of the tablet's replicas
        // into a single location even if load-based criterion would favor that.
        // Prefer such a distribution of replicas that will keep the majority
        // of replicas alive if any single location fails. So, if placing one
        // extra replica would add up to the majority in case of odd replication
        // factor or add up to the half of all replicas in case of even
        // replication factor, place this location into the overflow group.
        location_per_load_overflow.emplace(
            GetLocationLoad(location, locations_info), location);
        continue;
      }
    }
    location_per_load.emplace(
        GetLocationLoad(location, locations_info), location);
  }

  if (location_per_load.empty()) {
    // In case of one location or two locations and odd replication factor
    // it's not possible to make every location contain less than the majority
    // of replicas. Another case is when it's not enough tablet servers
    // to place the requested number of replicas.
    location_per_load.swap(location_per_load_overflow);
  }

  if (location_per_load.empty()) {
    // The cluster cannot accommodate any additional tablet replica even if
    // placing the majority of replicas into one location: not enough
    // tablet servers.
    return Status::NotFound("not enough tablet servers to satisfy replica "
                            "placement policy");
  }

  auto it = location_per_load.begin();
  const auto min_load = it->first;
  const auto eq_load_range = location_per_load.equal_range(min_load);
  const auto distance = std::distance(eq_load_range.first, eq_load_range.second);

  // In case if multiple location candidates, select a random one.
  std::advance(it, rng_->Uniform(distance));
  *location = it->second;
  return Status::OK();
}

} // namespace master
} // namespace kudu
