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

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <gtest/gtest_prod.h>

#include "kudu/master/ts_descriptor.h"
#include "kudu/util/status.h"

namespace kudu {

class ThreadSafeRandom;

namespace master {

// Utility class to help place tablet replicas on tablet servers according
// to the location awareness policy. Currently, this class implements the logic
// specific to location awareness as described in [1], but it could enforce
// other placement policies in the future.
//
// In essence (for details see [1]), the location awareness placement policy
// is about:
//   * in case of N locations, N > 2, not placing the majority of replicas
//     in one location
//   * spreading replicas evenly among available locations
//   * within a location, spreading load evenly among tablet servers
//
// [1] https://s.apache.org/location-awareness-design
//
// TODO(aserbin): add a link to the doc once it appears in the upstream repo.
//
// NOTE: in the implementation of this class, it's OK to use an empty string
// in place of boost::none for a location because valid location strings begin
// with '/' and therefore are nonempty.
class PlacementPolicy {
 public:

  // The 'descs' vector contains information on all available tablet servers
  // in the cluster, the 'rng' parameter points to an instance of a random
  // generator that the object references to. The random generator instance
  // must not be nullptr and must outlive the PlacementPolicy object.
  PlacementPolicy(TSDescriptorVector descs,
                  ThreadSafeRandom* rng);

  virtual ~PlacementPolicy() = default;

  size_t ts_num() const { return ts_num_; }

  // Select tablet servers to host the given number of replicas for a tablet.
  // The 'nreplicas' parameter specifies the desired replication factor,
  // the result set of tablet server descriptors is output into the 'ts_descs'
  // placeholder (must not be null).
  Status PlaceTabletReplicas(int nreplicas, TSDescriptorVector* ts_descs) const;

  // Select tablet server to host an additional tablet replica. The 'existing'
  // parameter lists current members of the tablet's Raft configuration,
  // the new member is output into 'ts_desc' placeholer (must not be null).
  Status PlaceExtraTabletReplica(TSDescriptorVector existing,
                                 std::shared_ptr<TSDescriptor>* ts_desc) const;

 private:
  // Tablet server descriptors per location. This is the most comprehensive
  // information on how tablet servers are placed among locations. Inherently,
  // the locations have a sense of proximity and a hierarchy, so '/mega/turbo0'
  // is closer to '/mega/turbo1' than '/giga/awesome0' and both '/mega/turbo0'
  // and '/mega/turbo1' are affected by conditions currently affecting '/mega'.
  // Number of locations is not supposed to be high: at the order of magnitude
  // scale, that's about tens.
  //
  // NOTE: this dictionary container is made unordered since currently no code
  //       is taking advantage of the order of the keys.
  typedef std::unordered_map<std::string, TSDescriptorVector>
      LocationToDescriptorsMap;

  // Number of tablet replicas per location.
  typedef std::unordered_map<std::string, int> ReplicaLocationsInfo;

  friend class PlacementPolicyTest;
  FRIEND_TEST(PlacementPolicyTest, SelectLocationRandomnessForExtraReplica);

  // Get the load of the location: a location with N tablet servers and
  // R replicas has load R/N.
  //
  // Parameters:
  //   'location'       The location in question.
  //   'locations_info' Information on tablet replicas slated for placement,
  //                    but not created yet. That's the placement information
  //                    on to-be-replicas in the context of optimizing tablet
  //                    replica distribution in the cluster.
  double GetLocationLoad(const std::string& location,
                         const ReplicaLocationsInfo& locations_info) const;

  // Select locations to place the given number of replicas ('nreplicas') for
  // a new tablet. The locations are be chosen according to the placement
  // policies.
  //
  // TODO (aserbin): add the reference to the document once it's in the repo.
  Status SelectReplicaLocations(int nreplicas,
                                ReplicaLocationsInfo* locations_info) const;

  // Select the given number ('nreplicas') from the set of specified tablet
  // servers to place tablet replicas.
  Status SelectReplicas(const TSDescriptorVector& source_ts_desc,
                        int nreplicas,
                        TSDescriptorVector* result_ts_desc) const;

  // Given the tablet servers in 'ts_descs', pick a tablet server to host
  // a tablet replica, excluding tablet servers in 'excluded'. If there are no
  // servers in 'ts_descs' that are not in 'existing', return nullptr.
  std::shared_ptr<TSDescriptor> SelectReplica(
      const TSDescriptorVector& ts_descs,
      const std::set<std::shared_ptr<TSDescriptor>>& excluded) const;

  // Select location for next replica of a tablet with the specified replication
  // factor. In essence, the algorithm picks the least loaded location,
  // making sure no location contains the majority of the replicas.
  //
  // Parameters:
  //   'num_replicas'   The total number of tablet replicas to place.
  //   'locations_info' Information on tablet replicas slated for placement,
  //                    but not created yet. That's the placement information
  //                    on to-be-replicas in the context of optimizing tablet
  //                    replica distribution in the cluster.
  //   'location'       The result location pointer, must not be null.
  Status SelectLocation(int num_replicas,
                        const ReplicaLocationsInfo& locations_info,
                        std::string* location) const;

  // Number of available tablet servers.
  const size_t ts_num_;

  // Random number generator used for selecting replica locations.
  // The object that rng_ points at is supposed to be available during the whole
  // lifetime of a PlacementPolicy object.
  mutable ThreadSafeRandom* rng_;

  // Location to TSDescriptorVector map: the distribution of all already
  // existing tablet replicas among available tablet servers in the cluster,
  // grouped by location.
  LocationToDescriptorsMap ltd_;

  // A set of known tablet server identifiers (derived from ltd_).
  std::unordered_set<std::string> known_ts_ids_;
};

} // namespace master
} // namespace kudu
