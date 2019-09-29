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

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class DnsResolver;
class NodeInstancePB;
class ServerRegistrationPB;

namespace master {

class LocationCache;
class SysCatalogTable;

// Tracks the servers that the master has heard from, along with their
// last heartbeat, etc.
//
// Note that TSDescriptors are never deleted, even if the TS crashes
// and has not heartbeated in quite a while. This makes it simpler to
// keep references to TSDescriptors elsewhere in the master without
// fear of lifecycle problems. Dead servers are "dead, but not forgotten"
// (they live on in the heart of the master).
//
// This class is thread-safe.
class TSManager {
 public:
  // 'location_cache' is a pointer to location mapping cache to use when
  // registering tablet servers. The location cache should outlive the
  // TSManager. 'metric_entity' is used to register metrics used by TSManager.
  TSManager(LocationCache* location_cache,
            const scoped_refptr<MetricEntity>& metric_entity);
  virtual ~TSManager();

  // Lookup the tablet server descriptor for the given instance identifier.
  // If the TS has never registered, or this instance doesn't match the
  // current instance ID for the TS, then a NotFound status is returned.
  // Otherwise, *desc is set and OK is returned.
  Status LookupTS(const NodeInstancePB& instance,
                  std::shared_ptr<TSDescriptor>* desc) const;

  // Lookup the tablet server descriptor for the given UUID.
  // Returns false if the TS has never registered.
  // Otherwise, *desc is set and returns true.
  bool LookupTSByUUID(const std::string& uuid,
                      std::shared_ptr<TSDescriptor>* desc) const;

  // Register or re-register a tablet server with the manager.
  //
  // If successful, *desc reset to the registered descriptor.
  Status RegisterTS(const NodeInstancePB& instance,
                    const ServerRegistrationPB& registration,
                    DnsResolver* dns_resolver,
                    std::shared_ptr<TSDescriptor>* desc);

  // Return all of the currently registered TS descriptors into the provided
  // list.
  void GetAllDescriptors(TSDescriptorVector* descs) const;

  // Return all of the currently registered TS descriptors that are available
  // for replica placement -- they have sent a heartbeat recently, indicating
  // that they're alive and well, and they aren't in a mode that would block
  // replication to them (e.g. maintenance mode).
  void GetDescriptorsAvailableForPlacement(TSDescriptorVector* descs) const;

  // Return any tablet servers UUIDs that can be in a failed state without
  // counting towards under-replication (e.g. because they're in maintenance
  // mode).
  std::unordered_set<std::string> GetUuidsToIgnoreForUnderreplication() const;

  // Get the TS count.
  int GetCount() const;

  // Sets the tserver state for the given tserver, persisting it to disk.
  //
  // If removing a tserver from maintenance mode, this also sets that all
  // tablet servers must report back a full tablet reports.
  Status SetTServerState(const std::string& ts_uuid,
                         TServerStatePB ts_state,
                         SysCatalogTable* sys_catalog);

  // Return the tserver state for the given tablet server UUID, or NONE if one
  // doesn't exist.
  TServerStatePB GetTServerState(const std::string& ts_uuid) const;

  // Resets the tserver states and reloads them from disk.
  Status ReloadTServerStates(SysCatalogTable* sys_catalog);

 private:
  friend class TServerStateLoader;

  int ClusterSkew() const;

  // Return the tserver state for the given tablet server UUID, or NONE if one
  // doesn't exist. Must hold 'ts_state_lock_' to call.
  TServerStatePB GetTServerStateUnlocked(const std::string& ts_uuid) const;

  // Returns whether the given server can have replicas placed on it (e.g. it
  // is not dead, not in maintenance mode).
  bool AvailableForPlacementUnlocked(const TSDescriptor& ts) const;

  // Sets that all registered tablet servers need to report back with a full
  // tablet report. This may be necessary, e.g., after exiting maintenance mode
  // to recheck any ignored failures.
  void SetAllTServersNeedFullTabletReports();

  FunctionGaugeDetacher metric_detacher_;

  // Protects 'servers_by_id_'.
  mutable rw_spinlock lock_;

  // TODO(awong): add a map from HostPort to descriptor so we aren't forced to
  // know UUIDs up front, e.g. if specifying a given tablet server for
  // maintenance mode, it'd be easier for users to specify the HostPort.
  typedef std::unordered_map<
      std::string, std::shared_ptr<TSDescriptor>> TSDescriptorMap;
  TSDescriptorMap servers_by_id_;

  // Protects 'ts_state_by_uuid_'. If both 'ts_state_lock_' and 'lock_' are to
  // be taken, 'ts_state_lock_' must be taken first.
  mutable RWMutex ts_state_lock_;

  // Maps from the UUIDs of tablet servers to their tserver state, if any.
  // Note: the states don't necessarily belong to registered tablet servers.
  std::unordered_map<std::string, TServerStatePB> ts_state_by_uuid_;

  LocationCache* location_cache_;

  DISALLOW_COPY_AND_ASSIGN(TSManager);
};

} // namespace master
} // namespace kudu

