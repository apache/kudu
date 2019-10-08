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

#include "kudu/master/ts_manager.h"

#include <time.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/location_cache.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

DEFINE_bool(location_mapping_by_uuid, false,
            "Whether the location command is given tablet server identifier "
            "instead of hostname/IP address (for tests only).");
TAG_FLAG(location_mapping_by_uuid, hidden);
TAG_FLAG(location_mapping_by_uuid, unsafe);

METRIC_DEFINE_gauge_int32(server, cluster_replica_skew,
                          "Cluster Replica Skew",
                          kudu::MetricUnit::kTablets,
                          "The difference between the number of replicas on "
                          "the tablet server hosting the most replicas and "
                          "the number of replicas on the tablet server hosting "
                          "the least replicas.");

using kudu::pb_util::SecureShortDebugString;
using std::lock_guard;
using std::unordered_set;
using std::shared_ptr;
using std::string;
using strings::Substitute;

namespace kudu {
namespace master {

class TServerStateLoader : public TServerStateVisitor {
 public:
  explicit TServerStateLoader(TSManager* ts_manager)
      : ts_manager_(ts_manager) {}

  Status Visit(const std::string& tserver_id,
               const SysTServerStateEntryPB& metadata) override {
    ts_manager_->ts_state_lock_.AssertAcquiredForWriting();
    TServerStatePB state = metadata.state();
    if (state == TServerStatePB::UNKNOWN_STATE) {
      LOG(WARNING) << Substitute("ignoring unknown tserver state: $0", metadata.state());
      return Status::OK();
    }
    DCHECK_NE(TServerStatePB::NONE, state);
    int64_t timestamp = metadata.has_timestamp_secs() ? metadata.timestamp_secs() : -1;
    InsertOrDie(&ts_manager_->ts_state_by_uuid_, tserver_id, { state, timestamp });
    return Status::OK();
  }

 private:
  TSManager* ts_manager_;
};

TSManager::TSManager(LocationCache* location_cache,
                     const scoped_refptr<MetricEntity>& metric_entity)
    : ts_state_lock_(RWMutex::Priority::PREFER_READING),
      location_cache_(location_cache) {
  METRIC_cluster_replica_skew.InstantiateFunctionGauge(
      metric_entity,
      Bind(&TSManager::ClusterSkew, Unretained(this)))
    ->AutoDetach(&metric_detacher_);
}

TSManager::~TSManager() {
}

Status TSManager::LookupTS(const NodeInstancePB& instance,
                           shared_ptr<TSDescriptor>* ts_desc) const {
  shared_lock<rw_spinlock> l(lock_);
  const shared_ptr<TSDescriptor>* found_ptr =
    FindOrNull(servers_by_id_, instance.permanent_uuid());
  if (!found_ptr) {
    return Status::NotFound("unknown tablet server ID",
        SecureShortDebugString(instance));
  }
  const shared_ptr<TSDescriptor>& found = *found_ptr;

  if (instance.instance_seqno() != found->latest_seqno()) {
    return Status::NotFound("mismatched instance sequence number",
                            SecureShortDebugString(instance));
  }

  *ts_desc = found;
  return Status::OK();
}

bool TSManager::LookupTSByUUID(const string& uuid,
                               std::shared_ptr<TSDescriptor>* ts_desc) const {
  shared_lock<rw_spinlock> l(lock_);
  return FindCopy(servers_by_id_, uuid, ts_desc);
}

Status TSManager::RegisterTS(const NodeInstancePB& instance,
                             const ServerRegistrationPB& registration,
                             DnsResolver* dns_resolver,
                             std::shared_ptr<TSDescriptor>* desc) {
  // Pre-condition: registration info should contain at least one RPC end-point.
  if (registration.rpc_addresses().empty()) {
    return Status::InvalidArgument(
        "invalid registration: must have at least one RPC address",
        SecureShortDebugString(registration));
  }

  const string& uuid = instance.permanent_uuid();

  // Assign the location for the tablet server outside the lock: assigning
  // a location involves calling the location mapping script which is relatively
  // long and expensive operation.
  boost::optional<string> location;
  if (location_cache_) {
    // In some test scenarios the location is assigned per tablet server UUID.
    // That's the case when multiple (or even all) tablet servers have the same
    // IP address for their RPC endpoint.
    const auto& cmd_arg = PREDICT_FALSE(FLAGS_location_mapping_by_uuid)
        ? uuid : registration.rpc_addresses(0).host();
    TRACE(Substitute("tablet server $0: assigning location", uuid));
    string location_str;
    const auto s = location_cache_->GetLocation(cmd_arg, &location_str);
    TRACE(Substitute(
        "tablet server $0: assigned location '$1'", uuid, location_str));

    // If location resolution fails, log the error and return the status.
    if (!s.ok()) {
      CHECK(!registration.rpc_addresses().empty());
      const auto& addr = registration.rpc_addresses(0);
      KLOG_EVERY_N_SECS(ERROR, 60) << Substitute(
          "Unable to assign location to tablet server $0: $1",
          Substitute("$0 ($1:$2)", uuid, addr.host(), addr.port()),
          s.ToString());
      return s;
    }
    location.emplace(std::move(location_str));
  }

  shared_ptr<TSDescriptor> descriptor;
  bool new_tserver = false;
  {
    lock_guard<rw_spinlock> l(lock_);
    auto* descriptor_ptr = FindOrNull(servers_by_id_, uuid);
    if (descriptor_ptr) {
      descriptor = *descriptor_ptr;
      RETURN_NOT_OK(descriptor->Register(
          instance, registration, location, dns_resolver));
    } else {
      RETURN_NOT_OK(TSDescriptor::RegisterNew(
          instance, registration, location, dns_resolver, &descriptor));
      InsertOrDie(&servers_by_id_, uuid, descriptor);
      new_tserver = true;
    }
  }
  LOG(INFO) << Substitute("$0 tserver with Master: $1",
                          new_tserver ? "Registered new" : "Re-registered known",
                          descriptor->ToString());
  *desc = std::move(descriptor);

  return Status::OK();
}

void TSManager::GetAllDescriptors(TSDescriptorVector* descs) const {
  descs->clear();
  shared_lock<rw_spinlock> l(lock_);
  AppendValuesFromMap(servers_by_id_, descs);
}

int TSManager::GetCount() const {
  shared_lock<rw_spinlock> l(lock_);
  return servers_by_id_.size();
}

unordered_set<string> TSManager::GetUuidsToIgnoreForUnderreplication() const {
  unordered_set<string> uuids;
  shared_lock<RWMutex> tsl(ts_state_lock_);
  uuids.reserve(ts_state_by_uuid_.size());
  for (const auto& ts_and_state_timestamp : ts_state_by_uuid_) {
    if (ts_and_state_timestamp.second.first == TServerStatePB::MAINTENANCE_MODE) {
      uuids.emplace(ts_and_state_timestamp.first);
    }
  }
  return uuids;
}

TServerStateMap TSManager::GetTServerStates() const {
  shared_lock<RWMutex> tsl(ts_state_lock_);
  return ts_state_by_uuid_;
}

void TSManager::GetDescriptorsAvailableForPlacement(TSDescriptorVector* descs) const {
  descs->clear();
  shared_lock<RWMutex> tsl(ts_state_lock_);
  shared_lock<rw_spinlock> l(lock_);
  descs->reserve(servers_by_id_.size());
  for (const TSDescriptorMap::value_type& entry : servers_by_id_) {
    const shared_ptr<TSDescriptor>& ts = entry.second;
    if (AvailableForPlacementUnlocked(*ts)) {
      descs->push_back(ts);
    }
  }
}

Status TSManager::SetTServerState(const string& ts_uuid,
                                  TServerStatePB ts_state,
                                  ChangeTServerStateRequestPB::HandleMissingTS handle_missing_ts,
                                  SysCatalogTable* sys_catalog) {
  lock_guard<RWMutex> l(ts_state_lock_);
  auto existing_state = FindWithDefault(
      ts_state_by_uuid_, ts_uuid, { TServerStatePB::NONE, -1 }).first;
  if (existing_state == ts_state) {
    return Status::OK();
  }
  // If there is no existing state for the tserver, and the tserver hasn't yet
  // been registered, return an error, as appropriate.
  shared_ptr<TSDescriptor> ts_desc;
  if (handle_missing_ts != ChangeTServerStateRequestPB::ALLOW_MISSING_TSERVER &&
      existing_state == TServerStatePB::NONE && !LookupTSByUUID(ts_uuid, &ts_desc)) {
    return Status::NotFound(Substitute("Requested tserver $0 has not been registered",
                                       ts_uuid));
  }
  if (ts_state == TServerStatePB::NONE) {
    RETURN_NOT_OK_PREPEND(sys_catalog->RemoveTServerState(ts_uuid),
        Substitute("Failed to remove tserver state for $0", ts_uuid));
    ts_state_by_uuid_.erase(ts_uuid);
    // If exiting maintenance mode, make sure that any replica failures that
    // may have been ignored while in maintenance mode are reprocessed. To do
    // so, request full tablet reports across all tablet servers.
    SetAllTServersNeedFullTabletReports();
    LOG(INFO) << Substitute("Unset tserver state for $0 from $1",
                            ts_uuid, TServerStatePB_Name(existing_state));
    return Status::OK();
  }
  SysTServerStateEntryPB pb;
  pb.set_state(ts_state);
  int64_t timestamp = time(nullptr);
  pb.set_timestamp_secs(timestamp);
  RETURN_NOT_OK_PREPEND(sys_catalog->WriteTServerState(ts_uuid, pb),
      Substitute("Failed to set tserver state for $0 to $1",
                 ts_uuid, TServerStatePB_Name(ts_state)));
  LOG(INFO) << Substitute("Set tserver state for $0 to $1",
                          ts_uuid, TServerStatePB_Name(ts_state));
  InsertOrUpdate(&ts_state_by_uuid_, ts_uuid, { ts_state, timestamp });
  return Status::OK();
}

TServerStatePB TSManager::GetTServerStateUnlocked(const string& ts_uuid) const {
  ts_state_lock_.AssertAcquired();
  return FindWithDefault(ts_state_by_uuid_, ts_uuid, { TServerStatePB::NONE, -1 }).first;
}

TServerStatePB TSManager::GetTServerState(const string& ts_uuid) const {
  shared_lock<RWMutex> l(ts_state_lock_);
  return GetTServerStateUnlocked(ts_uuid);
}

Status TSManager::ReloadTServerStates(SysCatalogTable* sys_catalog) {
  lock_guard<RWMutex> l(ts_state_lock_);
  ts_state_by_uuid_ = {};
  TServerStateLoader loader(this);
  return sys_catalog->VisitTServerStates(&loader);
}

void TSManager::SetAllTServersNeedFullTabletReports() {
  lock_guard<rw_spinlock> l(lock_);
  for (auto& id_and_desc : servers_by_id_) {
    id_and_desc.second->UpdateNeedsFullTabletReport(true);
  }
}

int TSManager::ClusterSkew() const {
  int min_count = std::numeric_limits<int>::max();
  int max_count = 0;
  shared_lock<rw_spinlock> l(lock_);
  for (const TSDescriptorMap::value_type& entry : servers_by_id_) {
    const shared_ptr<TSDescriptor>& ts = entry.second;
    if (ts->PresumedDead()) {
      continue;
    }
    int num_live_replicas = ts->num_live_replicas();
    min_count = std::min(min_count, num_live_replicas);
    max_count = std::max(max_count, num_live_replicas);
  }
  return max_count - min_count;
}

bool TSManager::AvailableForPlacementUnlocked(const TSDescriptor& ts) const {
  ts_state_lock_.AssertAcquired();
  // TODO(KUDU-1827): this should also be used when decommissioning a server.
  if (GetTServerStateUnlocked(ts.permanent_uuid()) == TServerStatePB::MAINTENANCE_MODE) {
    return false;
  }
  // If the tablet server has heartbeated recently enough, it is considered
  // alive and available for placement.
  return !ts.PresumedDead();
}

} // namespace master
} // namespace kudu

