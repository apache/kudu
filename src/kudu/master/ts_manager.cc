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

#include <algorithm>
#include <limits>
#include <mutex>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"

METRIC_DEFINE_gauge_int32(server, cluster_replica_skew,
                          "Cluster Replica Skew",
                          kudu::MetricUnit::kTablets,
                          "The difference between the number of replicas on "
                          "the tablet server hosting the most replicas and "
                          "the number of replicas on the tablet server hosting "
                          "the least replicas.");

using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

TSManager::TSManager(const scoped_refptr<MetricEntity>& metric_entity) {
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
        pb_util::SecureShortDebugString(instance));
  }
  const shared_ptr<TSDescriptor>& found = *found_ptr;

  if (instance.instance_seqno() != found->latest_seqno()) {
    return Status::NotFound("mismatched instance sequence number",
                            pb_util::SecureShortDebugString(instance));
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
                             std::shared_ptr<TSDescriptor>* desc) {
  std::lock_guard<rw_spinlock> l(lock_);
  const string& uuid = instance.permanent_uuid();

  if (!ContainsKey(servers_by_id_, uuid)) {
    shared_ptr<TSDescriptor> new_desc;
    RETURN_NOT_OK(TSDescriptor::RegisterNew(instance, registration, &new_desc));
    InsertOrDie(&servers_by_id_, uuid, new_desc);
    LOG(INFO) << Substitute("Registered new tserver with Master: $0",
                            new_desc->ToString());
    desc->swap(new_desc);
  } else {
    shared_ptr<TSDescriptor> found(FindOrDie(servers_by_id_, uuid));
    RETURN_NOT_OK(found->Register(instance, registration));
    LOG(INFO) << Substitute("Re-registered known tserver with Master: $0",
                            found->ToString());
    desc->swap(found);
  }

  return Status::OK();
}

void TSManager::GetAllDescriptors(TSDescriptorVector* descs) const {
  descs->clear();
  shared_lock<rw_spinlock> l(lock_);
  AppendValuesFromMap(servers_by_id_, descs);
}

void TSManager::GetAllLiveDescriptors(TSDescriptorVector* descs) const {
  descs->clear();

  shared_lock<rw_spinlock> l(lock_);
  descs->reserve(servers_by_id_.size());
  for (const TSDescriptorMap::value_type& entry : servers_by_id_) {
    const shared_ptr<TSDescriptor>& ts = entry.second;
    if (!ts->PresumedDead()) {
      descs->push_back(ts);
    }
  }
}

int TSManager::GetCount() const {
  shared_lock<rw_spinlock> l(lock_);
  return servers_by_id_.size();
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

} // namespace master
} // namespace kudu

