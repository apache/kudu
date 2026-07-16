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

#include "kudu/master/auto_leader_rebalancer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/init.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::RaftPeerPB;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::map;
using std::nullopt;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DEFINE_uint32(auto_leader_rebalancing_rpc_timeout_seconds, 10,
              "auto leader rebalancing send leader step down rpc timeout seconds");
TAG_FLAG(auto_leader_rebalancing_rpc_timeout_seconds, advanced);
TAG_FLAG(auto_leader_rebalancing_rpc_timeout_seconds, runtime);

DEFINE_uint32(auto_leader_rebalancing_interval_seconds, 3600,
              "How long to sleep in between auto leader rebalancing cycles, before checking "
              "the cluster again to see if there is leader skew and if run task again.");
TAG_FLAG(auto_leader_rebalancing_interval_seconds, advanced);
TAG_FLAG(auto_leader_rebalancing_interval_seconds, runtime);

DEFINE_uint32(leader_rebalancing_max_moves_per_round, 10,
              "Max count of leader transfer when every leader rebalance runs");
TAG_FLAG(leader_rebalancing_max_moves_per_round, advanced);
TAG_FLAG(leader_rebalancing_max_moves_per_round, runtime);

DEFINE_bool(leader_rebalancing_ignore_soft_deleted_tables, false,
            "Whether to ignore rebalancing the soft deleted tables");
TAG_FLAG(leader_rebalancing_ignore_soft_deleted_tables, advanced);
TAG_FLAG(leader_rebalancing_ignore_soft_deleted_tables, runtime);

DEFINE_bool(auto_leader_rebalancing_fail_moves_for_test, false,
            "Force every leader step-down RPC issued by the per-table pass of "
            "the leader rebalancer (RunLeaderRebalanceForTable) to fail. This "
            "is only used for testing.");
TAG_FLAG(auto_leader_rebalancing_fail_moves_for_test, unsafe);
TAG_FLAG(auto_leader_rebalancing_fail_moves_for_test, hidden);

DECLARE_bool(auto_leader_rebalancing_enabled);

namespace kudu {
namespace master {

AutoLeaderRebalancerTask::AutoLeaderRebalancerTask(CatalogManager* catalog_manager,
                                                   TSManager* ts_manager)
    : catalog_manager_(catalog_manager),
      ts_manager_(ts_manager),
      shutdown_(1),
      random_generator_(random_device_()),
      number_of_loop_iterations_for_test_(0),
      moves_scheduled_this_round_for_test_(0) {}

AutoLeaderRebalancerTask::~AutoLeaderRebalancerTask() {
  if (thread_) {
    Shutdown();
  }
}

Status AutoLeaderRebalancerTask::Init() {
  DCHECK(!thread_) << "AutoleaderRebalancerTask is already initialized";
  MessengerBuilder builder("auto-leader-rebalancer");
  if (auto username = kudu::security::GetLoggedInUsernameFromKeytab()) {
    builder.set_sasl_proto_name(username.value());
  }
  RETURN_NOT_OK(std::move(builder).Build(&messenger_));
  return Thread::Create("catalog manager", "auto-leader-rebalancer",
                        [this]() { this->RunLoop(); }, &thread_);
}

void AutoLeaderRebalancerTask::Shutdown() {
  CHECK(thread_) << "AutoLeaderRebalancerTask is not initialized";
  if (!shutdown_.CountDown()) {
    return;
  }
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
  thread_.reset();
}

Status AutoLeaderRebalancerTask::RunLeaderRebalanceForTable(
    const scoped_refptr<TableInfo>& table_info,
    const vector<string>& tserver_uuids,
    const unordered_set<string>& exclude_dest_uuids,
    unordered_map<string, int>* global_leader_count,
    AutoLeaderRebalancerTask::ExecuteMode mode,
    int* num_scheduled_moves) {
  LOG(INFO) << Substitute("leader rebalance for table $0", table_info->table_name());
  TableMetadataLock table_l(table_info.get(), LockMode::READ);
  const SysTablesEntryPB& table_data = table_info->metadata().state().pb;
  int replication_factor = table_data.num_replicas();
  DCHECK_GT(replication_factor, 0);
  if (table_data.state() == SysTablesEntryPB::REMOVED) {
    // Don't worry about rebalancing replicas that belong to deleted tables.
    return Status::OK();
  }

  // tablet_id -> leader‘s tserver uuid
  map<string, string> leader_ts_uuid_by_tablet_id;
  // tablet_id -> followers' tserver uuids
  map<string, vector<string>> follower_ts_uuids_by_tablet_id;
  // tserver uuid -> leaders' replicas
  map<string, vector<string>> leader_tablet_ids_by_ts_uuid;
  // tserver uuid -> all replicas
  map<string, vector<string>> tablet_ids_by_ts_uuid;

  map<string, HostPort> host_port_by_leader_ts_uuid;

  vector<scoped_refptr<TabletInfo>> tablet_infos;
  table_info->GetAllTablets(&tablet_infos);

  // step 1. Get basic statistics
  for (const auto& tablet : tablet_infos) {
    TabletMetadataLock tablet_l(tablet.get(), LockMode::READ);

    // Retrieve all replicas of the tablet.
    TabletLocationsPB locs_pb;
    CatalogManager::TSInfosDict ts_infos_dict;

    {
      CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager_);
      RETURN_NOT_OK(leaderlock.first_failed_status());
      // This will only return tablet replicas in the RUNNING state, and
      // filter to only retrieve voter replicas.
      RETURN_NOT_OK(catalog_manager_->GetTabletLocations(
          tablet->id(),
          ReplicaTypeFilter::VOTER_REPLICA,
          /*use_external_addr=*/false,
          &locs_pb,
          &ts_infos_dict,
          nullopt));
    }

    // Build a summary for each replica of the tablet.
    for (const auto& r : locs_pb.interned_replicas()) {
      int index = r.ts_info_idx();
      const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs()[index]);
      string uuid = ts_info.permanent_uuid();
      if (r.role() == RaftPeerPB::LEADER) {
        auto& leader_uuids = LookupOrInsert(&leader_tablet_ids_by_ts_uuid, uuid, {});
        leader_uuids.emplace_back(tablet->id());
        InsertOrDie(&leader_ts_uuid_by_tablet_id, tablet->id(), uuid);
        InsertIfNotPresent(
            &host_port_by_leader_ts_uuid, uuid, HostPortFromPB(ts_info.rpc_addresses(0)));
      } else if (r.role() == RaftPeerPB::FOLLOWER) {
        auto& follower_uuids = LookupOrInsert(&follower_ts_uuids_by_tablet_id, tablet->id(), {});
        follower_uuids.emplace_back(uuid);
      } else {
        LOG(WARNING) << Substitute("table_id $0, permanent_uuid $1, not a VOTER, role: $2",
                                   tablet->id(),
                                   uuid,
                                   RaftPeerPB::Role_Name(r.role()));
        continue;
      }

      auto& uuid_replicas = LookupOrInsert(&tablet_ids_by_ts_uuid, ts_info.permanent_uuid(), {});
      uuid_replicas.emplace_back(tablet->id());
    }
  }

  // Count this table's leaders into the global map before the early return for
  // single replica tables below. Those leaders can't be moved, but they are
  // still real load. If we left them out, a tserver holding many of them would
  // look empty to the tie breaker and keep getting chosen as a move target.
  if (global_leader_count) {
    for (const auto& [uuid, tablet_ids] : leader_tablet_ids_by_ts_uuid) {
      (*global_leader_count)[uuid] += static_cast<int>(tablet_ids.size());
    }
  }

  // A tablet with a single replica has no follower to hand leadership to, so
  // there is nothing to rebalance. We already counted its leader above.
  if (replication_factor == 1) {
    return Status::OK();
  }

  // step 2.
  // pick the servers which number of leaders greater than 1/3 of number of all replicas
  // <uuid, number of replica, number of leader>
  map<string, pair<int32_t, int32_t>> replica_and_leader_count_by_ts_uuid;
  // uuid->leader should transfer count
  map<string, int32_t> leader_transfer_source;
  size_t remaining_tablets = tablet_infos.size();
  size_t remaining_tservers = tserver_uuids.size();
  for (const auto& uuid : tserver_uuids) {
    auto* tablet_ids_ptr = FindOrNull(tablet_ids_by_ts_uuid, uuid);
    int32_t replica_count = tablet_ids_ptr ? tablet_ids_ptr->size() : 0;
    if (replica_count == 0) {
      // means no replicas (and no leaders), maybe a tserver joined kudu cluster just now, skip it
      remaining_tservers--;
      continue;
    }
    auto* leader_tablet_ids_ptr = FindOrNull(leader_tablet_ids_by_ts_uuid, uuid);
    int32_t leader_count = leader_tablet_ids_ptr ? leader_tablet_ids_ptr->size() : 0;
    replica_and_leader_count_by_ts_uuid.insert(
        {uuid, pair<int32_t, int32_t>(replica_count, leader_count)});
    VLOG(1) << Substitute(
        "uuid: $0, replica_count: $1, leader_count: $2", uuid, replica_count, leader_count);

    // If the number of remaining tablets is divisible by the number of remaining tablet
    // servers, the leader num of all the remaining tablet servers should be the division
    // result.
    // Else, the maximum number of leader replicas per tablet server should be the ceil value
    // of the average leaders num. Transfer the excess leaders if a tablet server's
    // leader num is more than that.
    const uint32_t target_leader_count =
        (remaining_tablets + remaining_tservers - 1) / remaining_tservers;
    int32_t should_transfer_count = leader_count - static_cast<int32_t>(target_leader_count);
    if (should_transfer_count > 0) {
      leader_transfer_source.insert({uuid, should_transfer_count});
      VLOG(1) << Substitute("$0 should transfer leader count: $1", uuid, should_transfer_count);
    }
    if (remaining_tablets % remaining_tservers == 0) {
      remaining_tablets -= target_leader_count;
    } else {
      remaining_tablets -= (should_transfer_count >= 0 ? target_leader_count :
                            (target_leader_count - 1));
    }
    remaining_tservers--;
  }

  // Step 3.
  // Generate transfer task, <tablet_id, from_uuid, to_uuid>
  map<string, pair<string, string>> leader_transfer_tasks;
  for (const auto& from_info : leader_transfer_source) {
    string leader_uuid = from_info.first;
    int32_t need_transfer_count = from_info.second;
    int32_t pick_count = 0;
    vector<string>& uuid_leaders = leader_tablet_ids_by_ts_uuid[leader_uuid];
    std::shuffle(uuid_leaders.begin(), uuid_leaders.end(), random_generator_);
    // This loop would generate 'uuid_leaders.size()' leader transferring tasks at most.
    // Every task would pick a dest uuid to transfer leader.
    for (int i = 0; i < uuid_leaders.size(); i++) {
      const string& tablet_id = uuid_leaders[i];
      vector<string> uuid_followers = follower_ts_uuids_by_tablet_id[tablet_id];

      // TabletId leader transfer, pick a dest follower
      string dest_follower_uuid;
      if (uuid_followers.size() + 1 < replication_factor) {
        continue;
      }
      // Pick the follower with the lowest leader ratio for this table. If two
      // followers tie, prefer the one with fewer leaders across all tables. If
      // they still tie, fall back to the smaller uuid so the result does not
      // depend on the order replicas happen to come back in.
      //
      // The best ratio so far is tracked as a fraction (best_leader_count over
      // best_replica_count, starting at 1/1, the worst possible ratio). Ratios
      // are compared by cross multiplying, which keeps ties exact in a way that
      // comparing rounded doubles would not.
      int32_t best_leader_count = 1;
      int32_t best_replica_count = 1;
      int best_global_count = 0;
      bool dest_invalid = false;
      for (int j = 0; j < uuid_followers.size(); j++) {
        const string& follower_uuid = uuid_followers[j];
        if (ContainsKey(exclude_dest_uuids, follower_uuid)) {
          continue;
        }
        pair<int32_t, int32_t>& replica_and_leader_count =
            replica_and_leader_count_by_ts_uuid[follower_uuid];
        int32_t replica_count = replica_and_leader_count.first;
        if (replica_count <= 0) {
          dest_invalid = true;
          break;
        }
        int32_t leader_count = replica_and_leader_count.second;
        int global_count = global_leader_count
            ? FindWithDefault(*global_leader_count, follower_uuid, 0) : 0;
        // leader_count / replica_count  vs  best_leader_count / best_replica_count
        const int64_t lhs = static_cast<int64_t>(leader_count) * best_replica_count;
        const int64_t rhs = static_cast<int64_t>(best_leader_count) * replica_count;
        bool better;
        if (lhs != rhs) {
          better = lhs < rhs;
        } else if (global_count != best_global_count) {
          better = global_count < best_global_count;
        } else {
          // Still tied. A follower that is already full (ratio 1/1, our starting
          // value) is never a valid target, so only break the tie once we have
          // a real candidate.
          better = !dest_follower_uuid.empty() && follower_uuid < dest_follower_uuid;
        }
        if (better) {
          best_leader_count = leader_count;
          best_replica_count = replica_count;
          best_global_count = global_count;
          dest_follower_uuid = follower_uuid;
        }
      }
      if (dest_invalid || dest_follower_uuid.empty()) {
        continue;
      }
      pair<int32_t, int32_t>& replica_and_leader_count =
          replica_and_leader_count_by_ts_uuid[leader_uuid];
      int32_t replica_count = replica_and_leader_count.first;
      int32_t leader_count = replica_and_leader_count.second;
      // Skip the move if the destination already has a higher leader ratio than
      // the source, since that would only make the skew worse. Compared as
      // fractions to stay exact.
      if (static_cast<int64_t>(best_leader_count) * replica_count >
          static_cast<int64_t>(leader_count) * best_replica_count) {
        continue;
      }

      leader_transfer_tasks.insert(
          {tablet_id, pair<string, string>(leader_uuid, dest_follower_uuid)});
      replica_and_leader_count_by_ts_uuid[leader_uuid].second--;
      replica_and_leader_count_by_ts_uuid[dest_follower_uuid].second++;
      if (global_leader_count) {
        (*global_leader_count)[leader_uuid]--;
        (*global_leader_count)[dest_follower_uuid]++;
      }
      if (leader_transfer_tasks.size() >= FLAGS_leader_rebalancing_max_moves_per_round) {
        break;
      }
      if (++pick_count == need_transfer_count) {
        // Have picked enough leader transfer tasks for this tserver.
        break;
      }
    }
    if (leader_transfer_tasks.size() >= FLAGS_leader_rebalancing_max_moves_per_round) {
      VLOG(1) << Substitute(
          "leader rebalance reach the upper limit: $0, try do left leader transfer tasks next "
          "time", FLAGS_leader_rebalancing_max_moves_per_round);
    }
  }

  if (PREDICT_FALSE(mode == AutoLeaderRebalancerTask::ExecuteMode::TEST)) {
    if (!leader_transfer_tasks.empty()) {
      return Status::IllegalState(Substitute("leader_transfer_task size should be 0, but $0",
                                             leader_transfer_tasks.size()));
    }
    return Status::OK();
  }

  moves_scheduled_this_round_for_test_ = leader_transfer_tasks.size();
  if (num_scheduled_moves) {
    *num_scheduled_moves += leader_transfer_tasks.size();
  }
  VLOG(1) << Substitute("leader rebalance tasks, size: $0, leader_transfer_source, size: $1",
                        moves_scheduled_this_round_for_test_.load(),
                        leader_transfer_source.size());
  // Step 4. Do Leader transfer tasks.
  // @TODO(duyuqi), optimal speed
  // If leader rebalancing tasks is too many, each rpc of the thread wait the response
  // synchronously, which may be very slow.

  int leader_transfer_count = 0;
  for (const auto& task : leader_transfer_tasks) {
    const string& leader_uuid = task.second.first;
    LeaderStepDownRequestPB request;
    request.set_dest_uuid(task.second.first);
    request.set_tablet_id(task.first);
    request.set_mode(LeaderStepDownMode::GRACEFUL);
    request.set_new_leader_uuid(task.second.second);

    LeaderStepDownResponsePB response;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_leader_rebalancing_rpc_timeout_seconds));

    auto* host_port = FindOrNull(host_port_by_leader_ts_uuid, leader_uuid);
    if (!host_port) {
      continue;
    }
    shared_ptr<TSDescriptor> leader_desc;
    if (!ts_manager_->LookupTSByUUID(leader_uuid, &leader_desc)) {
      continue;
    }
    if (PREDICT_FALSE(TServerStatePB::MAINTENANCE_MODE ==
                      ts_manager_->GetTServerState(task.second.second))) {
      continue;
    }

    vector<Sockaddr> resolved;
    if (Status s = host_port->ResolveAddresses(&resolved); !s.ok()) {
      WARN_NOT_OK(s, Substitute("leader transfer for tablet $0: could not resolve $1",
                                task.first, host_port->ToString()));
      continue;
    }
    ConsensusServiceProxy proxy(messenger_, resolved[0], host_port->host());
    // A single leader transfer is best effort. A transient failure (an RPC
    // timeout, the target briefly unavailable, or an election in flight) should
    // not abort the whole rebalancing pass and skip the remaining tablets and
    // tables; the next round recomputes and retries.
    Status s = PREDICT_FALSE(FLAGS_auto_leader_rebalancing_fail_moves_for_test)
        ? Status::ServiceUnavailable("TEST: forced leader step-down failure")
        : proxy.LeaderStepDown(request, &response, &rpc);
    if (!s.ok()) {
      WARN_NOT_OK(s, Substitute("leader transfer for tablet $0 from $1 to $2 failed",
                                task.first, leader_uuid, task.second.second));
      continue;
    }
    if (!response.has_error()) {
      leader_transfer_count++;
      VLOG(1) << Substitute("leader transfer table: $0, tablet_id: $1, from: $2 to: $3",
                            table_data.name(),
                            task.first,
                            leader_uuid,
                            task.second.second);
    } else {
      LOG(WARNING) << Substitute(
          "leader transfer for tablet $0 (from $1 to $2) failed: $3",
          task.first, leader_uuid, task.second.second, response.error().ShortDebugString());
    }
  }
  // @TODO(duyuqi)
  // Add metrics to replace the log.
  VLOG(0) << Substitute("table: $0, leader rebalance finish, leader transfer count: $1",
                        table_data.name(),
                        leader_transfer_count);
  return Status::OK();
}

Status AutoLeaderRebalancerTask::RunGlobalLeaderRebalance(
    const vector<scoped_refptr<TableInfo>>& table_infos,
    const vector<string>& tserver_uuids,
    const unordered_set<string>& exclude_dest_uuids,
    unordered_map<string, int>* global_leader_count,
    AutoLeaderRebalancerTask::ExecuteMode mode) {
  // Callers always pass a non-null map; it isn't optional like it is for the
  // per-table method.
  DCHECK(global_leader_count);

  // Destinations in maintenance mode are excluded: such tservers should be
  // shedding leaders, so they neither count towards the global average nor act
  // as a move source here. A tserver that hosts no replicas yet (e.g. one that
  // just joined) still counts: it can only pull the average down, and since it
  // is a follower of nothing it can never be selected as a destination below,
  // so it is harmless.
  vector<string> eligible_uuids;
  eligible_uuids.reserve(tserver_uuids.size());
  for (const auto& uuid : tserver_uuids) {
    if (!ContainsKey(exclude_dest_uuids, uuid)) {
      eligible_uuids.emplace_back(uuid);
    }
  }
  // At least two tservers are needed to move a leader between them.
  if (eligible_uuids.size() < 2) {
    return Status::OK();
  }

  // 'ceil_avg' is the ceiling of the average number of leaders per eligible
  // tserver. A perfectly balanced cluster has every tserver at the floor or the
  // ceiling of that average. A tserver above 'ceil_avg' is overloaded and must
  // shed leaders; any tserver still below 'ceil_avg' has room to take one.
  // Using the ceiling (rather than the floor) as the destination bar matters
  // when the leaders don't divide evenly: it lets an overloaded tserver hand a
  // leader to one currently sitting at the floor, which is exactly the move
  // needed to reach e.g. {3,3,2,2} instead of getting stuck at {4,2,2,2}.
  //
  // A leader is only ever handed to a tserver that already hosts a follower of
  // the tablet in question, so a tserver holding no replicas (e.g. one that
  // just joined) can never receive leadership here. Such a tserver can drag the
  // average down, but at worst the pass makes no move for it rather than
  // shuffling leadership back and forth.
  int64_t total_leaders = 0;
  for (const auto& uuid : eligible_uuids) {
    total_leaders += FindWithDefault(*global_leader_count, uuid, 0);
  }
  const int64_t num_eligible = static_cast<int64_t>(eligible_uuids.size());
  const int64_t ceil_avg = (total_leaders + num_eligible - 1) / num_eligible;

  // Fast path: if nobody is above the ceiling there is nothing to correct, so
  // skip the relatively expensive tablet location gathering below. This is the
  // common case once a cluster has converged.
  bool any_overloaded = false;
  for (const auto& uuid : eligible_uuids) {
    if (FindWithDefault(*global_leader_count, uuid, 0) > ceil_avg) {
      any_overloaded = true;
      break;
    }
  }
  if (!any_overloaded) {
    return Status::OK();
  }

  // Per-tablet leadership for a single table, gathered fresh from the catalog.
  struct TabletPlacement {
    string tablet_id;
    string leader_uuid;
    vector<string> follower_uuids;
  };

  // Plan and (in NORMAL mode) execute corrective moves, table by table. The
  // global counts decide who is over/underloaded, while the per-table leader
  // counts keep each affected table within its own floor/ceil: a leader is
  // only moved from a tserver holding more of this table's leaders than the
  // destination, so the table itself does not become skewed.
  int remaining_moves = FLAGS_leader_rebalancing_max_moves_per_round;
  int global_moves_scheduled = 0;
  for (const auto& table_info : table_infos) {
    if (remaining_moves <= 0) {
      break;
    }
    TableMetadataLock table_l(table_info.get(), LockMode::READ);
    const SysTablesEntryPB& table_data = table_info->metadata().state().pb;
    const int replication_factor = table_data.num_replicas();
    // Removed tables aren't rebalanced, and a single replica tablet has no
    // follower to hand leadership to.
    if (table_data.state() == SysTablesEntryPB::REMOVED || replication_factor <= 1) {
      continue;
    }

    vector<TabletPlacement> placements;
    // tserver uuid -> number of this table's leaders it currently holds.
    map<string, int> table_leader_count_by_ts;
    map<string, HostPort> host_port_by_leader_ts_uuid;

    vector<scoped_refptr<TabletInfo>> tablet_infos;
    table_info->GetAllTablets(&tablet_infos);
    for (const auto& tablet : tablet_infos) {
      TabletMetadataLock tablet_l(tablet.get(), LockMode::READ);

      TabletLocationsPB locs_pb;
      CatalogManager::TSInfosDict ts_infos_dict;
      {
        CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager_);
        RETURN_NOT_OK(leaderlock.first_failed_status());
        RETURN_NOT_OK(catalog_manager_->GetTabletLocations(
            tablet->id(),
            ReplicaTypeFilter::VOTER_REPLICA,
            /*use_external_addr=*/false,
            &locs_pb,
            &ts_infos_dict,
            nullopt));
      }

      TabletPlacement placement;
      placement.tablet_id = tablet->id();
      for (const auto& r : locs_pb.interned_replicas()) {
        const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs()[r.ts_info_idx()]);
        const string& uuid = ts_info.permanent_uuid();
        if (r.role() == RaftPeerPB::LEADER) {
          placement.leader_uuid = uuid;
          table_leader_count_by_ts[uuid]++;
          InsertIfNotPresent(
              &host_port_by_leader_ts_uuid, uuid, HostPortFromPB(ts_info.rpc_addresses(0)));
        } else if (r.role() == RaftPeerPB::FOLLOWER) {
          placement.follower_uuids.emplace_back(uuid);
        }
      }
      // A tablet with no observed leader can't be a move source.
      if (!placement.leader_uuid.empty()) {
        placements.emplace_back(std::move(placement));
      }
    }

    // Plan this table's corrective transfers, <tablet_id, <from_uuid, to_uuid>>.
    map<string, pair<string, string>> leader_transfer_tasks;
    for (const auto& placement : placements) {
      if (remaining_moves <= 0) {
        break;
      }
      const string& source_uuid = placement.leader_uuid;
      // Only drain tservers that are above the global ceiling.
      if (FindWithDefault(*global_leader_count, source_uuid, 0) <= ceil_avg) {
        continue;
      }
      // Skip degraded tablets that lack a full set of voters; per-table
      // balancing avoids them too.
      if (placement.follower_uuids.size() + 1 < replication_factor) {
        continue;
      }
      const int source_table_count = FindWithDefault(table_leader_count_by_ts, source_uuid, 0);

      // Pick the most underloaded follower that can take a leader without
      // skewing this table. Ties are broken by the smaller uuid so the result
      // does not depend on the order replicas happen to come back in.
      string dest_uuid;
      int dest_global_count = 0;
      for (const auto& follower_uuid : placement.follower_uuids) {
        if (ContainsKey(exclude_dest_uuids, follower_uuid)) {
          continue;
        }
        const int follower_global_count =
            FindWithDefault(*global_leader_count, follower_uuid, 0);
        // The destination must have room below the global ceiling ...
        if (follower_global_count >= ceil_avg) {
          continue;
        }
        // ... and must hold strictly fewer of this table's leaders than the
        // source, so handing it one keeps the table within its own floor/ceil.
        if (FindWithDefault(table_leader_count_by_ts, follower_uuid, 0) >= source_table_count) {
          continue;
        }
        if (dest_uuid.empty() ||
            follower_global_count < dest_global_count ||
            (follower_global_count == dest_global_count && follower_uuid < dest_uuid)) {
          dest_uuid = follower_uuid;
          dest_global_count = follower_global_count;
        }
      }
      if (dest_uuid.empty()) {
        continue;
      }

      if (PREDICT_FALSE(mode == AutoLeaderRebalancerTask::ExecuteMode::TEST)) {
        // In test mode we only want to learn whether the cluster is globally
        // balanced, so report the first corrective move that would be made
        // without planning further or mutating the caller's counts.
        return Status::IllegalState(Substitute(
            "global leader rebalance would move a leader off $0 for table $1",
            source_uuid, table_data.name()));
      }

      // Update the counts as if the move has already happened so later tablets
      // in this round see a consistent picture. The execution below may end up
      // skipping a transfer (e.g. the destination enters maintenance mode or
      // the leader can no longer be looked up), so these counts can drift from
      // what actually transferred; that is fine, the next round rebuilds the
      // distribution from scratch.
      leader_transfer_tasks.insert(
          {placement.tablet_id, pair<string, string>(source_uuid, dest_uuid)});
      (*global_leader_count)[source_uuid]--;
      (*global_leader_count)[dest_uuid]++;
      table_leader_count_by_ts[source_uuid]--;
      table_leader_count_by_ts[dest_uuid]++;
      remaining_moves--;
    }

    if (leader_transfer_tasks.empty()) {
      continue;
    }
    global_moves_scheduled += leader_transfer_tasks.size();

    for (const auto& task : leader_transfer_tasks) {
      const string& tablet_id = task.first;
      const string& leader_uuid = task.second.first;
      const string& dest_uuid = task.second.second;
      if (PREDICT_FALSE(TServerStatePB::MAINTENANCE_MODE ==
                        ts_manager_->GetTServerState(dest_uuid))) {
        continue;
      }
      auto* host_port = FindOrNull(host_port_by_leader_ts_uuid, leader_uuid);
      if (!host_port) {
        continue;
      }
      shared_ptr<TSDescriptor> leader_desc;
      if (!ts_manager_->LookupTSByUUID(leader_uuid, &leader_desc)) {
        continue;
      }

      LeaderStepDownRequestPB request;
      request.set_dest_uuid(leader_uuid);
      request.set_tablet_id(tablet_id);
      request.set_mode(LeaderStepDownMode::GRACEFUL);
      request.set_new_leader_uuid(dest_uuid);

      LeaderStepDownResponsePB response;
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_leader_rebalancing_rpc_timeout_seconds));

      // This pass is best-effort: a transfer may fail transiently (for example
      // "already in progress" if a previous round's move for this tablet has
      // not settled yet). Log and move on rather than aborting the round, since
      // the next round re-evaluates the distribution from scratch.
      vector<Sockaddr> resolved;
      Status s = host_port->ResolveAddresses(&resolved);
      if (!s.ok()) {
        WARN_NOT_OK(s, Substitute("global leader rebalance: cannot resolve $0", leader_uuid));
        continue;
      }
      ConsensusServiceProxy proxy(messenger_, resolved[0], host_port->host());
      s = proxy.LeaderStepDown(request, &response, &rpc);
      if (!s.ok()) {
        WARN_NOT_OK(s, Substitute(
            "global leader rebalance: leader step down for tablet $0 failed", tablet_id));
        continue;
      }
      if (!response.has_error()) {
        VLOG(1) << Substitute(
            "global leader rebalance transfer table: $0, tablet_id: $1, from: $2 to: $3",
            table_data.name(), tablet_id, leader_uuid, dest_uuid);
      } else {
        LOG(WARNING) << Substitute(
            "global leader rebalance: transfer for tablet $0 (from $1 to $2) failed: $3",
            tablet_id, leader_uuid, dest_uuid, response.error().ShortDebugString());
      }
    }
  }

  // In TEST mode the method returns early above as soon as a move is found, so
  // reaching here means nothing was scheduled and this is a no-op increment.
  moves_scheduled_this_round_for_test_ += global_moves_scheduled;
  VLOG(0) << Substitute("global leader rebalance finished, scheduled $0 move(s)",
                        global_moves_scheduled);
  return Status::OK();
}

Status AutoLeaderRebalancerTask::RunLeaderRebalancer() {
  std::lock_guard guard(running_mutex_);

  // If catalog manager isn't initialized or isn't the leader, don't do leader
  // rebalancing. Putting the auto-rebalancer to sleep shouldn't affect the
  // master's ability to become the leader. When the thread wakes up and
  // discovers it is now the leader, then it can begin auto-rebalancing.
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
    if (!l.first_failed_status().ok()) {
      moves_scheduled_this_round_for_test_ = 0;
      return Status::OK();
    }
  }

  number_of_loop_iterations_for_test_++;

  // Leader balance need not disk capacity, so
  // we get all tserver uuids
  TSDescriptorVector descriptors;
  ts_manager_->GetAllDescriptors(&descriptors);
  if (PREDICT_FALSE(descriptors.empty())) {
    VLOG(1) << "No tserver registered for now, skipping this leader rebalancing round.";
    return Status::OK();
  }
  vector<string> tserver_uuids;
  for (const auto& e : descriptors) {
    if (e->PresumedDead()) {
      continue;
    }
    tserver_uuids.emplace_back(e->permanent_uuid());
  }

  // Avoid transferring leaders to tservers that are in MAINTENANCE_MODE.
  auto tserver_state_by_uuid = ts_manager_->GetTServerStates();
  unordered_set<string> exclude_dest_uuids;
  for (const auto& uuid_state : tserver_state_by_uuid) {
    if (uuid_state.second.first == TServerStatePB::MAINTENANCE_MODE) {
      exclude_dest_uuids.insert(uuid_state.first);
    }
  }
  vector<scoped_refptr<TableInfo>> table_infos;
  {
    CatalogManager::ScopedLeaderSharedLock leader_lock(catalog_manager_);
    RETURN_NOT_OK(leader_lock.first_failed_status());
    if (FLAGS_leader_rebalancing_ignore_soft_deleted_tables) {
      catalog_manager_->GetNormalizedTables(&table_infos);
    } else {
      catalog_manager_->GetAllTables(&table_infos);
    }
  }

  // Start the global map at 0 for every live tserver. As each table is processed
  // we add its real leader counts and apply any planned moves, so later tables
  // can see the leader load across all tables when they need to break a tie.
  unordered_map<string, int> global_leader_count_by_ts_uuid;
  for (const auto& uuid : tserver_uuids) {
    global_leader_count_by_ts_uuid[uuid] = 0;
  }

  int per_table_moves_scheduled = 0;
  for (const auto& table_info : table_infos) {
    RETURN_NOT_OK(RunLeaderRebalanceForTable(
        table_info, tserver_uuids, exclude_dest_uuids, &global_leader_count_by_ts_uuid,
        AutoLeaderRebalancerTask::ExecuteMode::NORMAL, &per_table_moves_scheduled));
  }
  // Every table is now balanced on its own, but the cluster can still be
  // globally skewed if one tserver kept getting the ceiling allocation across
  // tables (e.g. many single-tablet tables whose lone leader landed on it).
  // Run a global pass to shed leaders from globally overloaded tservers onto
  // underloaded ones without pushing any individual table out of balance.
  //
  // Only do this once per-table balancing has nothing left to do this round.
  // While per-table moves are still being scheduled the cluster is mid-flight:
  // running the global pass now would re-target tablets the per-table loop just
  // moved (the leadership transfers are asynchronous), and a follow-up round
  // does the global correction with a consistent view instead.
  //
  // TODO(gabriellalotz): if per-table balancing keeps finding work every round
  // (e.g. from continuous leader elections), the global pass can be starved.
  // Emit a metric when it's skipped for this reason so the situation is
  // diagnosable.
  if (per_table_moves_scheduled == 0) {
    RETURN_NOT_OK(RunGlobalLeaderRebalance(
        table_infos, tserver_uuids, exclude_dest_uuids, &global_leader_count_by_ts_uuid));
  }
  // @TODO(duyuqi)
  // Enrich the log and add metrics for leader rebalancer.
  LOG(INFO) << "All tables' leader rebalancing finished this round";
  return Status::OK();
}

void AutoLeaderRebalancerTask::RunLoop() {
  while (
      !shutdown_.WaitFor(MonoDelta::FromSeconds(FLAGS_auto_leader_rebalancing_interval_seconds))) {
    if (FLAGS_auto_leader_rebalancing_enabled) {
      WARN_NOT_OK(RunLeaderRebalancer(),
                  Substitute("the master instance isn't leader"));
    }
  }
}

}  // namespace master
}  // namespace kudu
