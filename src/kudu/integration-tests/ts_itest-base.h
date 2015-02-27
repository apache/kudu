// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_INTEGRATION_TESTS_ITEST_UTIL_H_
#define KUDU_INTEGRATION_TESTS_ITEST_UTIL_H_

#include <boost/foreach.hpp>
#include <string>
#include <utility>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/master/master.proxy.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"

DECLARE_int32(consensus_rpc_timeout_ms);

DEFINE_string(ts_flags, "", "Flags to pass through to tablet servers");
DEFINE_string(master_flags, "", "Flags to pass through to masters");

DEFINE_int32(num_tablet_servers, 3, "Number of tablet servers to start");
DEFINE_int32(num_replicas, 3, "Number of replicas per tablet server");

namespace kudu {
namespace tserver {

using consensus::OpId;
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::TabletLocationsPB;
using metadata::QuorumPeerPB;
using rpc::RpcController;
using std::vector;
using std::string;
using std::tr1::unordered_set;
using strings::Substitute;

static const int kMaxRetries = 20;

// A base for tablet server integration tests.
class TabletServerIntegrationTestBase : public TabletServerTestBase {
 public:

  TabletServerIntegrationTestBase() : random_(SeedRandom()) {}

  struct TServerDetails {
    NodeInstancePB instance_id;
    master::TSRegistrationPB registration;
    gscoped_ptr<TabletServerServiceProxy> tserver_proxy;
    gscoped_ptr<TabletServerAdminServiceProxy> tserver_admin_proxy;
    gscoped_ptr<consensus::ConsensusServiceProxy> consensus_proxy;
    ExternalTabletServer* external_ts;

    string ToString() {
      return Substitute("TabletServer: $0, Rpc address: $1",
                        instance_id.permanent_uuid(),
                        registration.rpc_addresses(0).ShortDebugString());
    }
   };

  typedef std::tr1::unordered_multimap<string, TServerDetails*> TabletReplicaMap;
  typedef std::tr1::unordered_map<string, TServerDetails*> TabletServerMap;

  void SetUp() OVERRIDE {
    TabletServerTestBase::SetUp();
  }

  void AddExtraFlags(const string& flags_str, vector<string>* flags) {
    if (flags_str.empty()) {
      return;
    }
    vector<string> split_flags = strings::Split(flags_str, " ");
    BOOST_FOREACH(const string& flag, split_flags) {
      flags->push_back(flag);
    }
  }

  void CreateCluster(const string& data_root_path,
                     const vector<std::string>& non_default_ts_flags,
                     const vector<std::string>& non_default_master_flags) {

    LOG(INFO) << "Starting cluster with:";
    LOG(INFO) << "--------------";
    LOG(INFO) << FLAGS_num_tablet_servers << " tablet servers";
    LOG(INFO) << FLAGS_num_replicas << " replicas per TS";
    LOG(INFO) << "--------------";

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = FLAGS_num_tablet_servers;
    opts.data_root = GetTestPath(data_root_path);

    // If the caller passed no flags use the default ones, where we stress consensus by setting
    // low timeouts and frequent cache misses.
    if (non_default_ts_flags.empty()) {
      opts.extra_tserver_flags.push_back("--log_cache_size_soft_limit_mb=5");
      opts.extra_tserver_flags.push_back("--log_cache_size_hard_limit_mb=10");
      opts.extra_tserver_flags.push_back(strings::Substitute("--consensus_rpc_timeout_ms=$0",
                                                             FLAGS_consensus_rpc_timeout_ms));
    } else {
      BOOST_FOREACH(const std::string& flag, non_default_ts_flags) {
        opts.extra_tserver_flags.push_back(flag);
      }
    }
    BOOST_FOREACH(const std::string& flag, non_default_master_flags) {
      opts.extra_master_flags.push_back(flag);
    }

    AddExtraFlags(FLAGS_ts_flags, &opts.extra_tserver_flags);
    AddExtraFlags(FLAGS_master_flags, &opts.extra_master_flags);

    cluster_.reset(new ExternalMiniCluster(opts));
    CHECK_OK(cluster_->Start());
    CreateTSProxies();
  }

  // Creates TSServerDetails instance for each TabletServer and stores them
  // in 'tablet_servers_'.
  void CreateTSProxies() {
    master::ListTabletServersRequestPB req;
    master::ListTabletServersResponsePB resp;
    rpc::RpcController controller;

    CHECK_OK(cluster_->master_proxy()->ListTabletServers(req, &resp, &controller));
    CHECK_OK(controller.status());
    CHECK(!resp.has_error()) << "Response had an error: " << resp.error().ShortDebugString();
    CHECK(tablet_servers_.empty());

    BOOST_FOREACH(const master::ListTabletServersResponsePB_Entry& entry, resp.servers()) {
      HostPort host_port;
      CHECK_OK(HostPortFromPB(entry.registration().rpc_addresses(0), &host_port));
      vector<Sockaddr> addresses;
      host_port.ResolveAddresses(&addresses);

      gscoped_ptr<TServerDetails> peer(new TServerDetails);
      peer->instance_id.CopyFrom(entry.instance_id());
      peer->registration.CopyFrom(entry.registration());

      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        if (cluster_->tablet_server(i)->instance_id().permanent_uuid() ==
            entry.instance_id().permanent_uuid()) {
          peer->external_ts = cluster_->tablet_server(i);
        }
      }

      CHECK_OK(CreateClientProxies(addresses[0],
                                   &peer->tserver_proxy,
                                   &peer->tserver_admin_proxy,
                                   &peer->consensus_proxy));

      InsertOrDie(&tablet_servers_, peer->instance_id.permanent_uuid(), peer.get());
      ignore_result(peer.release());
    }
  }

  // Waits that all replicas for a all tablets of 'kTableId' table are online
  // and creates the tablet_replicas_ map.
  void WaitForReplicasAndUpdateLocations() {
    int num_retries = 0;

    bool replicas_missing = true;
    do {
      std::tr1::unordered_multimap<string, TServerDetails*> tablet_replicas;
      GetTableLocationsRequestPB req;
      GetTableLocationsResponsePB resp;
      RpcController controller;
      req.mutable_table()->set_table_name(kTableId);
      controller.set_timeout(MonoDelta::FromSeconds(1));
      CHECK_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
      CHECK_OK(controller.status());
      CHECK(!resp.has_error()) << "Response had an error: " << resp.error().ShortDebugString();

      BOOST_FOREACH(const master::TabletLocationsPB& location, resp.tablet_locations()) {
        BOOST_FOREACH(const master::TabletLocationsPB_ReplicaPB& replica, location.replicas()) {
          TServerDetails* server = FindOrDie(tablet_servers_, replica.ts_info().permanent_uuid());
          tablet_replicas.insert(pair<string, TServerDetails*>(location.tablet_id(), server));
        }

        if (tablet_replicas.count(location.tablet_id()) < FLAGS_num_replicas) {
          LOG(WARNING)<< "Couldn't find the leader and/or replicas. Location: "
              << location.ShortDebugString();
          replicas_missing = true;
          SleepFor(MonoDelta::FromSeconds(1));
          num_retries++;
          break;
        }

        replicas_missing = false;
      }
      if (!replicas_missing) {
        tablet_replicas_ = tablet_replicas;
      }
    } while (replicas_missing && num_retries < kMaxRetries);
  }

  // Returns:
  // Status::OK() if the replica is alive and leader of the quorum.
  // Status::NotFound() if the replica is not part of the quorum or is dead.
  // Status::IllegalState() if the replica is live but not the leader.
  Status GetReplicaStatusAndCheckIfLeader(const string& tablet_id, TServerDetails* replica) {
    consensus::GetCommittedQuorumRequestPB req;
    consensus::GetCommittedQuorumResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(100));
    req.set_tablet_id(tablet_id);
    if (!replica->consensus_proxy->GetCommittedQuorum(req, &resp, &controller).ok() ||
        resp.has_error()) {
      VLOG(1) << "Error getting quorum from replica: " << replica->instance_id.permanent_uuid();
      return Status::NotFound("Error connecting to replica");
    }
    if (consensus::GetRoleInQuorum(replica->instance_id.permanent_uuid(),
                                   resp.quorum()) == QuorumPeerPB::LEADER) {
      return Status::OK();
    }
    VLOG(1) << "Replica not leader of quorum: " << replica->instance_id.permanent_uuid();
    return Status::IllegalState("Replica found but not leader");
  }

  // Returns the last committed leader of the quorum. Tries to get it from master
  // but then actually tries to the get the committed quorum to make sure.
  TServerDetails* GetLeaderReplicaOrNull(const string& tablet_id) {
    string leader_uuid;
    Status master_found_leader_result = GetTabletLeaderUUIDFromMaster(tablet_id, &leader_uuid);

    // See if the master is up to date. I.e. if it does report a leader and if the
    // replica it reports as leader is still alive and (at least thinks) its still
    // the leader.
    TServerDetails* leader;
    if (master_found_leader_result.ok()) {
      leader = GetReplicaWithUuidOrNull(tablet_id, leader_uuid);
      if (leader && GetReplicaStatusAndCheckIfLeader(tablet_id, leader).ok()) {
        return leader;
      }
    }

    // The replica we got from the master (if any) is either dead or not the leader.
    // Find the actual leader.
    pair<TabletReplicaMap::iterator, TabletReplicaMap::iterator> range =
        tablet_replicas_.equal_range(tablet_id);
    vector<TServerDetails*> replicas_copy;
    for (;range.first != range.second; ++range.first) {
      replicas_copy.push_back((*range.first).second);
    }

    std::random_shuffle(replicas_copy.begin(), replicas_copy.end());
    BOOST_FOREACH(TServerDetails* replica, replicas_copy) {
      if (GetReplicaStatusAndCheckIfLeader(tablet_id, replica).ok()) {
        return replica;
      }
    }
    return NULL;
  }

  Status GetLeaderReplicaWithRetries(const string& tablet_id,
                                     TServerDetails** leader,
                                     int max_attempts = 100) {
    int attempts = 0;
    while (attempts < max_attempts) {
      *leader = GetLeaderReplicaOrNull(tablet_id);
      if (*leader) {
        return Status::OK();
      }
      attempts++;
      SleepFor(MonoDelta::FromMilliseconds(100 * attempts));
    }
    return Status::NotFound("Leader replica not found");
  }

  Status GetTabletLeaderUUIDFromMaster(const std::string& tablet_id, std::string* leader_uuid) {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(100));
    req.mutable_table()->set_table_name(kTableId);

    RETURN_NOT_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
    BOOST_FOREACH(const TabletLocationsPB& loc, resp.tablet_locations()) {
      if (loc.tablet_id() == tablet_id) {
        BOOST_FOREACH(const TabletLocationsPB::ReplicaPB& replica, loc.replicas()) {
          if (replica.role() == QuorumPeerPB::LEADER) {
            *leader_uuid = replica.ts_info().permanent_uuid();
            return Status::OK();
          }
        }
      }
    }
    return Status::NotFound("Unable to find leader for tablet", tablet_id);
  }

  TServerDetails* GetReplicaWithUuidOrNull(const string& tablet_id,
                                           const string& uuid) {
    pair<TabletReplicaMap::iterator, TabletReplicaMap::iterator> range =
        tablet_replicas_.equal_range(tablet_id);
    for (;range.first != range.second; ++range.first) {
      if ((*range.first).second->instance_id.permanent_uuid() == uuid) {
        return (*range.first).second;
      }
    }
    return NULL;
  }

  // Gets the the locations of the quorum and waits until all replicas are available for
  // all tablets.
  void WaitForTSAndQuorum() {
    int num_retries = 0;
    // make sure the replicas are up and find the leader
    while (true) {
      if (num_retries >= kMaxRetries) {
        FAIL() << " Reached max. retries while looking up the quorum.";
      }

      Status status = cluster_->WaitForTabletServerCount(FLAGS_num_tablet_servers,
                                                         MonoDelta::FromSeconds(5));
      if (status.IsTimedOut()) {
        LOG(WARNING)<< "Timeout waiting for all replicas to be online, retrying...";
        num_retries++;
        continue;
      }
      break;
    }
    WaitForReplicasAndUpdateLocations();
  }

  // Removes a set of servers from the replicas_ list.
  // Handy for controlling who to validate against after killing servers.
  void PruneFromReplicas(const unordered_set<std::string>& uuids) {
    TabletReplicaMap::iterator iter = tablet_replicas_.begin();
    while (iter != tablet_replicas_.end()) {
      if (uuids.count((*iter).second->instance_id.permanent_uuid()) != 0) {
        iter = tablet_replicas_.erase(iter);
        continue;
      }
      ++iter;
    }

    BOOST_FOREACH(const string& uuid, uuids) {
      delete EraseKeyReturnValuePtr(&tablet_servers_, uuid);
    }
  }

  void GetOnlyLiveFollowerReplicas(const string& tablet_id, vector<TServerDetails*>* followers) {
    followers->clear();
    TServerDetails* leader;
    CHECK_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));

    vector<TServerDetails*> replicas;
    pair<TabletReplicaMap::iterator, TabletReplicaMap::iterator> range =
        tablet_replicas_.equal_range(tablet_id);
    for (;range.first != range.second; ++range.first) {
      replicas.push_back((*range.first).second);
    }

    BOOST_FOREACH(TServerDetails* replica, replicas) {
      if (leader != NULL &&
          replica->instance_id.permanent_uuid() == leader->instance_id.permanent_uuid()) {
        continue;
      }
      if (GetReplicaStatusAndCheckIfLeader(tablet_id, replica).IsIllegalState()) {
        followers->push_back(replica);
      }
    }
  }

  // Gets a vector containing the latest OpId for each of the given replicas.
  // Returns a bad Status if any replica cannot be reached.
  Status GetLastOpIdForEachReplica(const string& tablet_id,
                                   const vector<TServerDetails*>& replicas,
                                   vector<OpId>* op_ids) {
    consensus::GetLastOpIdRequestPB opid_req;
    consensus::GetLastOpIdResponsePB opid_resp;
    opid_req.set_tablet_id(tablet_id);
    RpcController controller;

    op_ids->clear();
    BOOST_FOREACH(TServerDetails* ts, replicas) {
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(3));
      opid_resp.Clear();
      opid_req.set_tablet_id(tablet_id);
      RETURN_NOT_OK_PREPEND(
        ts->consensus_proxy->GetLastOpId(opid_req, &opid_resp, &controller),
        Substitute("Failed to fetch last op id from $0", ts->instance_id.ShortDebugString()));
      op_ids->push_back(opid_resp.opid());
    }

    return Status::OK();
  }

  // Return the index within 'replicas' for the replica which is farthest ahead.
  int64_t GetFurthestAheadReplicaIdx(const string& tablet_id,
                                     const vector<TServerDetails*>& replicas) {
    vector<OpId> op_ids;
    CHECK_OK(GetLastOpIdForEachReplica(tablet_id, replicas, &op_ids));

    uint64 max_index = 0;
    int max_replica_index = -1;
    for (int i = 0; i < op_ids.size(); i++) {
      if (op_ids[i].index() > max_index) {
        max_index = op_ids[i].index();
        max_replica_index = i;
      }
    }

    CHECK_NE(max_replica_index, -1);

    return max_replica_index;
  }

  Status KillServerWithUUID(const std::string& uuid) {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ExternalTabletServer* ts = cluster_->tablet_server(i);
      if (ts->instance_id().permanent_uuid() == uuid) {
        ts->Shutdown();
        return Status::OK();
      }
    }
    return Status::NotFound("Unable to find server with UUID", uuid);
  }

  // Since we're fault-tolerant we might mask when a tablet server is
  // dead. This returns Status::IllegalState() if fewer than 'num_tablet_servers'
  // are alive.
  Status CheckTabletServersAreAlive(int num_tablet_servers) {
    int live_count = 0;
    string error = Substitute("Fewer than $0 TabletServers were alive. Dead TSs: ",
                              num_tablet_servers);
    RpcController controller;
    BOOST_FOREACH(const TabletServerMap::value_type& entry, tablet_servers_) {
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(1));
      PingRequestPB req;
      PingResponsePB resp;
      Status s = entry.second->tserver_proxy->Ping(req, &resp, &controller);
      if (!s.ok()) {
        error += "\n" + entry.second->ToString() +  " (" + s.ToString() + ")";
        continue;
      }
      live_count++;
    }
    if (live_count < num_tablet_servers) {
      return Status::IllegalState(error);
    }
    return Status::OK();
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
    STLDeleteValues(&tablet_servers_);
  }


 protected:
  gscoped_ptr<ExternalMiniCluster> cluster_;
  // Maps server uuid to TServerDetails
  TabletServerMap tablet_servers_;
  // Maps tablet to all replicas.
  TabletReplicaMap tablet_replicas_;

  ThreadSafeRandom random_;
};


}  // namespace tserver
}  // namespace kudu

#endif /* SRC_KUDU_INTEGRATION_TESTS_ITEST_UTIL_H_ */
