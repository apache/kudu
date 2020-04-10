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

#include "kudu/tools/tool_replica_util.h"

#include <cstdint>
#include <fstream>  // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

using kudu::MonoDelta;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTablet;
using kudu::client::KuduTabletServer;
using kudu::consensus::ADD_PEER;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigType;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::consensus::GetLastOpIdRequestPB;
using kudu::consensus::GetLastOpIdResponsePB;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::MODIFY_PEER;
using kudu::consensus::OpId;
using kudu::consensus::REMOVE_PEER;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::ReplicaManagementInfoPB;
using kudu::rpc::RpcController;
using std::cerr;
using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

Status GetLastCommittedOpId(const string& tablet_id,
                            const string& replica_uuid,
                            const HostPort& replica_hp,
                            const MonoDelta& timeout,
                            OpId* opid) {
  GetLastOpIdRequestPB req;
  GetLastOpIdResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);
  req.set_tablet_id(tablet_id);
  req.set_dest_uuid(replica_uuid);
  req.set_opid_type(consensus::COMMITTED_OPID);

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(replica_hp.host(), replica_hp.port(), &proxy));
  RETURN_NOT_OK(proxy->GetLastOpId(req, &resp, &controller));
  *opid = resp.opid();
  return Status::OK();
}

} // anonymous namespace

Status GetConsensusState(const unique_ptr<ConsensusServiceProxy>& proxy,
                         const string& tablet_id,
                         const string& replica_uuid,
                         const MonoDelta& timeout,
                         ConsensusStatePB* consensus_state,
                         bool* is_3_4_3_replication) {
  GetConsensusStateRequestPB req;
  GetConsensusStateResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);
  req.set_dest_uuid(replica_uuid);
  req.add_tablet_ids(tablet_id);
  RETURN_NOT_OK(proxy->GetConsensusState(req, &resp, &controller));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.tablets_size() == 0) {
    return Status::NotFound("tablet not found:", tablet_id);
  }
  DCHECK_EQ(1, resp.tablets_size());
  if (consensus_state) {
    *consensus_state = resp.tablets(0).cstate();
  }
  if (is_3_4_3_replication) {
    *is_3_4_3_replication = resp.replica_management_info().replacement_scheme() ==
        ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION;
  }
  return Status::OK();
}

Status DoLeaderStepDown(const string& tablet_id,
                        const string& leader_uuid,
                        const HostPort& leader_hp,
                        LeaderStepDownMode mode,
                        const boost::optional<string>& new_leader_uuid,
                        const MonoDelta& timeout) {
  if (mode == LeaderStepDownMode::ABRUPT && new_leader_uuid) {
    return Status::InvalidArgument(
        "cannot specify a new leader uuid for an abrupt stepdown");
  }

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  consensus::LeaderStepDownRequestPB req;
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(tablet_id);
  req.set_mode(mode);
  if (new_leader_uuid) {
    req.set_new_leader_uuid(new_leader_uuid.get());
  }

  RpcController rpc;
  rpc.set_timeout(timeout);
  consensus::LeaderStepDownResponsePB resp;
  RETURN_NOT_OK(proxy->LeaderStepDown(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status GetTabletLeader(const client::sp::shared_ptr<KuduClient>& client,
                       const string& tablet_id,
                       string* leader_uuid,
                       HostPort* leader_hp,
                       bool* is_no_leader) {
  KuduTablet* tablet_raw = nullptr;
  RETURN_NOT_OK(client->GetTablet(tablet_id, &tablet_raw));
  unique_ptr<KuduTablet> tablet(tablet_raw);

  for (const auto* r : tablet->replicas()) {
    if (r->is_leader()) {
      *leader_uuid = r->ts().uuid();
      leader_hp->set_host(r->ts().hostname());
      leader_hp->set_port(r->ts().port());
      return Status::OK();
    }
  }
  if (is_no_leader) {
    *is_no_leader = true;
  }

  return Status::NotFound(Substitute("No leader replica found for tablet $0",
                                     tablet_id));
}

// For the target (i.e. newly added replica) we have the following options:
//
//  * The tablet copy succeeds and the replica successfully bootstraps and
//    starts, so the tablet configuration contains the desired target replica.
//
//  * The newly added replica fails to copy the data or any other failure
//    happens during bootstrap or any other phase. In that case, the system
//    eventually kicks out the failed replica, so the target replica
//    will not be in the config.
//
// The former case and the absence of the source replica in the configuration
// signals about successful completion of the replica movement operation.
// The latter case manifests a failure of the replica movement operation.
Status CheckCompleteMove(const vector<string>& master_addresses,
                         const client::sp::shared_ptr<client::KuduClient>& client,
                         const string& tablet_id,
                         const string& from_ts_uuid,
                         const string& to_ts_uuid,
                         bool* is_complete,
                         Status* completion_status) {
  DCHECK(is_complete);
  DCHECK(completion_status);
  *is_complete = false;

  // Get the latest leader info. It may change later, due to our actions or
  // outside factors.
  string orig_leader_uuid;
  HostPort orig_leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id,
                                &orig_leader_uuid, &orig_leader_hp));
  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(orig_leader_hp.host(), orig_leader_hp.port(), &proxy));

  // Check if the replica with UUID 'to_ts_uuid' is in the config, and if it has
  // been promoted to voter.
  ConsensusStatePB cstate;
  bool is_343_scheme;
  RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, orig_leader_uuid,
                                  client->default_admin_operation_timeout(),
                                  &cstate, &is_343_scheme));
  bool to_ts_uuid_in_config = false;
  bool to_ts_uuid_is_a_voter = false;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == to_ts_uuid) {
      to_ts_uuid_in_config = true;
      if (peer.member_type() == RaftPeerPB::VOTER) {
        to_ts_uuid_is_a_voter = true;
      }
      break;
    }
  }

  // The failure case: the newly added replica is no longer in the config.
  // Maybe something was wrong with the replica and the system kicked it out?
  if (!to_ts_uuid.empty() && !to_ts_uuid_in_config) {
    *is_complete = true;
    *completion_status = Status::Incomplete(Substitute(
        "tablet $0, TS $1 -> TS $2 move failed, target replica disappeared",
        tablet_id, from_ts_uuid, to_ts_uuid));
    return Status::OK();
  }

  // Check if the replica slated for removal (the one with UUID 'from_ts_uuid')
  // is still in the config.
  bool from_ts_uuid_in_config = false;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == from_ts_uuid) {
      // Sanity check: in case of 3-4-3 replication mode, the source replica
      // must have the REPLACE attribute set. Otherwise, something has changed
      // in the middle and the source replica will never be evicted,
      // so it does not make sense to await its removal.
      if (is_343_scheme && !peer.attrs().replace()) {
        return Status::IllegalState(Substitute(
            "$0: source replica $1 does not have REPLACE attribute set",
            tablet_id, from_ts_uuid));
      }

      // Handle the case when the replica-to-be-removed (the one with UUID
      // 'from_ts_uuid') is the leader.
      // - It's possible that leadership changed and 'orig_leader_uuid' is not
      //   the leader's UUID by the time 'cstate' was collected. Let's
      //   cross-reference the two sources and only act if they agree.
      // - It doesn't make sense to have the leader step down if the newly-added
      //   replica hasn't been promoted to a voter yet, since changing
      //   leadership can only delay that process and the stepped-down leader
      //   replica will not be evicted until the newly added replica is promoted
      //   to voter.
      // - When using the 3-4-3 replica management scheme, the leader master
      //   will handle removing replicas, but when using the 3-2-3 scheme we
      //   have to do it, so we only do the stepdown when the tablet is healthy
      //   and we can proceed to kick out 'from_ts_uuid' once a new leader
      //   is elected.
      if (orig_leader_uuid == from_ts_uuid &&
          orig_leader_uuid == cstate.leader_uuid() &&
          (to_ts_uuid_is_a_voter || to_ts_uuid.empty()) &&
          (is_343_scheme || DoKsckForTablet(master_addresses, tablet_id).ok())) {
        // The leader is the node we intend to remove; make it step down.
        ignore_result(DoLeaderStepDown(tablet_id, orig_leader_uuid, orig_leader_hp,
                                       LeaderStepDownMode::GRACEFUL, boost::none,
                                       client->default_admin_operation_timeout()));
      }
      from_ts_uuid_in_config = true;
      break;
    }
  }

  // If we are operating under the 3-4-3 replica management scheme, the
  // newly-added replica has been promoted to a voter, and (if it was leader)
  // the replica-to-be-removed has stepped down as leader, then the move is
  // complete. The leader master will take care of removing the extra replica.
  if (is_343_scheme) {
    if (!from_ts_uuid_in_config &&
        (to_ts_uuid_is_a_voter || to_ts_uuid.empty())) {
      *is_complete = true;
      *completion_status = Status::OK();
    }
    return Status::OK();
  }

  // The 3-2-3 scheme requires explicitly removing the source replica since the
  // leader master doesn't take care of over-replicated tablets. Once the newly
  // added replica is caught up and ready (ksck returned OK), it's time to
  // remove the source replica. Once the replica is gone, it will be detected
  // next on the next retry of this function.
  if (from_ts_uuid_in_config &&
      DoKsckForTablet(master_addresses, tablet_id).ok()) {

    // Re-find the leader in case it changed (we may have caused it to change).
    string new_leader_uuid;
    HostPort new_leader_hp;
    RETURN_NOT_OK(GetTabletLeader(client, tablet_id,
                                  &new_leader_uuid, &new_leader_hp));
    // If leadership changed, we have to rebuild the proxy to the new leader.
    if (new_leader_uuid != orig_leader_uuid) {
      RETURN_NOT_OK(BuildProxy(new_leader_hp.host(), new_leader_hp.port(), &proxy));
    }

    // We can only act if the leader is not the replica being removed.
    if (from_ts_uuid != new_leader_uuid) {
      // DoChangeConfig() might return InvalidState if the newly elected leader
      // hasn't yet committed a single operation on its term. Let's make sure
      // the current leader has asserted its leadership.
      ConsensusStatePB cstate;
      RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, new_leader_uuid,
                                      client->default_admin_operation_timeout(),
                                      &cstate));
      if (!cstate.has_leader_uuid() || cstate.leader_uuid() != new_leader_uuid) {
        // Something has changed in the middle, the caller of this method
        // (i.e. the upper level) will need to retry.
        *is_complete = false;
        *completion_status = Status::Incomplete(
            Substitute("$0: leader info changed", tablet_id));
        return Status::OK();
      }
      // Make sure the current leader has asserted its leadership before sending
      // it the ChangeConfig request.
      OpId opid;
      RETURN_NOT_OK(GetLastCommittedOpId(tablet_id, new_leader_uuid, new_leader_hp,
                                         client->default_admin_operation_timeout(),
                                         &opid));
      if (opid.term() == cstate.current_term()) {
        bool cas_failed = false;
        const auto s = DoChangeConfig(master_addresses, tablet_id, from_ts_uuid,
                                      boost::none, REMOVE_PEER,
                                      cstate.committed_config().opid_index(),
                                      &cas_failed);
        if (cas_failed) {
          // Something has changed in the configuration, need to retry.
          *is_complete = false;
          *completion_status = s;
          return Status::OK();
        }
        RETURN_NOT_OK(s);
      }
    }
  }

  // The success case: the source replica has gone from the config and the
  // target replica is present as a full-fledged voter.
  if (!from_ts_uuid_in_config &&
      (to_ts_uuid_is_a_voter || to_ts_uuid.empty())) {
    *is_complete = true;
    *completion_status = Status::OK();
  }

  return Status::OK();
}

Status SetReplace(const client::sp::shared_ptr<client::KuduClient>& client,
                  const string& tablet_id,
                  const string& ts_uuid,
                  const boost::optional<int64_t>& cas_opid_idx,
                  bool* cas_failed) {
  // Safely set the 'cas_failed' output parameter to 'false' to cover an earlier
  // return due to an error.
  if (cas_failed) {
    *cas_failed = false;
  }
  // Find this tablet's leader replica. We need its UUID and RPC address.
  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));
  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  // Get information on current replication scheme: the move scenario depends
  // on the replication scheme used.
  bool is_343_scheme;
  ConsensusStatePB cstate;
  RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, leader_uuid,
                                  client->default_admin_operation_timeout(),
                                  &cstate, &is_343_scheme));
  // The 3-2-3 replica management scheme (pre-KUDU-1097) does not process
  // the attribute as expected.
  if (!is_343_scheme) {
    return Status::ConfigurationError(
        "cluster is running in 3-2-3 management scheme");
  }

  // Check whether the REPLACE attribute is already set for the source replica.
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == ts_uuid && peer.attrs().replace()) {
      // The replica is already marked with the REPLACE attribute.
      return Status::OK();
    }
  }

  BulkChangeConfigRequestPB req;
  auto* change = req.add_config_changes();
  change->set_type(MODIFY_PEER);
  *change->mutable_peer()->mutable_permanent_uuid() = ts_uuid;
  change->mutable_peer()->mutable_attrs()->set_replace(true);
  consensus::ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(client->default_admin_operation_timeout());
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(tablet_id);
  if (cas_opid_idx) {
    req.set_cas_config_opid_index(*cas_opid_idx);
  }
  RETURN_NOT_OK(proxy->BulkChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    if (resp.error().code() == tserver::TabletServerErrorPB::CAS_FAILED &&
        cas_failed) {
      *cas_failed = true;
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status CheckCompleteReplace(const client::sp::shared_ptr<client::KuduClient>& client,
                            const string& tablet_id,
                            const string& ts_uuid,
                            bool* is_complete,
                            Status* completion_status) {
  DCHECK(completion_status);
  DCHECK(is_complete);
  *is_complete = false;
  // Get the latest leader info. It may change later, due to our actions or
  // outside factors.
  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));
  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  ConsensusStatePB cstate;
  bool is_343_scheme;
  RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, leader_uuid,
                                  client->default_admin_operation_timeout(),
                                  &cstate, &is_343_scheme));
  if (!is_343_scheme) {
    return Status::ConfigurationError(
        "cluster is not running in 3-4-3 replica management scheme");
  }

  bool is_all_voters = true;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.member_type() != RaftPeerPB::VOTER) {
      is_all_voters = false;
      break;
    }
  }

  // Check if the replica slated for removal is still in the config.
  bool ts_uuid_in_config = false;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == ts_uuid) {
      ts_uuid_in_config = true;
      if (!peer.attrs().replace()) {
        // Sanity check: the replica must have the REPLACE attribute set.
        // Otherwise, something has changed in the middle and the replica will
        // never be evicted, so it does not make sense to await its removal.
        *is_complete = true;
        *completion_status = Status::IllegalState(Substitute(
            "$0: replica $1 does not have the REPLACE attribute set",
            tablet_id, ts_uuid));
      }
      // There is not much sense demoting current leader if a newly added
      // non-voter hasn't been promoted into voter role yet: the former leader
      // replica will not be evicted prior the new non-voter replica becomes
      // is promoted into voter. Demoting former leader too early might even
      // delay promotion of already caught-up non-leader replica.
      if (is_all_voters &&
          leader_uuid == ts_uuid && leader_uuid == cstate.leader_uuid()) {
        // The leader is the node we intend to remove; make it step down.
        ignore_result(DoLeaderStepDown(tablet_id, leader_uuid, leader_hp,
                                       LeaderStepDownMode::GRACEFUL, boost::none,
                                       client->default_admin_operation_timeout()));
      }
      break;
    }
  }

  if (!ts_uuid_in_config) {
    *is_complete = true;
    *completion_status = Status::OK();
  }
  return Status::OK();
}


Status ScheduleReplicaMove(const vector<string>& master_addresses,
                           const client::sp::shared_ptr<client::KuduClient>& client,
                           const string& tablet_id,
                           const string& from_ts_uuid,
                           const string& to_ts_uuid) {
  // Find this tablet's leader replica. We need its UUID and RPC address.
  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));
  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  // Get information on current replication scheme: the move scenario depends
  // on the replication scheme used.
  bool is_343_scheme;
  ConsensusStatePB cstate;
  RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, leader_uuid,
                                  client->default_admin_operation_timeout(),
                                  &cstate, &is_343_scheme));
  // Sanity check: the target replica should not be present in the config.
  // Anyway, ChangeConfig() RPC would return an error in that case, but this
  // pre-condition allows us to short-circuit that.
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == to_ts_uuid) {
      return Status::IllegalState(Substitute(
          "tablet $0: replica $1 is already present", tablet_id, to_ts_uuid));
    }
  }

  const auto cas_opid_idx = cstate.committed_config().opid_index();

  // The pre- KUDU-1097 way of moving a replica involves first adding a new
  // replica and then evicting the old one.
  if (!is_343_scheme) {
    if (to_ts_uuid.empty()) {
      return Status::OK();
    }
    return DoChangeConfig(master_addresses, tablet_id, to_ts_uuid,
                          RaftPeerPB::VOTER, ADD_PEER, cas_opid_idx);
  }

  // Check whether the REPLACE attribute is already set for the source replica.
  bool is_from_ts_uuid_replace_attr_set = false;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == from_ts_uuid) {
      is_from_ts_uuid_replace_attr_set = peer.attrs().replace();
      break;
    }
  }
  if (is_from_ts_uuid_replace_attr_set && to_ts_uuid.empty()) {
    // Nothing to do: the REPLACE attribute is already set for the source
    // replica and the target tablet server is not specified. So, that pure
    // 'remove replica' configuration change has already been applied. That
    // might happen due to concurrent activity or requesting the same
    // replica movement again.
    return Status::OK();
  }

  // In a post-KUDU-1097 world, the procedure to move a replica is to add the
  // replace=true attribute to the replica to remove while simultaneously
  // adding the replacement as a non-voter with promote=true.
  // The following code implements tablet movement in that paradigm.
  BulkChangeConfigRequestPB req;
  if (!is_from_ts_uuid_replace_attr_set) {
    auto* change = req.add_config_changes();
    change->set_type(MODIFY_PEER);
    *change->mutable_peer()->mutable_permanent_uuid() = from_ts_uuid;
    change->mutable_peer()->mutable_attrs()->set_replace(true);
  }
  if (!to_ts_uuid.empty()) {
    auto* change = req.add_config_changes();
    change->set_type(ADD_PEER);
    *change->mutable_peer()->mutable_permanent_uuid() = to_ts_uuid;
    change->mutable_peer()->set_member_type(RaftPeerPB::NON_VOTER);
    change->mutable_peer()->mutable_attrs()->set_promote(true);
    HostPort hp;
    RETURN_NOT_OK(GetRpcAddressForTS(client, to_ts_uuid, &hp));
    *change->mutable_peer()->mutable_last_known_addr() = HostPortToPB(hp);
  }
  req.set_cas_config_opid_index(cas_opid_idx);

  consensus::ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(client->default_admin_operation_timeout());
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(tablet_id);
  RETURN_NOT_OK(proxy->BulkChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status DoKsckForTablet(const vector<string>& master_addresses,
                       const string& tablet_id) {
  shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK(RemoteKsckCluster::Build(master_addresses, &cluster));

  // Print to an unopened ofstream to discard ksck output.
  // See https://stackoverflow.com/questions/8243743.
  std::ofstream null_stream;
  cluster->set_tablet_id_filters({ tablet_id });
  Ksck ksck(cluster, &null_stream);
  RETURN_NOT_OK(ksck.CheckMasterHealth());
  RETURN_NOT_OK(ksck.CheckMasterConsensus());
  RETURN_NOT_OK(ksck.CheckClusterRunning());
  RETURN_NOT_OK(ksck.FetchTableAndTabletInfo());
  // The return Status is ignored since a tserver that is not the destination
  // nor a host of a replica might be down, and in that case the move should
  // succeed. Problems with the destination tserver or the tablet will still
  // be detected by ksck or other commands.
  ignore_result(ksck.FetchInfoFromTabletServers());
  return ksck.CheckTablesConsistency();
}

Status GetRpcAddressForTS(const client::sp::shared_ptr<KuduClient>& client,
                          const string& uuid,
                          HostPort* hp) {
  vector<KuduTabletServer*> servers;
  ElementDeleter deleter(&servers);
  RETURN_NOT_OK(client->ListTabletServers(&servers));
  for (const auto* s : servers) {
    if (s->uuid() == uuid) {
      hp->set_host(s->hostname());
      hp->set_port(s->port());
      return Status::OK();
    }
  }

  return Status::NotFound(Substitute(
      "server $0 has no RPC address registered with the Master", uuid));
}

Status DoChangeConfig(const vector<string>& master_addresses,
                      const string& tablet_id,
                      const string& replica_uuid,
                      const boost::optional<RaftPeerPB::MemberType>& member_type,
                      ChangeConfigType cc_type,
                      const boost::optional<int64_t>& cas_opid_idx,
                      bool* cas_failed) {
  if (cas_failed) {
    *cas_failed = false;
  }
  if (cc_type == consensus::REMOVE_PEER && member_type) {
    return Status::InvalidArgument("cannot supply Raft member type when removing a server");
  }
  if ((cc_type == consensus::ADD_PEER || cc_type == consensus::MODIFY_PEER) &&
      !member_type) {
    return Status::InvalidArgument(
        "must specify member type when adding a server or changing member type");
  }

  RaftPeerPB peer_pb;
  peer_pb.set_permanent_uuid(replica_uuid);
  if (member_type) {
    peer_pb.set_member_type(*member_type);
  }

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));

  // When adding a new server, we need to provide the server's RPC address.
  if (cc_type == consensus::ADD_PEER) {
    HostPort hp;
    RETURN_NOT_OK(GetRpcAddressForTS(client, replica_uuid, &hp));
    *peer_pb.mutable_last_known_addr() = HostPortToPB(hp);
  }

  // Find this tablet's leader replica. We need its UUID and RPC address.
  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  consensus::ChangeConfigRequestPB req;
  consensus::ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(client->default_admin_operation_timeout());
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(tablet_id);
  req.set_type(cc_type);
  if (cas_opid_idx) {
    req.set_cas_config_opid_index(*cas_opid_idx);
  }
  *req.mutable_server() = peer_pb;
  RETURN_NOT_OK(proxy->ChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    if (resp.error().code() == tserver::TabletServerErrorPB::CAS_FAILED &&
        cas_failed) {
      *cas_failed = true;
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

// This could alternatively be implemented using the GetFlags API, but the
// GetFlags RPC is not supported on all versions with which the rebalancing
// tool would like to be compatible, and this method based on PB fields
// is less fragile than the string matching required to use GetFlags with old
// versions.
Status Is343SchemeCluster(const vector<string>& master_addresses,
                          const boost::optional<string>& tablet_id_in,
                          bool* is_343_scheme) {
  client::sp::shared_ptr<client::KuduClient> client;
  RETURN_NOT_OK(client::KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));
  string tablet_id;
  if (tablet_id_in) {
    tablet_id = *tablet_id_in;
  } else {
    vector<string> table_names;
    RETURN_NOT_OK(client->ListTables(&table_names));
    if (table_names.empty()) {
      return Status::Incomplete("not a single table found");
    }

    const auto& table_name = table_names.front();
    client::sp::shared_ptr<client::KuduTable> client_table;
    RETURN_NOT_OK(client->OpenTable(table_name, &client_table));
    vector<client::KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    client::KuduScanTokenBuilder builder(client_table.get());
    RETURN_NOT_OK(builder.Build(&tokens));
    if (tokens.empty()) {
      return Status::Incomplete(Substitute(
          "table '$0': not a single scan token returned", table_name));
    }
    tablet_id = tokens.front()->tablet().id();
  }

  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));
  unique_ptr<consensus::ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));
  return GetConsensusState(proxy, tablet_id, leader_uuid,
                           client->default_admin_operation_timeout(),
                           nullptr /* consensus_state */, is_343_scheme);
}

} // namespace tools
} // namespace kudu
