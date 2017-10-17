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

#include "kudu/integration-tests/cluster_itest_util.h"

#include <algorithm>
#include <ostream>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <rapidjson/document.h>

#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tablet_copy.proxy.h"
#include "kudu/tserver/tablet_server_test_util.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace itest {

using client::KuduSchema;
using client::KuduSchemaBuilder;
using consensus::ChangeConfigRequestPB;
using consensus::ChangeConfigResponsePB;
using consensus::ConsensusStatePB;
using consensus::CountVoters;
using consensus::GetConsensusStateRequestPB;
using consensus::GetConsensusStateResponsePB;
using consensus::GetLastOpIdRequestPB;
using consensus::GetLastOpIdResponsePB;
using consensus::LeaderStepDownRequestPB;
using consensus::LeaderStepDownResponsePB;
using consensus::OpId;
using consensus::OpIdType;
using consensus::RaftPeerPB;
using consensus::RunLeaderElectionResponsePB;
using consensus::RunLeaderElectionRequestPB;
using consensus::VoteRequestPB;
using consensus::VoteResponsePB;
using consensus::kInvalidOpIdIndex;
using master::ListTabletServersResponsePB;
using master::ListTabletServersResponsePB_Entry;
using master::MasterServiceProxy;
using master::TabletLocationsPB;
using pb_util::SecureDebugString;
using pb_util::SecureShortDebugString;
using rapidjson::Value;
using rpc::Messenger;
using rpc::RpcController;
using std::min;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using tablet::TabletDataState;
using tserver::CreateTsClientProxies;
using tserver::ListTabletsResponsePB;
using tserver::DeleteTabletRequestPB;
using tserver::DeleteTabletResponsePB;
using tserver::BeginTabletCopySessionRequestPB;
using tserver::BeginTabletCopySessionResponsePB;
using tserver::TabletCopyErrorPB;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

const string& TServerDetails::uuid() const {
  return instance_id.permanent_uuid();
}

string TServerDetails::ToString() const {
  return Substitute("TabletServer: $0, Rpc address: $1",
                    instance_id.permanent_uuid(),
                    SecureShortDebugString(registration.rpc_addresses(0)));
}

client::KuduSchema SimpleIntKeyKuduSchema() {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  CHECK_OK(b.Build(&s));
  return s;
}

Status GetLastOpIdForEachReplica(const string& tablet_id,
                                 const vector<TServerDetails*>& replicas,
                                 OpIdType opid_type,
                                 const MonoDelta& timeout,
                                 vector<OpId>* op_ids) {
  GetLastOpIdRequestPB opid_req;
  GetLastOpIdResponsePB opid_resp;
  opid_req.set_tablet_id(tablet_id);
  RpcController controller;

  op_ids->clear();
  for (TServerDetails* ts : replicas) {
    controller.Reset();
    controller.set_timeout(timeout);
    opid_resp.Clear();
    opid_req.set_dest_uuid(ts->uuid());
    opid_req.set_tablet_id(tablet_id);
    opid_req.set_opid_type(opid_type);
    RETURN_NOT_OK_PREPEND(
      ts->consensus_proxy->GetLastOpId(opid_req, &opid_resp, &controller),
      Substitute("Failed to fetch last op id from $0",
                 SecureShortDebugString(ts->instance_id)));
    op_ids->push_back(opid_resp.opid());
  }

  return Status::OK();
}

Status GetLastOpIdForReplica(const std::string& tablet_id,
                             TServerDetails* replica,
                             OpIdType opid_type,
                             const MonoDelta& timeout,
                             consensus::OpId* op_id) {
  vector<OpId> op_ids;
  RETURN_NOT_OK(GetLastOpIdForEachReplica(tablet_id, { replica }, opid_type, timeout, &op_ids));
  CHECK_EQ(1, op_ids.size());
  *op_id = op_ids[0];
  return Status::OK();
}

Status WaitForOpFromCurrentTerm(TServerDetails* replica,
                                const string& tablet_id,
                                OpIdType opid_type,
                                const MonoDelta& timeout,
                                OpId* opid) {
  const MonoTime kStart = MonoTime::Now();
  const MonoTime kDeadline = kStart + timeout;

  Status s;
  while (MonoTime::Now() < kDeadline) {
    ConsensusStatePB cstate;
    s = GetConsensusState(replica, tablet_id, kDeadline - MonoTime::Now(), &cstate);
    if (s.ok()) {
      OpId tmp_opid;
      s = GetLastOpIdForReplica(tablet_id, replica, opid_type, kDeadline - MonoTime::Now(),
                                &tmp_opid);
      if (s.ok()) {
        if (tmp_opid.term() == cstate.current_term()) {
          if (opid) {
            opid->Swap(&tmp_opid);
          }
          return Status::OK();
        }
        s = Status::IllegalState(Substitute("Terms don't match. Current term: $0. Latest OpId: $1",
                                 cstate.current_term(), OpIdToString(tmp_opid)));
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  return Status::TimedOut(Substitute("Timed out after $0 waiting for op from current term: $1",
                                     (MonoTime::Now() - kStart).ToString(),
                                     s.ToString()));
}

Status WaitForServersToAgree(const MonoDelta& timeout,
                             const TabletServerMap& tablet_servers,
                             const string& tablet_id,
                             int64_t minimum_index) {
  const MonoTime deadline = MonoTime::Now() + timeout;

  for (int i = 1; MonoTime::Now() < deadline; i++) {
    vector<TServerDetails*> servers;
    AppendValuesFromMap(tablet_servers, &servers);
    vector<OpId> ids;
    Status s = GetLastOpIdForEachReplica(tablet_id, servers, consensus::RECEIVED_OPID, timeout,
                                         &ids);
    if (s.ok()) {
      bool any_behind = false;
      bool any_disagree = false;
      int64_t cur_index = kInvalidOpIdIndex;
      for (const OpId& id : ids) {
        if (cur_index == kInvalidOpIdIndex) {
          cur_index = id.index();
        }
        if (id.index() != cur_index) {
          any_disagree = true;
          break;
        }
        if (id.index() < minimum_index) {
          any_behind = true;
          break;
        }
      }
      if (!any_behind && !any_disagree) {
        return Status::OK();
      }
    } else {
      LOG(WARNING) << "Got error getting last opid for each replica: " << s.ToString();
    }

    LOG(INFO) << "Not converged past " << minimum_index << " yet: " << ids;
    SleepFor(MonoDelta::FromMilliseconds(min(i * 100, 1000)));
  }
  return Status::TimedOut(Substitute("Index $0 not available on all replicas after $1. ",
                                              minimum_index, timeout.ToString()));
}

// Wait until all specified replicas have logged the given index.
Status WaitUntilAllReplicasHaveOp(const int64_t log_index,
                                  const string& tablet_id,
                                  const vector<TServerDetails*>& replicas,
                                  const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoDelta passed = MonoDelta::FromMilliseconds(0);
  while (true) {
    vector<OpId> op_ids;
    Status s = GetLastOpIdForEachReplica(tablet_id, replicas, consensus::RECEIVED_OPID, timeout,
                                         &op_ids);
    if (s.ok()) {
      bool any_behind = false;
      for (const OpId& op_id : op_ids) {
        if (op_id.index() < log_index) {
          any_behind = true;
          break;
        }
      }
      if (!any_behind) return Status::OK();
    } else {
      LOG(WARNING) << "Got error getting last opid for each replica: " << s.ToString();
    }
    passed = MonoTime::Now() - start;
    if (passed > timeout) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  string replicas_str;
  for (const TServerDetails* replica : replicas) {
    if (!replicas_str.empty()) replicas_str += ", ";
    replicas_str += "{ " + replica->ToString() + " }";
  }
  return Status::TimedOut(Substitute("Index $0 not available on all replicas after $1. "
                                     "Replicas: [ $2 ]",
                                     log_index, passed.ToString(), replicas_str));
}

Status CreateTabletServerMap(const shared_ptr<MasterServiceProxy>& master_proxy,
                             const shared_ptr<Messenger>& messenger,
                             unordered_map<string, TServerDetails*>* ts_map) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  vector<ListTabletServersResponsePB_Entry> tservers;
  RETURN_NOT_OK(ListTabletServers(master_proxy, kTimeout, &tservers));

  ts_map->clear();
  for (const auto& entry : tservers) {
    HostPort host_port;
    RETURN_NOT_OK(HostPortFromPB(entry.registration().rpc_addresses(0), &host_port));
    vector<Sockaddr> addresses;
    host_port.ResolveAddresses(&addresses);

    gscoped_ptr<TServerDetails> peer(new TServerDetails);
    peer->instance_id.CopyFrom(entry.instance_id());
    peer->registration.CopyFrom(entry.registration());

    CreateTsClientProxies(addresses[0],
                          messenger,
                          &peer->tablet_copy_proxy,
                          &peer->tserver_proxy,
                          &peer->tserver_admin_proxy,
                          &peer->consensus_proxy,
                          &peer->generic_proxy);

    InsertOrDie(ts_map, peer->instance_id.permanent_uuid(), peer.get());
    ignore_result(peer.release());
  }
  return Status::OK();
}

Status GetConsensusState(const TServerDetails* replica,
                         const string& tablet_id,
                         const MonoDelta& timeout,
                         ConsensusStatePB* consensus_state) {
  GetConsensusStateRequestPB req;
  GetConsensusStateResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);
  req.set_dest_uuid(replica->uuid());
  req.add_tablet_ids(tablet_id);

  RETURN_NOT_OK(replica->consensus_proxy->GetConsensusState(req, &resp, &controller));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.tablets_size() == 0) {
    return Status::NotFound("tablet not found:", tablet_id);
  }
  DCHECK_EQ(1, resp.tablets_size());
  *consensus_state = resp.tablets(0).cstate();
  return Status::OK();
}

Status WaitUntilCommittedConfigNumVotersIs(int config_size,
                                           const TServerDetails* replica,
                                           const std::string& tablet_id,
                                           const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;

  int backoff_exp = 0;
  const int kMaxBackoffExp = 7;
  Status s;
  ConsensusStatePB cstate;
  while (true) {
    MonoDelta remaining_timeout = deadline - MonoTime::Now();
    s = GetConsensusState(replica, tablet_id, remaining_timeout, &cstate);
    if (s.ok()) {
      if (CountVoters(cstate.committed_config()) == config_size) {
        return Status::OK();
      }
    }

    if (MonoTime::Now() > start + timeout) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1LLU << backoff_exp));
    backoff_exp = min(backoff_exp + 1, kMaxBackoffExp);
  }
  return Status::TimedOut(Substitute("Number of voters does not equal $0 after waiting for $1. "
                                     "Last consensus state: $2. Last status: $3",
                                     config_size, timeout.ToString(),
                                     SecureShortDebugString(cstate), s.ToString()));
}

Status WaitUntilCommittedConfigOpIdIndexIs(int64_t opid_index,
                                           const TServerDetails* replica,
                                           const std::string& tablet_id,
                                           const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;

  Status s;
  ConsensusStatePB cstate;
  while (true) {
    MonoDelta remaining_timeout = deadline - MonoTime::Now();
    s = GetConsensusState(replica, tablet_id, remaining_timeout, &cstate);
    if (s.ok() && cstate.committed_config().opid_index() == opid_index) {
      return Status::OK();
    }
    if (MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(Substitute("Committed config opid_index does not equal $0 "
                                     "after waiting for $1. "
                                     "Last consensus state: $2. Last status: $3",
                                     opid_index,
                                     (MonoTime::Now() - start).ToString(),
                                     SecureShortDebugString(cstate), s.ToString()));
}

Status ListTabletServers(
    const shared_ptr<MasterServiceProxy>& master_proxy,
    const MonoDelta& timeout,
    vector<ListTabletServersResponsePB_Entry>* tservers) {
  master::ListTabletServersRequestPB req;
  master::ListTabletServersResponsePB resp;
  rpc::RpcController controller;
  controller.set_timeout(timeout);

  RETURN_NOT_OK(master_proxy->ListTabletServers(req, &resp, &controller));
  RETURN_NOT_OK(controller.status());
  if (resp.has_error()) {
    return Status::RemoteError("Response had an error", SecureShortDebugString(resp.error()));
  }
  tservers->assign(resp.servers().begin(), resp.servers().end());
  return Status::OK();
}

Status WaitForNumTabletServers(
    const shared_ptr<MasterServiceProxy>& master_proxy,
    int num_servers, const MonoDelta& timeout) {
  const MonoTime kStartTime = MonoTime::Now();
  const MonoTime kDeadline = kStartTime + timeout;
  vector<ListTabletServersResponsePB_Entry> tservers;
  while (MonoTime::Now() < kDeadline) {
    RETURN_NOT_OK(ListTabletServers(master_proxy, kDeadline - MonoTime::Now(), &tservers));
    if (tservers.size() >= num_servers) return Status::OK();
    SleepFor(MonoDelta::FromMilliseconds(50));
  }

  return Status::TimedOut(Substitute(
      "Timed out waiting for $0 tablet servers to be registered with the master. Found $1",
      num_servers, tservers.size()));
}

Status WaitForReplicasReportedToMaster(
    const shared_ptr<master::MasterServiceProxy>& master_proxy,
    int num_replicas,
    const string& tablet_id,
    const MonoDelta& timeout,
    WaitForLeader wait_for_leader,
    bool* has_leader,
    master::TabletLocationsPB* tablet_locations) {
  MonoTime deadline(MonoTime::Now() + timeout);
  while (true) {
    RETURN_NOT_OK(GetTabletLocations(master_proxy, tablet_id, timeout, tablet_locations));
    *has_leader = false;
    if (tablet_locations->replicas_size() == num_replicas) {
      for (const master::TabletLocationsPB_ReplicaPB& replica :
                    tablet_locations->replicas()) {
        if (replica.role() == RaftPeerPB::LEADER) {
          *has_leader = true;
        }
      }
      if (wait_for_leader == DONT_WAIT_FOR_LEADER ||
          (wait_for_leader == WAIT_FOR_LEADER && *has_leader)) {
        break;
      }
    }
    if (deadline < MonoTime::Now()) {
      return Status::TimedOut(Substitute("Timed out while waiting "
          "for tablet $0 reporting to master with $1 replicas, has_leader: $2",
          tablet_id, num_replicas, *has_leader));
    }
    SleepFor(MonoDelta::FromMilliseconds(20));
  }
  if (num_replicas != tablet_locations->replicas_size()) {
      return Status::NotFound(Substitute("Number of replicas for tablet $0 "
          "reported to master $1:$2",
          tablet_id, tablet_locations->replicas_size(),
          SecureDebugString(*tablet_locations)));
  }
  if (wait_for_leader == WAIT_FOR_LEADER && !(*has_leader)) {
    return Status::NotFound(Substitute("Leader for tablet $0 not found on master, "
                                       "number of replicas $1:$2",
                                       tablet_id, tablet_locations->replicas_size(),
                                       SecureDebugString(*tablet_locations)));
  }
  return Status::OK();
}

Status WaitUntilCommittedOpIdIndexIs(int64_t opid_index,
                                     TServerDetails* replica,
                                     const string& tablet_id,
                                     const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;

  Status s;
  OpId op_id;
  while (true) {
    MonoDelta remaining_timeout = deadline - MonoTime::Now();
    s = GetLastOpIdForReplica(tablet_id, replica, consensus::COMMITTED_OPID, remaining_timeout,
                              &op_id);
    if (s.ok() && op_id.index() == opid_index) {
      return Status::OK();
    }
    if (MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(Substitute("Committed consensus opid_index does not equal $0 "
                                     "after waiting for $1. Last opid: $2. Last status: $3",
                                     opid_index,
                                     (MonoTime::Now() - start).ToString(),
                                     OpIdToString(op_id),
                                     s.ToString()));
}

Status GetReplicaStatusAndCheckIfLeader(const TServerDetails* replica,
                                        const string& tablet_id,
                                        const MonoDelta& timeout) {
  ConsensusStatePB cstate;
  Status s = GetConsensusState(replica, tablet_id, timeout, &cstate);
  if (PREDICT_FALSE(!s.ok())) {
    VLOG(1) << "Error getting consensus state from replica: "
            << replica->instance_id.permanent_uuid();
    return Status::NotFound("Error connecting to replica", s.ToString());
  }
  const string& replica_uuid = replica->instance_id.permanent_uuid();
  if (cstate.leader_uuid() == replica_uuid) {
    return Status::OK();
  }
  VLOG(1) << "Replica not leader of config: " << replica->instance_id.permanent_uuid();
  return Status::IllegalState("Replica found but not leader");
}

Status WaitUntilLeader(const TServerDetails* replica,
                       const string& tablet_id,
                       const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;

  int backoff_exp = 0;
  const int kMaxBackoffExp = 7;
  Status s;
  while (true) {
    MonoDelta remaining_timeout = deadline - MonoTime::Now();
    s = GetReplicaStatusAndCheckIfLeader(replica, tablet_id, remaining_timeout);
    if (s.ok()) {
      return Status::OK();
    }

    if (MonoTime::Now() > deadline) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1LLU << backoff_exp));
    backoff_exp = min(backoff_exp + 1, kMaxBackoffExp);
  }
  return Status::TimedOut(Substitute("Replica $0 is not leader after waiting for $1: $2",
                                     replica->ToString(), timeout.ToString(), s.ToString()));
}

Status FindTabletLeader(const TabletServerMap& tablet_servers,
                        const string& tablet_id,
                        const MonoDelta& timeout,
                        TServerDetails** leader) {
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers, &tservers);

  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;
  Status s;
  int i = 0;
  while (true) {
    MonoDelta remaining_timeout = deadline - MonoTime::Now();
    s = GetReplicaStatusAndCheckIfLeader(tservers[i], tablet_id, remaining_timeout);
    if (s.ok()) {
      *leader = tservers[i];
      return Status::OK();
    }

    if (MonoTime::Now() > deadline) break;
    i = (i + 1) % tservers.size();
    if (i == 0) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
  }
  return Status::TimedOut(Substitute("Unable to find leader of tablet $0 after $1. "
                                     "Status message: $2", tablet_id,
                                     (MonoTime::Now() - start).ToString(),
                                     s.ToString()));
}

Status FindTabletFollowers(const TabletServerMap& tablet_servers,
                           const string& tablet_id,
                           const MonoDelta& timeout,
                           vector<TServerDetails*>* followers) {
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers, &tservers);
  TServerDetails* leader;
  RETURN_NOT_OK(FindTabletLeader(tablet_servers, tablet_id, timeout, &leader));
  for (TServerDetails* ts : tservers) {
    if (ts->uuid() != leader->uuid()) {
      followers->push_back(ts);
    }
  }
  return Status::OK();
}

Status StartElection(const TServerDetails* replica,
                     const string& tablet_id,
                     const MonoDelta& timeout) {
  RunLeaderElectionRequestPB req;
  req.set_dest_uuid(replica->uuid());
  req.set_tablet_id(tablet_id);
  RunLeaderElectionResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(replica->consensus_proxy->RunLeaderElection(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status())
      .CloneAndPrepend(Substitute("Code $0", TabletServerErrorPB::Code_Name(resp.error().code())));
  }
  return Status::OK();
}

Status RequestVote(const TServerDetails* replica,
                   const std::string& tablet_id,
                   const std::string& candidate_uuid,
                   int64_t candidate_term,
                   const consensus::OpId& last_logged_opid,
                   boost::optional<bool> ignore_live_leader,
                   boost::optional<bool> is_pre_election,
                   const MonoDelta& timeout) {
  DCHECK(last_logged_opid.IsInitialized());
  VoteRequestPB req;
  req.set_dest_uuid(replica->uuid());
  req.set_tablet_id(tablet_id);
  req.set_candidate_uuid(candidate_uuid);
  req.set_candidate_term(candidate_term);
  *req.mutable_candidate_status()->mutable_last_received() = last_logged_opid;
  if (ignore_live_leader) req.set_ignore_live_leader(*ignore_live_leader);
  if (is_pre_election) req.set_is_pre_election(*is_pre_election);
  VoteResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(replica->consensus_proxy->RequestConsensusVote(req, &resp, &rpc));
  if (resp.has_vote_granted() && resp.vote_granted()) return Status::OK();
  if (resp.has_error()) return StatusFromPB(resp.error().status());
  if (resp.has_consensus_error()) return StatusFromPB(resp.consensus_error().status());
  return Status::IllegalState("Unknown error");
}

Status LeaderStepDown(const TServerDetails* replica,
                      const string& tablet_id,
                      const MonoDelta& timeout,
                      TabletServerErrorPB* error) {
  LeaderStepDownRequestPB req;
  req.set_dest_uuid(replica->uuid());
  req.set_tablet_id(tablet_id);
  LeaderStepDownResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(replica->consensus_proxy->LeaderStepDown(req, &resp, &rpc));
  if (resp.has_error()) {
    if (error != nullptr) {
      *error = resp.error();
    }
    return StatusFromPB(resp.error().status())
      .CloneAndPrepend(Substitute("Code $0", TabletServerErrorPB::Code_Name(resp.error().code())));
  }
  return Status::OK();
}

Status WriteSimpleTestRow(const TServerDetails* replica,
                          const std::string& tablet_id,
                          RowOperationsPB::Type write_type,
                          int32_t key,
                          int32_t int_val,
                          const string& string_val,
                          const MonoDelta& timeout) {
  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);

  req.set_tablet_id(tablet_id);
  Schema schema = GetSimpleTestSchema();
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  AddTestRowToPB(write_type, schema, key, int_val, string_val, req.mutable_row_operations());

  RETURN_NOT_OK(replica->tserver_proxy->Write(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status AddServer(const TServerDetails* leader,
                 const std::string& tablet_id,
                 const TServerDetails* replica_to_add,
                 consensus::RaftPeerPB::MemberType member_type,
                 const MonoDelta& timeout,
                 const boost::optional<int64_t>& cas_config_index,
                 TabletServerErrorPB::Code* error_code) {
  ChangeConfigRequestPB req;
  req.set_dest_uuid(leader->uuid());
  req.set_tablet_id(tablet_id);
  req.set_type(consensus::ADD_SERVER);
  RaftPeerPB* peer = req.mutable_server();
  peer->set_permanent_uuid(replica_to_add->uuid());
  peer->set_member_type(member_type);
  *peer->mutable_last_known_addr() = replica_to_add->registration.rpc_addresses(0);
  if (cas_config_index) {
    req.set_cas_config_opid_index(*cas_config_index);
  }

  ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(leader->consensus_proxy->ChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    if (error_code) {
      *error_code = resp.error().code();
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status RemoveServer(const TServerDetails* leader,
                    const std::string& tablet_id,
                    const TServerDetails* replica_to_remove,
                    const MonoDelta& timeout,
                    const boost::optional<int64_t>& cas_config_index,
                    TabletServerErrorPB::Code* error_code) {
  ChangeConfigRequestPB req;
  req.set_dest_uuid(leader->uuid());
  req.set_tablet_id(tablet_id);
  req.set_type(consensus::REMOVE_SERVER);
  if (cas_config_index) {
    req.set_cas_config_opid_index(*cas_config_index);
  }
  RaftPeerPB* peer = req.mutable_server();
  peer->set_permanent_uuid(replica_to_remove->uuid());

  ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(leader->consensus_proxy->ChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    if (error_code) {
      *error_code = resp.error().code();
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status ChangeReplicaType(const TServerDetails* leader,
                         const std::string& tablet_id,
                         const TServerDetails* target_replica,
                         RaftPeerPB::MemberType replica_type,
                         const MonoDelta& timeout,
                         const boost::optional<int64_t>& cas_config_index,
                         tserver::TabletServerErrorPB::Code* error_code) {
  ChangeConfigRequestPB req;
  req.set_dest_uuid(leader->uuid());
  req.set_tablet_id(tablet_id);
  req.set_type(consensus::CHANGE_REPLICA_TYPE);
  if (cas_config_index) {
    req.set_cas_config_opid_index(*cas_config_index);
  }
  RaftPeerPB* peer = req.mutable_server();
  peer->set_permanent_uuid(target_replica->uuid());
  peer->set_member_type(replica_type);

  ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(leader->consensus_proxy->ChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    if (error_code) {
      *error_code = resp.error().code();
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status ListTablets(const TServerDetails* ts,
                   const MonoDelta& timeout,
                   vector<ListTabletsResponsePB::StatusAndSchemaPB>* tablets) {
  tserver::ListTabletsRequestPB req;
  tserver::ListTabletsResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);

  RETURN_NOT_OK(ts->tserver_proxy->ListTablets(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  tablets->assign(resp.status_and_schema().begin(), resp.status_and_schema().end());
  return Status::OK();
}

Status ListRunningTabletIds(const TServerDetails* ts,
                            const MonoDelta& timeout,
                            vector<string>* tablet_ids) {
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  RETURN_NOT_OK(ListTablets(ts, timeout, &tablets));
  tablet_ids->clear();
  for (const ListTabletsResponsePB::StatusAndSchemaPB& t : tablets) {
    if (t.tablet_status().state() == tablet::RUNNING) {
      tablet_ids->push_back(t.tablet_status().tablet_id());
    }
  }
  return Status::OK();
}

Status GetTabletLocations(const shared_ptr<MasterServiceProxy>& master_proxy,
                          const string& tablet_id,
                          const MonoDelta& timeout,
                          master::TabletLocationsPB* tablet_locations) {
  master::GetTabletLocationsResponsePB resp;
  master::GetTabletLocationsRequestPB req;
  *req.add_tablet_ids() = tablet_id;
  rpc::RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(master_proxy->GetTabletLocations(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.errors_size() > 0) {
    CHECK_EQ(1, resp.errors_size()) << SecureShortDebugString(resp);
    return StatusFromPB(resp.errors(0).status());
  }
  CHECK_EQ(1, resp.tablet_locations_size()) << SecureShortDebugString(resp);
  *tablet_locations = resp.tablet_locations(0);
  return Status::OK();
}

Status GetTableLocations(const shared_ptr<MasterServiceProxy>& master_proxy,
                         const string& table_name,
                         const MonoDelta& timeout,
                         master::GetTableLocationsResponsePB* table_locations) {
  master::GetTableLocationsRequestPB req;
  req.mutable_table()->set_table_name(table_name);
  req.set_max_returned_locations(1000);
  rpc::RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(master_proxy->GetTableLocations(req, table_locations, &rpc));
  if (table_locations->has_error()) {
    return StatusFromPB(table_locations->error().status());
  }
  return Status::OK();
}

Status WaitForNumVotersInConfigOnMaster(const shared_ptr<MasterServiceProxy>& master_proxy,
                                        const std::string& tablet_id,
                                        int num_voters,
                                        const MonoDelta& timeout) {
  Status s;
  MonoTime deadline = MonoTime::Now() + timeout;
  int num_voters_found = 0;
  while (true) {
    TabletLocationsPB tablet_locations;
    MonoDelta time_remaining = deadline - MonoTime::Now();
    s = GetTabletLocations(master_proxy, tablet_id, time_remaining, &tablet_locations);
    if (s.ok()) {
      num_voters_found = 0;
      for (const TabletLocationsPB::ReplicaPB& r : tablet_locations.replicas()) {
        if (r.role() == RaftPeerPB::LEADER || r.role() == RaftPeerPB::FOLLOWER) num_voters_found++;
      }
      if (num_voters_found == num_voters) break;
    }
    if (MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  RETURN_NOT_OK(s);
  if (num_voters_found != num_voters) {
    return Status::IllegalState(
        Substitute("Did not find exactly $0 voters, found $1 voters",
                   num_voters, num_voters_found));
  }
  return Status::OK();
}

Status WaitForNumTabletsOnTS(TServerDetails* ts,
                             int count,
                             const MonoDelta& timeout,
                             vector<ListTabletsResponsePB::StatusAndSchemaPB>* tablets,
                             boost::optional<tablet::TabletStatePB> state) {
  // If the user doesn't care about collecting the resulting tablets, collect into a local
  // vector.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets_local;
  if (tablets == nullptr) tablets = &tablets_local;

  Status s;
  MonoTime deadline = MonoTime::Now() + timeout;
  while (true) {
    s = ListTablets(ts, MonoDelta::FromSeconds(10), tablets);
    if (s.ok() && state) {
      tablets->erase(
          std::remove_if(tablets->begin(), tablets->end(),
                         [&] (const ListTabletsResponsePB::StatusAndSchemaPB& t) {
                           return t.tablet_status().state() != state;
                         }),
          tablets->end());
    }

    if (s.ok() && tablets->size() == count) break;
    if (MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  RETURN_NOT_OK(s);
  if (tablets->size() != count) {
    return Status::IllegalState(
        Substitute("Did not find exactly $0 tablets, found $1 tablets",
                   count, tablets->size()));
  }
  return Status::OK();
}

Status CheckIfTabletInState(TServerDetails* ts,
                            const std::string& tablet_id,
                            tablet::TabletStatePB expected_state,
                            const MonoDelta& timeout) {
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  RETURN_NOT_OK(ListTablets(ts, timeout, &tablets));
  for (const ListTabletsResponsePB::StatusAndSchemaPB& t : tablets) {
    if (t.tablet_status().tablet_id() == tablet_id) {
      tablet::TabletStatePB current_state = t.tablet_status().state();
      if (current_state != expected_state) {
        return Status::IllegalState(Substitute("Tablet not in expected state $0 (state = $1)",
                                               TabletStatePB_Name(expected_state),
                                               TabletStatePB_Name(current_state)));
      }
      return Status::OK();
    }
  }
  return Status::NotFound("Tablet " + tablet_id + " not found");
}

Status CheckIfTabletRunning(TServerDetails* ts,
                            const std::string& tablet_id,
                            const MonoDelta& timeout) {
  return CheckIfTabletInState(ts, tablet_id, tablet::RUNNING, timeout);
}

Status WaitUntilTabletInState(TServerDetails* ts,
                              const std::string& tablet_id,
                              tablet::TabletStatePB state,
                              const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;
  Status s;
  while (true) {
    s = CheckIfTabletInState(ts, tablet_id, state, deadline - MonoTime::Now());
    if (s.ok()) {
      return Status::OK();
    }
    if (MonoTime::Now() > deadline) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(Substitute("T $0 P $1: Tablet not in $2 state after $3: "
                                     "Status message: $4",
                                     tablet_id, ts->uuid(),
                                     tablet::TabletStatePB_Name(state),
                                     (MonoTime::Now() - start).ToString(),
                                     s.ToString()));
}

// Wait until the specified tablet is in RUNNING state.
Status WaitUntilTabletRunning(TServerDetails* ts,
                              const std::string& tablet_id,
                              const MonoDelta& timeout) {
  return WaitUntilTabletInState(ts, tablet_id, tablet::RUNNING, timeout);
}

Status DeleteTablet(const TServerDetails* ts,
                    const std::string& tablet_id,
                    const TabletDataState& delete_type,
                    const MonoDelta& timeout,
                    const boost::optional<int64_t>& cas_config_index,
                    tserver::TabletServerErrorPB::Code* error_code) {
  DeleteTabletRequestPB req;
  req.set_dest_uuid(ts->uuid());
  req.set_tablet_id(tablet_id);
  req.set_delete_type(delete_type);
  if (cas_config_index) {
    req.set_cas_config_opid_index_less_or_equal(*cas_config_index);
  }

  DeleteTabletResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(ts->tserver_admin_proxy->DeleteTablet(req, &resp, &rpc));
  if (resp.has_error()) {
    if (error_code) {
      *error_code = resp.error().code();
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status DeleteTabletWithRetries(const TServerDetails* ts,
                               const string& tablet_id,
                               TabletDataState delete_type,
                               const MonoDelta& timeout,
                               const boost::optional<int64_t>& cas_config_index) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  Status s;
  while (true) {
    s = DeleteTablet(ts, tablet_id, delete_type, timeout, cas_config_index);
    if (s.ok()) {
      return s;
    }
    if (deadline < MonoTime::Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return s;
}

Status StartTabletCopy(const TServerDetails* ts,
                       const string& tablet_id,
                       const string& copy_source_uuid,
                       const HostPort& copy_source_addr,
                       int64_t caller_term,
                       const MonoDelta& timeout,
                       tserver::TabletServerErrorPB::Code* error_code) {
  consensus::StartTabletCopyRequestPB req;
  consensus::StartTabletCopyResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);

  req.set_dest_uuid(ts->uuid());
  req.set_tablet_id(tablet_id);
  req.set_copy_peer_uuid(copy_source_uuid);
  RETURN_NOT_OK(HostPortToPB(copy_source_addr, req.mutable_copy_peer_addr()));
  req.set_caller_term(caller_term);

  RETURN_NOT_OK(ts->consensus_proxy->StartTabletCopy(req, &resp, &rpc));
  if (resp.has_error()) {
    CHECK(resp.error().has_code()) << "Tablet copy error response has no code";
    CHECK(tserver::TabletServerErrorPB::Code_IsValid(resp.error().code()))
        << "Tablet copy error response code is not valid";
    if (error_code) {
      *error_code = resp.error().code();
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status BeginTabletCopySession(const TServerDetails* ts,
                              const string& tablet_id,
                              const string& caller_uuid,
                              const MonoDelta& timeout,
                              TabletCopyErrorPB::Code* error_code) {
  BeginTabletCopySessionRequestPB req;
  BeginTabletCopySessionResponsePB resp;
  req.set_tablet_id(tablet_id);
  req.set_requestor_uuid(caller_uuid);

  RpcController rpc;
  rpc.set_timeout(timeout);

  RETURN_NOT_OK(ts->tablet_copy_proxy->BeginTabletCopySession(req, &resp, &rpc));
  if (rpc.error_response()) {
    const TabletCopyErrorPB& error =
      rpc.error_response()->GetExtension(TabletCopyErrorPB::tablet_copy_error_ext);
    if (error_code) *error_code = error.code();
    return StatusFromPB(error.status());
  }
  return Status::OK();
}


Status GetInt64Metric(const HostPort& http_hp,
                      const MetricEntityPrototype* entity_proto,
                      const char* entity_id,
                      const MetricPrototype* metric_proto,
                      const char* value_field,
                      int64_t* value) {
  // Fetch metrics whose name matches the given prototype.
  string url = Substitute(
      "http://$0/jsonmetricz?metrics=$1",
      http_hp.ToString(), metric_proto->name());
  EasyCurl curl;
  faststring dst;
  RETURN_NOT_OK(curl.FetchURL(url, &dst));

  // Parse the results, beginning with the top-level entity array.
  JsonReader r(dst.ToString());
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));
  for (const Value* entity : entities) {
    // Find the desired entity.
    string type;
    RETURN_NOT_OK(r.ExtractString(entity, "type", &type));
    if (type != entity_proto->name()) {
      continue;
    }
    if (entity_id) {
      string id;
      RETURN_NOT_OK(r.ExtractString(entity, "id", &id));
      if (id != entity_id) {
        continue;
      }
    }

    // Find the desired metric within the entity.
    vector<const Value*> metrics;
    RETURN_NOT_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
    for (const Value* metric : metrics) {
      string name;
      RETURN_NOT_OK(r.ExtractString(metric, "name", &name));
      if (name != metric_proto->name()) {
        continue;
      }
      RETURN_NOT_OK(r.ExtractInt64(metric, value_field, value));
      return Status::OK();
    }
  }
  string msg;
  if (entity_id) {
    msg = Substitute("Could not find metric $0.$1 for entity $2",
                     entity_proto->name(), metric_proto->name(),
                     entity_id);
  } else {
    msg = Substitute("Could not find metric $0.$1",
                     entity_proto->name(), metric_proto->name());
  }
  return Status::NotFound(msg);
}

} // namespace itest
} // namespace kudu
