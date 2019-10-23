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
#include <set>
#include <utility>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>
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
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/thrift/client.h"
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
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using ::sentry::TSentryGrantOption;
using ::sentry::TSentryPrivilege;
using boost::optional;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::CountVoters;
using kudu::consensus::EXCLUDE_HEALTH_REPORT;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::consensus::GetLastOpIdRequestPB;
using kudu::consensus::GetLastOpIdResponsePB;
using kudu::consensus::IncludeHealthReport;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::OpId;
using kudu::consensus::OpIdType;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::RunLeaderElectionResponsePB;
using kudu::consensus::RunLeaderElectionRequestPB;
using kudu::consensus::VoteRequestPB;
using kudu::consensus::VoteResponsePB;
using kudu::consensus::kInvalidOpIdIndex;
using kudu::master::ListTabletServersResponsePB_Entry;
using kudu::master::MasterServiceProxy;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::Messenger;
using kudu::rpc::RpcController;
using kudu::sentry::SentryClient;
using kudu::tablet::TabletDataState;
using kudu::tserver::CreateTsClientProxies;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::DeleteTabletRequestPB;
using kudu::tserver::DeleteTabletResponsePB;
using kudu::tserver::BeginTabletCopySessionRequestPB;
using kudu::tserver::BeginTabletCopySessionResponsePB;
using kudu::tserver::TabletCopyErrorPB;
using kudu::tserver::TabletServerErrorPB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using rapidjson::Value;
using std::min;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

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
    s = GetConsensusState(replica, tablet_id, kDeadline - MonoTime::Now(), EXCLUDE_HEALTH_REPORT,
                          &cstate);
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
                             int64_t minimum_index,
                             consensus::OpIdType op_id_type) {
  const MonoTime deadline = MonoTime::Now() + timeout;

  vector<TServerDetails*> servers;
  AppendValuesFromMap(tablet_servers, &servers);
  for (int i = 1; MonoTime::Now() < deadline; i++) {
    vector<OpId> ids;
    Status s = GetLastOpIdForEachReplica(
        tablet_id, servers, op_id_type, timeout, &ids);
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
  return Status::TimedOut(
      Substitute("index $0 not available on all replicas after $1",
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

    unique_ptr<TServerDetails> peer(new TServerDetails);
    peer->instance_id.CopyFrom(entry.instance_id());
    peer->registration.CopyFrom(entry.registration());
    peer->location = entry.location();

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
                         IncludeHealthReport report_health,
                         ConsensusStatePB* consensus_state) {
  GetConsensusStateRequestPB req;
  GetConsensusStateResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);
  req.set_dest_uuid(replica->uuid());
  req.add_tablet_ids(tablet_id);
  req.set_report_health(report_health);

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

Status WaitUntilNoPendingConfig(const TServerDetails* replica,
                                const std::string& tablet_id,
                                const MonoDelta& timeout,
                                consensus::ConsensusStatePB* cstate) {
  ConsensusStatePB cstate_tmp;
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;
  MonoTime now;
  Status s;
  while ((now = MonoTime::Now()) < deadline) {
    s = GetConsensusState(replica, tablet_id, deadline - now, EXCLUDE_HEALTH_REPORT, &cstate_tmp);
    if (s.ok() && !cstate_tmp.has_pending_config()) {
      if (cstate) {
        *cstate = std::move(cstate_tmp);
      }
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(30));
  }
  return Status::TimedOut(Substitute("There is still a pending config after waiting for $0. "
                                     "Last consensus state: $1. Last status: $2",
                                     (MonoTime::Now() - start).ToString(),
                                     SecureShortDebugString(cstate_tmp), s.ToString()));
}

Status WaitUntilCommittedConfigNumVotersIs(int num_voters,
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
    s = GetConsensusState(replica, tablet_id, remaining_timeout, EXCLUDE_HEALTH_REPORT, &cstate);
    if (s.ok()) {
      if (CountVoters(cstate.committed_config()) == num_voters) {
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
                                     num_voters, timeout.ToString(),
                                     SecureShortDebugString(cstate), s.ToString()));
}

void WaitUntilCommittedConfigNumMembersIs(int num_members,
                                          const TServerDetails* replica,
                                          const std::string& tablet_id,
                                          const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  AssertEventually([&] {
    ConsensusStatePB cstate;
    ASSERT_OK(GetConsensusState(replica, tablet_id, deadline - MonoTime::Now(),
                                EXCLUDE_HEALTH_REPORT, &cstate));
    ASSERT_EQ(num_members, cstate.committed_config().peers_size());
  }, timeout);
  NO_PENDING_FATALS();
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
    s = GetConsensusState(replica, tablet_id, remaining_timeout, EXCLUDE_HEALTH_REPORT, &cstate);
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
  RpcController controller;
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
    master::ReplicaTypeFilter filter,
    bool* has_leader,
    master::GetTabletLocationsResponsePB* tablet_locations) {
  MonoTime deadline(MonoTime::Now() + timeout);
  while (true) {
    RETURN_NOT_OK(GetTabletLocations(master_proxy, tablet_id, timeout, filter, tablet_locations));
    *has_leader = false;
    if (tablet_locations->tablet_locations(0).interned_replicas_size() == num_replicas) {
      for (const auto& replica : tablet_locations->tablet_locations(0).interned_replicas()) {
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
  if (num_replicas != tablet_locations->tablet_locations(0).interned_replicas_size()) {
      return Status::NotFound(Substitute("Number of replicas for tablet $0 "
          "reported to master $1:$2",
          tablet_id, tablet_locations->tablet_locations(0).interned_replicas_size(),
          SecureDebugString(*tablet_locations)));
  }
  if (wait_for_leader == WAIT_FOR_LEADER && !(*has_leader)) {
    return Status::NotFound(Substitute(
        "Leader for tablet $0 not found on master, number of replicas $1:$2",
        tablet_id,
        tablet_locations->tablet_locations(0).interned_replicas_size(),
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
  Status s = GetConsensusState(replica, tablet_id, timeout, EXCLUDE_HEALTH_REPORT, &cstate);
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

// Fills the out parameter "followers" with the tablet servers hosting the "tablet_id".
// Non-empty "tablet_servers" is expected to include all the tablet servers and not subset
// of tablet servers that host the "tablet_id".
// Returns an error if the tablet servers do not have consensus or cannot be reached.
Status FindTabletFollowers(const TabletServerMap& tablet_servers,
                           const string& tablet_id,
                           const MonoDelta& timeout,
                           vector<TServerDetails*>* followers) {
  DCHECK(!tablet_servers.empty());

  // Sorted sets allow for set intersection needed below.
  // uuids of all supplied tablet servers.
  std::set<string> tablet_server_uuids;
  // uuids of the tablet server peers that host the specified tablet_id.
  std::set<string> peer_uuids;
  // uuid of the leader tablet server hosting the specified tablet_id.
  string leader_uuid;
  const MonoTime start = MonoTime::Now();
  const MonoTime deadline = MonoTime::Now() + timeout;
  bool no_leader_or_peers_found = false;
  for (const auto& entry : tablet_servers) {
    const auto now = MonoTime::Now();
    if (now > deadline) {
      return Status::TimedOut(Substitute("Unable to find followers for tablet $0 after $1.",
                                         tablet_id, (now - start).ToString()));
    }
    const auto& tserver_uuid = entry.first;
    const auto* tserver = entry.second;
    tablet_server_uuids.emplace(tserver_uuid);

    const MonoDelta remaining_timeout = deadline - now;
    ConsensusStatePB cstate;
    Status s = GetConsensusState(tserver, tablet_id, remaining_timeout, EXCLUDE_HEALTH_REPORT,
                                 &cstate);
    if (!s.ok()) {
      // Failure could be due to tablet server not hosting the tablet which is okay or other issue.
      // If all the tablet servers return error then the failure is reported back. See below.
      continue;
    }
    // At this point tablet server does host the tablet but it's possible there is no leader
    // or peers are unknown.
    if (cstate.committed_config().peers_size() == 0 || !cstate.has_leader_uuid()) {
      no_leader_or_peers_found = true;
      continue;
    }

    std::set<string> curr_peer_uuids;
    for (const auto& peer : cstate.committed_config().peers()) {
      curr_peer_uuids.emplace(peer.permanent_uuid());
    }
    DCHECK(!curr_peer_uuids.empty());
    DCHECK(!cstate.leader_uuid().empty());
    if (!leader_uuid.empty()) {
      DCHECK(!peer_uuids.empty());
      // Sanity checking that tablet servers with the specified tablet are reporting
      // the same leader and set of peers.
      if (leader_uuid != cstate.leader_uuid()) {
        return Status::IllegalState(Substitute(
            "Leader $0 reported by tablet server $1 for tablet $2 doesn't match with leader $3 "
            "reported by other tablet servers.", cstate.leader_uuid(), tserver_uuid, tablet_id,
            leader_uuid));
      }
      if (peer_uuids != curr_peer_uuids) {
        return Status::IllegalState(Substitute(
            "Peers reported by tablet server $0 for tablet $1 don't match with peers reported by "
            "other tablet servers.", tserver_uuid, tablet_id));
      }
    } else {
      DCHECK(peer_uuids.empty());
      leader_uuid = cstate.leader_uuid();
      peer_uuids.swap(curr_peer_uuids);
    }
  }

  // Unable to get leader and peer information from multiple supplied tablet servers.
  if (leader_uuid.empty()) {
    DCHECK(peer_uuids.empty());

    return no_leader_or_peers_found ?
           Status::IllegalState(
               Substitute("No leader or peers found for tablet $0.", tablet_id)) :
           Status::NotFound(
               Substitute("No tablet server found with tablet $0 or tablet servers not reachable.",
                          tablet_id));
  }

  if (peer_uuids != STLSetIntersection(peer_uuids, tablet_server_uuids)) {
    return Status::InvalidArgument(Substitute(
        "Not all peers reported by tablet servers are part of the supplied tablet servers."));
  }

  for (const auto& tserver_uuid : peer_uuids) {
    if (tserver_uuid != leader_uuid) {
      followers->emplace_back(FindOrDie(tablet_servers, tserver_uuid));
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
                 const consensus::RaftPeerAttrsPB& attrs,
                 const boost::optional<int64_t>& cas_config_index,
                 TabletServerErrorPB::Code* error_code) {
  ChangeConfigRequestPB req;
  req.set_dest_uuid(leader->uuid());
  req.set_tablet_id(tablet_id);
  req.set_type(consensus::ADD_PEER);
  RaftPeerPB* peer = req.mutable_server();
  peer->set_permanent_uuid(replica_to_add->uuid());
  peer->set_member_type(member_type);
  *peer->mutable_attrs() = attrs;
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
  req.set_type(consensus::REMOVE_PEER);
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
  req.set_type(consensus::MODIFY_PEER);
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

Status BulkChangeConfig(const TServerDetails* leader,
                        const std::string& tablet_id,
                        const vector<BulkChangeConfigRequestPB::ConfigChangeItemPB>& changes,
                        const MonoDelta& timeout,
                        const boost::optional<int64_t>& cas_config_index,
                        tserver::TabletServerErrorPB::Code* error_code) {
  BulkChangeConfigRequestPB req;
  req.set_dest_uuid(leader->uuid());
  req.set_tablet_id(tablet_id);
  if (cas_config_index) {
    req.set_cas_config_opid_index(*cas_config_index);
  }
  for (const auto& change : changes) {
    *req.add_config_changes() = change;
  }

  ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(leader->consensus_proxy->BulkChangeConfig(req, &resp, &rpc));
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
                          master::ReplicaTypeFilter filter,
                          master::GetTabletLocationsResponsePB* tablet_locations) {
  master::GetTabletLocationsRequestPB req;
  *req.add_tablet_ids() = tablet_id;
  req.set_replica_type_filter(filter);
  req.set_intern_ts_infos_in_response(true);
  rpc::RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(master_proxy->GetTabletLocations(req, tablet_locations, &rpc));
  if (tablet_locations->has_error()) {
    return StatusFromPB(tablet_locations->error().status());
  }
  if (tablet_locations->errors_size() > 0) {
    CHECK_EQ(1, tablet_locations->errors_size())
        << SecureShortDebugString(*tablet_locations);
    return StatusFromPB(tablet_locations->errors(0).status());
  }
  CHECK_EQ(1, tablet_locations->tablet_locations_size())
      << SecureShortDebugString(*tablet_locations);
  return Status::OK();
}

Status GetTableLocations(const shared_ptr<MasterServiceProxy>& master_proxy,
                         const string& table_name,
                         const MonoDelta& timeout,
                         master::ReplicaTypeFilter filter,
                         optional<const string&> table_id,
                         master::GetTableLocationsResponsePB* table_locations) {
  master::GetTableLocationsRequestPB req;
  req.mutable_table()->set_table_name(table_name);
  if (table_id) {
    req.mutable_table()->set_table_id(*table_id);
  }
  req.set_replica_type_filter(filter);
  req.set_max_returned_locations(1000);
  req.set_intern_ts_infos_in_response(true);
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
    master::GetTabletLocationsResponsePB tablet_locations;
    MonoDelta time_remaining = deadline - MonoTime::Now();
    s = GetTabletLocations(master_proxy, tablet_id, time_remaining,
                           master::VOTER_REPLICA, &tablet_locations);
    if (s.ok()) {
      num_voters_found = 0;
      for (const auto& r : tablet_locations.tablet_locations(0).interned_replicas()) {
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

Status WaitForNumTabletsOnTS(const TServerDetails* ts,
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

Status CheckIfTabletInState(const TServerDetails* ts,
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

Status CheckIfTabletRunning(const TServerDetails* ts,
                            const std::string& tablet_id,
                            const MonoDelta& timeout) {
  return CheckIfTabletInState(ts, tablet_id, tablet::RUNNING, timeout);
}

Status WaitUntilTabletInState(const TServerDetails* ts,
                              const std::string& tablet_id,
                              tablet::TabletStatePB state,
                              const MonoDelta& timeout) {
  static const MonoDelta kSleepInterval = MonoDelta::FromMilliseconds(10);
  const MonoTime start = MonoTime::Now();
  const MonoTime deadline = start + timeout;
  MonoTime now;
  Status s;
  while (true) {
    now = MonoTime::Now();
    s = CheckIfTabletInState(ts, tablet_id, state, deadline - now);
    if (s.ok()) {
      return Status::OK();
    }
    if (now + kSleepInterval >= deadline) {
      break;
    }
    SleepFor(kSleepInterval);
  }
  return Status::TimedOut(Substitute("T $0 P $1: Tablet not in $2 state after $3: "
                                     "Status message: $4",
                                     tablet_id, ts->uuid(),
                                     tablet::TabletStatePB_Name(state),
                                     (now - start).ToString(),
                                     s.ToString()));
}

// Wait until the specified tablet is in RUNNING state.
Status WaitUntilTabletRunning(const TServerDetails* ts,
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

Status GetTsCounterValue(ExternalTabletServer* ets,
                         MetricPrototype* metric,
                         int64_t* value) {
  return GetInt64Metric(ets->bound_http_hostport(),
                        &METRIC_ENTITY_server,
                        "kudu.tabletserver",
                        metric,
                        "value",
                        value);
}

Status SetupAdministratorPrivileges(MiniKdc* kdc,
                                    const HostPort& address) {
  DCHECK(kdc);
  RETURN_NOT_OK(kdc->CreateUserPrincipal("kudu"));
  RETURN_NOT_OK(kdc->Kinit("kudu"));

  thrift::ClientOptions sentry_opts;
  sentry_opts.service_principal = "sentry";
  sentry_opts.enable_kerberos = true;
  unique_ptr<SentryClient> sentry_client(
      new SentryClient(address, sentry_opts));
  RETURN_NOT_OK(sentry_client->Start());

  // Create an admin role for the "admin" group specified in mini_sentry.cc.
  // Grant this role all privileges for the server so the admin user can
  // perform any operations required in tests.
  RETURN_NOT_OK(master::CreateRoleAndAddToGroups(sentry_client.get(), "admin-role", "admin"));
  TSentryPrivilege privilege = master::GetServerPrivilege("ALL", TSentryGrantOption::DISABLED);
  RETURN_NOT_OK(master::AlterRoleGrantPrivilege(sentry_client.get(), "admin-role", privilege));
  return kdc->Kinit("test-admin");
}

Status AlterTableName(const shared_ptr<MasterServiceProxy>& master_proxy,
                      const string& table_id,
                      const string& old_table_name,
                      const string& new_table_name,
                      const MonoDelta& timeout) {
  master::AlterTableRequestPB req;
  req.mutable_table()->set_table_id(table_id);
  req.mutable_table()->set_table_name(old_table_name);
  req.set_new_table_name(new_table_name);
  master::AlterTableResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);

  RETURN_NOT_OK(master_proxy->AlterTable(req, &resp, &controller));
  RETURN_NOT_OK(controller.status());
  if (resp.has_error()) {
    return Status::RemoteError("Response had an error", SecureShortDebugString(resp.error()));
  }

  return Status::OK();
}

Status DeleteTable(const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
                   const std::string& table_id,
                   const std::string& table_name,
                   const MonoDelta& timeout) {
  master::DeleteTableRequestPB req;
  req.mutable_table()->set_table_id(table_id);
  req.mutable_table()->set_table_name(table_name);
  master::DeleteTableResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);

  RETURN_NOT_OK(master_proxy->DeleteTable(req, &resp, &controller));
  RETURN_NOT_OK(controller.status());
  if (resp.has_error()) {
    return Status::RemoteError("Response had an error", SecureShortDebugString(resp.error()));
  }

  return Status::OK();
}

} // namespace itest
} // namespace kudu
