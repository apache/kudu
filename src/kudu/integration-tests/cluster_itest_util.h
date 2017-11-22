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
//
// This header file contains generic helper utilities for writing tests against
// MiniClusters and ExternalMiniClusters. Ideally, the functions will be
// generic enough to use with either type of cluster, due to operating
// primarily through RPC-based APIs or through KuduClient.
// However, it's also OK to include common operations against a particular
// cluster type if it's general enough to use from multiple tests while not
// belonging in the InternalMiniCluster / ExternalMiniCluster classes themselves. But
// consider just putting stuff like that in those classes.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_copy.pb.h"
#include "kudu/tserver/tablet_copy.proxy.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"

namespace kudu {
class HostPort;
class MetricEntityPrototype;
class MetricPrototype;
class MonoDelta;
class Status;

namespace client {
class KuduSchema;
}

namespace consensus {
class OpId;
}

namespace master {
class GetTableLocationsResponsePB;
class ListTabletServersResponsePB_Entry;
class MasterServiceProxy;
class TabletLocationsPB;
} // namespace master

namespace rpc {
class Messenger;
}

namespace itest {

struct TServerDetails {
  NodeInstancePB instance_id;
  ServerRegistrationPB registration;
  std::unique_ptr<tserver::TabletCopyServiceProxy> tablet_copy_proxy;
  std::unique_ptr<tserver::TabletServerServiceProxy> tserver_proxy;
  std::unique_ptr<tserver::TabletServerAdminServiceProxy> tserver_admin_proxy;
  std::unique_ptr<consensus::ConsensusServiceProxy> consensus_proxy;
  std::unique_ptr<server::GenericServiceProxy> generic_proxy;

  // Convenience function to get the UUID from the instance_id struct.
  const std::string& uuid() const;

  std::string ToString() const;
};

// tablet_id -> replica map.
typedef std::unordered_multimap<std::string, TServerDetails*> TabletReplicaMap;

// uuid -> tablet server map.
typedef std::unordered_map<std::string, TServerDetails*> TabletServerMap;

// Returns possibly the simplest imaginable schema, with a single int key column.
client::KuduSchema SimpleIntKeyKuduSchema();

// Create a populated TabletServerMap by interrogating the master.
// Note: The bare-pointer TServerDetails values must be deleted by the caller!
// Consider using ValueDeleter (in gutil/stl_util.h) for that.
Status CreateTabletServerMap(const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
                             const std::shared_ptr<rpc::Messenger>& messenger,
                             std::unordered_map<std::string, TServerDetails*>* ts_map);

// Gets a vector containing the latest OpId for each of the given replicas.
// Returns a bad Status if any replica cannot be reached.
Status GetLastOpIdForEachReplica(const std::string& tablet_id,
                                 const std::vector<TServerDetails*>& replicas,
                                 consensus::OpIdType opid_type,
                                 const MonoDelta& timeout,
                                 std::vector<consensus::OpId>* op_ids);

// Like the above, but for a single replica.
Status GetLastOpIdForReplica(const std::string& tablet_id,
                             TServerDetails* replica,
                             consensus::OpIdType opid_type,
                             const MonoDelta& timeout,
                             consensus::OpId* op_id);

// Wait until the latest op on the target replica is from the current term.
Status WaitForOpFromCurrentTerm(TServerDetails* replica,
                                const std::string& tablet_id,
                                consensus::OpIdType opid_type,
                                const MonoDelta& timeout,
                                consensus::OpId* opid = nullptr);

// Wait until all of the servers have converged on the same log index.
// The converged index must be at least equal to 'minimum_index'.
//
// Requires that all servers are running. Returns Status::TimedOut if the
// indexes do not converge within the given timeout.
Status WaitForServersToAgree(const MonoDelta& timeout,
                             const TabletServerMap& tablet_servers,
                             const std::string& tablet_id,
                             int64_t minimum_index);

// Wait until all specified replicas have logged at least the given index.
// Unlike WaitForServersToAgree(), the servers do not actually have to converge
// or quiesce. They only need to progress to or past the given index.
Status WaitUntilAllReplicasHaveOp(const int64_t log_index,
                                  const std::string& tablet_id,
                                  const std::vector<TServerDetails*>& replicas,
                                  const MonoDelta& timeout);

// Get the consensus state from the given replica.
Status GetConsensusState(const TServerDetails* replica,
                         const std::string& tablet_id,
                         const MonoDelta& timeout,
                         consensus::ConsensusStatePB* consensus_state);

// Wait until the number of voters in the committed consensus configuration is
// 'quorum_size', according to the specified replica.
Status WaitUntilCommittedConfigNumVotersIs(int config_size,
                                           const TServerDetails* replica,
                                           const std::string& tablet_id,
                                           const MonoDelta& timeout);

// Wait until the opid_index of the committed consensus config on the
// specified tablet is 'opid_index'.
Status WaitUntilCommittedConfigOpIdIndexIs(int64_t opid_index,
                                           const TServerDetails* replica,
                                           const std::string& tablet_id,
                                           const MonoDelta& timeout);

// List the tablet servers registered with the specified master.
Status ListTabletServers(
    const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
    const MonoDelta& timeout,
    std::vector<master::ListTabletServersResponsePB_Entry>* tservers);

// Wait for *at least* the specified number of tablet servers to be registered
// with the master.
Status WaitForNumTabletServers(
    const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
    int num_servers, const MonoDelta& timeout);

enum WaitForLeader {
  DONT_WAIT_FOR_LEADER = 0,
  WAIT_FOR_LEADER = 1
};

// Wait for the specified number of replicas to be reported by the master for
// the given tablet. Fails when leader is not found or number of replicas
// did not match up, or timeout waiting for leader.
Status WaitForReplicasReportedToMaster(
    const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
    int num_replicas, const std::string& tablet_id,
    const MonoDelta& timeout,
    WaitForLeader wait_for_leader,
    bool* has_leader,
    master::TabletLocationsPB* tablet_locations);

// Wait until the last commited OpId has index exactly 'opid_index'.
Status WaitUntilCommittedOpIdIndexIs(int64_t opid_index,
                                     TServerDetails* replica,
                                     const std::string& tablet_id,
                                     const MonoDelta& timeout);

// Returns:
// Status::OK() if the replica is alive and leader of the consensus configuration.
// Status::NotFound() if the replica is not part of the consensus configuration or is dead.
// Status::IllegalState() if the replica is live but not the leader.
Status GetReplicaStatusAndCheckIfLeader(const TServerDetails* replica,
                                        const std::string& tablet_id,
                                        const MonoDelta& timeout);

// Wait until the specified replica is leader.
Status WaitUntilLeader(const TServerDetails* replica,
                       const std::string& tablet_id,
                       const MonoDelta& timeout);

// Loops over the replicas, attempting to determine the leader, until it finds
// the first replica that believes it is the leader.
Status FindTabletLeader(const TabletServerMap& tablet_servers,
                        const std::string& tablet_id,
                        const MonoDelta& timeout,
                        TServerDetails** leader);

// Grabs list of followers using FindTabletLeader() above.
Status FindTabletFollowers(const TabletServerMap& tablet_servers,
                           const std::string& tablet_id,
                           const MonoDelta& timeout,
                           std::vector<TServerDetails*>* followers);

// Start an election on the specified tserver.
// 'timeout' only refers to the RPC asking the peer to start an election. The
// StartElection() RPC does not block waiting for the results of the election,
// and neither does this call.
Status StartElection(const TServerDetails* replica,
                     const std::string& tablet_id,
                     const MonoDelta& timeout);

// Request the given replica to vote. This is thin wrapper around
// RequestConsensusVote(). See the definition of VoteRequestPB in
// consensus.proto for parameter details.
Status RequestVote(const TServerDetails* replica,
                   const std::string& tablet_id,
                   const std::string& candidate_uuid,
                   int64_t candidate_term,
                   const consensus::OpId& last_logged_opid,
                   boost::optional<bool> ignore_live_leader,
                   boost::optional<bool> is_pre_election,
                   const MonoDelta& timeout);

// Cause a leader to step down on the specified server.
// 'timeout' refers to the RPC timeout waiting synchronously for stepdown to
// complete on the leader side. Since that does not require communication with
// other nodes at this time, this call is rather quick.
Status LeaderStepDown(const TServerDetails* replica,
                      const std::string& tablet_id,
                      const MonoDelta& timeout,
                      tserver::TabletServerErrorPB* error = nullptr);

// Write a "simple test schema" row to the specified tablet on the given
// replica. This schema is commonly used by tests and is defined in
// wire_protocol-test-util.h
// The caller must specify whether this is an INSERT or UPDATE call via
// write_type.
Status WriteSimpleTestRow(const TServerDetails* replica,
                          const std::string& tablet_id,
                          RowOperationsPB::Type write_type,
                          int32_t key,
                          int32_t int_val,
                          const std::string& string_val,
                          const MonoDelta& timeout);

// Run a ConfigChange to ADD_PEER on 'replica_to_add'.
// The RPC request is sent to 'leader'.
Status AddServer(const TServerDetails* leader,
                 const std::string& tablet_id,
                 const TServerDetails* replica_to_add,
                 consensus::RaftPeerPB::MemberType member_type,
                 const MonoDelta& timeout,
                 const consensus::RaftPeerAttrsPB& attrs = {},
                 const boost::optional<int64_t>& cas_config_index = boost::none,
                 tserver::TabletServerErrorPB::Code* error_code = nullptr);

// Run a ConfigChange to REMOVE_PEER on 'replica_to_remove'.
// The RPC request is sent to 'leader'.
Status RemoveServer(const TServerDetails* leader,
                    const std::string& tablet_id,
                    const TServerDetails* replica_to_remove,
                    const MonoDelta& timeout,
                    const boost::optional<int64_t>& cas_config_index = boost::none,
                    tserver::TabletServerErrorPB::Code* error_code = nullptr);

// Change type of the given replica to the specified type.
// The RPC request is sent to 'leader'.
Status ChangeReplicaType(const TServerDetails* leader,
                         const std::string& tablet_id,
                         const TServerDetails* target_replica,
                         consensus::RaftPeerPB::MemberType replica_type,
                         const MonoDelta& timeout,
                         const boost::optional<int64_t>& cas_config_index = boost::none,
                         tserver::TabletServerErrorPB::Code* error_code = nullptr);

// Get the list of tablets from the remote server.
Status ListTablets(const TServerDetails* ts,
                   const MonoDelta& timeout,
                   std::vector<tserver::ListTabletsResponsePB_StatusAndSchemaPB>* tablets);

// Get the list of RUNNING tablet ids from the remote server.
Status ListRunningTabletIds(const TServerDetails* ts,
                            const MonoDelta& timeout,
                            std::vector<std::string>* tablet_ids);

// Get the list of tablet locations for the specified tablet from the Master.
Status GetTabletLocations(const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
                          const std::string& tablet_id,
                          const MonoDelta& timeout,
                          master::TabletLocationsPB* tablet_locations);

// Get the list of tablet locations for all tablets in the specified table from the Master.
Status GetTableLocations(const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
                         const std::string& table_name,
                         const MonoDelta& timeout,
                         master::GetTableLocationsResponsePB* table_locations);

// Wait for the specified number of voters to be reported to the config on the
// master for the specified tablet.
Status WaitForNumVotersInConfigOnMaster(
    const std::shared_ptr<master::MasterServiceProxy>& master_proxy,
    const std::string& tablet_id,
    int num_voters,
    const MonoDelta& timeout);

// Repeatedly invoke ListTablets(), waiting for up to 'timeout' time for the
// specified 'count' number of replicas. If 'state' is provided, the replicas
// must also be in the specified state for the wait to be considered
// successful.
Status WaitForNumTabletsOnTS(
    TServerDetails* ts,
    int count,
    const MonoDelta& timeout,
    std::vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB>* tablets,
    boost::optional<tablet::TabletStatePB> state = boost::none);

// Check if the tablet is in the specified state.
Status CheckIfTabletInState(TServerDetails* ts,
                            const std::string& tablet_id,
                            tablet::TabletStatePB expected_state,
                            const MonoDelta& timeout);

// Check if the given tablet is RUNNING.
Status CheckIfTabletRunning(TServerDetails* ts,
                            const std::string& tablet_id,
                            const MonoDelta& timeout);

// Wait until the specified replica is in the specified state.
Status WaitUntilTabletInState(TServerDetails* ts,
                              const std::string& tablet_id,
                              tablet::TabletStatePB state,
                              const MonoDelta& timeout);

// Wait until the specified tablet is in RUNNING state.
Status WaitUntilTabletRunning(TServerDetails* ts,
                              const std::string& tablet_id,
                              const MonoDelta& timeout);

// Send a DeleteTablet() to the server at 'ts' of the specified 'delete_type'.
// If set, the 'cas_config_index' corresponds to the
// 'cas_config_opid_index_less_or_equal' field of the request.
Status DeleteTablet(const TServerDetails* ts,
                    const std::string& tablet_id,
                    const tablet::TabletDataState& delete_type,
                    const MonoDelta& timeout,
                    const boost::optional<int64_t>& cas_config_index = boost::none,
                    tserver::TabletServerErrorPB::Code* error_code = nullptr);

// Repeatedly try to delete the tablet, retrying on failure up to the
// specified timeout. Deletion can fail when other operations, such as
// bootstrap or tablet copy, are running.
Status DeleteTabletWithRetries(
    const TServerDetails* ts,
    const std::string& tablet_id,
    tablet::TabletDataState delete_type,
    const MonoDelta& timeout,
    const boost::optional<int64_t>& cas_config_index = boost::none);

// Cause the remote to initiate tablet copy using the specified host as a
// source.
Status StartTabletCopy(const TServerDetails* ts,
                       const std::string& tablet_id,
                       const std::string& copy_source_uuid,
                       const HostPort& copy_source_addr,
                       int64_t caller_term,
                       const MonoDelta& timeout,
                       tserver::TabletServerErrorPB::Code* error_code = nullptr);

// Begin a tablet copy session on the remote.
Status BeginTabletCopySession(const TServerDetails* ts,
                              const std::string& tablet_id,
                              const std::string& caller_uuid,
                              const MonoDelta& timeout,
                              tserver::TabletCopyErrorPB::Code* error_code = nullptr);

// Retrieve the value of a given metric from the server whose webserver is
// bound to 'http_hp'. The metric must be of int64_t type.
//
// 'value_field' represents the particular field of the metric to be read.
// For example, for a counter or gauge, this should be 'value'. For a
// histogram, it might be 'total_count' or 'mean'.
//
// 'entity_id' may be NULL, in which case the first entity of the same type
// as 'entity_proto' will be matched.
Status GetInt64Metric(const HostPort& http_hp,
                      const MetricEntityPrototype* entity_proto,
                      const char* entity_id,
                      const MetricPrototype* metric_proto,
                      const char* value_field,
                      int64_t* value);


} // namespace itest
} // namespace kudu
