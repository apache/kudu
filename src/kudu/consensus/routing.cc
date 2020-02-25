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

#include "kudu/consensus/routing.h"

#include <unordered_set>

#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

using boost::optional;
using google::protobuf::util::MessageDifferencer;
using kudu::pb_util::SecureShortDebugString;
using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace consensus {

////////////////////////////////////////////////////////////////////////////////
// RoutingTable
////////////////////////////////////////////////////////////////////////////////

Status RoutingTable::Init(const RaftConfigPB& raft_config,
                          const ProxyTopologyPB& proxy_topology,
                          const std::string& leader_uuid) {
  unordered_map<string, Node*> index;
  unordered_map<string, unique_ptr<Node>> forest;

  Status s = ConstructForest(raft_config, proxy_topology, &index, &forest);
  if (PREDICT_FALSE(!s.ok() && !s.IsIncomplete())) {
    return s;
  }
  RETURN_NOT_OK(MergeForestIntoSingleRoutingTree(leader_uuid, index, &forest));
  ConstructNextHopIndicesRec(forest.begin()->second.get());

  index_ = std::move(index);
  topology_root_ = std::move(forest.begin()->second);

  return s;
}

Status RoutingTable::ConstructForest(
    const RaftConfigPB& raft_config,
    const ProxyTopologyPB& proxy_topology,
    std::unordered_map<std::string, Node*>* index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {

  RETURN_NOT_OK_PREPEND(VerifyProxyTopology(proxy_topology),
                        "invalid proxy topology");

  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(raft_config),
                        "invalid raft config");

  unordered_map<string, string> dest_to_proxy_from; // keyed by directed edge destination
  for (const auto& edge : proxy_topology.proxy_edges()) {
    InsertOrDie(&dest_to_proxy_from, edge.peer_uuid(), edge.proxy_from_uuid());
  }

  // Initially, construct a forest comprised of all peers in the Raft config,
  // with no proxy_from relationships represented.
  std::unordered_map<std::string, std::unique_ptr<Node>> tmp_forest;
  std::unordered_map<std::string, Node*> tmp_index;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    unique_ptr<Node> node(new Node(peer));
    tmp_index.emplace(peer.permanent_uuid(), node.get());
    tmp_forest.emplace(peer.permanent_uuid(), std::move(node));
  }

  // proxy_from nodes specified in ProxyTopologyPB that were not found in RaftConfigPB.
  vector<string> proxy_from_nodes_not_found;

  // Now, organize the forest into parent-child relationships, where the parent
  // is represented as proxy_from in each ProxyTopologyPB edge, and the child
  // is the destination. Any node without a valid proxy_from (either not
  // specified in ProxyTopologyPB or specified as a peer that isn't currently a
  // member of the Raft config) will be left as a tree root in the forest.
  for (const RaftPeerPB& peer : raft_config.peers()) {
    const string* proxy_from_uuid = FindOrNull(dest_to_proxy_from, peer.permanent_uuid());
    if (!proxy_from_uuid) {
      continue; // No 'proxy_from' specified for this peer.
    }

    // Node has proxy_from set, so we must link them and assign object
    // ownership as a child of the proxy_from Node.
    Node* proxy_from_ptr = FindWithDefault(tmp_index, *proxy_from_uuid, nullptr);
    if (!proxy_from_ptr) {
      // We skip over rules specifying proxy_from as a node not in the Raft
      // config and we warn about it.
      proxy_from_nodes_not_found.push_back(*proxy_from_uuid);
      continue;
    }

    // Move destination out of forest map and into the proxy_from Node as a child.
    const string& node_uuid = peer.permanent_uuid();
    auto iter = tmp_forest.find(node_uuid);
    DCHECK(iter != tmp_forest.end());
    unique_ptr<Node> node = std::move(iter->second);
    tmp_forest.erase(iter->first);
    node->proxy_from = proxy_from_ptr;
    auto result = proxy_from_ptr->children.emplace(node_uuid, std::move(node));
    DCHECK(result.second) << "unexpected duplicate uuid: " << node_uuid;
  }

  *index = std::move(tmp_index);
  *forest = std::move(tmp_forest);

  // This is just a warning, not an error.
  if (!proxy_from_nodes_not_found.empty()) {
    return Status::Incomplete(
        "the following proxy_from nodes specified in the proxy topology were "
        "not found in the active Raft config and have been ignored",
        JoinStrings(proxy_from_nodes_not_found, ", "));
  }

  return Status::OK();
}

Status RoutingTable::MergeForestIntoSingleRoutingTree(
    const std::string& leader_uuid,
    const std::unordered_map<std::string, Node*>& index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {
  Node* leader = FindWithDefault(index, leader_uuid, nullptr);
  if (!leader) {
    return Status::InvalidArgument("invalid config: cannot find leader",
                                   leader_uuid);
  }

  // Find the ultimate proxy root of the leader, if the leader as a proxy
  // assigned to it.
  Node* source_root = leader;
  while (source_root->proxy_from) {
    source_root = source_root->proxy_from;
  }

  // Make all trees, except the one the leader is in, children of the leader.
  // The result is a single tree.
  auto iter = forest->begin();
  while (iter != forest->end()) {
    if (iter->first == source_root->id()) {
      ++iter;
      continue;
    }
    const string& child_uuid = iter->first;
    iter->second->proxy_from = leader;
    leader->children.emplace(child_uuid, std::move(iter->second));
    iter = forest->erase(iter);
  }

  DCHECK_EQ(1, forest->size());
  return Status::OK();
}

void RoutingTable::ConstructNextHopIndicesRec(Node* cur) {
  for (const auto& child_entry : cur->children) {
    const string& child_uuid = child_entry.first;
    const auto& child = child_entry.second;
    ConstructNextHopIndicesRec(child.get());
    // Absorb child routes.
    for (const auto& child_route : child->routes) {
      const string& dest_uuid = child_route.first;
      cur->routes.emplace(dest_uuid, child_uuid);
    }
  }
  // Add self-route as a base case.
  cur->routes.emplace(cur->id(), cur->id());
}

Status RoutingTable::NextHop(const string& src_uuid,
                             const string& dest_uuid,
                             string* next_hop) const {
  Node* src = FindWithDefault(index_, src_uuid, nullptr);
  if (!src) {
    return Status::NotFound(Substitute("unknown source uuid: $0", src_uuid));
  }
  Node* dest = FindWithDefault(index_, dest_uuid, nullptr);
  if (!dest) {
    return Status::NotFound(Substitute("unknown destination uuid: $0", dest_uuid));
  }

  // Search children.
  string* next_uuid = FindOrNull(src->routes, dest_uuid);
  if (next_uuid) {
    *next_hop = *next_uuid;
    return Status::OK();
  }

  // If we can't route via a child, route via a parent.
  DCHECK(src->proxy_from);
  *next_hop = src->proxy_from->id();
  return Status::OK();
}

std::string RoutingTable::ToString() const {
  string out;
  out.reserve(4096);
  // DFS.
  ToStringHelperRec(topology_root_.get(), /*level=*/ 0, &out);
  return out;
}

void RoutingTable::ToStringHelperRec(Node* cur, int level, std::string* out) const {
  for (int i = level - 1; i >= 0; i--) {
    if (i > 0) {
      *out += "   ";
    } else {
      *out += "-> ";
    }
  }
  *out += strings::Substitute("$0 ($1)\n",
            cur->peer_pb.permanent_uuid(),
            SecureShortDebugString(cur->peer_pb.last_known_addr()));
  for (const auto& entry : cur->children) {
    ToStringHelperRec(entry.second.get(), level + 1, out);
  }
}

////////////////////////////////////////////////////////////////////////////////
// DurableRoutingTable
////////////////////////////////////////////////////////////////////////////////

Status DurableRoutingTable::Create(FsManager* fs_manager,
                                   std::string tablet_id,
                                   RaftConfigPB raft_config,
                                   ProxyTopologyPB proxy_topology,
                                   std::shared_ptr<DurableRoutingTable>* drt) {
  string path = fs_manager->GetProxyMetadataPath(tablet_id);
  if (fs_manager->env()->FileExists(path)) {
    return Status::AlreadyPresent(Substitute("File $0 already exists", path));
  }

  auto tmp_drt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(fs_manager,
                                                  std::move(tablet_id),
                                                  std::move(proxy_topology),
                                                  std::move(raft_config)));
  RETURN_NOT_OK(tmp_drt->Flush()); // no lock needed as object is unpublished
  *drt = std::move(tmp_drt);
  return Status::OK();
}

// Read from disk.
Status DurableRoutingTable::Load(FsManager* fs_manager,
                                 std::string tablet_id,
                                 RaftConfigPB raft_config,
                                 std::shared_ptr<DurableRoutingTable>* drt) {
  string path = fs_manager->GetProxyMetadataPath(tablet_id);

  ProxyTopologyPB proxy_topology;
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(fs_manager->env(),
                                                 path,
                                                 &proxy_topology));

  *drt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(fs_manager,
                                          std::move(tablet_id),
                                          std::move(proxy_topology),
                                          std::move(raft_config)));
  return Status::OK();
}

Status DurableRoutingTable::DeleteOnDiskData(FsManager* fs_manager, const string& tablet_id) {
  string path = fs_manager->GetProxyMetadataPath(tablet_id);
  RETURN_NOT_OK_PREPEND(fs_manager->env()->DeleteFile(path),
                        Substitute("Unable to delete durable routing table file for tablet $0",
                                   tablet_id));
  return Status::OK();
}

Status DurableRoutingTable::UpdateProxyTopology(ProxyTopologyPB proxy_topology) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routing_table;
  if (leader_uuid_) {
    Status s = routing_table.Init(raft_config_, proxy_topology, *leader_uuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
  }

  // Only flush the proxy graph protobuf to disk when it changes.
  if (!MessageDifferencer::Equals(proxy_topology, proxy_topology_)) {
    VLOG_WITH_PREFIX(3) << "proxy routes updated, flushing to disk...";
    RETURN_NOT_OK(Flush());
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  proxy_topology_ = std::move(proxy_topology);

  if (leader_uuid_) {
    routing_table_ = std::move(routing_table);
    VLOG_WITH_PREFIX(2) << "updated proxy routes: \n" << routing_table_->ToString();
  } else {
    routing_table_ = boost::none;
    VLOG_WITH_PREFIX(2) << "proxy routing disabled";
  }

  return Status::OK();
}

Status DurableRoutingTable::UpdateRaftConfig(RaftConfigPB raft_config) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routing_table;
  bool leader_in_config = false;
  if (leader_uuid_) {
    leader_in_config = IsRaftConfigMember(*leader_uuid_, raft_config);
  }
  if (leader_in_config) {
    Status s = routing_table.Init(raft_config, proxy_topology_, *leader_uuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
    VLOG_WITH_PREFIX(2) << "updated proxy routes: \n" << routing_table.ToString();
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  raft_config_ = std::move(raft_config);

  if (leader_in_config) {
    routing_table_ = std::move(routing_table);
    VLOG_WITH_PREFIX(2) << "updated proxy routes: \n" << routing_table_->ToString();
  } else {
    routing_table_ = boost::none;
    VLOG_WITH_PREFIX(2) << "proxy routing disabled";
  }

  return Status::OK();
}

void DurableRoutingTable::UpdateLeader(string leader_uuid) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  RoutingTable routing_table;
  bool initialized = false;
  if (IsRaftConfigMember(leader_uuid, raft_config_)) {
    // Rebuild the routing table. If this fails, remember the new leader anyway.
    Status s = routing_table.Init(raft_config_, proxy_topology_, leader_uuid);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
      initialized = true;
    } else if (PREDICT_FALSE(!s.ok())) {
      initialized = true;
    } else {
      LOG_WITH_PREFIX(WARNING) << "unable to initialize proxy routing table: " << s.ToString();
    }
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  leader_uuid_ = std::move(leader_uuid);
  if (initialized) {
    routing_table_ = std::move(routing_table);
    VLOG_WITH_PREFIX(2) << "updated proxy routes: \n" << routing_table_->ToString();
  } else {
    routing_table_ = boost::none;
    VLOG_WITH_PREFIX(2) << "proxy routing disabled";
  }
}

Status DurableRoutingTable::NextHop(const std::string& src_uuid,
                                    const std::string& dest_uuid,
                                    std::string* next_hop) const {
  shared_lock<RWCLock> l(lock_);
  if (routing_table_) {
    return routing_table_->NextHop(src_uuid, dest_uuid, next_hop);
  }
  if (!IsRaftConfigMember(dest_uuid, raft_config_)) {
    return Status::NotFound(
        Substitute("peer with uuid $0 not found in consensus config", dest_uuid));
  }

  *next_hop = dest_uuid;
  return Status::OK();
}

ProxyTopologyPB DurableRoutingTable::GetProxyTopology() const {
  shared_lock<RWCLock> l(lock_);
  return proxy_topology_;
}

string DurableRoutingTable::ToString() const {
  shared_lock<RWCLock> l(lock_);
  if (routing_table_) {
    return routing_table_->ToString();
  }
  return "";
}

DurableRoutingTable::DurableRoutingTable(FsManager* fs_manager,
                                         string tablet_id,
                                         ProxyTopologyPB proxy_topology,
                                         RaftConfigPB raft_config)
    : fs_manager_(fs_manager),
      tablet_id_(std::move(tablet_id)),
      proxy_topology_(std::move(proxy_topology)),
      raft_config_(std::move(raft_config)) {
  // TODO(mpercy): Do we have any validation to perform here?
}

Status DurableRoutingTable::Flush() const {
  // TODO(mpercy): This entire method is copy / pasted from
  // ConsensusMetadata::Flush(). Factor out?

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool created_dir = false;
  RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(
      fs_manager_->env(), dir, &created_dir),
                        "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(created_dir)) {
    string parent_dir = DirName(dir);
    RETURN_NOT_OK_PREPEND(Env::Default()->SyncDir(parent_dir),
                          "Unable to fsync consensus parent dir " + parent_dir);
  }

  string path = fs_manager_->GetProxyMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(pb_util::WritePBContainerToPath(
      fs_manager_->env(), path, proxy_topology_, pb_util::OVERWRITE,
      // We use FLAGS_log_force_fsync_all here because the consensus metadata is
      // essentially an extension of the primary durability mechanism of the
      // consensus subsystem: the WAL. Using the same flag ensures that the WAL
      // and the consensus metadata get the same durability guarantees.
      FLAGS_log_force_fsync_all ? pb_util::SYNC : pb_util::NO_SYNC),
          Substitute("Unable to write proxy metadata file for tablet $0 to path $1",
                     tablet_id_, path));
  return Status::OK();
}

string DurableRoutingTable::LogPrefix() const {
  return strings::Substitute("T $0 P $1: ", tablet_id_, fs_manager_->uuid());
}

////////////////////////////////////////////////////////////////////////////////
// Global functions.
////////////////////////////////////////////////////////////////////////////////

Status VerifyProxyTopology(const ProxyTopologyPB& proxy_topology) {
  unordered_set<string> seen;
  for (const auto& entry : proxy_topology.proxy_edges()) {
    if (entry.peer_uuid().empty()) {
      return Status::InvalidArgument(Substitute("empty peer_uuid specified: $0",
                                                SecureShortDebugString(entry)));
    }
    if (entry.proxy_from_uuid().empty()) {
      return Status::InvalidArgument(Substitute("empty proxy_from_uuid specified: $0",
                                                SecureShortDebugString(entry)));
    }
    if (entry.peer_uuid() == entry.proxy_from_uuid()) {
      return Status::InvalidArgument(Substitute("illegal self-loop specified: $0",
                                                SecureShortDebugString(entry)));
    }
    if (!InsertIfNotPresent(&seen, entry.peer_uuid())) {
      return Status::InvalidArgument(Substitute("duplicate peer_uuid specified: $0",
                                                entry.peer_uuid()));
    }
  }
  return Status::OK();
}

} // namespace consensus
} // namespace kudu
