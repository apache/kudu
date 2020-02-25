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

#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "kudu/consensus/consensus-test-util.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;

namespace kudu {
namespace consensus {

static void AddEdge(ProxyTopologyPB* proxy_topology, string peer, string upstream_uuid) {
  ProxyEdgePB* edge = proxy_topology->add_proxy_edges();
  edge->set_peer_uuid(std::move(peer));
  edge->set_proxy_from_uuid(std::move(upstream_uuid));
}

TEST(RoutingTest, TestRoutingTable) {
  RaftConfigPB raft_config = BuildRaftConfigPBForTests(/*num_voters=*/6);
  raft_config.set_opid_index(1); // required for validation
  ProxyTopologyPB proxy_topology;
  AddEdge(&proxy_topology, /*to=*/"peer-1", /*proxy_from=*/"peer-0");
  AddEdge(&proxy_topology, /*to=*/"peer-3", /*proxy_from=*/"peer-2");
  AddEdge(&proxy_topology, /*to=*/"peer-4", /*proxy_from=*/"peer-3");
  AddEdge(&proxy_topology, /*to=*/"peer-5", /*proxy_from=*/"peer-3");

  // Specify a leader that has a parent (proxy_from).
  const string kLeaderUuid = "peer-3";

  RoutingTable routing_table;
  ASSERT_OK(routing_table.Init(raft_config, proxy_topology, kLeaderUuid));

  string next_hop;
  ASSERT_OK(routing_table.NextHop("peer-3", "peer-5", &next_hop));
  ASSERT_EQ("peer-5", next_hop);
  ASSERT_OK(routing_table.NextHop("peer-3", "peer-1", &next_hop));
  ASSERT_EQ("peer-0", next_hop);
  ASSERT_OK(routing_table.NextHop("peer-5", "peer-1", &next_hop));
  ASSERT_EQ("peer-3", next_hop);
  ASSERT_OK(routing_table.NextHop("peer-2", "peer-4", &next_hop));
  ASSERT_EQ("peer-3", next_hop);
}

// Test the case where an instance of "proxy_from" is not in the Raft config.
TEST(RoutingTest, TestProxyFromNotInRaftConfig) {
  const string kLeaderUuid = "peer-0";
  const string kBogusUuid = "bogus";

  RaftConfigPB raft_config = BuildRaftConfigPBForTests(/*num_voters=*/2);
  raft_config.set_opid_index(1); // required for validation
  ProxyTopologyPB proxy_topology;
  AddEdge(&proxy_topology, /*to=*/"peer-1", /*proxy_from=*/kBogusUuid);

  RoutingTable routing_table;
  Status s = routing_table.Init(raft_config, proxy_topology, kLeaderUuid);
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "have been ignored: " + kBogusUuid);

  string next_hop;
  ASSERT_OK(routing_table.NextHop(/*src_uuid=*/"peer-0", /*dest_uuid=*/"peer-1", &next_hop));
  ASSERT_EQ("peer-1", next_hop); // Direct routing fallback.
}

} // namespace consensus
} // namespace kudu
