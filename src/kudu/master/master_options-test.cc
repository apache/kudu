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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::cluster::MiniCluster;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {
class MasterOptionsTest : public KuduTest {
};

// Verifies the InternalMiniCluster has a single master with 'last_known_addr' in the
// Raft config set to the 'expected_addr'. If 'expected_addr' is empty then verifies that
// the 'last_known_addr' field is not set.
void VerifySingleMasterRaftConfig(const InternalMiniCluster& cluster,
                                  const string& expected_addr = "") {
  auto consensus = cluster.mini_master()->master()->catalog_manager()->master_consensus();
  ASSERT_NE(nullptr, consensus.get());
  auto config = consensus->CommittedConfig();
  ASSERT_EQ(1, config.peers_size());
  const auto& peer = config.peers(0);
  if (!expected_addr.empty()) {
    ASSERT_TRUE(peer.has_last_known_addr());
    ASSERT_EQ(expected_addr, HostPortFromPB(peer.last_known_addr()).ToString());
  } else {
    ASSERT_FALSE(peer.has_last_known_addr());
  }
}

// Test bringing up a cluster with a single master config by populating --master_addresses flag
// with a single address using ExternalMiniCluster that closely simulates a real Kudu cluster.
TEST_F(MasterOptionsTest, TestSingleMasterWithMasterAddresses) {
  // Reserving a port upfront for the master so that its address can be specified in
  // --master_addresses.
  unique_ptr<Socket> reserved_socket;
  ASSERT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, 1 /* index */,
                                             kDefaultBindMode, &reserved_socket));
  Sockaddr reserved_addr;
  ASSERT_OK(reserved_socket->GetSocketAddress(&reserved_addr));
  const string reserved_hp_str = HostPort(reserved_addr).ToString();

  // ExternalMiniCluster closely simulates a real cluster where MasterOptions
  // is constructed from the supplied flags.
  ExternalMiniClusterOptions opts;
  opts.num_masters = 1;
  opts.extra_master_flags = { "--master_addresses=" + reserved_hp_str };

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());
  ASSERT_OK(cluster.WaitForTabletServerCount(cluster.num_tablet_servers(),
                                             MonoDelta::FromSeconds(5)));
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster.CreateClient(nullptr, &client));

  auto verification_steps = [&] {
    ASSERT_EQ(reserved_hp_str, client->GetMasterAddresses());
    ASSERT_FALSE(client->IsMultiMaster());

    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(cluster.master_proxy()->ListMasters(req, &resp, &rpc));
    ASSERT_EQ(1, resp.masters_size());
    ASSERT_EQ(consensus::RaftPeerPB::LEADER, resp.masters(0).role());
  };
  verification_steps();

  // Restarting the cluster exercises loading the existing system catalog code-path.
  ASSERT_OK(cluster.Restart());
  ASSERT_OK(cluster.WaitForTabletServerCount(cluster.num_tablet_servers(),
                                             MonoDelta::FromSeconds(5)));
  verification_steps();
}

// Test that verifies the 'last_known_addr' field in Raft config is set with a single master
// configuration when '--master_addresses' field is supplied on a fresh kudu deployment.
TEST_F(MasterOptionsTest, TestSingleMasterForRaftConfigFresh) {
  InternalMiniClusterOptions opts;
  opts.num_masters = 1;
  opts.supply_single_master_addr = true;
  InternalMiniCluster cluster(env_, opts);
  ASSERT_OK(cluster.Start());
  const string master_addr = HostPort(cluster.mini_master()->bound_rpc_addr()).ToString();
  VerifySingleMasterRaftConfig(cluster, master_addr);
}

// Test that verifies existing single master kudu deployments where '--master_addresses' will not
// be set after upgrade. On specifying '--master_addresses' followed by a master restart the test
// verifies that 'last_known_addr' field is set in the master Raft config.
// Also verifies other cases like mismatch in '--master_addresses' field and persisted master
// Raft config and missing '--master_addresses' field but presence of 'last_known_addr' field
// in master Raft config. See the corresponding implementation in SysCatalogTable::Load().
TEST_F(MasterOptionsTest, TestSingleMasterForRaftConfigUpgrade) {
  // Bring up a single master new cluster without supplying '--master_addresses' field.
  InternalMiniClusterOptions opts;
  opts.num_masters = 1;
  opts.supply_single_master_addr = false;
  InternalMiniCluster cluster(env_, opts);
  ASSERT_OK(cluster.Start());

  // Verify 'last_known_addr' field is not set in Raft config.
  VerifySingleMasterRaftConfig(cluster);

  // Verify 'last_known_addr' field is set in Raft config on supplying '--master_addresses'
  // field after restart.
  const vector<HostPort> master_addresses = { HostPort(cluster.mini_master()->bound_rpc_addr()) };
  const string& master_addr = master_addresses[0].ToString();
  cluster.mini_master()->Shutdown();
  cluster.mini_master()->SetMasterAddresses(master_addresses);
  ASSERT_OK(cluster.mini_master()->Restart());
  VerifySingleMasterRaftConfig(cluster, master_addr);

  // Verify 'last_known_addr' field is set in Raft even after restart.
  cluster.mini_master()->Shutdown();
  ASSERT_OK(cluster.mini_master()->Restart());
  VerifySingleMasterRaftConfig(cluster, master_addr);

  // If --master_addresses flag is omitted when it was previously supplied then it'll
  // result in a warning and still bring up master.
  cluster.mini_master()->Shutdown();
  cluster.mini_master()->SetMasterAddresses({});
  ASSERT_OK(cluster.mini_master()->Restart());
  VerifySingleMasterRaftConfig(cluster, master_addr);

  // Supplying --master_addresses flag that's different from what's persisted in Raft config
  // will result in master bring up error.
  cluster.mini_master()->Shutdown();
  HostPort incorrect_hp("foorbarbaz", Master::kDefaultPort);
  cluster.mini_master()->SetMasterAddresses({incorrect_hp});
  Status s = cluster.mini_master()->Restart();
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(),
                      Substitute("Single master Raft config error. On-disk master: $0 and supplied "
                                 "master: $1 are different", master_addr, incorrect_hp.ToString()));

  // Supplying multiple masters in --master_addresses flag to a single master configuration will
  // result in master bring up error.
  cluster.mini_master()->Shutdown();
  cluster.mini_master()->SetMasterAddresses(
      {master_addresses[0], HostPort("master-2", Master::kDefaultPort)});
  // For multi-master configuration, as derived from --master_addresses flag, Restart()
  // doesn't wait for catalog manager init.
  s = cluster.mini_master(0)->Restart();
  ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
  s = cluster.mini_master(0)->master()->WaitForCatalogManagerInit();
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_MATCHES(s.ToString(),
                     "Unable to initialize catalog manager: Failed to initialize sys tables async.*"
                     "Their symmetric difference is: master-2.*");
}

} // namespace master
} // namespace kudu
