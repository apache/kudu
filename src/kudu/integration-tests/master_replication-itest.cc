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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_bool(raft_prepare_replacement_before_eviction);

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::consensus::ReplicaManagementInfoPB;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

class TSDescriptor;

const char * const kTableId1 = "testMasterReplication-1";
const char * const kTableId2 = "testMasterReplication-2";

const int kNumTabletServerReplicas = 3;

class MasterReplicationTest : public KuduTest {
 public:
  MasterReplicationTest() {
    opts_.num_masters = 3;
    opts_.num_tablet_servers = kNumTabletServerReplicas;
  }

  virtual void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, opts_));
    ASSERT_OK(cluster_->Start());
  }

  Status RestartCluster() {
    cluster_->Shutdown();
    RETURN_NOT_OK(cluster_->Start());
    return Status::OK();
  }

  // This method is meant to be run in a separate thread.
  void StartClusterDelayed(int64_t millis) {
    LOG(INFO) << "Sleeping for "  << millis << " ms...";
    SleepFor(MonoDelta::FromMilliseconds(millis));
    LOG(INFO) << "Attempting to start the cluster...";
    CHECK_OK(cluster_->Start());
  }

  void ListMasterServerAddrs(vector<string>* out) {
    for (const auto& hostport : cluster_->master_rpc_addrs()) {
      out->emplace_back(hostport.ToString());
    }
  }

  Status CreateClient(shared_ptr<KuduClient>* out) {
    KuduClientBuilder builder;
    for (int i = 0; i < cluster_->num_masters(); i++) {
      if (!cluster_->mini_master(i)->master()->IsShutdown()) {
        builder.add_master_server_addr(cluster_->mini_master(i)->bound_rpc_addr_str());
      }
    }
    return builder.Build(out);
  }


  Status CreateTable(const shared_ptr<KuduClient>& client,
                     const std::string& table_name) {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->NotNull();
    CHECK_OK(b.Build(&schema));
    gscoped_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    return table_creator->table_name(table_name)
        .set_range_partition_columns({ "key" })
        .schema(&schema)
        .Create();
  }

 protected:
  InternalMiniClusterOptions opts_;
  gscoped_ptr<InternalMiniCluster> cluster_;
};

// Basic test. Verify that:
//
// 1) We can start multiple masters in a distributed configuration and
// that the clients and tablet servers can connect to the leader
// master.
//
// 2) We can create a table (using the standard client APIs) on the
// the leader and ensure that the appropriate table/tablet info is
// replicated to the newly elected leader.
TEST_F(MasterReplicationTest, TestSysTablesReplication) {
  shared_ptr<KuduClient> client;

  // Create the first table.
  ASSERT_OK(CreateClient(&client));
  ASSERT_OK(CreateTable(client, kTableId1));

  // Repeat the same for the second table.
  ASSERT_OK(CreateTable(client, kTableId2));

  // Verify that both tables exist. There can be a leader election at any time
  // so we need to loop and try all masters.
  while (true) {
    for (int i = 0; i < cluster_->num_masters(); i++) {
      CatalogManager* catalog =
          cluster_->mini_master(i)->master()->catalog_manager();
      CatalogManager::ScopedLeaderSharedLock l(catalog);
      if (l.first_failed_status().ok()) {
        bool exists;
        ASSERT_OK(catalog->TableNameExists(kTableId1, &exists));
        ASSERT_TRUE(exists);
        ASSERT_OK(catalog->TableNameExists(kTableId2, &exists));
        ASSERT_TRUE(exists);
        return;
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
}

// When all masters are down, test that we can timeout the connection
// attempts after a specified deadline.
TEST_F(MasterReplicationTest, TestTimeoutWhenAllMastersAreDown) {
  vector<string> master_addrs;
  ListMasterServerAddrs(&master_addrs);

  cluster_->Shutdown();

  shared_ptr<KuduClient> client;
  KuduClientBuilder builder;
  builder.master_server_addrs(master_addrs);
  builder.default_rpc_timeout(MonoDelta::FromMilliseconds(100));
  Status s = builder.Build(&client);
  EXPECT_TRUE(!s.ok());
  EXPECT_TRUE(s.IsTimedOut());

  // We need to reset 'cluster_' so that TearDown() can run correctly.
  cluster_.reset();
}

// Shut the cluster down, start initializing the client, and then
// bring the cluster back up during the initialization (but before the
// timeout can elapse).
TEST_F(MasterReplicationTest, TestCycleThroughAllMasters) {
  vector<string> master_addrs;
  ListMasterServerAddrs(&master_addrs);

  // Shut the cluster down and ...
  cluster_->Shutdown();
  // ... start the cluster after a delay.
  scoped_refptr<kudu::Thread> start_thread;
  ASSERT_OK(Thread::Create("TestCycleThroughAllMasters", "start_thread",
                                  &MasterReplicationTest::StartClusterDelayed,
                                  this,
                                  1000, // start after 1000 millis.
                                  &start_thread));

  // Verify that the client doesn't give up even though the entire
  // cluster is down for a little while.
  //
  // The timeouts for both RPCs and operations are increased to cope with slow
  // clusters (i.e. TSAN builds).
  shared_ptr<KuduClient> client;
  KuduClientBuilder builder;
  builder.master_server_addrs(master_addrs);
  builder.default_admin_operation_timeout(MonoDelta::FromSeconds(90));
  builder.default_rpc_timeout(MonoDelta::FromSeconds(15));
  EXPECT_OK(builder.Build(&client));

  ASSERT_OK(ThreadJoiner(start_thread.get()).Join());
}

// Test that every master accepts heartbeats, and that a heartbeat to any
// master updates its TSDescriptor cache.
TEST_F(MasterReplicationTest, TestHeartbeatAcceptedByAnyMaster) {
  // Register a fake tserver with every master.
  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid("fake-ts-uuid");
  common.mutable_ts_instance()->set_instance_seqno(1);
  ServerRegistrationPB fake_reg;
  HostPortPB* pb = fake_reg.add_rpc_addresses();
  pb->set_host("localhost");
  pb->set_port(1000);
  pb = fake_reg.add_http_addresses();
  pb->set_host("localhost");
  pb->set_port(2000);
  std::shared_ptr<rpc::Messenger> messenger;
  rpc::MessengerBuilder bld("Client");
  ASSERT_OK(bld.Build(&messenger));

  // Information on replica management scheme.
  ReplicaManagementInfoPB rmi;
  rmi.set_replacement_scheme(FLAGS_raft_prepare_replacement_before_eviction
      ? ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
      : ReplicaManagementInfoPB::EVICT_FIRST);

  for (int i = 0; i < cluster_->num_masters(); i++) {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    rpc::RpcController rpc;

    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);

    const auto& addr = cluster_->mini_master(i)->bound_rpc_addr();
    MasterServiceProxy proxy(messenger, addr, addr.host());

    // All masters (including followers) should accept the heartbeat.
    ASSERT_OK(proxy.TSHeartbeat(req, &resp, &rpc));
    SCOPED_TRACE(pb_util::SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // Now each master should have four registered tservers.
  vector<std::shared_ptr<TSDescriptor>> descs;
  ASSERT_OK(cluster_->WaitForTabletServerCount(
      kNumTabletServerReplicas + 1,
      InternalMiniCluster::MatchMode::DO_NOT_MATCH_TSERVERS, &descs));
}

TEST_F(MasterReplicationTest, TestMasterPeerSetsDontMatch) {
  // Restart one master with an additional entry in --master_addresses. The
  // discrepancy with the on-disk list of masters should trigger a failure.
  vector<HostPort> master_rpc_addrs = cluster_->master_rpc_addrs();
  cluster_->mini_master(0)->Shutdown();
  master_rpc_addrs.emplace_back("127.0.0.1", 55555);
  cluster_->mini_master(0)->SetMasterAddresses(master_rpc_addrs);
  ASSERT_OK(cluster_->mini_master(0)->Start());
  Status s = cluster_->mini_master(0)->WaitForCatalogManagerInit();
  SCOPED_TRACE(s.ToString());
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "55555");
}

TEST_F(MasterReplicationTest, TestConnectToClusterReturnsAddresses) {
  for (int i = 0; i < cluster_->num_masters(); i++) {
    SCOPED_TRACE(Substitute("Connecting to master $0", i));
    auto proxy = cluster_->master_proxy(i);
    rpc::RpcController rpc;
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    ASSERT_OK(proxy->ConnectToMaster(req, &resp, &rpc));
    ASSERT_EQ(cluster_->num_masters(), resp.master_addrs_size());
    for (int j = 0; j < cluster_->num_masters(); j++) {
      const auto& addr = resp.master_addrs(j);
      ASSERT_EQ(cluster_->mini_master(j)->bound_rpc_addr().ToString(),
                Substitute("$0:$1", addr.host(), addr.port()));
    }
  }
}


// Test for KUDU-2200: if a user specifies just one of the masters, and that master is a
// follower, we should give a status message that explains their mistake.
TEST_F(MasterReplicationTest, TestConnectToFollowerMasterOnly) {
  int successes = 0;
  for (int i = 0; i < cluster_->num_masters(); i++) {
    SCOPED_TRACE(Substitute("Connecting to master $0", i));

    shared_ptr<KuduClient> client;
    KuduClientBuilder builder;
    builder.add_master_server_addr(cluster_->mini_master(i)->bound_rpc_addr_str());
    Status s = builder.Build(&client);
    if (s.ok()) {
      successes++;
    } else {
      ASSERT_STR_MATCHES(s.ToString(),
                         R"(Configuration error: .*Client configured with 1 master\(s\) \(.+\) )"
                         R"(but cluster indicates it expects 3.*)");
    }
  }
  // It's possible that we get either 0 or 1 success in the above loop:
  // - 0, in the case that no master had elected itself yet
  // - 1, in the case that one master had become leader by the time we connected.
  EXPECT_LE(successes, 1);
}


} // namespace master
} // namespace kudu
