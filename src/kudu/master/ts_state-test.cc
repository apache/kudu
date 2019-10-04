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

#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/barrier.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::consensus::ReplicaManagementInfoPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

namespace {
const char* kTServer = "tserver";

TServerStatePB PickRandomState() {
  switch (rand() % 2) {
    case 0:
      return TServerStatePB::NONE;
    case 1:
      return TServerStatePB::MAINTENANCE_MODE;
  }
  return TServerStatePB::NONE;
}

} // anonymous namespace

class TServerStateTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    NO_FATALS(ResetMaster());
  }

  void TearDown() override {
    mini_master_->Shutdown();
    KuduTest::TearDown();
  }

  // Restarts the master and resets any affiliated references to it.
  void ResetMaster() {
    mini_master_.reset(new MiniMaster(GetTestPath("master"),
                                      HostPort("127.0.0.1", 0)));
    ASSERT_OK(mini_master_->Start());
    master_ = mini_master_->master();
    ASSERT_OK(master_->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
        MonoDelta::FromSeconds(30)));
    ts_manager_ = master_->ts_manager();
    MessengerBuilder builder("client");
    ASSERT_OK(builder.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_,
                                        mini_master_->bound_rpc_addr(),
                                        mini_master_->bound_rpc_addr().host()));
  }

  // Sets the tserver state for the given tablet server to 'state'.
  Status SetTServerState(const string& tserver_uuid, TServerStatePB state) {
    return master_->ts_manager()->SetTServerState(
        tserver_uuid, state, master_->catalog_manager()->sys_catalog());
  }

  // Pretends to be a tserver by sending a heartbeat to the master from the
  // given tserver.
  Status SendHeartbeat(const string& tserver) {
    TSHeartbeatRequestPB req;
    NodeInstancePB* ts_instance = req.mutable_common()->mutable_ts_instance();
    ts_instance->set_permanent_uuid(tserver);
    ts_instance->set_instance_seqno(0);
    req.mutable_replica_management_info()->set_replacement_scheme(
        ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION);
    ServerRegistrationPB* reg = req.mutable_registration();
    HostPortPB* rpc_hp = reg->add_rpc_addresses();
    rpc_hp->set_host(Substitute("$0-host", tserver));
    rpc_hp->set_port(7051);
    TabletReportPB* tablet_report = req.mutable_tablet_report();
    tablet_report->set_sequence_number(0);
    tablet_report->set_is_incremental(false);

    RpcController rpc;
    TSHeartbeatResponsePB resp_pb;
    return proxy_->TSHeartbeat(req, &resp_pb, &rpc);
  }

  // Creates a table with the given name with a simple schema, a default
  // replication factor, and a single partition.
  Status CreateTable(const string& table_name) {
    RpcController rpc;
    CreateTableResponsePB resp;
    CreateTableRequestPB req;
    req.set_name(table_name);
    RETURN_NOT_OK(SchemaToPB(GetSimpleTestSchema(), req.mutable_schema()));

    RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &rpc));
    if (resp.has_error()) {
      RETURN_NOT_OK(StatusFromPB(resp.error().status()));
    }
    if (!resp.has_table_id()) {
      return Status::RuntimeError("expected table id");
    }
    return Status::OK();
  }

 protected:
  unique_ptr<MiniMaster> mini_master_;
  Master* master_;
  TSManager* ts_manager_;
  unique_ptr<MasterServiceProxy> proxy_;
  shared_ptr<Messenger> client_messenger_;
};

// Basic test that sets some tserver states for registered and unregistered
// tablet servers, and checks that they still exist after restarting.
TEST_F(TServerStateTest, TestReloadTServerState) {
  // Sanity check that we've got no tservers yet.
  ASSERT_EQ(0, ts_manager_->GetCount());

  // Register a tserver and then set maintenance mode.
  ASSERT_OK(SendHeartbeat(kTServer));
  ASSERT_EQ(1, ts_manager_->GetCount());
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::MAINTENANCE_MODE));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));

  // Restart the master; the maintenance mode should still be there, even if
  // the tablet server hasn't re-registered.
  NO_FATALS(ResetMaster());
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));
  ASSERT_EQ(0, ts_manager_->GetCount());

  // When the tserver registers, maintenance mode should still be there.
  ASSERT_OK(SendHeartbeat(kTServer));
  ASSERT_EQ(1, ts_manager_->GetCount());

  // When maintenance mode is turned off, this should be reflected on the
  // master, even after another restart.
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::NONE));
  ASSERT_EQ(TServerStatePB::NONE, ts_manager_->GetTServerState(kTServer));
  NO_FATALS(ResetMaster());
  ASSERT_EQ(TServerStatePB::NONE, ts_manager_->GetTServerState(kTServer));
}

// Test that setting tserver states that are already set end up as no-ops for
// both registered and unregistered tservers.
TEST_F(TServerStateTest, TestRepeatedTServerStates) {
  // We should start out with no tserver states.
  ASSERT_EQ(TServerStatePB::NONE, ts_manager_->GetTServerState(kTServer));

  // And "exiting" the tserver state should be a no-op.
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::NONE));
  ASSERT_EQ(TServerStatePB::NONE, ts_manager_->GetTServerState(kTServer));

  // The same should be true for entering a tserver state.
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::MAINTENANCE_MODE));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::MAINTENANCE_MODE));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));

  // Now do the same thing with the tserver registered.
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::NONE));
  ASSERT_OK(SendHeartbeat(kTServer));
  ASSERT_EQ(1, ts_manager_->GetCount());
  ASSERT_EQ(TServerStatePB::NONE, ts_manager_->GetTServerState(kTServer));

  // Exiting the tserver state should be a no-op.
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::NONE));
  ASSERT_EQ(TServerStatePB::NONE, ts_manager_->GetTServerState(kTServer));

  // Setting maintenance mode on a tserver that's already in maintenance mode
  // should also no-op.
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::MAINTENANCE_MODE));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));
  ASSERT_OK(SetTServerState(kTServer, TServerStatePB::MAINTENANCE_MODE));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));
}

// Test that setting both in-memory and on-disk tserver state is atomic.
TEST_F(TServerStateTest, TestConcurrentSetTServerState) {
  const int kNumTServers = 10;
  const int kNumThreadsPerTServer = 10;
  vector<thread> threads;
  vector<string> tservers(kNumTServers);
  for (int i = 0; i < kNumTServers; i++) {
    tservers[i] = Substitute("$0-$1", kTServer, i);
  }
  // Spin up a bunch of threads that contend for setting the state for a
  // limited number of tablet servers.
  Barrier b(kNumThreadsPerTServer * kNumTServers + kNumTServers);
  for (int i = 0; i < kNumThreadsPerTServer; i++) {
    for (const auto& ts : tservers) {
      threads.emplace_back([&, ts] {
        b.Wait();
        CHECK_OK(SetTServerState(ts, PickRandomState()));
      });
    }
  }
  // Concurrently, register the servers.
  for (const auto& ts : tservers) {
    threads.emplace_back([&, ts] {
      b.Wait();
      CHECK_OK(SendHeartbeat(ts));
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  // Ensure that the in-memory state matches the on-disk state by reloading the
  // master state from disk and checking it against the original in-memory
  // state.
  vector<TServerStatePB> in_memory_states(tservers.size());
  for (int i = 0; i < tservers.size(); i++) {
    in_memory_states[i] = ts_manager_->GetTServerState(tservers[i]);
  }
  NO_FATALS(ResetMaster());
  for (int i = 0; i < tservers.size(); i++) {
    ASSERT_EQ(in_memory_states[i], ts_manager_->GetTServerState(tservers[i]));
  }
}

// Test that tablet servers that are in maintenance mode don't get tablet
// replicas placed on them.
TEST_F(TServerStateTest, MaintenanceModeTServerDoesntGetNewReplicas) {
  const int kNumTServers = 4;
  vector<string> tserver_ids;

  // Report to the master from a few tablet servers to register them.
  for (int i = 0; i < kNumTServers; i++) {
    string tserver_id = Substitute("$0-$1", kTServer, i);
    ASSERT_OK(SendHeartbeat(tserver_id));
    tserver_ids.emplace_back(std::move(tserver_id));
  }

  // Put one of the tablet servers in maintenance mode and create a few tables.
  const string& first_maintenance_tserver = tserver_ids[0];
  ASSERT_OK(SetTServerState(first_maintenance_tserver, TServerStatePB::MAINTENANCE_MODE));
  const int kNumTables = 10;
  for (int i = 0; i < kNumTables; i++) {
    ASSERT_OK(CreateTable(Substitute("table-$0", i)));
  }
  TSDescriptorVector descs;
  master_->ts_manager()->GetAllDescriptors(&descs);
  for (const auto& desc : descs) {
    if (desc->permanent_uuid() == first_maintenance_tserver) {
      // The tablet server in maintenance mode should have had no replicas
      // placed on it because it's in maintenance mode.
      ASSERT_EQ(0, desc->RecentReplicaCreations());
    } else {
      // All others should have some. Note that we can't compare an exact
      // number because the replica creations has a decay factor built into it.
      ASSERT_LT(0, desc->RecentReplicaCreations());
    }
  }
  // If we put another tserver in maintenance mode, we'll only have two
  // remaining tservers, and won't be able to create new tables with the
  // default replication factor.
  ASSERT_OK(SetTServerState(tserver_ids[1], TServerStatePB::MAINTENANCE_MODE));
  string sad_table_id;
  Status s = CreateTable("sad-table");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "not enough live tablet servers");

  // And if we exit maintenance mode, we should be able to create tables again.
  ASSERT_OK(SetTServerState(tserver_ids[1], TServerStatePB::NONE));
  ASSERT_OK(CreateTable("happy-table"));
}

// Test to exercise the RPC endpoint to change the tserver state.
TEST_F(TServerStateTest, TestRPCs) {
  ChangeTServerStateRequestPB req;
  Status s;
  // Sends a state change RPC and ensures there's an error, matching the
  // input error string if provided.
  const auto send_req_check_failed = [&] (const string& error) {
    RpcController rpc;
    ChangeTServerStateResponsePB resp;
    s = proxy_->ChangeTServerState(req, &resp, &rpc);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    if (!error.empty()) {
      ASSERT_STR_CONTAINS(s.ToString(), error);
    }
  };
  NO_FATALS(send_req_check_failed("must contain tserver state change"));
  TServerStateChangePB* ts_state_change = req.mutable_change();

  NO_FATALS(send_req_check_failed("uuid not provided"));
  ts_state_change->set_uuid(kTServer);

  NO_FATALS(send_req_check_failed("state change not provided"));

  // Now send over a correct request. Do this a couple times to sanity check
  // that repeated calls are just no-ops.
  ts_state_change->set_change(TServerStateChangePB::ENTER_MAINTENANCE_MODE);
  const int kNumRepeatedCalls = 2;
  for (int i = 0; i < kNumRepeatedCalls; i++) {
    RpcController rpc;
    ChangeTServerStateResponsePB resp;
    ASSERT_OK(proxy_->ChangeTServerState(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager_->GetTServerState(kTServer));
  }
}

} // namespace master
} // namespace kudu
