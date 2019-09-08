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
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
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
    Master* master = mini_master_->master();
    ASSERT_OK(master->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
        MonoDelta::FromSeconds(30)));
    ts_manager_ = master->ts_manager();
    MessengerBuilder builder("client");
    ASSERT_OK(builder.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_,
                                        mini_master_->bound_rpc_addr(),
                                        mini_master_->bound_rpc_addr().host()));
  }

  // Sets the tserver state for the given tablet server to 'state'.
  Status SetTServerState(const string& tserver_uuid, TServerStatePB state) {
    Master* master = mini_master_->master();
    return master->ts_manager()->SetTServerState(
        tserver_uuid, state, master->catalog_manager()->sys_catalog());
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

 protected:
  unique_ptr<MiniMaster> mini_master_;
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
  Barrier b(kNumThreadsPerTServer * kNumTServers);
  for (int i = 0; i < kNumThreadsPerTServer; i++) {
    for (const auto& ts : tservers) {
      threads.emplace_back([&, ts] {
        b.Wait();
        CHECK_OK(SetTServerState(ts, PickRandomState()));
      });
    }
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

} // namespace master
} // namespace kudu
