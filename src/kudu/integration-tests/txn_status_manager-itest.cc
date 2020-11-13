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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/master/txn_manager.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::TabletIdAndTableName;
using kudu::itest::TServerDetails;
using kudu::itest::WaitForOpFromCurrentTerm;
using kudu::rpc::Messenger;
using kudu::rpc::RpcController;
using kudu::transactions::CommitTransactionRequestPB;
using kudu::transactions::CommitTransactionResponsePB;
using kudu::transactions::BeginTransactionRequestPB;
using kudu::transactions::BeginTransactionResponsePB;
using kudu::transactions::GetTransactionStateRequestPB;
using kudu::transactions::GetTransactionStateResponsePB;
using kudu::transactions::KeepTransactionAliveRequestPB;
using kudu::transactions::KeepTransactionAliveResponsePB;
using kudu::transactions::TxnManagerServiceProxy;
using kudu::transactions::TxnStatePB;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class TxnStatusManagerITest : public ExternalMiniClusterITestBase {
 public:
  static const constexpr auto kNumTabletServers = 3;
  static const constexpr auto kTxnTrackerIntervalMs = 100;
  static const constexpr auto kTxnKeepaliveIntervalMs =
      kTxnTrackerIntervalMs * 5;
  static const constexpr auto kRaftHbIntervalMs = 50;
  static const constexpr char* const kTxnTrackerIntervalFlag =
      "txn_staleness_tracker_interval_ms";

  TxnStatusManagerITest() {
    cluster_opts_.num_tablet_servers = kNumTabletServers;

    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    cluster_opts_.extra_master_flags.emplace_back(
        "--txn_manager_enabled=true");
    // Scenarios based on this test fixture assume the txn status table
    // is created at start, not on first transaction-related operation.
    cluster_opts_.extra_master_flags.emplace_back(
        "--txn_manager_lazily_initialized=false");

    // To speed up test scenarios, set shorter intervals for the transaction
    // keepalive interval ...
    cluster_opts_.extra_tserver_flags.emplace_back(Substitute(
        "--txn_keepalive_interval_ms=$0", kTxnKeepaliveIntervalMs));
    // ... and the polling interval of the txn staleness tracker.
    cluster_opts_.extra_tserver_flags.emplace_back(Substitute(
        "--$0=$1", kTxnTrackerIntervalFlag, kTxnTrackerIntervalMs));
    cluster_opts_.extra_tserver_flags.emplace_back(Substitute(
        "--txn_staleness_tracker_disabled_interval_ms=$0", kTxnTrackerIntervalMs));
    // Speed up Raft re-elections in case of non-responsive leader replicas.
    cluster_opts_.extra_tserver_flags.emplace_back(Substitute(
        "--raft_heartbeat_interval_ms=$0", kRaftHbIntervalMs));
    cluster_opts_.extra_tserver_flags.emplace_back(
        "--leader_failure_max_missed_heartbeat_periods=1.25");
  }

  void SetUp() override {
    static constexpr const char* const kTxnStatusTableName =
        "kudu_system.kudu_transactions";

    // This assertion is an explicit statement that all scenarios in this test
    // assume there is only one TxnManager instance in the cluster.
    //
    // TODO(aserbin): make this tests parameterized by the number of TxnManger
    //                instances (i.e. Kudu masters) and extend the scenarios
    //                as needed.
    ASSERT_EQ(1, cluster_opts_.num_masters);

    NO_FATALS(StartClusterWithOpts(cluster_opts_));

    // Wait for txn status tablets created at each of the tablet servers.
    vector<TabletIdAndTableName> tablets_info;
    for (auto idx = 0; idx < kNumTabletServers; ++idx) {
      vector<TabletIdAndTableName> info;
      ASSERT_OK(cluster_->WaitForTabletsRunning(
          cluster_->tablet_server(idx), 1, kTimeout, &info));
      ASSERT_EQ(1, info.size());
      tablets_info.emplace_back(std::move(*info.begin()));
    }

    const string& tablet_id = tablets_info.begin()->tablet_id;
    for (const auto& elem : tablets_info) {
      ASSERT_EQ(tablet_id, elem.tablet_id);
      ASSERT_EQ(kTxnStatusTableName, elem.table_name);
    }
    txn_status_tablet_id_ = tablet_id;

    rpc::MessengerBuilder b("txn-keepalive");
    ASSERT_OK(b.Build(&messenger_));
    const auto rpc_addr = cluster_->master()->bound_rpc_addr();
    txn_manager_proxy_.reset(new TxnManagerServiceProxy(
        messenger_, rpc_addr, rpc_addr.host()));
  }

  Status GetTxnStatusTabletLeader(string* ts_uuid) {
    CHECK(ts_uuid);
    TServerDetails* leader;
    RETURN_NOT_OK(FindTabletLeader(
        ts_map_, txn_status_tablet_id_, kTimeout, &leader));
    *ts_uuid = leader->uuid();

    return Status::OK();
  }

  Status BeginTransaction(int64_t* txn_id, uint32_t* keepalive_ms) {
    CHECK(txn_id);
    RpcController ctx;
    PrepareRpcController(&ctx);
    BeginTransactionRequestPB req;
    BeginTransactionResponsePB resp;
    RETURN_NOT_OK(txn_manager_proxy_->BeginTransaction(req, &resp, &ctx));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    CHECK(resp.has_txn_id());
    *txn_id = resp.txn_id();
    CHECK(resp.has_keepalive_millis());
    *keepalive_ms = resp.keepalive_millis();

    return Status::OK();
  }

  Status CommitTransaction(int64_t txn_id) {
    CHECK(txn_id);
    RpcController ctx;
    PrepareRpcController(&ctx);
    CommitTransactionRequestPB req;
    req.set_txn_id(txn_id);
    CommitTransactionResponsePB resp;
    RETURN_NOT_OK(txn_manager_proxy_->CommitTransaction(req, &resp, &ctx));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  Status GetTxnState(int64_t txn_id, TxnStatePB* state) {
    CHECK(state);
    CHECK(txn_manager_proxy_);
    GetTransactionStateRequestPB req;
    req.set_txn_id(txn_id);
    GetTransactionStateResponsePB resp;
    RpcController ctx;
    PrepareRpcController(&ctx);
    RETURN_NOT_OK(txn_manager_proxy_->GetTransactionState(req, &resp, &ctx));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    CHECK(resp.has_state());
    *state = resp.state();

    return Status::OK();
  }

  void CheckTxnState(int64_t txn_id, TxnStatePB expected_state) {
    TxnStatePB txn_state;
    ASSERT_OK(GetTxnState(txn_id, &txn_state));
    ASSERT_EQ(expected_state, txn_state);
  }

 protected:
  const string& txn_tablet_id() const {
    return txn_status_tablet_id_;
  }

  static void PrepareRpcController(RpcController* ctx) {
    static const MonoDelta kRpcTimeout = MonoDelta::FromSeconds(15);
    CHECK_NOTNULL(ctx)->set_timeout(kRpcTimeout);
  }

  static const MonoDelta kTimeout;

  ExternalMiniClusterOptions cluster_opts_;
  string txn_status_tablet_id_;

  shared_ptr<rpc::Messenger> messenger_;
  unique_ptr<TxnManagerServiceProxy> txn_manager_proxy_;
};

const MonoDelta TxnStatusManagerITest::kTimeout = MonoDelta::FromSeconds(15);

// TODO(aserbin): enable all scenarios below once [1] is committed. Without [1],
//                these scenarios sometimes fails upon calling GetTxnState():
//
//   Bad status: Not found: Failed to write to server:
//   7c968757cc19497a93b15b6c6a48e446 (127.13.78.3:33027):
//   transaction ID 0 not found, current highest txn ID: -1
//
//                The issue here is that a non-leader replica might load
//                the information from tablet that's lagging behind the
//                leader, and once the replica becomes a new leader later on,
//                the information is stale because TxnStatusManager's data
//                isn't yet reloaded upon the becoming a leader. Once the
//                patch above is merged, remove this TODO and remove '#if 0'
//                for the code below.
//
//                [1] https://gerrit.cloudera.org/#/c/16648/

// The test to verify basic functionality of the transaction tracker: it should
// detect transactions that haven't received KeepTransactionAlive() requests
// for longer than the transaction's keepalive interval and automatically abort
// those.
TEST_F(TxnStatusManagerITest, DISABLED_StaleTransactionsCleanup) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Check that the transaction staleness is detected and the stale transaction
  // is aborted while the transaction is in OPEN state.
  {
    int64_t txn_id;
    uint32_t keepalive_interval_ms;
    // ASSERT_EVENTUALLY is needed because this test uses raw TxnManagerProxy,
    // and we don't wait for TxnManager to initialize in SetUp().
    ASSERT_EVENTUALLY([&]() {
      ASSERT_OK(BeginTransaction(&txn_id, &keepalive_interval_ms));
    });

    // Wait for longer than the transaction keep-alive interval to allow
    // the transaction tracker to detect the staleness of the transaction
    // and abort it. An extra margin here is to avoid flakiness due to
    // scheduling anomalies.
    SleepFor(MonoDelta::FromMilliseconds(3 * keepalive_interval_ms));
    NO_FATALS(CheckTxnState(txn_id, TxnStatePB::ABORTED));
  }

  // Check that the transaction staleness is detected and the stale transaction
  // is aborted while the transaction is in COMMIT_IN_PROGRESS state.
  {
    int64_t txn_id;
    uint32_t keepalive_interval_ms;
    ASSERT_OK(BeginTransaction(&txn_id, &keepalive_interval_ms));
    ASSERT_OK(CommitTransaction(txn_id));
    NO_FATALS(CheckTxnState(txn_id, TxnStatePB::COMMIT_IN_PROGRESS));

    // A transaction in COMMIT_IN_PROGRESS state isn't automatically aborted
    // even if no txn keepalive messages are received.
    SleepFor(MonoDelta::FromMilliseconds(3 * keepalive_interval_ms));
    NO_FATALS(CheckTxnState(txn_id, TxnStatePB::COMMIT_IN_PROGRESS));
  }
}

// Make sure it's possible to disable and enable back the transaction
// staleness tracking in run-time without restarting the processes hosting
// TxnStatusManager instances (i.e. tablet servers).
TEST_F(TxnStatusManagerITest, DISABLED_ToggleStaleTxnTrackerInRuntime) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Disable txn transaction tracking in run-time.
  for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
    auto* ts = cluster_->tablet_server(i);
    ASSERT_OK(cluster_->SetFlag(ts, kTxnTrackerIntervalFlag, "0"));
  }

  int64_t txn_id;
  uint32_t keepalive_interval_ms;
  // ASSERT_EVENTUALLY is needed because this test uses raw TxnManagerProxy,
  // and we don't wait for TxnManager to initialize in SetUp().
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(BeginTransaction(&txn_id, &keepalive_interval_ms));
  });
  ASSERT_EQ(kTxnKeepaliveIntervalMs, keepalive_interval_ms);

  // Now, with no transaction staleness tracking, the transaction should
  // not be aborted automatically even if not sending keepalive requests.
  SleepFor(MonoDelta::FromMilliseconds(3 * keepalive_interval_ms));
  NO_FATALS(CheckTxnState(txn_id, TxnStatePB::OPEN));

  // Re-enable txn transaction tracking in run-time.
  for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
    auto* ts = cluster_->tablet_server(i);
    ASSERT_OK(cluster_->SetFlag(
        ts, kTxnTrackerIntervalFlag, std::to_string(kTxnTrackerIntervalMs)));
  }

  // Check that the transaction staleness is detected and the stale transaction
  // is aborted once stale transaction tracking is re-enabled.
  SleepFor(MonoDelta::FromMilliseconds(3 * keepalive_interval_ms));
  NO_FATALS(CheckTxnState(txn_id, TxnStatePB::ABORTED));
}

// Verify the functionality of the stale transaction tracker in TxnStatusManager
// in case of replicated txn status table. The crux of this scenario is to make
// sure that a transaction isn't aborted if keepalive requests are sent as
// required even in case of Raft leader re-elections and restarts
// of the TxnStatusManager instances.
TEST_F(TxnStatusManagerITest, DISABLED_TxnKeepAliveMultiTxnStatusManagerInstances) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  int64_t txn_id;
  uint32_t keepalive_interval_ms;
  // ASSERT_EVENTUALLY is needed because this test uses raw TxnManagerProxy,
  // and we don't wait for TxnManager to initialize in SetUp().
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(BeginTransaction(&txn_id, &keepalive_interval_ms));
  });

  CountDownLatch latch(1);
  Status keep_txn_alive_status;
  thread txn_keepalive_sender([&] {
    const auto period = MonoDelta::FromMilliseconds(keepalive_interval_ms / 2);
    const auto timeout = MonoDelta::FromMilliseconds(keepalive_interval_ms / 4);
    // Keepalive thread uses its own messenger and proxy.
    shared_ptr<rpc::Messenger> m;
    rpc::MessengerBuilder b("txn-keepalive");
    ASSERT_OK(b.Build(&m));
    const auto rpc_addr = cluster_->master()->bound_rpc_addr();
    TxnManagerServiceProxy txn_manager_proxy(m, rpc_addr, rpc_addr.host());
    do {
      // The timeout for KeepTransactionAlive() requests should be short,
      // otherwise this thread might miss sending the requests with proper
      // timing.
      RpcController ctx;
      ctx.set_timeout(timeout);
      KeepTransactionAliveRequestPB req;
      req.set_txn_id(txn_id);
      KeepTransactionAliveResponsePB resp;
      auto s = txn_manager_proxy.KeepTransactionAlive(req, &resp, &ctx);
      if (resp.has_error()) {
        if (resp.error().has_status()) {
          keep_txn_alive_status = StatusFromPB(resp.error().status());
        } else {
          keep_txn_alive_status = Status::RemoteError("unspecified status");
        }
      } else {
        keep_txn_alive_status = s;
      }
      if (!keep_txn_alive_status.ok()) {
        LOG(WARNING) << Substitute(
            "KeepTransactionAlive() returned non-OK status: $0",
            keep_txn_alive_status.ToString());
      }
    } while (!latch.WaitFor(period));
  });
  auto cleanup = MakeScopedCleanup([&] {
    latch.CountDown();
    txn_keepalive_sender.join();
  });

  // Pause tserver processes. This is to check how the stale txn tracker works
  // in case of 'frozen' and then 'thawed' processes. The essence is to make
  // sure the former leader doesn't abort a transaction in case of scheduling
  // anomalies, and TxnManager forwards txn keep-alive messages to proper
  // TxnStatusMananger instance when leadership changes.
  for (auto i = 0; i < 5; ++i) {
    string old_leader_uuid;
    ASSERT_OK(GetTxnStatusTabletLeader(&old_leader_uuid));
    auto* ts = cluster_->tablet_server_by_uuid(old_leader_uuid);
    ASSERT_EVENTUALLY([&]{
      ASSERT_OK(ts->Pause());
      SleepFor(MonoDelta::FromMilliseconds(
          std::max(3 * kRaftHbIntervalMs, 2 * kTxnKeepaliveIntervalMs)));
      ASSERT_OK(ts->Resume());
      string new_leader_uuid;
      ASSERT_OK(GetTxnStatusTabletLeader(&new_leader_uuid));
      ASSERT_NE(old_leader_uuid, new_leader_uuid);
      auto* other_ts = FindOrDie(ts_map_, new_leader_uuid);
      // Make sure the new leader has established itself up to the point that
      // it can write into its backing txn status tablet: this is necessary to
      // make sure it can abort the transaction, if it finds the transaction
      // has stalled.
      ASSERT_OK(WaitForOpFromCurrentTerm(
          other_ts, txn_tablet_id(), consensus::COMMITTED_OPID, kTimeout));
    });
    // Give the TxnStatusManager instance running with the leader tablet replica
    // a chance to detect and abort stale transactions, if any detected.
    SleepFor(MonoDelta::FromMilliseconds(2 * kTxnKeepaliveIntervalMs));
  }

  NO_FATALS(CheckTxnState(txn_id, TxnStatePB::OPEN));

  // Restart tablet servers hosting TxnStatusManager instances. This is to
  // check that starting TxnStatusManager doesn't abort an open transaction
  // which sending txn keepalive messages as required.
  for (auto i = 0; i < 5; ++i) {
    string old_leader_uuid;
    ASSERT_OK(GetTxnStatusTabletLeader(&old_leader_uuid));
    auto* ts = cluster_->tablet_server_by_uuid(old_leader_uuid);
    ts->Shutdown();
    ASSERT_EVENTUALLY([&]{
      string new_leader_uuid;
      ASSERT_OK(GetTxnStatusTabletLeader(&new_leader_uuid));
      ASSERT_NE(old_leader_uuid, new_leader_uuid);
      auto* other_ts = FindOrDie(ts_map_, new_leader_uuid);
      // Make sure the new txn status tablet leader has established itself. For
      // the reasoning, see the comment in the 'pause scenario' scope above.
      ASSERT_OK(WaitForOpFromCurrentTerm(
          other_ts, txn_tablet_id(), consensus::COMMITTED_OPID, kTimeout));
    });
    ASSERT_OK(ts->Restart());
    SleepFor(MonoDelta::FromMilliseconds(2 * kTxnKeepaliveIntervalMs));
  }

  NO_FATALS(CheckTxnState(txn_id, TxnStatePB::OPEN));

  latch.CountDown();
  txn_keepalive_sender.join();
  cleanup.cancel();

  // An extra sanity check: make sure the recent keepalive requests were sent
  // successfully, as expected.
  ASSERT_OK(keep_txn_alive_status);

  // Now, when no txn keepalive heartbeats are sent, the transaction
  // should be automatically aborted by TxnStatusManager running with the
  // leader replica of the txn status tablet.
  ASSERT_EVENTUALLY([&]{
    NO_FATALS(CheckTxnState(txn_id, TxnStatePB::ABORTED));
  });

  NO_FATALS(cluster_->AssertNoCrashes());
}

} // namespace kudu
