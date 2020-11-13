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

#include "kudu/master/txn_manager.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/master/txn_manager.proxy.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/util/barrier.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::rpc::RpcController;
using kudu::transactions::TxnStatePB;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_int32(rpc_service_queue_length);
DECLARE_int64(txn_manager_status_table_range_partition_span);
DECLARE_uint32(txn_manager_status_table_num_replicas);
DECLARE_uint32(txn_keepalive_interval_ms);

namespace kudu {
namespace transactions {

class TxnManagerTest : public KuduTest {
 protected:
  TxnManagerTest()
      : master_(nullptr) {
    // Master is necessary since it hosts the TxnManager RPC service.
    opts_.num_masters = 1;
    // At least one tablet server is necessary to host transaction status
    // tablets.
    opts_.num_tablet_servers = 1;
  }

  void SetUp() override {
    // Explicitly setting the flags just for better readability.
    FLAGS_txn_manager_enabled = true;
    FLAGS_txn_manager_lazily_initialized = true;

    // In this test, there is just a single tablet servers in the cluster.
    FLAGS_txn_manager_status_table_num_replicas = 1;

    // Make TxnManager creating new ranges in the transaction status table more
    // often, so it's not necessary to start too many transactions to see it
    // switching to a new tablet of the transaction status table.
    FLAGS_txn_manager_status_table_range_partition_span = 1024;

    // A few scenarios (e.g. LazyInitializationConcurrentCalls) might require
    // an insanely high capacity of the service RPC queue since their workload
    // depends on the number of CPU cores available. They can send many requests
    // at once to stress the system while verifying important invariants.
    FLAGS_rpc_service_queue_length = 10000;

    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, opts_));
    ASSERT_OK(Start());
  }

  void TearDown() override {
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

  // Prepare the cluster to run test scenarios: start the cluster, wait for
  // TxnManager initialization, and setup TxnManager's proxy.
  Status Start() {
    RETURN_NOT_OK(cluster_->Start());
    // InternalMiniCluster::Start() resets the set of mini-masters: need to
    // update the shortcut pointer.
    master_ = cluster_->mini_master()->master();
    if (!FLAGS_txn_manager_lazily_initialized) {
      RETURN_NOT_OK(cluster_->mini_master()->master()->WaitForTxnManagerInit());
    }

    proxy_.reset(new TxnManagerServiceProxy(
        cluster_->messenger(),
        cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host()));
    return Status::OK();
  }

  static void PrepareRpcController(RpcController* ctx) {
    static const MonoDelta kRpcTimeout = MonoDelta::FromSeconds(30);
    ASSERT_NE(nullptr, ctx);
    ctx->set_timeout(kRpcTimeout);
  }

  InternalMiniClusterOptions opts_;
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<TxnManagerServiceProxy> proxy_;
  // A shortcut to the Master object.
  master::Master* master_;
};

// Verify the basic functionality when TxnManager is lazily initialized.
TEST_F(TxnManagerTest, LazyInitialization) {
  // The lazy initialization mode is on by default.
  ASSERT_TRUE(FLAGS_txn_manager_lazily_initialized);
  ASSERT_TRUE(master_->txn_manager()->is_lazily_initialized_);
  ASSERT_FALSE(master_->txn_manager()->initialized_);

  // Timeout is not very relevant here, it only limits the amount of time to
  // wait. By default, the lazy initialization mode is on, so TxnManager
  // should not be initialized.
  {
    const MonoDelta kTimeout = MonoDelta::FromSeconds(1);
    auto s = master_->WaitForTxnManagerInit(kTimeout);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "timed out waiting for TxnManager to initialize");
  }

  // Since the lazy initialization mode is on and there haven't been any calls
  // to the TxnManager so far, the TxnManager should not be initialized.
  ASSERT_FALSE(master_->txn_manager()->initialized_);

  // Make a call to TxnManager using a not-yet-seen txn_id. The result should
  // be an error, but after this TxnManager should become initialized.
  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    GetTransactionStateRequestPB req;
    GetTransactionStateResponsePB resp;
    req.set_txn_id(0);
    ASSERT_OK(proxy_->GetTransactionState(req, &resp, &ctx));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 0 not found");
  }

  ASSERT_OK(master_->WaitForTxnManagerInit());
  ASSERT_TRUE(master_->txn_manager()->initialized_);

  // Current implementation starts assigning transaction identifiers with 0,
  // and the very first range partition created upon initialization covers
  // 0+ range. If making a call to TxnManager using a negative txn_id, then
  // the result shows the fact that the corresponding tablet doesn't exist.
  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    AbortTransactionRequestPB req;
    AbortTransactionResponsePB resp;
    req.set_txn_id(-1);
    ASSERT_OK(proxy_->AbortTransaction(req, &resp, &ctx));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "No tablet covering the requested range partition");
  }

  // Shutdown and start the cluster again. This is to verify that initialization
  // code works as expected when the transaction status table already exists.
  cluster_->Shutdown();
  ASSERT_OK(Start());
  ASSERT_FALSE(master_->txn_manager()->initialized_);

  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    CommitTransactionRequestPB req;
    CommitTransactionResponsePB resp;
    req.set_txn_id(0);
    ASSERT_OK(proxy_->CommitTransaction(req, &resp, &ctx));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 0 not found");
  }
  ASSERT_OK(master_->WaitForTxnManagerInit());
  ASSERT_TRUE(master_->txn_manager()->initialized_);
}

// Scenario to verify that the lazy initialization of the TxnManager works as
// expected in the presence of multiple concurrent calls.
TEST_F(TxnManagerTest, LazyInitializationConcurrentCalls) {
  // In this functor CHECK_ is used instead of ASSERT_ because it's targeted
  // for multi-thread use.
  const auto txn_initiator = [this](size_t txn_num, Barrier* b) {
    // Create its own proxy: this is important if trying to create more
    // concurrency since a proxy serializes RPC calls.
    TxnManagerServiceProxy p(
        cluster_->messenger(),
        cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host());
    for (auto id = 0; id < txn_num; ++id) {
      RpcController ctx;
      PrepareRpcController(&ctx);
      GetTransactionStateRequestPB req;
      GetTransactionStateResponsePB resp;
      req.set_txn_id(id);
      b->Wait();
      CHECK_OK(proxy_->GetTransactionState(req, &resp, &ctx));
      CHECK(resp.has_error());
      auto s = StatusFromPB(resp.error().status());
      CHECK(s.IsNotFound()) << s.ToString();
    }
  };

  static constexpr int64_t kNumCallsPerThread = 8;
  const int kNumCPUs = base::NumCPUs();
  const size_t kNumThreads = 2 * kNumCPUs;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  Barrier barrier(kNumThreads);
  for (auto idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back(txn_initiator, kNumCallsPerThread, &barrier);
  }
  for (auto& t : threads) {
    t.join();
  }

  // TxnManager should be initialized, of course.
  ASSERT_OK(master_->WaitForTxnManagerInit());
  ASSERT_TRUE(master_->txn_manager()->initialized_);
}

// Verify the basic functionality when TxnManager is initialized in a
// non-lazy manner.
TEST_F(TxnManagerTest, NonlazyInitialization) {
  FLAGS_txn_manager_lazily_initialized = false;
  cluster_.reset(new InternalMiniCluster(env_, opts_));
  ASSERT_OK(Start());
  ASSERT_FALSE(master_->txn_manager()->is_lazily_initialized_);
  // Eventually, TxnManager should come up initialized: master initializes
  // it on startup in case of non-lazy initialization mode.
  ASSERT_OK(master_->WaitForTxnManagerInit());
  ASSERT_TRUE(master_->txn_manager()->initialized_);

  // Shutdown and start the cluster again. This is to verify that initialization
  // code works as expected when the transaction status table already exists.
  cluster_->Shutdown();
  ASSERT_OK(Start());
  ASSERT_FALSE(master_->txn_manager()->is_lazily_initialized_);
  ASSERT_OK(master_->WaitForTxnManagerInit());
  ASSERT_TRUE(master_->txn_manager()->initialized_);

  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    CommitTransactionRequestPB req;
    CommitTransactionResponsePB resp;
    req.set_txn_id(0);
    ASSERT_OK(proxy_->CommitTransaction(req, &resp, &ctx));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 0 not found");
  }
}

// This is scenario calls almost all methods of the TxnManager.
TEST_F(TxnManagerTest, AbortedTransactionLifecycle) {
  const auto fetch_txn_status = [this] (int64_t txn_id, TxnStatePB* state) {
    RpcController ctx;
    PrepareRpcController(&ctx);
    GetTransactionStateRequestPB req;
    GetTransactionStateResponsePB resp;
    req.set_txn_id(txn_id);
    ASSERT_OK(proxy_->GetTransactionState(req, &resp, &ctx));
    ASSERT_FALSE(resp.has_error())
        << StatusFromPB(resp.error().status()).ToString();
    ASSERT_TRUE(resp.has_state());
    *state = resp.state();
  };

  int64_t txn_id = -1;
  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    BeginTransactionRequestPB req;
    BeginTransactionResponsePB resp;
    ASSERT_OK(proxy_->BeginTransaction(req, &resp, &ctx));
    ASSERT_FALSE(resp.has_error())
        << StatusFromPB(resp.error().status()).ToString();
    ASSERT_TRUE(resp.has_txn_id());
    txn_id = resp.txn_id();
    ASSERT_LE(0, txn_id);
    ASSERT_TRUE(resp.has_keepalive_millis());
    ASSERT_EQ(FLAGS_txn_keepalive_interval_ms, resp.keepalive_millis());
    TxnStatePB txn_state;
    NO_FATALS(fetch_txn_status(txn_id, &txn_state));
    ASSERT_EQ(TxnStatePB::OPEN, txn_state);
  }

  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    KeepTransactionAliveRequestPB req;
    KeepTransactionAliveResponsePB resp;
    req.set_txn_id(txn_id);
    ASSERT_OK(proxy_->KeepTransactionAlive(req, &resp, &ctx));
    ASSERT_FALSE(resp.has_error())
        << StatusFromPB(resp.error().status()).ToString();
    TxnStatePB txn_state;
    NO_FATALS(fetch_txn_status(txn_id, &txn_state));
    ASSERT_EQ(TxnStatePB::OPEN, txn_state);
  }

  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    CommitTransactionRequestPB req;
    CommitTransactionResponsePB resp;
    req.set_txn_id(txn_id);
    ASSERT_OK(proxy_->CommitTransaction(req, &resp, &ctx));
    ASSERT_FALSE(resp.has_error())
        << StatusFromPB(resp.error().status()).ToString();
    TxnStatePB txn_state;
    NO_FATALS(fetch_txn_status(txn_id, &txn_state));
    ASSERT_EQ(TxnStatePB::COMMIT_IN_PROGRESS, txn_state);
  }

  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    AbortTransactionRequestPB req;
    AbortTransactionResponsePB resp;
    req.set_txn_id(txn_id);
    ASSERT_OK(proxy_->AbortTransaction(req, &resp, &ctx));
    ASSERT_FALSE(resp.has_error())
        << StatusFromPB(resp.error().status()).ToString();
    TxnStatePB txn_state;
    NO_FATALS(fetch_txn_status(txn_id, &txn_state));
    ASSERT_EQ(TxnStatePB::ABORTED, txn_state);
  }

  // Try to send keep-alive for already aborted transaction.
  {
    RpcController ctx;
    PrepareRpcController(&ctx);
    KeepTransactionAliveRequestPB req;
    KeepTransactionAliveResponsePB resp;
    req.set_txn_id(txn_id);
    ASSERT_OK(proxy_->KeepTransactionAlive(req, &resp, &ctx));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        Substitute("transaction ID $0 is already in terminal state", txn_id));
    // The transaction should stay in ABORTED state, of course.
    TxnStatePB txn_state;
    NO_FATALS(fetch_txn_status(txn_id, &txn_state));
    ASSERT_EQ(TxnStatePB::ABORTED, txn_state);
  }
}

TEST_F(TxnManagerTest, BeginManyTransactions) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // In this functor CHECK_ is used instead of ASSERT_ because it's targeted
  // for multi-thread use, and ASSERT_ macros do not seem working as expected
  // in such case.
  const auto txn_initiator = [this](
      size_t txn_num,
      vector<int64_t>* txn_ids) {
    // Create its own proxy: this is important if trying to create more
    // concurrency since a proxy serializes RPC calls.
    TxnManagerServiceProxy p(
        cluster_->messenger(),
        cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host());
    int64_t max_txn_id = -1;
    for (auto id = 0; id < txn_num; ++id) {
      BeginTransactionResponsePB resp;
      while (true) {
        RpcController ctx;
        PrepareRpcController(&ctx);
        BeginTransactionRequestPB req;
        resp.Clear();
        auto s = p.BeginTransaction(req, &resp, &ctx);
        // The only acceptable non-OK status here is Status::ServiceUnavailable.
        if (s.IsServiceUnavailable() ||
            (resp.has_error() &&
             StatusFromPB(resp.error().status()).IsServiceUnavailable())) {
          SleepFor(MonoDelta::FromMilliseconds(10));
          continue;
        }
        break;
      }
      CHECK(!resp.has_error()) << StatusFromPB(resp.error().status()).ToString();
      CHECK(resp.has_txn_id());
      int64_t txn_id = resp.txn_id();
      CHECK_GT(txn_id, max_txn_id);
      max_txn_id = txn_id;
      if (txn_ids) {
        txn_ids->emplace_back(txn_id);
      }
      CHECK(resp.has_keepalive_millis());
      CHECK_EQ(FLAGS_txn_keepalive_interval_ms, resp.keepalive_millis());
    }
  };

  // First, a simple sequential case: start many transactions one after another.
  // The point here is to make sure the TxnManager:
  //   * takes care adding new range partitions to the transaction status table
  //   * transaction identifiers assigned to the newly started transactions are
  //       ** unique
  //       ** increase monotonically
  {
    const int64_t kNumTransactions =
        FLAGS_txn_manager_status_table_range_partition_span * 3;

    // TxnManager is lazily initialized, so no tablets of the transaction
    // status tablet should be created yet.
    const auto txn_tablets_before =
        cluster_->mini_tablet_server(0)->ListTablets();
    ASSERT_EQ(0, txn_tablets_before.size());

    vector<int64_t> txn_ids;
    txn_initiator(kNumTransactions, &txn_ids);
    ASSERT_EQ(kNumTransactions, txn_ids.size());
    int64_t prev_txn_id = -1;
    for (const auto& txn_id : txn_ids) {
      ASSERT_GT(txn_id, prev_txn_id);
      prev_txn_id = txn_id;
    }

    // Check that corresponding tablets have been created for the transaction
    // status table.
    const auto txn_tablets_after =
        cluster_->mini_tablet_server(0)->ListTablets();
    auto expected_tablets_num = 1 +
        prev_txn_id / FLAGS_txn_manager_status_table_range_partition_span;
    ASSERT_EQ(expected_tablets_num, txn_tablets_after.size());
  }

  // A more complex case: run multiple threads, each starting many transactions.
  // Make sure the generated transaction identifiers are unique.
  {
    const int64_t kNumTransactionsPerThread =
        FLAGS_txn_manager_status_table_range_partition_span * 2;
    const int kNumCPUs = base::NumCPUs();
    const size_t kNumThreads = 2 * kNumCPUs;
    vector<thread> threads;
    threads.reserve(kNumThreads);
    vector<vector<int64_t>> txn_ids_per_thread;
    txn_ids_per_thread.resize(kNumThreads);
    for (auto& slice : txn_ids_per_thread) {
      slice.reserve(kNumTransactionsPerThread);
    }
    for (auto idx = 0; idx < kNumThreads; ++idx) {
      threads.emplace_back(txn_initiator,
                           kNumTransactionsPerThread,
                           &txn_ids_per_thread[idx]);
    }
    for (auto& t : threads) {
      t.join();
    }

    // Verify the uniqueness of the identifiers across all the threads. Instead
    // of sort/unique, use std::unordered_set.
    unordered_set<int64_t> txn_ids;
    size_t total_size = 0;
    for (const auto& slice: txn_ids_per_thread) {
      EXPECT_EQ(kNumTransactionsPerThread, slice.size());
      txn_ids.insert(slice.begin(), slice.end());
      total_size += slice.size();
    }
    ASSERT_EQ(kNumTransactionsPerThread * kNumThreads, total_size);
    ASSERT_EQ(total_size, txn_ids.size());
  }

  // Now start a single transaction to get the highest assigned txn_id so far.
  // This is to check for the number of tablets in the transaction status table
  // after all this activity.
  {
    vector<int64_t> txn_ids;
    txn_initiator(1, &txn_ids);
    ASSERT_EQ(1, txn_ids.size());
    const auto txn_tablets = cluster_->mini_tablet_server(0)->ListTablets();
    auto expected_tablets_num = 1 +
        txn_ids[0] / FLAGS_txn_manager_status_table_range_partition_span;
    ASSERT_EQ(expected_tablets_num, txn_tablets.size());
  }
}

// TODO(aserbin): add test scenarios involving a multi-master Kudu cluster
//                (hence there will be multiple TxnManager instances) and verify
//                how all this works in case of frequent master re-elections.

TEST_F(TxnManagerTest, KeepTransactionAliveNonExistingTxnId) {
  RpcController ctx;
  PrepareRpcController(&ctx);
  KeepTransactionAliveRequestPB req;
  req.set_txn_id(123);
  KeepTransactionAliveResponsePB resp;
  ASSERT_OK(proxy_->KeepTransactionAlive(req, &resp, &ctx));
  ASSERT_TRUE(resp.has_error());
  auto s = StatusFromPB(resp.error().status());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 123 not found");
}

} // namespace transactions
} // namespace kudu
