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
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/txn_manager.pb.h"
#include "kudu/master/txn_manager.proxy.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/barrier.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::rpc::RpcController;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_int32(rpc_service_queue_length);

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

// BeginTransaction implementation is moved into a follow-up changelist.
// TODO(aserbin): update this scenario once BeginTransction is here
TEST_F(TxnManagerTest, BeginTransactionRpc) {
  RpcController ctx;
  PrepareRpcController(&ctx);
  BeginTransactionRequestPB req;
  BeginTransactionResponsePB resp;
  ASSERT_OK(proxy_->BeginTransaction(req, &resp, &ctx));

  ASSERT_TRUE(resp.has_error());
  auto s = StatusFromPB(resp.error().status());
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "Not implemented: BeginTransaction is not supported yet");
}

// KeepTransactionAlive is not yet supported.
// TODO(aserbin): update this scenario once KeepTransactionAlive is implemented
TEST_F(TxnManagerTest, KeepTransactionAliveRpc) {
  RpcController ctx;
  PrepareRpcController(&ctx);
  KeepTransactionAliveRequestPB req;
  req.set_txn_id(0);
  KeepTransactionAliveResponsePB resp;
  ASSERT_OK(proxy_->KeepTransactionAlive(req, &resp, &ctx));
  ASSERT_TRUE(resp.has_error());
  auto s = StatusFromPB(resp.error().status());
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "Not implemented: KeepTransactionAlive is not supported yet");
}

} // namespace transactions
} // namespace kudu
