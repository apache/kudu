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

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/value.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(leader_memory_pressure_rejections);
METRIC_DECLARE_counter(follower_memory_pressure_rejections);

using strings::Substitute;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

using client::KuduClient;
using client::KuduScanner;
using client::KuduTable;
using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;

class ClientStressTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts = default_opts();
    if (multi_master()) {
      opts.num_masters = 3;
    }
    opts.num_tablet_servers = 3;
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
  }

  virtual void TearDown() OVERRIDE {
    alarm(0);
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  void ScannerThread(KuduClient* client, const CountDownLatch* go_latch, int32_t start_key) {
    client::sp::shared_ptr<KuduTable> table;
    CHECK_OK(client->OpenTable(TestWorkload::kDefaultTableName, &table));
    vector<string> rows;

    go_latch->Wait();

    KuduScanner scanner(table.get());
    CHECK_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
        "key", client::KuduPredicate::GREATER_EQUAL,
        client::KuduValue::FromInt(start_key))));
    CHECK_OK(ScanToStrings(&scanner, &rows));
  }

  virtual bool multi_master() const {
    return false;
  }

  virtual ExternalMiniClusterOptions default_opts() const {
    return ExternalMiniClusterOptions();
  }

  unique_ptr<ExternalMiniCluster> cluster_;
};

// Stress test a case where most of the operations are expected to time out.
// This is a regression test for various bugs we've seen in timeout handling,
// especially with concurrent requests.
TEST_F(ClientStressTest, TestLookupTimeouts) {
  const int kSleepMillis = AllowSlowTests() ? 5000 : 100;

  TestWorkload work(cluster_.get());
  work.set_num_write_threads(64);
  work.set_write_timeout_millis(10);
  work.set_timeout_allowed(true);
  work.Setup();
  work.Start();
  SleepFor(MonoDelta::FromMilliseconds(kSleepMillis));
}

// Regression test for KUDU-1104, a race in which multiple scanners racing to populate a
// cold meta cache on a shared Client would crash.
//
// This test creates a table with a lot of tablets (so that we require many round-trips to
// the master to populate the meta cache) and then starts a bunch of parallel threads which
// scan starting at random points in the key space.
TEST_F(ClientStressTest, TestStartScans) {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i), "log_preallocate_segments", "0"));
  }
  TestWorkload work(cluster_.get());
  work.set_num_tablets(40);
  work.set_num_replicas(1);
  work.Setup();

  // We run the guts of the test several times -- it takes a while to build
  // the 40 tablets above, but the actual scans are very fast since the table
  // is empty.
  for (int run = 1; run <= (AllowSlowTests() ? 10 : 2); run++) {
    LOG(INFO) << "Starting run " << run;
    client::sp::shared_ptr<KuduClient> client;
    CHECK_OK(cluster_->CreateClient(nullptr, &client));

    CountDownLatch go_latch(1);
    vector<scoped_refptr<Thread> > threads;
    const int kNumThreads = 60;
    Random rng(run);
    for (int i = 0; i < kNumThreads; i++) {
      int32_t start_key = rng.Next32();
      scoped_refptr<kudu::Thread> new_thread;
      CHECK_OK(kudu::Thread::Create(
          "test", strings::Substitute("test-scanner-$0", i),
          &ClientStressTest_TestStartScans_Test::ScannerThread, this,
          client.get(), &go_latch, start_key,
          &new_thread));
      threads.push_back(new_thread);
    }
    SleepFor(MonoDelta::FromMilliseconds(50));

    go_latch.CountDown();

    for (const scoped_refptr<kudu::Thread>& thr : threads) {
      CHECK_OK(ThreadJoiner(thr.get()).Join());
    }
  }
}

// Override the base test to run in multi-master mode.
class ClientStressTest_MultiMaster : public ClientStressTest {
 protected:
  virtual bool multi_master() const OVERRIDE {
    return true;
  }
};

// Stress test a case where most of the operations are expected to time out.
// This is a regression test for KUDU-614 - it would cause a deadlock prior
// to fixing that bug.
TEST_F(ClientStressTest_MultiMaster, TestLeaderResolutionTimeout) {
  TestWorkload work(cluster_.get());
  work.set_num_write_threads(64);

  // This timeout gets applied to the master requests. It's lower than the
  // amount of time that we sleep the masters, to ensure they timeout.
  work.set_client_default_rpc_timeout_millis(250);
  // This is the time budget for the whole request. It has to be longer than
  // the above timeout so that the client actually attempts to resolve
  // the leader.
  work.set_write_timeout_millis(280);
  work.set_timeout_allowed(true);
  work.Setup();

  work.Start();

  ASSERT_OK(cluster_->tablet_server(0)->Pause());
  ASSERT_OK(cluster_->tablet_server(1)->Pause());
  ASSERT_OK(cluster_->tablet_server(2)->Pause());
  ASSERT_OK(cluster_->master(0)->Pause());
  ASSERT_OK(cluster_->master(1)->Pause());
  ASSERT_OK(cluster_->master(2)->Pause());
  SleepFor(MonoDelta::FromMilliseconds(300));
  ASSERT_OK(cluster_->tablet_server(0)->Resume());
  ASSERT_OK(cluster_->tablet_server(1)->Resume());
  ASSERT_OK(cluster_->tablet_server(2)->Resume());
  ASSERT_OK(cluster_->master(0)->Resume());
  ASSERT_OK(cluster_->master(1)->Resume());
  ASSERT_OK(cluster_->master(2)->Resume());
  SleepFor(MonoDelta::FromMilliseconds(100));

  // Set an explicit timeout. This test has caused deadlocks in the past.
  // Also make sure to dump stacks before the alarm goes off.
  PstackWatcher watcher(MonoDelta::FromSeconds(30));
  alarm(60);
}


// Override the base test to start a cluster with a low memory limit.
class ClientStressTest_LowMemory : public ClientStressTest {
 protected:
  virtual ExternalMiniClusterOptions default_opts() const OVERRIDE {
    // There's nothing scientific about this number; it must be low enough to
    // trigger memory pressure request rejection yet high enough for the
    // servers to make forward progress.
    const int kMemLimitBytes = 64 * 1024 * 1024;
    ExternalMiniClusterOptions opts;
    opts.extra_tserver_flags.push_back(Substitute(
        "--memory_limit_hard_bytes=$0", kMemLimitBytes));
    opts.extra_tserver_flags.emplace_back("--memory_limit_soft_percentage=0");
    // Since --memory_limit_soft_percentage=0, any nonzero block cache usage
    // will cause memory pressure. Since that's part of the point of the test,
    // we'll allow it.
    opts.extra_tserver_flags.push_back("--force_block_cache_capacity");
    return opts;
  }
};

// Stress test where, due to absurdly low memory limits, many client requests
// are rejected, forcing the client to retry repeatedly.
TEST_F(ClientStressTest_LowMemory, TestMemoryThrottling) {
#ifdef THREAD_SANITIZER
  // TSAN tests run much slower, so we don't want to wait for as many
  // rejections before declaring the test to be passed.
  const int64_t kMinRejections = 20;
#else
  const int64_t kMinRejections = 100;
#endif

  const MonoDelta kMaxWaitTime = MonoDelta::FromSeconds(60);

  TestWorkload work(cluster_.get());
  work.Setup();
  work.Start();

  // Wait until we've rejected some number of requests.
  MonoTime deadline = MonoTime::Now() + kMaxWaitTime;
  while (true) {
    int64_t total_num_rejections = 0;

    // It can take some time for the tablets (and their metric entities) to
    // appear on every server. Rather than explicitly wait for that above,
    // we'll just treat the lack of a metric as non-fatal. If the entity
    // or metric is truly missing, we'll eventually timeout and fail.
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      int64_t value;
      Status s = itest::GetInt64Metric(
          cluster_->tablet_server(i)->bound_http_hostport(),
          &METRIC_ENTITY_tablet,
          nullptr,
          &METRIC_leader_memory_pressure_rejections,
          "value",
          &value);
      if (!s.IsNotFound()) {
        ASSERT_OK(s);
        total_num_rejections += value;
      }
      s = itest::GetInt64Metric(
          cluster_->tablet_server(i)->bound_http_hostport(),
          &METRIC_ENTITY_tablet,
          nullptr,
          &METRIC_follower_memory_pressure_rejections,
          "value",
          &value);
      if (!s.IsNotFound()) {
        ASSERT_OK(s);
        total_num_rejections += value;
      }
    }
    if (total_num_rejections >= kMinRejections) {
      break;
    } else if (MonoTime::Now() > deadline) {
      FAIL() << "Ran for " << kMaxWaitTime.ToString() << ", deadline expired and only saw "
             << total_num_rejections << " memory rejections";
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
}

// This test just creates a bunch of clients and makes sure that the generated client ids
// are unique among the created clients.
TEST_F(ClientStressTest, TestUniqueClientIds) {
  set<string> client_ids;
  for (int i = 0; i < 1000; i++) {
    client::sp::shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    const string& client_id = client->data_->client_id_;
    auto result = client_ids.emplace(client_id);
    EXPECT_TRUE(result.second) << "Unique id generation failed. New client id: "
                               << client_id
                               << " had already been used. Generated ids: "
                               << JoinStrings(client_ids, ",");
  }
}

} // namespace kudu
