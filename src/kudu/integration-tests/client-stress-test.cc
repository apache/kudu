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
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
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
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(leader_memory_pressure_rejections);
METRIC_DECLARE_counter(follower_memory_pressure_rejections);

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDeleteIgnore;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class ClientStressTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts = default_opts();
    if (multi_master()) {
      opts.num_masters = 3;
    }
    opts.num_tablet_servers = 3;
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    alarm(0);
    if (cluster_) {
      cluster_->Shutdown();
    }
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
    constexpr int kNumThreads = 60;
    vector<thread> threads;
    threads.reserve(kNumThreads);
    Random rng(run);
    for (int i = 0; i < kNumThreads; i++) {
      int32_t start_key = rng.Next32();
      threads.emplace_back([this, client, &go_latch, start_key]() {
        this->ScannerThread(client.get(), &go_latch, start_key);
      });
    }
    SleepFor(MonoDelta::FromMilliseconds(50));

    go_latch.CountDown();

    for (auto& t : threads) {
      t.join();
    }
  }
}

// Override the base test to run in multi-master mode.
class ClientStressTest_MultiMaster : public ClientStressTest {
 protected:
  bool multi_master() const override {
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

#if !defined(THREAD_SANITIZER) && !defined(ADDRESS_SANITIZER)
// Test for stress scenarios exercising meta-cache lookup path. The scenarios
// not to be run under sanitizer builds since they are CPU intensive.
class MetaCacheLookupStressTest : public ClientStressTest {
 public:
  void SetUp() override {
    using client::sp::shared_ptr;

    // All scenarios of this test are supposed to be CPU-intensive, so check
    // for KUDU_ALLOW_SLOW_TESTS during the SetUp() phase.
    SKIP_IF_SLOW_NOT_ALLOWED();
    ClientStressTest::SetUp();

    KuduSchemaBuilder b;
    b.AddColumn(kKeyColumn)->Type(KuduColumnSchema::INT64)->NotNull();
    b.AddColumn(kStrColumn)->Type(KuduColumnSchema::STRING)->NotNull();
    b.SetPrimaryKey({ kKeyColumn, kStrColumn });
    KuduSchema schema;
    ASSERT_OK(b.Build(&schema));

    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

    unique_ptr<KuduTableCreator> tc(client_->NewTableCreator());

    // The table contains many tablets, so give it extra time to create
    // corresponding tablets.
    ASSERT_OK(tc->table_name(kTableName)
        .schema(&schema)
        .num_replicas(1)
        .add_hash_partitions({ kKeyColumn, kStrColumn }, 64)
        .timeout(MonoDelta::FromSeconds(300))
        .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));

    // Prime the client's meta-cache.
    shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    for (int32_t idx = 0; idx < kNumRows; ++idx) {
      unique_ptr<KuduDeleteIgnore> op(table_->NewDeleteIgnore());
      ASSERT_OK(op->mutable_row()->SetInt64(0, idx));
      ASSERT_OK(op->mutable_row()->SetString(1,
          string(1, static_cast<char>(idx % 128))));
      ASSERT_OK(session->Apply(op.release()));
    }
    ASSERT_OK(session->Flush());
  }

 protected:
  static constexpr auto kNumRows = 1000000;
  static constexpr const char* kTableName = "meta_cache_lookup";
  static constexpr const char* kKeyColumn = "key";
  static constexpr const char* kStrColumn = "str_val";

  ExternalMiniClusterOptions default_opts() const override {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 10;
    return opts;
  }

  vector<unique_ptr<KuduDeleteIgnore>> GenerateOperations(int64_t num_ops) {
    vector<unique_ptr<KuduDeleteIgnore>> ops;
    ops.reserve(num_ops);
    for (int64_t idx = 0; idx < num_ops; ++idx) {
      unique_ptr<KuduDeleteIgnore> op(table_->NewDeleteIgnore());
      CHECK_OK(op->mutable_row()->SetInt64(0, idx));
      CHECK_OK(op->mutable_row()->SetString(
          1, std::to_string(idx) + string(1, static_cast<char>(idx % 128))));
      ops.emplace_back(std::move(op));
    }
    return ops;
  }

  client::sp::shared_ptr<KuduClient> client_;
  client::sp::shared_ptr<KuduTable> table_;
};

// This test scenario creates a table with many partitions and generates a lot
// of rows, stressing meta-cache fast lookup path in the client.
TEST_F(MetaCacheLookupStressTest, PerfSynthetic) {
  vector<unique_ptr<KuduDeleteIgnore>> ops(GenerateOperations(kNumRows));

  // Start the stopwatch and run the benchmark. All the lookups in the
  // meta-cache should be fast since the meta-cache has been populated
  // by the activity above.
  Stopwatch sw;
  sw.start();
  {
    auto& meta_cache = client_->data_->meta_cache_;
    for (const auto& op : ops) {
      client::internal::MetaCacheEntry entry;
      ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(
          table_.get(), table_->partition_schema().EncodeKey(op->row()), &entry));
    }
  }
  sw.stop();

  const auto wall_spent_ms = sw.elapsed().wall_millis();
  LOG(INFO) << Substitute("Total time spent: $0 ms", wall_spent_ms);
  LOG(INFO) << Substitute("Time per row: $0 ms", wall_spent_ms / kNumRows);
}

TEST_F(MetaCacheLookupStressTest, Perf) {
  vector<unique_ptr<KuduDeleteIgnore>> ops(GenerateOperations(kNumRows));
  // Create a session using manual flushing mode and set the buffer to be
  // large enough to accommodate all the generated operations at once.
  client::sp::shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(session->SetMutationBufferSpace(64 * 1024 * 1024));

  // Start the stopwatch and run the benchmark. All the lookups in the
  // meta-cache should be fast since the meta-cache has been populated
  // by the activity above. The lookups starts once Apply() is called,
  // and to make sure every entry has been lookup and, KuduSession::Flush()
  // is called: in addition to lookups in the meta-cache, it sends all
  // the accumulated operations to the server.
  Stopwatch sw;
  sw.start();
  for (auto& op : ops) {
    ASSERT_OK(session->Apply(op.release()));
  }
  ASSERT_OK(session->Flush());
  sw.stop();

  const auto wall_spent_ms = sw.elapsed().wall_millis();
  LOG(INFO) << Substitute("Total time spent: $0 ms", wall_spent_ms);
  LOG(INFO) << Substitute("Time per row: $0 ms", wall_spent_ms / kNumRows);
}
#endif // #if !defined(THREAD_SANITIZER) && !defined(ADDRESS_SANITIZER)

} // namespace kudu
