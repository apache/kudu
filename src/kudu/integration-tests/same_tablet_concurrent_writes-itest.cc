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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/spinlock_profiling.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_inserter_threads, 32,
             "number of threads performing concurrent inserts into the tablets");
DEFINE_int32(runtime_sec, 5, "how long to run test scenarios (in seconds)");

DECLARE_bool(log_inject_latency);
DECLARE_int32(group_commit_queue_size_bytes);
DECLARE_int32(log_inject_latency_ms_mean);
DECLARE_int32(log_inject_latency_ms_stddev);
DECLARE_int32(log_segment_size_mb);
DECLARE_int32(max_num_columns);
DECLARE_int32(rpc_num_service_threads);
DECLARE_int32(rpc_service_queue_length);

METRIC_DECLARE_counter(rpcs_queue_overflow);
METRIC_DECLARE_counter(rpcs_timed_out_in_queue);
METRIC_DECLARE_gauge_uint64(spinlock_contention_time);
METRIC_DECLARE_histogram(handler_latency_kudu_consensus_ConsensusService_UpdateConsensus);
METRIC_DECLARE_histogram(handler_latency_kudu_tserver_TabletServerService_Write);
METRIC_DECLARE_histogram(op_apply_queue_time);
METRIC_DECLARE_histogram(op_apply_run_time);
METRIC_DECLARE_histogram(reactor_active_latency_us);
METRIC_DECLARE_histogram(rpc_incoming_queue_time);

using kudu::KuduPartialRow;
using kudu::client::KuduClient;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::endl;
using std::ostringstream;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

// The purpose of this test is to check for lock contention when running
// multiple write operations on the same tablet concurrently. Multiple threads
// are involved in the process of pushing Raft consensus updates corresponding
// to the incoming write requests, and the test pinpoints the contention among
// various threads involved.
class SameTabletConcurrentWritesTest : public KuduTest {
 public:
  static constexpr int kNumColumns = 250;

  SameTabletConcurrentWritesTest()
      : num_inserter_threads_(FLAGS_num_inserter_threads),
        runtime_(MonoDelta::FromSeconds(FLAGS_runtime_sec)),
        do_run_(true) {
  }

  void SetUp() override {
    KuduTest::SetUp();

    // Use schema with multiple string fields to make rows heavier.
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(client::KuduColumnSchema::INT64)->
        NotNull()->PrimaryKey();
    for (auto i = 1; i < kNumColumns; ++i) {
      schema_builder.AddColumn(Substitute("col$0", i))->
          Type(client::KuduColumnSchema::STRING)->NotNull();
    }
    ASSERT_OK(schema_builder.Build(&schema_));
  }

  Status CreateTestTable(int num_replicas) {
    client::sp::shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    return table_creator->table_name(kTableName)
        .schema(&schema_)
        .num_replicas(num_replicas)
        .add_hash_partitions({ "key" }, 2)
        .Create();
  }

  void InserterTask(size_t task_idx, Status* result_status, size_t* counter) {
    using client::sp::shared_ptr;
    static constexpr const char kValPattern[] =
        "$0.00000000000000000000000000000000000000000000000000000000000000000"
        "$0.9876543210123456789"
        "$0.11111111111111111111111111111111111111111111111111111111111111111"
        "$0.0123456789abcdef.$0.abcdef0123456789."
        "$0.22222222222222222222222222222222222222222222222222222222222222222"
        "$0.`1234567890-=qwertyuioop[]asdfghjkl;'zxcvbnm,./."
        "$0.33333333333333333333333333333333333333333333333333333333333333333"
        "$0.9876543210123456789"
        "$0.44444444444444444444444444444444444444444444444444444444444444444"
        "$0.9876543210123456789"
        "$0.55555555555555555555555555555555555555555555555555555555555555555"
        "$0.9876543210123456789"
        "$0.66666666666666666666666666666666666666666666666666666666666666666"
        "$0.9876543210123456789"
        "$0.77777777777777777777777777777777777777777777777777777777777777777"
        "$0.9876543210123456789"
        "$0.88888888888888888888888888888888888888888888888888888888888888888"
        "$0.9876543210123456789"
        "$0.99999999999999999999999999999999999999999999999999999999999999999";

    #define RET_IF_NOT_OK(_s, _session) \
      do { \
        const Status s = (_s); \
        Status status = s; \
        if (!s.ok()) { \
          const shared_ptr<KuduSession>& ses((_session)); \
          if (ses) { \
            vector<KuduError*> errors; \
            ElementDeleter deleter(&errors); \
            ses->GetPendingErrors(&errors, nullptr); \
            if (!errors.empty()) { \
              for (const auto* err : errors) { \
                status = status.CloneAndAppend(err->status().message()); \
              } \
            } \
          } \
          *result_status = status; \
          return; \
        } \
      } while (false)

    shared_ptr<KuduClient> client;
    shared_ptr<KuduSession> session;
    RET_IF_NOT_OK(cluster_->CreateClient(nullptr, &client), session);
    session = client->NewSession();
    // Flush upon applying a row: this is to flood tablet servers with
    // requests.
    RET_IF_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC), session);
    session->SetTimeoutMillis(15 * 1000);

    shared_ptr<KuduTable> table;
    RET_IF_NOT_OK(client->OpenTable(kTableName, &table), session);
    size_t i = task_idx;
    while (do_run_) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      RET_IF_NOT_OK(row->SetInt64(0, i), session);
      i += num_inserter_threads_;
      for (auto idx = 1; idx < kNumColumns; ++idx) {
        RET_IF_NOT_OK(row->SetString(idx, Substitute(kValPattern, i)), session);
      }
      RET_IF_NOT_OK(session->Apply(insert.release()), session);
      ++(*counter);

    }

    *result_status = Status::OK();
  }

 protected:
  static constexpr const char* const kTableName = "test";

  const int num_inserter_threads_;
  const MonoDelta runtime_;
  InternalMiniClusterOptions opts_;
  std::unique_ptr<InternalMiniCluster> cluster_;
  KuduSchema schema_;
  std::atomic<bool> do_run_;
};

// Run many inserters into the same tablet when WAL sync calls are slow due to
// injected latency; report on metrics like spinlock contention cycles and
// the number of RPC queue's overflows.
TEST_F(SameTabletConcurrentWritesTest, InsertsOnly) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr int kNumTabletServers = 3;

  // Custom settings for kudu-master's flags.
  //
  // Increase number of columns in the table to make every write operation
  // heavier.
  FLAGS_max_num_columns = kNumColumns;

  // Custom settings for kudu-tserver's flags.
  //
  // Inject latency into WAL sync operations. This is to allow the point of
  // lock contention in RaftConsensus::UpdateReplica() to manifest itself.
  FLAGS_log_inject_latency = true;
  FLAGS_log_inject_latency_ms_mean = 200;
  FLAGS_log_inject_latency_ms_stddev = 0;

  // Another lock contention manifests itself when RPC service threads are
  // trying to obtain a lock already taken by another thread, where the latter
  // thread is waiting for available space in the WAL's entry batch queue.
  // Instead of sending huge amount of data to fill it in, let's reduce the
  // capacity of the entry batch queue to make the lock contention manifesting
  // itself in this test scenario.
  FLAGS_group_commit_queue_size_bytes = 64;

  // Keep log segments small to hit the injected latency as often as possible.
  FLAGS_log_segment_size_mb = 1;

  // To make the lock contention manifest itself at the side where RPCs are
  // arriving, let's decrease the number of RPC workers and make the RPC
  // queue shorter. An alternative approach might be creating many more
  // concurrent writer threads and write much more data.
  FLAGS_rpc_num_service_threads = 2;
  FLAGS_rpc_service_queue_length = 3;

  InternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTabletServers;
  cluster_.reset(new InternalMiniCluster(env_, opts));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(CreateTestTable(kNumTabletServers));

  vector<thread> threads;
  threads.reserve(num_inserter_threads_);
  vector<Status> statuses(num_inserter_threads_);
  vector<size_t> counters(num_inserter_threads_, 0);
  for (auto idx = 0; idx < num_inserter_threads_; idx++) {
    threads.emplace_back(&SameTabletConcurrentWritesTest::InserterTask, this,
                         idx, &statuses[idx], &counters[idx]);
  }

  SleepFor(runtime_);

  do_run_ = false;
  for_each(threads.begin(), threads.end(), [](thread& t) { t.join(); });

  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }

  int64_t num_queue_overflows = 0;
  for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
    const auto& ent = cluster_->mini_tablet_server(i)->server()->metric_entity();

    // spinlock_contention_time is the same for each tablet server because
    // this test scenario uses all-in-one-process internal mini-cluster.
    auto gauge = METRIC_spinlock_contention_time.InstantiateFunctionGauge(
          ent, []() { return GetSpinLockContentionMicros(); });
    LOG(INFO) << "spinlock_contention_time for tserver " << i
              << ": " << gauge->value();

    for (auto* elem : {
        &METRIC_rpcs_queue_overflow,
        &METRIC_rpcs_timed_out_in_queue,
    }) {
      auto counter = elem->Instantiate(ent);
      const char* const name = counter->prototype()->name();
      if (strcmp("rpcs_queue_overflow", name) == 0) {
        num_queue_overflows += counter->value();
      }
      LOG(INFO) << "Counter value for tserver " << i
                << " on " << name << ": " << counter->value();
    }

    for (auto* elem : {
        &METRIC_handler_latency_kudu_consensus_ConsensusService_UpdateConsensus,
        &METRIC_handler_latency_kudu_tserver_TabletServerService_Write,
        &METRIC_op_apply_queue_time,
        &METRIC_op_apply_run_time,
        &METRIC_reactor_active_latency_us,
        &METRIC_rpc_incoming_queue_time,
    }) {
      auto hist = elem->Instantiate(ent);
      ostringstream ostr;
      ostr << "Stats for tserver " << i << " on " << elem->name() << ":" << endl;
      hist->histogram()->DumpHumanReadable(&ostr);
      LOG(INFO) << ostr.str();
    }
  }

  const double total = accumulate(counters.begin(), counters.end(), 0UL);
  LOG(INFO) << Substitute(
      "write RPC request rate: $0 req/sec",
      total / runtime_.ToSeconds());
  LOG(INFO) << Substitute(
      "total count of RPC queue overflows: $0", num_queue_overflows);
}

} // namespace itest
} // namespace kudu
