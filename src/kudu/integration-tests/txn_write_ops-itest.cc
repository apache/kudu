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
#include <cstdlib>
#include <deque>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/barrier.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::KuduPartialRow;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDeleteIgnore;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduInsertIgnore;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTransaction;
using kudu::client::KuduUpdate;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::cluster::TabletIdAndTableName;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
using kudu::transactions::TxnTokenPB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::atomic;
using std::deque;
using std::map;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

// The run-time flags below are for TxnWriteOpsITest.TxnWriteOpPerf scenario.
DEFINE_bool(prime_connections_to_tservers, true,
            "whether to open connections to tablet servers prior to sending "
            "transactional write operations");
DEFINE_uint32(clients, 8, "number of Kudu clients to run");
DEFINE_uint32(sessions_per_client, 1,
              "number of concurrent sessions per Kudu client: "
              "there will be --clients * --sessions_per_client concurrent "
              "writer threads in total, i.e. one writer thread per session");
DEFINE_uint32(benchmark_run_time_ms, 50,
              "time interval to run the benchmark, in milliseconds");
DEFINE_uint32(max_pending_txn_write_ops, 10,
              "setting for tserver's --tablet_max_pending_txn_write_ops flag");
DEFINE_bool(txn_enabled, true, "whether to use transactional sessions");

DECLARE_bool(enable_txn_system_client_init);
DECLARE_bool(tserver_txn_write_op_handling_enabled);
DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_int32(txn_participant_begin_op_inject_latency_ms);
DECLARE_int32(txn_participant_registration_inject_latency_ms);
DECLARE_int64(txn_system_client_op_timeout_ms);
DECLARE_uint32(tablet_max_pending_txn_write_ops);
DECLARE_uint32(txn_manager_status_table_num_replicas);
DECLARE_uint32(txn_staleness_tracker_interval_ms);

namespace kudu {

namespace {

Status BuildSchema(KuduSchema* schema) {
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32);
  return b.Build(schema);
}

unique_ptr<KuduInsert> BuildInsert(KuduTable* table, int64_t key) {
  unique_ptr<KuduInsert> op(table->NewInsert());
  KuduPartialRow* row = op->mutable_row();
  CHECK_OK(row->SetInt64(0, key));
  return op;
}

unique_ptr<KuduInsertIgnore> BuildInsertIgnore(KuduTable* table, int64_t key) {
  unique_ptr<KuduInsertIgnore> op(table->NewInsertIgnore());
  KuduPartialRow* row = op->mutable_row();
  CHECK_OK(row->SetInt64(0, key));
  return op;
}

unique_ptr<KuduDeleteIgnore> BuildDeleteIgnore(KuduTable* table, int64_t key) {
  unique_ptr<KuduDeleteIgnore> op(table->NewDeleteIgnore());
  KuduPartialRow* row = op->mutable_row();
  CHECK_OK(row->SetInt64(0, key));
  return op;
}

int64_t GetTxnId(const shared_ptr<KuduTransaction>& txn) {
  string txn_token;
  CHECK_OK(txn->Serialize(&txn_token));
  TxnTokenPB token;
  CHECK(token.ParseFromString(txn_token));
  CHECK(token.has_txn_id());
  return token.txn_id();
}

Status CountRows(KuduTable* table, size_t* num_rows) {
  KuduScanner scanner(table);
  RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_YOUR_WRITES));
  RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(scanner.Open());
  size_t count = 0;
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    count += batch.NumRows();
  }
  *num_rows = count;
  return Status::OK();
}

Status CountRows(KuduClient* client, const string& table_name, size_t* num_rows) {
  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));
  return CountRows(table.get(), num_rows);
}

Status GetSingleRowError(KuduSession* session) {
  vector<KuduError*> errors;
  ElementDeleter drop(&errors);
  bool overflowed;
  session->GetPendingErrors(&errors, &overflowed);
  CHECK(!overflowed);
  CHECK_EQ(1, errors.size());
  return errors.front()->status();
}

void InsertRows(KuduTable* table, KuduSession* session,
                int64_t count, int64_t start_key = 0) {
  for (int64_t key = start_key; key < start_key + count; ++key) {
    unique_ptr<KuduInsert> insert(BuildInsert(table, key));
    ASSERT_OK(session->Apply(insert.release()));
  }
}

} // anonymous namespace

class TxnWriteOpsITest : public ExternalMiniClusterITestBase {
 protected:
  TxnWriteOpsITest()
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
      : hb_interval_ms_(64),
        run_time_seconds_(3)
#else
      : hb_interval_ms_(16),
        run_time_seconds_(AllowSlowTests() ? 60 : 3)
#endif
  {
    CHECK_OK(BuildSchema(&schema_));
  }

  Status CreateTable() {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(kTableName)
                  .schema(&schema_)
                  .add_hash_partitions({ "key" }, kNumPartitions)
                  .num_replicas(kNumTabletServers)
                  .Create());
    return client_->OpenTable(kTableName, &table_);
  }

  // Create a test table and wait all replicas of its tablets running.
  // Output the UUIDs of tablets into the 'tablet_uuids', if it's non-null.
  void Prepare(vector<string>* tablet_uuids = nullptr) {
    ASSERT_OK(CreateTable());
    // In this test, replication factor is set to kNumTabletServers.
    const size_t total_replicas_num = kNumPartitions * kNumTabletServers;
    NO_FATALS(WaitForAllTabletsRunning(total_replicas_num, tablet_uuids));
  }

  // Assuming there is only one table in the system, this method awaits for
  // all replicas of that test table to be up and running.
  void WaitForAllTabletsRunning(size_t expected_total_replicas_num,
                                vector<string>* tablet_uuids = nullptr) {
    set<string> uuids;
    ASSERT_EVENTUALLY([&] {
      uuids.clear();
      size_t total_tablet_replicas_num = 0;
      for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
        vector<TabletIdAndTableName> tablets_info;
        ASSERT_OK(cluster_->WaitForTabletsRunning(
            cluster_->tablet_server(i), 0, kTimeout, &tablets_info));
        for (auto& info : tablets_info) {
          if (info.table_name != kTableName) {
            // There might be txn status tablets as well: skip those.
            continue;
          }
          ++total_tablet_replicas_num;
          EmplaceIfNotPresent(&uuids, info.tablet_id);
        }
      }
      ASSERT_EQ(expected_total_replicas_num, total_tablet_replicas_num);
      ASSERT_EQ(kNumPartitions, uuids.size());
    });
    if (tablet_uuids) {
      tablet_uuids->reserve(uuids.size());
      std::move(uuids.begin(), uuids.end(), std::back_inserter(*tablet_uuids));
    }
  }

 protected:
  static constexpr auto kNumRowsPerTxn = 8;
  static constexpr auto kNumTabletServers = 3;
  static constexpr auto kNumPartitions = 2;
  static constexpr const char* const kTableName = "txn_write_ops_test";
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  const int hb_interval_ms_;
  const int64_t run_time_seconds_;
  KuduSchema schema_;
  shared_ptr<KuduTable> table_;
  string tablet_uuid_;
};

// Make sure txn commit timestamp is being propagated to a client with a call
// to KuduTransaction::IsCommitComplete(). This includes committing a
// transaction synchronously by calling KuduTransaction::Commit().
TEST_F(TxnWriteOpsITest, CommitTimestampPropagation) {
  static constexpr int kRowsNum = 1000;

  const vector<string> master_flags = {
    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    "--txn_manager_enabled=true",

    // Scenarios based on this test fixture assume the txn status table
    // is created at start, not on first transaction-related operation.
    "--txn_manager_lazily_initialized=false",
  };
  NO_FATALS(StartCluster({ "--enable_txn_system_client_init=true" },
                         master_flags, kNumTabletServers));
  NO_FATALS(Prepare());

  // Start a transaction, write a bunch or rows into the test table, and then
  // commit the transaction asynchronously. Check for transaction status and
  // make sure the latest observed timestamp changes accordingly once the
  // transaction is committed.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    NO_FATALS(InsertRows(table_.get(), session.get(), kRowsNum));
    ASSERT_OK(session->Flush());
    ASSERT_EQ(0, session->CountPendingErrors());

    const auto ts_before_commit = client_->GetLatestObservedTimestamp();
    ASSERT_OK(txn->StartCommit());
    const auto ts_after_commit_async = client_->GetLatestObservedTimestamp();
    ASSERT_EQ(ts_before_commit, ts_after_commit_async);

    uint64_t ts_after_committed = 0;
    ASSERT_EVENTUALLY([&] {
      bool is_complete = false;
      Status completion_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
      ASSERT_TRUE(is_complete);
      ts_after_committed = client_->GetLatestObservedTimestamp();
    });
    ASSERT_GT(ts_after_committed, ts_before_commit);

    // A sanity check: calling IsCommitComplete() again after the commit
    // timestamp has been propagated doesn't change the timestamp observed
    // by the client.
    for (auto i = 0; i < 10; ++i) {
      bool is_complete = false;
      Status completion_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
      ASSERT_TRUE(is_complete);
      ASSERT_EQ(ts_after_committed, client_->GetLatestObservedTimestamp());
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    size_t count;
    ASSERT_OK(CountRows(table_.get(), &count));
    ASSERT_EQ(kRowsNum, count);
  }

  // Start a transaction, write a bunch or rows into the test table, and then
  // commit the transaction synchronously. Make sure the latest observed
  // timestamp changes accordingly once the transaction is committed.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    NO_FATALS(InsertRows(table_.get(), session.get(), kRowsNum, kRowsNum));
    ASSERT_OK(session->Flush());
    ASSERT_EQ(0, session->CountPendingErrors());

    const auto ts_before_commit = client_->GetLatestObservedTimestamp();
    ASSERT_OK(txn->Commit());
    const auto ts_after_sync_commit = client_->GetLatestObservedTimestamp();
    ASSERT_GT(ts_after_sync_commit, ts_before_commit);

    size_t count;
    ASSERT_OK(CountRows(table_.get(), &count));
    ASSERT_EQ(2 * kRowsNum, count);
  }

  // An empty transaction doesn't have a timestamp, so there is nothing to
  // propagate back to client when an empty transaction is committed.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));

    const auto ts_before_commit = client_->GetLatestObservedTimestamp();
    ASSERT_OK(txn->Commit());
    const auto ts_after_sync_commit = client_->GetLatestObservedTimestamp();
    ASSERT_EQ(ts_before_commit, ts_after_sync_commit);

    size_t count;
    ASSERT_OK(CountRows(table_.get(), &count));
    ASSERT_EQ(2 * kRowsNum, count);
  }
}

// Test that our deadlock prevention mechanisms work by writing across
// different tablets concurrently from multiple transactions.
TEST_F(TxnWriteOpsITest, DeadlockPrevention) {
  constexpr const int kNumTxns = 8;
  const vector<string> master_flags = {
    "--txn_manager_enabled=true",

    // Scenarios based on this test fixture assume the txn status table
    // is created at start, not on first transaction-related operation.
    "--txn_manager_lazily_initialized=false",
  };
  NO_FATALS(StartCluster({ "--enable_txn_system_client_init=true" },
                         master_flags, kNumTabletServers));
  NO_FATALS(Prepare());
  vector<thread> threads;
  threads.reserve(kNumTxns);
  vector<int> random_keys(kNumTxns * 2);
  std::iota(random_keys.begin(), random_keys.end(), 1);
  std::mt19937 gen(SeedRandom());
  std::shuffle(random_keys.begin(), random_keys.end(), gen);
  for (int i = 0; i < kNumTxns; i++) {
    threads.emplace_back([&, i] {
      bool succeeded = false;
      while (!succeeded) {
        shared_ptr<KuduTransaction> txn;
        ASSERT_OK(client_->NewTransaction(&txn));
        shared_ptr<KuduSession> session;
        ASSERT_OK(txn->CreateSession(&session));
        ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

        string txn_str;
        ASSERT_OK(txn->Serialize(&txn_str));
        TxnTokenPB token;
        ASSERT_TRUE(token.ParseFromString(txn_str));
        bool needs_retry = false;
        for (const auto key_idx : { 2 * i, 2 * i + 1 }) {
          const auto& row_key = random_keys[key_idx];
          unique_ptr<KuduInsert> insert(BuildInsert(table_.get(), row_key));
          Status s = session->Apply(insert.release());
          LOG(INFO) << Substitute("Txn $0 wrote row $1: $2",
                                  token.txn_id(), row_key, s.ToString());
          // If the write op failed because of a locking error, retry the
          // transaction after waiting a bit.
          if (!s.ok()) {
            vector<KuduError*> errors;
            ElementDeleter d(&errors);
            bool overflow;
            session->GetPendingErrors(&errors, &overflow);
            ASSERT_EQ(1, errors.size());
            const auto& error = errors[0]->status();
            LOG(INFO) << Substitute("Txn $0 wrote row $1: $2",
                                    token.txn_id(), row_key, error.ToString());
            // While the below delay between retries should help prevent
            // deadlocks, it's possible that "waiting" write ops (i.e. "wait"
            // in wait-die, that get retried) will still time out, after
            // contending a bit with other ops.
            ASSERT_TRUE(error.IsAborted() || error.IsTimedOut()) << error.ToString();
            needs_retry = true;

            // Wait a bit before retrying the entire transaction to allow for
            // the current lock holder to complete.
            SleepFor(MonoDelta::FromSeconds(5));
            break;
          }
        }
        if (!needs_retry) {
          succeeded = true;
          ASSERT_OK(txn->Commit());
        }
      }
    });
  }
  for (auto& t : threads) { t.join(); }
  size_t count;
  ASSERT_OK(CountRows(table_.get(), &count));
  ASSERT_EQ(kNumTxns * 2, count);
}

// Send transactions that span more than a single range of the transaction
// status table, ensuring we can write to newly-added ranges.
TEST_F(TxnWriteOpsITest, TestWriteToNewRangeOfTxnIds) {
  constexpr const auto kNumTxns = 10;
  const vector<string> kMasterFlags = {
    // Enable TxnManager in Kudu masters.
    "--txn_manager_enabled=true",
    // Set a small range so we can write to a new range of transactions IDs.
    Substitute("--txn_manager_status_table_range_partition_span=$0", kNumTxns / 3),
  };
  NO_FATALS(StartCluster({ "--enable_txn_system_client_init=true" },
                         kMasterFlags, kNumTabletServers));
  NO_FATALS(Prepare());
  for (int i = 0; i < kNumTxns; i++) {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    NO_FATALS(InsertRows(table_.get(), session.get(), 1, i));
    ASSERT_OK(txn->Commit());
    ASSERT_EQ(0, session->CountPendingErrors());
  }
  size_t count;
  ASSERT_OK(CountRows(table_.get(), &count));
  ASSERT_EQ(kNumTxns, count);
}

// Send multiple one-row write operations to a tablet server in the context of a
// multi-row transaction, and commit the transaction. This scenario verifies
// that tablet servers are able to accept high number of write requests
// from a client while automatically registering corresponding tablets as
// transaction participants.
TEST_F(TxnWriteOpsITest, TxnMultipleSingleRowWritesCommit) {
  static constexpr int kRowsNum = 1000;
  const vector<string> master_flags = {
    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    "--txn_manager_enabled=true",

    // Scenarios based on this test fixture assume the txn status table
    // is created at start, not on first transaction-related operation.
    "--txn_manager_lazily_initialized=false",
  };
  NO_FATALS(StartCluster({ "--enable_txn_system_client_init=true" },
                         master_flags, kNumTabletServers));

  NO_FATALS(Prepare());
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  shared_ptr<KuduSession> session;
  ASSERT_OK(txn->CreateSession(&session));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  NO_FATALS(InsertRows(table_.get(), session.get(), kRowsNum));
  ASSERT_OK(txn->Commit());
  size_t count;
  ASSERT_OK(CountRows(table_.get(), &count));
  ASSERT_EQ(kRowsNum, count);
  ASSERT_EQ(0, session->CountPendingErrors());
}

// This scenario induces high rate of leader elections while starting many
// multi-row transactions, writing few rows per transaction. The essence of this
// scenario is to make sure that tablet servers are able to automatically
// register corresponding tablets as transaction participants even if leadership
// transfer happens when a tablet tries to push BEGIN_TXN operation as a part
// of preparing to apply incoming write request from a client.
TEST_F(TxnWriteOpsITest, FrequentElections) {
  static constexpr auto kNumThreads = 8;
  SKIP_IF_SLOW_NOT_ALLOWED();

  const vector<string> ts_flags = {
    // Disabling pre-elections to make manual election request to take effect.
    "--raft_enable_pre_election=false",

    // Custom settings for heartbeat interval helps to complete Raft elections
    // rounds faster than with the default settings.
    Substitute("--heartbeat_interval_ms=$0", hb_interval_ms_),

    // Disable the partition lock as there are concurrent transactions.
    // TODO(awong): update this when implementing finer grained locking.
    "--enable_txn_partition_lock=false",
    "--enable_txn_system_client_init=true",
  };
  const vector<string> master_flags = {
    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    "--txn_manager_enabled=true",

    // Scenarios based on this test fixture assume the txn status table
    // is created at start, not on first transaction-related operation.
    "--txn_manager_lazily_initialized=false",
  };
  NO_FATALS(StartCluster(ts_flags, master_flags, kNumTabletServers));

  vector<string> tablets_uuids;
  NO_FATALS(Prepare(&tablets_uuids));

  // Using deque instead of vector to avoid too many reallocations.
  deque<shared_ptr<KuduTransaction>> transactions;
  simple_spinlock transactions_lock;

  atomic<bool> done = false;
  atomic<size_t> row_count = 0;
  vector<thread> writers;
  writers.reserve(kNumThreads);
  for (auto thread_idx = 0; thread_idx < kNumThreads; ++thread_idx) {
    writers.emplace_back([&, thread_idx] {
      for (int64_t iter = 0; !done; ++iter) {
        if (done) {
          break;
        }
        shared_ptr<KuduTransaction> txn;
        CHECK_OK(client_->NewTransaction(&txn));
        shared_ptr<KuduSession> session;
        CHECK_OK(txn->CreateSession(&session));
        CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
        int64_t start_key = kNumRowsPerTxn * (kNumThreads * iter + thread_idx);
        for (auto i = 0; i < kNumRowsPerTxn; ++i) {
          unique_ptr<KuduInsert> op(BuildInsert(table_.get(), start_key + i));
          CHECK_OK(session->Apply(op.release()));
        }
        if (iter % 8 == 0) {
          CHECK_OK(txn->StartCommit());
          row_count += kNumRowsPerTxn;
        } else {
          CHECK_OK(txn->Rollback());
        }
        {
          std::lock_guard<simple_spinlock> guard(transactions_lock);
          transactions.emplace_back(std::move(txn));
        }
      }
    });
  }
  auto cleanup = MakeScopedCleanup([&]() {
    done = true;
    std::for_each(writers.begin(), writers.end(), [](thread& t) { t.join(); });
  });

  // The main thread induces election by issuing step-down requests.
  const auto run_until =
      MonoTime::Now() + MonoDelta::FromSeconds(run_time_seconds_);
  double max_sleep_ms = 1;
  while (!done) {
    for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
      LOG(INFO) << "attempting to promote replicas at tserver " << i;
      consensus::ConsensusServiceProxy proxy(
          cluster_->messenger(),
          cluster_->tablet_server(i)->bound_rpc_addr(),
          "tserver");
      for (const auto& uuid : tablets_uuids) {
        consensus::RunLeaderElectionRequestPB req;
        consensus::RunLeaderElectionResponsePB resp;
        RpcController rpc;
        req.set_tablet_id(uuid);
        req.set_dest_uuid(cluster_->tablet_server(i)->uuid());
        rpc.set_timeout(MonoDelta::FromSeconds(5));
        // A best effort call: the replica might already be a leader or electing
        // a new leader might fail, etc.
        proxy.RunLeaderElection(req, &resp, &rpc);
      }
      int sleep_time = rand() % static_cast<int>(max_sleep_ms);
      if (MonoTime::Now() > run_until) {
        done = true;
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(sleep_time));
      max_sleep_ms = std::min(max_sleep_ms * 1.1, 1000.0);
    }
  }
  std::for_each(writers.begin(), writers.end(), [](thread& t) { t.join(); });
  cleanup.cancel();

  NO_FATALS(cluster_->AssertNoCrashes());
  for (auto& txn : transactions) {
    ASSERT_EVENTUALLY([&txn] {
      bool is_complete = false;
      Status completion_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
      ASSERT_TRUE(is_complete);
    });
  }

  // Check for the number of inserted rows: all write operations successfully
  // passed through many TxnOpDispatcher instances should be persisted.
  size_t count;
  ASSERT_OK(CountRows(table_.get(), &count));
  ASSERT_EQ(row_count, count);
}

// This scenario runs a benchmark to measure rate of transactional write
// operations. This is a scenario to evaluate --tablet_max_pending_txn_write_ops
// flag setting for tablet servers.
TEST_F(TxnWriteOpsITest, WriteOpPerf) {
  const vector<string> ts_flags = {
    Substitute("--tablet_max_pending_txn_write_ops=$0",
               FLAGS_max_pending_txn_write_ops),

    // Disable the partition lock as there are concurrent transactions.
    // TODO(awong): update this when implementing finer grained locking.
    "--enable_txn_partition_lock=false",
    "--enable_txn_system_client_init=true",
  };
  const vector<string> master_flags = {
    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    "--txn_manager_enabled=true",

    // Scenarios based on this test fixture assume the txn status table
    // is created at start, not on first transaction-related operation.
    "--txn_manager_lazily_initialized=false",
  };
  NO_FATALS(StartCluster(ts_flags, master_flags, kNumTabletServers));
  NO_FATALS(Prepare());

  const auto num_clients = FLAGS_clients;
  vector<shared_ptr<KuduClient>> clients;
  clients.reserve(num_clients);
  for (auto i = 0; i < num_clients; ++i) {
    KuduClientBuilder b;
    b.default_admin_operation_timeout(kTimeout);
    b.default_rpc_timeout(kTimeout);
    shared_ptr<KuduClient> c;
    ASSERT_OK(cluster_->CreateClient(&b, &c));
    clients.emplace_back(std::move(c));
  }

  const bool txn_enabled = FLAGS_txn_enabled;
  const auto num_sessions = num_clients * FLAGS_sessions_per_client;
  vector<shared_ptr<KuduTransaction>> txns;
  txns.reserve(num_sessions);
  vector<shared_ptr<KuduSession>> sessions;
  sessions.reserve(num_sessions);
  for (auto i = 0; i < num_sessions; ++i) {
    const auto client_idx = i % num_clients;
    auto& c = clients[client_idx];
    shared_ptr<KuduSession> s;
    if (!txn_enabled) {
      s = c->NewSession();
    } else {
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(c->NewTransaction(&txn));
      ASSERT_OK(txn->CreateSession(&s));
      txns.emplace_back(std::move(txn));
    }
    ASSERT_NE(nullptr, s.get());
    ASSERT_OK(s->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    sessions.emplace_back(std::move(s));
  }

  // Run multiple writer threads (one thread per session), where every thread
  // sends as many write operations as it can. For now, using INSERT operations:
  // INSERT and INSERT_IGNORE are the only write operations supported by
  // multi-row transaction sessions.
  atomic<bool> done = false;
  Barrier barrier(num_sessions + 1);
  vector<thread> writers;
  writers.reserve(num_sessions);
  vector<Status> session_statuses(num_sessions);
  vector<size_t> row_counters(num_sessions, 0);
  const bool prime_connections = FLAGS_prime_connections_to_tservers;
  for (auto session_idx = 0; session_idx < num_sessions; ++session_idx) {
    writers.emplace_back([&, session_idx] {
      auto& session = sessions[session_idx];
      const auto client_idx = session_idx % num_clients;
      auto& c = clients[client_idx];
      shared_ptr<KuduTable> table;
      auto s = c->OpenTable(kTableName, &table);
      if (PREDICT_FALSE(!s.ok())) {
        session_statuses[session_idx] = s;
        return;
      }
      if (prime_connections) {
        // If requested, send several INSERT_INGORE/DELETE_IGNORE operations
        // to open connections to all tablet servers in the cluster.
        // Number of rows is set to have at least one row per every tablet:
        // it's a hash-partitioned table with kNumPartitions tablets.
        constexpr const auto kNumPreliminaryRows = kNumPartitions * 10;
        shared_ptr<KuduSession> priming_session = c->NewSession();
        CHECK_OK(priming_session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
        for (auto i = 0; i < kNumPreliminaryRows; ++i) {
          unique_ptr<KuduInsertIgnore> op = BuildInsertIgnore(table.get(), i);
          auto s = priming_session->Apply(op.release());
          if (PREDICT_FALSE(!s.ok())) {
            session_statuses[session_idx] = s;
            return;
          }
        }
        for (auto i = 0; i < kNumPreliminaryRows; ++i) {
          unique_ptr<KuduDeleteIgnore> op = BuildDeleteIgnore(table.get(), i);
          auto s = priming_session->Apply(op.release());
          if (PREDICT_FALSE(!s.ok())) {
            session_statuses[session_idx] = s;
            return;
          }
        }
      }
      size_t op_idx = 0;
      barrier.Wait();
      while (!done) {
        int64_t key = num_sessions * op_idx + session_idx;
        unique_ptr<KuduInsert> op(BuildInsert(table.get(), key));
        auto s = session->Apply(op.release());
        if (PREDICT_FALSE(!s.ok())) {
          session_statuses[session_idx] = s;
          return;
        }
        // Every Write RPC results in one row because of AUTO_FLUSH_SYNC mode.
        ++row_counters[session_idx];
        ++op_idx;
      }
    });
  }
  const auto run_time = MonoDelta::FromMilliseconds(FLAGS_benchmark_run_time_ms);
  barrier.Wait(); // start writers
  SleepFor(run_time);
  done = true;    // stop writers
  std::for_each(writers.begin(), writers.end(), [](thread& t) { t.join(); });

  NO_FATALS(cluster_->AssertNoCrashes());
  for (auto i = 0; i < session_statuses.size(); ++i) {
    SCOPED_TRACE(Substitute("session index idx $0", i));
    const auto& s = session_statuses[i];
    ASSERT_OK(s);
  }
  for (auto& txn : txns) {
    ASSERT_OK(txn->Commit());
  }
  // Sanity check: make sure all the transactions are reported as complete.
  for (auto& txn : txns) {
    bool is_complete = false;
    Status completion_status;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(is_complete);
  }

  const size_t rows_total = std::accumulate(
      row_counters.begin(), row_counters.end(), 0UL);
  LOG(INFO) << Substitute("$0write RPCs completed: $1",
                          txn_enabled ? "txn " : "", rows_total);
  LOG(INFO) << Substitute(
      "$0write RPC rate: $1 req/sec",
      txn_enabled ? "txn " : "",
      static_cast<double>(rows_total) / run_time.ToSeconds());

  // Another sanity check: make sure all the rows have been persisted.
  size_t count;
  ASSERT_OK(CountRows(table_.get(), &count));
  ASSERT_EQ(rows_total, count);
}

// Send a write operation to a tablet server in the context of non-existent
// transaction. The server should respond back with appropriate error status.
TEST_F(TxnWriteOpsITest, WriteOpForNonExistentTxn) {
  const vector<string> master_flags = {
    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    "--txn_manager_enabled=true",
  };
  NO_FATALS(StartCluster({ "--enable_txn_system_client_init=true" },
                         master_flags, kNumTabletServers));
  NO_FATALS(Prepare());

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  string txn_token;
  ASSERT_OK(txn->Serialize(&txn_token));
  TxnTokenPB token;
  ASSERT_TRUE(token.ParseFromString(txn_token));
  ASSERT_TRUE(token.has_txn_id());

  const auto fake_txn_id = token.txn_id() + 100;
  token.set_txn_id(fake_txn_id);
  string fake_token;
  ASSERT_TRUE(token.SerializeToString(&fake_token));

  shared_ptr<KuduTransaction> fake_txn;
  ASSERT_OK(KuduTransaction::Deserialize(client_, fake_token, &fake_txn));
  shared_ptr<KuduSession> session;
  ASSERT_OK(fake_txn->CreateSession(&session));

  {
    ASSERT_FALSE(session->HasPendingOperations());
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

    unique_ptr<KuduInsert> insert(table_->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("key", 12345));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 67890));

    const auto s = session->Apply(insert.release());
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    const auto err_status = GetSingleRowError(session.get());
    ASSERT_TRUE(err_status.IsInvalidArgument()) << err_status.ToString();
    ASSERT_STR_CONTAINS(err_status.ToString(),
                        "Failed to write batch of 1 ops to tablet");
    ASSERT_STR_CONTAINS(err_status.ToString(),
                        Substitute("transaction ID $0 not found", fake_txn_id));
  }
}

// Try to write an extra row in the context of a transaction which has already
// been committed.
TEST_F(TxnWriteOpsITest, TxnWriteAfterCommit) {
  const vector<string> master_flags = {
    // Enable TxnManager in Kudu masters.
    // TODO(aserbin): remove this customization once the flag is 'on' by default
    "--txn_manager_enabled=true",
  };
  NO_FATALS(StartCluster({ "--enable_txn_system_client_init=true" },
                         master_flags, kNumTabletServers));
  NO_FATALS(Prepare());
  int idx = 0;
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

    {
      unique_ptr<KuduInsert> insert(BuildInsert(table_.get(), idx++));
      ASSERT_OK(session->Apply(insert.release()));
    }
    ASSERT_OK(txn->Commit());

    {
      unique_ptr<KuduInsert> insert(BuildInsert(table_.get(), idx++));
      auto s = session->Apply(insert.release());
      ASSERT_TRUE(s.IsIOError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
      const auto err_status = GetSingleRowError(session.get());
      ASSERT_TRUE(err_status.IsIllegalState()) << err_status.ToString();
      ASSERT_STR_CONTAINS(err_status.ToString(),
                          "Failed to write batch of 1 ops to tablet");
      ASSERT_STR_MATCHES(err_status.ToString(), "transaction .* not open");
    }
  }
  // A scenario similar to one above, but restart tablet servers before an
  // attempt to write an extra row for the transaction which has already been
  // committed.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

    {
      unique_ptr<KuduInsert> insert(BuildInsert(table_.get(), idx++));
      ASSERT_OK(session->Apply(insert.release()));
    }
    ASSERT_OK(txn->Commit());

    // Restart all tablet servers. This is to clear run-time information
    // in tablet servers which is used to serve write operations in the context
    // of a multi-row transaction.
    for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
      auto* ts = cluster_->tablet_server(i);
      ts->Shutdown();
      ASSERT_OK(ts->Restart());
    }

    {
      unique_ptr<KuduInsert> insert(BuildInsert(table_.get(), idx++));
      auto s = session->Apply(insert.release());
      ASSERT_TRUE(s.IsIOError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
      const auto err_status = GetSingleRowError(session.get());
      ASSERT_TRUE(err_status.IsIllegalState()) << err_status.ToString();
      ASSERT_STR_CONTAINS(err_status.ToString(),
                          "Failed to write batch of 1 ops to tablet");
      ASSERT_STR_MATCHES(err_status.ToString(), "transaction .* not open");
    }
  }
}

// Test to peek into TxnOpDispatcher's internals.
class TxnOpDispatcherITest : public KuduTest {
 public:
  TxnOpDispatcherITest() {
    CHECK_OK(BuildSchema(&schema_));
  }

  void SetupCluster(int num_tservers, int num_replicas = 0) {
    if (num_replicas == 0) {
      num_replicas = num_tservers;
    }
    FLAGS_txn_manager_enabled = true;
    FLAGS_txn_manager_lazily_initialized = false;
    FLAGS_txn_manager_status_table_num_replicas = num_replicas;
    FLAGS_enable_txn_system_client_init = true;

    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_tservers;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->StartSync());
  }

  void Prepare(int num_tservers, bool create_table = true, int num_replicas = 0) {
    if (num_replicas == 0) {
      num_replicas = num_tservers;
    }
    NO_FATALS(SetupCluster(num_tservers, num_replicas));
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(kTimeout);
    ASSERT_OK(cluster_->CreateClient(&builder, &client_));
    if (create_table) {
      ASSERT_OK(CreateTable(num_replicas));
    }
    for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
      auto* ts = cluster_->mini_tablet_server(i);
      ASSERT_OK(ts->WaitStarted());
    }
  }

  Status CreateTable(int num_replicas) {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(kTableName)
                  .schema(&schema_)
                  .add_hash_partitions({ "key" }, kNumPartitions)
                  .num_replicas(num_replicas)
                  .Create());
    return client_->OpenTable(kTableName, &table_);
  }

  // Insert rows in a context of the specified transaction; if the 'txn' is
  // nullptr, use non-transactional session for inserts. The result session
  // is output into the 'session_out' parameter if it's set to non-null.
  Status InsertRows(KuduTransaction* txn,
                    int num_rows,
                    int64_t* key,
                    shared_ptr<KuduSession>* session_out = nullptr) {
    shared_ptr<KuduSession> session;
    if (txn) {
      RETURN_NOT_OK(txn->CreateSession(&session));
    } else {
      session = client_->NewSession();
    }
    if (session_out) {
      *session_out = session;
    }
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    for (auto i = 0; i < num_rows; ++i) {
      unique_ptr<KuduInsert> ins = BuildInsert(table_.get(), (*key)++);
      RETURN_NOT_OK(session->Apply(ins.release()));
    }
    return Status::OK();
  }

  // Get all replicas of the test table.
  vector<scoped_refptr<TabletReplica>> GetAllReplicas(const string& table_name = "") const {
    const string& target_table = table_name.empty() ? kTableName : table_name;
    vector<scoped_refptr<TabletReplica>> result;
    for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
      auto* server = cluster_->mini_tablet_server(i)->server();
      vector<scoped_refptr<TabletReplica>> replicas;
      server->tablet_manager()->GetTabletReplicas(&replicas);
      for (auto& r : replicas) {
        if (r->tablet()->metadata()->table_name() == target_table) {
          result.emplace_back(std::move(r));
        }
      }
    }
    return result;
  }

  size_t GetTxnOpDispatchersTotalCount(
      vector<scoped_refptr<TabletReplica>> replicas = {},
      const string& table_name = "") {
    if (replicas.empty()) {
      // No replicas were specified, get the list of all test table's replicas.
      replicas = GetAllReplicas(table_name);
    }
    size_t elem_count = 0;
    for (auto& r : replicas) {
      std::lock_guard<simple_spinlock> guard(r->txn_op_dispatchers_lock_);
      elem_count += r->txn_op_dispatchers_.size();
    }
    return elem_count;
  }

  std::shared_ptr<typename TabletReplica::TxnOpDispatcher>
      GetSingleTxnOpDispatcher() {
    auto replicas = GetAllReplicas();
    std::shared_ptr<typename TabletReplica::TxnOpDispatcher> d;
    size_t count = 0;
    for (auto& r : replicas) {
      std::lock_guard<simple_spinlock> guard(r->txn_op_dispatchers_lock_);
      auto& dispatchers = r->txn_op_dispatchers_;
      if (!dispatchers.empty()) {
        d = dispatchers.begin()->second;
        ++count;
      }
    }
    CHECK_EQ(1, count);
    return CHECK_NOTNULL(d);
  }

  typedef vector<std::shared_ptr<typename TabletReplica::TxnOpDispatcher>>
      OpDispatchers;
  typedef map<int64_t, OpDispatchers> OpDispatchersPerTxnId;
  OpDispatchersPerTxnId GetTxnOpDispatchers(const string& table_name = "") {
    auto replicas = GetAllReplicas(table_name);
    OpDispatchersPerTxnId result;
    for (auto& r : replicas) {
      std::lock_guard<simple_spinlock> guard(r->txn_op_dispatchers_lock_);
      auto& dispatchers = r->txn_op_dispatchers_;
      for (auto& [txn_id, d] : dispatchers) {
        auto& dispatchers = LookupOrEmplace(&result, txn_id, OpDispatchers());
        dispatchers.emplace_back(d);
      }
    }
    return result;
  }

 protected:
  static constexpr const char* const kTableName = "txn_op_dispatcher_test";
  static constexpr const int kNumPartitions = 2;
  static const MonoDelta kTimeout;

  KuduSchema schema_;
  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
};
const MonoDelta TxnOpDispatcherITest::kTimeout = MonoDelta::FromSeconds(10);

// A scenario to verify basic pre- and post-conditions of the TxnOpDispatcher's
// lifecycle.
TEST_F(TxnOpDispatcherITest, LifecycleBasic) {
  NO_FATALS(Prepare(1));

  // Next value for the primary key column in the test table.
  int64_t key = 0;

  vector<scoped_refptr<TabletReplica>> replicas = GetAllReplicas();
  ASSERT_EQ(kNumPartitions, replicas.size());

  // At first, there should be no TxnOpDispatchers across all tablet replicas.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  // Start and commit an empty transaction.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
    ASSERT_OK(txn->Commit());
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }

  // Start and rollback an empty transaction.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
    ASSERT_OK(txn->Rollback());
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }

  // Start a single transaction and commit it after inserting a few rows.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));

    // There should be no TxnOpDispatchers yet because not a single write
    // operations has been sent to tablet servers yet.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

    // Insert a single row.
    ASSERT_OK(InsertRows(txn.get(), 1, &key));

    // Only one tablet replica should get the txn write request and register
    // TxnOpDispatcher for the transaction.
    ASSERT_EQ(1, GetTxnOpDispatchersTotalCount());

    // Write some more rows ensuring all hash buckets of the table's partition
    // will get at least one element.
    ASSERT_OK(InsertRows(txn.get(), 5, &key));

    // Now all tablet replicas should get one TxnOpDispatcher.
    for (auto& r : replicas) {
      ASSERT_EQ(1, r->txn_op_dispatchers_.size());
    }

    const auto ref_txn_id = GetTxnId(txn);

    // Since all write operations inserts were successfully processed, all
    // TxnOpDispatchers should not be buffering any write operations.
    for (auto& r : replicas) {
      for (const auto& [txn_id, dispatcher] : r->txn_op_dispatchers_) {
        ASSERT_EQ(ref_txn_id, txn_id);
        {
          std::lock_guard<simple_spinlock> guard(dispatcher->lock_);
          ASSERT_TRUE(dispatcher->preliminary_tasks_completed_);
          ASSERT_TRUE(dispatcher->ops_queue_.empty());
          ASSERT_FALSE(dispatcher->unregistered_);
          ASSERT_OK(dispatcher->inflight_status_);
        }
      }
    }

    // Now, commit the transaction.
    ASSERT_OK(txn->Commit());

    // All dispatchers should be unregistered once the transaction is committed.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }

  // Start a single transaction and roll it back after inserting a few rows.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(InsertRows(txn.get(), 8, &key));
    ASSERT_EQ(kNumPartitions, GetTxnOpDispatchersTotalCount());
    ASSERT_OK(txn->Rollback());

    // Since KuduTransaction::Rollback() just schedules the transaction abort,
    // wait for the rollback to finalize.
    ASSERT_EVENTUALLY([&] {
      Status status;
      bool complete = false;
      ASSERT_OK(txn->IsCommitComplete(&complete, &status));
      ASSERT_TRUE(complete);
      ASSERT_TRUE(status.IsAborted()) << status.ToString();
    });
    // No dispatchers should be registered once the transaction is rolled back.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }
}

// Test that the automatic abort to avoid deadlock gets retried if the op times
// out.
TEST_F(TxnOpDispatcherITest, TestRetryWaitDieAbortsWhenTServerUnavailable) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Disable the staleness tracker so we know any aborts were done by the
  // wait-die deadlock prevention.
  FLAGS_txn_staleness_tracker_interval_ms = 0;
  // Set a low system client timeout to make sure our abort task retries.
  FLAGS_txn_system_client_op_timeout_ms = 1000;

  NO_FATALS(Prepare(/*num_tservers*/2, /*create_table*/false, /*num_replicas*/1));
  // First, figure out which tablet server hosts the TxnStatusManager.
  tserver::MiniTabletServer* tsm_server = nullptr;
  ASSERT_EVENTUALLY([&] {
    for (int i = 0; i < cluster_->num_tablet_servers() && tsm_server == nullptr; i++) {
      auto* mts = cluster_->mini_tablet_server(i);
      auto* tablet_manager = mts->server()->tablet_manager();
      vector<scoped_refptr<TabletReplica>> replicas;
      tablet_manager->GetTabletReplicas(&replicas);
      if (!replicas.empty()) {
        tsm_server = mts;
      }
    }
    ASSERT_FALSE(tsm_server == nullptr);
  });

  // Create a single-tablet table so shutting down the TxnStatusManager doesn't
  // affect writes.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema_)
            .set_range_partition_columns({ "key" })
            .num_replicas(1)
            .Create());
  ASSERT_OK(client_->OpenTable(kTableName, &table_));
  shared_ptr<KuduTransaction> first_txn;
  shared_ptr<KuduTransaction> second_txn;
  ASSERT_OK(client_->NewTransaction(&first_txn));
  ASSERT_OK(client_->NewTransaction(&second_txn));

  int64_t key = 0;
  ASSERT_OK(InsertRows(first_txn.get(), 1, &key));

  // The second transaction should always fail because it's attempting to lock
  // a tablet that's already locked.
  Status s = InsertRows(second_txn.get(), 1, &key);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  // Immediately shutdown, reducing the likelihood that the automatic abort
  // task will complete. Then sleep for long enough that the system client
  // would timeout and try again.
  tsm_server->Shutdown();
  SleepFor(MonoDelta::FromMilliseconds(3 * FLAGS_txn_system_client_op_timeout_ms));
  ASSERT_OK(tsm_server->Restart());
  ASSERT_EVENTUALLY([&] {
    bool is_complete = false;
    Status completion_status;
    ASSERT_OK(second_txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });
}

// Test that when attempting to lock a transaction that is locked by an earlier
// transaction, we abort the newer transaction.
TEST_F(TxnOpDispatcherITest, BeginTxnLockAbort) {
  NO_FATALS(Prepare(1));

  // Next value for the primary key column in the test table.
  int64_t key = 0;
  vector<scoped_refptr<TabletReplica>> replicas = GetAllReplicas();
  ASSERT_EQ(kNumPartitions, replicas.size());
  shared_ptr<KuduTransaction> first_txn;
  shared_ptr<KuduTransaction> second_txn;

  // Start a single transaction and perform some writes with it.
  {
    ASSERT_OK(client_->NewTransaction(&first_txn));

    // There should be no TxnOpDispatchers yet because not a single write
    // operations has been sent to tablet servers yet.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

    // Insert a single row.
    ASSERT_OK(InsertRows(first_txn.get(), 1, &key));

    // Only one tablet replica should get the txn write request and register
    // TxnOpDispatcher for the transaction.
    ASSERT_EQ(1, GetTxnOpDispatchersTotalCount());

    // Write some more rows ensuring all hash buckets of the table's partition
    // will get at least one element.
    ASSERT_OK(InsertRows(first_txn.get(), 5, &key));
    ASSERT_EQ(kNumPartitions, GetTxnOpDispatchersTotalCount());

    // Non transactional operations should fail as the partition lock
    // is held by the transaction at the moment.
    shared_ptr<KuduSession> session;
    Status s = InsertRows(nullptr /* txn */, 1, &key, &session);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    auto row_status = GetSingleRowError(session.get());
    ASSERT_TRUE(row_status.IsAborted()) << row_status.ToString();
    ASSERT_STR_CONTAINS(row_status.ToString(),
                        "Write op should be aborted");
  }

  // Start a new transaction.
  {
    ASSERT_OK(client_->NewTransaction(&second_txn));
    ASSERT_EQ(kNumPartitions, GetTxnOpDispatchersTotalCount());

    // Operations of the transaction should fail as the partition
    // lock is held by the transaction at the moment.
    shared_ptr<KuduSession> session;
    Status s = InsertRows(second_txn.get(), 1, &key, &session);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    auto row_status = GetSingleRowError(session.get());
    ASSERT_TRUE(row_status.IsAborted()) << row_status.ToString();
    ASSERT_STR_CONTAINS(row_status.ToString(), "should be aborted");

    // The dispatcher for the new transactional write should eventually
    // disappear because the transaction is automatically aborted.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(kNumPartitions, GetTxnOpDispatchersTotalCount());
    });
  }
  {
    // Now, commit the first transaction.
    ASSERT_OK(first_txn->Commit());

    // All dispatchers should be unregistered once the transaction is committed.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

    // The second transaction should have been automatically aborted in its
    // attempt to write to avoid deadlock.
    Status s = InsertRows(second_txn.get(), 1, &key);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
    ASSERT_EVENTUALLY([&] {
      bool is_complete;
      Status commit_status;
      ASSERT_OK(second_txn->IsCommitComplete(&is_complete, &commit_status));
      ASSERT_TRUE(is_complete);
      ASSERT_TRUE(commit_status.IsAborted()) << commit_status.ToString();
    });
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }
}

TEST_F(TxnOpDispatcherITest, BeginTxnLockRetry) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  NO_FATALS(Prepare(1));

  // Next value for the primary key column in the test table.
  int64_t key = 0;

  vector<scoped_refptr<TabletReplica>> replicas = GetAllReplicas();
  ASSERT_EQ(kNumPartitions, replicas.size());
  shared_ptr<KuduTransaction> first_txn;
  shared_ptr<KuduTransaction> second_txn;

  // Start a single transaction.
  {
    ASSERT_OK(client_->NewTransaction(&second_txn));

    // There should be no TxnOpDispatchers yet because not a single write
    // operations has been sent to tablet servers yet.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }

  // Start another single transaction and perform some writes with it.
  {
    ASSERT_OK(client_->NewTransaction(&first_txn));
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

    // Write some more rows ensuring all hash buckets of the table's partition
    // will get at least one element.
    ASSERT_OK(InsertRows(first_txn.get(), 5, &key));
    ASSERT_EQ(kNumPartitions, GetTxnOpDispatchersTotalCount());
  }

  {
    // Operations of the second transaction should fail as the partition
    // lock is held by the first transaction at the moment.
    shared_ptr<KuduSession> session;
    Status s = InsertRows(second_txn.get(), 1, &key, &session);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    auto row_status = GetSingleRowError(session.get());
    ASSERT_TRUE(row_status.IsTimedOut()) << row_status.ToString();
    ASSERT_STR_CONTAINS(row_status.ToString(), "passed its deadline");

    // We should have an extra dispatcher for the new transactional write.
    ASSERT_EQ(1 + kNumPartitions, GetTxnOpDispatchersTotalCount());

    ASSERT_OK(first_txn->Commit());
    // We should still have an op dispatcher for the second transaction.
    ASSERT_EQ(1, GetTxnOpDispatchersTotalCount());
  }
}

// A scenario to verify TxnOpDispatcher lifecycle when there is an error
// while trying to register a tablet as a participant in a transaction.
TEST_F(TxnOpDispatcherITest, ErrorInParticipantRegistration) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(Prepare(1));

  // It's a clean slate: no TxnOpDispatchers should be around yet.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  // Next value for the primary key column in the test table.
  int64_t key = 0;

  // This sub-scenario tries to submit a write operation as a part
  // of a nonexistent transaction.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));

    string txn_token_str;
    ASSERT_OK(txn->Serialize(&txn_token_str));
    TxnTokenPB txn_token;
    ASSERT_TRUE(txn_token.ParseFromString(txn_token_str));
    ASSERT_TRUE(txn_token.has_txn_id());
    const int64_t txn_id = txn_token.txn_id();
    const int64_t fake_txn_id = txn_id + 10;
    txn_token.set_txn_id(fake_txn_id);

    string fake_txn_token_str;
    ASSERT_TRUE(txn_token.SerializeToString(&fake_txn_token_str));

    shared_ptr<KuduTransaction> fake_txn;
    ASSERT_OK(KuduTransaction::Deserialize(client_, fake_txn_token_str, &fake_txn));

    shared_ptr<KuduSession> session;
    auto s = InsertRows(fake_txn.get(), 1, &key, &session);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    auto row_status = GetSingleRowError(session.get());
    ASSERT_TRUE(row_status.IsInvalidArgument()) << row_status.ToString();
    ASSERT_STR_CONTAINS(row_status.ToString(),
                        "transaction ID 10 not found, current highest txn ID");

    // There should be no TxnOpDispatchers: they should be cleaned up when
    // getting an error upon registering a tablet as a participant of a
    // non-existent transaction.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
    });

    // Make sure nothing unexpected happens when committing the original empty
    // transaction.
    ASSERT_OK(txn->Commit());
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }

  // This sub-scenario tries to submit a write operation as a part
  // of already committed transaction.
  {
    // Here a custom client with shorter timeout is used to avoid making
    // too many pointless retries upon receiving Status::IllegalState()
    // from the tablet server.
    const MonoDelta kCustomTimeout = MonoDelta::FromSeconds(2);
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(kCustomTimeout);
    builder.default_rpc_timeout(kCustomTimeout);
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(&builder, &client));
    shared_ptr<KuduTransaction> txn_orig_handle;
    ASSERT_OK(client->NewTransaction(&txn_orig_handle));

    string txn_token;
    ASSERT_OK(txn_orig_handle->Serialize(&txn_token));

    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(KuduTransaction::Deserialize(client_, txn_token, &txn));
    ASSERT_OK(txn_orig_handle->Commit());

    int64_t key = 0;
    shared_ptr<KuduSession> session;
    auto s = InsertRows(txn.get(), 1, &key, &session);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    auto row_status = GetSingleRowError(session.get());
    ASSERT_TRUE(row_status.IsIllegalState()) << row_status.ToString();
    ASSERT_STR_CONTAINS(row_status.ToString(),
                        "Failed to write batch of 1 ops to tablet");

    // There should be no TxnOpDispatchers: they should be cleaned up upon getting
    // Status::IllegalState() error when registering a tablet as a participant
    // of a no-longer-open transaction.
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  }
}

TEST_F(TxnOpDispatcherITest, ErrorInProcessingWriteOp) {
  NO_FATALS(Prepare(1));

  // It's a clean slate: no TxnOpDispatchers should be around yet.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  // Next value for the primary key column in the test table.
  int64_t key = 0;

  // Try to submit an update (not an insert): it's not yet possible to have
  // an update as a part of a multi-row transaction in Kudu.
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  shared_ptr<KuduSession> session;
  ASSERT_OK(txn->CreateSession(&session));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  unique_ptr<KuduUpdate> op(table_->NewUpdate());
  KuduPartialRow* row = op->mutable_row();
  ASSERT_OK(row->SetInt64(0, key));
  ASSERT_OK(row->SetInt32(1, 1));
  auto s = session->Apply(op.release());
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  auto row_status = GetSingleRowError(session.get());
  ASSERT_TRUE(row_status.IsNotSupported()) << row_status.ToString();
  ASSERT_STR_CONTAINS(row_status.ToString(), "transactions may only insert");

  // The corresponding TxnOpDispatcher instance should be still registered. It
  // will be gone after committing or rolling back the transaction (see below).
  ASSERT_EQ(1, GetTxnOpDispatchersTotalCount());

  // Find and examine the TxnOpDispatcher instance.
  auto dispatcher = GetSingleTxnOpDispatcher();
  ASSERT_EVENTUALLY([&] {
    std::lock_guard<simple_spinlock> guard(dispatcher->lock_);
    // Due to scheduling anomalies, the operation above might be responded,
    // but TxnOpDispatcher::preliminary_tasks_completed_ field is not updated
    // yet. So, using ASSERT_EVENTUALLY here.
    ASSERT_TRUE(dispatcher->preliminary_tasks_completed_);
    ASSERT_FALSE(dispatcher->unregistered_);
    ASSERT_OK(dispatcher->inflight_status_);
    ASSERT_TRUE(dispatcher->ops_queue_.empty());
  });

  // It should be still possible to successfully insert rows in the context of
  // current transaction.
  static constexpr const size_t kNumRows = 8;
  ASSERT_OK(InsertRows(txn.get(), kNumRows, &key));

  // Every tablet should get at least one write operation, so there should be
  // total of two TxnOpDispachers.
  ASSERT_EQ(2, GetTxnOpDispatchersTotalCount());

  // Try to insert rows with duplicate keys.
  {
    int64_t duplicate_key = 0;
    shared_ptr<KuduSession> session;
    s = InsertRows(txn.get(), kNumRows, &duplicate_key, &session);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    session->GetPendingErrors(&errors, nullptr);
    for (const auto* e : errors) {
      const auto& s = e->status();
      EXPECT_TRUE(s.IsAlreadyPresent()) << s.ToString();
    }
  }

  // Same TxnOpDispatchers should be handling all the write operations.
  ASSERT_EQ(2, GetTxnOpDispatchersTotalCount());

  const auto dispatchers_per_txn_id = GetTxnOpDispatchers();
  ASSERT_EQ(1, dispatchers_per_txn_id.size());
  const OpDispatchers& dispatchers = dispatchers_per_txn_id.begin()->second;
  ASSERT_EQ(2, dispatchers.size());

  for (auto& d : dispatchers) {
    std::lock_guard<simple_spinlock> guard(d->lock_);
    ASSERT_TRUE(d->preliminary_tasks_completed_);
    ASSERT_FALSE(d->unregistered_);
    ASSERT_OK(d->inflight_status_);
    ASSERT_TRUE(d->ops_queue_.empty());
  }

  // Now commit the transaction.
  ASSERT_OK(txn->Commit());
  // Upon committing, the TxnOpDispatcher should be automatically unregistered.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  // Make sure all rows which were successfully written through TxnOpDispatcher
  // are persisted.
  size_t row_count;
  ASSERT_OK(CountRows(table_.get(), &row_count));
  ASSERT_EQ(kNumRows, row_count);

  // TODO(aserbin): stop the replica and try to submit a write operations
  // Check for the number of inserted rows.
}

// Make sure TxnOpDispatcher's logic works as expected when the maximum number
// of buffered/pending write operations is set to 0. In this case, all
// transactional write requests from a client are responded with
// Status::ServiceUnavailable() until all preliminary work of registering
// the tablet as a participant and issuing in TXN_BEGIN operation is complete.
TEST_F(TxnOpDispatcherITest, NoPendingWriteOps) {
  FLAGS_tablet_max_pending_txn_write_ops = 0;
  NO_FATALS(Prepare(1));

  // It's a clean slate: no TxnOpDispatchers should be around yet.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  const auto txn_id = GetTxnId(txn);
  string authn_creds;
  ASSERT_OK(client_->ExportAuthenticationCredentials(&authn_creds));
  client::AuthenticationCredentialsPB pb;
  ASSERT_TRUE(pb.ParseFromString(authn_creds));
  ASSERT_TRUE(pb.has_real_user());
  string client_user = pb.real_user();

  auto proxy = cluster_->tserver_proxy(0);
  ASSERT_NE(nullptr, proxy.get());

  const auto replicas = GetAllReplicas();
  ASSERT_EQ(kNumPartitions, replicas.size());
  for (auto i = 0; i < kNumPartitions; ++i) {
    const auto& tablet_id = replicas[i]->tablet_id();
    const auto schema = KuduSchema::ToSchema(schema_);
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt64("key", 0));
    ASSERT_OK(row.SetInt32("int_val", 1));

    // This isn't a well formed write request, but it shouldn't be submitted
    // into the tablet server's prepare queue anyway since
    // FLAGS_tablet_max_pending_txn_write_ops is set to 0 and a not-yet-ready
    // TxnOpDispatcher should immediately respond back with ServiceUnavailable.
    WriteRequestPB req;
    req.set_tablet_id(tablet_id);
    req.set_txn_id(txn_id);

    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    RpcController ctl;
    ctl.set_timeout(kTimeout);

    WriteResponsePB resp;
    auto s = proxy->Write(req, &resp, &ctl);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_FALSE(resp.has_error());
    const auto* err_status_pb = ctl.error_response();
    ASSERT_NE(nullptr, err_status_pb);
    ASSERT_TRUE(err_status_pb->has_code());
    ASSERT_EQ(ErrorStatusPB::ERROR_SERVER_TOO_BUSY, err_status_pb->code());
    ASSERT_TRUE(err_status_pb->has_message());
    ASSERT_STR_CONTAINS(err_status_pb->message(),
                        "pending operations queue is at capacity");
  }

  const auto dmap = GetTxnOpDispatchers();
  ASSERT_EQ(1, dmap.size());
  auto& [dmap_txn_id, dispatchers] = *(dmap.begin());
  ASSERT_EQ(txn_id, dmap_txn_id);
  for (auto& d : dispatchers) {
    ASSERT_EVENTUALLY([d] {
      // Eventually, every involved tablet should be registered as a transaction
      // participant and BEGIN_TXN should be issued.
      std::lock_guard<simple_spinlock> guard(d->lock_);
      ASSERT_TRUE(d->ops_queue_.empty());
      ASSERT_FALSE(d->unregistered_);
      ASSERT_OK(d->inflight_status_);
      ASSERT_TRUE(d->preliminary_tasks_completed_);
    });
  }

  // Now, insert several rows into the table.
  int64_t key = 0;
  constexpr const auto kNumRows = 8;
  ASSERT_OK(InsertRows(txn.get(), kNumRows, &key));

  // Now commit the transaction and make sure the rows are persisted.
  ASSERT_OK(txn->Commit());
  size_t num_rows = 0;
  ASSERT_OK(CountRows(table_.get(), &num_rows));
  ASSERT_EQ(kNumRows, num_rows);

  // No dispatchers should be there after the transaction is committed.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
}

// Make sure TxnOpDispatchers are not engaged when a tablet server processes
// non-transactional write requests.
TEST_F(TxnOpDispatcherITest, NonTransactionalWrites) {
  NO_FATALS(Prepare(1));

  // It's a clean slate: no TxnOpDispatchers should be around.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  int64_t key = 0;
  ASSERT_OK(InsertRows(nullptr, 2, &key));

  // No dispatchers should be around: those were non-transactional write ops.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
}

// Make sure TxnOpDispatcher responds back with Status::TimedOut() status for
// all pending operations if it takes too long to perform the preliminary tasks
// of registering a tablet as a participant and issuing BEGIN_TXN operation
// for target tablet replica.
TEST_F(TxnOpDispatcherITest, PreliminaryTasksTimeout) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr auto kNumThreads = 8;
  constexpr auto kNumRowsPerThread = 1;
  const auto kShortTimeout = MonoDelta::FromMilliseconds(1000);
  const auto kInjectedDelayMs = 2 * kShortTimeout.ToMilliseconds();

  // This should be enough to accommodate all write operations below, even with
  // some margin since there are two tablets in a hash-partitioned test table.
  FLAGS_tablet_max_pending_txn_write_ops = kNumThreads * kNumRowsPerThread;

  NO_FATALS(Prepare(1));

  for (auto iteration = 0; iteration < 2; ++iteration) {
    FLAGS_txn_participant_registration_inject_latency_ms =
        iteration == 0 ? kInjectedDelayMs : 0;
    FLAGS_txn_participant_begin_op_inject_latency_ms =
        iteration == 1 ? kInjectedDelayMs : 0;

    shared_ptr<KuduTransaction> txn_orig_client;
    ASSERT_OK(client_->NewTransaction(&txn_orig_client));

    // Create an instance of client with shorter timeouts to work with
    // transactional sessions.
    shared_ptr<KuduClient> c;
    {
      KuduClientBuilder builder;
      builder.default_admin_operation_timeout(kShortTimeout);
      builder.default_rpc_timeout(kShortTimeout);
      ASSERT_OK(cluster_->CreateClient(&builder, &c));
    }
    ASSERT_NE(nullptr, c.get());

    // To switch to the txn operations bound to the client with short timeout
    // for operations, serialize/deserialize the txn handle.
    string txn_token;
    ASSERT_OK(txn_orig_client->Serialize(&txn_token));
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(KuduTransaction::Deserialize(c, txn_token, &txn));

    // Now, make writer threads issue single-row write operations using the
    // handle bound to the client with shorter RPC timeout.
    Barrier barrier(kNumThreads + 1);
    vector<thread> writers;
    writers.reserve(kNumThreads);
    vector<shared_ptr<KuduSession>> sessions(kNumThreads);
    vector<Status> statuses(kNumThreads);
    for (auto thread_idx = 0; thread_idx < kNumThreads; ++thread_idx) {
      writers.emplace_back([&, thread_idx] {
        shared_ptr<KuduSession> session;
        CHECK_OK(txn->CreateSession(&session));
        CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
        int64_t key = thread_idx;
        unique_ptr<KuduInsert> ins = BuildInsert(table_.get(), key);
        barrier.Wait();
        auto s = session->Apply(ins.release());
        sessions[thread_idx] = std::move(session);
        statuses[thread_idx] = std::move(s);
      });
    }
    // Signal writer threads to send their operations.
    barrier.Wait();

    // Wait for all write operations to be responded.
    std::for_each(writers.begin(), writers.end(), [](thread& t) { t.join(); });

    // Check for the statuses reported by each of the writer threads.
    for (auto i = 0; i < kNumThreads; ++i) {
      SCOPED_TRACE(Substitute("writer thread idx $0", i));

      const auto& s = statuses[i];
      ASSERT_TRUE(s.IsIOError()) << s.ToString();

      auto& session = sessions[i];
      const auto row_status = GetSingleRowError(session.get());
      ASSERT_TRUE(row_status.IsTimedOut()) << row_status.ToString();
      ASSERT_STR_CONTAINS(row_status.ToString(),
                          "Failed to write batch of 1 ops to tablet");
    }

    // Eventually, all TxnOpDispatchers should be gone: after about
    // FLAGS_txn_participant_registration_inject_latency_ms milliseconds
    // corresponding callbacks should be called and TxnOpDispatchers should be
    // unregistered.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
    });

    // Just in case, try to commit the transaction to make sure the operations
    // didn't get through. There is a couple of expected outcomes, depending
    // whether the tablet has or hasn't been registered as a participant in the
    // transaction.
    const auto s = txn->Commit();
    if (iteration == 0) {
      // This is the case when not a single tablet was registered as participant
      // in the transaction. In this case, the should be able to commit
      // with no issues.
      ASSERT_OK(s);
    } else {
      // This is the case when tablets have been registered as participants.
      // In this case, the transaction should not be able to finalize.
      ASSERT_TRUE(s.IsAborted()) << s.ToString();
    }

    // No rows should be persisted.
    size_t row_count;
    ASSERT_OK(CountRows(table_.get(), &row_count));
    ASSERT_EQ(0, row_count);
  }
}

// This scenario makes sure that TxnOpDispatcher's logic behaves as expected
// when txn participant registration takes too long for first few write
// operations which time out. However, after the 'unfreeze', future operations
// sent in the context of the same transaction should be successful.
TEST_F(TxnOpDispatcherITest, DuplicateTxnParticipantRegistration) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const auto kShortTimeout = MonoDelta::FromMilliseconds(1000);
  FLAGS_txn_participant_begin_op_inject_latency_ms =
      2 * kShortTimeout.ToMilliseconds();

  NO_FATALS(Prepare(1));

  // Create a custom instance of client with shorter timeouts to work with
  // transactional sessions, and create a txn handle bound to the custom client.
  shared_ptr<KuduTransaction> txn_orig_client;
  ASSERT_OK(client_->NewTransaction(&txn_orig_client));
  shared_ptr<KuduClient> c;
  {
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(kShortTimeout);
    builder.default_rpc_timeout(kShortTimeout);
    ASSERT_OK(cluster_->CreateClient(&builder, &c));
  }
  string txn_token;
  ASSERT_OK(txn_orig_client->Serialize(&txn_token));
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(KuduTransaction::Deserialize(c, txn_token, &txn));

  shared_ptr<KuduSession> session;
  int64_t key = 0;
  auto s = InsertRows(txn.get(), 1, &key, &session);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  auto row_status = GetSingleRowError(session.get());
  ASSERT_TRUE(row_status.IsTimedOut()) << row_status.ToString();
  ASSERT_STR_CONTAINS(row_status.ToString(),
                      "Failed to write batch of 1 ops to tablet");

  // Now remove the injected latency.
  FLAGS_txn_participant_begin_op_inject_latency_ms = 0;
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
  });

  constexpr auto kRowsNum = 8;
  ASSERT_OK(InsertRows(txn.get(), kRowsNum, &key));

  {
    const auto dmap = GetTxnOpDispatchers();
    ASSERT_EQ(1, dmap.size());
    auto& [dmap_txn_id, dispatchers] = *(dmap.begin());
    ASSERT_EQ(GetTxnId(txn), dmap_txn_id);
    for (auto& d : dispatchers) {
      std::lock_guard<simple_spinlock> guard(d->lock_);
      ASSERT_TRUE(d->ops_queue_.empty());
      ASSERT_FALSE(d->unregistered_);
      ASSERT_OK(d->inflight_status_);
      ASSERT_TRUE(d->preliminary_tasks_completed_);
    }
  }

  // Commit the transaction and verify the row count.
  ASSERT_OK(txn_orig_client->Commit());
  size_t row_count;
  ASSERT_OK(CountRows(table_.get(), &row_count));
  ASSERT_EQ(kRowsNum, row_count);

  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
}

// This scenario exercises the case when a request to rollback a transaction
// arrives while TxnOpDispatcher still has pending write requests in its queue.
// The registration of the txn participant is not yet complete when the rollback
// request arrives.
TEST_F(TxnOpDispatcherITest, RollbackWriteOpPendingParticipantNotYetRegistered) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr auto kDelayMs = 1000;
  FLAGS_txn_participant_registration_inject_latency_ms = 2 * kDelayMs;

  NO_FATALS(Prepare(1));

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  Status rollback_status;
  thread rollback([&txn, &rollback_status]{
    SleepFor(MonoDelta::FromMilliseconds(kDelayMs));
    rollback_status = txn->Rollback();
  });
  auto cleanup = MakeScopedCleanup([&]() {
    rollback.join();
  });

  shared_ptr<KuduSession> session;
  int64_t key = 0;
  const auto s = InsertRows(txn.get(), 1, &key, &session);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  const auto row_status = GetSingleRowError(session.get());
  ASSERT_TRUE(row_status.IsIllegalState()) << s.ToString();

  rollback.join();
  cleanup.cancel();
  ASSERT_OK(rollback_status);

  bool is_complete = false;
  Status completion_status;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(is_complete);
  });
  const auto errmsg = completion_status.ToString();
  ASSERT_TRUE(completion_status.IsAborted()) << errmsg;
  ASSERT_STR_MATCHES(errmsg, "transaction has been aborted");

  size_t num_rows;
  ASSERT_OK(CountRows(table_.get(), &num_rows));
  ASSERT_EQ(0, num_rows);

  // Since the write operation has been responded with an error, a proper
  // clean up should be run and there should be no TxnOpDispatcher registered
  // for the transaction.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
}

// This scenario exercises the case when a request to rollback a transaction
// arrives while TxnOpDispatcher still has pending write requests in its queue
// but it has already completed the registration of the txn participant.
// BEGIN_TXN hasn't yet been sent for the participant tablet when the rollback
// request arrives.
TEST_F(TxnOpDispatcherITest, RollbackWriteOpPendingParticipantRegistered) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr auto kDelayMs = 1000;
  FLAGS_txn_participant_begin_op_inject_latency_ms = 2 * kDelayMs;

  NO_FATALS(Prepare(1));

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  Status rollback_status;
  thread rollback([&txn, &rollback_status]{
    SleepFor(MonoDelta::FromMilliseconds(kDelayMs));
    rollback_status = txn->Rollback();
  });
  auto cleanup = MakeScopedCleanup([&]() {
    rollback.join();
  });

  shared_ptr<KuduSession> session;
  int64_t key = 0;
  Status s = InsertRows(txn.get(), 1, &key, &session);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  rollback.join();
  cleanup.cancel();
  ASSERT_OK(rollback_status);

  bool is_complete = false;
  Status completion_status;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(is_complete);
  });
  const auto errmsg = completion_status.ToString();
  ASSERT_TRUE(completion_status.IsAborted()) << errmsg;
  ASSERT_STR_MATCHES(errmsg, "transaction has been aborted");

  size_t num_rows = 0;
  ASSERT_OK(CountRows(table_.get(), &num_rows));
  ASSERT_EQ(0, num_rows);
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());
}

TEST_F(TxnOpDispatcherITest, TxnWriteWhileReplicaDeleted) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr auto kDelayMs = 1000;
  FLAGS_txn_participant_begin_op_inject_latency_ms = 2 * kDelayMs;
  constexpr auto kMaxPendingOps = 8;
  FLAGS_tablet_max_pending_txn_write_ops = kMaxPendingOps;

  NO_FATALS(Prepare(1));
  auto replicas = GetAllReplicas();
  ASSERT_FALSE(replicas.empty());
  const auto tablet_id = replicas.front()->tablet_id();

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  Status tablet_delete_status;
  thread tablet_deleter([&]{
    SleepFor(MonoDelta::FromMilliseconds(kDelayMs));
    tablet_delete_status = cluster_->mini_tablet_server(0)->server()->
        tablet_manager()->DeleteTablet(tablet_id,
                                       tablet::TABLET_DATA_TOMBSTONED,
                                       boost::none);
  });
  auto cleanup = MakeScopedCleanup([&]() {
    tablet_deleter.join();
  });

  shared_ptr<KuduSession> session;
  int64_t key = 0;
  // Send multiple rows (up to the capacity of the queue in TxnOpDispatcher),
  // so at least one row is sent to the deleted replica.
  auto s = InsertRows(txn.get(), kMaxPendingOps, &key, &session);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  auto op_status = GetSingleRowError(session.get());
  ASSERT_TRUE(op_status.IsTimedOut()) << op_status.ToString();
  ASSERT_STR_CONTAINS(op_status.ToString(), "STOPPED");

  // The leader replica of other tablet might still have its TxnOpDispatcher
  // registered.
  ASSERT_GE(1, GetTxnOpDispatchersTotalCount(replicas));

  cleanup.cancel();
  tablet_deleter.join();
  ASSERT_OK(tablet_delete_status);
}

// This is similar to TxnWriteWhileReplicaDeleted, but this more synthetic
// scenario with multiple tablet servers is to verify a couple of edge cases:
//   * TxnOpDispatchers are properly unregistered when there is an error while
//     sumbitting the accumulated write operations from the queue
//   * it's possible to rollback such a transaction with write operations
//     failed due to non-running tablet replicas
TEST_F(TxnOpDispatcherITest, TxnWriteWhenReplicaIsShutdown) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr auto kDelayMs = 1000;
  FLAGS_txn_participant_begin_op_inject_latency_ms = 2 * kDelayMs;

  constexpr auto kMaxPendingOps = 16;
  FLAGS_tablet_max_pending_txn_write_ops = kMaxPendingOps;

  constexpr auto kTServers = 3;
  NO_FATALS(Prepare(kTServers));
  auto replicas = GetAllReplicas();
  ASSERT_FALSE(replicas.empty());

  // No dispatches should be registered at this point.
  ASSERT_GE(0, GetTxnOpDispatchersTotalCount(replicas));

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  atomic<size_t> txn_dispatchers_count = 0;
  vector<thread> stoppers;
  stoppers.reserve(replicas.size());
  for (auto r : replicas) {
    stoppers.emplace_back([r, &txn_dispatchers_count, this]{
      SleepFor(MonoDelta::FromMilliseconds(kDelayMs));
      txn_dispatchers_count += GetTxnOpDispatchersTotalCount({ r });
      r->Shutdown();
    });
  }
  auto cleanup = MakeScopedCleanup([&]() {
    std::for_each(stoppers.begin(), stoppers.end(), [](thread& t) { t.join(); });
  });

  // Send multiple rows up to the capacity of the queue in TxnOpDispatcher.
  int64_t key = 0;
  shared_ptr<KuduSession> session;
  auto s = InsertRows(txn.get(), kMaxPendingOps, &key, &session);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  auto op_status = GetSingleRowError(session.get());
  ASSERT_TRUE(op_status.IsTimedOut()) << op_status.ToString();
  ASSERT_STR_CONTAINS(op_status.ToString(), "SHUTDOWN");

  // Make sure there was at least one dispatcher spotted while accumulating
  // write operations. All write requests were sent to leader replicas of two
  // tablets, and there might be a change in a replica's leadership.
  ASSERT_GT(txn_dispatchers_count, 0);
  // No TxnOpDispatchers should be left at leader replicas after write
  // operations failed.
  ASSERT_LE(GetTxnOpDispatchersTotalCount(replicas),
            kNumPartitions * (kTServers - 1));

  // It should be possible to rollback the transaction.
  ASSERT_OK(txn->Rollback());

  // There should be no TxnOpDispatchers registered after rolling back
  // the transaction.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount(replicas));
}

// Verify the functionality of the --tserver_txn_write_op_handling_enabled flag
// (the flag is for testing purposes only, though).
TEST_F(TxnOpDispatcherITest, TxnWriteOpHandlingEnabledFlag) {
  FLAGS_tserver_txn_write_op_handling_enabled = false;
  NO_FATALS(Prepare(1));

  // It's a clean slate: no TxnOpDispatchers should be around.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  int64_t key = 0;
  shared_ptr<KuduSession> session;
  auto s = InsertRows(txn.get(), 2, &key, &session);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  auto row_status = GetSingleRowError(session.get());
  ASSERT_TRUE(row_status.IsNotFound());
  ASSERT_STR_CONTAINS(row_status.ToString(),
                      Substitute("txn $0 not found on tablet", GetTxnId(txn)));

  // No dispatchers should be around: --tserver_txn_write_op_handling_enabled
  // is set to false.
  ASSERT_EQ(0, GetTxnOpDispatchersTotalCount());

  // It should be possible to commit an empty transaction after that.
  ASSERT_OK(txn->Commit());
  size_t num_rows = 0;
  ASSERT_OK(CountRows(table_.get(), &num_rows));
  ASSERT_EQ(0, num_rows);
}

// This scenario verifies that tablet servers are able to accept transactional
// write requests upon restarting while automatically registering corresponding
// tablets as transaction participants.
//
// TODO(aserbin): clarify why sometimes both the first and the second
//                sub-scenarios fail with timeout error even if the timeout
//                set to ample 300 seconds
//
// TODO(aserbin): clarify why sometimes the scond sub-scenario below fails with
//                an error like below:
//
//  src/kudu/integration-tests/txn_write_ops-itest.cc:1210: Failure
//  Expected equality of these values:
//    16
//    row_count
//      Which is: 13
//
TEST_F(TxnOpDispatcherITest, DISABLED_TxnMultipleSingleRowsWithServerRestart) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(Prepare(3));

  // The test scenario below might require multiple retries from the client if
  // running on a slow or overloaded machine, so the timeout for RPC operations
  // is set higher than the default setting to avoid false positives.
  shared_ptr<KuduClient> c;
  {
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(MonoDelta::FromSeconds(300));
    builder.default_rpc_timeout(MonoDelta::FromSeconds(300));
    ASSERT_OK(cluster_->CreateClient(&builder, &c));
  }

  int64_t key = 0;
  // Restart all tablet servers between every row written, waiting for each
  // tablet server to be up and running before trying to write the next row.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(c->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    shared_ptr<KuduTable> table;
    ASSERT_OK(c->OpenTable(kTableName, &table));

    for (auto row_idx = 0; row_idx < 8; ++row_idx) {
      unique_ptr<KuduInsert> ins = BuildInsert(table.get(), key++);
      ASSERT_OK(session->Apply(ins.release()));
      for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
        auto* ts = cluster_->mini_tablet_server(i);
        ts->Shutdown();
        ASSERT_OK(ts->Restart());
        ASSERT_OK(ts->WaitStarted());
      }
    }

    ASSERT_OK(txn->Commit());
    size_t row_count = 0;
    ASSERT_OK(CountRows(table_.get(), &row_count));
    ASSERT_EQ(8, row_count);
  }

  // Restart one tablet server in a round-robin fashion with every row written,
  // not waiting for the tablet server to be up and running before trying
  // to write the next row.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(c->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    shared_ptr<KuduTable> table;
    ASSERT_OK(c->OpenTable(kTableName, &table));

    const auto num_servers = cluster_->num_tablet_servers();
    for (auto row_idx = 8; row_idx < 16; ++row_idx) {
      unique_ptr<KuduInsert> ins = BuildInsert(table.get(), key++);
      ASSERT_OK(session->Apply(ins.release()));
      auto* ts = cluster_->mini_tablet_server(row_idx % num_servers);
      ts->Shutdown();
      ASSERT_OK(ts->Restart());
    }

    ASSERT_OK(txn->Commit());
    size_t row_count = 0;
    ASSERT_OK(CountRows(table_.get(), &row_count));
    ASSERT_EQ(16, row_count);
  }
}

// Test beginning and aborting a transaction from the same test workload.
TEST_F(TxnOpDispatcherITest, TestBeginAbortTransactionalTestWorkload) {
  NO_FATALS(SetupCluster(1));
  TestWorkload w(cluster_.get(), TestWorkload::PartitioningType::HASH);
  w.set_num_replicas(1);
  w.set_num_tablets(3);
  w.set_begin_txn();
  w.set_rollback_txn();
  w.Setup();
  w.Start();
  const auto& table_name = w.table_name();
  while (w.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }
  // Each participant should have a dispatcher.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(3, GetTxnOpDispatchersTotalCount({}, table_name));
  });
  w.StopAndJoin();
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount({}, table_name));
  });
  // By the end of it, we should have aborted the rows and they should not be
  // visible to clients.
  size_t num_rows;
  ASSERT_OK(CountRows(w.client().get(), table_name, &num_rows));
  ASSERT_EQ(0, num_rows);
}

// Test beginning and committing a transaction from the same test workload.
TEST_F(TxnOpDispatcherITest, TestBeginCommitTransactionalTestWorkload) {
  NO_FATALS(SetupCluster(1));
  TestWorkload w(cluster_.get(), TestWorkload::PartitioningType::HASH);
  w.set_num_replicas(1);
  w.set_num_tablets(3);
  w.set_begin_txn();
  w.set_commit_txn();
  w.Setup();
  w.Start();
  const auto& table_name = w.table_name();
  while (w.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }
  // Each participant should have a dispatcher.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(3, GetTxnOpDispatchersTotalCount({}, table_name));
  });
  w.StopAndJoin();
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, GetTxnOpDispatchersTotalCount({}, table_name));
  });
  // By the end of it, we should have committed the rows and they should be
  // visible to clients.
  size_t num_rows;
  ASSERT_OK(CountRows(w.client().get(), table_name, &num_rows));
  ASSERT_EQ(w.rows_inserted(), num_rows);
}

// Test beginning and committing a transaction from separate test workloads.
TEST_F(TxnOpDispatcherITest, TestSeparateBeginCommitTestWorkloads) {
  NO_FATALS(SetupCluster(1));
  int64_t txn_id;
  string first_table_name;
  size_t first_rows_inserted;
  {
    TestWorkload w(cluster_.get(), TestWorkload::PartitioningType::HASH);
    w.set_begin_txn();
    w.set_num_replicas(1);
    w.set_num_tablets(3);
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 1000) {
      SleepFor(MonoDelta::FromMilliseconds(5));
    }
    first_table_name = w.table_name();
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(3, GetTxnOpDispatchersTotalCount({}, first_table_name));
    });
    w.StopAndJoin();
    first_rows_inserted = w.rows_inserted();
    txn_id = w.txn_id();
    size_t num_rows;
    ASSERT_OK(CountRows(w.client().get(), first_table_name, &num_rows));
    ASSERT_EQ(0, num_rows);
  }
  // Create a new workload, and insert as a part of the same transaction.
  {
    TestWorkload w(cluster_.get(), TestWorkload::PartitioningType::HASH);
    const auto& kSecondTableName = "default.second_table";
    w.set_txn_id(txn_id);
    w.set_commit_txn();
    w.set_table_name(kSecondTableName);
    w.set_num_replicas(1);
    w.set_num_tablets(3);
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 1000) {
      SleepFor(MonoDelta::FromMilliseconds(5));
    }
    // We should have dispatchers for both tables.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(3, GetTxnOpDispatchersTotalCount({}, first_table_name));
      ASSERT_EQ(3, GetTxnOpDispatchersTotalCount({}, kSecondTableName));
    });
    w.StopAndJoin();
    // Once committed, the dispatchers should be unregistered.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(0, GetTxnOpDispatchersTotalCount({}, first_table_name));
      ASSERT_EQ(0, GetTxnOpDispatchersTotalCount({}, kSecondTableName));
    });
    size_t num_rows;
    ASSERT_OK(CountRows(w.client().get(), first_table_name, &num_rows));
    ASSERT_EQ(first_rows_inserted, num_rows);
    ASSERT_OK(CountRows(w.client().get(), kSecondTableName, &num_rows));
    ASSERT_EQ(w.rows_inserted(), num_rows);
  }
}


} // namespace kudu
