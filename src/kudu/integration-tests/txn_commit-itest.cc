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

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_coordinator.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_bool(txn_schedule_background_tasks);
DECLARE_int32(txn_status_manager_inject_latency_finalize_commit_ms);
DECLARE_uint32(txn_background_rpc_timeout_ms);
DECLARE_uint32(txn_keepalive_interval_ms);
DECLARE_uint32(txn_manager_status_table_num_replicas);
DECLARE_uint32(txn_staleness_tracker_interval_ms);

using kudu::client::KuduClient;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTransaction;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tablet::TabletReplica;
using kudu::tablet::TxnParticipant;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnSystemClient;
using kudu::transactions::TxnTokenPB;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::ParticipantOpPB;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

class TxnCommitITest : public KuduTest {
 public:
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  const int kNumRowsPerTxn = 10;

  void SetUp() override {
    KuduTest::SetUp();
    // Speed up the staleness checks to help stress cases where it might race
    // with commits.
    FLAGS_txn_keepalive_interval_ms = 300;
    FLAGS_txn_staleness_tracker_interval_ms = 100;
    NO_FATALS(SetUpClusterAndTable(1));
  }

  // Sets up a cluster with the given number of tservers, creating a
  // single-replica transaction status table and user-defined table.
  void SetUpClusterAndTable(int num_tservers, int num_replicas = 1) {
    FLAGS_txn_manager_enabled = true;
    FLAGS_txn_manager_lazily_initialized = false;
    FLAGS_txn_manager_status_table_num_replicas = num_replicas;

    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_tservers;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_EVENTUALLY([&] {
      // Find the TxnStatusManager's tablet ID.
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        auto* ts = cluster_->mini_tablet_server(i);
        ASSERT_OK(ts->WaitStarted());
        const auto tablet_ids = ts->ListTablets();
        if (tablet_ids.empty()) continue;
        tsm_id_ = tablet_ids[0];
      }
      ASSERT_FALSE(tsm_id_.empty());
    });
    TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client_);
    ASSERT_OK(txn_client_->OpenTxnStatusTable());

    client::KuduClientBuilder builder;
    builder.default_admin_operation_timeout(kTimeout);
    ASSERT_OK(cluster_->CreateClient(&builder, &client_));
    string authn_creds;
    ASSERT_OK(client_->ExportAuthenticationCredentials(&authn_creds));
    client::AuthenticationCredentialsPB pb;
    ASSERT_TRUE(pb.ParseFromString(authn_creds));
    ASSERT_TRUE(pb.has_real_user());
    client_user_ = pb.real_user();

    TestWorkload w(cluster_.get(), TestWorkload::PartitioningType::HASH);
    w.set_num_replicas(num_replicas);
    w.set_num_tablets(2);
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 1) {
      SleepFor(MonoDelta::FromMilliseconds(50));
    }
    w.StopAndJoin();
    table_name_ = w.table_name();
    initial_row_count_ = w.rows_inserted();

    // Since the test table uses the hash partitioning scheme, every tablet gets
    // at least one write operation when inserting several rows into the test
    // table. So, for every transaction inserting several rows into the test
    // table, it's easy to build the list of transaction participants.
    unordered_set<string> tablet_ids;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      for (const auto& tablet_id : ts->ListTablets()) {
        if (tablet_id != tsm_id_) {
          tablet_ids.emplace(tablet_id);
        }
      }
    }
    for (auto& t : tablet_ids) {
      participant_ids_.emplace_back(std::move(t));
    }
  }

  // Start a transaction, manually registering the given participants, and
  // returning the associated transaction and session handles.
  Status BeginTransaction(shared_ptr<KuduTransaction>* txn,
                          shared_ptr<KuduSession>* session) {
    shared_ptr<KuduTransaction> txn_local;
    RETURN_NOT_OK(client_->NewTransaction(&txn_local));
    shared_ptr<KuduSession> txn_session_local;
    RETURN_NOT_OK(txn_local->CreateSession(&txn_session_local));
    *txn = std::move(txn_local);
    *session = std::move(txn_session_local);
    return Status::OK();
  }

  // Insert 'num_rows' rows to the given session, starting with 'start_row', to
  // every table in 'table_names' or 'table_name_' if not set.
  Status InsertToSession(
      const shared_ptr<KuduSession>& txn_session, int start_row, int num_rows,
      vector<string> table_names = {}) {
    if (table_names.empty()) {
      table_names = { table_name_ };
    }
    for (const auto& table_name : table_names) {
      shared_ptr<KuduTable> table;
      RETURN_NOT_OK(client_->OpenTable(table_name, &table));
      const int target_row_id = start_row + num_rows;
      for (int i = start_row; i < target_row_id; i++) {
        auto* insert = table->NewInsertIgnore();
        RETURN_NOT_OK(insert->mutable_row()->SetInt32(0, i));
        RETURN_NOT_OK(insert->mutable_row()->SetInt32(1, i));
        RETURN_NOT_OK(txn_session->Apply(insert));
        RETURN_NOT_OK(txn_session->Flush());
      }
    }
    return Status::OK();
  }

  Status CountRows(int* num_rows) {
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name_, &table));
    KuduScanner scanner(table.get());
    RETURN_NOT_OK(scanner.Open());
    int rows = 0;
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      RETURN_NOT_OK(scanner.NextBatch(&batch));
      rows += batch.NumRows();
    }
    *num_rows = rows;
    return Status::OK();
  }

  // Returns the transaction IDs participated in by 'tablet_id' that have been
  // aborted on 'num_replicas' replicas.
  Status GetAbortedTxnsForParticipant(const string& tablet_id, int num_replicas,
                                      vector<TxnId>* aborted_txns) {
    vector<TxnId> txn_ids;
    unordered_map<int64_t, int> aborts_per_txn;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      scoped_refptr<TabletReplica> r;
      Status s = cluster_->mini_tablet_server(i)->server()->tablet_manager()->GetTabletReplica(
          tablet_id, &r);
      if (!s.ok()) {
        continue;
      }
      for (const auto& e : r->tablet()->txn_participant()->GetTxnsForTests()) {
        if (e.state == tablet::TxnState::kAborted) {
          LookupOrEmplace(&aborts_per_txn, e.txn_id, 0)++;
        }
      }
    }
    for (const auto& txn_id_and_count : aborts_per_txn) {
      if (txn_id_and_count.second == num_replicas) {
        txn_ids.emplace_back(txn_id_and_count.first);
      }
    }
    *aborted_txns = std::move(txn_ids);
    return Status::OK();
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<TxnSystemClient> txn_client_;

  shared_ptr<KuduClient> client_;
  string client_user_;

  string table_name_;
  int initial_row_count_;

  // Needed for checking on internals of txn participants.
  string tsm_id_;
  vector<string> participant_ids_;
};

TEST_F(TxnCommitITest, TestBasicCommits) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));

  // Even though we've inserted, we shouldn't be able to see any new rows until
  // after we commit.
  int num_rows = 0;
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_, num_rows);

  ASSERT_OK(txn->Commit());
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_ + kNumRowsPerTxn, num_rows);

  // IsCommitComplete() should verify that the transaction is in the right
  // state.
  Status completion_status;
  bool is_complete;
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_OK(completion_status);
  ASSERT_TRUE(is_complete);
}

TEST_F(TxnCommitITest, TestBasicAborts) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));

  int num_rows = 0;
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_, num_rows);

  ASSERT_OK(txn->Rollback());
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_, num_rows);

  // IsCommitComplete() should verify that the transaction is aborted. We need
  // to wait for this to happen, since 'is_complete' is contingent on the abort
  // tasks finishing.
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });

  // On the participants, we should see the transaction as aborted.
  ASSERT_EVENTUALLY([&] {
    for (const auto& tablet_id : participant_ids_) {
      vector<TxnId> aborted_txns;
      ASSERT_OK(GetAbortedTxnsForParticipant(tablet_id, 1, &aborted_txns));
      ASSERT_EQ(1, aborted_txns.size());
    }
  });
}

TEST_F(TxnCommitITest, TestAbortInProgress) {
  FLAGS_txn_schedule_background_tasks = false;
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  ASSERT_OK(txn->Rollback());

  // When background tasks are disabled, we'll be left in ABORT_IN_PROGRESS,
  // and we should be able to determine it isn't complete.
  Status completion_status;
  bool is_complete = true;
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
  ASSERT_STR_CONTAINS(completion_status.ToString(), "transaction is being aborted");
  ASSERT_FALSE(is_complete);

  // Once enabled, background tasks should take hold and the abort should
  // complete.
  FLAGS_txn_schedule_background_tasks = true;
  auto* mts = cluster_->mini_tablet_server(0);
  mts->Shutdown();
  ASSERT_OK(mts->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_STR_CONTAINS(completion_status.ToString(), "transaction has been aborted");
    ASSERT_TRUE(is_complete);
  });
}

TEST_F(TxnCommitITest, TestBackgroundAborts) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  string serialized_txn;
  {
    shared_ptr<KuduTransaction> txn;
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&txn, &txn_session));
    ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));

    int num_rows = 0;
    ASSERT_OK(CountRows(&num_rows));
    ASSERT_EQ(initial_row_count_, num_rows);
    ASSERT_OK(txn->Serialize(&serialized_txn));
  }
  // Wait a bit for a background abort to happen.
  SleepFor(MonoDelta::FromMilliseconds(5 * FLAGS_txn_keepalive_interval_ms));

  // IsCommitComplete() should verify that the transaction is aborted.
  Status completion_status;
  bool is_complete = false;
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(KuduTransaction::Deserialize(client_, serialized_txn, &txn));
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
  ASSERT_TRUE(is_complete);

  // On the participants, we should see the transaction as aborted.
  ASSERT_EVENTUALLY([&] {
    for (const auto& tablet_id : participant_ids_) {
      vector<TxnId> aborted_txns;
      ASSERT_OK(GetAbortedTxnsForParticipant(tablet_id, 1, &aborted_txns));
      ASSERT_EQ(1, aborted_txns.size());
    }
  });
}

// Test that if we delete the TxnStatusManager while tasks are on-going,
// nothing goes catastrophically wrong (i.e. no crashes).
TEST_F(TxnCommitITest, TestCommitWhileDeletingTxnStatusManager) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));

  ASSERT_OK(txn->Commit(/*wait*/false));
  ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()->DeleteTablet(
      tsm_id_, tablet::TABLET_DATA_TOMBSTONED, boost::none));

  Status completion_status;
  bool is_complete;
  Status s = txn->IsCommitComplete(&is_complete, &completion_status);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
}

TEST_F(TxnCommitITest, TestCommitAfterDeletingParticipant) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  ASSERT_OK(client_->DeleteTable(table_name_));
  ASSERT_OK(txn->Commit(/*wait*/false));

  // The transaction should eventually fail, treating the deleted participant
  // as a fatal error.
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });
}

TEST_F(TxnCommitITest, TestCommitAfterDroppingRangeParticipant) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  ASSERT_OK(client_->DeleteTable(table_name_));
  const auto& schema = client::KuduSchema::FromSchema(GetSimpleTestSchema());
  unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name_));
  alterer->DropRangePartition(schema.NewRow(), schema.NewRow());
  alterer.reset();

  ASSERT_OK(txn->Commit(/*wait*/false));

  // The transaction should eventually abort, treating the deleted participant
  // as fatal, resulting in an aborted transaction.
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });
}

TEST_F(TxnCommitITest, TestRestartingWhileCommitting) {
  FLAGS_txn_status_manager_inject_latency_finalize_commit_ms = 10000;
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  ASSERT_OK(txn->Commit(/*wait*/false));
  // Stop the tserver without allowing the finalize commit to complete.
  cluster_->mini_tablet_server(0)->Shutdown();

  FLAGS_txn_schedule_background_tasks = false;
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());

  // The transaction should be incomplete, as background tasks are disabled,
  // and since we shut down before allowing to finish committing.
  Status completion_status;
  bool is_complete = false;
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsIncomplete()) << completion_status.ToString();
  ASSERT_FALSE(is_complete);

  // If we re-enable background tasks, background tasks should be scheduled to
  // commit the transaction.
  FLAGS_txn_schedule_background_tasks = true;
  cluster_->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_OK(completion_status);
    ASSERT_TRUE(is_complete);
  });
}

// Test aborting a botched commit mid-way by deleting some of its participants
// while committing. The result should be that the transaction gets aborted and
// all participants abort the local transactions.
TEST_F(TxnCommitITest, TestAbortRacingWithBotchedCommit) {
  // First, create another table that we'll delete later on.
  const string kSecondTableName = "default.second_table";
  TestWorkload w(cluster_.get(), TestWorkload::PartitioningType::HASH);
  w.set_num_replicas(1);
  w.set_num_tablets(2);
  w.set_table_name(kSecondTableName);
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 1) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  w.StopAndJoin();
  unordered_set<string> participant_ids;
  auto* mts = cluster_->mini_tablet_server(0);
  for (const auto& tablet_id : mts->ListTablets()) {
    if (tablet_id != tsm_id_) {
      participant_ids.emplace(tablet_id);
    }
  }
  ASSERT_EQ(4, participant_ids.size());
  vector<string> both_tables_participant_ids(participant_ids.begin(), participant_ids.end());
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn,
                            { table_name_, kSecondTableName }));
  ASSERT_OK(client_->DeleteTable(kSecondTableName));
  FLAGS_txn_status_manager_inject_latency_finalize_commit_ms = 2000;
  ASSERT_OK(txn->Commit(/*wait*/false));
  ASSERT_OK(txn->Rollback());
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });

  // Let's confirm that all remaining participants see the same transaction
  // metadata.
  vector<scoped_refptr<TabletReplica>> replicas;
  mts->server()->tablet_manager()->GetTabletReplicas(&replicas);
  vector<vector<TxnParticipant::TxnEntry>> txn_entries_per_replica;
  for (const auto& r : replicas) {
    if (r->tablet_id() != tsm_id_ && r->tablet_metadata()->table_name() != kSecondTableName) {
      txn_entries_per_replica.emplace_back(r->tablet()->txn_participant()->GetTxnsForTests());
    }
  }
  ASSERT_GT(txn_entries_per_replica.size(), 1);
  for (int i = 1; i < txn_entries_per_replica.size(); i++) {
    const auto& txns = txn_entries_per_replica[i];
    ASSERT_FALSE(txns.empty());
    for (const auto& txn_entry : txns) {
      ASSERT_EQ(tablet::kAborted, txn_entry.state);
    }
    EXPECT_EQ(txn_entries_per_replica[0], txns);
  }
}

// Test restarting while commit tasks are on-going, while at the same time,
// some participants are deleted. The transaction should be aborted on all
// participants.
TEST_F(TxnCommitITest, TestRestartingWhileCommittingAndDeleting) {
  // First, create another table that we'll delete later on.
  const string kSecondTableName = "default.second_table";
  TestWorkload w(cluster_.get());
  w.set_num_replicas(1);
  w.set_num_tablets(2);
  w.set_table_name(kSecondTableName);
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 1) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  w.StopAndJoin();

  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn,
                            { table_name_, kSecondTableName }));
  ASSERT_OK(client_->DeleteTable(kSecondTableName));
  FLAGS_txn_status_manager_inject_latency_finalize_commit_ms = 10000;
  ASSERT_OK(txn->Commit(/*wait*/false));

  // Shut down without giving time for the commit to complete.
  auto* mts = cluster_->mini_tablet_server(0);
  mts->Shutdown();
  ASSERT_OK(mts->Restart());

  // Delete some of the participants. Upon completion, the commit process
  // should result in an abort.
  ASSERT_OK(mts->server()->tablet_manager()->WaitForAllBootstrapsToFinish());
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });

  // Let's confirm that all remaining participants see the same transaction
  // metadata.
  vector<scoped_refptr<TabletReplica>> replicas;
  mts->server()->tablet_manager()->GetTabletReplicas(&replicas);
  vector<vector<TxnParticipant::TxnEntry>> txn_entries_per_replica;
  for (const auto& r : replicas) {
    if (r->tablet_id() != tsm_id_ && r->tablet_metadata()->table_name() != kSecondTableName) {
      txn_entries_per_replica.emplace_back(r->tablet()->txn_participant()->GetTxnsForTests());
    }
  }
  ASSERT_GT(txn_entries_per_replica.size(), 1);
  for (int i = 1; i < txn_entries_per_replica.size(); i++) {
    const auto& txns = txn_entries_per_replica[i];
    ASSERT_FALSE(txns.empty());
    for (const auto& txn_entry : txns) {
      ASSERT_EQ(tablet::kAborted, txn_entry.state);
    }
    EXPECT_EQ(txn_entries_per_replica[0], txns);
  }
}

// Test that when loading the TxnStatusManagers, nothing catastrophic happens
// if we can't connect to the masters.
TEST_F(TxnCommitITest, TestLoadTxnStatusManagerWhenNoMasters) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));

  cluster_->mini_master()->Shutdown();
  cluster_->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());

  // While the master is down, we can't contact the TxnManager.
  Status s = BeginTransaction(&txn, &txn_session);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Once restarted, it should be business as usual.
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(BeginTransaction(&txn, &txn_session));
  });
  scoped_refptr<tablet::TabletReplica> tsm_replica;
  auto* tablet_manager = cluster_->mini_tablet_server(0)->server()->tablet_manager();
  ASSERT_OK(tablet_manager->GetTabletReplica(tsm_id_, &tsm_replica));
  auto participants_by_txn_id =
      DCHECK_NOTNULL(tsm_replica->txn_coordinator())->GetParticipantsByTxnIdForTests();
  ASSERT_EQ(2, participants_by_txn_id.size());
}

// Test what happens if a participant is aborted somehow, and we try to commit.
// We don't expect this to happen since aborts should start with writing an
// abort record to the TxnStatusManager, but let's at least make sure we
// understand what happens if this does occur.
TEST_F(TxnCommitITest, TestCommitAfterParticipantAbort) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));

  // Send an ABORT_TXN op to the participant.
  ParticipantOpPB op_pb;
  op_pb.set_txn_id(0);
  op_pb.set_type(ParticipantOpPB::ABORT_TXN);
  ASSERT_OK(txn_client_->ParticipateInTransaction(
      participant_ids_[0], op_pb, MonoDelta::FromSeconds(3)));

  // When we try to commit, we should end up not completing.
  ASSERT_OK(txn->Commit(/*wait*/false));

  SleepFor(MonoDelta::FromSeconds(3));
  Status completion_status;
  bool is_complete;
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
}

// Try concurrently beginning to commit a bunch of different transactions.
TEST_F(TxnCommitITest, TestConcurrentCommitCalls) {
  constexpr const int kNumTxns = 4;
  vector<shared_ptr<KuduTransaction>> txns(kNumTxns);
  int row_start = initial_row_count_;
  for (int i = 0; i < kNumTxns; i++) {
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&txns[i], &txn_session));
    ASSERT_OK(InsertToSession(txn_session, row_start, kNumRowsPerTxn));
    row_start += kNumRowsPerTxn;
  }
  int num_rows = 0;
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_, num_rows);

  vector<thread> threads;
  vector<Status> results(kNumTxns);
  for (int i = 0; i < kNumTxns; i++) {
    threads.emplace_back([&, i] {
      results[i] = txns[i]->Commit(/*wait*/false);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& s : results) {
    EXPECT_OK(s);
  }
  ASSERT_EVENTUALLY([&] {
    for (const auto& txn : txns) {
      Status completion_status;
      bool is_complete;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
      ASSERT_OK(completion_status);
      ASSERT_TRUE(is_complete);
    }
  });
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_ + kNumRowsPerTxn * kNumTxns, num_rows);
}

TEST_F(TxnCommitITest, TestConcurrentAbortsAndCommits) {
  constexpr const int kNumTxns = 10;
  vector<shared_ptr<KuduTransaction>> txns(kNumTxns);
  int row_start = initial_row_count_;
  for (int i = 0; i < kNumTxns; i++) {
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&txns[i], &txn_session));
    ASSERT_OK(InsertToSession(txn_session, row_start, kNumRowsPerTxn));
    row_start += kNumRowsPerTxn;
  }
  int num_rows = 0;
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_, num_rows);
  // To encourage races between concurrent aborts and commits, inject a random
  // sleep before each call.
  constexpr const int kMaxSleepMs = 1000;
  std::atomic<int> num_committed_txns = 0;
  vector<thread> threads;
  for (int i = 0; i < kNumTxns; i++) {
    threads.emplace_back([&, i] {
      SleepFor(MonoDelta::FromMilliseconds(rand() % kMaxSleepMs));
      Status s = txns[i]->Commit(/*wait*/true);
      if (s.ok()) {
        num_committed_txns++;
      }
    });
    threads.emplace_back([&, i] {
      SleepFor(MonoDelta::FromMilliseconds(rand() % kMaxSleepMs));
      ignore_result(txns[i]->Rollback());
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  ASSERT_OK(CountRows(&num_rows));
  // NOTE: we can compare an exact count here and not worry about whether we
  // completed aborting rows because even if we didn't complete the abort, they
  // shouldn't be visible to clients anyway.
  const int expected_rows = initial_row_count_ + kNumRowsPerTxn * num_committed_txns;
  VLOG(1) << Substitute("Expecting $0 rows from $1 committed transactions",
                        expected_rows, num_committed_txns.load());
  ASSERT_EQ(expected_rows, num_rows);

  // Ensure all transactions are either committed or aborted.
  vector<scoped_refptr<TabletReplica>> replicas;
  cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletReplicas(&replicas);
  for (const auto& r : replicas) {
    ASSERT_EVENTUALLY([&] {
      if (r->tablet_metadata()->table_name() == table_name_) {
        const auto txns = r->tablet()->txn_participant()->GetTxnsForTests();
        for (const auto& txn : txns) {
          ASSERT_TRUE(txn.state == tablet::kAborted || txn.state == tablet::kCommitted)
              << Substitute("Txn in unexpected state: $0", txn.state);;
        }
      }
    });
  }
}

// Test that committing the same transaction concurrently doesn't lead to any
// issues.
TEST_F(TxnCommitITest, TestConcurrentRepeatedCommitCalls) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  int num_rows = 0;
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_, num_rows);

  constexpr const int kNumThreads = 4;
  vector<thread> threads;
  vector<Status> results(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      results[i] = txn->Commit(/*wait*/false);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& s : results) {
    EXPECT_OK(s);
  }
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_OK(completion_status);
    ASSERT_TRUE(is_complete);
  });
  ASSERT_OK(CountRows(&num_rows));
  ASSERT_EQ(initial_row_count_ + kNumRowsPerTxn, num_rows);
}

TEST_F(TxnCommitITest, TestDontBackgroundAbortIfCommitInProgress) {
  FLAGS_txn_status_manager_inject_latency_finalize_commit_ms = 1000;
  string serialized_txn;
  {
    shared_ptr<KuduTransaction> txn;
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&txn, &txn_session));
    ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
    ASSERT_OK(txn->Commit(/*wait*/false));
    ASSERT_OK(txn->Serialize(&serialized_txn));
  }
  // Wait a bit to allow would-be background aborts to happen.
  SleepFor(MonoDelta::FromSeconds(1));

  // Since we've already begun committing, we shouldn't abort. On the contrary,
  // we should eventually successfully fully commit the transaction.
  bool is_complete = false;
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(KuduTransaction::Deserialize(client_, serialized_txn, &txn));
  Status completion_status;
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_FALSE(completion_status.IsAborted()) << completion_status.ToString();
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_OK(completion_status);
    ASSERT_TRUE(is_complete);
  });
}

// Test that we can abort if a transaction hasn't finalized its commit yet.
TEST_F(TxnCommitITest, TestAbortIfCommitInProgress) {
  FLAGS_txn_status_manager_inject_latency_finalize_commit_ms = 1000;
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  ASSERT_OK(txn->Commit(/*wait*/false));
  ASSERT_OK(txn->Rollback());
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted());
    ASSERT_TRUE(is_complete);
  });
}

// Test that has two nodes so we can place the TxnStatusManager and transaction
// participant on separate nodes. This can be useful for testing when some
// nodes are down.
class TwoNodeTxnCommitITest : public TxnCommitITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    NO_FATALS(SetUpClusterAndTable(2));

    // Figure out which tserver has the participant and which has the
    // TxnStatusManager.
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      const auto tablet_ids = ts->ListTablets();
      for (const auto& tablet_id : tablet_ids) {
        if (tablet_id == tsm_id_) {
          tsm_ts_ = ts;
          break;
        }
      }
    }
    DCHECK(tsm_ts_);
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      if (cluster_->mini_tablet_server(i) != tsm_ts_) {
        prt_ts_ = cluster_->mini_tablet_server(i);
        break;
      }
    }
    DCHECK(prt_ts_);
  }
 protected:
  // The tablet server that has a TxnStatusManager.
  MiniTabletServer* tsm_ts_;

  // A tablet server that has at least one participant.
  MiniTabletServer* prt_ts_;
};

// Test that nothing goes wrong when participants are down, and that we'll
// retry until they become available again.
TEST_F(TwoNodeTxnCommitITest, TestCommitWhenParticipantsAreDown) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));
  ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  prt_ts_->Shutdown();
  ASSERT_OK(txn->Commit(/*wait*/false));

  // Since our participant is down, we can't proceed with the commit.
  Status completion_status;
  bool is_complete;
  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_FALSE(is_complete);
  ASSERT_TRUE(completion_status.IsIncomplete()) << completion_status.ToString();
  SleepFor(MonoDelta::FromSeconds(5));

  ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_FALSE(is_complete);
  ASSERT_TRUE(completion_status.IsIncomplete()) << completion_status.ToString();

  // Once we start the tserver with the participant, the commit should complete
  // automatically.
  ASSERT_OK(prt_ts_->Restart());
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(is_complete);
  });
}

// Test that when we start up, pending commits will start background tasks to
// finalize the commit or abort.
TEST_F(TwoNodeTxnCommitITest, TestStartTasksDuringStartup) {
  shared_ptr<KuduTransaction> committed_txn;
  {
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&committed_txn, &txn_session));
    ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  }
  shared_ptr<KuduTransaction> aborted_txn;
  {
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&aborted_txn, &txn_session));
    ASSERT_OK(InsertToSession(txn_session, initial_row_count_ + kNumRowsPerTxn, kNumRowsPerTxn));
  }

  // Shut down our participant's tserver so our commit task keeps retrying.
  prt_ts_->Shutdown();
  ASSERT_OK(committed_txn->Commit(/*wait*/false));
  ASSERT_OK(aborted_txn->Rollback());

  Status completion_status;
  bool is_complete;
  ASSERT_OK(committed_txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_FALSE(is_complete);
  ASSERT_TRUE(completion_status.IsIncomplete()) << completion_status.ToString();

  ASSERT_OK(aborted_txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
  ASSERT_FALSE(is_complete);

  // Shut down the TxnStatusManager to stop our tasks.
  tsm_ts_->Shutdown();

  // Restart both tservers. The background tasks should be restarted and
  // eventually succeed.
  ASSERT_OK(prt_ts_->Restart());
  ASSERT_OK(tsm_ts_->Restart());
  ASSERT_EVENTUALLY([&] {
    Status completion_status;
    bool is_complete;
    ASSERT_OK(committed_txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_OK(completion_status);
    ASSERT_TRUE(is_complete);

    ASSERT_OK(aborted_txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });
  ASSERT_EVENTUALLY([&] {
    for (const auto& tablet_id : participant_ids_) {
      vector<TxnId> aborted_txns;
      ASSERT_OK(GetAbortedTxnsForParticipant(tablet_id, 1, &aborted_txns));
      ASSERT_EQ(1, aborted_txns.size());
    }
  });
}

// Abruptly shut down the tablet server while running commit tasks, ensuring
// nothing bad happens.
TEST_F(TwoNodeTxnCommitITest, TestCommitWhileShuttingDownTxnStatusManager) {
  shared_ptr<KuduTransaction> txn;
  shared_ptr<KuduSession> txn_session;
  ASSERT_OK(BeginTransaction(&txn, &txn_session));

  ASSERT_OK(txn->Commit(/*wait*/false));
  tsm_ts_->Shutdown();
  ASSERT_OK(tsm_ts_->Restart());

  Status completion_status;
  bool is_complete = false;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(is_complete);
  });
}

// Test that has three nodes so we can test leadership.
class ThreeNodeTxnCommitITest : public TxnCommitITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    NO_FATALS(SetUpClusterAndTable(3, 3));

    // Quiesce all but 'leader_idx_', so it becomes the leader.
    int leader_idx = 0;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = i != leader_idx;
    }
    leader_ts_ = cluster_->mini_tablet_server(leader_idx);
    // We should have two leaders for our table, and one for the
    // TxnStatusManager.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(3, leader_ts_->server()->num_raft_leaders()->value());
    });
  }
 protected:
  MiniTabletServer* leader_ts_;
};

TEST_F(ThreeNodeTxnCommitITest, TestCommitTasksReloadOnLeadershipChange) {
  FLAGS_txn_schedule_background_tasks = false;
  shared_ptr<KuduTransaction> committed_txn;
  shared_ptr<KuduTransaction> aborted_txn;
  {
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&committed_txn, &txn_session));
    ASSERT_OK(InsertToSession(txn_session, initial_row_count_, kNumRowsPerTxn));
  }
  {
    shared_ptr<KuduSession> txn_session;
    ASSERT_OK(BeginTransaction(&aborted_txn, &txn_session));
    ASSERT_OK(InsertToSession(txn_session, initial_row_count_ + kNumRowsPerTxn, kNumRowsPerTxn));
  }
  ASSERT_OK(committed_txn->Commit(/*wait*/ false));
  Status completion_status;
  bool is_complete = false;
  ASSERT_OK(committed_txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsIncomplete()) << completion_status.ToString();
  ASSERT_FALSE(is_complete);

  ASSERT_OK(aborted_txn->Rollback());
  ASSERT_OK(aborted_txn->IsCommitComplete(&is_complete, &completion_status));
  ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
  ASSERT_FALSE(is_complete);
  for (const auto& tablet_id : participant_ids_) {
    vector<TxnId> aborted_txns;
    ASSERT_OK(GetAbortedTxnsForParticipant(tablet_id, 3, &aborted_txns));
    ASSERT_EQ(0, aborted_txns.size());
  }

  FLAGS_txn_schedule_background_tasks = true;
  // Change our quiescing states so a new leader can be elected.
  *leader_ts_->server()->mutable_quiescing() = true;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto* mts = cluster_->mini_tablet_server(i);
    if (leader_ts_ != mts) {
      *mts->server()->mutable_quiescing() = false;
    }
  }
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, leader_ts_->server()->num_raft_leaders()->value());
  });
  // Upon becoming leader, we should have started our commit task and completed
  // the commit.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(committed_txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(is_complete);
  });
  // The aborted transaction should still be aborted, and we should be able to
  // validate its abort status in its participants' metadata.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(aborted_txn->IsCommitComplete(&is_complete, &completion_status));
    ASSERT_TRUE(completion_status.IsAborted()) << completion_status.ToString();
    ASSERT_TRUE(is_complete);
  });
  ASSERT_EVENTUALLY([&] {
    for (const auto& tablet_id : participant_ids_) {
      vector<TxnId> aborted_txns;
      ASSERT_OK(GetAbortedTxnsForParticipant(tablet_id, 3, &aborted_txns));
      ASSERT_EQ(1, aborted_txns.size());
    }
  });
}

} // namespace itest
} // namespace kudu
