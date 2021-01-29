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
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/none_t.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/common/common.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(raft_enable_pre_election);
DECLARE_bool(txn_status_tablet_failover_inject_timeout_error);
DECLARE_bool(txn_status_tablet_inject_load_failure_error);
DECLARE_bool(txn_status_tablet_inject_uninitialized_leader_status_error);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_string(superuser_acl);
DECLARE_string(user_acl);
DECLARE_uint32(txn_keepalive_interval_ms);

using kudu::client::AuthenticationCredentialsPB;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::itest::TServerDetails;
using kudu::itest::TabletServerMap;
using kudu::tablet::TabletReplica;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnStatusEntryPB;
using kudu::transactions::TxnStatusTablet;
using kudu::transactions::TxnSystemClient;
using std::map;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

namespace {
string ParticipantId(int i) {
  return Substitute("strawhat-$0", i);
}
} // anonymous namespace

class TxnStatusTableITest : public KuduTest {
 public:
  TxnStatusTableITest() {}

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, {}));
    ASSERT_OK(cluster_->Start());

    // Create the txn system client with which to communicate with the cluster.
    ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_sys_client_));
  }

  // Ensures that all replicas have the right table type set.
  void CheckTableTypes(const map<TableTypePB, int>& expected_counts) {
    // Check that the tablets of the table all have transaction coordinators.
    vector<scoped_refptr<TabletReplica>> replicas;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      const auto& tablets = ts->ListTablets();
      for (const auto& t : tablets) {
        scoped_refptr<TabletReplica> r;
        ASSERT_TRUE(ts->server()->tablet_manager()->LookupTablet(t, &r));
        replicas.emplace_back(std::move(r));
      }
    }
    map<TableTypePB, int> type_counts;
    for (const auto& r : replicas) {
      const auto optional_table_type = r->tablet_metadata()->table_type();
      if (boost::none == optional_table_type) {
        LookupOrEmplace(&type_counts, TableTypePB::DEFAULT_TABLE, 0)++;
      } else {
        ASSERT_EQ(TableTypePB::TXN_STATUS_TABLE, *optional_table_type);
        ASSERT_NE(nullptr, r->txn_coordinator());
        LookupOrEmplace(&type_counts, *optional_table_type, 0)++;
      }
    }
    ASSERT_EQ(expected_counts, type_counts);
  }

  // Creates and returns a client as the given user.
  Status CreateClientAs(const string& user, shared_ptr<KuduClient>* client) {
    KuduClientBuilder client_builder;
    string authn_creds;
    AuthenticationCredentialsPB pb;
    pb.set_real_user(user);
    CHECK(pb.SerializeToString(&authn_creds));
    client_builder.import_authentication_credentials(authn_creds);
    shared_ptr<KuduClient> c;
    RETURN_NOT_OK(cluster_->CreateClient(&client_builder, &c));
    *client = std::move(c);
    return Status::OK();
  }

  // Sets the superuser and user ACLs, restarts the master, and waits for some
  // tablet servers to register, yielding a fatal if anything fails.
  void SetSuperuserAndUser(const string& superuser, const string& user) {
    FLAGS_superuser_acl = superuser;
    FLAGS_user_acl = user;
    cluster_->mini_master()->Shutdown();
    ASSERT_OK(cluster_->mini_master()->Restart());
    ASSERT_EVENTUALLY([&] {
      ASSERT_LT(0, cluster_->mini_master()->master()->ts_manager()->GetLiveCount());
    });
  }

  shared_ptr<KuduClient> client_sp() {
    return txn_sys_client_->client_;
  }

 protected:
  static constexpr const char* kUser = "user";
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<TxnSystemClient> txn_sys_client_;
};

// Test that the transaction status table is not listed. We expect it to be
// fairly common for a super-user to list tables to backup a cluster, so we
// hide the transaction status table, which we don't expect to back up.
TEST_F(TxnStatusTableITest, TestTxnStatusTableNotListed) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  auto client = client_sp();
  vector<string> tables_list;
  ASSERT_OK(client->ListTables(&tables_list));
  ASSERT_TRUE(tables_list.empty());

  // While we prevent the table from being listed, clients should still be able
  // to view it if it's explicitly asked for.
  const string& kTableName = TxnStatusTablet::kTxnStatusTableName;
  bool exists = false;
  ASSERT_OK(client->TableExists(kTableName, &exists));
  ASSERT_TRUE(exists);

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_NE(nullptr, table);
}

// Test that only the service- or super-user can create or alter the
// transaction status table.
TEST_F(TxnStatusTableITest, TestProtectCreateAndAlter) {
  auto service_client = client_sp();
  // We're both a super user and a service user since the ACLs default to "*".
  // We should thus have access to the table.
  ASSERT_OK(TxnSystemClient::CreateTxnStatusTableWithClient(100, 1, service_client.get()));
  ASSERT_OK(TxnSystemClient::AddTxnStatusTableRangeWithClient(100, 200, service_client.get()));
  const string& kTableName = TxnStatusTablet::kTxnStatusTableName;
  ASSERT_OK(service_client->DeleteTable(kTableName));

  // The service user should be able to create and alter the transaction status
  // table.
  NO_FATALS(SetSuperuserAndUser("nobody", "nobody"));
  ASSERT_OK(TxnSystemClient::CreateTxnStatusTableWithClient(100, 1, service_client.get()));
  ASSERT_OK(TxnSystemClient::AddTxnStatusTableRangeWithClient(100, 200, service_client.get()));

  // The service user doesn't have access to the delete table endpoint.
  Status s = service_client->DeleteTable(kTableName);
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");

  // The super user should be able to create, alter, and delete the transaction
  // status table.
  NO_FATALS(SetSuperuserAndUser("bob", "nobody"));
  shared_ptr<KuduClient> bob_client;
  ASSERT_OK(CreateClientAs("bob", &bob_client));
  ASSERT_OK(bob_client->DeleteTable(kTableName));
  ASSERT_OK(TxnSystemClient::CreateTxnStatusTableWithClient(100, 1, bob_client.get()));
  ASSERT_OK(TxnSystemClient::AddTxnStatusTableRangeWithClient(100, 200, bob_client.get()));
  ASSERT_OK(bob_client->DeleteTable(kTableName));

  // Regular users shouldn't be able to do anything.
  NO_FATALS(SetSuperuserAndUser("nobody", "bob"));
  s = TxnSystemClient::CreateTxnStatusTableWithClient(100, 1, bob_client.get());
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_OK(TxnSystemClient::CreateTxnStatusTableWithClient(100, 1, service_client.get()));
  s = TxnSystemClient::AddTxnStatusTableRangeWithClient(100, 200, bob_client.get());
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  s = service_client->DeleteTable(kTableName);
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
}

// Test that accessing the transaction system table only succeeds for service
// or super users.
TEST_F(TxnStatusTableITest, TestProtectAccess) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  auto client_sp = this->client_sp();
  const string& kTableName = TxnStatusTablet::kTxnStatusTableName;
#define STORE_AND_PREPEND(s, msg) do { \
    Status _s = (s); \
    results.emplace_back(_s.CloneAndPrepend(msg)); \
  } while (0)
  const auto attempt_accesses_and_delete = [&] (KuduClient* client) {
    // Go through various table access methods on the transaction status table.
    // Collect the results so we can verify whether we were authorized
    // properly.
    vector<Status> results;
    bool unused;
    STORE_AND_PREPEND(client->TableExists(kTableName, &unused), "failed to check existence");
    shared_ptr<KuduTable> unused_table;
    STORE_AND_PREPEND(client->OpenTable(kTableName, &unused_table), "failed to open table");
    client::KuduTableStatistics* unused_stats = nullptr;
    STORE_AND_PREPEND(client->GetTableStatistics(kTableName, &unused_stats), "failed to get stats");
    unique_ptr<client::KuduTableStatistics> unused_unique_stats(unused_stats);
    STORE_AND_PREPEND(client->DeleteTable(kTableName), "failed to delete table");
    return results;
  };
  // We're both a super user and a service user since the ACLs default to "*".
  // We should thus have access to the table.
  auto results = attempt_accesses_and_delete(client_sp.get());
  for (const auto& r : results) {
    ASSERT_OK(r);
  }
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));

  // Even though we're the service user, we shouldn't be able to access
  // anything because most RPCs require us to be a super-user or user.
  NO_FATALS(SetSuperuserAndUser("nobody", "nobody"));
  results = attempt_accesses_and_delete(client_sp.get());
  for (const auto& r : results) {
    // NOTE: we don't check the type because authorization errors may surface
    // as RemoteErrors or NotAuthorized errors.
    ASSERT_FALSE(r.ok());
    ASSERT_STR_CONTAINS(r.ToString(), "Not authorized");
  }
  // As a sanity check, our last delete have failed, and we should be unable to
  // create another transaction status table.
  Status s = txn_sys_client_->CreateTxnStatusTable(100);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();

  // If we're a super user but not a service user, we should have access to the
  // table.
  NO_FATALS(SetSuperuserAndUser("bob", "nobody"));
  // Create a client as 'bob', who is not the service user.
  shared_ptr<KuduClient> bob_client;
  ASSERT_OK(CreateClientAs("bob", &bob_client));
  results = attempt_accesses_and_delete(bob_client.get());
  for (const auto& r : results) {
    ASSERT_OK(r);
  }
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));

  // If we're a regular user and nothing more, we shouldn't be able to access
  // the table.
  NO_FATALS(SetSuperuserAndUser("nobody", "bob"));
  results = attempt_accesses_and_delete(bob_client.get());
  for (const auto& r : results) {
    ASSERT_FALSE(r.ok());
    ASSERT_STR_CONTAINS(r.ToString(), "Not authorized");
  }
  s = txn_sys_client_->CreateTxnStatusTable(100);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
}

// Test that DDL for the transaction status table results in the creation of
// transaction status tablets.
TEST_F(TxnStatusTableITest, TestCreateTxnStatusTablePartitions) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  Status s = txn_sys_client_->CreateTxnStatusTable(100);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckTableTypes({ { TableTypePB::TXN_STATUS_TABLE, 1 } }));

  // Now add more partitions and try again.
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(100, 200));
  s = txn_sys_client_->AddTxnStatusTableRange(100, 200);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckTableTypes({ { TableTypePB::TXN_STATUS_TABLE, 2 } }));

  // Ensure we still create transaction status tablets even after the master is
  // restarted.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(200, 300));
  NO_FATALS(CheckTableTypes({ { TableTypePB::TXN_STATUS_TABLE, 3 } }));
}

// Test that tablet servers can host both transaction status tablets and
// regular tablets.
TEST_F(TxnStatusTableITest, TestTxnStatusTableColocatedWithTables) {
  TestWorkload w(cluster_.get());
  w.set_num_replicas(1);
  w.Setup();
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  NO_FATALS(CheckTableTypes({
      { TableTypePB::TXN_STATUS_TABLE, 1 },
      { TableTypePB::DEFAULT_TABLE, 1 }
  }));
}

TEST_F(TxnStatusTableITest, TestSystemClientFindTablets) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  uint32_t txn_keepalive_ms;
  ASSERT_OK(txn_sys_client_->BeginTransaction(1, kUser, &txn_keepalive_ms));
  ASSERT_EQ(FLAGS_txn_keepalive_interval_ms, txn_keepalive_ms);
  ASSERT_OK(txn_sys_client_->AbortTransaction(1, kUser));

  // If we write out of range, we should see an error.
  {
    int64_t highest_seen_txn_id = -1;
    uint32_t txn_keepalive_ms = 0;
    auto s = txn_sys_client_->BeginTransaction(
        100, kUser, &txn_keepalive_ms, &highest_seen_txn_id);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    // The 'highest_seen_txn_id' should be left untouched.
    ASSERT_EQ(-1, highest_seen_txn_id);
    // txn_keepalive_ms isn't assigned in case of non-OK status.
    ASSERT_EQ(0, txn_keepalive_ms);
    ASSERT_NE(0, FLAGS_txn_keepalive_interval_ms);
  }
  {
    auto s = txn_sys_client_->BeginCommitTransaction(100, kUser);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
  {
    auto s = txn_sys_client_->AbortTransaction(100, kUser);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  // Once we add a new range, we should be able to leverage it.
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(100, 200));
  int64_t highest_seen_txn_id = -1;
  ASSERT_OK(txn_sys_client_->BeginTransaction(
      100, kUser, nullptr /* txn_keepalive_ms */, &highest_seen_txn_id));
  ASSERT_EQ(100, highest_seen_txn_id);
  ASSERT_OK(txn_sys_client_->BeginCommitTransaction(100, kUser));
  ASSERT_OK(txn_sys_client_->AbortTransaction(100, kUser));
}

TEST_F(TxnStatusTableITest, TestSystemClientTServerDown) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());

  cluster_->mini_tablet_server(0)->Shutdown();

  // When the only server is down, the system client should keep trying until
  // it times out.
  {
    int64_t highest_seen_txn_id = -1;
    auto s = txn_sys_client_->BeginTransaction(
        1, kUser, nullptr /* txn_keepalive_ms */, &highest_seen_txn_id,
        MonoDelta::FromMilliseconds(100));
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    // The 'highest_seen_txn_id' should be left untouched.
    ASSERT_EQ(-1, highest_seen_txn_id);
  }

  // Now try with a longer timeout and ensure that if the server comes back up,
  // the system client will succeed.
  thread t([&] {
    // Wait a bit to give some time for the system client to make its request
    // and retry some.
    SleepFor(MonoDelta::FromMilliseconds(500));
    ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
  });
  SCOPED_CLEANUP({
    t.join();
  });

  int64_t highest_seen_txn_id = -1;
  ASSERT_OK(txn_sys_client_->BeginTransaction(
      1, kUser, nullptr /* txn_keepalive_ms */, &highest_seen_txn_id,
      MonoDelta::FromSeconds(3)));
  ASSERT_EQ(highest_seen_txn_id, 1);
}

TEST_F(TxnStatusTableITest, TestSystemClientBeginTransactionErrors) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  int64_t highest_seen_txn_id = -1;
  uint32_t txn_keepalive_ms;
  ASSERT_OK(txn_sys_client_->BeginTransaction(
      1, kUser, &txn_keepalive_ms, &highest_seen_txn_id));
  ASSERT_EQ(1, highest_seen_txn_id);
  ASSERT_EQ(FLAGS_txn_keepalive_interval_ms, txn_keepalive_ms);

  // Trying to start another transaction with a used ID should yield an error.
  {
    int64_t highest_seen_txn_id = -1;
    auto s = txn_sys_client_->BeginTransaction(
        1, kUser, nullptr /* txn_keepalive_ms */, &highest_seen_txn_id);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ(1, highest_seen_txn_id);
    ASSERT_STR_CONTAINS(s.ToString(), "not higher than the highest ID");
  }

  // The same should be true with a different user.
  {
    int64_t highest_seen_txn_id = -1;
    auto s = txn_sys_client_->BeginTransaction(
        1, "stranger", nullptr /* txn_keepalive_ms */, &highest_seen_txn_id);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ(1, highest_seen_txn_id);
    ASSERT_STR_CONTAINS(s.ToString(), "not higher than the highest ID");
  }
}

TEST_F(TxnStatusTableITest, TestSystemClientRegisterParticipantErrors) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  Status s = txn_sys_client_->RegisterParticipant(1, "participant", kUser);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "transaction ID.*not found, current highest txn ID:.*");

  ASSERT_OK(txn_sys_client_->BeginTransaction(1, kUser));
  ASSERT_OK(txn_sys_client_->RegisterParticipant(1, ParticipantId(1), kUser));

  // If a user other than the transaction owner is passed as an argument, the
  // request should be rejected.
  s = txn_sys_client_->RegisterParticipant(1, ParticipantId(2), "stranger");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

TEST_F(TxnStatusTableITest, SystemClientCommitAndAbortTransaction) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());

  ASSERT_OK(txn_sys_client_->BeginTransaction(1, kUser));
  ASSERT_OK(txn_sys_client_->BeginCommitTransaction(1, kUser));

  // Calling BeginCommitTransaction() on already committing transaction is OK.
  ASSERT_OK(txn_sys_client_->BeginCommitTransaction(1, kUser));
  // It's completely legal to abort a transaction that has entered the commit
  // phase but hasn't finalized yet.
  ASSERT_OK(txn_sys_client_->AbortTransaction(1, kUser));
  // Aborting already aborted transaction is fine (aborting is idempotent).
  ASSERT_OK(txn_sys_client_->AbortTransaction(1, kUser));

  // Even if the transaction is aborted, an attempt to start another transaction
  // with already used ID should yield an error.
  {
    int64_t highest_seen_txn_id = -1;
    auto s = txn_sys_client_->BeginTransaction(
        1, kUser, nullptr /* txn_keepalive_ms */, &highest_seen_txn_id);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ(1, highest_seen_txn_id);
    ASSERT_STR_CONTAINS(s.ToString(), "not higher than the highest ID");
  }

  // An attempt to start committing a non-existent transaction should report
  // an error.
  {
    auto s = txn_sys_client_->BeginCommitTransaction(2, kUser);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 2 not found");
  }

  // An attempt to abort a non-existent transaction should report an error.
  {
    auto s = txn_sys_client_->AbortTransaction(2, kUser);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 2 not found");
  }

  // An attempt to commit already aborted transaction should fail with an error.
  {
    ASSERT_OK(txn_sys_client_->BeginTransaction(2, kUser));
    ASSERT_OK(txn_sys_client_->AbortTransaction(2, kUser));
    auto s = txn_sys_client_->BeginCommitTransaction(2, kUser);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 2 is not open");
  }

  // Commit/Abort transaction should be called as of the transaction's owner.
  {
    ASSERT_OK(txn_sys_client_->BeginTransaction(3, kUser));
    auto s = txn_sys_client_->BeginCommitTransaction(3, "stranger");
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 3 not owned by stranger");
    s = txn_sys_client_->AbortTransaction(3, "stranger");
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "transaction ID 3 not owned by stranger");
  }
}

TEST_F(TxnStatusTableITest, TServerInitializesTxnSystemClient) {
  auto* mts = cluster_->mini_tablet_server(0);
  auto* client_initializer = mts->server()->txn_client_initializer();
  TxnSystemClient* txn_client = nullptr;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(client_initializer->GetClient(&txn_client));
  });
  // We should be able to get the client, and use it to make a call.
  ASSERT_NE(txn_client, nullptr);
  ASSERT_OK(txn_client->CreateTxnStatusTable(100));
  txn_client = nullptr;

  // Try shutting down the master, and then restarting the tablet server to
  // attempt to rebuild a TxnSystemClient. This should fail until the master
  // comes back up.
  cluster_->mini_master()->Shutdown();
  mts->Shutdown();
  ASSERT_OK(mts->Restart());
  client_initializer = mts->server()->txn_client_initializer();
  Status s = client_initializer->GetClient(&txn_client);
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  ASSERT_EQ(txn_client, nullptr);

  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(client_initializer->GetClient(&txn_client));
  });
  // We should be able to get the client, and use it to make a call.
  ASSERT_NE(txn_client, nullptr);
  ASSERT_OK(txn_client->OpenTxnStatusTable());
}

TEST_F(TxnStatusTableITest, GetTransactionStatus) {
  const auto verify_state = [&](TxnStatePB state) {
    TxnStatusEntryPB txn_status;
    ASSERT_OK(txn_sys_client_->GetTransactionStatus(1, kUser, &txn_status));
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_STREQ(kUser, txn_status.user().c_str());
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(state, txn_status.state());
  };

  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());

  ASSERT_OK(txn_sys_client_->BeginTransaction(1, kUser));
  NO_FATALS(verify_state(TxnStatePB::OPEN));

  ASSERT_OK(txn_sys_client_->BeginCommitTransaction(1, kUser));
  NO_FATALS(verify_state(TxnStatePB::COMMIT_IN_PROGRESS));

  ASSERT_OK(txn_sys_client_->AbortTransaction(1, kUser));
  NO_FATALS(verify_state(TxnStatePB::ABORTED));

  {
    auto s = txn_sys_client_->BeginCommitTransaction(1, kUser);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    NO_FATALS(verify_state(TxnStatePB::ABORTED));
  }

  // In the negative scenarios below, check for the expected status code
  // and make sure nothing is set in the output argument.
  {
    TxnStatusEntryPB empty;
    auto s = txn_sys_client_->GetTransactionStatus(1, "interloper", &empty);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_FALSE(empty.has_user());
    ASSERT_FALSE(empty.has_state());
  }

  {
    TxnStatusEntryPB empty;
    auto s = txn_sys_client_->GetTransactionStatus(2, kUser, &empty);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_FALSE(empty.has_user());
    ASSERT_FALSE(empty.has_state());
  }

  {
    TxnStatusEntryPB empty;
    auto s = txn_sys_client_->GetTransactionStatus(2, "", &empty);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_FALSE(empty.has_user());
    ASSERT_FALSE(empty.has_state());
  }
}

// Test that a transaction system client can make concurrent calls to multiple
// transaction status tablets.
TEST_F(TxnStatusTableITest, TestSystemClientConcurrentCalls) {
  int kPartitionWidth = 10;
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(kPartitionWidth));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(kPartitionWidth, 2 * kPartitionWidth));
  vector<thread> threads;
  Status statuses[2 * kPartitionWidth];
  for (int t = 0; t < 2; t++) {
    threads.emplace_back([&, t] {
      // NOTE: we can "race" transaction IDs so they're not monotonically
      // increasing, as long as within each range partition they are
      // monotonically increasing.
      for (int i = 0; i < kPartitionWidth; i++) {
        int64_t txn_id = t * kPartitionWidth + i;
        Status s = txn_sys_client_->BeginTransaction(txn_id, kUser).AndThen([&] {
          return txn_sys_client_->RegisterParticipant(txn_id, ParticipantId(1), kUser);
        });
        if (PREDICT_FALSE(!s.ok())) {
          statuses[txn_id] = s;
        }
      }
    });
  }
  std::for_each(threads.begin(), threads.end(), [&] (thread& t) { t.join(); });
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
}

class MultiServerTxnStatusTableITest : public TxnStatusTableITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = 4;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_sys_client_));

    // Create the initial transaction status table partitions and start an
    // initial transaction.
    ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100, 3));
    ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
    ASSERT_OK(txn_sys_client_->BeginTransaction(1, kUser));
  }

  // Returns the tablet ID found in the cluster, expecting a single tablet.
  string GetTabletId() const {
    string tablet_id;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      const auto& tablets = cluster_->mini_tablet_server(i)->ListTablets();
      if (!tablets.empty()) {
        if (!tablet_id.empty()) {
          DCHECK_EQ(tablet_id, tablets[0]);
          continue;
        }
        tablet_id = tablets[0];
      }
    }
    DCHECK(!tablet_id.empty());
    return tablet_id;
  }

  // Returns the UUID of the given tablet's leader replica.
  Status FindLeaderId(const string& tablet_id, string* uuid) {
    TabletServerMap ts_map;
    RETURN_NOT_OK(CreateTabletServerMap(cluster_->master_proxy(), cluster_->messenger(), &ts_map));
    ValueDeleter deleter(&ts_map);
    TServerDetails* leader_ts;
    RETURN_NOT_OK(FindTabletLeader(ts_map, tablet_id, MonoDelta::FromSeconds(10), &leader_ts));
    *uuid = leader_ts->uuid();
    return Status::OK();
  }
};

TEST_F(MultiServerTxnStatusTableITest, TestSystemClientDeletedTablet) {
  // Find the leader and force it to step down. The system client should be
  // able to find the new leader.
  const string& tablet_id = GetTabletId();
  ASSERT_FALSE(tablet_id.empty());
  string orig_leader_uuid;
  ASSERT_OK(FindLeaderId(tablet_id, &orig_leader_uuid));
  ASSERT_FALSE(orig_leader_uuid.empty());
  ASSERT_OK(cluster_->mini_tablet_server_by_uuid(
      orig_leader_uuid)->server()->tablet_manager()->DeleteTablet(
          tablet_id, tablet::TABLET_DATA_TOMBSTONED, /*cas_config_index*/boost::none));

  // The client should automatically try to get a new location for the tablet.
  ASSERT_OK(txn_sys_client_->BeginTransaction(2, kUser));
}

TEST_F(MultiServerTxnStatusTableITest, TestSystemClientLeadershipChange) {
  // Find the leader and force it to step down. The system client should be
  // able to find the new leader.
  const string& tablet_id = GetTabletId();
  ASSERT_FALSE(tablet_id.empty());
  string orig_leader_uuid;
  ASSERT_OK(FindLeaderId(tablet_id, &orig_leader_uuid));
  ASSERT_FALSE(orig_leader_uuid.empty());
  cluster_->mini_tablet_server_by_uuid(
      orig_leader_uuid)->server()->mutable_quiescing()->store(true);
  ASSERT_EVENTUALLY([&] {
    string new_leader_uuid;
    ASSERT_OK(FindLeaderId(tablet_id, &new_leader_uuid));
    ASSERT_NE(new_leader_uuid, orig_leader_uuid);
    ASSERT_OK(txn_sys_client_->BeginTransaction(2, kUser));
  });
}

TEST_F(MultiServerTxnStatusTableITest, TestSystemClientCrashedNodes) {
  // Find the leader and shut it down. The system client should be able to
  // find the new leader.
  const auto& tablet_id = GetTabletId();
  ASSERT_FALSE(tablet_id.empty());
  string leader_uuid;
  ASSERT_OK(FindLeaderId(tablet_id, &leader_uuid));
  ASSERT_FALSE(leader_uuid.empty());
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  cluster_->mini_tablet_server_by_uuid(leader_uuid)->Shutdown();
  int txn_id = 2;
  ASSERT_OK(txn_sys_client_->BeginTransaction(++txn_id, kUser));
}

enum InjectedErrorType {
  kFailoverTimeoutError,
  kLeaderStatusError,
  kLoadFromTabletError
};
class TxnStatusTableRetryITest : public MultiServerTxnStatusTableITest,
                                 public ::testing::WithParamInterface<InjectedErrorType> {
};

TEST_P(TxnStatusTableRetryITest, TestRetryOnError) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  auto error_type = GetParam();
  switch (error_type) {
    case kFailoverTimeoutError:
      FLAGS_txn_status_tablet_failover_inject_timeout_error = true;
      break;
    case kLeaderStatusError:
      FLAGS_txn_status_tablet_inject_uninitialized_leader_status_error = true;
      break;
    case kLoadFromTabletError:
      FLAGS_txn_status_tablet_inject_load_failure_error = true;
      break;
  }
  // Find the leader and force it to step down. The system client should be
  // able to find the new leader.
  const string& tablet_id = GetTabletId();
  ASSERT_FALSE(tablet_id.empty());
  string orig_leader_uuid;
  ASSERT_OK(FindLeaderId(tablet_id, &orig_leader_uuid));
  ASSERT_FALSE(orig_leader_uuid.empty());
  cluster_->mini_tablet_server_by_uuid(
      orig_leader_uuid)->server()->mutable_quiescing()->store(true);

  string new_leader_uuid;
  TxnStatusEntryPB txn_status;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FindLeaderId(tablet_id, &new_leader_uuid));
    ASSERT_NE(new_leader_uuid, orig_leader_uuid);
    Status s = txn_sys_client_->GetTransactionStatus(1, kUser, &txn_status);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  });
  orig_leader_uuid = new_leader_uuid;
  // After disabling fault injection flag, calls should succeed given
  // the failed ones will be retried.
  switch (error_type) {
    case kFailoverTimeoutError:
      FLAGS_txn_status_tablet_failover_inject_timeout_error = false;
      break;
    case kLeaderStatusError:
      FLAGS_txn_status_tablet_inject_uninitialized_leader_status_error = false;
      break;
    case kLoadFromTabletError:
      FLAGS_txn_status_tablet_inject_load_failure_error = false;
      break;
  }
  cluster_->mini_tablet_server_by_uuid(
      orig_leader_uuid)->server()->mutable_quiescing()->store(true);
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FindLeaderId(tablet_id, &new_leader_uuid));
    ASSERT_NE(new_leader_uuid, orig_leader_uuid);
    ASSERT_OK(txn_sys_client_->GetTransactionStatus(1, kUser, &txn_status));
  });
}

// Test that calls to transaction status tablets will retry when:
//   1) timeout on waiting the replica to catch up with all replicated
//      operations in previous term.
//   2) leader status is not initialized yet.
INSTANTIATE_TEST_CASE_P(TestTxnStatusTableRetryOnError,
                        TxnStatusTableRetryITest,
                        ::testing::Values(kFailoverTimeoutError,
                                          kLeaderStatusError,
                                          kLoadFromTabletError));

class TxnStatusTableElectionStormITest : public TxnStatusTableITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    // Make leader elections more frequent to get through this test a bit more
    // quickly.
    FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
    FLAGS_raft_heartbeat_interval_ms = 30;
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_sys_client_));

    // Create the initial transaction status table partitions.
    ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100, 3));
    ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());

   // Inject latency so elections become more frequent and wait a bit for our
   // latency injection to kick in.
   FLAGS_raft_enable_pre_election = false;
   FLAGS_consensus_inject_latency_ms_in_notifications = 1.5 * FLAGS_raft_heartbeat_interval_ms;
   SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));
  }
};

TEST_F(TxnStatusTableElectionStormITest, TestFrequentElections) {
  // Ensure concurrent transaction read and write work as expected under frequent
  // leader elections.
  const int kNumTxnsPerThread = 5;
  const int kNumThreads = 5;
  vector<thread> threads;
  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kNumTxnsPerThread; i++) {
        int txn_id = t * kNumTxnsPerThread + i;
        TxnStatusEntryPB txn_status;
        Status s = txn_sys_client_->BeginTransaction(txn_id, kUser);
        // As we don't have guarantee of the threads' execution order, transactions to
        // be created later can have lower txn ID than previous created ones, which is
        // not allowed.
        if (s.ok()) {
          // Make sure a re-election happens before the following read.
          SleepFor(MonoDelta::FromMilliseconds(3 * FLAGS_raft_heartbeat_interval_ms));

          CHECK_OK(txn_sys_client_->GetTransactionStatus(txn_id, kUser, &txn_status));
          CHECK(txn_status.has_user());
          CHECK_STREQ(kUser, txn_status.user().c_str());
          CHECK(txn_status.has_state());
          CHECK_EQ(TxnStatePB::OPEN, txn_status.state());
        }
      }
    });
  }
  std::for_each(threads.begin(), threads.end(), [&] (thread& t) { t.join(); });
}

} // namespace itest
} // namespace kudu
