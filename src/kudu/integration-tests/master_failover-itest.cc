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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_kudu_consensus_ConsensusService_GetNodeInstance);

namespace kudu {

// Note: this test needs to be in the client namespace in order for
// KuduClient::Data class methods to be visible via FRIEND_TEST macro.
namespace client {

const int kNumTabletServerReplicas = 3;

using sp::shared_ptr;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

class MasterFailoverTest : public KuduTest {
 public:
  enum CreateTableMode {
    kWaitForCreate = 0,
    kNoWaitForCreate = 1
  };

  MasterFailoverTest() {
    opts_.master_rpc_ports = { 11010, 11011, 11012 };
    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
    opts_.num_tablet_servers = kNumTabletServerReplicas;

    // Reduce various timeouts below as to make the detection of
    // leader master failures (specifically, failures as result of
    // long pauses) more rapid.

    // Set max missed heartbeats periods to 1.0 (down from 3.0).
    opts_.extra_master_flags.push_back("--leader_failure_max_missed_heartbeat_periods=1.0");

    // Set the TS->master heartbeat timeout to 1 second (down from 15 seconds).
    opts_.extra_tserver_flags.push_back("--heartbeat_rpc_timeout_ms=1000");
    // Allow one TS heartbeat failure before retrying with back-off (down from 3).
    opts_.extra_tserver_flags.push_back("--heartbeat_max_failures_before_backoff=1");
    // Wait for 500 ms after 'max_consecutive_failed_heartbeats'
    // before trying again (down from 1 second).
    opts_.extra_tserver_flags.push_back("--heartbeat_interval_ms=500");
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_NO_FATAL_FAILURE(RestartCluster());
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  void RestartCluster() {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    cluster_.reset(new ExternalMiniCluster(opts_));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder builder;

    // Create and alter table operation timeouts can be extended via their
    // builders, but there's no such option for DeleteTable, so we extend
    // the global operation timeout.
    builder.default_admin_operation_timeout(MonoDelta::FromSeconds(90));
    ASSERT_OK(cluster_->CreateClient(&builder, &client_));
  }

  Status CreateTable(const std::string& table_name, CreateTableMode mode) {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->NotNull();
    CHECK_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    return table_creator->table_name(table_name)
        .schema(&schema)
        .set_range_partition_columns({ "key" })
        .wait(mode == kWaitForCreate)
        .Create();
  }

  Status RenameTable(const std::string& table_name_orig, const std::string& table_name_new) {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_name_orig));
    return table_alterer
      ->RenameTo(table_name_new)
      ->wait(true)
      ->Alter();
  }

 protected:
  int num_masters_;
  ExternalMiniClusterOptions opts_;
  unique_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// Test that synchronous CreateTable (issue CreateTable call and then
// wait until the table has been created) works even when the original
// leader master has been paused.
TEST_F(MasterFailoverTest, TestCreateTableSync) {
  const char* kTableName = "testCreateTableSync";

  if (!AllowSlowTests()) {
    LOG(INFO) << "This test can only be run in slow mode.";
    return;
  }

  LOG(INFO) << "Pausing leader master";
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  cluster_->master(leader_idx)->Pause();
  ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

  // As Pause() is asynchronous, the following sequence of events is possible:
  //
  // 1. Pause() is issued.
  // 2. CreateTable() is issued.
  // 3. Leader master receives CreateTable() and creates the table.
  // 4. Leader master is paused before the CreateTable() response is sent.
  // 5. Client times out, finds the new master, and retries CreateTable().
  // 6. The retry fails because the table was already created in step 3.
  Status s = CreateTable(kTableName, kWaitForCreate);
  ASSERT_TRUE(s.ok() || s.IsAlreadyPresent());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_EQ(0, CountTableRows(table.get()));
}

// Test that we can issue a CreateTable call, pause the leader master
// immediately after, then verify that the table has been created on
// the newly elected leader master.
TEST_F(MasterFailoverTest, TestPauseAfterCreateTableIssued) {
  const char* kTableName = "testPauseAfterCreateTableIssued";

  if (!AllowSlowTests()) {
    LOG(INFO) << "This test can only be run in slow mode.";
    return;
  }

  ASSERT_OK(CreateTable(kTableName, kNoWaitForCreate));

  LOG(INFO) << "Pausing leader master";
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  cluster_->master(leader_idx)->Pause();
  ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(90);
  ASSERT_OK(client_->data_->WaitForCreateTableToFinish(client_.get(),
                                                       kTableName, deadline));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_EQ(0, CountTableRows(table.get()));
}

// Test the scenario where we create a table, pause the leader master,
// and then issue the DeleteTable call: DeleteTable should go to the newly
// elected leader master and succeed.
TEST_F(MasterFailoverTest, TestDeleteTableSync) {
  const char* kTableName = "testDeleteTableSync";
  if (!AllowSlowTests()) {
    LOG(INFO) << "This test can only be run in slow mode.";
    return;
  }

  ASSERT_OK(CreateTable(kTableName, kWaitForCreate));

  LOG(INFO) << "Pausing leader master";
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  cluster_->master(leader_idx)->Pause();
  ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

  // It's possible for DeleteTable() to delete the table and still return
  // NotFound. See TestCreateTableSync for details.
  Status s = client_->DeleteTable(kTableName);
  ASSERT_TRUE(s.ok() || s.IsNotFound());

  shared_ptr<KuduTable> table;
  s = client_->OpenTable(kTableName, &table);
  ASSERT_TRUE(s.IsNotFound());
}

// Test the scenario where we create a table, pause the leader master,
// and then issue the AlterTable call renaming a table: AlterTable
// should go to the newly elected leader master and succeed, renaming
// the table.
//
// TODO: Add an equivalent async test. Add a test for adding and/or
// renaming a column in a table.
TEST_F(MasterFailoverTest, TestRenameTableSync) {
  const char* kTableNameOrig = "testAlterTableSync";
  const char* kTableNameNew = "testAlterTableSyncRenamed";

  if (!AllowSlowTests()) {
    LOG(INFO) << "This test can only be run in slow mode.";
    return;
  }

  ASSERT_OK(CreateTable(kTableNameOrig, kWaitForCreate));

  LOG(INFO) << "Pausing leader master";
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  cluster_->master(leader_idx)->Pause();
  ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

  // It's possible for AlterTable() to rename the table and still return
  // NotFound. See TestCreateTableSync for details.
  Status s = RenameTable(kTableNameOrig, kTableNameNew);
  ASSERT_TRUE(s.ok() || s.IsNotFound());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableNameNew, &table));
  s = client_->OpenTable(kTableNameOrig, &table);
  ASSERT_TRUE(s.IsNotFound());
}


TEST_F(MasterFailoverTest, TestKUDU1374) {
  const char* kTableName = "testKUDU1374";

  // Wait at least one additional heartbeat interval after creating the table.
  // The idea is to guarantee that all tservers sent a tablet report with the
  // new (dirty) tablet, and should now be sending empty incremental tablet
  // reports.
  ASSERT_OK(CreateTable(kTableName, kWaitForCreate));
  SleepFor(MonoDelta::FromMilliseconds(1000));

  // Force all asynchronous RPCs sent by the leader master to fail. This
  // means the subsequent AlterTable() will be hidden from the tservers,
  // at least while this master is leader.
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  ExternalDaemon* leader_master = cluster_->master(leader_idx);
  ASSERT_OK(cluster_->SetFlag(leader_master,
                              "catalog_manager_fail_ts_rpcs", "true"));

  unique_ptr<KuduTableAlterer> alter(client_->NewTableAlterer(kTableName));
  alter->AddColumn("new_col")->Type(KuduColumnSchema::INT32);
  ASSERT_OK(alter
    ->wait(false)
    ->Alter());

  leader_master->Shutdown();

  // Wait for the AlterTable() to finish. Progress is as follows:
  // 1. TS notices the change in leadership and sends a full tablet report.
  // 2. Leader master notices that the reported tablet isn't fully altered
  //    and sends the TS an AlterSchema() RPC.
  // 3. TS updates the tablet's schema. This also dirties the tablet.
  // 4. TS send an incremental tablet report containing the dirty tablet.
  // 5. Leader master sees that all tablets in the table now have the new
  //    schema and ends the AlterTable() operation.
  // 6. The next IsAlterTableInProgress() call returns false.
  MonoTime now = MonoTime::Now();
  MonoTime deadline = now + MonoDelta::FromSeconds(90);
  while (now < deadline) {
    bool in_progress;
    ASSERT_OK(client_->IsAlterTableInProgress(kTableName, &in_progress));
    if (!in_progress) {
      break;
    }

    SleepFor(MonoDelta::FromMilliseconds(100));
    now = MonoTime::Now();
  }
  ASSERT_TRUE(now < deadline)
      << "Deadline elapsed before alter table completed";
}

TEST_F(MasterFailoverTest, TestMasterUUIDResolution) {
  // After a fresh start, the masters should have received RPCs asking for
  // their UUIDs.
  for (int i = 0; i < cluster_->num_masters(); i++) {
    int64_t num_get_node_instances;
    ASSERT_OK(cluster_->master(i)->GetInt64Metric(
        &METRIC_ENTITY_server, "kudu.master",
        &METRIC_handler_latency_kudu_consensus_ConsensusService_GetNodeInstance,
        "total_count", &num_get_node_instances));

    // Client-side timeouts may increase the number of calls beyond the raw
    // number of masters.
    ASSERT_GE(num_get_node_instances, cluster_->num_masters());
  }

  // Restart the masters. They should reuse one another's UUIDs from the cached
  // consensus metadata instead of sending RPCs to discover them. See KUDU-526.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());

  // To determine whether the cached UUIDs were used, let's look at the number
  // of GetNodeInstance() calls each master serviced. It should be zero.
  for (int i = 0; i < cluster_->num_masters(); i++) {
    ExternalMaster* master = cluster_->master(i);
    int64_t num_get_node_instances;
    ASSERT_OK(master->GetInt64Metric(
        &METRIC_ENTITY_server, "kudu.master",
        &METRIC_handler_latency_kudu_consensus_ConsensusService_GetNodeInstance,
        "total_count", &num_get_node_instances));
    EXPECT_EQ(0, num_get_node_instances) <<
        Substitute(
            "Following restart, master $0 has serviced $1 GetNodeInstance() calls",
            master->bound_rpc_hostport().ToString(),
            num_get_node_instances);
  }
}

TEST_F(MasterFailoverTest, TestMasterPermanentFailure) {
  const string kBinPath = cluster_->GetBinaryPath("kudu");
  Random r(SeedRandom());

  // Repeat the test for each master.
  for (int i = 0; i < cluster_->num_masters(); i++) {
    ExternalMaster* failed_master = cluster_->master(i);

    // "Fail" a master and blow away its state completely.
    failed_master->Shutdown();
    string data_root = failed_master->data_dir();
    env_->DeleteRecursively(data_root);

    // Pick another master at random to serve as a basis for recovery.
    //
    // This isn't completely safe; see KUDU-1556 for details.
    ExternalMaster* other_master;
    do {
      other_master = cluster_->master(r.Uniform(cluster_->num_masters()));
    } while (other_master->uuid() == failed_master->uuid());

    // Find the UUID of the failed master using the other master's cmeta file.
    string uuid;
    {
      vector<string> args = {
          kBinPath,
          "local_replica",
          "cmeta",
          "print_replica_uuids",
          "--fs_wal_dir=" + other_master->data_dir(),
          "--fs_data_dirs=" + other_master->data_dir(),
          master::SysCatalogTable::kSysCatalogTabletId
      };
      string output;
      ASSERT_OK(Subprocess::Call(args, &output));
      StripWhiteSpace(&output);
      LOG(INFO) << "UUIDS: " << output;
      set<string> uuids = Split(output, " ");

      // Isolate the failed master's UUID by eliminating the UUIDs of the
      // healthy masters from the set.
      for (int j = 0; j < cluster_->num_masters(); j++) {
        if (j == i) continue;
        uuids.erase(cluster_->master(j)->uuid());
      }
      ASSERT_EQ(1, uuids.size());
      uuid = *uuids.begin();
    }

    // Format a new filesystem with the same UUID as the failed master.
    {
      vector<string> args = {
          kBinPath,
          "fs",
          "format",
          "--fs_wal_dir=" + data_root,
          "--fs_data_dirs=" + data_root,
          "--uuid=" + uuid
      };
      ASSERT_OK(Subprocess::Call(args));
    }

    // Copy the master tablet from the other master.
    {
      vector<string> args = {
          kBinPath,
          "local_replica",
          "copy_from_remote",
          "--fs_wal_dir=" + data_root,
          "--fs_data_dirs=" + data_root,
          master::SysCatalogTable::kSysCatalogTabletId,
          other_master->bound_rpc_hostport().ToString()
      };
      ASSERT_OK(Subprocess::Call(args));
    }

    // Bring up the new master.
    //
    // Technically this reuses the failed master's data directory, but that
    // directory was emptied when we "failed" the master, so this still
    // qualifies as a "new" master for all intents and purposes.
    ASSERT_OK(failed_master->Start());

    // Do some operations.

    string table_name = Substitute("table-$0", i);
    ASSERT_OK(CreateTable(table_name, kWaitForCreate));

    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));
    ASSERT_EQ(0, CountTableRows(table.get()));

    // Repeat these operations with each of the masters paused.
    //
    // Only in slow mode.
    if (AllowSlowTests()) {
      for (int j = 0; j < cluster_->num_masters(); j++) {
        cluster_->master(j)->Pause();
        ScopedResumeExternalDaemon resume_daemon(cluster_->master(j));
        string table_name = Substitute("table-$0-$1", i, j);

        // See TestCreateTableSync to understand why we must check for
        // IsAlreadyPresent as well.
        Status s = CreateTable(table_name, kWaitForCreate);
        ASSERT_TRUE(s.ok() || s.IsAlreadyPresent());

        ASSERT_OK(client_->OpenTable(table_name, &table));
        ASSERT_EQ(0, CountTableRows(table.get()));
      }
    }
  }
}

} // namespace client
} // namespace kudu
