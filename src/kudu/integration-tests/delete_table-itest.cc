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

#include <memory>
#include <string>
#include <tuple>

#include <boost/optional.hpp>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/subprocess.h"

using kudu::client::KuduClient;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaFromSchema;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::consensus::CONSENSUS_CONFIG_COMMITTED;
using kudu::consensus::ConsensusMetadataPB;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::RaftPeerPB;
using kudu::itest::TServerDetails;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_READY;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletDataState;
using kudu::tablet::TabletSuperBlockPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::TabletServerErrorPB;
using std::numeric_limits;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_TSHeartbeat);
METRIC_DECLARE_histogram(handler_latency_kudu_tserver_TabletServerAdminService_DeleteTablet);

namespace kudu {

class DeleteTableITest : public ExternalMiniClusterITestBase {
 protected:
  enum IsCMetaExpected {
    CMETA_NOT_EXPECTED = 0,
    CMETA_EXPECTED = 1
  };

  enum IsSuperBlockExpected {
    SUPERBLOCK_NOT_EXPECTED = 0,
    SUPERBLOCK_EXPECTED = 1
  };

  enum ErrorDumpStackSelector {
    ON_ERROR_DO_NOT_DUMP_STACKS = 0,
    ON_ERROR_DUMP_STACKS = 1,
  };

  // Get the UUID of the leader of the specified tablet, as seen by the TS with
  // the given 'ts_uuid'.
  string GetLeaderUUID(const string& ts_uuid, const string& tablet_id);

  Status CheckTabletTombstonedOrDeletedOnTS(
      int index,
      const string& tablet_id,
      TabletDataState data_state,
      IsCMetaExpected is_cmeta_expected,
      IsSuperBlockExpected is_superblock_expected);

  Status CheckTabletTombstonedOnTS(int index,
                                   const string& tablet_id,
                                   IsCMetaExpected is_cmeta_expected);

  Status CheckTabletDeletedOnTS(int index,
                                const string& tablet_id,
                                IsSuperBlockExpected is_superblock_expected);

  void WaitForTabletTombstonedOnTS(int index,
                                   const string& tablet_id,
                                   IsCMetaExpected is_cmeta_expected);

  void WaitForTabletDeletedOnTS(int index,
                                const string& tablet_id,
                                IsSuperBlockExpected is_superblock_expected);

  void WaitForTSToCrash(int index);
  void WaitForAllTSToCrash();
  void WaitUntilTabletRunning(int index, const std::string& tablet_id);

  // Delete the given table. If the operation times out, optionally dump
  // the master stacks to help debug master-side deadlocks.
  void DeleteTable(const string& table_name,
                   ErrorDumpStackSelector selector = ON_ERROR_DUMP_STACKS);
};

string DeleteTableITest::GetLeaderUUID(const string& ts_uuid, const string& tablet_id) {
  ConsensusStatePB cstate;
  CHECK_OK(itest::GetConsensusState(ts_map_[ts_uuid], tablet_id, CONSENSUS_CONFIG_COMMITTED,
                                    MonoDelta::FromSeconds(10), &cstate));
  return cstate.leader_uuid();
}

Status DeleteTableITest::CheckTabletTombstonedOrDeletedOnTS(
      int index,
      const string& tablet_id,
      TabletDataState data_state,
      IsCMetaExpected is_cmeta_expected,
      IsSuperBlockExpected is_superblock_expected) {
  CHECK(data_state == TABLET_DATA_TOMBSTONED || data_state == TABLET_DATA_DELETED) << data_state;
  // There should be no WALs and no cmeta.
  if (inspect_->CountFilesInWALDirForTS(index, tablet_id) > 0) {
    return Status::IllegalState("WAL segments exist for tablet", tablet_id);
  }
  if (is_cmeta_expected == CMETA_EXPECTED &&
      !inspect_->DoesConsensusMetaExistForTabletOnTS(index, tablet_id)) {
    return Status::IllegalState("Expected cmeta for tablet " + tablet_id + " but it doesn't exist");
  }
  if (is_superblock_expected == SUPERBLOCK_EXPECTED) {
    RETURN_NOT_OK(inspect_->CheckTabletDataStateOnTS(index, tablet_id, { data_state }));
  } else {
    TabletSuperBlockPB superblock_pb;
    Status s = inspect_->ReadTabletSuperBlockOnTS(index, tablet_id, &superblock_pb);
    if (!s.IsNotFound()) {
      return Status::IllegalState("Found unexpected superblock for tablet " + tablet_id);
    }
  }
  return Status::OK();
}

Status DeleteTableITest::CheckTabletTombstonedOnTS(int index,
                                                  const string& tablet_id,
                                                  IsCMetaExpected is_cmeta_expected) {
  return CheckTabletTombstonedOrDeletedOnTS(index, tablet_id, TABLET_DATA_TOMBSTONED,
                                            is_cmeta_expected, SUPERBLOCK_EXPECTED);
}

Status DeleteTableITest::CheckTabletDeletedOnTS(int index,
                                               const string& tablet_id,
                                               IsSuperBlockExpected is_superblock_expected) {
  return CheckTabletTombstonedOrDeletedOnTS(index, tablet_id, TABLET_DATA_DELETED,
                                            CMETA_NOT_EXPECTED, is_superblock_expected);
}

void DeleteTableITest::WaitForTabletTombstonedOnTS(int index,
                                                  const string& tablet_id,
                                                  IsCMetaExpected is_cmeta_expected) {
  Status s;
  for (int i = 0; i < 6000; i++) {
    s = CheckTabletTombstonedOnTS(index, tablet_id, is_cmeta_expected);
    if (s.ok()) return;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_OK(s);
}

void DeleteTableITest::WaitForTabletDeletedOnTS(int index,
                                               const string& tablet_id,
                                               IsSuperBlockExpected is_superblock_expected) {
  Status s;
  for (int i = 0; i < 6000; i++) {
    s = CheckTabletDeletedOnTS(index, tablet_id, is_superblock_expected);
    if (s.ok()) return;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_OK(s);
}

void DeleteTableITest::WaitForTSToCrash(int index) {
  auto ts = cluster_->tablet_server(index);
  SCOPED_TRACE(ts->instance_id().permanent_uuid());
  ASSERT_OK(ts->WaitForInjectedCrash(MonoDelta::FromSeconds(60)));
}

void DeleteTableITest::WaitForAllTSToCrash() {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    NO_FATALS(WaitForTSToCrash(i));
  }
}

void DeleteTableITest::WaitUntilTabletRunning(int index, const std::string& tablet_id) {
  ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(index)->uuid()],
                                          tablet_id, MonoDelta::FromSeconds(60)));
}

void DeleteTableITest::DeleteTable(const string& table_name,
                                  ErrorDumpStackSelector selector) {
  Status s = client_->DeleteTable(table_name);
  if (s.IsTimedOut() && (ON_ERROR_DUMP_STACKS == selector)) {
    WARN_NOT_OK(PstackWatcher::DumpPidStacks(cluster_->master()->pid()),
                "Couldn't dump stacks");
  }
  ASSERT_OK(s);
}

// Test deleting an empty table, and ensure that the tablets get removed,
// and the master no longer shows the table as existing.
TEST_F(DeleteTableITest, TestDeleteEmptyTable) {
  NO_FATALS(StartCluster());
  // Create a table on the cluster. We're just using TestWorkload
  // as a convenient way to create it.
  TestWorkload(cluster_.get()).Setup();

  // The table should have replicas on all three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));

  // Grab the tablet ID (used later).
  vector<string> tablets = inspect_->ListTabletsOnTS(1);
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  // Delete it and wait for the replicas to get deleted.
  // We should have no tablets at the filesystem layer after deleting the table.
  NO_FATALS(DeleteTable(TestWorkload::kDefaultTableName));
  ASSERT_OK(inspect_->WaitForNoData());

  // Check that the master no longer exposes the table in any way:

  // 1) Should not list it in ListTables.
  vector<string> table_names;
  ASSERT_OK(client_->ListTables(&table_names));
  ASSERT_TRUE(table_names.empty()) << "table still exposed in ListTables";

  // 2) Should respond to GetTableSchema with a NotFound error.
  KuduSchema schema;
  Status s = client_->GetTableSchema(TestWorkload::kDefaultTableName, &schema);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // 3) Should return an error for GetTabletLocations RPCs.
  {
    rpc::RpcController rpc;
    master::GetTabletLocationsRequestPB req;
    master::GetTabletLocationsResponsePB resp;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    req.add_tablet_ids()->assign(tablet_id);
    ASSERT_OK(cluster_->master_proxy()->GetTabletLocations(req, &resp, &rpc));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_EQ(1, resp.errors_size());
    ASSERT_STR_CONTAINS(SecureShortDebugString(resp.errors(0)),
                        "code: NOT_FOUND message: \"Tablet deleted: Table deleted");
  }

  // 4) The master 'dump-entities' page should not list the deleted table or tablets.
  EasyCurl c;
  faststring entities_buf;
  ASSERT_OK(c.FetchURL(Substitute("http://$0/dump-entities",
                                  cluster_->master()->bound_http_hostport().ToString()),
                       &entities_buf));
  rapidjson::Document doc;
  doc.Parse<0>(entities_buf.ToString().c_str());
  ASSERT_EQ(0, doc["tables"].Size());
  ASSERT_EQ(0, doc["tablets"].Size());
}

// Test that a DeleteTablet RPC is rejected without a matching destination UUID.
TEST_F(DeleteTableITest, TestDeleteTabletDestUuidValidation) {
  NO_FATALS(StartCluster());
  // Create a table on the cluster. We're just using TestWorkload
  // as a convenient way to create it.
  TestWorkload(cluster_.get()).Setup();
  ASSERT_OK(inspect_->WaitForReplicaCount(3));

  vector<string> tablets = inspect_->ListTabletsOnTS(1);
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];

  tserver::DeleteTabletRequestPB req;
  tserver::DeleteTabletResponsePB resp;
  rpc::RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(20));

  req.set_dest_uuid("fake-uuid");
  req.set_tablet_id(tablet_id);
  req.set_delete_type(TABLET_DATA_TOMBSTONED);
  ASSERT_OK(ts->tserver_admin_proxy->DeleteTablet(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(tserver::TabletServerErrorPB::WRONG_SERVER_UUID, resp.error().code())
      << SecureShortDebugString(resp);
  ASSERT_STR_CONTAINS(StatusFromPB(resp.error().status()).ToString(),
                      "Wrong destination UUID");
}

// Test the atomic CAS argument to DeleteTablet().
TEST_F(DeleteTableITest, TestAtomicDeleteTablet) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  NO_FATALS(StartCluster());
  // Create a table on the cluster. We're just using TestWorkload
  // as a convenient way to create it.
  TestWorkload(cluster_.get()).Setup();

  // The table should have replicas on all three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));

  // Grab the tablet ID (used later).
  vector<string> tablets = inspect_->ListTabletsOnTS(1);
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  const int kTsIndex = 0;
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];

  // The committed config starts off with an opid_index of -1, so choose something lower.
  boost::optional<int64_t> opid_index(-2);
  tserver::TabletServerErrorPB::Code error_code;
  ASSERT_OK(itest::WaitUntilTabletRunning(ts, tablet_id, timeout));

  Status s;
  for (int i = 0; i < 100; i++) {
    s = itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, opid_index, timeout,
                            &error_code);
    if (error_code == TabletServerErrorPB::CAS_FAILED) break;
    // If we didn't get the expected CAS_FAILED error, it's OK to get 'TABLET_NOT_RUNNING'
    // because the "creating" maintenance state persists just slightly after it starts to
    // expose 'RUNNING' state in ListTablets()
    ASSERT_EQ(TabletServerErrorPB::TABLET_NOT_RUNNING, error_code)
        << "unexpected error: " << s.ToString();
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  ASSERT_EQ(TabletServerErrorPB::CAS_FAILED, error_code) << "unexpected error: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "of -2 but the committed config has opid_index of -1");

  // Now use the "latest", which is -1.
  opid_index = -1;
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, opid_index, timeout,
                                &error_code));
  inspect_->CheckTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_TOMBSTONED });

  // Now that the tablet is already tombstoned, our opid_index should be
  // ignored (because it's impossible to check it).
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, -9999, timeout,
                                &error_code));
  inspect_->CheckTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_TOMBSTONED });

  // Same with TOMBSTONED -> DELETED.
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_DELETED, -9999, timeout,
                                &error_code));
  inspect_->CheckTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_DELETED });
}

TEST_F(DeleteTableITest, TestDeleteTableWithConcurrentWrites) {
  NO_FATALS(StartCluster());
  int n_iters = AllowSlowTests() ? 20 : 1;
  for (int i = 0; i < n_iters; i++) {
    LOG(INFO) << "Running iteration " << i;
    TestWorkload workload(cluster_.get());
    workload.set_table_name(Substitute("table-$0", i));

    // We'll delete the table underneath the writers, so we expcted
    // a NotFound error during the writes.
    workload.set_not_found_allowed(true);
    workload.Setup();

    // Start the workload, and wait to see some rows actually inserted
    workload.Start();
    while (workload.rows_inserted() < 100) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    // Delete it and wait for the replicas to get deleted.
    NO_FATALS(DeleteTable(workload.table_name()));
    ASSERT_OK(inspect_->WaitForNoData());

    // Sleep just a little longer to make sure client threads send
    // requests to the missing tablets.
    SleepFor(MonoDelta::FromMilliseconds(50));

    workload.StopAndJoin();
    NO_FATALS(cluster_->AssertNoCrashes());
  }
}

// Test that a tablet replica is automatically tombstoned on startup if a local
// crash occurs in the middle of Tablet Copy. Additionally acts as a regression
// test for KUDU-1605.
TEST_F(DeleteTableITest, TestAutoTombstoneAfterCrashDuringTabletCopy) {
  // Set up flags to flush frequently, so that we get data on disk.
  vector<string> ts_flags = {
    "--flush_threshold_mb=0",
    "--maintenance_manager_polling_interval_ms=100"
  };
  vector<string> master_flags = {
    "--allow_unsafe_replication_factor=true"
  };
  NO_FATALS(StartCluster(ts_flags, master_flags));
  const MonoDelta timeout = MonoDelta::FromSeconds(10);
  const int kTsIndex = 0; // We'll test with the first TS.

  // Shut down TS 1, 2, write some data to TS 0 alone.
  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(cluster_->WaitForTabletServerCount(1, MonoDelta::FromSeconds(30)));

  // Set up a table which has a tablet only on TS 0. This will be used to test for
  // "collateral damage" bugs where incorrect handling of the main test tablet
  // accidentally removes blocks from another tablet.
  // We use a sequential workload so that we just flush and don't compact.
  // Thus we use a contiguous set of block IDs starting with 1, with no "holes"
  // from deleted blocks.
  string unaffected_tablet_id;
  TestWorkload unrepl_workload(cluster_.get());
  {
    unrepl_workload.set_num_replicas(1);
    unrepl_workload.set_table_name("other-table");
    unrepl_workload.set_num_write_threads(1);
    unrepl_workload.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
    unrepl_workload.Setup();

    // Figure out the tablet ID.
    vector<string> tablets = inspect_->ListTabletsOnTS(kTsIndex);
    ASSERT_EQ(1, tablets.size());
    unaffected_tablet_id = tablets[0];

    // Write rows until it has flushed a few times.
    unrepl_workload.Start();
    TabletSuperBlockPB sb;
    while (sb.rowsets().size() < 3) {
      SleepFor(MonoDelta::FromMilliseconds(10));
      ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(kTsIndex, unaffected_tablet_id, &sb));
    }
    unrepl_workload.StopAndJoin();
  }

  // Restart the other two servers.
  ASSERT_OK(cluster_->tablet_server(1)->Restart());
  ASSERT_OK(cluster_->tablet_server(2)->Restart());
  cluster_->tablet_server(kTsIndex)->Shutdown();

  // Restart the master to be sure that it only sees the live servers.
  // Otherwise it may try to create a tablet with a replica on the down server.
  // The table creation would eventually succeed after picking a different set of
  // replicas, but not before causing a timeout.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(cluster_->WaitForTabletServerCount(2, MonoDelta::FromSeconds(30)));

  // Create a new table with a single tablet replicated on the other two servers.
  // We use the same sequential workload. This produces block ID sequences
  // that look like:
  //   TS 0: |---- blocks from 'other-table' ---]
  //   TS 1: |---- blocks from default workload table ---]
  //   TS 2: |---- blocks from default workload table ---]
  // So, if we were to accidentally reuse or delete block IDs from TS 1 or TS 2 when
  // copying a tablet to TS 0, we'd surely notice it!
  string replicated_tablet_id;

  TestWorkload repl_workload(cluster_.get());
  {
    repl_workload.set_num_replicas(2);
    repl_workload.set_num_write_threads(1);
    repl_workload.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
    repl_workload.Setup();
    ASSERT_OK(inspect_->WaitForReplicaCount(3)); // 1 original plus 2 new replicas.

    // Figure out the tablet id of the new table.
    vector<string> tablets = inspect_->ListTabletsOnTS(1);
    ASSERT_EQ(1, tablets.size());
    replicated_tablet_id = tablets[0];

    repl_workload.Start();
    TabletSuperBlockPB sb;
    while (sb.rowsets().size() < 3) {
      SleepFor(MonoDelta::FromMilliseconds(10));
      ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(1, replicated_tablet_id, &sb));
    }
    repl_workload.StopAndJoin();
  }

  // Enable a fault crash when Tablet Copy occurs on TS 0.
  cluster_->tablet_server(kTsIndex)->mutable_flags()->push_back(
      "--fault_crash_after_tc_files_fetched=1.0");

  // Restart TS 0 and add it to the config. It will crash when tablet copy starts.
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  string leader_uuid = GetLeaderUUID(cluster_->tablet_server(1)->uuid(), replicated_tablet_id);
  TServerDetails* leader = DCHECK_NOTNULL(ts_map_[leader_uuid]);
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  ASSERT_OK(itest::AddServer(leader, replicated_tablet_id, ts, RaftPeerPB::VOTER,
                             boost::none, timeout));
  NO_FATALS(WaitForTSToCrash(kTsIndex));
  cluster_->tablet_server(kTsIndex)->Shutdown();

  LOG(INFO) << "Test progress: crashed on first attempt to copy";

  // The superblock should be in TABLET_DATA_COPYING state on disk.
  NO_FATALS(inspect_->CheckTabletDataStateOnTS(kTsIndex, replicated_tablet_id,
                                               { TABLET_DATA_COPYING }));

  // Restart and let it crash one more time.
  cluster_->tablet_server(kTsIndex)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  NO_FATALS(WaitForTSToCrash(kTsIndex));
  LOG(INFO) << "Test progress: crashed on second attempt to copy";

  // Remove the fault flag and let it successfully copy the tablet.
  cluster_->tablet_server(kTsIndex)->mutable_flags()->pop_back();
  cluster_->tablet_server(kTsIndex)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());

  // Everything should be consistent now.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(repl_workload.table_name(), ClusterVerifier::AT_LEAST,
                            repl_workload.rows_inserted()));
  NO_FATALS(v.CheckRowCount(unrepl_workload.table_name(), ClusterVerifier::AT_LEAST,
                            unrepl_workload.rows_inserted()));

  // Now we want to test the case where crashed while copying over a previously-tombstoned tablet.
  // So, we first remove the server, causing it to get tombstoned.
  ASSERT_OK(itest::RemoveServer(leader, replicated_tablet_id, ts, boost::none, timeout));
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, replicated_tablet_id, CMETA_EXPECTED));

  // ... and then add it back, with the fault runtime-enabled.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(kTsIndex),
                              "fault_crash_after_tc_files_fetched", "1.0"));
  ASSERT_OK(itest::AddServer(leader, replicated_tablet_id, ts, RaftPeerPB::VOTER,
                             boost::none, timeout));
  NO_FATALS(WaitForTSToCrash(kTsIndex));
  LOG(INFO) << "Test progress: crashed on attempt to copy over tombstoned";

  // Finally, crashing with no fault flags should fully recover again.
  cluster_->tablet_server(kTsIndex)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(repl_workload.table_name(), ClusterVerifier::AT_LEAST,
                            repl_workload.rows_inserted()));
  NO_FATALS(v.CheckRowCount(unrepl_workload.table_name(), ClusterVerifier::AT_LEAST,
                            unrepl_workload.rows_inserted()));
}

// Test that a tablet replica automatically tombstones itself if the remote
// server fails in the middle of the Tablet Copy process.
// Also test that we can Copy Tablet over a tombstoned tablet.
TEST_F(DeleteTableITest, TestAutoTombstoneAfterTabletCopyRemoteFails) {
  vector<string> ts_flags = {
      "--enable_leader_failure_detection=false",  // Make test deterministic.
      "--log_segment_size_mb=1",                  // Faster log rolls.
      "--log_compression_codec=NO_COMPRESSION"    // Faster log rolls.
  };
  vector<string> master_flags = {
      "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
      "--allow_unsafe_replication_factor=true"
  };
  NO_FATALS(StartCluster(ts_flags, master_flags));
  const MonoDelta kTimeout = MonoDelta::FromSeconds(20);
  const int kTsIndex = 0; // We'll test with the first TS.

  // We'll do a config change to Tablet Copy a replica here later. For
  // now, shut down TS-0.
  LOG(INFO) << "Shutting down TS " << cluster_->tablet_server(kTsIndex)->uuid();
  cluster_->tablet_server(kTsIndex)->Shutdown();

  // Bounce the Master so it gets new tablet reports and doesn't try to assign
  // a replica to the dead TS.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  cluster_->WaitForTabletServerCount(2, kTimeout);

  // Start a workload on the cluster, and run it for a little while.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2);
  workload.Setup();
  ASSERT_OK(inspect_->WaitForReplicaCount(2));

  // Figure out the tablet id.
  vector<string> tablets = inspect_->ListTabletsOnTS(1);
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  for (int i = 1; i <= 2; i++) {
    NO_FATALS(WaitUntilTabletRunning(i, tablet_id));
  }

  // Elect a leader and run some data through the cluster.
  const int kLeaderIndex = 1;
  string kLeaderUuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  ASSERT_OK(itest::StartElection(ts_map_[kLeaderUuid], tablet_id, kTimeout));
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Tablet Copy doesn't see the active WAL segment, and we need to
  // download a file to trigger the fault in this test. Due to the log index
  // chunks, that means 3 files minimum: One in-flight WAL segment, one index
  // chunk file (these files grow much more slowly than the WAL segments), and
  // one completed WAL segment.
  ASSERT_OK(inspect_->WaitForMinFilesInTabletWalDirOnTS(kLeaderIndex, tablet_id, 3));
  workload.StopAndJoin();

  // Cause the leader to crash when a follower tries to initiate Tablet Copy from it.
  const string& fault_flag = "fault_crash_on_handle_tc_fetch_data";
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(kLeaderIndex), fault_flag, "1.0"));

  // Add TS-0 as a new member to the config and wait for the leader to crash.
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  TServerDetails* leader = ts_map_[kLeaderUuid];
  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];
  // The server may crash before responding to our RPC.
  Status s = itest::AddServer(leader, tablet_id, ts, RaftPeerPB::VOTER, boost::none, kTimeout);
  ASSERT_TRUE(s.ok() || s.IsNetworkError()) << s.ToString();
  NO_FATALS(WaitForTSToCrash(kLeaderIndex));

  // The tablet server will detect that the leader failed, and automatically
  // tombstone its replica. Shut down the other non-leader replica to avoid
  // interference while we wait for this to happen.
  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, tablet_id, CMETA_NOT_EXPECTED));

  // Check the textual status message for the failed copy.
  {
    vector<ListTabletsResponsePB::StatusAndSchemaPB> status_pbs;
    ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, kTimeout, &status_pbs));
    ASSERT_STR_MATCHES(status_pbs[0].tablet_status().last_status(),
                       "Tablet Copy: Tombstoned tablet .*: Tablet copy aborted");
  }

  // Now bring the other replicas back, re-elect the previous leader (TS-1),
  // and wait for the leader to Tablet Copy the tombstoned replica. This
  // will have replaced a tablet with no consensus metadata.
  ASSERT_OK(cluster_->tablet_server(1)->Restart());
  ASSERT_OK(cluster_->tablet_server(2)->Restart());
  for (int i = 1; i <= 2; i++) {
    NO_FATALS(WaitUntilTabletRunning(i, tablet_id));
  }
  ASSERT_OK(itest::StartElection(ts_map_[kLeaderUuid], tablet_id, kTimeout));
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_READY }));

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(), ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));

  // Now pause the other replicas and tombstone our replica again.
  ASSERT_OK(cluster_->tablet_server(1)->Pause());
  ASSERT_OK(cluster_->tablet_server(2)->Pause());
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none, kTimeout));
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, tablet_id, CMETA_NOT_EXPECTED));

  // Bring them back again, let them yet again Copy a new replica on top of our tombstoned replica.
  // This time, the leader will have replaced a tablet with consensus metadata.
  ASSERT_OK(cluster_->tablet_server(1)->Resume());
  ASSERT_OK(cluster_->tablet_server(2)->Resume());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_READY }));

  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(), ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));
}

// Test for correct Tablet Copy merge of consensus metadata.
TEST_F(DeleteTableITest, TestMergeConsensusMetadata) {
  // Enable manual leader selection.
  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));
  const MonoDelta timeout = MonoDelta::FromSeconds(10);
  const int kTsIndex = 0;

  TestWorkload workload(cluster_.get());
  workload.Setup();
  ASSERT_OK(inspect_->WaitForReplicaCount(3));

  // Figure out the tablet id to Tablet Copy.
  vector<string> tablets = inspect_->ListTabletsOnTS(1);
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    NO_FATALS(WaitUntilTabletRunning(i, tablet_id));
  }

  // Elect a leader and run some data through the cluster.
  int leader_index = 1;
  string leader_uuid = cluster_->tablet_server(leader_index)->uuid();
  ASSERT_OK(itest::StartElection(ts_map_[leader_uuid], tablet_id, timeout));
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));

  // Verify that TS 0 voted for the chosen leader.
  ConsensusMetadataPB cmeta_pb;
  ASSERT_OK(inspect_->ReadConsensusMetadataOnTS(kTsIndex, tablet_id, &cmeta_pb));
  ASSERT_EQ(1, cmeta_pb.current_term());
  ASSERT_EQ(leader_uuid, cmeta_pb.voted_for());

  // Shut down all but TS 0 and try to elect TS 0. The election will fail but
  // the TS will record a vote for itself as well as a new term (term 2).
  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();
  NO_FATALS(WaitUntilTabletRunning(kTsIndex, tablet_id));
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  ASSERT_OK(itest::StartElection(ts, tablet_id, timeout));
  for (int i = 0; i < 6000; i++) {
    Status s = inspect_->ReadConsensusMetadataOnTS(kTsIndex, tablet_id, &cmeta_pb);
    if (s.ok() &&
        cmeta_pb.current_term() == 2 &&
        cmeta_pb.voted_for() == ts->uuid()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_EQ(2, cmeta_pb.current_term());
  ASSERT_EQ(ts->uuid(), cmeta_pb.voted_for());

  // Tombstone our special little guy, then shut him down.
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, tablet_id, CMETA_EXPECTED));
  cluster_->tablet_server(kTsIndex)->Shutdown();

  // Restart the other dudes and re-elect the same leader.
  ASSERT_OK(cluster_->tablet_server(1)->Restart());
  ASSERT_OK(cluster_->tablet_server(2)->Restart());
  TServerDetails* leader = ts_map_[leader_uuid];
  NO_FATALS(WaitUntilTabletRunning(1, tablet_id));
  NO_FATALS(WaitUntilTabletRunning(2, tablet_id));
  ASSERT_OK(itest::StartElection(leader, tablet_id, timeout));
  ASSERT_OK(itest::WaitUntilLeader(leader, tablet_id, timeout));

  // Bring our special little guy back up.
  // Wait until he gets Tablet Copied.
  LOG(INFO) << "Bringing TS " << cluster_->tablet_server(kTsIndex)->uuid()
            << " back up...";
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_READY }));

  // Assert that the election history is retained (voted for self).
  ASSERT_OK(inspect_->ReadConsensusMetadataOnTS(kTsIndex, tablet_id, &cmeta_pb));
  ASSERT_EQ(2, cmeta_pb.current_term());
  ASSERT_EQ(ts->uuid(), cmeta_pb.voted_for());

  // Now do the same thing as above, where we tombstone TS 0 then trigger a new
  // term (term 3) on the other machines. TS 0 will get copied
  // again, but this time the vote record on TS 0 for term 2 should not be
  // retained after Tablet Copy occurs.
  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();

  // Delete with retries because the tablet might still be bootstrapping.
  NO_FATALS(itest::DeleteTabletWithRetries(ts, tablet_id,
                                           TABLET_DATA_TOMBSTONED, boost::none, timeout));
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, tablet_id, CMETA_EXPECTED));

  ASSERT_OK(cluster_->tablet_server(1)->Restart());
  ASSERT_OK(cluster_->tablet_server(2)->Restart());
  NO_FATALS(WaitUntilTabletRunning(1, tablet_id));
  NO_FATALS(WaitUntilTabletRunning(2, tablet_id));
  ASSERT_OK(itest::StartElection(leader, tablet_id, timeout));
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kTsIndex, tablet_id, { TABLET_DATA_READY }));

  // The election history should have been wiped out.
  ASSERT_OK(inspect_->ReadConsensusMetadataOnTS(kTsIndex, tablet_id, &cmeta_pb));
  ASSERT_EQ(3, cmeta_pb.current_term());
  ASSERT_TRUE(!cmeta_pb.has_voted_for()) << SecureShortDebugString(cmeta_pb);
}

// Regression test for KUDU-987, a bug where followers with transactions in
// REPLICATING state, which means they have not yet been committed to a
// majority, cannot shut down during a DeleteTablet() call.
TEST_F(DeleteTableITest, TestDeleteFollowerWithReplicatingTransaction) {
  if (!AllowSlowTests()) {
    // We will typically wait at least 5 seconds for timeouts to occur.
    LOG(INFO) << "Skipping test in fast-test mode.";
    return;
  }

  const MonoDelta timeout = MonoDelta::FromSeconds(10);

  const int kNumTabletServers = 5;
  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  ts_flags.push_back("--flush_threshold_mb=0"); // Always be flushing.
  ts_flags.push_back("--maintenance_manager_polling_interval_ms=100");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags, kNumTabletServers));

  const int kTsIndex = 0; // We'll test with the first TS.
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kNumTabletServers);
  workload.Setup();

  // Figure out the tablet ids of the created tablets.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect TS 1 as leader.
  const int kLeaderIndex = 1;
  const string kLeaderUuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  TServerDetails* leader = ts_map_[kLeaderUuid];
  ASSERT_OK(itest::StartElection(leader, tablet_id, timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1));

  // Kill a majority, but leave the leader and a single follower.
  LOG(INFO) << "Killing majority";
  for (int i = 2; i < kNumTabletServers; i++) {
    cluster_->tablet_server(i)->Shutdown();
  }

  // Now write a single row to the leader.
  // We give 5 seconds for the timeout to pretty much guarantee that a flush
  // will occur due to the low flush threshold we set.
  LOG(INFO) << "Writing a row";
  Status s = WriteSimpleTestRow(leader, tablet_id, RowOperationsPB::INSERT,
                                1, 1, "hola, world", MonoDelta::FromSeconds(5));
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_STR_CONTAINS(s.ToString(), "timed out");

  LOG(INFO) << "Killing the leader...";
  cluster_->tablet_server(kLeaderIndex)->Shutdown();

  // Now tombstone the follower tablet. This should succeed even though there
  // are uncommitted operations on the replica.
  LOG(INFO) << "Tombstoning tablet " << tablet_id << " on TS " << ts->uuid();
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));
}

// Test that orphaned blocks are cleared from the superblock when a tablet is
// tombstoned.
TEST_F(DeleteTableITest, TestOrphanedBlocksClearedOnDelete) {
  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags, master_flags;
  ts_flags.push_back("--enable_leader_failure_detection=false");
  ts_flags.push_back("--flush_threshold_mb=0"); // Flush quickly since we wait for a flush to occur.
  ts_flags.push_back("--maintenance_manager_polling_interval_ms=100");
  master_flags.push_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  const int kFollowerIndex = 0;
  TServerDetails* follower_ts = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(follower_ts, 1, timeout, &tablets));
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  // Elect TS 1 as leader.
  const int kLeaderIndex = 1;
  const string kLeaderUuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  TServerDetails* leader_ts = ts_map_[kLeaderUuid];
  ASSERT_OK(itest::StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1));

  // Run a write workload and wait until we see some rowsets flush on the follower.
  workload.Start();
  TabletSuperBlockPB superblock_pb;
  for (int i = 0; i < 3000; i++) {
    ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(kFollowerIndex, tablet_id, &superblock_pb));
    if (!superblock_pb.rowsets().empty()) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_GT(superblock_pb.rowsets_size(), 0)
      << "Timed out waiting for rowset flush on TS " << follower_ts->uuid() << ": "
      << "Superblock:\n" << SecureDebugString(superblock_pb);

  // Shut down the leader so it doesn't try to copy a new replica to our follower later.
  workload.StopAndJoin();
  cluster_->tablet_server(kLeaderIndex)->Shutdown();

  // Tombstone the follower and check that there are no rowsets or orphaned
  // blocks retained in the superblock.
  ASSERT_OK(itest::DeleteTablet(follower_ts, tablet_id, TABLET_DATA_TOMBSTONED,
                                boost::none, timeout));
  NO_FATALS(WaitForTabletTombstonedOnTS(kFollowerIndex, tablet_id, CMETA_EXPECTED));
  ASSERT_OK(inspect_->ReadTabletSuperBlockOnTS(kFollowerIndex, tablet_id, &superblock_pb));
  ASSERT_EQ(0, superblock_pb.rowsets_size()) << SecureDebugString(superblock_pb);
  ASSERT_EQ(0, superblock_pb.orphaned_blocks_size()) << SecureDebugString(superblock_pb);
}

vector<const string*> Grep(const string& needle, const vector<string>& haystack) {
  vector<const string*> results;
  for (const string& s : haystack) {
    if (s.find(needle) != string::npos) {
      results.push_back(&s);
    }
  }
  return results;
}

vector<string> ListOpenFiles(pid_t pid) {
  string cmd = strings::Substitute("export PATH=$$PATH:/usr/bin:/usr/sbin; lsof -n -p $0", pid);
  vector<string> argv = { "bash", "-c", cmd };
  string out;
  CHECK_OK(Subprocess::Call(argv, "", &out));
  vector<string> lines = strings::Split(out, "\n");
  return lines;
}

int PrintOpenTabletFiles(pid_t pid, const string& tablet_id) {
  vector<string> lines = ListOpenFiles(pid);
  vector<const string*> wal_lines = Grep(tablet_id, lines);
  LOG(INFO) << "There are " << wal_lines.size() << " open WAL files for pid " << pid << ":";
  for (const string* l : wal_lines) {
    LOG(INFO) << *l;
  }
  return wal_lines.size();
}

// Regression test for tablet deletion FD leak. See KUDU-1288.
TEST_F(DeleteTableITest, TestFDsNotLeakedOnTabletTombstone) {
  const MonoDelta timeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster({}, {}, 1));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts_map_.begin()->second, 1, timeout, &tablets));
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Tombstone the tablet and then ensure that lsof does not list any
  // tablet-related paths.
  ExternalTabletServer* ets = cluster_->tablet_server(0);
  ASSERT_OK(itest::DeleteTablet(ts_map_[ets->uuid()],
                                tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));
  ASSERT_EQ(0, PrintOpenTabletFiles(ets->pid(), tablet_id));

  // Restart the TS after deletion and then do the same lsof check again.
  ets->Shutdown();
  ASSERT_OK(ets->Restart());
  ASSERT_EQ(0, PrintOpenTabletFiles(ets->pid(), tablet_id));
}

// Regression test for KUDU-1545: crash when visiting the tablet page for a
// tombstoned tablet.
TEST_F(DeleteTableITest, TestWebPageForTombstonedTablet) {
  const MonoDelta timeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster({}, {}, 1));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts_map_.begin()->second, 1, timeout, &tablets));
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Tombstone the tablet.
  ExternalTabletServer* ets = cluster_->tablet_server(0);
  AssertEventually([&]() {
    ASSERT_OK(itest::DeleteTablet(ts_map_[ets->uuid()],
                                  tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));
  });

  // Check the various web pages associated with the tablet, ensuring
  // they don't crash and at least have the tablet ID within them.
  EasyCurl c;
  const auto& pages = { "tablet",
                        "tablet-rowsetlayout-svg",
                        "tablet-consensus-status",
                        "log-anchors" };
  for (const auto& page : pages) {
    faststring buf;
    ASSERT_OK(c.FetchURL(Substitute(
        "http://$0/$1?id=$2",
        cluster_->tablet_server(0)->bound_http_hostport().ToString(),
        page,
        tablet_id), &buf));
    ASSERT_STR_CONTAINS(buf.ToString(), tablet_id);
  }
}


TEST_F(DeleteTableITest, TestUnknownTabletsAreNotDeleted) {
  // Speed up heartbeating so that the unknown tablet is detected faster.
  vector<string> extra_ts_flags = { "--heartbeat_interval_ms=10" };

  NO_FATALS(StartCluster(extra_ts_flags, {}, 1));

  Schema schema(GetSimpleTestSchema());
  client::KuduSchema client_schema(client::KuduSchemaFromSchema(schema));
  unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
  ASSERT_OK(creator->table_name("test")
      .schema(&client_schema)
      .set_range_partition_columns({"key"})
      .num_replicas(1)
      .Create());

  // Figure out the tablet id of the created tablet.
  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts_map_.begin()->second, 1, timeout, &tablets));
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Delete the master's metadata and start it back up. The tablet created
  // above is now unknown, but should not be deleted!
  cluster_->master()->Shutdown();
  ASSERT_OK(env_->DeleteRecursively(cluster_->master()->data_dir()));
  ASSERT_OK(cluster_->master()->Restart());

  // Give the master a chance to finish writing the new master tablet to disk
  // so that it can be found after the subsequent restart below.
  ASSERT_OK(cluster_->master()->WaitForCatalogManager());

  int64_t num_delete_attempts;
  AssertEventually([&]() {
    int64_t num_heartbeats;
    ASSERT_OK(cluster_->master()->GetInt64Metric(
        &METRIC_ENTITY_server, "kudu.master",
        &METRIC_handler_latency_kudu_master_MasterService_TSHeartbeat, "total_count",
        &num_heartbeats));
    ASSERT_GE(num_heartbeats, 1);
  });
  ASSERT_OK(cluster_->tablet_server(0)->GetInt64Metric(
      &METRIC_ENTITY_server, "kudu.tabletserver",
      &METRIC_handler_latency_kudu_tserver_TabletServerAdminService_DeleteTablet,
      "total_count", &num_delete_attempts));
  ASSERT_EQ(0, num_delete_attempts);

  // Now restart the master with orphan deletion enabled. The tablet should get
  // deleted.
  // We also need to restart the tablet server, or else the old tablet server
  // won't be able to authenticate to the new master, due to it having a new
  // CA certificate which the old tserver doesn't trust.
  //
  // TODO(KUDU-65): perhaps this is actually a feature? should we have tablet servers
  // remember the CA cert persistently so that it's impossible to connect an
  // old tserver to a new cluster?
  cluster_->Shutdown();
  cluster_->master()->mutable_flags()->push_back(
      "--catalog_manager_delete_orphaned_tablets");
  ASSERT_OK(cluster_->Restart());
  AssertEventually([&]() {
    ASSERT_OK(cluster_->tablet_server(0)->GetInt64Metric(
        &METRIC_ENTITY_server, "kudu.tabletserver",
        &METRIC_handler_latency_kudu_tserver_TabletServerAdminService_DeleteTablet,
        "total_count", &num_delete_attempts));
    // Sometimes the tablet server has time to report the orphaned tablet multiple times
    // before the delete succeeds. This is ok because tablet deletion is idempotent.
    ASSERT_GE(num_delete_attempts, 1);
    ASSERT_OK(CheckTabletDeletedOnTS(0, tablet_id, SUPERBLOCK_NOT_EXPECTED));
  });

}

// Parameterized test case for TABLET_DATA_DELETED deletions.
class DeleteTableDeletedParamTest : public DeleteTableITest,
                                    public ::testing::WithParamInterface<const char*> {
};

// Test that if a server crashes mid-delete that the delete will be rolled
// forward on startup. Parameterized by different fault flags that cause a
// crash at various points.
TEST_P(DeleteTableDeletedParamTest, TestRollForwardDelete) {
  NO_FATALS(StartCluster());
  const string fault_flag = GetParam();
  LOG(INFO) << "Running with fault flag: " << fault_flag;

  // Dynamically set the fault flag so they crash when DeleteTablet() is called
  // by the Master.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i), fault_flag, "1.0"));
  }

  // Create a table on the cluster. We're just using TestWorkload
  // as a convenient way to create it.
  TestWorkload(cluster_.get()).Setup();

  // The table should have replicas on all three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));

  // Delete it and wait for the tablet servers to crash.
  NO_FATALS(DeleteTable(TestWorkload::kDefaultTableName));
  NO_FATALS(WaitForAllTSToCrash());

  // There should still be data left on disk.
  Status s = inspect_->CheckNoData();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // Now restart the tablet servers. They should roll forward their deletes.
  // We don't have to reset the fault flag here because it was set dynamically.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster_->tablet_server(i)->Shutdown();
    ASSERT_OK(cluster_->tablet_server(i)->Restart());
  }
  ASSERT_OK(inspect_->WaitForNoData());
}

// Faults appropriate for the TABLET_DATA_DELETED case.
const char* deleted_faults[] = {"fault_crash_after_blocks_deleted",
                                "fault_crash_after_wal_deleted",
                                "fault_crash_after_cmeta_deleted"};

INSTANTIATE_TEST_CASE_P(FaultFlags, DeleteTableDeletedParamTest,
                        ::testing::ValuesIn(deleted_faults));

// Parameterized test case for TABLET_DATA_TOMBSTONED deletions.
class DeleteTableTombstonedParamTest : public DeleteTableITest,
                                       public ::testing::WithParamInterface<const char*> {
};

// Regression test for tablet tombstoning. Tests:
// 1. basic creation & tombstoning of a tablet.
// 2. roll-forward (crash recovery) of a partially-completed tombstoning of a tablet.
// 3. permanent deletion of a TOMBSTONED tablet
//    (transition from TABLET_DATA_TOMBSTONED to TABLET_DATA_DELETED).
TEST_P(DeleteTableTombstonedParamTest, TestTabletTombstone) {
  vector<string> flags;
  // We want fast log rolls and deterministic preallocation, since we wait for
  // a certain number of logs at the beginning of the test.
  flags.push_back("--log_segment_size_mb=1");
  flags.push_back("--log_async_preallocate_segments=false");
  flags.push_back("--log_min_segments_to_retain=3");
  flags.push_back("--log_compression_codec=NO_COMPRESSION");
  NO_FATALS(StartCluster(flags));
  const string fault_flag = GetParam();
  LOG(INFO) << "Running with fault flag: " << fault_flag;

  MonoDelta timeout = MonoDelta::FromSeconds(30);

  // Create a table with 2 tablets. We delete the first tablet without
  // injecting any faults, then we delete the second tablet while exercising
  // several fault injection points.
  const int kNumTablets = 2;
  vector<const KuduPartialRow*> split_rows;
  Schema schema(GetSimpleTestSchema());
  client::KuduSchema client_schema(client::KuduSchemaFromSchema(schema));
  KuduPartialRow* split_row = client_schema.NewRow();
  ASSERT_OK(split_row->SetInt32(0, numeric_limits<int32_t>::max() / kNumTablets));
  split_rows.push_back(split_row);
  gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(TestWorkload::kDefaultTableName)
                          .split_rows(split_rows)
                          .schema(&client_schema)
                          .set_range_partition_columns({ "key" })
                          .num_replicas(3)
                          .Create());

  // Start a workload on the cluster, and run it until we find WALs on disk.
  TestWorkload workload(cluster_.get());
  workload.set_payload_bytes(32 * 1024); // Write ops of size 32KB to quickly fill the logs.
  workload.set_write_batch_size(1);
  workload.Setup();

  // The table should have 2 tablets (1 split) on all 3 tservers (for a total of 6).
  ASSERT_OK(inspect_->WaitForReplicaCount(6));

  // Set up the proxies so we can easily send DeleteTablet() RPCs.
  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];

  // Ensure the tablet server is reporting 2 tablets.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(itest::WaitForNumTabletsOnTS(ts, 2, timeout, &tablets));

  LOG(INFO) << "Starting workload...";

  // Run the workload against whoever the leader is until WALs appear on TS 0
  // for the tablets we created.
  const int kTsIndex = 0; // Index of the tablet server we'll use for the test.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  LOG(INFO) << "Waiting for 3 wal files for tablet "
            << tablets[0].tablet_status().tablet_id() << "...";
  ASSERT_OK(inspect_->WaitForMinFilesInTabletWalDirOnTS(kTsIndex,
            tablets[0].tablet_status().tablet_id(), 3));

  LOG(INFO) << "Waiting for 3 wal files for tablet "
            << tablets[1].tablet_status().tablet_id() << "...";
  ASSERT_OK(inspect_->WaitForMinFilesInTabletWalDirOnTS(kTsIndex,
            tablets[1].tablet_status().tablet_id(), 3));

  LOG(INFO) << "Stopping workload...";
  workload.StopAndJoin();

  // Shut down the master and the other tablet servers so they don't interfere
  // by attempting to create or copy tablets while we delete tablets.
  cluster_->master()->Shutdown();
  cluster_->tablet_server(1)->Shutdown();
  cluster_->tablet_server(2)->Shutdown();

  // Tombstone the first tablet.
  string tablet_id = tablets[0].tablet_status().tablet_id();
  LOG(INFO) << "Tombstoning first tablet " << tablet_id << "...";
  ASSERT_TRUE(inspect_->DoesConsensusMetaExistForTabletOnTS(kTsIndex, tablet_id)) << tablet_id;
  ASSERT_OK(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));
  LOG(INFO) << "Waiting for first tablet to be tombstoned...";
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, tablet_id, CMETA_EXPECTED));

  ASSERT_OK(itest::WaitForNumTabletsOnTS(ts, 2, timeout, &tablets));
  for (const ListTabletsResponsePB::StatusAndSchemaPB& t : tablets) {
    if (t.tablet_status().tablet_id() == tablet_id) {
      ASSERT_EQ(tablet::SHUTDOWN, t.tablet_status().state());
      ASSERT_EQ(TABLET_DATA_TOMBSTONED, t.tablet_status().tablet_data_state())
          << t.tablet_status().tablet_id() << " not tombstoned";
    }
  }

  // Now tombstone the 2nd tablet, causing a fault.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(kTsIndex), fault_flag, "1.0"));
  tablet_id = tablets[1].tablet_status().tablet_id();
  LOG(INFO) << "Tombstoning second tablet " << tablet_id << "...";
  ignore_result(itest::DeleteTablet(ts, tablet_id, TABLET_DATA_TOMBSTONED, boost::none, timeout));
  NO_FATALS(WaitForTSToCrash(kTsIndex));

  // Restart the tablet server and wait for the WALs to be deleted and for the
  // superblock to show that it is tombstoned.
  cluster_->tablet_server(kTsIndex)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  LOG(INFO) << "Waiting for second tablet to be tombstoned...";
  NO_FATALS(WaitForTabletTombstonedOnTS(kTsIndex, tablet_id, CMETA_EXPECTED));
  // The tombstoned tablets will still show up in ListTablets(),
  // just with their data state set as TOMBSTONED. They should also be listed
  // as NOT_STARTED because we restarted the server.
  ASSERT_OK(itest::WaitForNumTabletsOnTS(ts, 2, timeout, &tablets));
  for (const ListTabletsResponsePB::StatusAndSchemaPB& t : tablets) {
    ASSERT_EQ(tablet::NOT_STARTED, t.tablet_status().state());
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, t.tablet_status().tablet_data_state())
        << t.tablet_status().tablet_id() << " not tombstoned";
  }

  // Check that, upon restart of the tablet server with a tombstoned tablet,
  // we don't unnecessary "roll forward" and rewrite the tablet metadata file
  // when it is already fully deleted.
  int64_t orig_mtime = inspect_->GetTabletSuperBlockMTimeOrDie(kTsIndex, tablet_id);
  cluster_->tablet_server(kTsIndex)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kTsIndex)->Restart());
  int64_t new_mtime = inspect_->GetTabletSuperBlockMTimeOrDie(kTsIndex, tablet_id);
  ASSERT_EQ(orig_mtime, new_mtime)
      << "Tablet superblock should not have been re-flushed unnecessarily";

  // Finally, delete all tablets on the TS, and wait for all data to be gone.
  LOG(INFO) << "Deleting all tablets...";
  for (const ListTabletsResponsePB::StatusAndSchemaPB& tablet : tablets) {
    string tablet_id = tablet.tablet_status().tablet_id();
    // We need retries here, since some of the tablets may still be
    // bootstrapping after being restarted above.
    NO_FATALS(itest::DeleteTabletWithRetries(ts, tablet_id,
                                             TABLET_DATA_DELETED, boost::none, timeout));
  }
  ASSERT_OK(inspect_->WaitForNoDataOnTS(kTsIndex));
}

// Faults appropriate for the TABLET_DATA_TOMBSTONED case.
// Tombstoning a tablet does not delete the consensus metadata.
const char* tombstoned_faults[] = {"fault_crash_after_blocks_deleted",
                                   "fault_crash_after_wal_deleted"};
INSTANTIATE_TEST_CASE_P(FaultFlags, DeleteTableTombstonedParamTest,
                        ::testing::ValuesIn(tombstoned_faults));


class DeleteTableWhileScanInProgressParamTest :
    public DeleteTableITest,
    public ::testing::WithParamInterface<
        std::tuple<KuduScanner::ReadMode, KuduClient::ReplicaSelection>> {
};

// Make sure the tablet server keeps the necessary data to serve scan request in
// progress if tablet is marked for deletion.
TEST_P(DeleteTableWhileScanInProgressParamTest, Test) {
  const auto read_mode_to_string = [](KuduScanner::ReadMode mode) {
    switch (mode) {
      case KuduScanner::READ_LATEST:
        return "READ_LATEST";
      case KuduScanner::READ_AT_SNAPSHOT:
        return "READ_AT_SNAPSHOT";
      default:
        return "UNKNOWN";
    }
  };
  const auto replica_sel_to_string = [](KuduClient::ReplicaSelection sel) {
    switch (sel) {
      case KuduClient::LEADER_ONLY:
        return "LEADER_ONLY";
      case KuduClient::CLOSEST_REPLICA:
        return "CLOSEST_REPLICA";
      case KuduClient::FIRST_REPLICA:
        return "FIRST_REPLICA";
      default:
        return "UNKNOWN";
    }
  };

  const std::vector<std::string> extra_ts_flags = {
    // Set the flush threshold low so that we have a mix of flushed and
    // unflushed operations in the WAL, when we bootstrap.
    "--flush_threshold_mb=1",

    // Set the compaction budget to be low so that we get multiple passes
    // of compaction instead of selecting all of the rowsets in a single
    // compaction of the whole tablet.
    "--tablet_compaction_budget_mb=1",

    // Set the major delta compaction ratio low enough that we trigger
    // a lot of them.
    "--tablet_delta_store_major_compact_min_ratio=0.001",
  };

  // Approximate number of rows to insert. This is not exact number due to the
  // way the test controls the progress of the test workload.
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  // Test is too slow in ASAN/TSAN.
  const size_t rows_to_insert = 10000;
#else
  const size_t rows_to_insert = AllowSlowTests() ? 100000 : 10000;
#endif

  const auto& param = GetParam();
  const auto mode = std::get<0>(param);
  const auto sel = std::get<1>(param);

  // In case of failure, print out string representation of the parameterized
  // case for ease of troubleshooting.
  SCOPED_TRACE(Substitute("read mode $0; replica selection mode $1",
                          read_mode_to_string(mode),
                          replica_sel_to_string(sel)));
  NO_FATALS(StartCluster(extra_ts_flags));

  TestWorkload w(cluster_.get());
  w.set_write_pattern(TestWorkload::INSERT_RANDOM_ROWS);
  w.Setup();

  // Start the workload, and wait to see some rows actually inserted.
  w.Start();
  while (w.rows_inserted() < rows_to_insert) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  w.StopAndJoin();
  const int64_t ref_row_count = w.rows_inserted();

  using kudu::client::sp::shared_ptr;
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(w.table_name(), &table));
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetReadMode(mode));
  ASSERT_OK(scanner.SetSelection(sel));
  // Setup batch size to be small enough to guarantee the scan
  // will not fetch all the data at once.
  ASSERT_OK(scanner.SetBatchSizeBytes(1));
  ASSERT_OK(scanner.Open());
  ASSERT_TRUE(scanner.HasMoreRows());
  KuduScanBatch batch;
  ASSERT_OK(scanner.NextBatch(&batch));
  size_t row_count = batch.NumRows();

  // Once the first batch of data has been fetched and there is some more
  // to fetch, delete the table.
  NO_FATALS(DeleteTable(w.table_name(), ON_ERROR_DO_NOT_DUMP_STACKS));

  // Wait while the table is no longer advertised on the cluster.
  // This ensures the table deletion request has been processed by tablet
  // servers.
  vector<string> tablets;
  do {
    SleepFor(MonoDelta::FromMilliseconds(250));
    tablets = inspect_->ListTablets();
  } while (!tablets.empty());

  // Make sure the scanner can continue and fetch the rest of rows.
  ASSERT_TRUE(scanner.HasMoreRows());
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    const Status s = scanner.NextBatch(&batch);
    ASSERT_TRUE(s.ok()) << s.ToString();
    row_count += batch.NumRows();
  }

  // Verify the total row count. The exact count must be there in case of
  // READ_AT_SNAPSHOT mode regardless of replica selection or if reading
  // from a leader tablet in any scan mode. In the case of the READ_LATEST
  // mode the data might be fetched from a lagging replica and the scan
  // row count might be less than the inserted row count.
  if (mode == KuduScanner::READ_AT_SNAPSHOT ||
      sel == KuduClient::LEADER_ONLY) {
    EXPECT_EQ(ref_row_count, row_count);
  }

  // Close the scanner to make sure it does not hold any references on the
  // data about to be deleted by the hosting tablet server.
  scanner.Close();

  // Make sure the table has been deleted.
  EXPECT_OK(inspect_->WaitForNoData());
  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(StopCluster());
}

const KuduScanner::ReadMode read_modes[] = {
  KuduScanner::READ_LATEST,
  KuduScanner::READ_AT_SNAPSHOT,
};
const KuduClient::ReplicaSelection replica_selectors[] = {
  KuduClient::LEADER_ONLY,
  KuduClient::CLOSEST_REPLICA,
  KuduClient::FIRST_REPLICA,
};
INSTANTIATE_TEST_CASE_P(
    Params, DeleteTableWhileScanInProgressParamTest,
    ::testing::Combine(::testing::ValuesIn(read_modes),
                       ::testing::ValuesIn(replica_selectors)));

} // namespace kudu
