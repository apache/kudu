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

#include "kudu/tools/ksck_remote.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/data_gen_util.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_checksum.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/promise.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(checksum_scan);
DECLARE_bool(consensus);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(safe_time_max_lag_ms);
DECLARE_int32(scanner_max_wait_ms);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_int32(timestamp_update_period_ms);
DECLARE_int32(wait_before_setting_snapshot_timestamp_ms);
DECLARE_string(location_mapping_cmd);

DEFINE_int32(experimental_flag_for_ksck_test, 0,
             "A flag marked experimental so it can be used to test ksck's "
             "unusual flag detection features");
TAG_FLAG(experimental_flag_for_ksck_test, experimental);

using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::thread;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

static const char *kTableName = "ksck-test-table";

class RemoteKsckTest : public KuduTest {
 public:
  RemoteKsckTest()
    : random_(SeedRandom()) {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    CHECK_OK(b.Build(&schema_));
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Speed up testing, saves about 700ms per TEST_F.
    FLAGS_heartbeat_interval_ms = 10;

    InternalMiniClusterOptions opts;

    opts.num_masters = 3;
    opts.num_tablet_servers = 3;
    mini_cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(mini_cluster_->Start());

    // Connect to the cluster.
    ASSERT_OK(mini_cluster_->CreateClient(nullptr, &client_));

    // Create one table.
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    ASSERT_OK(table_creator->table_name(kTableName)
                     .schema(&schema_)
                     .num_replicas(3)
                     .set_range_partition_columns({ "key" })
                     .split_rows(GenerateSplitRows())
                     .Create());
#pragma GCC diagnostic pop
    // Make sure we can open the table.
    shared_ptr<KuduTable> client_table;
    ASSERT_OK(client_->OpenTable(kTableName, &client_table));

    vector<string> master_addresses;
    master_addresses.reserve(opts.num_masters);
    for (int i = 0; i < mini_cluster_->num_masters(); i++) {
        master_addresses.push_back(
            mini_cluster_->mini_master(i)->bound_rpc_addr_str());
    }
    ASSERT_OK(RemoteKsckCluster::Build(master_addresses, &cluster_));
    ksck_.reset(new Ksck(cluster_, &err_stream_));
  }

  virtual void TearDown() OVERRIDE {
    if (mini_cluster_) {
      mini_cluster_->Shutdown();
      mini_cluster_.reset();
    }
    KuduTest::TearDown();
  }

  // Writes rows to the table until the continue_writing flag is set to false.
  void GenerateRowWritesLoop(CountDownLatch* started_writing,
                             const AtomicBool& continue_writing,
                             Promise<Status>* promise) {
    shared_ptr<KuduTable> table;
    Status status;
    SCOPED_CLEANUP({
      promise->Set(status);
      started_writing->CountDown(1);
    });
    status = client_->OpenTable(kTableName, &table);
    if (!status.ok()) {
      return;
    }
    shared_ptr<KuduSession> session(client_->NewSession());
    session->SetTimeoutMillis(10000);
    status = session->SetFlushMode(KuduSession::MANUAL_FLUSH);
    if (!status.ok()) {
      return;
    }

    for (uint64_t i = 0; continue_writing.Load(); i++) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      GenerateDataForRow(table->schema(), i, &random_, insert->mutable_row());
      status = session->Apply(insert.release());
      if (!status.ok()) {
        return;
      }
      status = session->Flush();
      if (!status.ok()) {
        return;
      }
      // Wait for the first 100 writes so that it's very likely all replicas have committed a
      // message in each tablet; otherwise, safe time might not have been updated on all replicas
      // and some might refuse snapshot scans because of lag.
      if (i > 100) {
        started_writing->CountDown(1);
      }
    }
  }

 protected:
  // Generate a set of split rows for tablets used in this test.
  vector<const KuduPartialRow*> GenerateSplitRows() {
    vector<const KuduPartialRow*> split_rows;
    int num_tablets = AllowSlowTests() ? 10 : 3;
    for (int i = 1; i < num_tablets; i++) {
      KuduPartialRow* row = schema_.NewRow();
      CHECK_OK(row->SetInt32(0, i * 10));
      split_rows.push_back(row);
    }
    return split_rows;
  }

  Status GenerateRowWrites(uint64_t num_rows) {
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table));
    shared_ptr<KuduSession> session(client_->NewSession());
    session->SetTimeoutMillis(10000);
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    for (uint64_t i = 0; i < num_rows; i++) {
      VLOG(1) << "Generating write for row id " << i;
      unique_ptr<KuduInsert> insert(table->NewInsert());
      GenerateDataForRow(table->schema(), i, &random_, insert->mutable_row());
      RETURN_NOT_OK(session->Apply(insert.release()));
    }
    RETURN_NOT_OK(session->Flush());
    return Status::OK();
  }

  void CreateTableWithNoTablet(const string& table_name) {
    // Create a table with one range partition.
    client::KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("value")->Type(KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    int lower_bound = 0;
    int upper_bound = 100;
    unique_ptr<KuduPartialRow> lower(schema.NewRow());
    unique_ptr<KuduPartialRow> upper(schema.NewRow());
    ASSERT_OK(lower->SetInt32("key", lower_bound));
    ASSERT_OK(upper->SetInt32("key", upper_bound));
    ASSERT_OK(table_creator->table_name(table_name)
                            .schema(&schema)
                            .set_range_partition_columns({ "key" })
                            .add_range_partition(lower.release(),upper.release())
                            .num_replicas(3)
                            .Create());

    // Drop range partition.
    lower.reset(schema.NewRow());
    upper.reset(schema.NewRow());
    ASSERT_OK(lower->SetInt32("key", lower_bound));
    ASSERT_OK(upper->SetInt32("key", upper_bound));
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_name));
    table_alterer->DropRangePartition(lower.release(), upper.release());
    ASSERT_OK(table_alterer->Alter());
  }

  Status GetTabletIds(vector<string>* tablet_ids) {
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table));

    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);

    KuduScanTokenBuilder builder(table.get());
    RETURN_NOT_OK(builder.Build(&tokens));
    for (const auto* t : tokens) {
      tablet_ids->emplace_back(t->tablet().id());
    }
    return Status::OK();
  }

  unique_ptr<InternalMiniCluster> mini_cluster_;
  unique_ptr<Ksck> ksck_;
  shared_ptr<client::KuduClient> client_;
  std::shared_ptr<KsckCluster> cluster_;

  // Captures logged messages from ksck.
  std::ostringstream err_stream_;

 private:
  client::KuduSchema schema_;
  Random random_;
};

TEST_F(RemoteKsckTest, TestClusterOk) {
  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckMasterConsensus());
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->CheckTabletServerUnusualFlags());
}

TEST_F(RemoteKsckTest, TestCheckUnusualFlags) {
  FLAGS_experimental_flag_for_ksck_test = 1;

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->CheckTabletServerUnusualFlags());
  ASSERT_OK(ksck_->PrintResults());

  const string& err_string = err_stream_.str();
  ASSERT_STR_CONTAINS(err_string,
      "Some masters have unsafe, experimental, or hidden flags set");
  ASSERT_STR_CONTAINS(err_string,
      "Some tablet servers have unsafe, experimental, or hidden flags set");
  ASSERT_STR_CONTAINS(err_string, "experimental_flag_for_ksck_test");
}

TEST_F(RemoteKsckTest, TestTabletServerMismatchedUUID) {
  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckMasterConsensus());
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());

  tserver::MiniTabletServer* tablet_server = mini_cluster_->mini_tablet_server(0);
  string old_uuid = tablet_server->uuid();
  string root_dir = mini_cluster_->GetTabletServerFsRoot(0) + "2";
  HostPort address = HostPort(tablet_server->bound_rpc_addr());

  tablet_server->Shutdown();
  tserver::MiniTabletServer new_tablet_server(root_dir, address);
  ASSERT_OK(new_tablet_server.Start());
  ASSERT_OK(new_tablet_server.WaitStarted());

  string new_uuid = new_tablet_server.uuid();

  ASSERT_TRUE(ksck_->FetchInfoFromTabletServers().IsNetworkError());
  ASSERT_OK(ksck_->PrintResults());

  string match_string = "Error from $0: Remote error: ID reported by tablet server "
                        "($1) doesn't match the expected ID: $2 (WRONG_SERVER_UUID)";
  ASSERT_STR_CONTAINS(err_stream_.str(), Substitute(match_string, address.ToString(),
                                                    new_uuid, old_uuid));
  tserver::MiniTabletServer* ts = mini_cluster_->mini_tablet_server(1);
  ASSERT_STR_CONTAINS(err_stream_.str(), Substitute("$0 | $1 | HEALTHY           | <none>",
                                                    ts->uuid(),
                                                    ts->bound_rpc_addr().ToString()));
  ts = mini_cluster_->mini_tablet_server(2);
  ASSERT_STR_CONTAINS(err_stream_.str(), Substitute("$0 | $1 | HEALTHY           | <none>",
                                                    ts->uuid(),
                                                    ts->bound_rpc_addr().ToString()));
}

TEST_F(RemoteKsckTest, TestTableConsistency) {
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  Status s;
  while (MonoTime::Now() < deadline) {
    ASSERT_OK(ksck_->CheckMasterHealth());
    ASSERT_OK(ksck_->CheckMasterUnusualFlags());
    ASSERT_OK(ksck_->CheckMasterConsensus());
    ASSERT_OK(ksck_->CheckClusterRunning());
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
    ASSERT_OK(ksck_->FetchInfoFromTabletServers());
    s = ksck_->CheckTablesConsistency();
    if (s.ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_OK(s);
}

TEST_F(RemoteKsckTest, TestChecksum) {
  uint64_t num_writes = 100;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));

  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  Status s;
  while (MonoTime::Now() < deadline) {
    ASSERT_OK(ksck_->CheckMasterHealth());
    ASSERT_OK(ksck_->CheckMasterUnusualFlags());
    ASSERT_OK(ksck_->CheckMasterConsensus());
    ASSERT_OK(ksck_->CheckClusterRunning());
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
    ASSERT_OK(ksck_->FetchInfoFromTabletServers());

    err_stream_.str("");
    s = ksck_->ChecksumData(KsckChecksumOptions(MonoDelta::FromSeconds(1),
                                                MonoDelta::FromSeconds(1),
                                                16, false, 0));
    if (s.ok()) {
      // Check the status message at the end of the checksum.
      // We expect '0B from disk' because we didn't write enough data to trigger a flush
      // in this short-running test.
      ASSERT_STR_CONTAINS(err_stream_.str(),
                          AllowSlowTests() ?
                          "0/30 replicas remaining (0B from disk, 300 rows summed)" :
                          "0/9 replicas remaining (0B from disk, 300 rows summed)");
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_OK(s);
}

TEST_F(RemoteKsckTest, TestChecksumTimeout) {
  uint64_t num_writes = 10000;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  // Use an impossibly low timeout value of zero!
  Status s = ksck_->ChecksumData(KsckChecksumOptions(MonoDelta::FromNanoseconds(0),
                                                     MonoDelta::FromSeconds(5),
                                                     16, false, 0));
  ASSERT_TRUE(s.IsTimedOut()) << "Expected TimedOut Status, got: " << s.ToString();
}

TEST_F(RemoteKsckTest, TestChecksumSnapshot) {
  CountDownLatch started_writing(1);
  AtomicBool continue_writing(true);
  Promise<Status> promise;

  // Allow the checksum scan to wait for longer in case it takes a while for the
  // writer thread to advance safe time.
  FLAGS_scanner_max_wait_ms = 10000;

  thread writer_thread([&]() {
    this->GenerateRowWritesLoop(&started_writing, continue_writing, &promise);
  });
  {
    SCOPED_CLEANUP({
      continue_writing.Store(false);
      writer_thread.join();
    });
    started_writing.Wait();

    // The writer thread may have errored out before actually writing anything;
    // check for this before proceeding with the test.
    const auto* s = promise.WaitFor(MonoDelta::FromSeconds(0));
    if (s) {
      ASSERT_OK(*s);
    }

    uint64_t ts = client_->GetLatestObservedTimestamp();
    ASSERT_OK(ksck_->CheckMasterHealth());
    ASSERT_OK(ksck_->CheckMasterUnusualFlags());
    ASSERT_OK(ksck_->CheckMasterConsensus());
    ASSERT_OK(ksck_->CheckClusterRunning());
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
    ASSERT_OK(ksck_->FetchInfoFromTabletServers());
    ASSERT_OK(ksck_->ChecksumData(KsckChecksumOptions(MonoDelta::FromSeconds(10),
                                                      MonoDelta::FromSeconds(10),
                                                      16, true, ts)));
  }
  ASSERT_OK(promise.Get());
}

// Test that followers & leader wait until safe time to respond to a snapshot
// scan at current timestamp.
TEST_F(RemoteKsckTest, TestChecksumSnapshotCurrentTimestamp) {
  CountDownLatch started_writing(1);
  AtomicBool continue_writing(true);
  Promise<Status> promise;

  // Allow the checksum scan to wait for longer in case it takes a while for the
  // writer thread to advance safe time.
  FLAGS_scanner_max_wait_ms = 10000;

  thread writer_thread([&]() {
    this->GenerateRowWritesLoop(&started_writing, continue_writing, &promise);
  });
  {
    SCOPED_CLEANUP({
      continue_writing.Store(false);
      writer_thread.join();
    });
    started_writing.Wait();

    // The writer thread may have errored out before actually writing anything;
    // check for this before proceeding with the test.
    const auto* s = promise.WaitFor(MonoDelta::FromSeconds(0));
    if (s) {
      ASSERT_OK(*s);
    }

    ASSERT_OK(ksck_->CheckMasterHealth());
    ASSERT_OK(ksck_->CheckMasterUnusualFlags());
    ASSERT_OK(ksck_->CheckMasterConsensus());
    ASSERT_OK(ksck_->CheckClusterRunning());
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
    ASSERT_OK(ksck_->FetchInfoFromTabletServers());
    // It's possible for scans to fail because the tablets haven't been written
    // to yet and haven't elected a leader.
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(ksck_->ChecksumData(KsckChecksumOptions(MonoDelta::FromSeconds(10),
                                                        MonoDelta::FromSeconds(10),
                                                        16, true,
                                                        KsckChecksumOptions::kCurrentTimestamp)));
    });
  }
  ASSERT_OK(promise.Get());
}

// Regression test for KUDU-2179: If the checksum process takes long enough that
// the snapshot timestamp falls behind the ancient history mark, new replica
// checksums will fail.
TEST_F(RemoteKsckTest, TestChecksumSnapshotLastingLongerThanAHM) {
  // This test is really slow because -tablet_history_max_age_sec's lowest
  // acceptable value is 1.
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  // This test relies on somewhat precise timing: the timestamp update must
  // happen during the wait to start the checksum, for each tablet. It's likely
  // this sometimes won't happen in builds that are slower, so we'll just
  // disable the test for those builds.
  #if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
    LOG(WARNING) << "test is skipped in TSAN and ASAN builds";
    return;
  #endif

  // Write something so we have rows to checksum, and because we need a valid
  // timestamp from the client to use for a checksum scan.
  constexpr uint64_t num_writes = 100;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));

  // Update timestamps frequently.
  FLAGS_timestamp_update_period_ms = 200;
  // Keep history for 1 second. This means snapshot scans with a timestamp older
  // than 1 second will be rejected.
  FLAGS_tablet_history_max_age_sec = 1;
  // Wait for the AHM to pass before assigning a timestamp.
  FLAGS_wait_before_setting_snapshot_timestamp_ms = 1100;

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckMasterConsensus());
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());

  // Run a checksum scan at the latest timestamp known to the client. This
  // should fail, since we will wait until after the AHM has passed to start
  // any scans.
  constexpr int timeout_sec = 30;
  constexpr int scan_concurrency = 16;
  constexpr bool use_snapshot = true;
  uint64_t ts = client_->GetLatestObservedTimestamp();
  Status s = ksck_->ChecksumData(KsckChecksumOptions(
        MonoDelta::FromSeconds(timeout_sec),
        MonoDelta::FromSeconds(timeout_sec),
        scan_concurrency,
        use_snapshot,
        ts));
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Invalid argument: snapshot scan end timestamp is earlier than "
                      "the ancient history mark.");

  // Now let's try again using the special current timestamp, which will run
  // checksums using timestamps updated from the servers, and should succeed.
  ASSERT_OK(ksck_->ChecksumData(KsckChecksumOptions(
      MonoDelta::FromSeconds(timeout_sec),
      MonoDelta::FromSeconds(timeout_sec),
      scan_concurrency,
      use_snapshot,
      KsckChecksumOptions::kCurrentTimestamp)));
}

TEST_F(RemoteKsckTest, TestLeaderMasterDown) {
  // Make sure ksck's client is created with the current leader master and that
  // all masters are healthy.
  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckMasterConsensus());
  ASSERT_OK(ksck_->CheckClusterRunning());

  // Shut down the leader master.
  int leader_idx;
  ASSERT_OK(mini_cluster_->GetLeaderMasterIndex(&leader_idx));
  mini_cluster_->mini_master(leader_idx)->Shutdown();

  // Check that the bad master health is detected.
  ASSERT_TRUE(ksck_->CheckMasterHealth().IsNetworkError());

  // Try to ksck. The underlying client will need to find the new leader master
  // in order for the test to pass.
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
}

TEST_F(RemoteKsckTest, TestClusterWithLocation) {
  const string location_cmd_path = JoinPathSegments(GetTestExecutableDirectory(),
                                                   "testdata/first_argument.sh");
  const string location = "/foo";
  FLAGS_location_mapping_cmd = Substitute("$0 $1", location_cmd_path, location);

  // After setting the --location_mapping_cmd flag it's necessary to restart
  // the masters to pick up the new value.
  for (auto idx = 0; idx < mini_cluster_->num_masters(); ++idx) {
    auto* master = mini_cluster_->mini_master(idx);
    master->Shutdown();
    ASSERT_OK(master->Start());
  }

  ASSERT_OK(mini_cluster_->AddTabletServer());
  ASSERT_EQ(4, mini_cluster_->num_tablet_servers());

  // In case of TSAN builds and running the test at inferior machines
  // with lot of concurrent activity, the masters and tablet servers run Raft
  // re-elections from time to time. Also, establishing and negotiation
  // a connection takes much longer. To avoid flakiness of this test scenario,
  // few calls below are wrapped into ASSERT_EVENTUALLY().
  //
  // TODO(KUDU-2709): remove ASSERT_EVENTUALLY around CheckMasterConsensus
  //                  when KUDU-2709 is addressed.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(ksck_->CheckMasterHealth()); // Need to refresh master cstate.
    ASSERT_OK(ksck_->CheckMasterConsensus());
  });
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(ksck_->CheckClusterRunning());
  });
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  });
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  });

  ASSERT_OK(ksck_->PrintResults());
  const string& err_string = err_stream_.str();

  // With the flag set and masters restarted, all tablet servers are assigned
  // with location '/foo': both the existing ones and the newly added.
  for (int idx = 0; idx < mini_cluster_->num_tablet_servers(); ++idx) {
    auto *ts = mini_cluster_->mini_tablet_server(idx);
    ASSERT_STR_CONTAINS(err_string, Substitute("$0 | $1 | HEALTHY | $2",
                                               ts->uuid(),
                                               ts->bound_rpc_addr().ToString(),
                                               location));
  }
  ASSERT_STR_CONTAINS(err_string,
    "Tablet Server Location Summary\n"
    " Location |  Count\n"
    "----------+---------\n");
  ASSERT_STR_CONTAINS(err_string,
    " /foo     |       4\n");
}

// Test filtering on a cluster with no table.
TEST_F(RemoteKsckTest, TestFilterOnNoTableCluster) {
  client_->DeleteTable(kTableName);
  cluster_->set_table_filters({ "ksck-test-table" });
  FLAGS_checksum_scan = true;
  FLAGS_consensus = false;
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tables");
}

// Test filtering on a non-matching table pattern.
TEST_F(RemoteKsckTest, TestNonMatchingTableFilter) {
  cluster_->set_table_filters({ "fake-table" });
  FLAGS_checksum_scan = true;
  FLAGS_consensus = false;
  ASSERT_TRUE(ksck_->RunAndPrintResults().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  EXPECT_EQ("Not found: checksum scan error: No table found. Filter: table_filters=fake-table",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tables");
}

// Test filtering with a matching table pattern.
TEST_F(RemoteKsckTest, TestMatchingTableFilter) {
  uint64_t num_writes = 100;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));

  cluster_->set_table_filters({ "ksck-te*" });
  FLAGS_checksum_scan = true;
  FLAGS_consensus = false;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  Status s;
  while (MonoTime::Now() < deadline) {
    ksck_.reset(new Ksck(cluster_, &err_stream_));
    s = ksck_->RunAndPrintResults();
    if (s.ok()) {
      // Check the status message at the end of the checksum.
      // We expect '0B from disk' because we didn't write enough data to trigger a flush
      // in this short-running test.
      ASSERT_STR_CONTAINS(err_stream_.str(),
                          AllowSlowTests() ?
                          "0/30 replicas remaining (0B from disk, 300 rows summed)" :
                          "0/9 replicas remaining (0B from disk, 300 rows summed)");
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_OK(s);
}

// Test filtering on a table with no tablet.
TEST_F(RemoteKsckTest, TestFilterOnNotabletTable) {
  NO_FATALS(CreateTableWithNoTablet("other-table"));
  cluster_->set_table_filters({ "other-table" });
  FLAGS_checksum_scan = true;
  FLAGS_consensus = false;
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tablets");
}

// Test filtering on a non-matching tablet id pattern.
TEST_F(RemoteKsckTest, TestNonMatchingTabletIdFilter) {
  cluster_->set_tablet_id_filters({ "tablet-id-fake" });
  FLAGS_checksum_scan = true;
  FLAGS_consensus = false;
  ASSERT_TRUE(ksck_->RunAndPrintResults().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  EXPECT_EQ(
      "Not found: checksum scan error: No tablet replicas found. "
      "Filter: tablet_id_filters=tablet-id-fake",
      error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tablets");
}

// Test filtering with a matching tablet ID pattern.
TEST_F(RemoteKsckTest, TestMatchingTabletIdFilter) {
  uint64_t num_writes = 300;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));

  vector<string> tablet_ids;
  ASSERT_OK(GetTabletIds(&tablet_ids));
  ASSERT_EQ(tablet_ids.size(), AllowSlowTests() ? 10 : 3);

  cluster_->set_tablet_id_filters({ tablet_ids[0] });
  FLAGS_checksum_scan = true;
  FLAGS_consensus = false;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  Status s;
  while (MonoTime::Now() < deadline) {
    ksck_.reset(new Ksck(cluster_, &err_stream_));
    s = ksck_->RunAndPrintResults();
    if (s.ok()) {
      // Check the status message at the end of the checksum.
      // We expect '0B from disk' because we didn't write enough data to trigger a flush
      // in this short-running test.
      ASSERT_STR_CONTAINS(err_stream_.str(),
                          "0/3 replicas remaining (0B from disk, 30 rows summed)");
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_OK(s);
}

TEST_F(RemoteKsckTest, TestTableFiltersNoMatch) {
  cluster_->set_table_filters({ "fake-table" });
  FLAGS_consensus = false;
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(), "The cluster doesn't have any matching tables");
  ASSERT_STR_NOT_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n");
}

TEST_F(RemoteKsckTest, TestTableFilters) {
  NO_FATALS(CreateTableWithNoTablet("other-table"));
  cluster_->set_table_filters({ "ksck-test-table" });
  FLAGS_consensus = false;
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      AllowSlowTests() ?
      "                | Total Count\n"
      "----------------+-------------\n"
      " Masters        | 3\n"
      " Tablet Servers | 3\n"
      " Tables         | 1\n"
      " Tablets        | 10\n"
      " Replicas       | 30\n" :
      "                | Total Count\n"
      "----------------+-------------\n"
      " Masters        | 3\n"
      " Tablet Servers | 3\n"
      " Tables         | 1\n"
      " Tablets        | 3\n"
      " Replicas       | 9\n");
}

TEST_F(RemoteKsckTest, TestTabletFiltersNoMatch) {
  cluster_->set_tablet_id_filters({ "tablet-id-fake" });
  FLAGS_consensus = false;
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(), "The cluster doesn't have any matching tablets");
  ASSERT_STR_NOT_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n");
}

TEST_F(RemoteKsckTest, TestTabletFilters) {
  vector<string> tablet_ids;
  ASSERT_OK(GetTabletIds(&tablet_ids));
  ASSERT_EQ(tablet_ids.size(), AllowSlowTests() ? 10 : 3);

  cluster_->set_tablet_id_filters({ tablet_ids[0] });
  FLAGS_consensus = false;
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n"
      " Masters        | 3\n"
      " Tablet Servers | 3\n"
      " Tables         | 1\n"
      " Tablets        | 1\n"
      " Replicas       | 3\n");
}

} // namespace tools
} // namespace kudu
