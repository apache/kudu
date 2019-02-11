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
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/core/ref.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_checksum.h"
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
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(safe_time_max_lag_ms);
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
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
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
    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
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
    for (int i = 0; i < mini_cluster_->num_masters(); i++) {
        master_addresses.push_back(
            mini_cluster_->mini_master(i)->bound_rpc_addr_str());
    }
    std::shared_ptr<KsckCluster> cluster;
    ASSERT_OK(RemoteKsckCluster::Build(master_addresses, &cluster));
    ksck_.reset(new Ksck(cluster, &err_stream_));
  }

  virtual void TearDown() OVERRIDE {
    if (mini_cluster_) {
      mini_cluster_->Shutdown();
      mini_cluster_.reset();
    }
    KuduTest::TearDown();
  }

  // Writes rows to the table until the continue_writing flag is set to false.
  //
  // Public for use with boost::bind.
  void GenerateRowWritesLoop(CountDownLatch* started_writing,
                             const AtomicBool& continue_writing,
                             Promise<Status>* promise) {
    shared_ptr<KuduTable> table;
    Status status;
    status = client_->OpenTable(kTableName, &table);
    if (!status.ok()) {
      promise->Set(status);
    }
    shared_ptr<KuduSession> session(client_->NewSession());
    session->SetTimeoutMillis(10000);
    status = session->SetFlushMode(KuduSession::MANUAL_FLUSH);
    if (!status.ok()) {
      promise->Set(status);
    }

    for (uint64_t i = 0; continue_writing.Load(); i++) {
      gscoped_ptr<KuduInsert> insert(table->NewInsert());
      GenerateDataForRow(table->schema(), i, &random_, insert->mutable_row());
      status = session->Apply(insert.release());
      if (!status.ok()) {
        promise->Set(status);
      }
      status = session->Flush();
      if (!status.ok()) {
        promise->Set(status);
      }
      // Wait for the first 100 writes so that it's very likely all replicas have committed a
      // message in each tablet; otherwise, safe time might not have been updated on all replicas
      // and some might refuse snapshot scans because of lag.
      if (i > 100) {
        started_writing->CountDown(1);
      }
    }
    promise->Set(Status::OK());
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

  unique_ptr<InternalMiniCluster> mini_cluster_;
  unique_ptr<Ksck> ksck_;
  shared_ptr<client::KuduClient> client_;

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
  scoped_refptr<Thread> writer_thread;

  Thread::Create("RemoteKsckTest", "TestChecksumSnapshot",
                 &RemoteKsckTest::GenerateRowWritesLoop, this,
                 &started_writing, boost::cref(continue_writing), &promise,
                 &writer_thread);
  CHECK(started_writing.WaitFor(MonoDelta::FromSeconds(30)));

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
  continue_writing.Store(false);
  ASSERT_OK(promise.Get());
  writer_thread->Join();
}

// Test that followers & leader wait until safe time to respond to a snapshot
// scan at current timestamp.
TEST_F(RemoteKsckTest, TestChecksumSnapshotCurrentTimestamp) {
  CountDownLatch started_writing(1);
  AtomicBool continue_writing(true);
  Promise<Status> promise;
  scoped_refptr<Thread> writer_thread;

  Thread::Create("RemoteKsckTest", "TestChecksumSnapshotCurrentTimestamp",
                 &RemoteKsckTest::GenerateRowWritesLoop, this,
                 &started_writing, boost::cref(continue_writing), &promise,
                 &writer_thread);
  CHECK(started_writing.WaitFor(MonoDelta::FromSeconds(30)));

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckMasterConsensus());
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->ChecksumData(KsckChecksumOptions(MonoDelta::FromSeconds(10),
                                                    MonoDelta::FromSeconds(10),
                                                    16, true,
                                                    KsckChecksumOptions::kCurrentTimestamp)));
  continue_writing.Store(false);
  ASSERT_OK(promise.Get());
  writer_thread->Join();
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

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  // In case of TSAN builds and running the test at inferior machines
  // with lot of concurrent activity, the masters and tablet servers run Raft
  // re-elections from time to time. Also, establishing and negotiation
  // a connection takes much longer. To avoid flakiness of this test scenario,
  // few calls below are wrapped into ASSERT_EVENTUALLY().
  //
  // TODO(KUDU-2709): remove ASSERT_EVENTUALLY around CheckMasterConsensus
  //                  when KUDU-2709 is addressed.
  ASSERT_EVENTUALLY([&]() {
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

} // namespace tools
} // namespace kudu
