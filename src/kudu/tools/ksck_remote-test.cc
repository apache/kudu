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

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/core/ref.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/promise.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {
namespace tools {

using client::KuduColumnSchema;
using client::KuduInsert;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableCreator;
using client::sp::shared_ptr;
using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;

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

    // Hard-coded ports for the masters. This is safe, as these tests run under
    // a resource lock (see CMakeLists.txt in this directory).
    // TODO we should have a generic method to obtain n free ports.
    opts.master_rpc_ports = { 11010, 11011, 11012 };

    opts.num_masters = opts.master_rpc_ports.size();
    opts.num_tablet_servers = 3;
    mini_cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(mini_cluster_->Start());

    // Connect to the cluster.
    ASSERT_OK(mini_cluster_->CreateClient(nullptr, &client_));

    // Create one table.
    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
                     .schema(&schema_)
                     .num_replicas(3)
                     .set_range_partition_columns({ "key" })
                     .split_rows(GenerateSplitRows())
                     .Create());
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
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
}

TEST_F(RemoteKsckTest, TestTabletServerMismatchedUUID) {
  ASSERT_OK(ksck_->CheckMasterHealth());
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

  string match_string = "Remote error: ID reported by tablet server "
                        "($0) doesn't match the expected ID: $1";
  ASSERT_STR_CONTAINS(err_stream_.str(), strings::Substitute(match_string, new_uuid, old_uuid));
}

TEST_F(RemoteKsckTest, TestTableConsistency) {
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  Status s;
  while (MonoTime::Now() < deadline) {
    ASSERT_OK(ksck_->CheckMasterHealth());
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
    ASSERT_OK(ksck_->CheckClusterRunning());
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
    ASSERT_OK(ksck_->FetchInfoFromTabletServers());

    err_stream_.str("");
    s = ksck_->ChecksumData(ChecksumOptions(MonoDelta::FromSeconds(1), 16, false, 0));
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
  Status s = ksck_->ChecksumData(ChecksumOptions(MonoDelta::FromNanoseconds(0), 16, false, 0));
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
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->ChecksumData(ChecksumOptions(MonoDelta::FromSeconds(10), 16, true, ts)));
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
  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->ChecksumData(ChecksumOptions(MonoDelta::FromSeconds(10), 16, true,
                                                ChecksumOptions::kCurrentTimestamp)));
  continue_writing.Store(false);
  ASSERT_OK(promise.Get());
  writer_thread->Join();
}

TEST_F(RemoteKsckTest, TestLeaderMasterDown) {
  // Make sure ksck's client is created with the current leader master and that
  // all masters are healthy.
  ASSERT_OK(ksck_->CheckMasterHealth());
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

} // namespace tools
} // namespace kudu
