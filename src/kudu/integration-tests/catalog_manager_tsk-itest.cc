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
#include <cstdlib>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::atomic;
using std::back_inserter;
using std::copy;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

class CatalogManagerTskITest : public KuduTest {
 public:
  CatalogManagerTskITest()
      : num_masters_(3),
        num_tservers_(1),
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
        hb_interval_ms_(32),
        run_time_seconds_(5)
#else
        hb_interval_ms_(16),
        run_time_seconds_(AllowSlowTests() ? 100 : 5)
#endif
        {
    cluster_opts_.num_masters = num_masters_;
    cluster_opts_.num_tablet_servers = num_tservers_;

    // Add master-only flags.
    const vector<string> master_flags = {
      "--catalog_manager_inject_latency_prior_tsk_write_ms=1000",
      "--raft_enable_pre_election=false",
      "--master_non_leader_masters_propagate_tsk",
      "--tsk_rotation_seconds=2",
    };
    copy(master_flags.begin(), master_flags.end(),
        back_inserter(cluster_opts_.extra_master_flags));

    // Add tserver-only flags.
    const vector<string> tserver_flags = {
      Substitute("--heartbeat_interval_ms=$0", hb_interval_ms_),
    };
    copy(tserver_flags.begin(), tserver_flags.end(),
        back_inserter(cluster_opts_.extra_tserver_flags));
  }

  void StartCluster() {
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    ASSERT_OK(cluster_->Start());
  }

  void SmokeTestCluster() {
    using ::kudu::client::sp::shared_ptr;
    static const char* kTableName = "test-table";
    // Using the setting for both RPC and admin operation timeout.
    const MonoDelta timeout = MonoDelta::FromSeconds(120);
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(timeout).default_rpc_timeout(timeout);
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(&builder, &client));

    // Create a table.
    auto schema = KuduSchema::FromSchema(CreateKeyValueTestSchema());
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());

    ASSERT_OK(table_creator->table_name(kTableName)
              .set_range_partition_columns({ "key" })
              .schema(&schema)
              .num_replicas(num_tservers_)
              .Create());

    // Insert a row.
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(kTableName, &table));
    unique_ptr<KuduInsert> ins(table->NewInsert());
    ASSERT_OK(ins->mutable_row()->SetInt32(0, 12345));
    ASSERT_OK(ins->mutable_row()->SetInt32(1, 54321));
    shared_ptr<KuduSession> session = client->NewSession();
    ASSERT_OK(session->Apply(ins.release()));
    FlushSessionOrDie(session);

    // Read it back.
    ASSERT_EQ(1, CountTableRows(table.get()));

    // Delete the table.
    ASSERT_OK(client->DeleteTable(kTableName));
  }

 protected:
  const int num_masters_;
  const int num_tservers_;
  const int hb_interval_ms_;
  const int64_t run_time_seconds_;
  ExternalMiniClusterOptions cluster_opts_;
  std::shared_ptr<ExternalMiniCluster> cluster_;
};

// Check that master servers do not crash on change of leadership while
// writing newly generated TSKs. The leadership changes are provoked
// by a separate thread which just forces each leader to call elections
// in turn, separated by random sleeps.
TEST_F(CatalogManagerTskITest, LeadershipChangeOnTskGeneration) {
  NO_FATALS(StartCluster());

  std::atomic<bool> done { false };
  std::thread t([&]() {
      // At the start of the test, cause leader elections rapidly,
      // but then space them out further and further as the test goes
      // to ensure that we eventually do get a successful run.
      double max_sleep_ms = 5;
      while (!done) {
        for (int i = 0; i < cluster_->num_masters() && !done; i++) {
          LOG(INFO) << "Attempting to promote master " << i << " to leader";
          consensus::ConsensusServiceProxy proxy(
              cluster_->messenger(), cluster_->master(i)->bound_rpc_addr(), "master");
          consensus::RunLeaderElectionRequestPB req;
          consensus::RunLeaderElectionResponsePB resp;
          rpc::RpcController rpc;
          req.set_tablet_id(master::SysCatalogTable::kSysCatalogTabletId);
          req.set_dest_uuid(cluster_->master(i)->uuid());
          rpc.set_timeout(MonoDelta::FromSeconds(10));
          WARN_NOT_OK(proxy.RunLeaderElection(req, &resp, &rpc),
                      "couldn't promote new leader");
          int s = rand() % static_cast<int>(max_sleep_ms);
          LOG(INFO) << "Sleeping for " << s;
          SleepFor(MonoDelta::FromMilliseconds(s));
          max_sleep_ms = std::min(max_sleep_ms * 1.1, 3000.0);
        }
      }
    });
  SCOPED_CLEANUP({ done = true; t.join(); });

  const MonoTime t_stop = MonoTime::Now() +
      MonoDelta::FromSeconds(run_time_seconds_);
  while (MonoTime::Now() < t_stop) {
    NO_FATALS(SmokeTestCluster());
    NO_FATALS(cluster_->AssertNoCrashes());
  }
  LOG(INFO) << "Done. Waiting on elector thread.";
}

} // namespace master
} // namespace kudu
