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
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
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
    cluster_opts_.master_rpc_ports = { 11030, 11031, 11032 };
    cluster_opts_.num_tablet_servers = num_tservers_;

    // Add common flags for both masters and tservers.
    const vector<string> common_flags = {
      Substitute("--raft_heartbeat_interval_ms=$0", hb_interval_ms_),
    };
    copy(common_flags.begin(), common_flags.end(),
        back_inserter(cluster_opts_.extra_master_flags));
    copy(common_flags.begin(), common_flags.end(),
        back_inserter(cluster_opts_.extra_tserver_flags));

    // Add master-only flags.
    const vector<string> master_flags = {
      "--catalog_manager_inject_latency_prior_tsk_write_ms=1000",
      "--raft_enable_pre_election=false",
      Substitute("--leader_failure_exp_backoff_max_delta_ms=$0",
          hb_interval_ms_ * 4),
      "--leader_failure_max_missed_heartbeat_periods=1.0",
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
    const MonoDelta timeout = MonoDelta::FromSeconds(600);
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(timeout).default_rpc_timeout(timeout);
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(&builder, &client));

    // Create a table.
    KuduSchema schema = client::KuduSchemaFromSchema(CreateKeyValueTestSchema());
    gscoped_ptr<KuduTableCreator> table_creator(client->NewTableCreator());

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
// by the injected latency just after generating a TSK but prior to writing it
// into the system table: setting --leader_failure_max_missed_heartbeat_periods
// flag to just one heartbeat period and unsetting --raft_enable_pre_election
// gives high chances of re-election to happen while current leader has blocked
// its leadership-related activity.
TEST_F(CatalogManagerTskITest, LeadershipChangeOnTskGeneration) {
  NO_FATALS(StartCluster());

  const MonoTime t_stop = MonoTime::Now() +
      MonoDelta::FromSeconds(run_time_seconds_);
  while (MonoTime::Now() < t_stop) {
    NO_FATALS(SmokeTestCluster());
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

} // namespace master
} // namespace kudu
