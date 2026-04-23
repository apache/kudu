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

#include "kudu/mini-cluster/external_mini_cluster.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h> // IWYU pragma: keep
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h" // IWYU pragma: keep
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_histogram(log_gc_duration);
METRIC_DECLARE_entity(tablet);

using kudu::client::sp::shared_ptr;
using rapidjson::Value;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cluster {

enum class Kerberos {
  DISABLED,
  ENABLED,
};

enum class HiveMetastore {
  DISABLED,
  ENABLED,
};

enum BuiltInNtp {
  DISABLED = 0,
  ENABLED_SINGLE_SERVER = 1,
  ENABLED_MULTIPLE_SERVERS = 5,
};

// Beautifies test output if a test scenario fails.
std::ostream& operator<<(std::ostream& o, Kerberos opt) {
  switch (opt) {
    case Kerberos::ENABLED:
      return o << "Kerberos::ENABLED";
    case Kerberos::DISABLED:
      return o << "Kerberos::DISABLED";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, HiveMetastore opt) {
  switch (opt) {
    case HiveMetastore::ENABLED:
      return o << "HiveMetastore::ENABLED";
    case HiveMetastore::DISABLED:
      return o << "HiveMetastore::DISABLED";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, BuiltInNtp opt) {
  switch (opt) {
    case BuiltInNtp::DISABLED:
      return o << "BuiltInNtp::DISABLED";
    case BuiltInNtp::ENABLED_SINGLE_SERVER:
      return o << "BuiltInNtp::ENABLED_SINGLE_SERVER";
    case BuiltInNtp::ENABLED_MULTIPLE_SERVERS:
      return o << "BuiltInNtp::ENABLED_MULTIPLE_SERVERS";
  }
  return o;
}

class ExternalMiniClusterTest :
    public KuduTest,
#if !defined(NO_CHRONY)
    public testing::WithParamInterface<tuple<Kerberos, HiveMetastore, BuiltInNtp>>
#else
    public testing::WithParamInterface<tuple<Kerberos, HiveMetastore>>
#endif
{
};

INSTANTIATE_TEST_SUITE_P(,
    ExternalMiniClusterTest,
    ::testing::Combine(
        ::testing::Values(Kerberos::DISABLED, Kerberos::ENABLED),
        ::testing::Values(HiveMetastore::DISABLED, HiveMetastore::ENABLED)
#if !defined(NO_CHRONY)
        ,
        ::testing::Values(BuiltInNtp::DISABLED,
                          BuiltInNtp::ENABLED_SINGLE_SERVER,
                          BuiltInNtp::ENABLED_MULTIPLE_SERVERS)
#endif // #if !defined(NO_CHRONY) ...
                          ));

void SmokeTestKerberizedCluster(ExternalMiniClusterOptions opts) {
  ASSERT_TRUE(opts.enable_kerberos);
  int num_tservers = opts.num_tablet_servers;

  ExternalMiniCluster cluster(std::move(opts));
  ASSERT_OK(cluster.Start());

  // Sleep long enough to ensure that the tserver's ticket would have expired
  // if not for the renewal thread doing its thing.
  SleepFor(MonoDelta::FromSeconds(16));

  // Re-kinit for the client, since the client's ticket would have expired as well
  // since the renewal thread doesn't run for the test client.
  ASSERT_OK(cluster.kdc()->Kinit("test-admin"));

  // Restart the master, and make sure the tserver is still able to reconnect and
  // authenticate.
  cluster.master(0)->Shutdown();
  ASSERT_OK(cluster.master(0)->Restart());
  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster.WaitForTabletServerCount(num_tservers, MonoDelta::FromSeconds(30)));
  cluster.Shutdown();
}

void SmokeExternalMiniCluster(const ExternalMiniClusterOptions& opts,
                              ExternalMiniCluster* cluster,
                              vector<HostPort>* master_rpc_addresses) {
  CHECK(cluster);
  CHECK(master_rpc_addresses);
  master_rpc_addresses->clear();

  // Verify each of the masters.
  for (int i = 0; i < opts.num_masters; i++) {
    SCOPED_TRACE(i);
    ExternalMaster* master = CHECK_NOTNULL(cluster->master(i));
    HostPort master_endpoint = master->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster->GetBindIpForMaster(i));
    if (cluster->bind_mode() == BindMode::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(master_endpoint.ToString(), expected_prefix))
        << master_endpoint.ToString();

    HostPort master_http = master->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(master_http.ToString(), expected_prefix)) << master_http.ToString();
    master_rpc_addresses->emplace_back(std::move(master_endpoint));
  }

  // Verify each of the tablet servers.
  for (int i = 0; i < opts.num_tablet_servers; i++) {
    SCOPED_TRACE(i);
    ExternalTabletServer* ts = CHECK_NOTNULL(cluster->tablet_server(i));
    HostPort ts_rpc = ts->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster->GetBindIpForTabletServer(i));
    if (cluster->bind_mode() == BindMode::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(ts_rpc.ToString(), expected_prefix)) << ts_rpc.ToString();

    HostPort ts_http = ts->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(ts_http.ToString(), expected_prefix)) << ts_http.ToString();
  }

  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster->WaitForTabletServerCount(opts.num_tablet_servers, MonoDelta::FromSeconds(30)));

  // Restart a master and a tablet server. Make sure they come back up with the same ports.
  ExternalMaster* master = cluster->master(0);
  HostPort master_rpc = master->bound_rpc_hostport();
  HostPort master_http = master->bound_http_hostport();

  master->Shutdown();
  ASSERT_OK(master->Restart());

  ASSERT_EQ(master_rpc.ToString(), master->bound_rpc_hostport().ToString());
  ASSERT_EQ(master_http.ToString(), master->bound_http_hostport().ToString());

  ExternalTabletServer* ts = cluster->tablet_server(0);

  HostPort ts_rpc = ts->bound_rpc_hostport();
  HostPort ts_http = ts->bound_http_hostport();

  ts->Shutdown();
  ASSERT_OK(ts->Restart());

  ASSERT_EQ(ts_rpc.ToString(), ts->bound_rpc_hostport().ToString());
  ASSERT_EQ(ts_http.ToString(), ts->bound_http_hostport().ToString());

  // Verify that the HMS is reachable.
  if (opts.hms_mode == HmsMode::ENABLE_HIVE_METASTORE) {
    thrift::ClientOptions hms_client_opts;
    hms_client_opts.enable_kerberos = opts.enable_kerberos;
    hms_client_opts.service_principal = "hive";
    hms::HmsClient hms_client(cluster->hms()->address(), hms_client_opts);
    ASSERT_OK(hms_client.Start());
    vector<string> tables;
    ASSERT_OK(hms_client.GetTableNames("default", &tables));
    ASSERT_TRUE(tables.empty()) << "tables: " << tables;
  }

  // Verify that, in a Kerberized cluster, if we drop our Kerberos environment,
  // we can't make RPCs to a server.
  if (opts.enable_kerberos) {
    ASSERT_OK(cluster->kdc()->Kdestroy());
    Status s = cluster->SetFlag(ts, "foo", "bar");
    // The error differs depending on the version of Kerberos, so we match
    // either message.
    ASSERT_STR_CONTAINS(s.ToString(),
                        "server requires authentication, "
                        "but client does not have Kerberos credentials available");
  }

  // ExternalTabletServer::Restart() still returns OK, even if the tablet server crashed.
  ts->Shutdown();
  ts->mutable_flags()->push_back("--fault_before_start=1.0");
  ASSERT_OK(ts->Restart());
  ASSERT_FALSE(ts->IsProcessAlive());
  // Since the process should have already crashed, waiting for an injected crash with no
  // timeout should still return OK.
  ASSERT_OK(ts->WaitForInjectedCrash(MonoDelta::FromSeconds(0)));
  ts->mutable_flags()->pop_back();
  ts->Shutdown();
  ASSERT_OK(ts->Restart());
  ASSERT_TRUE(ts->IsProcessAlive());
}

TEST_F(ExternalMiniClusterTest, TestKerberosReacquire) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  // Set the kerberos ticket lifetime as 15 seconds to force ticket reacquisition every 15 seconds.
  // Note that we do not renew tickets but always acquire a new one.
  opts.mini_kdc_options.ticket_lifetime = "15s";
  opts.num_tablet_servers = 1;

  NO_FATALS(SmokeTestKerberizedCluster(std::move(opts)));
}

TEST_P(ExternalMiniClusterTest, TestBasicOperation) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  const auto& param = GetParam();
  opts.enable_kerberos = std::get<0>(param) == Kerberos::ENABLED;
  if (std::get<1>(param) == HiveMetastore::ENABLED) {
    opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  }
#if !defined(NO_CHRONY)
  opts.num_ntp_servers = std::get<2>(param);
#endif

  opts.num_masters = 3;
  opts.num_tablet_servers = 3;

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());
  vector<HostPort> master_rpc_addresses;
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));

  // Destroy the cluster object, create a new one with the same options,
  // and run the same scenario again at already existing data directory
  // structure.
  // This is to make sure that:
  //   * the cluster's components are shutdown upon the destruction
  //     of the object
  //   * configuration files and other persistent data for cluster components
  //     are either reused or rewritten/recreated in consistent manner
  // The only cluster options to preserve is the masters' RPC addresses from
  // the prior run.
  opts.master_rpc_addresses = master_rpc_addresses;
  cluster.reset(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));
  ASSERT_EQ(opts.master_rpc_addresses, master_rpc_addresses);

  // Shutdown the cluster explicitly. This is not strictly necessary since
  // the cluster will be shutdown upon the call of ExternalMiniCluster's
  // destructor, but this is done in the context of testing. This is to verify
  // that ExternalMiniCluster object destructor works as expected in the case
  // if the cluster has already been shutdown.
  cluster->Shutdown();
}

TEST_P(ExternalMiniClusterTest, TestAddMaster) {
  ExternalMiniClusterOptions opts;
  const auto& param = GetParam();
  opts.enable_kerberos = std::get<0>(param) == Kerberos::ENABLED;
  if (std::get<1>(param) == HiveMetastore::ENABLED) {
    opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  }
#if !defined(NO_CHRONY)
  opts.num_ntp_servers = std::get<2>(param);
#endif

  opts.num_masters = 3;
  opts.num_tablet_servers = 1;

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());

  // Add a master and wait for it to start up and get reported to.
  ASSERT_OK(cluster->AddMaster());
  ASSERT_OK(cluster->master(opts.num_masters)->WaitForCatalogManager());
  cluster->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster->tablet_server(0)->Restart());
  ASSERT_OK(cluster->WaitForTabletServerCount(
      opts.num_tablet_servers, MonoDelta::FromSeconds(30), /*master_idx*/opts.num_masters));

  // Smoke the cluster using an updated 'opts' that expects the new number of
  // masters.
  opts.num_masters++;
  vector<HostPort> master_rpc_addresses;
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));

  // Shutdown the cluster explicitly on top of the one in the cluster's
  // destructor to test repeated calls to Shutdown().
  cluster->Shutdown();
}

class Kudu3762Test : public ExternalMiniClusterTest {};
// Verifies that a new Master can join the cluster when its initial log index (0)
// is already garbage collected on the leader. This specifically tests the fix
// for KUDU-3762, ensuring a newly joining peer isn't prematurely flagged as
// unrecoverable (wal_catchup_possible = false) while its log index is still 0
// during initialization.
TEST_F(Kudu3762Test, AddMaster) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;

  opts.num_masters = 2;
  opts.num_tablet_servers = 3;
  // We need at least one WAL segment to be garbage collected to reproduce the issue. So
  // ensure we flush to the disk aggressively while retaining lower number of smaller sized
  // log segments. More number of maintenance manager threads with low polling intervals
  // increase the odd of the WAL segment getting garbage collected
  opts.extra_master_flags.emplace_back("--flush_threshold_mb=0");
  opts.extra_master_flags.emplace_back("--flush_threshold_secs=1");
  opts.extra_master_flags.emplace_back("--log_cache_size_limit_mb=1");
  opts.extra_master_flags.emplace_back("--log_compression_codec=no_compression");
  opts.extra_master_flags.emplace_back("--log_max_segments_to_retain=3");
  opts.extra_master_flags.emplace_back("--log_segment_size_mb=1");
  opts.extra_master_flags.emplace_back("--maintenance_manager_num_threads=4");
  opts.extra_master_flags.emplace_back("--maintenance_manager_polling_interval_ms=10");
  opts.extra_master_flags.emplace_back("--unlock_experimental_flags=true");

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());

  // Create a client to add and delete a table in loop to constantly add new entries/indices
  // in the WAL of system catalog
  client::KuduSchema schema;
  {
    client::KuduSchemaBuilder builder;
    builder.AddColumn("key")->NotNull()->Type(client::KuduColumnSchema::INT64)->PrimaryKey();
    builder.AddColumn("col")->Nullable()->Type(client::KuduColumnSchema::INT64);
    ASSERT_OK(builder.Build(&schema));
  }

  shared_ptr<client::KuduClient> new_client;
  ASSERT_OK(cluster->CreateClient(nullptr, &new_client));
  std::atomic<bool> stop_churn(false);
  Status churn_status = Status::OK();
  int64_t num_wal_gc_count = 0;

  std::thread churn_thread([&]() {
    for (int i = 0; !stop_churn.load(); ++i) {
      string table_name = Substitute("table-$0", i);
      unique_ptr<client::KuduTableCreator> table_creator(new_client->NewTableCreator());

      // Create a table and wait for its creation to finish
      auto run_step = [&]() -> Status {
        RETURN_NOT_OK(table_creator->table_name(table_name)
                     .schema(&schema)
                     .num_replicas(3)
                     .add_hash_partitions({"key"}, 16)
                     .Create());

        bool in_progress = true;
        MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
        while (in_progress && MonoTime::Now() < deadline) {
          RETURN_NOT_OK(new_client->IsCreateTableInProgress(table_name, &in_progress));
          if (in_progress) {
            SleepFor(MonoDelta::FromMilliseconds(100));
          }
        }
        if (in_progress) {
          return Status::TimedOut("Table creation stuck");
        }
        return new_client->DeleteTable(table_name);
      };

      if (Status s = run_step(); !s.ok()) {
        churn_status = s;
        stop_churn = true;
        break;
      }
    }
  });

  auto cleanup = MakeScopedCleanup([&](){
    stop_churn = true;
    if (churn_thread.joinable()) {
      churn_thread.join();
    }
  });

  // Wait a maximum of two minutes for at least one WAL segment to be garbage collected
  AssertEventually([&]() {
    ASSERT_OK(itest::GetInt64Metric(cluster->leader_master()->bound_http_hostport(),
        &METRIC_ENTITY_tablet,kudu::master::SysCatalogTable::kSysCatalogTabletId,
        &METRIC_log_gc_duration,"total_count", &num_wal_gc_count));
    ASSERT_GT(num_wal_gc_count, 0);
  }, MonoDelta::FromSeconds(120));

  // Add a master and wait to see if the new master gets stuck in the LMP loop
  ASSERT_OK(cluster->AddMaster());
  ASSERT_OK(cluster->master(opts.num_masters)->WaitForCatalogManager());

  // Stop the continuous creation and deletion of tables
  cleanup.run();
  ASSERT_OK(churn_status);

  // The new master should be caught up and be promoted to VOTER
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(cluster->VerifyVotersOnAllMasters(opts.num_masters + 1));
  });
}

} // namespace cluster
} // namespace kudu
