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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/util/env.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::MiniCluster;
using kudu::cluster::ScopedResumeExternalDaemon;
using kudu::master::SysCatalogTable;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class MasterMigrationTest : public KuduTest {
};

static Status CreateTable(ExternalMiniCluster* cluster,
                          const std::string& table_name) {
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(cluster->CreateClient(nullptr, &client));
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  RETURN_NOT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  return table_creator->table_name(table_name)
      .schema(&schema)
      .set_range_partition_columns({ "key" })
      .num_replicas(1)
      .Create();
}

// Tests migration of a deployment from one master to multiple masters.
TEST_F(MasterMigrationTest, TestEndToEndMigration) {
  const int kNumMasters = 3;

  // Collect and keep alive the set of master sockets bound with SO_REUSEPORT.
  // This allows the ports to be reserved up front, so that they won't be taken
  // while the test restarts nodes.
  vector<unique_ptr<Socket>> reserved_sockets;
  vector<HostPort> master_rpc_addresses;
  for (int i = 0; i < kNumMasters; i++) {
    unique_ptr<Socket> reserved_socket;
    ASSERT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, i,
                                               kDefaultBindMode,
                                               &reserved_socket));
    Sockaddr addr;
    ASSERT_OK(reserved_socket->GetSocketAddress(&addr));
    master_rpc_addresses.emplace_back(addr.host(), addr.port());
    reserved_sockets.emplace_back(std::move(reserved_socket));
  }

  ExternalMiniClusterOptions opts;
  opts.num_masters = 1;
  opts.master_rpc_addresses = { master_rpc_addresses[0] };
  opts.bind_mode = BindMode::LOOPBACK;

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());

  const string kTableName = "test";
  const string kBinPath = cluster->GetBinaryPath("kudu");

  // Initial state: single-master cluster with one table.
  ASSERT_OK(CreateTable(cluster.get(), kTableName));
  cluster->Shutdown();

  // List of every master UUIDs.
  vector<string> uuids = { cluster->master()->uuid() };

  // Format a filesystem tree for each of the new masters and get the uuids.
  for (int i = 1; i < kNumMasters; i++) {
    string data_root = cluster->GetDataPath(Substitute("master-$0", i));
    string wal_dir = cluster->GetWalPath(Substitute("master-$0", i));
    ASSERT_OK(env_->CreateDir(DirName(data_root)));
    ASSERT_OK(env_->CreateDir(wal_dir));
    {
      vector<string> args = {
          kBinPath,
          "fs",
          "format",
          "--fs_wal_dir=" + wal_dir,
          "--fs_data_dirs=" + data_root
      };
      ASSERT_OK(Subprocess::Call(args));
    }
    {
      vector<string> args = {
          kBinPath,
          "fs",
          "dump",
          "uuid",
          "--fs_wal_dir=" + wal_dir,
          "--fs_data_dirs=" + data_root
      };
      string uuid;
      ASSERT_OK(Subprocess::Call(args, "", &uuid));
      StripWhiteSpace(&uuid);
      uuids.emplace_back(uuid);
    }
  }

  // Rewrite the single master's cmeta to reflect the new Raft configuration.
  {
    string data_root = cluster->GetDataPath("master-0");
    vector<string> args = {
        kBinPath,
        "local_replica",
        "cmeta",
        "rewrite_raft_config",
        "--fs_wal_dir=" + cluster->GetWalPath("master-0"),
        "--fs_data_dirs=" + data_root,
        SysCatalogTable::kSysCatalogTabletId
    };
    for (int i = 0; i < kNumMasters; i++) {
      args.emplace_back(Substitute("$0:$1", uuids[i], master_rpc_addresses[i].ToString()));
    }
    ASSERT_OK(Subprocess::Call(args));
  }

  // Temporarily bring up the cluster (in its old configuration) to remote
  // bootstrap the new masters.
  //
  // The single-node master is running in an odd state. The cmeta changes have
  // made it aware that it should replicate to the new masters, but they're not
  // actually running. Thus, it cannot become leader or do any real work. But,
  // it can still service remote bootstrap requests.
  ASSERT_OK(cluster->Restart());

  // Use remote bootstrap to copy the master tablet to each of the new masters'
  // filesystems.
  for (int i = 1; i < kNumMasters; i++) {
    string data_root = cluster->GetDataPath(Substitute("master-$0", i));
    string wal_dir = cluster->GetWalPath(Substitute("master-$0", i));
    vector<string> args = {
        kBinPath,
        "local_replica",
        "copy_from_remote",
        "--fs_wal_dir=" + wal_dir,
        "--fs_data_dirs=" + data_root,
        SysCatalogTable::kSysCatalogTabletId,
        cluster->master()->bound_rpc_hostport().ToString()
    };
    ASSERT_OK(Subprocess::Call(args));
  }

  // Bring down the old cluster configuration and bring up the new one.
  cluster->Shutdown();
  opts.num_masters = 3;
  opts.master_rpc_addresses = master_rpc_addresses;
  ExternalMiniCluster migrated_cluster(std::move(opts));
  ASSERT_OK(migrated_cluster.Start());

  // Perform an operation that requires an elected leader.
  shared_ptr<KuduClient> client;
  ASSERT_OK(migrated_cluster.CreateClient(nullptr, &client));

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ(0, CountTableRows(table.get()));

  // Perform an operation that requires replication.
  ASSERT_OK(CreateTable(&migrated_cluster, "second_table"));

  // Repeat these operations with each of the masters paused.
  //
  // Only in slow mode.
  if (AllowSlowTests()) {
    for (int i = 0; i < kNumMasters; i++) {
      ASSERT_OK(migrated_cluster.master(i)->Pause());
      ScopedResumeExternalDaemon resume_daemon(migrated_cluster.master(i));
      ASSERT_OK(client->OpenTable(kTableName, &table));
      ASSERT_EQ(0, CountTableRows(table.get()));

      // See MasterFailoverTest.TestCreateTableSync to understand why we must
      // check for IsAlreadyPresent as well.
      Status s = CreateTable(&migrated_cluster, Substitute("table-$0", i));
      ASSERT_TRUE(s.ok() || s.IsAlreadyPresent());
    }
  }
}

} // namespace kudu
