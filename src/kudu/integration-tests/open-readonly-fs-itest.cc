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
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduWriteOperation;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;

using std::unique_ptr;

namespace kudu {
namespace itest {

const auto kTimeout = MonoDelta::FromSeconds(60);
const char kTableName[] = "test-table";
const int kNumColumns = 50;

class OpenReadonlyFsITest : public KuduTest {

  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;

    // To repro KUDU-1657 with high probability, the container file size needs
    // to be increasing frequently. To ensure this, we reduce the MRS flush
    // threshold to increase flush frequency and increase the number of MM
    // threads to encourage frequent compactions. The net effect of both of
    // these changes: more blocks are written to disk. Turning off container
    // file preallocation forces every append to increase the file size.
    opts.extra_tserver_flags.emplace_back("--log_container_preallocate_bytes=0");
    opts.extra_tserver_flags.emplace_back("--maintenance_manager_num_threads=16");
    opts.extra_tserver_flags.emplace_back("--flush_threshold_mb=1");

    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());

    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

 protected:
  unique_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// Integration test that creates a tablet, then in parallel:
//  - writes as fast as possible to the tablet, causing as many flushes as possible
//  - opens read-only instances of the FS manager on the tablet's data directory
//
// This is a regression test for KUDU-1657. It typically takes about 35 seconds
// to trigger that bug.
TEST_F(OpenReadonlyFsITest, TestWriteAndVerify) {
  if (!AllowSlowTests()) return;

  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();

  for (int i = 1; i < kNumColumns; i++) {
    b.AddColumn(strings::Substitute("value-$0", i))
                       ->Type(KuduColumnSchema::INT64)
                       ->NotNull();
  }

  ASSERT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
                .schema(&schema)
                .set_range_partition_columns({ })
                .num_replicas(1);

  ASSERT_OK(table_creator->Create());

  shared_ptr<KuduTable> table;
  client_->OpenTable(kTableName, &table);

  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  Random rng(SeedRandom());
  auto deadline = MonoTime::Now() + kTimeout;

  auto t = std::thread([this, deadline] () {
      FsManagerOpts fs_opts;
      fs_opts.read_only = true;
      fs_opts.update_instances = fs::UpdateInstanceBehavior::DONT_UPDATE;
      fs_opts.wal_root = cluster_->tablet_server(0)->wal_dir();
      fs_opts.data_roots = cluster_->tablet_server(0)->data_dirs();
      while (MonoTime::Now() < deadline) {
        FsManager fs(Env::Default(), fs_opts);
        CHECK_OK(fs.Open());
      }
  });

  int64_t count = 0;
  while (MonoTime::Now() < deadline) {
    // Upsert a batch of [500, 1500) random rows.
    int batch_count = 500 + rng.Next() % 1000;
    for (int i = 0; i < batch_count; i++) {
      unique_ptr<KuduWriteOperation> row(table->NewUpsert());
      for (int i = 0; i < kNumColumns; i++) {
        ASSERT_OK(row->mutable_row()->SetInt64(i, rng.Next64()));
      }
      ASSERT_OK(session->Apply(row.release()));
    }
    ASSERT_OK(session->Flush());
    ASSERT_EQ(0, session->CountPendingErrors());
    count += batch_count;
    LOG(INFO) << count << " rows inserted";
  }

  t.join();
}

} // namespace itest
} // namespace kudu
