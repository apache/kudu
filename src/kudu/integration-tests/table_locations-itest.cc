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

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::pb_util::SecureDebugString;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

DECLARE_string(location_mapping_cmd);
DECLARE_int32(max_create_tablets_per_ts);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableLocations);

DEFINE_int32(benchmark_runtime_secs, 5, "Number of seconds to run the benchmark");
DEFINE_int32(benchmark_num_threads, 16, "Number of threads to run the benchmark");
DEFINE_int32(benchmark_num_tablets, 60, "Number of tablets to create");

namespace kudu {
namespace master {

const int kNumTabletServers = 3;

// Test the master GetTableLocations RPC. This can't be done in master-test,
// since it requires a running tablet server in order for the catalog manager to
// report tablet locations.
class TableLocationsTest : public KuduTest {
 public:

  void SetUp() override {
    KuduTest::SetUp();

    FLAGS_max_create_tablets_per_ts = 1000;
    SetUpConfig();

    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;

    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());

    // Create a client proxy to the master.
    MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&client_messenger_));
    const auto& addr = cluster_->mini_master()->bound_rpc_addr();
    proxy_.reset(new MasterServiceProxy(client_messenger_, addr, addr.host()));
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

  virtual void SetUpConfig() {}

  Status CreateTable(const string& table_name,
                     const Schema& schema,
                     const vector<KuduPartialRow>& split_rows,
                     const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds);

  // Check that the master doesn't give back partial results while the table is being created.
  void CheckMasterTableCreation(const string &table_name, int tablet_locations_size);

  shared_ptr<Messenger> client_messenger_;
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<MasterServiceProxy> proxy_;
};

Status TableLocationsTest::CreateTable(const string& table_name,
                                       const Schema& schema,
                                       const vector<KuduPartialRow>& split_rows = {},
                                       const vector<pair<KuduPartialRow,
                                                         KuduPartialRow>>& bounds = {}) {

  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name(table_name);
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());
  for (const KuduPartialRow& row : split_rows) {
    encoder.Add(RowOperationsPB::SPLIT_ROW, row);
  }
  for (const auto& bound : bounds) {
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, bound.first);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, bound.second);
  }

  return proxy_->CreateTable(req, &resp, &controller);
}

void TableLocationsTest::CheckMasterTableCreation(const string &table_name,
                                                  int tablet_locations_size) {
  GetTableLocationsRequestPB req;
  GetTableLocationsResponsePB resp;
  RpcController controller;
  req.set_max_returned_locations(1000);
  req.mutable_table()->set_table_name(table_name);

  for (int i = 1; ; i++) {
    if (i > 10) {
      FAIL() << "Create table timed out";
    }

    controller.Reset();
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    if (resp.has_error()) {
      ASSERT_EQ(MasterErrorPB::TABLET_NOT_RUNNING, resp.error().code());
      SleepFor(MonoDelta::FromMilliseconds(i * i * 100));
    } else {
      ASSERT_EQ(tablet_locations_size, resp.tablet_locations().size());
      break;
    }
  }
}

// Test the tablet server location is properly set in the master GetTableLocations RPC.
class TableLocationsWithTSLocationTest : public TableLocationsTest {
 public:
  void SetUpConfig() override {
    const string location_cmd_path = JoinPathSegments(GetTestExecutableDirectory(),
                                                      "testdata/first_argument.sh");
    const string location = "/foo";
    FLAGS_location_mapping_cmd = strings::Substitute("$0 $1", location_cmd_path, location);
  }
};

// Test that when the client requests table locations for a non-covered
// partition range, the master returns the first tablet previous to the begin
// partition key, as specified in the non-covering range partitions design
// document.
TEST_F(TableLocationsTest, TestGetTableLocations) {
  const string table_name = "test";
  Schema schema({ ColumnSchema("key", STRING) }, 1);
  KuduPartialRow row(&schema);

  vector<KuduPartialRow> splits(6, row);
  ASSERT_OK(splits[0].SetStringNoCopy(0, "aa"));
  ASSERT_OK(splits[1].SetStringNoCopy(0, "ab"));
  ASSERT_OK(splits[2].SetStringNoCopy(0, "ac"));
  ASSERT_OK(splits[3].SetStringNoCopy(0, "ca"));
  ASSERT_OK(splits[4].SetStringNoCopy(0, "cb"));
  ASSERT_OK(splits[5].SetStringNoCopy(0, "cc"));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds(2, { row, row });
  ASSERT_OK(bounds[0].first.SetStringNoCopy(0, "a"));
  ASSERT_OK(bounds[0].second.SetStringNoCopy(0, "b"));
  ASSERT_OK(bounds[1].first.SetStringNoCopy(0, "c"));
  ASSERT_OK(bounds[1].second.SetStringNoCopy(0, "d"));

  ASSERT_OK(CreateTable(table_name, schema, splits, bounds));

  NO_FATALS(CheckMasterTableCreation(table_name, 8));

  { // from "a"
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("a");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("a", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("aa", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ab", resp.tablet_locations(2).partition().partition_key_start());
  }

  { // from ""
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("a", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("aa", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ab", resp.tablet_locations(2).partition().partition_key_start());

    // Check that a UUID was returned for every replica
    for (const auto& loc : resp.tablet_locations()) {
      ASSERT_GT(loc.replicas_size(), 0);
      ASSERT_EQ(0, loc.interned_replicas_size());
      for (const auto& replica : loc.replicas()) {
        ASSERT_NE("", replica.ts_info().permanent_uuid());
      }
    }
  }

  { // from "", with TSInfo interning enabled.
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("");
    req.set_max_returned_locations(3);
    req.set_intern_ts_infos_in_response(true);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("a", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("aa", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ab", resp.tablet_locations(2).partition().partition_key_start());
    // Check that a UUID was returned for every replica
    for (const auto& loc : resp.tablet_locations()) {
      ASSERT_EQ(loc.replicas_size(), 0);
      ASSERT_GT(loc.interned_replicas_size(), 0);
      for (const auto& replica : loc.interned_replicas()) {
        int idx = replica.ts_info_idx();
        ASSERT_GE(idx, 0);
        ASSERT_LE(idx, resp.ts_infos_size());
        ASSERT_NE("", resp.ts_infos(idx).permanent_uuid());
      }
    }
  }

  { // from "b"
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("b");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("ac", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("c", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ca", resp.tablet_locations(2).partition().partition_key_start());
  }

  { // from "z"
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("z");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(1, resp.tablet_locations().size());
    EXPECT_EQ("cc", resp.tablet_locations(0).partition().partition_key_start());
  }

  { // Check that the tablet server location should be an empty string if not set.
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(1);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(1, resp.tablet_locations().size());
    ASSERT_EQ(3, resp.tablet_locations(0).replicas_size());
    for (int i = 0; i < 3; i++) {
      EXPECT_EQ("", resp.tablet_locations(0).replicas(i).ts_info().location());
    }
  }
}

TEST_F(TableLocationsWithTSLocationTest, TestGetTSLocation) {
  const string table_name = "test";
  Schema schema({ ColumnSchema("key", STRING) }, 1);
  ASSERT_OK(CreateTable(table_name, schema));

  NO_FATALS(CheckMasterTableCreation(table_name, 1));

  {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(1, resp.tablet_locations().size());
    ASSERT_EQ(3, resp.tablet_locations(0).replicas_size());
    for (int i = 0; i < 3; i++) {
      ASSERT_EQ("/foo", resp.tablet_locations(0).replicas(i).ts_info().location());
    }
  }
}

TEST_F(TableLocationsTest, GetTableLocationsBenchmark) {
  const int kNumSplits = FLAGS_benchmark_num_tablets - 1;
  const int kNumThreads = FLAGS_benchmark_num_threads;
  const auto kRuntime = MonoDelta::FromSeconds(FLAGS_benchmark_runtime_secs);

  const string table_name = "test";
  Schema schema({ ColumnSchema("key", INT32) }, 1);
  KuduPartialRow row(&schema);

  vector<KuduPartialRow> splits(kNumSplits, row);
  for (int i = 0; i < kNumSplits; i++) {
    ASSERT_OK(splits[i].SetInt32(0, i*1000));
  }

  ASSERT_OK(CreateTable(table_name, schema, splits));

  NO_FATALS(CheckMasterTableCreation(table_name, kNumSplits + 1));

  // Make one proxy per thread, so each thread gets its own messenger and
  // reactor. If there were only one messenger, then only one reactor thread
  // would be used for the connection to the master, so this benchmark would
  // probably be testing the serialization and network code rather than the
  // master GTL code.
  vector<unique_ptr<MasterServiceProxy>> proxies;
  proxies.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    MessengerBuilder bld("Client");
    bld.set_num_reactors(1);
    shared_ptr<Messenger> msg;
    ASSERT_OK(bld.Build(&msg));
    const auto& addr = cluster_->mini_master()->bound_rpc_addr();
    proxies.emplace_back(new MasterServiceProxy(std::move(msg), addr, addr.host()));
  }

  std::atomic<bool> stop { false };
  vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i]() {
        while (!stop) {
          GetTableLocationsRequestPB req;
          GetTableLocationsResponsePB resp;
          RpcController controller;
          req.mutable_table()->set_table_name(table_name);
          req.set_max_returned_locations(1000);
          req.set_intern_ts_infos_in_response(true);
          CHECK_OK(proxies[i]->GetTableLocations(req, &resp, &controller));
          CHECK_EQ(resp.tablet_locations_size(), kNumSplits + 1);
        }
      });
  }

  SleepFor(kRuntime);
  stop = true;
  for (auto& t : threads) {
    t.join();
  }

  const auto& ent = cluster_->mini_master()->master()->metric_entity();
  auto hist = METRIC_handler_latency_kudu_master_MasterService_GetTableLocations
      .Instantiate(ent);

  cluster_->Shutdown();

  hist->histogram()->DumpHumanReadable(&LOG(INFO));
}

} // namespace master
} // namespace kudu
