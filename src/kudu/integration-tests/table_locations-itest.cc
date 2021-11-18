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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/arena.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduSchema;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::master::ReplicaTypeFilter;
using kudu::pb_util::SecureDebugString;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::ostringstream;
using std::pair;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DEFINE_int32(benchmark_runtime_secs, 5, "Number of seconds to run the benchmark");
DEFINE_int32(benchmark_num_threads, 16, "Number of threads to run the benchmark");
DEFINE_int32(benchmark_num_tablets, 60, "Number of tablets to create");

DECLARE_bool(enable_per_range_hash_schemas);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(max_create_tablets_per_ts);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(rpc_num_service_threads);
DECLARE_int32(rpc_service_queue_length);
DECLARE_int32(table_locations_ttl_ms);
DECLARE_string(location_mapping_cmd);
DECLARE_uint32(table_locations_cache_capacity_mb);

METRIC_DECLARE_entity(server);

METRIC_DECLARE_counter(rpcs_queue_overflow);
METRIC_DECLARE_counter(table_locations_cache_evictions);
METRIC_DECLARE_counter(table_locations_cache_hits);
METRIC_DECLARE_counter(table_locations_cache_inserts);
METRIC_DECLARE_counter(table_locations_cache_lookups);
METRIC_DECLARE_counter(table_locations_cache_misses);
METRIC_DECLARE_gauge_uint64(table_locations_cache_memory_usage);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableLocations);

namespace kudu {
namespace master {

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
    opts.num_tablet_servers = 3;

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

  struct HashDimension {
    vector<string> columns;
    int32_t num_buckets;
    uint32_t seed;
  };
  typedef vector<HashDimension> HashSchema;

  Status CreateTable(
      const string& table_name,
      const Schema& schema,
      const vector<KuduPartialRow>& split_rows = {},
      const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds = {},
      const vector<HashSchema>& range_hash_schemas = {},
      const HashSchema& table_hash_schema = {});

  void CreateTable(const string& table_name, int num_splits);

  // Check that the master doesn't give back partial results while the table is being created.
  void CheckMasterTableCreation(const string &table_name, int tablet_locations_size);

  shared_ptr<Messenger> client_messenger_;
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<MasterServiceProxy> proxy_;
};

Status TableLocationsTest::CreateTable(
    const string& table_name,
    const Schema& schema,
    const vector<KuduPartialRow>& split_rows,
    const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds,
    const vector<HashSchema>& range_hash_schemas,
    const HashSchema& table_hash_schema) {

  if (!range_hash_schemas.empty() && range_hash_schemas.size() != bounds.size()) {
    return Status::InvalidArgument(
        "'bounds' and 'range_hash_schemas' must be of the same size");
  }

  CreateTableRequestPB req;
  req.set_name(table_name);
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  RowOperationsPBEncoder splits_encoder(req.mutable_split_rows_range_bounds());
  for (const KuduPartialRow& row : split_rows) {
    splits_encoder.Add(RowOperationsPB::SPLIT_ROW, row);
  }
  for (const auto& bound : bounds) {
    splits_encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, bound.first);
    splits_encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, bound.second);
  }

  auto* ps_pb = req.mutable_partition_schema();
  for (size_t i = 0; i < range_hash_schemas.size(); ++i) {
    const auto& bound = bounds[i];
    const auto& hash_schema = range_hash_schemas[i];
    auto* range = ps_pb->add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, bound.first);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, bound.second);
    for (const auto& hash_dimension : hash_schema) {
      auto* hash_dimension_pb = range->add_hash_schema();
      for (const string& col_name : hash_dimension.columns) {
        hash_dimension_pb->add_columns()->set_name(col_name);
      }
      hash_dimension_pb->set_num_buckets(hash_dimension.num_buckets);
      hash_dimension_pb->set_seed(hash_dimension.seed);
    }
  }

  if (!table_hash_schema.empty()) {
    for (const auto& hash_dimension : table_hash_schema) {
      auto* hash_schema_pb = ps_pb->add_hash_schema();
      for (const string& col_name : hash_dimension.columns) {
        hash_schema_pb->add_columns()->set_name(col_name);
      }
      hash_schema_pb->set_num_buckets(hash_dimension.num_buckets);
      hash_schema_pb->set_seed(hash_dimension.seed);
    }
  }

  CreateTableResponsePB resp;
  RpcController controller;
  RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &controller));
  if (resp.has_error()) {
    RETURN_NOT_OK(StatusFromPB(resp.error().status()));
  }
  return Status::OK();
}

void TableLocationsTest::CreateTable(const string& table_name, int num_splits) {
  Schema schema({ ColumnSchema("key", INT32) }, 1);
  KuduPartialRow row(&schema);
  vector<KuduPartialRow> splits(num_splits, row);
  for (int i = 0; i < num_splits; i++) {
    ASSERT_OK(splits[i].SetInt32(0, i*1000));
  }

  ASSERT_OK(CreateTable(table_name, schema, splits));
  NO_FATALS(CheckMasterTableCreation(table_name, num_splits + 1));
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
    FLAGS_location_mapping_cmd = Substitute("$0 $1", location_cmd_path, location);
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

    auto get_tablet_location = [](MasterServiceProxy* proxy, const string& tablet_id) {
      rpc::RpcController rpc_old;
      GetTabletLocationsRequestPB req_old;
      GetTabletLocationsResponsePB resp_old;
      *req_old.add_tablet_ids() = tablet_id;
      ASSERT_OK(proxy->GetTabletLocations(req_old, &resp_old, &rpc_old));
      const auto& loc_old = resp_old.tablet_locations(0);
      ASSERT_GT(loc_old.deprecated_replicas_size(), 0);
      ASSERT_EQ(0, loc_old.interned_replicas_size());

      rpc::RpcController rpc_new;
      GetTabletLocationsRequestPB req_new;
      GetTabletLocationsResponsePB resp_new;
      *req_new.add_tablet_ids() = tablet_id;
      req_new.set_intern_ts_infos_in_response(true);
      ASSERT_OK(proxy->GetTabletLocations(req_new, &resp_new, &rpc_new));
      const auto& loc_new = resp_new.tablet_locations(0);
      ASSERT_GT(loc_new.interned_replicas_size(), 0);
      ASSERT_EQ(0, loc_new.deprecated_replicas_size());

      ASSERT_EQ(loc_old.tablet_id(), loc_new.tablet_id());
    };

    // Check that a UUID was returned for every replica
    for (const auto& loc : resp.tablet_locations()) {
      NO_FATALS(get_tablet_location(proxy_.get(), loc.tablet_id()));
      ASSERT_GT(loc.deprecated_replicas_size(), 0);
      ASSERT_EQ(0, loc.interned_replicas_size());
      for (const auto& replica : loc.deprecated_replicas()) {
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
      ASSERT_EQ(loc.deprecated_replicas_size(), 0);
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
    req.set_intern_ts_infos_in_response(true);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(1, resp.tablet_locations().size());
    ASSERT_EQ(3, resp.tablet_locations(0).interned_replicas_size());
    const auto& loc = resp.tablet_locations(0);
    for (const auto& replica : loc.interned_replicas()) {
      EXPECT_EQ("", resp.ts_infos(replica.ts_info_idx()).location());
    }
  }
}

TEST_F(TableLocationsTest, RangeSpecificHashingSameDimensions) {
  const string table_name = CURRENT_TEST_NAME();
  Schema schema({
                  ColumnSchema("str_0", STRING),
                  ColumnSchema("int_1", INT32),
                  ColumnSchema("str_2", STRING),
                }, 3);
  KuduPartialRow row(&schema);

  FLAGS_enable_per_range_hash_schemas = true; // enable for testing.

  // Use back-to-back and sparse range placement.
  vector<pair<KuduPartialRow, KuduPartialRow>> bounds(5, { row, row });
  ASSERT_OK(bounds[0].first.SetStringNoCopy(0, "a"));
  ASSERT_OK(bounds[0].second.SetStringNoCopy(0, "b"));
  ASSERT_OK(bounds[1].first.SetStringNoCopy(0, "b"));
  ASSERT_OK(bounds[1].second.SetStringNoCopy(0, "c"));
  ASSERT_OK(bounds[2].first.SetStringNoCopy(0, "c"));
  ASSERT_OK(bounds[2].second.SetStringNoCopy(0, "d"));
  ASSERT_OK(bounds[3].first.SetStringNoCopy(0, "e"));
  ASSERT_OK(bounds[3].second.SetStringNoCopy(0, "f"));
  ASSERT_OK(bounds[4].first.SetStringNoCopy(0, "g"));
  ASSERT_OK(bounds[4].second.SetStringNoCopy(0, "h"));

  vector<HashSchema> range_hash_schemas;
  HashSchema hash_schema_5 = { { { "str_0", "int_1" }, 5, 0 } };
  range_hash_schemas.emplace_back(hash_schema_5);
  HashSchema hash_schema_4 = { { { "str_0" }, 4, 1 } };
  range_hash_schemas.emplace_back(hash_schema_4);
  HashSchema hash_schema_3 = { { { "int_1" }, 3, 2 } };
  range_hash_schemas.emplace_back(hash_schema_3);
  HashSchema hash_schema_6 = { { { "str_2", "str_0" }, 6, 3 } };
  range_hash_schemas.emplace_back(hash_schema_6);

  // Use 2 bucket hash schema as the table-wide one.
  HashSchema table_hash_schema_2 = { { { "str_2" }, 2, 4 } };
  // Apply table-wide hash schema applied to this range.
  range_hash_schemas.emplace_back(table_hash_schema_2);

  ASSERT_OK(CreateTable(
      table_name, schema, {}, bounds, range_hash_schemas, table_hash_schema_2));
  NO_FATALS(CheckMasterTableCreation(table_name, 20));

  // The default setting for GetTableLocationsRequestPB::max_returned_locations
  // is 10 , but here it's necessary to fetch all the existing tablets.
  GetTableLocationsRequestPB req;
  req.mutable_table()->set_table_name(table_name);
  req.clear_max_returned_locations();
  GetTableLocationsResponsePB resp;
  RpcController controller;
  ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_FALSE(resp.has_error());
  ASSERT_EQ(20, resp.tablet_locations().size());

  vector<PartitionKey> partition_keys_ref =  {
      { string("\0\0\0\0", 4), string("a" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\1", 4), string("a" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\2", 4), string("a" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\3", 4), string("a" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\4", 4), string("a" "\0\0\0\0\0\0", 7) },

      { string("\0\0\0\0", 4), string("b" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\1", 4), string("b" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\2", 4), string("b" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\3", 4), string("b" "\0\0\0\0\0\0", 7) },

      { string("\0\0\0\0", 4), string("c" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\1", 4), string("c" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\2", 4), string("c" "\0\0\0\0\0\0", 7) },

      { string("\0\0\0\0", 4), string("e" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\1", 4), string("e" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\2", 4), string("e" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\3", 4), string("e" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\4", 4), string("e" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\5", 4), string("e" "\0\0\0\0\0\0", 7) },

      { string("\0\0\0\0", 4), string("g" "\0\0\0\0\0\0", 7) },
      { string("\0\0\0\1", 4), string("g" "\0\0\0\0\0\0", 7) },
  };
  // Sorting partition keys to match the tablets that are returned in sorted order.
  sort(partition_keys_ref.begin(), partition_keys_ref.end());
  ASSERT_EQ(partition_keys_ref.size(), resp.tablet_locations_size());
  for (auto i = 0; i < resp.tablet_locations_size(); ++i) {
    EXPECT_EQ(partition_keys_ref[i].ToString(),
              resp.tablet_locations(i).partition().partition_key_start());
  }
}

// TODO(aserbin): update this scenario once varying hash dimensions per range
//                are supported
TEST_F(TableLocationsTest, RangeSpecificHashingVaryingDimensions) {
  const string table_name = "test";
  Schema schema({ ColumnSchema("key", STRING), ColumnSchema("val", STRING) }, 2);
  KuduPartialRow row(&schema);

  FLAGS_enable_per_range_hash_schemas = true; // enable for testing.

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds(2, { row, row });
  ASSERT_OK(bounds[0].first.SetStringNoCopy(0, "a"));
  ASSERT_OK(bounds[0].second.SetStringNoCopy(0, "b"));
  ASSERT_OK(bounds[1].first.SetStringNoCopy(0, "c"));
  ASSERT_OK(bounds[1].second.SetStringNoCopy(0, "d"));

  vector<HashSchema> range_hash_schemas;
  HashSchema hash_schema_3_by_2 = { { { "key" }, 3, 0 }, { { "val" }, 2, 1 } };
  range_hash_schemas.emplace_back(hash_schema_3_by_2);

  // Use 4 bucket hash schema as the table-wide one.
  HashSchema table_hash_schema_4 = { { { "val" }, 4, 2 } };
  // Apply table-wide hash schema applied to this range.
  range_hash_schemas.emplace_back(table_hash_schema_4);

  const auto s = CreateTable(
      table_name, schema, {}, bounds, range_hash_schemas, table_hash_schema_4);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
      "varying number of hash dimensions per range is not yet supported");
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
    req.set_intern_ts_infos_in_response(true);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(1, resp.tablet_locations().size());
    ASSERT_EQ(3, resp.tablet_locations(0).interned_replicas_size());
    const auto& loc = resp.tablet_locations(0);
    for (const auto& replica : loc.interned_replicas()) {
      ASSERT_EQ("/foo", resp.ts_infos(replica.ts_info_idx()).location());
    }
  }
}

TEST_F(TableLocationsTest, GetTableLocationsBenchmark) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const int kNumSplits = FLAGS_benchmark_num_tablets - 1;
  const int kNumThreads = FLAGS_benchmark_num_threads;
  const auto kRuntime = MonoDelta::FromSeconds(FLAGS_benchmark_runtime_secs);

  const string table_name = "test";
  NO_FATALS(CreateTable(table_name, kNumSplits));

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
  vector<thread> threads;
  threads.reserve(kNumThreads);
  // If a thread encounters an error, the test is failed.
  vector<uint64_t> err_counters(kNumThreads, 0);
  for (auto i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
        GetTableLocationsRequestPB req;
        GetTableLocationsResponsePB resp;
        while (!stop) {
          RpcController controller;
          req.Clear();
          resp.Clear();
          req.mutable_table()->set_table_name(table_name);
          req.set_max_returned_locations(1000);
          req.set_intern_ts_infos_in_response(true);
          const auto s = proxies[i]->GetTableLocations(req, &resp, &controller);
          if (!s.ok()) {
            LOG(WARNING) << "GetTableLocations() failed: " << s.ToString();
            ++err_counters[i];
            break;
          }
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
  auto counter = METRIC_rpcs_queue_overflow.Instantiate(ent);
  auto hist = METRIC_handler_latency_kudu_master_MasterService_GetTableLocations
      .Instantiate(ent);

  cluster_->Shutdown();

  LOG(INFO) << "RPC queue overflows: " << counter->value();
  const auto errors = accumulate(err_counters.begin(), err_counters.end(), 0UL);
  if (errors != 0) {
    FAIL() << Substitute("detected $0 errors", errors);
  }

  LOG(INFO) << Substitute(
      "GetTableLocations RPC: $0 req/sec",
      hist->histogram()->TotalCount() / kRuntime.ToSeconds());

  ostringstream ostr;
  ostr << "Stats on GetTableLocations RPC (times in microseconds): ";
  hist->histogram()->DumpHumanReadable(&ostr);
  LOG(INFO) << ostr.str();
}

// Similar to GetTableLocationsBenchmark, but calls the function directly within
// the mini-cluster process instead of issuing RPC.
TEST_F(TableLocationsTest, GetTableLocationsBenchmarkFunctionCall) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const string kUserName = "testuser";
  const size_t kNumSplits = FLAGS_benchmark_num_tablets - 1;
  const size_t kNumThreads = FLAGS_benchmark_num_threads;
  const auto kRuntime = MonoDelta::FromSeconds(FLAGS_benchmark_runtime_secs);

  const string table_name = "test";
  NO_FATALS(CreateTable(table_name, kNumSplits));

  CatalogManager* cm = cluster_->mini_master()->master()->catalog_manager();
  const boost::optional<const string&> username = kUserName;

  std::atomic<bool> stop(false);
  vector<thread> threads;
  threads.reserve(kNumThreads);
  vector<uint64_t> req_counters(kNumThreads, 0);
  vector<uint64_t> err_counters(kNumThreads, 0);
  for (size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back([&, idx]() {
      GetTableLocationsRequestPB req;
      google::protobuf::Arena arena;
      while (!stop) {
        req.Clear();
        arena.Reset();
        auto* resp = google::protobuf::Arena::CreateMessage<GetTableLocationsResponsePB>(&arena);
        req.mutable_table()->set_table_name(table_name);
        req.set_max_returned_locations(1000);
        req.set_intern_ts_infos_in_response(true);
        ++req_counters[idx];
        {
          CatalogManager::ScopedLeaderSharedLock l(cm);
          auto s = cm->GetTableLocations(&req, resp, username);
          if (!s.ok()) {
            ++err_counters[idx];
          }
        }
      }
    });
  }

  SleepFor(kRuntime);
  stop = true;
  for (auto& t : threads) {
    t.join();
  }
  cluster_->Shutdown();

  const auto errors = accumulate(err_counters.begin(), err_counters.end(), 0UL);
  if (errors != 0) {
    FAIL() << Substitute("detected $0 errors", errors);
  }

  const double total = accumulate(req_counters.begin(), req_counters.end(), 0UL);
  LOG(INFO) << Substitute(
      "GetTableLocations function call: $0 req/sec",
      total / kRuntime.ToSeconds());
}

// A small benchmark to see whether multiple clients refreshing table location
// might create the thundering herd pattern with their GetTableLocations()
// requests, overflowing the RPC service queue.
class RefreshTableLocationsTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // Set the RPC queue size limit and the number of RPC service threads small
    // to register RPC queue overlows, if any. The idea is to make this test
    // use smaller numbers, while in real life there would be more clients
    // and regular length of the RPC service queue.
    FLAGS_rpc_num_service_threads = 2;
    FLAGS_rpc_service_queue_length = std::max(1, FLAGS_benchmark_num_threads / 3);
    // Make table location information expire faster so the clients would try
    // to refresh the information during the scenario, while not spending too
    // much time to run the scenario.
    FLAGS_table_locations_ttl_ms = 1500;
    {
      InternalMiniClusterOptions opts;
      opts.num_masters = 1;
      opts.num_tablet_servers = 3;
      cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    }
    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
};

TEST_F(RefreshTableLocationsTest, ThunderingHerd) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const auto kNumThreads = FLAGS_benchmark_num_threads;
  const auto kRuntime =
      MonoDelta::FromMilliseconds(5 * FLAGS_table_locations_ttl_ms);

  // Create test tables and populate them with data. These tables are used by
  // the threads below, each running its own read test workload.
  for (auto idx = 0; idx < kNumThreads; ++idx) {
    TestWorkload w(cluster_.get());
    w.set_num_replicas(3);
    w.set_num_tablets(10);
    w.set_table_name(Substitute("test_table_$0", idx));
    w.set_num_write_threads(1);
    w.set_num_read_threads(0);
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 100) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }

  std::atomic<bool> stop(false);
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (auto idx = 0; idx < kNumThreads; ++idx) {
    // It's important to have multiple workloads to simulate multiple clients,
    // each with its own table locations cache.
    threads.emplace_back([&, idx]() {
      TestWorkload w(cluster_.get());
      w.set_table_name(Substitute("test_table_$0", idx));
      w.set_num_read_threads(1);
      w.set_num_write_threads(0);
      w.Setup();
      w.Start();
      while (!stop) {
        SleepFor(MonoDelta::FromMilliseconds(10));
      }
      w.StopAndJoin();
    });
  }

  // Make sure the reader threads refresh the expired information on table
  // locations few times during the scenario.
  SleepFor(kRuntime);
  stop = true;
  for (auto& t : threads) {
    t.join();
  }

  const auto& ent = cluster_->mini_master()->master()->metric_entity();
  auto counter = METRIC_rpcs_queue_overflow.Instantiate(ent);
  auto hist = METRIC_handler_latency_kudu_master_MasterService_GetTableLocations.
      Instantiate(ent);

  cluster_->Shutdown();

  LOG(INFO) << "RPC queue overflows: " << counter->value();
  LOG(INFO) << Substitute(
      "GetTableLocations RPC: $0 req/sec",
      hist->histogram()->TotalCount() / kRuntime.ToSeconds());

  ostringstream ostr;
  ostr << "Stats on GetTableLocations RPC (times in microseconds): ";
  hist->histogram()->DumpHumanReadable(&ostr);
  LOG(INFO) << ostr.str();
}

class TableLocationsCacheBaseTest : public KuduTest {
 public:
  TableLocationsCacheBaseTest(int num_tablet_servers,
                              int cache_capacity_mb)
      : num_tablet_servers_(num_tablet_servers),
        cache_capacity_mb_(cache_capacity_mb),
        schema_(itest::SimpleIntKeyKuduSchema()) {
  }

  void SetUp() override {
    KuduTest::SetUp();

    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_tablet_servers_;

    // Setting the cache's capacity to anything > 0 enables caching of table
    // locations.
    FLAGS_table_locations_cache_capacity_mb = cache_capacity_mb_;

    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema_)
        .set_range_partition_columns({ "key" })
        .num_replicas(std::min<int>(3, num_tablet_servers_))
        .add_hash_partitions({ "key" }, 5)
        .Create());
    metric_entity_ = cluster_->mini_master()->master()->metric_entity();
    ASSERT_NE(nullptr, metric_entity_.get());
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

  int64_t GetCacheInserts() const {
    return METRIC_table_locations_cache_inserts.Instantiate(metric_entity_)->value();
  }

  int64_t GetCacheLookups() const {
    return METRIC_table_locations_cache_lookups.Instantiate(metric_entity_)->value();
  }

  int64_t GetCacheHits() const {
    return METRIC_table_locations_cache_hits.Instantiate(metric_entity_)->value();
  }

  int64_t GetCacheMisses() const {
    return METRIC_table_locations_cache_misses.Instantiate(metric_entity_)->value();
  }

  int64_t GetCacheEvictions() const {
    return METRIC_table_locations_cache_evictions.Instantiate(metric_entity_)->value();
  }

  uint64_t GetCacheMemoryUsage() const {
    return METRIC_table_locations_cache_memory_usage.Instantiate(metric_entity_, 0)->value();
  }

  // Insert about the 'rows_num' into the table named 'table_name', returning
  // the actual number of rows written.
  int64_t InsertRows(const string& table_name, int64_t rows_num = 10) {
    TestWorkload w(cluster_.get());
    w.set_table_name(table_name);
    w.set_schema(schema_);
    w.set_num_read_threads(0);
    w.set_num_write_threads(1);
    w.set_write_batch_size(1);
    w.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
    w.set_already_present_allowed(true);
    w.Setup();
    w.Start();
    while (w.rows_inserted() < rows_num) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();

    // Return the actual number of rows inserted.
    return w.rows_inserted();
  }

 protected:
  static constexpr const char* const kTableName = "table_locations_cache_test";
  const int num_tablet_servers_;
  const int cache_capacity_mb_;

  const KuduSchema schema_;

  unique_ptr<InternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  scoped_refptr<MetricEntity> metric_entity_;
};

class TableLocationsCacheDisabledTest : public TableLocationsCacheBaseTest {
 public:
  TableLocationsCacheDisabledTest()
      : TableLocationsCacheBaseTest(1 /*num_tablet_servers*/,
                                    0 /*cache_capacity_mb*/) {}
};

TEST_F(TableLocationsCacheDisabledTest, Basic) {
  // Verify that by default tablet location cache is disabled.
  google::CommandLineFlagInfo flag_info;
  ASSERT_TRUE(google::GetCommandLineFlagInfo(
      "table_locations_cache_capacity_mb", &flag_info));
  ASSERT_TRUE(flag_info.is_default);
  ASSERT_EQ("0", flag_info.default_value);

  InsertRows(kTableName);

  // When disabled, cache should not be used.
  EXPECT_EQ(0, GetCacheEvictions());
  EXPECT_EQ(0, GetCacheHits());
  EXPECT_EQ(0, GetCacheInserts());
  EXPECT_EQ(0, GetCacheLookups());
  EXPECT_EQ(0, GetCacheMisses());
  EXPECT_EQ(0, GetCacheMemoryUsage());
}

class TableLocationsCacheTest : public TableLocationsCacheBaseTest {
 public:
  TableLocationsCacheTest()
      : TableLocationsCacheBaseTest(1 /*num_tablet_servers*/,
                                    1 /*cache_capacity_mb*/) {}
};

// Verify that requests with different essential parameters produce
// different entries in the table locations cache.
TEST_F(TableLocationsCacheTest, DifferentRequestParameters) {
  MessengerBuilder bld("test_builder");
  shared_ptr<Messenger> messenger;
  ASSERT_OK(bld.Build(&messenger));
  const auto& addr = cluster_->mini_master()->bound_rpc_addr();
  MasterServiceProxy proxy(messenger, addr, addr.host());

  {
    // Issue one query many times -- there should be just one record inserted.
    const auto prev_cache_inserts = GetCacheInserts();
    for (auto i = 0; i < 5; ++i) {
      GetTableLocationsRequestPB req;
      GetTableLocationsResponsePB resp;
      req.mutable_table()->set_table_name(kTableName);
      req.set_max_returned_locations(1);
      RpcController ctl;
      ASSERT_OK(proxy.GetTableLocations(req, &resp, &ctl));
      ASSERT_TRUE(!resp.has_error());
    }
    ASSERT_EQ(prev_cache_inserts + 1, GetCacheInserts());
  }

  {
    // Issue a query with different value of 'max_returned_locations'. A new
    // entry should be added into the cache even if other parameters are the
    // same as in requests sent prior.
    const auto prev_cache_inserts = GetCacheInserts();
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);
    req.set_max_returned_locations(10);
    RpcController ctl;
    ASSERT_OK(proxy.GetTableLocations(req, &resp, &ctl));
    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(prev_cache_inserts + 1, GetCacheInserts());
  }

  {
    // Set 'replica_type_filter' parameter.
    const auto prev_cache_inserts = GetCacheInserts();
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);
    req.set_max_returned_locations(10);
    req.set_replica_type_filter(ReplicaTypeFilter::ANY_REPLICA);
    RpcController ctl;
    ASSERT_OK(proxy.GetTableLocations(req, &resp, &ctl));
    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(prev_cache_inserts + 1, GetCacheInserts());
  }

  {
    // Switch 'replica_type_filter' parameter to a different value.
    const auto prev_cache_inserts = GetCacheInserts();
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);
    req.set_max_returned_locations(10);
    req.set_replica_type_filter(ReplicaTypeFilter::VOTER_REPLICA);
    RpcController ctl;
    ASSERT_OK(proxy.GetTableLocations(req, &resp, &ctl));
    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(prev_cache_inserts + 1, GetCacheInserts());
  }
}

// This scenario verifies basic functionality of the table locations cache.
TEST_F(TableLocationsCacheTest, Basic) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  int64_t prev_cache_hits = 0;
  int64_t prev_cache_inserts = 0;
  int64_t prev_cache_lookups = 0;
  int64_t prev_cache_misses = 0;
  int64_t prev_cache_evictions = 0;

  {
    // Run a workload to prime the table locations cache.
    InsertRows(kTableName);
    const auto cache_lookups = GetCacheLookups();
    EXPECT_GT(cache_lookups, 0);
    const auto cache_misses = GetCacheMisses();
    const auto cache_inserts = GetCacheInserts();
    EXPECT_EQ(cache_misses, cache_inserts);
    const auto cache_hits = GetCacheHits();
    EXPECT_EQ(cache_lookups - cache_misses, cache_hits);

    // The cache usage should be non-zero since some entries are present.
    EXPECT_GT(GetCacheMemoryUsage(), 0);

    // Store the counters for the next round.
    prev_cache_hits = cache_hits;
    prev_cache_inserts = cache_inserts;
    prev_cache_lookups = cache_lookups;
    prev_cache_misses = cache_misses;
    prev_cache_evictions = GetCacheEvictions();
  }

  {
    // The cache has been primed -- now run another workload.
    InsertRows(kTableName);

    // No new cache inserts expected.
    EXPECT_EQ(prev_cache_inserts, GetCacheInserts());
    // No new cache misses expected.
    EXPECT_EQ(prev_cache_misses, GetCacheMisses());

    // There should be new cache lookups.
    const auto cache_lookups = GetCacheLookups();
    EXPECT_LT(prev_cache_lookups, cache_lookups);

    // There should be new cache hits.
    const auto cache_hits = GetCacheHits();
    EXPECT_LT(prev_cache_hits, cache_hits);

    // All new lookups should hit the cache.
    EXPECT_EQ(cache_lookups - prev_cache_lookups, cache_hits - prev_cache_hits);
  }

  {
    // Alter the test table and make sure the information isn't purged from the
    // table locations cache since the table locations haven't changed.
    const string new_table_name = string(kTableName) + "_renamed";

    unique_ptr<KuduTableAlterer> a0(client_->NewTableAlterer(kTableName));
    auto s = a0->RenameTo(new_table_name)->Alter();
    ASSERT_OK(s);

    // Wait for the master to get notified on the change.
    SleepFor(MonoDelta::FromMilliseconds(3 * FLAGS_heartbeat_interval_ms));

    // No new cache evictions are expected.
    EXPECT_EQ(prev_cache_evictions, GetCacheEvictions());

    InsertRows(new_table_name);

    EXPECT_EQ(prev_cache_evictions, GetCacheEvictions());
    // No new cache inserts nor misses are expected.
    EXPECT_EQ(prev_cache_inserts, GetCacheInserts());
    EXPECT_EQ(prev_cache_misses, GetCacheMisses());

    // Rename the table back.
    unique_ptr<KuduTableAlterer> a1(client_->NewTableAlterer(new_table_name));
    s = a1->RenameTo(kTableName)->Alter();
    ASSERT_OK(s);
  }

  {
    // Drop the test table and make sure the cached location information is
    // purged from the table locations cache.
    ASSERT_OK(client_->DeleteTable(kTableName));

    // Wait for the master to get notified on the change.
    SleepFor(MonoDelta::FromMilliseconds(3 * FLAGS_heartbeat_interval_ms));

    // All the previously inserted records should be purged.
    EXPECT_EQ(GetCacheEvictions(), GetCacheInserts());
    EXPECT_EQ(0, GetCacheMemoryUsage());
  }
}

class TableLocationsCacheTabletChangeTest :
    public TableLocationsCacheBaseTest {
 public:
  TableLocationsCacheTabletChangeTest()
      : TableLocationsCacheBaseTest(5 /*num_tablet_servers*/,
                                    1 /*cache_capacity_mb*/) {}
};

// Verify the behavior of the table locations cache when table's tablets change
// leadership roles or move around.
TEST_F(TableLocationsCacheTabletChangeTest, Basic) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  {
    // Run a workload to prime the table locations cache.
    InsertRows(kTableName);
    const auto cache_lookups = GetCacheLookups();
    EXPECT_GT(cache_lookups, 0);
    const auto cache_misses = GetCacheMisses();
    EXPECT_GT(cache_misses, 0);
    EXPECT_EQ(cache_misses, GetCacheInserts());
    EXPECT_EQ(cache_lookups - cache_misses, GetCacheHits());
  }

  // Restart tablet servers to reshuffle leadership role of tablet replicas.
  for (auto idx = 0; idx < cluster_->num_tablet_servers(); ++idx) {
    cluster_->mini_tablet_server(idx)->Shutdown();
    SleepFor(MonoDelta::FromMilliseconds(static_cast<int64_t>(
        2 * FLAGS_leader_failure_max_missed_heartbeat_periods *
        FLAGS_raft_heartbeat_interval_ms) + 3 * FLAGS_heartbeat_interval_ms));
    ASSERT_OK(cluster_->mini_tablet_server(idx)->Restart());
  }

  {
    // All the previously inserted records should be purged.
    EXPECT_EQ(GetCacheEvictions(), GetCacheInserts());
    const auto prev_cache_inserts = GetCacheInserts();
    const auto prev_cache_misses = GetCacheMisses();

    InsertRows(kTableName);

    // New cache inserts and misses are expected since the previously inserted
    // records have been just purged.
    EXPECT_LT(prev_cache_inserts, GetCacheInserts());
    EXPECT_LT(prev_cache_misses, GetCacheMisses());
  }

  // Shutdown one tablet server and get the system some time to re-replicate
  // affected tablet replicas elsewhere.
  FLAGS_follower_unavailable_considered_failed_sec = 1;
  cluster_->mini_tablet_server(0)->Shutdown();
  SleepFor(MonoDelta::FromMilliseconds(static_cast<int64_t>(
      2 * FLAGS_leader_failure_max_missed_heartbeat_periods *
          FLAGS_raft_heartbeat_interval_ms +
      3 * 1000 * FLAGS_follower_unavailable_considered_failed_sec +
      3 * FLAGS_heartbeat_interval_ms)));

  // All the previously inserted records should be purged.
  EXPECT_EQ(GetCacheEvictions(), GetCacheInserts());
  EXPECT_EQ(0, GetCacheMemoryUsage());
}

class TableLocationsCacheMultiMasterTest : public KuduTest {
 public:
  TableLocationsCacheMultiMasterTest()
      : schema_(itest::SimpleIntKeyKuduSchema()) {
  }

  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 3;
    opts.extra_master_flags = {
      "--table_locations_cache_capacity_mb=8",
      Substitute("--raft_heartbeat_interval_ms=$0",
          kRaftHeartbeatIntervalMs),
      Substitute("--leader_failure_max_missed_heartbeat_periods=$0",
          kMaxMissedHeartbeatPeriods),
    };
    cluster_.reset(new cluster::ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());

    client::sp::shared_ptr<client::KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));

    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema_)
        .set_range_partition_columns({ "key" })
        .num_replicas(3)
        .add_hash_partitions({ "key" }, 5)
        .Create());
  }

  int64_t GetTableLocationCacheMetric(
      int master_idx,
      const MetricPrototype* metric_proto) const {
    CHECK_LT(master_idx, cluster_->num_masters());
    int64_t value;
    CHECK_OK(itest::GetInt64Metric(
        cluster_->master(master_idx)->bound_http_hostport(),
        &METRIC_ENTITY_server, "kudu.master", metric_proto, "value", &value));
    return value;
  }

  int64_t GetTableLocationCacheMetricAllMasters(
      const MetricPrototype* metric_proto) const {
    int64_t total = 0;
    for (auto idx = 0; idx < cluster_->num_masters(); ++idx) {
      const auto val = GetTableLocationCacheMetric(idx, metric_proto);
      total += val;
    }
    return total;
  }

  void CheckCacheMetricsReset(int master_idx) const {
    for (const auto* metric : {
         &METRIC_table_locations_cache_evictions,
         &METRIC_table_locations_cache_hits,
         &METRIC_table_locations_cache_inserts,
         &METRIC_table_locations_cache_lookups,
         &METRIC_table_locations_cache_misses, }) {
      SCOPED_TRACE(Substitute(
          "verifying value of '$0' metric ($1) for master at index $2",
          metric->name(), metric->label(), master_idx));
      const auto val = GetTableLocationCacheMetric(master_idx, metric);
      EXPECT_EQ(0, val);
    }
    const auto cache_memory_usage = GetTableLocationCacheMetric(
        master_idx, &METRIC_table_locations_cache_memory_usage);
    EXPECT_EQ(0, cache_memory_usage);
  }

 protected:
  static constexpr const char* const kTableName = "test_locations_cache_multi_master";
  static constexpr int32_t kRaftHeartbeatIntervalMs = 300;
  static constexpr int32_t kMaxMissedHeartbeatPeriods = 2;
  const KuduSchema schema_;
  std::unique_ptr<cluster::ExternalMiniCluster> cluster_;
};

// Verify that the table location cache is reset upon change once master
// starts its leadership role.
TEST_F(TableLocationsCacheMultiMasterTest, ResetCache) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Make sure the cache's metrics are zeroed if no client activity has been
  // there yet.
  for (auto idx = 0; idx < cluster_->num_masters(); ++idx) {
    NO_FATALS(CheckCacheMetricsReset(idx));
  }

  TestWorkload w(cluster_.get());
  w.set_table_name(kTableName);
  w.set_schema(schema_);
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();

  // Make sure some items are added into the cache.
  EXPECT_GT(GetTableLocationCacheMetricAllMasters(
      &METRIC_table_locations_cache_inserts), 0);

  int former_leader_master_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&former_leader_master_idx));

  int leader_master_idx = -1;
  ASSERT_EVENTUALLY([&] {
    // Induce a change of the masters' leadership.
    ASSERT_OK(cluster_->master(former_leader_master_idx)->Pause());
    // Make one master stop sending heartbeats, and give the rest about three
    // heartbeat periods to elect a new leader (include an extra margin to keep
    // the scenario stable).
    SleepFor(MonoDelta::FromMilliseconds(
        2 * (kRaftHeartbeatIntervalMs * kMaxMissedHeartbeatPeriods +
             kRaftHeartbeatIntervalMs * 3)));
    ASSERT_OK(cluster_->master(former_leader_master_idx)->Resume());
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
    ASSERT_NE(former_leader_master_idx, leader_master_idx);
  });
  // An extra sanity check.
  ASSERT_NE(-1, leader_master_idx);

  // Make sure all the cache's metrics are reset once master just has become
  // a leader.
  NO_FATALS(CheckCacheMetricsReset(leader_master_idx));
}

} // namespace master
} // namespace kudu
