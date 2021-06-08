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
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_per_range_hash_schemas);
DECLARE_int32(heartbeat_interval_ms);

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::master::CatalogManager;
using kudu::client::sp::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {

class FlexPartitioningTest : public KuduTest {
 public:
  static constexpr const char* const kKeyColumn = "key";
  static constexpr const char* const kIntValColumn = "int_val";
  static constexpr const char* const kStringValColumn = "string_val";

  FlexPartitioningTest() {
    KuduSchemaBuilder b;
    b.AddColumn("key")->
        Type(KuduColumnSchema::INT32)->
        NotNull()->
        PrimaryKey();
    b.AddColumn("int_val")->
        Type(KuduColumnSchema::INT32)->
        NotNull();
    b.AddColumn("string_val")->
        Type(KuduColumnSchema::STRING)->
        Nullable();
    CHECK_OK(b.Build(&schema_));
  }

  void SetUp() override {
    KuduTest::SetUp();

    // Reduce the TS<->Master heartbeat interval to speed up testing.
    FLAGS_heartbeat_interval_ms = 10;

    // Explicitly enable support for custom hash schemas per range partition.
    FLAGS_enable_per_range_hash_schemas = true;

    // Start minicluster and wait for tablet servers to connect to master.
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());

    // Connect to the cluster.
    ASSERT_OK(KuduClientBuilder()
        .add_master_server_addr(
            cluster_->mini_master()->bound_rpc_addr().ToString())
        .Build(&client_));
  }

 protected:
  typedef unique_ptr<KuduTableCreator::KuduRangePartition> RangePartition;
  typedef vector<RangePartition> RangePartitions;

  static Status ApplyInsert(KuduSession* session,
                            const shared_ptr<KuduTable>& table,
                            int32_t key_val,
                            int32_t int_val,
                            string string_val) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    RETURN_NOT_OK(insert->mutable_row()->SetInt32(kKeyColumn, key_val));
    RETURN_NOT_OK(insert->mutable_row()->SetInt32(kIntValColumn, int_val));
    RETURN_NOT_OK(insert->mutable_row()->SetStringCopy(
        kStringValColumn, std::move(string_val)));
    return session->Apply(insert.release());
  }

  Status InsertTestRows(
      const char* table_name,
      int32_t key_beg,
      int32_t key_end,
      KuduSession::FlushMode flush_mode = KuduSession::AUTO_FLUSH_SYNC) {
    CHECK_LE(key_beg, key_end);
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name, &table));
    shared_ptr<KuduSession> session = client_->NewSession();
    RETURN_NOT_OK(session->SetFlushMode(flush_mode));
    session->SetTimeoutMillis(60000);
    for (int32_t key_val = key_beg; key_val < key_end; ++key_val) {
      RETURN_NOT_OK(ApplyInsert(
          session.get(), table, key_val, rand(), std::to_string(rand())));
    }
    return session->Flush();
  }

  Status CreateTable(const char* table_name, RangePartitions partitions) {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(table_name)
        .schema(&schema_)
        .num_replicas(1)
        .set_range_partition_columns({ kKeyColumn });

    for (auto& p : partitions) {
      table_creator->add_custom_range_partition(p.release());
    }

    return table_creator->Create();
  }

  RangePartition CreateRangePartition(int32_t lower_boundary = 0,
                                      int32_t upper_boundary = 100) {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    CHECK_OK(lower->SetInt32(kKeyColumn, lower_boundary));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    CHECK_OK(upper->SetInt32(kKeyColumn, upper_boundary));
    return unique_ptr<KuduTableCreator::KuduRangePartition>(
        new KuduTableCreator::KuduRangePartition(lower.release(),
                                                 upper.release()));
  }

  void CheckTabletCount(const char* table_name, int expected_count) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));

    scoped_refptr<master::TableInfo> table_info;
    {
      auto* cm = cluster_->mini_master(0)->master()->catalog_manager();
      CatalogManager::ScopedLeaderSharedLock l(cm);
      ASSERT_OK(cm->GetTableInfo(table->id(), &table_info));
    }
    ASSERT_EQ(expected_count, table_info->num_tablets());
  }

  KuduSchema schema_;
  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// Test for scenarios covering range partitioning with custom bucket schemas
// specified when creating a table.
class FlexPartitioningCreateTableTest : public FlexPartitioningTest {};

// Create tables with range partitions using custom hash bucket schemas only.
//
// TODO(aserbin): turn the sub-scenarios with non-primary-key columns for
//                custom hash buckets into negative ones after proper
//                checks are added at the server side
// TODO(aserbin): add verification based on PartitionSchema provided by
//                KuduTable::partition_schema() once PartitionPruner
//                recognized custom hash bucket schema for ranges
// TODO(aserbin): add InsertTestRows() when proper key encoding is implemented
TEST_F(FlexPartitioningCreateTableTest, CustomHashBuckets) {
  // One-level hash bucket structure: { 3, "key" }.
  {
    constexpr const char* const kTableName = "3@key";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 0));
    ASSERT_OK(CreateTable(kTableName, std::move(partitions)));
    NO_FATALS(CheckTabletCount(kTableName, 3));
  }

  // One-level hash bucket structure with hashing on non-key column only:
  // { 3, "int_val" }.
  {
    constexpr const char* const kTableName = "3@int_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kIntValColumn }, 2, 0));
    ASSERT_OK(CreateTable(kTableName, std::move(partitions)));
    NO_FATALS(CheckTabletCount(kTableName, 2));
  }

  // One-level hash bucket structure with hashing on non-key nullable column:
  // { 5, "string_val" }.
  {
    constexpr const char* const kTableName = "3@string_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kStringValColumn }, 5, 0));
    ASSERT_OK(CreateTable(kTableName, std::move(partitions)));
    NO_FATALS(CheckTabletCount(kTableName, 5));
  }

  // Two-level hash bucket structure: { 3, "key" } x { 3, "key" }.
  {
    constexpr const char* const kTableName = "3@key_x_3@key";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 0));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    ASSERT_OK(CreateTable(kTableName, std::move(partitions)));
    NO_FATALS(CheckTabletCount(kTableName, 9));
  }

  // Two-level hash bucket structure: { 2, "key" } x { 3, "int_val" }.
  {
    constexpr const char* const kTableName = "2@key_x_3@int_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 0));
    ASSERT_OK(p->add_hash_partitions({ kIntValColumn }, 3, 1));
    ASSERT_OK(CreateTable(kTableName, std::move(partitions)));
    NO_FATALS(CheckTabletCount(kTableName, 6));
  }

  // Two-level hash bucket structure: { 3, "key" } x { 2, "key", "int_val" }.
  {
    constexpr const char* const kTableName = "3@key_x_2@key:int_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 0));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn, kIntValColumn }, 2, 1));
    ASSERT_OK(CreateTable(kTableName, std::move(partitions)));
    NO_FATALS(CheckTabletCount(kTableName, 6));
  }
}

// Create a table with mixed set of range partitions, using both table-wide and
// custom hash bucket schemas.
//
// TODO(aserbin): add verification based on PartitionSchema provided by
//                KuduTable::partition_schema() once PartitionPruner
//                recognized custom hash bucket schema for ranges
// TODO(aserbin): add InsertTestRows() when proper key encoding is implemented
TEST_F(FlexPartitioningCreateTableTest, DefaultAndCustomHashBuckets) {
  // Create a table with the following partitions:
  //
  //            hash bucket
  //   key    0               1               2               3
  //         --------------------------------------------------------------
  //  <111    x:{key}         x:{key}         -               -
  // 111-222  x:{key}         x:{key}         x:{key}         -
  // 222-333  x:{int_val}     x:{int_val}     x:{int_val}     x:{int_val}
  // 333-444  x:{key,int_val} x:{key,int_val} -               -
  constexpr const char* const kTableName = "DefaultAndCustomHashBuckets";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 2)
      .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, INT32_MIN));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 3 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(111, 222);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    table_creator->add_custom_range_partition(p.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 4 buckets with hash based on the "int_val" column with hash seed 2.
  {
    auto p = CreateRangePartition(222, 333);
    ASSERT_OK(p->add_hash_partitions({ kIntValColumn }, 4, 2));
    table_creator->add_custom_range_partition(p.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 3 buckets hashing on the { "key", "int_val" } columns with hash seed 3.
  {
    auto p = CreateRangePartition(333, 444);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn, kIntValColumn }, 2, 3));
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 11));

  // Make sure it's possible to insert rows into the table.
  //ASSERT_OK(InsertTestRows(kTableName, 111, 444));
}

// Negative tests scenarios to cover non-OK status codes for various operations
// related to custom hash bucket schema per range.
TEST_F(FlexPartitioningCreateTableTest, Negatives) {
  // Try adding hash partitions on an empty set of columns.
  {
    auto p = CreateRangePartition();
    const auto s = p->add_hash_partitions({}, 2, 0);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(), "set of columns for hash partitioning must not be empty");
  }

  // Try adding hash partitions with just one bucket.
  {
    auto p = CreateRangePartition();
    const auto s = p->add_hash_partitions({ kKeyColumn }, 1, 0);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        "at least two buckets are required to establish hash partitioning");
  }

  // Try adding hash partition on a non-existent column: appropriate error
  // surfaces during table creation.
  {
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ "nonexistent" }, 2, 0));
    const auto s = CreateTable("nicetry", std::move(partitions));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "unknown column: name: \"nonexistent\"");
  }

  // Try adding creating a table where both range splits and custom hash bucket
  // schema per partition are both specified -- that should not be possible.
  {
    RangePartition p(CreateRangePartition(0, 100));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 0));

    unique_ptr<KuduPartialRow> split(schema_.NewRow());
    ASSERT_OK(split->SetInt32(kKeyColumn, 50));

    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name("nicetry")
        .schema(&schema_)
        .num_replicas(1)
        .set_range_partition_columns({ kKeyColumn })
        .add_range_partition_split(split.release())
        .add_custom_range_partition(p.release());

    const auto s = creator->Create();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        "split rows and custom hash bucket schemas for ranges are incompatible: "
        "choose one or the other");
  }

  // Same as the sub-scenario above, but using deprecated client API to specify
  // so-called split rows.
  {
    RangePartition p(CreateRangePartition(0, 100));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 0));

    unique_ptr<KuduPartialRow> split_row(schema_.NewRow());
    ASSERT_OK(split_row->SetInt32(kKeyColumn, 50));

    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name("nicetry")
        .schema(&schema_)
        .num_replicas(1)
        .set_range_partition_columns({ kKeyColumn })
        .add_custom_range_partition(p.release());

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    creator->split_rows({ split_row.release() });
#pragma GCC diagnostic pop

    const auto s = creator->Create();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        "split rows and custom hash bucket schemas for ranges are incompatible: "
        "choose one or the other");
  }
}

} // namespace client
} // namespace kudu
