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
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_per_range_hash_schemas);
DECLARE_int32(heartbeat_interval_ms);

METRIC_DECLARE_counter(scans_started);

using kudu::client::sp::shared_ptr;
using kudu::client::KuduValue;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::master::CatalogManager;
using std::string;
using std::unique_ptr;
using std::vector;

static constexpr const char* const kKeyColumn = "key";
static constexpr const char* const kIntValColumn = "int_val";
static constexpr const char* const kStringValColumn = "string_val";

namespace kudu {
namespace client {

class FlexPartitioningTest : public KuduTest {
 public:
  FlexPartitioningTest() {
    KuduSchemaBuilder b;
    b.AddColumn(kKeyColumn)->
        Type(KuduColumnSchema::INT32)->
        NotNull()->
        PrimaryKey();
    b.AddColumn(kIntValColumn)->
        Type(KuduColumnSchema::INT32)->
        NotNull();
    b.AddColumn(kStringValColumn)->
        Type(KuduColumnSchema::STRING)->
        Nullable();
    CHECK_OK(b.Build(&schema_));
  }

  void SetUp() override {
    KuduTest::SetUp();

    // Reduce the TS<->Master heartbeat interval to speed up testing.
    FLAGS_heartbeat_interval_ms = 10;

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

  static Status FetchSessionErrors(KuduSession* session,
                                   vector<KuduError*>* errors = nullptr) {
    if (errors) {
      bool overflowed;
      session->GetPendingErrors(errors, &overflowed);
      if (PREDICT_FALSE(overflowed)) {
        return Status::RuntimeError("session error buffer overflowed");
      }
    }
    return Status::OK();
  }

  Status InsertTestRows(
      const char* table_name,
      int32_t key_beg,
      int32_t key_end,
      KuduSession::FlushMode flush_mode = KuduSession::AUTO_FLUSH_SYNC,
      vector<KuduError*>* errors = nullptr) {
    CHECK_LE(key_beg, key_end);
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name, &table));
    shared_ptr<KuduSession> session = client_->NewSession();
    RETURN_NOT_OK(session->SetFlushMode(flush_mode));
    session->SetTimeoutMillis(60000);
    for (int32_t key_val = key_beg; key_val < key_end; ++key_val) {
      if (const auto s = ApplyInsert(session.get(),
                                     table,
                                     key_val,
                                     rand(),
                                     std::to_string(rand()));
          !s.ok()) {
        RETURN_NOT_OK(FetchSessionErrors(session.get(), errors));
        return s;
      }
    }

    const auto s = session->Flush();
    if (!s.ok()) {
      RETURN_NOT_OK(FetchSessionErrors(session.get(), errors));
    }

    return s;
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

  RangePartition CreateRangePartition(int32_t lower_bound = 0,
                                      int32_t upper_bound = 100) {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    CHECK_OK(lower->SetInt32(kKeyColumn, lower_bound));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    CHECK_OK(upper->SetInt32(kKeyColumn, upper_bound));
    return unique_ptr<KuduTableCreator::KuduRangePartition>(
        new KuduTableCreator::KuduRangePartition(lower.release(),
                                                 upper.release()));
  }

  RangePartition CreateRangePartitionNoUpperBound(int32_t lower_bound) {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    CHECK_OK(lower->SetInt32(kKeyColumn, lower_bound));
    return unique_ptr<KuduTableCreator::KuduRangePartition>(
        new KuduTableCreator::KuduRangePartition(lower.release(),
                                                 schema_.NewRow()));
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

  void CheckTableRowsNum(const char* table_name,
                         int64_t expected_count) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
    ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_YOUR_WRITES));
    ASSERT_OK(scanner.Open());

    int64_t count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(expected_count, count);
  }

  void CheckScanWithColumnPredicate(const string& table_name, Slice col_name,
                                    int expected_count, int tablets, int lower, int upper) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));

    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetTimeoutMillis(60000));
    if (lower != INT8_MIN) {
      ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
          col_name, KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(lower))));
    }
    if (upper != INT8_MAX) {
      ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
          col_name, KuduPredicate::LESS, KuduValue::FromInt(upper))));
    }
    ASSERT_OK(scanner.Open());

    int64_t count = 0;
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      ASSERT_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(count, expected_count);

    NO_FATALS(CheckTabletsScanned(table_name, tablets));
  }

  void CheckTabletsScanned(const string& table_name, int tablets) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));
    vector<scoped_refptr<master::TabletInfo>> all_tablets_info;
    {
      auto* cm = cluster_->mini_master(0)->master()->catalog_manager();
      CatalogManager::ScopedLeaderSharedLock l(cm);
      scoped_refptr<master::TableInfo> table_info;
      ASSERT_OK(cm->GetTableInfo(table->id(), &table_info));
      table_info->GetAllTablets(&all_tablets_info);
    }

    int64_t tablets_scanned = 0;
    for (const auto& tablet_info : all_tablets_info) {
      for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
        scoped_refptr<tablet::TabletReplica> replica;
        ASSERT_TRUE(cluster_->mini_tablet_server(i)->server()->
        tablet_manager()->LookupTablet(tablet_info->id(), &replica));
        ASSERT_TRUE(replica->tablet()->GetMetricEntity());
        auto scans_started = METRIC_scans_started.Instantiate(replica->tablet()->GetMetricEntity());
        tablets_scanned += scans_started->value();
        scans_started->Reset();
      }
    }
    ASSERT_EQ(tablets, tablets_scanned);
  }

  void CheckTableRowsNum(const char* table_name, const char* col_name,
                         int lower, int upper, int expected_row_count) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetTimeoutMillis(60000));
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
        col_name, KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(lower))));
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
        col_name, KuduPredicate::LESS, KuduValue::FromInt(upper))));

    ASSERT_OK(scanner.Open());
    KuduScanBatch batch;
    int live_row_count = 0;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      live_row_count += batch.NumRows();
    }
    ASSERT_EQ(expected_row_count, live_row_count);
  }

  KuduSchema schema_;
  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// Test for scenarios covering range partitioning with custom bucket schemas
// specified when creating a table.
class FlexPartitioningCreateTableTest : public FlexPartitioningTest {};

TEST_F(FlexPartitioningCreateTableTest, TableWideHashSchema) {
  // Create a table with the following partitions:
  //
  //            hash bucket
  //   key    0           1
  //         -------------------------
  //  <111    x:{key}     x:{key}
  constexpr const char* const kTableName = "TableWideHashSchema";

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

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 2));
  ASSERT_OK(InsertTestRows(kTableName, -111, 111, KuduSession::MANUAL_FLUSH));
  NO_FATALS(CheckTableRowsNum(kTableName, 222));
}

TEST_F(FlexPartitioningCreateTableTest, EmptyTableWideHashSchema) {
  constexpr const char* const kTableName = "EmptyTableWideHashSchema";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules: no hash
  // bucketing at all.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, INT32_MIN));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a custom range: no hash bucketing as well.
  {
    auto p = CreateRangePartition(111, 222);
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  // There should be 2 tablets total: one per each range created.
  NO_FATALS(CheckTabletCount(kTableName, 2));
  ASSERT_OK(InsertTestRows(kTableName, -111, 222, KuduSession::MANUAL_FLUSH));
  NO_FATALS(CheckTableRowsNum(kTableName, 333));
}

// Create tables with range partitions using custom hash bucket schemas only.
TEST_F(FlexPartitioningCreateTableTest, CustomHashSchemaDiffersFromTableWide) {
  // Using one-level hash bucketing { 3, "key" } as the custom hash schema
  // for the newly created range partition. Note that the table-wide hash schema
  // is empty per FlexPartitioningTest::FlexPartitioningTest(), so the attempt
  // to create such a table fails with the Status::NotSupported() status.
  constexpr const char* const kTableName = "3@key";
  RangePartitions partitions;
  partitions.emplace_back(CreateRangePartition(0, 100));
  auto& p = partitions.back();
  ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 0));
  const auto s = CreateTable(kTableName, std::move(partitions));
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
      "varying number of hash dimensions per range is not yet supported");
}

// TODO(aserbin): re-enable this scenario once varying hash dimensions per range
//                are supported
TEST_F(FlexPartitioningCreateTableTest, DISABLED_SingleCustomRangeEmptyHashSchema) {
  // Create a table with the following partitions:
  //
  //           hash bucket
  //   key    0           1
  //         -------------------------
  //  <111    x:{key}     x:{key}
  // 111-222  -           -
  constexpr const char* const kTableName = "SingleCustomRangeEmptyHashSchema";

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

  // Add a range partition with no hash bucketing. Not calling
  // KuduRangePartition::add_hash_partitions() on the newly created range means
  // the range doesn't have any hash bucketing.
  {
    auto p = CreateRangePartition(111, 222);
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 3));

  // Make sure it's possible to insert rows into the table for all the existing
  // the partitions: first check the range of table-wide schema, then check
  // the ranges with custom hash schemas.
  ASSERT_OK(InsertTestRows(kTableName, -111, 0));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));
  ASSERT_OK(InsertTestRows(kTableName, 111, 222));
  NO_FATALS(CheckTableRowsNum(kTableName, 222));
}

// Create a table with mixed set of range partitions, using both table-wide and
// custom hash bucket schemas.
TEST_F(FlexPartitioningCreateTableTest, DefaultAndCustomHashSchemas) {
  // Create a table with the following partitions:
  //
  //            hash bucket
  //   key    0           1           2               3
  //         -----------------------------------------------------------
  //  <111    x:{key}     x:{key}     -               -
  // 111-222  x:{key}     x:{key}     x:{key}         -
  // 222-333  x:{key}     x:{key}     x:{key}     x:{key}
  // 333-444  x:{key}     x:{key}     -               -
  constexpr const char* const kTableName = "DefaultAndCustomHashSchemas";

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
  // 4 buckets with hash based on the "key" column with hash seed 2.
  {
    auto p = CreateRangePartition(222, 333);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 4, 2));
    table_creator->add_custom_range_partition(p.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 2 buckets hashing on the "key" column with hash seed 3.
  {
    auto p = CreateRangePartition(333, 444);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 3));
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 11));

  // Make sure it's possible to insert rows into the table for all the existing
  // the partitions: first check the range of table-wide schema, then check
  // the ranges with custom hash schemas.
  ASSERT_OK(InsertTestRows(kTableName, -111, 0));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));
  ASSERT_OK(InsertTestRows(kTableName, 111, 444));
  NO_FATALS(CheckTableRowsNum(kTableName, 444));

  // Meanwhile, inserting into non-covered ranges should result in a proper
  // error status return to the client attempting such an operation.
  {
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    auto s = InsertTestRows(
        kTableName, 444, 445, KuduSession::AUTO_FLUSH_SYNC, &errors);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    ASSERT_EQ(1, errors.size());
    const auto& err = errors[0]->status();
    EXPECT_TRUE(err.IsNotFound()) << err.ToString();
    ASSERT_STR_CONTAINS(err.ToString(),
                        "No tablet covering the requested range partition");
  }
  // Try same as in the scope above, but do so for multiple rows to make sure
  // custom hash bucketing isn't inducing any unexpected side effects.
  {
    constexpr int kNumRows = 10;
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    auto s = InsertTestRows(
        kTableName, 445, 445 + kNumRows, KuduSession::MANUAL_FLUSH, &errors);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    ASSERT_EQ(kNumRows, errors.size());
    for (const auto& e : errors) {
      const auto& err = e->status();
      EXPECT_TRUE(err.IsNotFound()) << err.ToString();
      ASSERT_STR_CONTAINS(err.ToString(),
                          "No tablet covering the requested range partition");
    }
  }
}

// Parameters for a single hash dimension.
struct HashDimensionParameters {
  vector<string> columns; // names of the columns to use for hash bucketing
  int32_t num_buckets;    // number of hash buckets
};
std::ostream& operator<<(std::ostream& os,
                         const HashDimensionParameters& params) {
  os << "{columns:{";
  for (const auto& c : params.columns) {
    os << c << ",";
  }
  os << "},";
  os << "num_buckets:" << params.num_buckets << ",";
  return os;
}

// Hash bucketing parameters for a range partition.
typedef vector<HashDimensionParameters> HashPartitionParameters;
std::ostream& operator<<(std::ostream& os,
                         const HashPartitionParameters& params) {
  os << "{";
  for (const auto& e : params) {
    os << e << ",";
  }
  os << "}";
  return os;
}

// Range partition bounds.
struct RangeBounds {
  int32_t lower;  // inclusive lower bound
  int32_t upper;  // exclusive upper bound
};

// The whole set of parameters per range.
struct PerRangeParameters {
  RangeBounds bounds;
  HashPartitionParameters hash_params;
};
std::ostream& operator<<(std::ostream& os,
                         const PerRangeParameters& params) {
  os << "{bounds:["
     << params.bounds.lower << ","
     << params.bounds.upper << "),{";
  for (const auto& hp : params.hash_params) {
    os << hp << ",";
  }
  os << "}}";
  return os;
}

typedef vector<PerRangeParameters> TablePartitionParameters;
std::ostream& operator<<(std::ostream& os,
                         const TablePartitionParameters& params) {
  os << "{";
  for (const auto& per_range_params : params) {
    os << per_range_params;
  }
  os << "}";
  return os;
}

struct PerRangeTestParameters {
  bool allowed;
  TablePartitionParameters partition_params;
  vector<HashDimensionParameters> table_wide_hash_schema_params;
};
std::ostream& operator<<(std::ostream& os,
                         const PerRangeTestParameters& params) {
  os << "allowed:" << std::boolalpha << params.allowed
     << " partition_params:" << params.partition_params;
  return os;
}

class FlexPartitioningCreateTableParamTest :
    public FlexPartitioningCreateTableTest,
    public testing::WithParamInterface<PerRangeTestParameters> {
 public:
  FlexPartitioningCreateTableParamTest() {
    KuduSchemaBuilder b;
    b.AddColumn(kKeyColumn)->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn(kIntValColumn)->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn(kStringValColumn)->Type(KuduColumnSchema::STRING);
    b.SetPrimaryKey({ kKeyColumn, kIntValColumn });
    CHECK_OK(b.Build(&schema_));
  }

  void Run() {
    constexpr const char* const kTableName = "PerRangeHashSchemaVariations";
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(kTableName)
        .schema(&schema_)
        .num_replicas(1)
        .set_range_partition_columns({ kKeyColumn });

    const PerRangeTestParameters& params = GetParam();
    for (const auto& per_range_params : params.partition_params) {
      auto p = CreateRangePartition(per_range_params.bounds.lower,
                                    per_range_params.bounds.upper);
      const auto& hash_params = per_range_params.hash_params;
      for (const auto& hd : hash_params) {
        ASSERT_OK(p->add_hash_partitions(hd.columns, hd.num_buckets, 0));
      }
      table_creator->add_custom_range_partition(p.release());
    }

    for (const auto& table_wide_params : params.table_wide_hash_schema_params) {
      table_creator->add_hash_partitions(table_wide_params.columns,
                                         table_wide_params.num_buckets);
    }

    if (params.allowed) {
      ASSERT_OK(table_creator->Create());
    } else {
      const auto s = table_creator->Create();
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(),
          "varying number of hash dimensions per range is not yet supported");
    }
  }
};

const vector<PerRangeTestParameters> kPerRangeTestParameters = {
  // A single range with no hashing at all.
  {
    true,
    {
      { { INT32_MIN, 0 }, {} },
    },
    {}  // empty table-wide hash schema
  },
  // A single range with minimal hash schema.
  {
    true,
    {
      { { INT32_MIN, 0 }, { { { kKeyColumn }, 2, } } },
    },
    {
      { { kKeyColumn }, 3 },
    }
  },
  // A single range with hashing on both columns of the primary key.
  {
    true,
    {
      { { INT32_MIN, 0 }, { { { kKeyColumn, kIntValColumn }, 2, } } },
    },
    {
      { { kKeyColumn }, 5 },
    }
  },
  // Varying the number of hash buckets.
  {
    true,
    {
      { {   0, 100 }, { { { kKeyColumn }, 2, } } },
      { { 100, 200 }, { { { kKeyColumn }, 3, } } },
      { { 200, 300 }, { { { kKeyColumn }, 2, } } },
      { { 300, 400 }, { { { kKeyColumn }, 10, } } },
    },
    {
      { { kIntValColumn }, 5 },
    }
  },
  // Varying the set of columns for hash bucketing.
  {
    true,
    {
      { { 100, 200 }, { { { kKeyColumn }, 2, } } },
      { { 200, 300 }, { { { kKeyColumn, kIntValColumn }, 2, } } },
      { { 300, 400 }, { { { kIntValColumn }, 2, } } },
      { { 400, 500 }, { { { kIntValColumn, kKeyColumn }, 2, } } },
    },
    {
      { { kIntValColumn }, 3 },
    }
  },
  // Varying the set of columns for hash bucketing and number of buckets.
  {
    true,
    {
      { { 200, 300 }, { { { kKeyColumn }, 10, } } },
      { { 300, 400 }, { { { kKeyColumn, kIntValColumn }, 2, } } },
      { { 400, 500 }, { { { kKeyColumn, kIntValColumn }, 5, } } },
      { { 500, 600 }, { { { kIntValColumn }, 8, } } },
      { { 600, 700 }, { { { kKeyColumn }, 3, } } },
      { { 700, 800 }, { { { kIntValColumn, kKeyColumn }, 16, } } },
      { { 800, 900 }, { { { kIntValColumn }, 32, } } },
    },
    {
      { { kIntValColumn }, 7 },
    }
  },
  // Below are multiple scenarios with varying number of hash dimensions
  // for hash bucketing.
  {
    false,
    {
      { { INT32_MIN, 0 }, {} },
      { { 0, 100 }, { { { kKeyColumn }, 2 } } },
    },
    {}  // an empty table-wide schema
  },
  {
    false,
    {
      { { INT32_MIN, 0 }, { { { kKeyColumn }, 3 } } },
      { { 0, 100 }, {} },
    },
    {
      { { kKeyColumn }, 8 },
    }
  },
  {
    false,
    {
      { { INT32_MIN, 0 }, {} },
      { {   0, 100 }, { { { kKeyColumn }, 2 } } },
      { { 100, 200 }, {} },
    },
    {}  // an empty table-wide schema
  },
  {
    false,
    {
      { { INT32_MIN, 0 }, { { { kKeyColumn }, 3 } } },
      { {   0, 100 }, {} },
      { { 100, 200 }, { { { kIntValColumn }, 2 } } },
    },
    {}  // an empty table-wide schema
  },
  {
    false,
    {
      { {  0,  100 }, { { { kKeyColumn }, 2 }, { { kIntValColumn }, 2 } } },
      { { 100, 200 }, { { { kKeyColumn, kIntValColumn }, 2 } } },
    },
    {
      { { kKeyColumn }, 7 },
      { { kIntValColumn }, 3 },
    }
  },
  {
    false,
    {
      { { 100, 200 }, { { { kKeyColumn }, 2 }, { { kIntValColumn }, 3 } } },
      { { 200, 300 }, { { { kKeyColumn }, 3 } } },
    },
    {
      { { kKeyColumn }, 7 },
    }
  },
  {
    false,
    {
      { { 300, 400 }, { { { kKeyColumn }, 2 }, { { kIntValColumn }, 3 } } },
      { { 400, 500 }, { { { kKeyColumn }, 3 } } },
    },
    {
      { { kKeyColumn }, 7 },
    }
  },
  {
    false,
    {
      { { 500, 600 }, { { { kKeyColumn }, 10 }, { { kIntValColumn }, 5 } } },
      { { 600, 700 }, { { { kKeyColumn }, 2 } } },
      { { 700, INT32_MAX }, {} },
    },
    {}  // an empty table-wide schema
  },
};
INSTANTIATE_TEST_SUITE_P(Variations, FlexPartitioningCreateTableParamTest,
                         testing::ValuesIn(kPerRangeTestParameters));
TEST_P(FlexPartitioningCreateTableParamTest, PerRangeHashSchemaVariations) {
  NO_FATALS(Run());
}

TEST_F(FlexPartitioningCreateTableTest, CustomHashSchemasVaryingDimensionsNumber) {
  constexpr const char* const kTableName = "CustomHashSchemasVaryingDimensions";

  // Try creating a table with the following partitions:
  //
  //            hash bucket
  //   key    0           1
  //         -------------------------
  //  <111    x:{key}     x:{key}
  // 111-222  -           -
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

  // Add a range partition with no hash bucketing. Not calling
  // KuduRangePartition::add_hash_partitions() on the newly created range
  // means the range doesn't have any hash bucketing.
  {
    auto p = CreateRangePartition(111, 222);
    table_creator->add_custom_range_partition(p.release());
  }

  const auto s = table_creator->Create();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
      "varying number of hash dimensions per range is not yet supported");
}

// This test scenario creates a table with a range partition having no upper
// bound. The range partition has a custom empty hash schema (i.e. no hash
// bucketing for the range) in the presence of non-empty table-wide hash schema.
//
// TODO(aserbin): re-enable this scenario once varying hash dimensions per range
//                are supported
TEST_F(FlexPartitioningCreateTableTest, DISABLED_NoUpperBoundRangeCustomHashSchema) {
  // Create a table with the following partitions:
  //
  //            hash bucket
  //   key    0           1           2
  //         --------------------------------
  //   0-111  x:{key}     x:{key}     x:{key}
  // 111-222  x:{key}     x:{key}     -
  //  >=222   -           -           -
  constexpr const char* const kTableName = "NoUpperBoundRangeCustomHashSchema";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 3)
      .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 2 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(111, 222);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 0));
    table_creator->add_custom_range_partition(p.release());
  }

  // Add unbounded range partition with no hash bucketing.
  {
    auto p = CreateRangePartitionNoUpperBound(222);
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 6));

  // Make sure it's possible to insert rows into the table for all the existing
  // partitions.
  ASSERT_OK(InsertTestRows(kTableName, 0, 555));
  NO_FATALS(CheckTableRowsNum(kTableName, 555));
}

TEST_F(FlexPartitioningCreateTableTest, ScansWithRangePredicates) {

  constexpr const char* const kTableName = "ScansWithRangePredicates";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
    .schema(&schema_)
    .num_replicas(1)
    .add_hash_partitions({ kKeyColumn }, 2)
    .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 4 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(111, 222);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 4, 1));
    table_creator->add_custom_range_partition(p.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 3 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(222, 333);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 0, 111));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 111, 222));
  NO_FATALS(CheckTableRowsNum(kTableName, 222));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 222, 333));
  NO_FATALS(CheckTableRowsNum(kTableName, 333));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  // Filler values for lower or upper bound when it's not meant to be set.
  constexpr int lower_filler = INT8_MIN;
  constexpr int upper_filler = INT8_MAX;

  CheckScanWithColumnPredicate(kTableName, "key", 150, 6,  0, 150);
  CheckScanWithColumnPredicate(kTableName, "key", 183, 7, 150, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 75, 2, 0, 75);
  CheckScanWithColumnPredicate(kTableName, "key", 175, 9, 75, 250);
  CheckScanWithColumnPredicate(kTableName, "key", 83, 3, 250, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 0, 1);
  CheckScanWithColumnPredicate(kTableName, "key", 2, 6, 110, 112);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, -10, -5);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 350, 400);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 332, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 333, 334);
  CheckScanWithColumnPredicate(kTableName, "key", 100, 2, lower_filler, 100);
  CheckScanWithColumnPredicate(kTableName, "key", 250, 9, lower_filler, 250);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, lower_filler, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, lower_filler, 334);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, -10, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 233, 9, 100, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 133, 7, 200, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 83, 3, 250, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 333, upper_filler);

  // Meanwhile, inserting into non-covered ranges should result in a proper
  // error status return to the client attempting such an operation.
  {
    constexpr int kNumRows = 10;
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    auto s = InsertTestRows(
        kTableName, 334, 334 + kNumRows, KuduSession::MANUAL_FLUSH, &errors);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    ASSERT_EQ(kNumRows, errors.size());
    for (const auto& e : errors) {
      const auto& err = e->status();
      EXPECT_TRUE(err.IsNotFound()) << err.ToString();
      ASSERT_STR_CONTAINS(err.ToString(),
                          "No tablet covering the requested range partition");
    }
  }
}

TEST_F(FlexPartitioningCreateTableTest, ScansWithRangePredicatesWithSameHashBuckets) {

  constexpr const char* const kTableName = "ScansWithRangePredicatesWithSameHashBuckets";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
  .schema(&schema_)
  .num_replicas(1)
  .add_hash_partitions({ kKeyColumn }, 3)
  .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 111));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 222));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 222));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 333));
    table_creator->add_range_partition(lower.release(), upper.release());
  }


  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 0, 111));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 111, 222));
  NO_FATALS(CheckTableRowsNum(kTableName, 222));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 222, 333));
  NO_FATALS(CheckTableRowsNum(kTableName, 333));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  // Filler values for lower or upper bound when it's not meant to be set.
  constexpr int lower_filler = INT8_MIN;
  constexpr int upper_filler = INT8_MAX;

  // The test cases commented out below fail. Initially, it was thought to be a regression but these
  // failures are not reproducible in a real cluster.
  CheckScanWithColumnPredicate(kTableName, "key", 150, 6, 0, 150);
  CheckScanWithColumnPredicate(kTableName, "key", 183, 6, 150, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 75, 3, 0, 75);
  CheckScanWithColumnPredicate(kTableName, "key", 175, 9, 75, 250);
  CheckScanWithColumnPredicate(kTableName, "key", 83, 3, 250, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 0, 1);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 110, 111);
  CheckScanWithColumnPredicate(kTableName, "key", 3, 6, 110, 113);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 332, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 333, 334);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, -10, -5);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 350, 400);
  CheckScanWithColumnPredicate(kTableName, "key", 100, 3, lower_filler, 100);
  CheckScanWithColumnPredicate(kTableName, "key", 250, 9, lower_filler, 250);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, lower_filler, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, lower_filler, 334);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, -10, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 233, 9, 100, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 133, 6, 200, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 83, 3, 250, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 333, upper_filler);

  // Meanwhile, inserting into non-covered ranges should result in a proper
  // error status return to the client attempting such an operation.
  {
    constexpr int kNumRows = 10;
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    auto s = InsertTestRows(
        kTableName, 334, 334 + kNumRows, KuduSession::MANUAL_FLUSH, &errors);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    ASSERT_EQ(kNumRows, errors.size());
    for (const auto& e : errors) {
      const auto& err = e->status();
      EXPECT_TRUE(err.IsNotFound()) << err.ToString();
      ASSERT_STR_CONTAINS(err.ToString(),
                          "No tablet covering the requested range partition");
    }
  }
}

TEST_F(FlexPartitioningCreateTableTest, ScansWithNonCoveringRanges) {

  constexpr const char* const kTableName = "ScansWithNonCoveringRanges";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
  .schema(&schema_)
  .num_replicas(1)
  .add_hash_partitions({ kKeyColumn }, 2)
  .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with custom hash sub-partitioning rules:
  // 4 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(0, 111);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 4, 1));
    table_creator->add_custom_range_partition(p.release());
  }

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 222));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 333));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 3 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(444, 555);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    table_creator->add_custom_range_partition(p.release());
  }

  ASSERT_OK(table_creator->Create());
  NO_FATALS(CheckTabletCount(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 0, 111));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 222, 333));
  NO_FATALS(CheckTableRowsNum(kTableName, 222));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  ASSERT_OK(InsertTestRows(kTableName, 444, 555));
  NO_FATALS(CheckTableRowsNum(kTableName, 333));
  NO_FATALS(CheckTabletsScanned(kTableName, 9));

  // Filler values for lower or upper bound when it's not meant to be set.
  constexpr int lower_filler = INT8_MIN;
  constexpr int upper_filler = INT8_MAX;

  CheckScanWithColumnPredicate(kTableName, "key", 111, 4, -10, 111);
  CheckScanWithColumnPredicate(kTableName, "key", 111, 4, -10, 150);
  CheckScanWithColumnPredicate(kTableName, "key", 111, 2, 150, 333);
  CheckScanWithColumnPredicate(kTableName, "key", 33, 6, 100, 244);
  CheckScanWithColumnPredicate(kTableName, "key", 111, 2, 144, 344);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, -50, 600);
  CheckScanWithColumnPredicate(kTableName, "key", 222, 5, 150, 600);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 0, 1);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, -10, -5);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 600, 650);
  CheckScanWithColumnPredicate(kTableName, "key", 1, 1, 554, 555);
  CheckScanWithColumnPredicate(kTableName, "key", 0, 0, 555, 556);
  CheckScanWithColumnPredicate(kTableName, "key", 111, 4, lower_filler, 150);
  CheckScanWithColumnPredicate(kTableName, "key", 222, 6, lower_filler, 350);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, lower_filler, 560);
  CheckScanWithColumnPredicate(kTableName, "key", 333, 9, -10, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 222, 5, 150, upper_filler);
  CheckScanWithColumnPredicate(kTableName, "key", 111, 3, 350, upper_filler);
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

  {
    constexpr const char* const kTableName = "3@key_x_3@key";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 0));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    const auto s = CreateTable(kTableName, std::move(partitions));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "hash bucket schema components must not contain "
                        "columns in common");
  }

  {
    constexpr const char* const kTableName = "3@key_x_3@key:int_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 0));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn, kIntValColumn }, 3, 1));
    const auto s = CreateTable(kTableName, std::move(partitions));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "hash bucket schema components must not contain "
                        "columns in common");
  }

  {
    constexpr const char* const kTableName = "3@int_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kIntValColumn }, 2, 0));
    const auto s = CreateTable(kTableName, std::move(partitions));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "must specify only primary key columns for hash "
                        "bucket partition components");
  }

  {
    constexpr const char* const kTableName = "3@string_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kStringValColumn }, 5, 0));
    const auto s = CreateTable(kTableName, std::move(partitions));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "must specify only primary key columns for hash "
                        "bucket partition components");
  }

  {
    constexpr const char* const kTableName = "2@key_x_3@int_val";
    RangePartitions partitions;
    partitions.emplace_back(CreateRangePartition());
    auto& p = partitions.back();
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 0));
    ASSERT_OK(p->add_hash_partitions({ kIntValColumn }, 3, 1));
    const auto s = CreateTable(kTableName, std::move(partitions));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "must specify only primary key columns for hash "
                        "bucket partition components");
  }
}

// When working with a cluster that doesn't support range-specific hash schemas
// for tables, the client should receive proper error while trying to create
// a table with custom hash schema for at least one of its ranges.
TEST_F(FlexPartitioningCreateTableTest, UnsupportedRangeSpecificHashSchema) {
  // Turn off the support for range-specific hash schemas, emulating the
  // situation when a Kudu cluster is running an older version released prior
  // to the introduction of the feature.
  FLAGS_enable_per_range_hash_schemas = false;

  constexpr const char* const kTableName =
      "UnsupportedRangeSpecificHashSchemaCreateTable";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 3)
      .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());
  ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
  table_creator->add_range_partition(lower.release(), upper.release());

  // Add a range partition with custom hash schema.
  auto p = CreateRangePartition(111, 222);
  ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 5, 0));
  table_creator->add_custom_range_partition(p.release());

  const auto s = table_creator->Create();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "cluster does not support CreateTable with "
                                    "feature(s) RANGE_SPECIFIC_HASH_SCHEMA");
}

// Test for scenarios covering range partitioning with custom hash schemas
// specified when adding a new custom hash schema partition to a table.
class FlexPartitioningAlterTableTest : public FlexPartitioningTest {};

TEST_F(FlexPartitioningAlterTableTest, AddAndDropCustomRangePartition) {
  // Create a table with the following partitions:
  //
  //            hash bucket
  //   key    0           1
  //         -------------------------
  // <111    x:{key}     x:{key}
  //
  // Later add the following partition:
  //            hash bucket
  //   key    0           1           2
  //         --------------------------------
  // 111-222 x:{key}     x:{key}     x:{key}
  constexpr const char *const kTableName = "AddAndDropCustomRangePartition";

  // Create a table with a single Range partition.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  ASSERT_OK(lower->SetInt32(kKeyColumn, INT32_MIN));
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());
  ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 2)
      .set_range_partition_columns({ kKeyColumn })
      .add_range_partition(lower.release(), upper.release());
  ASSERT_OK(table_creator->Create());

  NO_FATALS(CheckTabletCount(kTableName, 2));

  // Add a range partition with custom hash sub-partitioning rules:
  // 3 buckets with hash based on the "key" column with hash seed 1.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    auto p = CreateRangePartition(111, 222);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    table_alterer->AddRangePartition(p.release());
    ASSERT_OK(table_alterer->Alter());
  }

  NO_FATALS(CheckTabletCount(kTableName, 5));

  // Try adding a range partition with custom hash sub-partitioning rules:
  // 3 buckets with hash based on the "key" column with hash seed 1.
  // 2 buckets with hash based on the "key" column with hash seed 1.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    auto p = CreateRangePartition(222, 333);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 1));
    table_alterer->AddRangePartition(p.release());
    const auto s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "varying number of hash dimensions per range is not yet supported");
  }

  // Drop the successfully created partition.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lower_drop(schema_.NewRow());
    ASSERT_OK(lower_drop->SetInt32(kKeyColumn, 111));
    unique_ptr<KuduPartialRow> upper_drop(schema_.NewRow());
    ASSERT_OK(upper_drop->SetInt32(kKeyColumn, 222));
    table_alterer->DropRangePartition(lower_drop.release(), upper_drop.release());
    ASSERT_OK(table_alterer->Alter());
  }

  NO_FATALS(CheckTabletCount(kTableName, 2));
}

TEST_F(FlexPartitioningAlterTableTest, ReadAndWriteToCustomRangePartition) {
  // Create a table with the following partitions:
  //
  //            hash bucket
  //   key    0           1           2               3
  //         -----------------------------------------------------------
  //  <111    x:{key}     x:{key}     -               -

  // Later add the following partitions:
  //            hash bucket
  //   key    0           1           2               3
  //         -----------------------------------------------------------
  // 111-222  x:{key}     x:{key}     x:{key}         -
  // 222-333  x:{key}     x:{key}     x:{key}     x:{key}
  // 333-444  x:{key}     x:{key}     -               -

  constexpr const char* const kTableName = "ReadAndWriteToCustomRangePartition";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  ASSERT_OK(lower->SetInt32(kKeyColumn, INT32_MIN));
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());
  ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 2)
      .set_range_partition_columns({ kKeyColumn })
      .add_range_partition(lower.release(), upper.release());
  ASSERT_OK(table_creator->Create());

  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  // Add a range partition with custom hash sub-partitioning rules:
  // 3 buckets with hash based on the "key" column with hash seed 1.
  {
    auto p = CreateRangePartition(111, 222);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 3, 1));
    table_alterer->AddRangePartition(p.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 4 buckets with hash based on the "key" column with hash seed 2.
  {
    auto p = CreateRangePartition(222, 333);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 4, 2));
    table_alterer->AddRangePartition(p.release());
  }

  // Add a range partition with custom hash sub-partitioning rules:
  // 2 buckets hashing on the "key" column with hash seed 3.
  {
    auto p = CreateRangePartition(333, 444);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 2, 3));
    table_alterer->AddRangePartition(p.release());
  }

  ASSERT_OK(table_alterer->Alter());
  NO_FATALS(CheckTabletCount(kTableName, 11));

  // Make sure it's possible to insert rows into the table for all the existing
  // the partitions: first check the range of table-wide schema, then check
  // the ranges with custom hash schemas.
  ASSERT_OK(InsertTestRows(kTableName, 0, 111));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));
  ASSERT_OK(InsertTestRows(kTableName, 111, 444));
  NO_FATALS(CheckTableRowsNum(kTableName, 444));

  NO_FATALS(CheckTableRowsNum(kTableName, kKeyColumn, 111, 222, 111));
  NO_FATALS(CheckTableRowsNum(kTableName, kKeyColumn, 0, 444, 444));
  // Drop a partition and re-scan
  {
    unique_ptr<KuduTableAlterer> table_alterer_drop(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lower_drop(schema_.NewRow());
    ASSERT_OK(lower_drop->SetInt32(kKeyColumn, 111));
    unique_ptr<KuduPartialRow> upper_drop(schema_.NewRow());
    ASSERT_OK(upper_drop->SetInt32(kKeyColumn, 222));
    table_alterer_drop->DropRangePartition(lower_drop.release(), upper_drop.release());
    ASSERT_OK(table_alterer_drop->Alter());
  }
  NO_FATALS(CheckTableRowsNum(kTableName, kKeyColumn, 0, 444, 333));

  // Meanwhile, inserting into non-covered ranges should result in a proper
  // error status return to the client attempting such an operation.
  {
    constexpr int kNumRows = 10;
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    auto s = InsertTestRows(
        kTableName, 445, 445 + kNumRows, KuduSession::MANUAL_FLUSH, &errors);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    ASSERT_EQ(kNumRows, errors.size());
    for (const auto& e : errors) {
      const auto& err = e->status();
      EXPECT_TRUE(err.IsNotFound()) << err.ToString();
      ASSERT_STR_CONTAINS(err.ToString(),
                          "No tablet covering the requested range partition");
    }
  }
}

// When working with a cluster that doesn't support range-specific hash schemas
// for tables, the client should receive proper error while trying to add
// a range with custom hash schema.
TEST_F(FlexPartitioningAlterTableTest, UnsupportedRangeSpecificHashSchema) {
  // Turn off the support for range-specific hash schemas, emulating the
  // situation when a Kudu cluster is running an older version released prior
  // to the introduction of the feature.
  FLAGS_enable_per_range_hash_schemas = false;

  constexpr const char* const kTableName =
      "UnsupportedRangeSpecificHashSchemaAlterTable";
  constexpr const char* const kErrMsg = "cluster does not support AlterTable "
      "with feature(s) RANGE_SPECIFIC_HASH_SCHEMA";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 2)
      .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());
  ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
  table_creator->add_range_partition(lower.release(), upper.release());

  ASSERT_OK(table_creator->Create());

  // Try to add a single range with custom hash schema.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    auto p = CreateRangePartition(111, 222);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 5, 0));
    alterer->AddRangePartition(p.release());

    const auto s = alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), kErrMsg);
  }

  // Try to add a mix of ranges: with the table-wide and custom hash schemas.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));

    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 111));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 222));
    alterer->AddRangePartition(lower.release(), upper.release());

    auto p = CreateRangePartition(222, 333);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 8, 0));
    alterer->AddRangePartition(p.release());

    const auto s = alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), kErrMsg);
  }

  // Temporary enable support for the RANGE_SPECIFIC_HASH_SCHEMA feature to add
  // a new range with custom hash schema to be dropped later.
  FLAGS_enable_per_range_hash_schemas = true;
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));

    auto p0 = CreateRangePartition(111, 222);
    ASSERT_OK(p0->add_hash_partitions({ kKeyColumn }, 5, 0));
    alterer->AddRangePartition(p0.release());

    auto p1 = CreateRangePartition(222, 333);
    ASSERT_OK(p1->add_hash_partitions({ kKeyColumn }, 8, 0));
    alterer->AddRangePartition(p1.release());

    ASSERT_OK(alterer->Alter());
  }
  // Disable the support for the RANGE_SPECIFIC_HASH_SCHEMA.
  FLAGS_enable_per_range_hash_schemas = false;

  // Dropping ranges with the table-wide or custom hash schemas should be fine
  // even if the cluster doesn't support the RANGE_SPECIFIC_HASH_SCHEMA schema
  // It's rather a hypothetical situation unless they toggle the flag after
  // adding ranges with custom hash schemas or running older Kudu binaries with
  // new data.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 111));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 222));
    alterer->DropRangePartition(lower.release(), upper.release());
    ASSERT_OK(alterer->Alter());
  }

  // Dropping tables having ranges with custom hash schema should be fine
  // even if the server side has no RANGE_SPECIFIC_HASH_SCHEMA feature.
  ASSERT_OK(client_->DeleteTable(kTableName));
}

// Make sure adding and dropping ranges with the table-wide hash schema works
// as expected.
TEST_F(FlexPartitioningAlterTableTest, AddDropTableWideHashSchemaPartitions) {
  constexpr const char* const kTableName =
      "AddDropTableWideHashSchemaPartitions";

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ kKeyColumn }, 2)
      .set_range_partition_columns({ kKeyColumn });

  // Add a range partition with the table-wide hash partitioning rules.
  {
    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
    table_creator->add_range_partition(lower.release(), upper.release());
  }

  ASSERT_OK(table_creator->Create());

  ASSERT_OK(InsertTestRows(kTableName, 0, 111));
  NO_FATALS(CheckTableRowsNum(kTableName, 111));

  // To have mix of ranges, add a single range with custom hash schema.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    auto p = CreateRangePartition(-111, 0);
    ASSERT_OK(p->add_hash_partitions({ kKeyColumn }, 5, 1));
    alterer->AddRangePartition(p.release());
    ASSERT_OK(alterer->Alter());
  }

  ASSERT_OK(InsertTestRows(kTableName, -111, 0));
  NO_FATALS(CheckTableRowsNum(kTableName, 222));

  // Add one more range partition with the table-wide hash schema.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));

    unique_ptr<KuduPartialRow> lower(schema_.NewRow());
    ASSERT_OK(lower->SetInt32(kKeyColumn, 111));
    unique_ptr<KuduPartialRow> upper(schema_.NewRow());
    ASSERT_OK(upper->SetInt32(kKeyColumn, 222));
    alterer->AddRangePartition(lower.release(), upper.release());

    ASSERT_OK(alterer->Alter());
  }

  ASSERT_OK(InsertTestRows(kTableName, 111, 222));
  NO_FATALS(CheckTableRowsNum(kTableName, 333));

  // Drop the ranges with the table-wide hash partitions.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));

    {
      unique_ptr<KuduPartialRow> lower(schema_.NewRow());
      ASSERT_OK(lower->SetInt32(kKeyColumn, 111));
      unique_ptr<KuduPartialRow> upper(schema_.NewRow());
      ASSERT_OK(upper->SetInt32(kKeyColumn, 222));
      alterer->DropRangePartition(lower.release(), upper.release());
    }
    {
      unique_ptr<KuduPartialRow> lower(schema_.NewRow());
      ASSERT_OK(lower->SetInt32(kKeyColumn, 0));
      unique_ptr<KuduPartialRow> upper(schema_.NewRow());
      ASSERT_OK(upper->SetInt32(kKeyColumn, 111));
      alterer->DropRangePartition(lower.release(), upper.release());
    }

    ASSERT_OK(alterer->Alter());
  }

  NO_FATALS(CheckTableRowsNum(kTableName, 111));

  ASSERT_OK(client_->DeleteTable(kTableName));
}

} // namespace client
} // namespace kudu
