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
//
// Integration test for flexible partitioning (eg buckets, range partitioning
// of PK subsets, etc).

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::none;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanner;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::MasterErrorPB;
using kudu::rpc::RpcController;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

static const char* const kTableName = "test-table";

struct HashPartitionOptions {
  vector<string> columns;
  int32_t num_buckets;
};

struct RangePartitionOptions {
  vector<string> columns;
  vector<vector<int32_t>> splits;
  vector<pair<vector<int32_t>, vector<int32_t>>> bounds;
};

int NumPartitions(const vector<HashPartitionOptions>& hash_partitions,
                  const RangePartitionOptions& range_partition) {
  int partitions = std::max(1UL, range_partition.bounds.size()) + range_partition.splits.size();
  for (const auto& hash_partition : hash_partitions) {
    partitions *= hash_partition.num_buckets;
  }
  return partitions;
}

string RowToString(const vector<int32_t> row) {
  string s = "(";
  for (int i = 0; i < row.size(); i++) {
    if (i != 0) s.append(", ");
    s.append(std::to_string(row[i]));
  }
  s.append(")");
  return s;
}

string PartitionOptionsToString(const vector<HashPartitionOptions>& hash_partitions,
                                const RangePartitionOptions& range_partition) {
  string s;
  for (const auto& hash_partition : hash_partitions) {
    s.append("HASH (");
    for (int i = 0; i < hash_partition.columns.size(); i++) {
      if (i != 0) s.append(", ");
      s.append(hash_partition.columns[i]);
    }
    s.append(") INTO ");
    s.append(std::to_string(hash_partition.num_buckets));
    s.append(" BUCKETS, ");
  }

  s.append("RANGE (");
  for (int i = 0; i < range_partition.columns.size(); i++) {
    if (i != 0) s.append(", ");
    s.append(range_partition.columns[i]);
  }
  s.append(")");

  if (!range_partition.splits.empty()) {
    s.append(" SPLIT ROWS ");

    for (int i = 0; i < range_partition.splits.size(); i++) {
      if (i != 0) s.append(", ");
      s.append(RowToString(range_partition.splits[i]));
    }
  }

  if (!range_partition.bounds.empty()) {
    s.append(" BOUNDS (");

    for (int i = 0; i < range_partition.bounds.size(); i++) {
      if (i != 0) s.append(", ");
      s.append("[");
      s.append(RowToString(range_partition.bounds[i].first));
      s.append(", ");
      s.append(RowToString(range_partition.bounds[i].second));
      s.append(")");
    }
    s.append(")");
  }
  return s;
}

typedef std::tuple<vector<HashPartitionOptions>, RangePartitionOptions> TestParamType;

class FlexPartitioningITest : public KuduTest,
                              public testing::WithParamInterface<TestParamType> {
 public:
  FlexPartitioningITest()
    : random_(GetRandomSeed32()) {
  }

  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    // This test produces lots of tablets. With container and log preallocation,
    // we end up using quite a bit of disk space. So, we disable them.
    opts.extra_tserver_flags.emplace_back("--log_container_preallocate_bytes=0");
    opts.extra_tserver_flags.emplace_back("--log_preallocate_segments=false");
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());

    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

 protected:

  void TestPartitionOptions(const vector<HashPartitionOptions> hash_options,
                            const RangePartitionOptions range_options) {
    NO_FATALS(CreateTable(hash_options, range_options));
    NO_FATALS(InsertAndVerifyScans(range_options));
    DeleteTable();
  }

  void CreateTable(const vector<HashPartitionOptions> hash_partitions,
                   const RangePartitionOptions range_partition) {
    KuduSchemaBuilder b;
    b.AddColumn("c0")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("c1")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("c2")->Type(KuduColumnSchema::INT32)->NotNull();
    b.SetPrimaryKey({ "c0", "c1", "c2" });
    KuduSchema schema;
    ASSERT_OK(b.Build(&schema));

    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(kTableName)
        .schema(&schema)
        .num_replicas(1);

    for (const auto& hash_partition : hash_partitions) {
      table_creator->add_hash_partitions(hash_partition.columns, hash_partition.num_buckets);
    }

    vector<const KuduPartialRow*> split_rows;
    for (const vector<int32_t> split : range_partition.splits) {
      KuduPartialRow* row = schema.NewRow();
      for (int i = 0; i < split.size(); i++) {
        ASSERT_OK(row->SetInt32(range_partition.columns[i], split[i]));
      }
      split_rows.push_back(row);
    }

    table_creator->set_range_partition_columns(range_partition.columns);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    table_creator->split_rows(split_rows);
#pragma GCC diagnostic pop

    for (const auto& bound : range_partition.bounds) {
      KuduPartialRow* lower = schema.NewRow();
      KuduPartialRow* upper = schema.NewRow();

      for (int i = 0; i < bound.first.size(); i++) {
        ASSERT_OK(lower->SetInt32(range_partition.columns[i], bound.first[i]));
      }
      for (int i = 0; i < bound.second.size(); i++) {
        ASSERT_OK(upper->SetInt32(range_partition.columns[i], bound.second[i]));
      }
      table_creator->add_range_partition(lower, upper);
    }

    ASSERT_OK(table_creator->Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
    ASSERT_EQ(NumPartitions(hash_partitions, range_partition), CountTablets());
  }

  void DeleteTable() {
    inserted_rows_.clear();
    client_->DeleteTable(table_->name());
    table_.reset();
  }

  int CountTablets() {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    req.set_max_returned_locations(100);

    for (int i = 1; ; i++) {
      CHECK_LE(i, 10) << "CountTablets timed out";

      controller.Reset();
      CHECK_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));

      if (resp.has_error()) {
        CHECK_EQ(MasterErrorPB::TABLET_NOT_RUNNING, resp.error().code());
        SleepFor(MonoDelta::FromMilliseconds(i * i * 100));
      } else {
        return resp.tablet_locations().size();
      }
    }
  }

  // Insert rows into the given table. The first column 'c0' is ascending,
  // but the rest are random int32s. A single row will be inserted for each
  // unique c0 value in the range bounds. If there are no range bounds, then
  // c0 values [0, 1000) will be used. The number of inserted rows is returned
  // in 'row_count'.
  Status InsertRows(const RangePartitionOptions& range_partition, int* row_count);

  // Perform a scan with a predicate on 'col_name' BETWEEN 'lower' AND 'upper'.
  // Verifies that the results match up with applying the same scan against our
  // in-memory copy 'inserted_rows_'.
  void CheckScanWithColumnPredicate(Slice col_name, int lower, int upper);

  // Perform a scan via the Scan Token API with a predicate on 'col_name'
  // BETWEEN 'lower' AND 'upper'. Verifies that the results match up with
  // 'expected_rows'. Called by CheckScanWithColumnPredicates as an additional
  // check.
  void CheckScanTokensWithColumnPredicate(Slice col_name,
                                          int lower,
                                          int upper,
                                          const vector<string>& expected_rows);

  // Like the above, but uses the primary key range scan API in the client to
  // scan between 'inserted_rows_[lower]' (inclusive) and 'inserted_rows_[upper]'
  // (exclusive).
  void CheckPKRangeScan(int lower, int upper);
  void CheckPartitionKeyRangeScanWithPKRange(int lower, int upper);

  // Performs a series of scans, each over a single tablet in the table, and
  // verifies that the aggregated results match up with 'inserted_rows_'.
  void CheckPartitionKeyRangeScan();

  // Inserts data into the table, then performs a number of scans to verify that
  // the data can be retrieved.
  void InsertAndVerifyScans(const RangePartitionOptions& range_partition);

  Random random_;

  gscoped_ptr<ExternalMiniCluster> cluster_;

  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
  vector<unique_ptr<KuduPartialRow>> inserted_rows_;
};

Status FlexPartitioningITest::InsertRows(const RangePartitionOptions& range_partition,
                                         int* row_count) {
  static const vector<pair<vector<int32_t>, vector<int32_t>>> kDefaultBounds =
    {{ { 0 }, { 1000 } }};
  CHECK(inserted_rows_.empty());

  const vector<pair<vector<int32_t>, vector<int32_t>>>& bounds =
    range_partition.bounds.empty() ? kDefaultBounds : range_partition.bounds;

  shared_ptr<KuduSession> session(client_->NewSession());
  session->SetTimeoutMillis(60000);
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  int count = 0;
  for (const auto& bound : bounds) {
    for (int32_t i = bound.first[0]; i < bound.second[0]; i++) {
      gscoped_ptr<KuduInsert> insert(table_->NewInsert());
      tools::GenerateDataForRow(table_->schema(), i, &random_, insert->mutable_row());
      inserted_rows_.emplace_back(new KuduPartialRow(*insert->mutable_row()));
      RETURN_NOT_OK(session->Apply(insert.release()));
      count++;
    }
  }

  RETURN_NOT_OK(session->Flush());
  *row_count = count;
  return Status::OK();
}

void FlexPartitioningITest::CheckScanWithColumnPredicate(Slice col_name, int lower, int upper) {
  KuduScanner scanner(table_.get());
  ASSERT_OK(scanner.SetTimeoutMillis(60000));
  ASSERT_OK(scanner.AddConjunctPredicate(table_->NewComparisonPredicate(
      col_name, KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(lower))));
  ASSERT_OK(scanner.AddConjunctPredicate(table_->NewComparisonPredicate(
      col_name, KuduPredicate::LESS_EQUAL, KuduValue::FromInt(upper))));

  vector<string> rows;
  ASSERT_OK(ScanToStrings(&scanner, &rows));
  std::sort(rows.begin(), rows.end());

  // Manually evaluate the predicate against the data we think we inserted.
  vector<string> expected_rows;
  for (auto& row : inserted_rows_) {
    int32_t val;
    ASSERT_OK(row->GetInt32(col_name, &val));
    if (val >= lower && val <= upper) {
      expected_rows.push_back("(" + row->ToString() + ")");
    }
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(expected_rows.size(), rows.size());
  ASSERT_EQ(expected_rows, rows);

  NO_FATALS(CheckScanTokensWithColumnPredicate(col_name, lower, upper, expected_rows));
}

void FlexPartitioningITest::CheckScanTokensWithColumnPredicate(
    Slice col_name, int lower, int upper, const vector<string>& expected_rows) {
  KuduScanTokenBuilder builder(table_.get());
  ASSERT_OK(builder.SetTimeoutMillis(60000));

  ASSERT_OK(builder.AddConjunctPredicate(table_->NewComparisonPredicate(
      col_name, KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(lower))));
  ASSERT_OK(builder.AddConjunctPredicate(table_->NewComparisonPredicate(
      col_name, KuduPredicate::LESS_EQUAL, KuduValue::FromInt(upper))));

  vector<KuduScanToken*> tokens;
  ElementDeleter DeleteTable(&tokens);
  ASSERT_OK(builder.Build(&tokens));

  vector<string> rows;
  for (auto token : tokens) {
    KuduScanner* scanner_ptr;
    ASSERT_OK(token->IntoKuduScanner(&scanner_ptr));
    unique_ptr<KuduScanner> scanner(scanner_ptr);
    ASSERT_OK(ScanToStrings(scanner.get(), &rows));
  }
  std::sort(rows.begin(), rows.end());

  ASSERT_EQ(expected_rows.size(), rows.size());
  ASSERT_EQ(expected_rows, rows);
}

void FlexPartitioningITest::CheckPKRangeScan(int lower, int upper) {
  KuduScanner scanner(table_.get());
  scanner.SetTimeoutMillis(60000);
  ASSERT_OK(scanner.AddLowerBound(*inserted_rows_[lower]));
  ASSERT_OK(scanner.AddExclusiveUpperBound(*inserted_rows_[upper]));
  vector<string> rows;
  ASSERT_OK(ScanToStrings(&scanner, &rows));
  std::sort(rows.begin(), rows.end());

  vector<string> expected_rows;
  for (int i = lower; i < upper; i++) {
    expected_rows.push_back("(" + inserted_rows_[i]->ToString() + ")");
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(rows.size(), expected_rows.size());
  ASSERT_EQ(rows, expected_rows);
}

void FlexPartitioningITest::CheckPartitionKeyRangeScan() {
  GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(),
                              table_->name(),
                              MonoDelta::FromSeconds(32),
                              master::VOTER_REPLICA,
                              /*table_id=*/none,
                              &table_locations));

  vector<string> rows;

  for (const master::TabletLocationsPB& tablet_locations :
                table_locations.tablet_locations()) {

    string partition_key_start = tablet_locations.partition().partition_key_start();
    string partition_key_end = tablet_locations.partition().partition_key_end();

    KuduScanner scanner(table_.get());
    scanner.SetTimeoutMillis(60000);
    ASSERT_OK(scanner.AddLowerBoundPartitionKeyRaw(partition_key_start));
    ASSERT_OK(scanner.AddExclusiveUpperBoundPartitionKeyRaw(partition_key_end));
    ASSERT_OK(ScanToStrings(&scanner, &rows));
  }
  std::sort(rows.begin(), rows.end());

  vector<string> expected_rows;
  for (auto& row : inserted_rows_) {
    expected_rows.push_back("(" + row->ToString() + ")");
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(rows.size(), expected_rows.size());
  ASSERT_EQ(rows, expected_rows);
}

void FlexPartitioningITest::CheckPartitionKeyRangeScanWithPKRange(int lower, int upper) {
  GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(),
                              table_->name(),
                              MonoDelta::FromSeconds(32),
                              master::VOTER_REPLICA,
                              /*table_id=*/none,
                              &table_locations));
  vector<string> rows;

  for (const master::TabletLocationsPB& tablet_locations :
                table_locations.tablet_locations()) {

    string partition_key_start = tablet_locations.partition().partition_key_start();
    string partition_key_end = tablet_locations.partition().partition_key_end();

    KuduScanner scanner(table_.get());
    scanner.SetTimeoutMillis(60000);
    ASSERT_OK(scanner.AddLowerBoundPartitionKeyRaw(partition_key_start));
    ASSERT_OK(scanner.AddExclusiveUpperBoundPartitionKeyRaw(partition_key_end));
    ASSERT_OK(scanner.AddLowerBound(*inserted_rows_[lower]));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*inserted_rows_[upper]));
    ASSERT_OK(ScanToStrings(&scanner, &rows));
  }
  std::sort(rows.begin(), rows.end());

  vector<string> expected_rows;
  for (int i = lower; i < upper; i++) {
    expected_rows.push_back("(" + inserted_rows_[i]->ToString() + ")");
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(rows.size(), expected_rows.size());
  ASSERT_EQ(rows, expected_rows);
}

void FlexPartitioningITest::InsertAndVerifyScans(const RangePartitionOptions& range_partition) {
  int row_count;
  ASSERT_OK(InsertRows(range_partition, &row_count));

  // First, ensure that we get back the same number we put in.
  {
    vector<string> rows;
    ScanTableToStrings(table_.get(), &rows);
    std::sort(rows.begin(), rows.end());
    ASSERT_EQ(row_count, rows.size());
  }

  // Perform some scans with predicates.

  // 1) Various predicates on 'c0', which has non-random data.
  // We concentrate around the value '500' since there is a split point
  // there.
  NO_FATALS(CheckScanWithColumnPredicate("c0", 100, 120));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 490, 610));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 499, 499));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 500, 500));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 501, 501));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 499, 501));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 499, 500));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 500, 501));

  // 2) Random range predicates on the other columns, which are random ints.
  for (int col_idx = 1; col_idx < table_->schema().num_columns(); col_idx++) {
    SCOPED_TRACE(col_idx);
    for (int i = 0; i < 10; i++) {
      int32_t lower = random_.Next32();
      int32_t upper = random_.Next32();
      if (upper < lower) {
        std::swap(lower, upper);
      }

      NO_FATALS(CheckScanWithColumnPredicate(table_->schema().Column(col_idx).name(),
                                             lower, upper));
    }
  }

  // 3) Use the "primary key range" API.
  {
    NO_FATALS(CheckPKRangeScan(100, 120));
    NO_FATALS(CheckPKRangeScan(490, 610));
    NO_FATALS(CheckPKRangeScan(499, 499));
    NO_FATALS(CheckPKRangeScan(500, 500));
    NO_FATALS(CheckPKRangeScan(501, 501));
    NO_FATALS(CheckPKRangeScan(499, 501));
    NO_FATALS(CheckPKRangeScan(499, 500));
    NO_FATALS(CheckPKRangeScan(500, 501));
  }

  // 4) Use the Per-tablet "partition key range" API.
  {
    NO_FATALS(CheckPartitionKeyRangeScan());
  }

  // 5) Use the Per-tablet "partition key range" API with primary key range.
  {
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(100, 120));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(200, 400));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(490, 610));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(499, 499));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(500, 500));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(501, 501));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(499, 501));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(499, 500));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(500, 501));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(650, 700));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(700, 800));
  }
}

const vector<vector<HashPartitionOptions>> kHashOptions {
  // No hash partitioning
  {},
  // HASH (c1) INTO 4 BUCKETS
  { HashPartitionOptions { { "c1" }, 4 } },
  // HASH (c0, c1) INTO 3 BUCKETS
  { HashPartitionOptions { { "c0", "c1" }, 3 } },
  // HASH (c1, c0) INTO 3 BUCKETS, HASH (c2) INTO 3 BUCKETS
  { HashPartitionOptions { { "c1", "c0" }, 3 },
    HashPartitionOptions { { "c2" }, 3 } },
  // HASH (c2) INTO 2 BUCKETS, HASH (c1) INTO 2 BUCKETS, HASH (c0) INTO 2 BUCKETS
  { HashPartitionOptions { { "c2" }, 2 },
    HashPartitionOptions { { "c1" }, 2 },
    HashPartitionOptions { { "c0" }, 2 } },
};

const vector<RangePartitionOptions> kRangeOptions {
  // No range partitioning
  RangePartitionOptions { {}, {}, {} },
  // RANGE (c0)
  RangePartitionOptions { { "c0" }, { }, { } },
  // RANGE (c0) SPLIT ROWS (500)
  RangePartitionOptions { { "c0" }, { { 500 } }, { } },
  // RANGE (c2, c1) SPLIT ROWS (500, 0), (500, 500), (1000, 0)
  RangePartitionOptions { { "c2", "c1" }, { { 500, 0 }, { 500, 500 }, { 1000, 0 } }, { } },
  // RANGE (c0) BOUNDS ((0), (500)), ((500), (1000))
  RangePartitionOptions { { "c0" }, { }, { { { 0 }, { 500 } }, { { 500 }, { 1000 } } } },
  // RANGE (c0) SPLIT ROWS (500) BOUNDS ((0), (1000))
  RangePartitionOptions { { "c0" }, { }, { { { 0 }, { 500 } }, { { 500 }, { 1000 } } } },
  // RANGE (c0, c1) SPLIT ROWS (500), (2001), (2500), (2999)
  //                BOUNDS ((0), (1000)), ((2000), (3000))
   RangePartitionOptions{ { "c0", "c1" }, { { 500 }, { 2001 }, { 2500 }, { 2999 } },
                          { { { 0 }, { 1000 } }, { { 2000 }, { 3000 } } } },
};

// Instantiate all combinations of hash options and range options.
INSTANTIATE_TEST_CASE_P(Shards, FlexPartitioningITest,
                        testing::Combine(
                            testing::ValuesIn(kHashOptions),
                            testing::ValuesIn(kRangeOptions)));

TEST_P(FlexPartitioningITest, TestFlexPartitioning) {
  const auto& hash_option = std::get<0>(GetParam());
  const auto& range_option = std::get<1>(GetParam());
  NO_FATALS(TestPartitionOptions(hash_option, range_option));
}
} // namespace itest
} // namespace kudu

// Define a gtest printer overload so that the test output clearly identifies the test case that
// failed.
namespace testing {
template <>
std::string PrintToString<kudu::itest::TestParamType>(const kudu::itest::TestParamType& param) {
  const auto& hash_option = std::get<0>(param);
  const auto& range_option = std::get<1>(param);
  return kudu::itest::PartitionOptionsToString(hash_option, range_option);
}
} // namespace testing
