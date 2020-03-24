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
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/types.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/type_traits.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/char_util.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/int128.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_rows_per_tablet, 100, "The number of rows to be inserted into each tablet");

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::client::sp::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

static const int kNumTabletServers = 3;
static const int kNumTablets = 3;
static const int kMaxBatchSize = 8 * 1024 * 1024;

template<typename KeyTypeWrapper>
struct SliceKeysTestSetup {

  SliceKeysTestSetup()
   : max_rows_(MathLimits<int>::kMax),
     rows_per_tablet_(std::min(max_rows_/ kNumTablets, FLAGS_num_rows_per_tablet)),
     increment_(static_cast<int>(MathLimits<int>::kMax / kNumTablets)) {
  }

  void AddKeyColumnsToSchema(KuduSchemaBuilder* builder) const {
    auto column_spec = builder->AddColumn("key");
    column_spec->Type(client::FromInternalDataType(KeyTypeWrapper::kType))
        ->NotNull()->PrimaryKey();
    if (KeyTypeWrapper::kType == DECIMAL32) {
      column_spec->Precision(kMaxDecimal32Precision);
    } else if (KeyTypeWrapper::kType == DECIMAL64) {
      column_spec->Precision(kMaxDecimal64Precision);
    } else if (KeyTypeWrapper::kType == DECIMAL128) {
      column_spec->Precision(kMaxDecimal128Precision);
    }
  }

  // Split points are calculated by equally partitioning the int64_t key space and then
  // using the stringified hexadecimal representation to create the split keys (with
  // zero padding).
  vector<const KuduPartialRow*> GenerateSplitRows(const KuduSchema& schema) const {
    vector<string> splits;
    splits.reserve(kNumTablets - 1);
    for (int i = 1; i < kNumTablets; i++) {
      int split = i * increment_;
      splits.push_back(StringPrintf("%08x", split));
    }
    vector<const KuduPartialRow*> rows;
    for (string val : splits) {
      Slice slice(val);
      KuduPartialRow* row = schema.NewRow();
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::kType> >(0, slice));
      rows.push_back(row);
    }
    return rows;
  }

  Status GenerateRowKey(KuduInsert* insert, int split_idx, int row_idx) const {
    int row_key_num = (split_idx * increment_) + row_idx;
    string row_key = StringPrintf("%08x", row_key_num);
    Slice row_key_slice(row_key);
    return insert->mutable_row()->SetSliceCopy<TypeTraits<KeyTypeWrapper::kType> >(0,
                                                                                  row_key_slice);
  }

  Status VerifyRowKeySlice(Slice row_key_slice, int split_idx, int row_idx) const {
    int expected_row_key_num = (split_idx * increment_) + row_idx;
    string expected_row_key = StringPrintf("%08x", expected_row_key_num);
    Slice expected_row_key_slice(expected_row_key);
    if (expected_row_key_slice.compare(row_key_slice) != 0) {
      return Status::Corruption(strings::Substitute("Keys didn't match. Expected: $0 Got: $1",
                                                    expected_row_key_slice.ToDebugString(),
                                                    row_key_slice.ToDebugString()));
    }
    return Status::OK();
  }

  Status VerifyRowKey(const KuduRowResult& result, int split_idx, int row_idx) const {
    Slice row_key;
    RETURN_NOT_OK(result.Get<TypeTraits<KeyTypeWrapper::kType>>(0, &row_key));
    return VerifyRowKeySlice(row_key, split_idx, row_idx);
  }

  Status VerifyRowKeyRaw(const uint8_t* raw_key, int split_idx, int row_idx) const {
    Slice row_key = *reinterpret_cast<const Slice*>(raw_key);
    return VerifyRowKeySlice(row_key, split_idx, row_idx);
  }

  int GetRowsPerTablet() const {
    return rows_per_tablet_;
  }

  int GetMaxRows() const {
    return max_rows_;
  }

  vector<string> GetKeyColumns() const {
    vector<string> key_col;
    key_col.emplace_back("key");
    return key_col;
  }

  int max_rows_;
  int rows_per_tablet_;
  int increment_;
};

template<typename KeyTypeWrapper>
struct IntKeysTestSetup {
  typedef typename TypeTraits<KeyTypeWrapper::kType>::cpp_type CppType;

  IntKeysTestSetup()
     // If CppType is actually bigger than int (e.g. int64_t) casting the max to int
     // returns -1, so we make sure in that case we get max from int directly.
   : max_rows_(static_cast<int>(MathLimits<CppType>::kMax) != -1 ?
       static_cast<int>(MathLimits<CppType>::kMax) : MathLimits<int>::kMax),
     increment_(max_rows_ / kNumTablets),
     rows_per_tablet_(std::min(increment_, FLAGS_num_rows_per_tablet)) {
    DCHECK(base::is_integral<CppType>::value);
  }

  void AddKeyColumnsToSchema(KuduSchemaBuilder* builder) const {
    auto column_spec = builder->AddColumn("key");
    column_spec->Type(client::FromInternalDataType(KeyTypeWrapper::kType))
        ->NotNull()->PrimaryKey();
    if (KeyTypeWrapper::kType == DECIMAL32) {
      column_spec->Precision(kMaxDecimal32Precision);
    } else if (KeyTypeWrapper::kType == DECIMAL64) {
      column_spec->Precision(kMaxDecimal64Precision);
    } else if (KeyTypeWrapper::kType == DECIMAL128) {
      column_spec->Precision(kMaxDecimal128Precision);
    }
  }

  vector<const KuduPartialRow*> GenerateSplitRows(const KuduSchema& schema) const {
    vector<CppType> splits;
    splits.reserve(kNumTablets - 1);
    for (int64_t i = 1; i < kNumTablets; i++) {
      splits.push_back(i * increment_);
    }
    vector<const KuduPartialRow*> rows;
    for (CppType val : splits) {
      KuduPartialRow* row = schema.NewRow();
      CHECK_OK(row->Set<TypeTraits<KeyTypeWrapper::kType> >(0, val));
      rows.push_back(row);
    }
    return rows;
  }

  Status GenerateRowKey(KuduInsert* insert, int split_idx, int row_idx) const {
    CppType val = (split_idx * increment_) + row_idx;
    return insert->mutable_row()->Set<TypeTraits<KeyTypeWrapper::kType> >(0, val);
  }

  Status VerifyIntRowKey(CppType val, int split_idx, int row_idx) const {
    int expected = (split_idx * increment_) + row_idx;
    if (val != expected) {
      return Status::Corruption(strings::Substitute("Keys didn't match. Expected: $0 Got: $1",
                                                    expected, val));
    }
    return Status::OK();
  }

  Status VerifyRowKey(const KuduRowResult& result, int split_idx, int row_idx) const {
    CppType val;
    RETURN_NOT_OK(result.Get<TypeTraits<KeyTypeWrapper::kType>>(0, &val));
    return VerifyIntRowKey(val, split_idx, row_idx);
  }

  Status VerifyRowKeyRaw(const uint8_t* raw_key, int split_idx, int row_idx) const {
    CppType val = UnalignedLoad<CppType>(raw_key);
    return VerifyIntRowKey(val, split_idx, row_idx);
  }

  int GetRowsPerTablet() const {
    return rows_per_tablet_;
  }

  int GetMaxRows() const {
    return max_rows_;
  }

  vector<string> GetKeyColumns() const {
    vector<string> key_col;
    key_col.emplace_back("key");
    return key_col;
  }

  int max_rows_;
  int increment_;
  int rows_per_tablet_;
};

struct DateKeysTestSetup {

  DateKeysTestSetup()
   : min_value(DataTypeTraits<DATE>::kMinValue),
     max_rows_(DataTypeTraits<DATE>::kMaxValue - min_value),
     increment_(max_rows_ / kNumTablets),
     rows_per_tablet_(std::min(increment_, FLAGS_num_rows_per_tablet)) {
  }

  void AddKeyColumnsToSchema(KuduSchemaBuilder* builder) const {
    auto column_spec = builder->AddColumn("key");
    column_spec->Type(KuduColumnSchema::DATE)
        ->NotNull()->PrimaryKey();
  }

  vector<const KuduPartialRow*> GenerateSplitRows(const KuduSchema& schema) const {
    vector<int> splits;
    splits.reserve(kNumTablets - 1);
    for (int64_t i = 1; i < kNumTablets; i++) {
      splits.push_back(min_value + i * increment_);
    }
    vector<const KuduPartialRow*> rows;
    for (int val : splits) {
      KuduPartialRow* row = schema.NewRow();
      CHECK_OK(row->SetDate(0, val));
      rows.push_back(row);
    }
    return rows;
  }

  Status GenerateRowKey(KuduInsert* insert, int split_idx, int row_idx) const {
    int val = min_value + (split_idx * increment_) + row_idx;
    return insert->mutable_row()->SetDate(0, val);
  }

  Status VerifyIntRowKey(int val, int split_idx, int row_idx) const {
    int expected = min_value + (split_idx * increment_) + row_idx;
    if (val != expected) {
      return Status::Corruption(strings::Substitute("Keys didn't match. Expected: $0 Got: $1",
                                                    expected, val));
    }
    return Status::OK();
  }

  Status VerifyRowKey(const KuduRowResult& result, int split_idx, int row_idx) const {
    int val;
    RETURN_NOT_OK(result.GetDate(0, &val));
    return VerifyIntRowKey(val, split_idx, row_idx);
  }

  Status VerifyRowKeyRaw(const uint8_t* raw_key, int split_idx, int row_idx) const {
    int val = UnalignedLoad<int32_t>(raw_key);
    return VerifyIntRowKey(val, split_idx, row_idx);
  }

  int GetRowsPerTablet() const {
    return rows_per_tablet_;
  }

  int GetMaxRows() const {
    return max_rows_;
  }

  vector<string> GetKeyColumns() const {
    vector<string> key_col;
    key_col.emplace_back("key");
    return key_col;
  }

  int min_value;
  int max_rows_;
  int increment_;
  int rows_per_tablet_;
};

struct ExpectedVals {
  int8_t expected_int8_val;
  int16_t expected_int16_val;
  int32_t expected_int32_val;
  int64_t expected_int64_val;
  int64_t expected_timestamp_val;
  int32_t expected_date_val;
  string slice_content;
  Slice expected_slice_val;
  Slice expected_binary_val;
  Slice expected_varchar_val;
  bool expected_bool_val;
  float expected_float_val;
  double expected_double_val;
  int128_t expected_decimal_val;
};

// Integration that writes, scans and verifies all types.
template <class TestSetup>
class AllTypesItest : public KuduTest {
 public:
  AllTypesItest() {
    if (AllowSlowTests()) {
      FLAGS_num_rows_per_tablet = 10000;
    }
    SeedRandom();
    setup_ = TestSetup();
  }

  // Builds a schema that includes all (frontend) supported types.
  // The key is templated so that we can try different key types.
  void CreateAllTypesSchema() {
    KuduSchemaBuilder builder;
    setup_.AddKeyColumnsToSchema(&builder);
    builder.AddColumn("int8_val")->Type(KuduColumnSchema::INT8);
    builder.AddColumn("int16_val")->Type(KuduColumnSchema::INT16);
    builder.AddColumn("int32_val")->Type(KuduColumnSchema::INT32);
    builder.AddColumn("int64_val")->Type(KuduColumnSchema::INT64);
    builder.AddColumn("timestamp_val")->Type(KuduColumnSchema::UNIXTIME_MICROS);
    builder.AddColumn("date_val")->Type(KuduColumnSchema::DATE);
    builder.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    builder.AddColumn("varchar_val")->Type(KuduColumnSchema::VARCHAR)->Length(kMaxVarcharLength);
    builder.AddColumn("bool_val")->Type(KuduColumnSchema::BOOL);
    builder.AddColumn("float_val")->Type(KuduColumnSchema::FLOAT);
    builder.AddColumn("double_val")->Type(KuduColumnSchema::DOUBLE);
    builder.AddColumn("binary_val")->Type(KuduColumnSchema::BINARY);
    builder.AddColumn("decimal32_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal32Precision);
    builder.AddColumn("decimal64_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal64Precision);
    builder.AddColumn("decimal128_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal128Precision);
    CHECK_OK(builder.Build(&schema_));
  }

  Status CreateCluster() {
    static const vector<string> kTsFlags = {
      // Set the flush threshold low so that we have flushes and test the on-disk formats.
      "--flush_threshold_mb=1",

      // Set the major delta compaction ratio low enough that we trigger a lot of them.
      "--tablet_delta_store_major_compact_min_ratio=0.001",

      // TODO(KUDU-1346) Remove custom consensus_max_batch_size_bytes setting
      // once KUDU-1346 is fixed. It's necessary to change the default
      // value of the consensus_max_batch_size_bytes flag to avoid
      // triggering debug assert when a relatively big chunk of write operations
      // is flushed to the tablet server.
      "--consensus_max_batch_size_bytes=2097152",
    };

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;

    for (const std::string& flag : kTsFlags) {
      opts.extra_tserver_flags.push_back(flag);
    }

    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    RETURN_NOT_OK(cluster_->Start());
    return cluster_->CreateClient(nullptr, &client_);
  }

  Status CreateTable() {
    CreateAllTypesSchema();
    vector<const KuduPartialRow*> split_rows = setup_.GenerateSplitRows(schema_);
    unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());

    for (const KuduPartialRow* row : split_rows) {
      split_rows_.push_back(*row);
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    RETURN_NOT_OK(table_creator->table_name("all-types-table")
                  .schema(&schema_)
                  .set_range_partition_columns({ "key" })
                  .split_rows(split_rows)
                  .num_replicas(kNumTabletServers)
                  .Create());
#pragma GCC diagnostic pop
    return client_->OpenTable("all-types-table", &table_);
  }

  Status GenerateRow(KuduSession* session, int split_idx, int row_idx) {
    KuduInsert* insert = table_->NewInsert();
    RETURN_NOT_OK(setup_.GenerateRowKey(insert, split_idx, row_idx));
    int int_val = (split_idx * setup_.GetRowsPerTablet()) + row_idx;
    KuduPartialRow* row = insert->mutable_row();
    RETURN_NOT_OK(row->SetInt8("int8_val", int_val));
    RETURN_NOT_OK(row->SetInt16("int16_val", int_val));
    RETURN_NOT_OK(row->SetInt32("int32_val", int_val));
    RETURN_NOT_OK(row->SetInt64("int64_val", int_val));
    RETURN_NOT_OK(row->SetUnixTimeMicros("timestamp_val", int_val));
    RETURN_NOT_OK(row->SetDate("date_val", int_val));
    string content = strings::Substitute("hello $0", int_val);
    Slice slice_val(content);
    RETURN_NOT_OK(row->SetStringCopy("string_val", slice_val));
    RETURN_NOT_OK(row->SetBinaryCopy("binary_val", slice_val));
    RETURN_NOT_OK(row->SetVarchar("varchar_val", slice_val));
    double double_val = int_val;
    RETURN_NOT_OK(row->SetDouble("double_val", double_val));
    RETURN_NOT_OK(row->SetFloat("float_val", double_val));
    RETURN_NOT_OK(row->SetBool("bool_val", int_val % 2));
    RETURN_NOT_OK(row->SetUnscaledDecimal("decimal32_val", int_val));
    RETURN_NOT_OK(row->SetUnscaledDecimal("decimal64_val", int_val));
    RETURN_NOT_OK(row->SetUnscaledDecimal("decimal128_val", int_val));
    VLOG(1) << "Inserting row[" << split_idx << "," << row_idx << "]" << insert->ToString();
    RETURN_NOT_OK(session->Apply(insert));
    return Status::OK();
  }

  // This inserts kNumRowsPerTablet in each of the tablets. In the end we should have
  // perfectly partitioned table, if the encoding of the keys was correct and the rows
  // ended up in the right place.
  Status InsertRows() {
    shared_ptr<KuduSession> session = client_->NewSession();
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    const int max_rows_per_tablet = setup_.GetRowsPerTablet();
    for (int i = 0; i < kNumTablets; ++i) {
      for (int j = 0; j < max_rows_per_tablet; ++j) {
        RETURN_NOT_OK(GenerateRow(session.get(), i, j));
      }
    }
    RETURN_NOT_OK(session->Flush());
    return Status::OK();
  }

  void SetupProjection(vector<string>* projection) {
    vector<string> keys = setup_.GetKeyColumns();
    for (const string& key : keys) {
      projection->push_back(key);
    }
    projection->push_back("int8_val");
    projection->push_back("int16_val");
    projection->push_back("int32_val");
    projection->push_back("int64_val");
    projection->push_back("timestamp_val");
    projection->push_back("date_val");
    projection->push_back("string_val");
    projection->push_back("binary_val");
    projection->push_back("varchar_val");
    projection->push_back("double_val");
    projection->push_back("float_val");
    projection->push_back("bool_val");
    projection->push_back("decimal32_val");
    projection->push_back("decimal64_val");
    projection->push_back("decimal128_val");
  }

  ExpectedVals GetExpectedValsForRow(int split_idx, int row_idx) {
    ExpectedVals vals;
    int64_t expected_int_val = (split_idx * setup_.GetRowsPerTablet()) + row_idx;
    vals.expected_int8_val = static_cast<int8_t>(expected_int_val);
    vals.expected_int16_val = static_cast<int16_t>(expected_int_val);
    vals.expected_int32_val = static_cast<int32_t>(expected_int_val);
    vals.expected_int64_val = expected_int_val;
    vals.expected_timestamp_val = expected_int_val;
    vals.expected_date_val = static_cast<int32_t>(expected_int_val);
    vals.slice_content = strings::Substitute("hello $0", expected_int_val);
    vals.expected_slice_val = Slice(vals.slice_content);
    vals.expected_varchar_val = Slice(vals.slice_content);
    vals.expected_binary_val = Slice(vals.slice_content);
    vals.expected_bool_val = expected_int_val % 2;
    vals.expected_float_val = expected_int_val;
    vals.expected_double_val = expected_int_val;
    vals.expected_decimal_val = expected_int_val;
    return vals;
  }

  void VerifyRow(const KuduRowResult& row, int split_idx, int row_idx) {
    ASSERT_OK(setup_.VerifyRowKey(row, split_idx, row_idx));

    ExpectedVals vals = GetExpectedValsForRow(split_idx, row_idx);

    int8_t int8_val;
    ASSERT_OK(row.GetInt8("int8_val", &int8_val));
    ASSERT_EQ(int8_val, vals.expected_int8_val);
    int16_t int16_val;
    ASSERT_OK(row.GetInt16("int16_val", &int16_val));
    ASSERT_EQ(int16_val, vals.expected_int16_val);
    int32_t int32_val;
    ASSERT_OK(row.GetInt32("int32_val", &int32_val));
    ASSERT_EQ(int32_val, vals.expected_int32_val);
    int64_t int64_val;
    ASSERT_OK(row.GetInt64("int64_val", &int64_val));
    ASSERT_EQ(int64_val, vals.expected_int64_val);
    int64_t timestamp_val;
    ASSERT_OK(row.GetUnixTimeMicros("timestamp_val", &timestamp_val));
    ASSERT_EQ(timestamp_val, vals.expected_timestamp_val);
    int32_t date_val;
    ASSERT_OK(row.GetDate("date_val", &date_val));
    ASSERT_EQ(date_val, vals.expected_date_val);
    Slice string_val;
    ASSERT_OK(row.GetString("string_val", &string_val));
    ASSERT_EQ(string_val, vals.expected_slice_val);
    Slice binary_val;
    ASSERT_OK(row.GetBinary("binary_val", &binary_val));
    ASSERT_EQ(binary_val, vals.expected_binary_val);
    Slice varchar_val;
    ASSERT_OK(row.GetVarchar("varchar_val", &varchar_val));
    ASSERT_EQ(varchar_val, vals.expected_varchar_val);
    bool bool_val;
    ASSERT_OK(row.GetBool("bool_val", &bool_val));
    ASSERT_EQ(bool_val, vals.expected_bool_val);
    double double_val;
    ASSERT_OK(row.GetDouble("double_val", &double_val));
    ASSERT_EQ(double_val, vals.expected_double_val);
    float float_val;
    ASSERT_OK(row.GetFloat("float_val", &float_val));
    ASSERT_EQ(float_val, vals.expected_float_val);
    int128_t decimal32_val;
    ASSERT_OK(row.GetUnscaledDecimal("decimal32_val", &decimal32_val));
    ASSERT_EQ(decimal32_val, vals.expected_decimal_val);
    int128_t decimal64_val;
    ASSERT_OK(row.GetUnscaledDecimal("decimal64_val", &decimal64_val));
    ASSERT_EQ(decimal64_val, vals.expected_decimal_val);
    int128_t decimal128_val;
    ASSERT_OK(row.GetUnscaledDecimal("decimal128_val", &decimal128_val));
    ASSERT_EQ(decimal128_val, vals.expected_decimal_val);
  }

  typedef std::function<Status (KuduScanner* scanner)> ScannerSetup;
  typedef std::function<void (const KuduScanBatch& batch,
                              int num_tablet,
                              int* total_rows_in_tablet)> RowVerifier;

  Status VerifyRows(const ScannerSetup& scanner_setup, const RowVerifier& verifier) {
    int total_rows = 0;
    // Scan a single tablet and make sure it has the rows we expect in the amount we
    // expect.
    for (int i = 0; i < kNumTablets; ++i) {
      KuduScanner scanner(table_.get());
      string low_split;
      string high_split;
      if (i != 0) {
        const KuduPartialRow& split = split_rows_[i - 1];
        RETURN_NOT_OK(scanner.AddLowerBound(split));
        low_split = split.ToString();
      }
      if (i != kNumTablets - 1) {
        const KuduPartialRow& split = split_rows_[i];
        RETURN_NOT_OK(scanner.AddExclusiveUpperBound(split));
        high_split = split.ToString();
      }

      RETURN_NOT_OK(scanner_setup(&scanner));
      RETURN_NOT_OK(scanner.SetBatchSizeBytes(kMaxBatchSize));
      RETURN_NOT_OK(scanner.SetFaultTolerant());
      RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));

      RETURN_NOT_OK(scanner.Open());
      LOG(INFO) << "Scanning tablet: [" << low_split << ", " << high_split << ")";

      int total_rows_in_tablet = 0;
      while (scanner.HasMoreRows()) {
        KuduScanBatch batch;
        scanner.NextBatch(&batch);
        verifier(batch, i, &total_rows_in_tablet);
      }
      CHECK_EQ(total_rows_in_tablet, setup_.GetRowsPerTablet());
      total_rows += total_rows_in_tablet;
    }
    CHECK_EQ(total_rows, setup_.GetRowsPerTablet() * kNumTablets);
    return Status::OK();
  }

  void RunTest(const ScannerSetup& scanner_setup, const RowVerifier& verifier) {
    ASSERT_OK(CreateCluster());
    ASSERT_OK(CreateTable());
    ASSERT_OK(InsertRows());
    // Check that all of the replicas agree on the inserted data. This retries until
    // all replicas are up-to-date, which is important to ensure that the following
    // Verify always passes.
    NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
    // Check that the inserted data matches what we thought we inserted.
    ASSERT_OK(VerifyRows(scanner_setup, verifier));
  }

  virtual void TearDown() OVERRIDE {
    cluster_->AssertNoCrashes();
    cluster_->Shutdown();
  }

 protected:
  TestSetup setup_;
  KuduSchema schema_;
  vector<KuduPartialRow> split_rows_;
  shared_ptr<KuduClient> client_;
  unique_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduTable> table_;
};

// Wrap the actual DataType so that we can have the setup structs be friends of other classes
// without leaking DataType.
template<DataType KeyType>
struct KeyTypeWrapper {
  static const DataType kType = KeyType;
};

typedef ::testing::Types<IntKeysTestSetup<KeyTypeWrapper<INT8> >,
                         IntKeysTestSetup<KeyTypeWrapper<INT16> >,
                         IntKeysTestSetup<KeyTypeWrapper<INT32> >,
                         IntKeysTestSetup<KeyTypeWrapper<INT64> >,
                         IntKeysTestSetup<KeyTypeWrapper<DECIMAL32> >,
                         IntKeysTestSetup<KeyTypeWrapper<DECIMAL64> >,
                         IntKeysTestSetup<KeyTypeWrapper<DECIMAL128> >,
                         IntKeysTestSetup<KeyTypeWrapper<UNIXTIME_MICROS> >,
                         DateKeysTestSetup,
                         SliceKeysTestSetup<KeyTypeWrapper<STRING> >,
                         SliceKeysTestSetup<KeyTypeWrapper<BINARY> >
                         > KeyTypes;

TYPED_TEST_CASE(AllTypesItest, KeyTypes);

TYPED_TEST(AllTypesItest, TestAllKeyTypes) {
  vector<string> projection;
  this->SetupProjection(&projection);
  auto scanner_setup = [&](KuduScanner* scanner) {
    return scanner->SetProjectedColumnNames(projection);
  };
  auto row_verifier = [&](const KuduScanBatch& batch, int num_tablet, int* total_rows_in_tablet) {
    for (int i = 0; i < batch.NumRows(); i++) {
      NO_FATALS(this->VerifyRow(batch.Row(i), num_tablet, *total_rows_in_tablet + i));
    }
    *total_rows_in_tablet += batch.NumRows();
  };

  this->RunTest(scanner_setup, row_verifier);
}

TYPED_TEST(AllTypesItest, TestTimestampPadding) {
  vector<string> projection;
  this->SetupProjection(&projection);
  auto scanner_setup = [&](KuduScanner* scanner) -> Status {
    // Each time this function is called we shuffle the projection to get the chance
    // of having timestamps in different places of the projection and before/after
    // different types.
    std::random_shuffle(projection.begin(), projection.end());
    RETURN_NOT_OK(scanner->SetProjectedColumnNames(projection));
    int row_format_flags = KuduScanner::NO_FLAGS;
    row_format_flags |= KuduScanner::PAD_UNIXTIME_MICROS_TO_16_BYTES;
    return scanner->SetRowFormatFlags(row_format_flags);
  };

  auto row_verifier = [&](const KuduScanBatch& batch, int num_tablet, int* total_rows_in_tablet) {
    // Timestamps are padded to 16 bytes.
    int kPaddedTimestampSize = 16;

    // Calculate the projection size, each of the column offsets and the size of the null bitmap.
    const KuduSchema* schema = batch.projection_schema();
    vector<int> projection_offsets;
    int row_stride = 0;
    int num_nullable_cols = 0;
    for (int i = 0; i < schema->num_columns(); i++) {
      KuduColumnSchema col_schema = schema->Column(i);
      if (col_schema.is_nullable()) num_nullable_cols++;
      switch (col_schema.type()) {
        case KuduColumnSchema::UNIXTIME_MICROS:
          projection_offsets.push_back(kPaddedTimestampSize);
          row_stride += kPaddedTimestampSize;
          break;
        default:
          int col_size = GetTypeInfo(ToInternalDataType(col_schema.type(),
                                                        col_schema.type_attributes()))->size();
          projection_offsets.push_back(col_size);
          row_stride += col_size;
      }
    }

    int non_null_bitmap_size = BitmapSize(num_nullable_cols);
    row_stride += non_null_bitmap_size;

    Slice direct_data = batch.direct_data();

    ASSERT_EQ(direct_data.size(), row_stride * batch.NumRows());

    const uint8_t* row_data = direct_data.data();

    for (int i = 0; i < batch.NumRows(); i++) {
      for (int j = 0; j < schema->num_columns(); j++) {
        KuduColumnSchema col_schema = schema->Column(j);

        if (col_schema.name() == "key") {
          ASSERT_OK(this->setup_.VerifyRowKeyRaw(row_data, num_tablet, *total_rows_in_tablet + i));
        } else {
          ExpectedVals vals = this->GetExpectedValsForRow(num_tablet, *total_rows_in_tablet + i);
          DataType internal_type = ToInternalDataType(col_schema.type(),
                                                      col_schema.type_attributes());
          switch (col_schema.type()) {
            case KuduColumnSchema::INT8:
              ASSERT_EQ(*reinterpret_cast<const int8_t*>(row_data), vals.expected_int8_val);
              break;
            case KuduColumnSchema::INT16:
              ASSERT_EQ(*reinterpret_cast<const int16_t*>(row_data), vals.expected_int16_val);
              break;
            case KuduColumnSchema::INT32:
              ASSERT_EQ(*reinterpret_cast<const int32_t*>(row_data), vals.expected_int32_val);
              break;
            case KuduColumnSchema::INT64:
              ASSERT_EQ(*reinterpret_cast<const int64_t*>(row_data), vals.expected_int64_val);
              break;
            case KuduColumnSchema::UNIXTIME_MICROS:
              ASSERT_EQ(*reinterpret_cast<const int64_t*>(row_data), vals.expected_timestamp_val);
              break;
            case KuduColumnSchema::DATE:
              ASSERT_EQ(*reinterpret_cast<const int32_t*>(row_data), vals.expected_date_val);
              break;
            case KuduColumnSchema::STRING:
              ASSERT_EQ(*reinterpret_cast<const Slice*>(row_data), vals.expected_slice_val);
              break;
            case KuduColumnSchema::BINARY:
              ASSERT_EQ(*reinterpret_cast<const Slice*>(row_data), vals.expected_binary_val);
              break;
            case KuduColumnSchema::VARCHAR:
              ASSERT_EQ(*reinterpret_cast<const Slice*>(row_data), vals.expected_varchar_val);
              break;
            case KuduColumnSchema::BOOL:
              ASSERT_EQ(*reinterpret_cast<const bool*>(row_data), vals.expected_bool_val);
              break;
            case KuduColumnSchema::FLOAT:
              ASSERT_EQ(*reinterpret_cast<const float*>(row_data), vals.expected_float_val);
              break;
            case KuduColumnSchema::DOUBLE:
              ASSERT_EQ(*reinterpret_cast<const double*>(row_data), vals.expected_double_val);
              break;
            case KuduColumnSchema::DECIMAL:
              switch (internal_type) {
                case DECIMAL32:
                  ASSERT_EQ(*reinterpret_cast<const int32_t*>(row_data),
                            vals.expected_decimal_val);
                  break;
                case DECIMAL64:
                  ASSERT_EQ(*reinterpret_cast<const int64_t*>(row_data),
                            vals.expected_decimal_val);
                  break;
                case DECIMAL128:
                  ASSERT_EQ(UnalignedLoad<int128_t>(row_data),
                            vals.expected_decimal_val);
                  break;
                default:
                  LOG(FATAL) << "Unexpected internal decimal type: " << internal_type;
              }
              break;
            default:
              LOG(FATAL) << "Unexpected type: " << col_schema.type();
          }
        }
        row_data += projection_offsets[j];
      }
      row_data += non_null_bitmap_size;
    }
    *total_rows_in_tablet += batch.NumRows();
  };

  this->RunTest(scanner_setup, row_verifier);
}

} // namespace client
} // namespace kudu
